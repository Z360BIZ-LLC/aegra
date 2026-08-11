"""Background loop that dispatches runs queued behind an org's run limit.

When a run is held at its organization's concurrency ceiling it stays
``pending`` instead of being rejected. This loop watches for freed capacity
and re-dispatches the oldest queued run for each org, so the queue drains as
soon as an org "gets some breath".

It also expires runs that have waited past
``ORG_RUN_MAX_QUEUE_WAIT_SECONDS``: starting one hours late would answer a
conversation that has already moved on, so the run is failed and its webhook
fires, handing the retry decision back to the caller.

Replaces the lease reaper's stuck-pending sweep while limits are enforcing —
that sweep re-enqueues any pending run older than a threshold with no
capacity check, which would fight this loop.
"""

import asyncio
import contextlib

import structlog
from observability.cloudwatch_emf import emit_metric

from aegra_api.core.orm import _get_session_maker
from aegra_api.services import run_limits
from aegra_api.services.executor import executor
from aegra_api.services.run_status import set_thread_status
from aegra_api.services.webhook_service import send_run_webhook
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)

_QUEUE_EXPIRY_ERROR = "Run exceeded the maximum queue wait for its organization's concurrency limit"


class RunPromoter:
    """Dispatches queued runs as org capacity frees, and expires stale ones."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="run_promoter")
        logger.info(
            "Run promoter started",
            interval_seconds=settings.run_limits.ORG_RUN_PROMOTER_INTERVAL_SECONDS,
            limit=settings.run_limits.ORG_MAX_CONCURRENT_RUNS,
            max_queue_wait_seconds=settings.run_limits.ORG_RUN_MAX_QUEUE_WAIT_SECONDS,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Run promoter stopped")

    async def _loop(self) -> None:
        interval = settings.run_limits.ORG_RUN_PROMOTER_INTERVAL_SECONDS
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self.tick()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in run promoter")

    async def tick(self) -> None:
        """Expire overdue runs, then dispatch what capacity allows.

        Expiry runs first so a doomed run is not dispatched microseconds
        before being killed.
        """
        await self._expire_overdue()
        await self._promote_ready()

    @staticmethod
    async def _promote_ready() -> None:
        """Dispatch queued runs whose org now has a free slot."""
        maker = _get_session_maker()
        async with maker() as session:
            run_ids = await run_limits.find_promotable_runs(
                session,
                batch_size=settings.run_limits.ORG_RUN_PROMOTER_BATCH_SIZE,
            )

        if not run_ids:
            return

        logger.info("Promoting queued runs", count=len(run_ids))
        for run_id in run_ids:
            try:
                await executor.promote(run_id)
            except Exception:
                # One bad dispatch must not stop the rest of the batch; the
                # run stays pending and the next tick retries it.
                logger.exception("Failed to promote queued run", run_id=run_id)
                continue
            emit_metric("OrgRunPromoted", 1, properties={"run_id": run_id})

    async def _expire_overdue(self) -> None:
        """Fail runs that have waited past the maximum queue wait."""
        maker = _get_session_maker()
        async with maker() as session:
            overdue = await run_limits.find_expired_queued_runs(
                session,
                batch_size=settings.run_limits.ORG_RUN_PROMOTER_BATCH_SIZE,
            )

        for expired in overdue:
            await self._expire_run(expired)

    @staticmethod
    async def _expire_run(expired: run_limits.ExpiredRun) -> None:
        """Fail one overdue run, but only if it is still queued.

        The conditional claim is what makes this safe to run on every pod and
        alongside promotion: whoever wins the row owns the notification.
        """
        maker = _get_session_maker()
        async with maker() as session:
            won = await run_limits.claim_expired_run(session, expired.run_id, error=_QUEUE_EXPIRY_ERROR)
            if not won:
                await session.rollback()
                logger.debug("Overdue run already started or expired elsewhere", run_id=expired.run_id)
                return
            await set_thread_status(session, expired.thread_id, "error")
            await session.commit()

        logger.warning(
            "Expired run that exceeded the maximum queue wait",
            run_id=expired.run_id,
            org_id=expired.org_id,
        )
        emit_metric(
            "OrgRunQueueExpired",
            1,
            properties={
                "run_id": expired.run_id,
                "thread_id": expired.thread_id,
                "org_id": expired.org_id,
            },
        )

        if expired.webhook_url is None:
            return
        await send_run_webhook(
            webhook_url=expired.webhook_url,
            run_id=expired.run_id,
            thread_id=expired.thread_id,
            status="error",
            output={},
            error_message=_QUEUE_EXPIRY_ERROR,
        )


run_promoter = RunPromoter()
