"""Background task that recovers runs with expired worker leases.

Periodically scans the runs table for rows where
``status='running' AND lease_expires_at < now()``. It atomically either
returns them to ``pending`` or marks their retry budget exhausted, then
re-enqueues only retryable run IDs.
"""

import asyncio
import contextlib
from datetime import UTC, datetime

import structlog
from observability.cloudwatch_emf import emit_metric
from sqlalchemy import select, update

from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.observability.metrics import REAPER_RECOVERED_RUNS
from aegra_api.services.run_queue_signal import run_queue_signal
from aegra_api.services.run_status import set_thread_status_if_no_active_runs
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


class LeaseReaper:
    """Recovers runs whose worker leases have expired."""

    def __init__(self) -> None:
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "Lease reaper started",
            interval_seconds=settings.worker.REAPER_INTERVAL_SECONDS,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        logger.info("Lease reaper stopped")

    async def _loop(self) -> None:
        interval = settings.worker.REAPER_INTERVAL_SECONDS
        while self._running:
            await asyncio.sleep(interval)
            try:
                await self._reap()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in lease reaper")

    async def _reap(self) -> None:
        """Find crashed workers and recover their durable run rows."""
        crashed = await self._find_recoverable()

        if not crashed:
            return

        logger.warning("Reaping crashed worker runs", count=len(crashed), run_ids=crashed)
        retryable, exhausted = await self._recover_crashed_runs(crashed)
        if exhausted:
            REAPER_RECOVERED_RUNS.labels(outcome="crashed_exhausted").inc(len(exhausted))
            for run_id in exhausted:
                emit_metric("RunMaxRetriesExceeded", 1, properties={"run_id": run_id})
        if retryable:
            REAPER_RECOVERED_RUNS.labels(outcome="crashed_retried").inc(len(retryable))
            for run_id in retryable:
                emit_metric("LeaseExpiredRecovered", 1, properties={"run_id": run_id})

        logger.info(
            "Lease recovery complete",
            crashed_recovered=len(crashed),
        )

    @staticmethod
    async def _find_recoverable() -> list[str]:
        """Find running rows whose worker lease has expired."""
        now = datetime.now(UTC)
        maker = _get_session_maker()
        async with maker() as session:
            crashed_result = await session.execute(
                select(RunORM.run_id).where(
                    RunORM.status == "running",
                    RunORM.lease_expires_at.isnot(None),
                    RunORM.lease_expires_at < now,
                )
            )
            return [row[0] for row in crashed_result.fetchall()]

    @staticmethod
    async def _recover_crashed_runs(run_ids: list[str]) -> tuple[list[str], list[str]]:
        """Atomically classify expired runs and apply their next state."""
        now = datetime.now(UTC)
        max_retries = settings.worker.BG_JOB_MAX_RETRIES
        retryable: list[str] = []
        exhausted: list[str] = []
        exhausted_threads_by_user: dict[str, set[str]] = {}

        maker = _get_session_maker()
        async with maker() as session:
            locked_result = await session.execute(
                select(
                    RunORM.run_id,
                    RunORM.thread_id,
                    RunORM.user_id,
                    RunORM.execution_params,
                )
                .where(
                    RunORM.run_id.in_(run_ids),
                    RunORM.status == "running",
                    RunORM.lease_expires_at < now,
                )
                .with_for_update(skip_locked=True)
            )

            for run_id, thread_id, user_id, execution_params in locked_result.fetchall():
                params = dict(execution_params or {})
                retry_count = params.get("_retry_count", 0) + 1
                params["_retry_count"] = retry_count

                values: dict[str, object] = {
                    "execution_params": params,
                    "claimed_by": None,
                    "lease_expires_at": None,
                    "updated_at": now,
                }
                is_exhausted = retry_count > max_retries
                if is_exhausted:
                    values.update(
                        status="error",
                        error_message="Max retries exceeded after repeated worker failures",
                    )
                else:
                    values["status"] = "pending"

                update_result = await session.execute(
                    update(RunORM)
                    .where(
                        RunORM.run_id == run_id,
                        RunORM.user_id == user_id,
                        RunORM.status == "running",
                        RunORM.lease_expires_at < now,
                    )
                    .values(**values)
                    .returning(RunORM.run_id)
                )
                if update_result.scalar_one_or_none() is None:
                    continue

                if is_exhausted:
                    exhausted.append(run_id)
                    exhausted_threads_by_user.setdefault(user_id, set()).add(thread_id)
                    logger.error(
                        "Run exceeded max retries, marking as permanently failed",
                        run_id=run_id,
                        retries=retry_count,
                        max_retries=max_retries,
                    )
                else:
                    retryable.append(run_id)
                    logger.info(
                        "Incrementing retry count",
                        run_id=run_id,
                        retry_count=retry_count,
                        max_retries=max_retries,
                    )

            for user_id, thread_ids in exhausted_threads_by_user.items():
                await set_thread_status_if_no_active_runs(
                    session,
                    thread_ids,
                    "error",
                    user_id=user_id,
                )
            await session.commit()

        if retryable or exhausted:
            run_queue_signal.notify()

        return retryable, exhausted


lease_reaper = LeaseReaper()
