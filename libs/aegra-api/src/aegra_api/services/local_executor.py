"""In-process executor using asyncio tasks.

Used in development mode (REDIS_BROKER_ENABLED=false). Runs execute
as background coroutines in the same event loop as the API server.
"""

import asyncio
import contextlib
import os
import socket
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import select

from aegra_api.core.active_runs import active_runs
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.run_job import RunJob
from aegra_api.observability.span_enrichment import make_run_trace_context
from aegra_api.services import run_limits
from aegra_api.services.base_executor import BaseExecutor

# Lease bookkeeping is identical for both executors; sharing it keeps a local
# run's liveness signal in the same shape the reaper and the run-limit count
# already understand.
from aegra_api.services.worker_executor import _heartbeat_loop, _release_lease
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Runs graphs as local asyncio tasks (single-instance dev mode)."""

    def __init__(self) -> None:
        self._owner = f"local-{socket.gethostname()}-{os.getpid()}"
        self._lease_tasks: set[asyncio.Task[None]] = set()

    async def submit(self, job: RunJob) -> None:
        # With limits off the run starts immediately and execute_run performs
        # the pending -> running transition, exactly as it always has.
        if not settings.run_limits.enforcing:
            self._spawn(job)
            return

        if not await self._claim(job.identity.run_id):
            logger.info(
                "Run left queued behind org run limit",
                run_id=job.identity.run_id,
            )
            return

        self._spawn(job)

    async def promote(self, run_id: str) -> None:
        """Start a queued run now that its org has a free slot."""
        job = await self._load_job(run_id)
        if job is None:
            return
        if not await self._claim(run_id):
            return
        self._spawn(job)

    def _spawn(self, job: RunJob) -> None:
        """Create the background task that executes the graph."""
        # Deferred import: run_executor imports services that reference
        # the executor singleton, creating a circular chain at module level.
        from aegra_api.services.run_executor import execute_run

        trace_ctx = make_run_trace_context(
            job.identity.run_id,
            job.identity.thread_id,
            job.identity.graph_id,
            job.user.identity,
            extra_metadata=job.run_metadata,
        )
        task = asyncio.create_task(execute_run(job), context=trace_ctx)
        active_runs[job.identity.run_id] = task
        if settings.run_limits.enforcing:
            keeper = asyncio.create_task(self._keep_lease(job.identity.run_id, task))
            self._lease_tasks.add(keeper)
            keeper.add_done_callback(self._lease_tasks.discard)
        logger.info(
            "Submitted run to local executor",
            run_id=job.identity.run_id,
            task_id=id(task),
        )

    async def _keep_lease(self, run_id: str, job_task: asyncio.Task[None]) -> None:
        """Hold the run's lease alive until it finishes, then release it.

        Without a heartbeat, a run killed by a restart stays ``running`` with
        no expiry — indistinguishable from a live run, so it would occupy its
        org's capacity forever and wedge the whole tenant.
        """
        heartbeat = asyncio.create_task(_heartbeat_loop(run_id, self._owner, job_task=job_task))
        try:
            await asyncio.wait({job_task})
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            await _release_lease(run_id, self._owner)

    async def _claim(self, run_id: str) -> bool:
        """Reserve a capacity slot for this run, committing the transition."""
        lease_until = datetime.now(UTC) + timedelta(seconds=settings.worker.LEASE_DURATION_SECONDS)
        maker = _get_session_maker()
        async with maker() as session:
            outcome = await run_limits.try_start_run(
                session,
                run_id,
                claimed_by=self._owner,
                lease_expires_at=lease_until,
            )
            if outcome is not run_limits.ClaimOutcome.CLAIMED:
                await session.rollback()
                return False
            await session.commit()
            return True

    @staticmethod
    async def _load_job(run_id: str) -> RunJob | None:
        """Rebuild a RunJob from its persisted execution params."""
        maker = _get_session_maker()
        async with maker() as session:
            run_orm = await session.scalar(select(RunORM).where(RunORM.run_id == run_id))
            if run_orm is None or run_orm.execution_params is None:
                logger.warning("Cannot promote run without execution_params", run_id=run_id)
                return None
            return RunJob.from_run_orm(run_orm)

    async def wait_for_completion(self, run_id: str, *, timeout: float = 300.0) -> None:
        task = active_runs.get(run_id)
        if task is None:
            return
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(task), timeout=timeout)

    async def start(self) -> None:
        logger.info("Local executor started (in-process asyncio tasks)")

    async def stop(self) -> None:
        tasks_to_cancel = [task for task in active_runs.values() if not task.done()]
        for task in tasks_to_cancel:
            task.cancel()
        if tasks_to_cancel:
            logger.info("Draining cancelled tasks", count=len(tasks_to_cancel))
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        logger.info("Local executor stopped")
