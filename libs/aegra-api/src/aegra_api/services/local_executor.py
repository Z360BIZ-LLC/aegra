"""In-process executor using asyncio tasks.

Used in development mode (REDIS_BROKER_ENABLED=false). Runs execute
as background coroutines in the same event loop as the API server.
"""

import asyncio
import contextlib

import structlog
from sqlalchemy import select

from aegra_api.core.active_runs import active_runs
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.run_job import RunJob
from aegra_api.observability.span_enrichment import make_run_trace_context
from aegra_api.services import run_limits
from aegra_api.services.base_executor import BaseExecutor
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Runs graphs as local asyncio tasks (single-instance dev mode)."""

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
        logger.info(
            "Submitted run to local executor",
            run_id=job.identity.run_id,
            task_id=id(task),
        )

    @staticmethod
    async def _claim(run_id: str) -> bool:
        """Reserve a capacity slot for this run, committing the transition."""
        maker = _get_session_maker()
        async with maker() as session:
            outcome = await run_limits.try_start_run(session, run_id)
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
