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

from aegra_api.core.active_runs import active_runs
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.run_job import RunJob
from aegra_api.observability.span_enrichment import make_run_trace_context
from aegra_api.services.base_executor import BaseExecutor
from aegra_api.services.run_admission import AdmissionOutcome, try_start_run
from aegra_api.services.run_executor import _shutdown_cancellations
from aegra_api.services.run_queue_signal import run_queue_signal

# Lease bookkeeping is identical for both executors; sharing it keeps a local
# run's liveness signal in the same shape the reaper and the run-limit count
# already understand.
from aegra_api.services.worker_executor import (
    _acquire_and_load,
    _heartbeat_loop,
    _is_run_terminal,
    _release_lease,
    _reset_drained_runs,
)
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


class LocalExecutor(BaseExecutor):
    """Runs graphs as local asyncio tasks (single-instance dev mode)."""

    def __init__(self) -> None:
        self._owner = f"local-{socket.gethostname()}-{os.getpid()}"
        self._lease_tasks: set[asyncio.Task[None]] = set()
        self._job_tasks: dict[str, asyncio.Task[None]] = {}

    async def submit(self, job: RunJob) -> None:
        outcome = await self._claim(job.identity.run_id)
        if outcome is not AdmissionOutcome.CLAIMED:
            logger.info(
                "Run not admitted by local executor",
                run_id=job.identity.run_id,
                outcome=outcome.value,
            )
            return

        self._spawn(job)

    async def promote(self, run_id: str) -> None:
        """Start a queued run now that its org has a free slot."""
        loaded = await _acquire_and_load(run_id, self._owner)
        if loaded is None:
            return
        self._spawn(loaded.job)

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
        self._job_tasks[job.identity.run_id] = task
        task.add_done_callback(lambda completed, run_id=job.identity.run_id: self._job_tasks.pop(run_id, None))
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

    async def _claim(self, run_id: str) -> AdmissionOutcome:
        """Reserve a capacity slot for this run, committing the transition."""
        lease_until = datetime.now(UTC) + timedelta(seconds=settings.worker.LEASE_DURATION_SECONDS)
        maker = _get_session_maker()
        async with maker() as session:
            outcome = await try_start_run(
                session,
                run_id,
                claimed_by=self._owner,
                lease_expires_at=lease_until,
            )
            if outcome is AdmissionOutcome.ALREADY_TAKEN:
                await session.rollback()
                return outcome
            await session.commit()
            return outcome

    async def wait_for_completion(self, run_id: str, *, timeout: float = 300.0) -> None:
        with contextlib.suppress(TimeoutError, asyncio.CancelledError):
            async with asyncio.timeout(timeout):
                while True:
                    task = active_runs.get(run_id)
                    if task is not None:
                        await asyncio.shield(task)
                        return
                    if await _is_run_terminal(run_id):
                        return
                    await asyncio.sleep(0.1)

    async def start(self) -> None:
        logger.info("Local executor started (in-process asyncio tasks)")

    async def stop(self) -> None:
        drained = [run_id for run_id, task in self._job_tasks.items() if not task.done()]
        if drained:
            _shutdown_cancellations.update(drained)
            logger.info("Handing off local runs at shutdown", count=len(drained))
            for run_id in drained:
                self._job_tasks[run_id].cancel()
            await asyncio.gather(
                *(self._job_tasks[run_id] for run_id in drained),
                return_exceptions=True,
            )

        if self._lease_tasks:
            await asyncio.gather(*self._lease_tasks, return_exceptions=True)

        if drained:
            reset_ids = await _reset_drained_runs(drained)
            if reset_ids:
                # Local mode has no Redis transport. PostgreSQL is the queue of
                # record and the promoter immediately rediscovers these rows.
                run_queue_signal.notify()
            _shutdown_cancellations.difference_update(drained)
        self._job_tasks.clear()
        self._lease_tasks.clear()
        logger.info("Local executor stopped")
