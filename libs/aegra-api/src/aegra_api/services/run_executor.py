"""Graph run execution logic.

Single source of truth for executing a graph run. Both LocalExecutor
(asyncio.create_task) and WorkerExecutor (Redis BLPOP) call
`execute_run`. All database and broker interactions are delegated to
`run_status` and `streaming_service` respectively.
"""

import asyncio
import json
from time import perf_counter
from typing import Any

import structlog
from observability.cloudwatch_emf import emit_metric

from aegra_api.core.active_runs import active_runs
from aegra_api.core.auth_ctx import with_auth_ctx
from aegra_api.core.redis_manager import redis_manager
from aegra_api.models.run_job import RunJob
from aegra_api.services.broker import broker_manager
from aegra_api.services.graph_streaming import stream_graph_events
from aegra_api.services.langgraph_service import (
    create_run_config,
    create_thread_config,
    get_langgraph_service,
)
from aegra_api.services.run_status import finalize_run, update_run_status
from aegra_api.services.streaming_service import streaming_service
from aegra_api.services.thread_state_service import ThreadStateService
from aegra_api.services.webhook_service import send_run_webhook
from aegra_api.settings import settings
from aegra_api.utils.run_utils import map_command_to_langgraph

logger = structlog.getLogger(__name__)

_DEFAULT_STREAM_MODES = ["values"]
_thread_state_service = ThreadStateService()
# Skip materialization for pathologically large states — the bytes already live
# in checkpoints; bloating the thread row would slow the search read path too.
_MAX_MATERIALIZED_STATE_BYTES = 4_000_000

# Run IDs whose cancellation was triggered by lease loss (not user action).
# When a heartbeat detects lease loss, it adds the run_id here before
# cancelling the job task. execute_run's CancelledError handler checks
# this set to skip finalize_run and SSE signaling — the reaper has already
# re-enqueued the run and another worker will execute it. Without this,
# the old worker would write status="interrupted" and send an SSE end event,
# prematurely closing client streams and potentially overwriting the new
# worker's status.
_lease_loss_cancellations: set[str] = set()


async def execute_run(job: RunJob) -> None:
    """Execute a graph run, stream events to the broker, and update DB.

    Handles the full lifecycle: status transitions, event streaming,
    interrupt detection, cancellation, and error signaling.
    """
    run_id = job.identity.run_id
    thread_id = job.identity.thread_id
    is_lease_loss = False
    # Bound for any except handler that fires before the post-status-update
    # rebind below. If update_run_status itself raises, RunDurationMs ends up
    # ~0 — accurate, since the graph never started.
    started_at = perf_counter()

    try:
        await update_run_status(run_id, "running")
        # Rebind so RunDurationMs measures only graph execution, not the
        # preceding status-update DB write.
        started_at = perf_counter()
        _emit_run_started_metric(job)

        final_output = await _stream_graph(job)

        if final_output.has_interrupt:
            await finalize_run(
                run_id,
                thread_id,
                status="interrupted",
                thread_status="interrupted",
                output=final_output.data,
                persist_state=final_output.materialized,
                materialized_values=final_output.state_values,
                materialized_interrupts=final_output.state_interrupts,
            )
            _emit_run_terminal_metrics(job, "interrupted", started_at)
            await _send_run_webhook(job, "interrupted", final_output.data)
        else:
            await finalize_run(
                run_id,
                thread_id,
                status="success",
                thread_status="idle",
                output=final_output.data,
                persist_state=final_output.materialized,
                materialized_values=final_output.state_values,
                materialized_interrupts=final_output.state_interrupts,
            )
            _emit_run_terminal_metrics(job, "success", started_at)
            await _send_run_webhook(job, "completed", final_output.data)

    except asyncio.CancelledError:
        if run_id in _lease_loss_cancellations:
            # Lease was lost — the reaper re-enqueued this run for another
            # worker.  Do NOT finalize, signal done, or clean up the broker.
            # The new worker owns the run now.
            is_lease_loss = True
            logger.info("Lease-loss cancel, skipping finalize", run_id=run_id)
            emit_metric("LeaseLossCancellation", 1, properties=_run_metric_properties(job))
        else:
            await finalize_run(run_id, thread_id, status="interrupted", thread_status="idle", output={})
            _emit_run_terminal_metrics(job, "interrupted", started_at, terminal_reason="cancelled")
            await _send_run_webhook(job, "cancelled", {})
            await _best_effort_signal(streaming_service.signal_run_cancelled, run_id)
        raise
    except Exception as exc:
        logger.exception("Run failed", run_id=run_id)
        safe_message = f"{type(exc).__name__}: execution failed"
        await finalize_run(run_id, thread_id, status="error", thread_status="error", output={}, error=str(exc))
        _emit_run_terminal_metrics(job, "error", started_at, error_class=type(exc).__name__)
        await _send_run_webhook(job, "failed", {}, error_message=str(exc))
        await _best_effort_signal(streaming_service.signal_run_error, run_id, safe_message, type(exc).__name__)
    else:
        status = "interrupted" if final_output.has_interrupt else "success"
        await _best_effort_signal(_signal_end_event, run_id, status)
    finally:
        _lease_loss_cancellations.discard(run_id)
        active_runs.pop(run_id, None)
        if not is_lease_loss:
            await streaming_service.cleanup_run(run_id)
            await _signal_run_done(run_id)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _emit_run_started_metric(job: RunJob) -> None:
    """Emit a count when a worker or local executor starts processing a run."""
    emit_metric("RunsStarted", 1, properties=_run_metric_properties(job))


def _emit_run_terminal_metrics(
    job: RunJob,
    status: str,
    started_at: float,
    *,
    terminal_reason: str | None = None,
    error_class: str | None = None,
) -> None:
    """Emit terminal count and duration metrics after DB status is committed."""
    properties = _run_metric_properties(job)
    properties["terminal_status"] = status
    if terminal_reason is not None:
        properties["terminal_reason"] = terminal_reason
    if error_class is not None:
        properties["error_class"] = error_class

    emit_metric("RunsTerminal", 1, dimensions={"Status": status}, properties=properties)
    emit_metric(
        "RunDurationMs",
        max(0.0, (perf_counter() - started_at) * 1000),
        unit="Milliseconds",
        dimensions={"Status": status},
        properties=properties,
    )


def _run_metric_properties(job: RunJob) -> dict[str, Any]:
    """Return high-cardinality run context as log properties, not dimensions."""
    return {
        "run_id": job.identity.run_id,
        "thread_id": job.identity.thread_id,
        "user_id": job.user.identity,
        "graph_id": job.identity.graph_id,
    }


async def _best_effort_signal(fn: Any, *args: Any) -> None:
    """Call a signaling function, logging but not raising on failure.

    Signaling (end events, cancel/error notifications) must not override
    an already-committed DB status if it fails.
    """
    try:
        await fn(*args)
    except Exception:
        logger.warning("Signal failed (best-effort, DB status already committed)", fn=fn.__name__)


async def _send_run_webhook(
    job: RunJob,
    status: str,
    output: dict[str, Any],
    error_message: str | None = None,
) -> None:
    """Send a completion webhook when the caller supplied a webhook URL."""
    await send_run_webhook(
        webhook_url=job.behavior.webhook_url,
        run_id=job.identity.run_id,
        thread_id=job.identity.thread_id,
        status=status,
        output=output,
        error_message=error_message,
    )


class _GraphResult:
    """Accumulates output and interrupt state during graph streaming."""

    __slots__ = ("data", "has_interrupt", "materialized", "state_values", "state_interrupts")

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.has_interrupt: bool = False
        self.materialized: bool = False
        self.state_values: dict[str, Any] = {}
        self.state_interrupts: dict[str, Any] = {}


async def _stream_graph(job: RunJob) -> _GraphResult:
    """Load the graph, stream events to the broker, return final output."""
    run_id = job.identity.run_id
    run_config = _build_run_config(job)
    execution_input = _resolve_input(job)
    stream_modes = _resolve_stream_modes(job.execution.stream_mode)

    langgraph_service = get_langgraph_service()
    result = _GraphResult()

    async with (
        langgraph_service.get_graph(
            job.identity.graph_id,
            config=run_config,
            access_context="threads.create_run",
            user=job.user,
            context=job.execution.context,
        ) as graph,
        with_auth_ctx(job.user, job.user.permissions),  # type: ignore[arg-type]
    ):
        async for event_type, event_data in stream_graph_events(
            graph=graph,
            input_data=execution_input,
            config=run_config,
            stream_mode=stream_modes,
            context=job.execution.context,
            subgraphs=job.behavior.subgraphs,
            on_checkpoint=lambda _: None,
            on_task_result=lambda _: None,
        ):
            event_id = await broker_manager.allocate_event_id(run_id)
            await streaming_service.put_to_broker(run_id, event_id, (event_type, event_data))

            if isinstance(event_data, dict) and "__interrupt__" in event_data:
                result.has_interrupt = True
            if event_type.startswith("values"):
                result.data = event_data

        # Still inside the graph context (checkpointer attached): capture the
        # graph's logical state to materialize onto the thread row.
        await _materialize_thread_state(graph, job, result)

    return result


async def _materialize_thread_state(graph: Any, job: RunJob, result: _GraphResult) -> None:
    """Best-effort: read aget_state and stash JSON-safe values/interrupts on
    ``result`` for persistence. Never fails the run — a materialization error
    just leaves the thread's previous state untouched."""
    try:
        config = create_thread_config(job.identity.thread_id, job.user)
        snapshot = await graph.with_config(config).aget_state(config)
        values, interrupts = _thread_state_service.materialize_state(snapshot)
        if len(json.dumps(values, default=str)) > _MAX_MATERIALIZED_STATE_BYTES:
            logger.warning("Skipping oversized thread state materialization", thread_id=job.identity.thread_id)
            return
        result.state_values = values
        result.state_interrupts = interrupts
        result.materialized = True
    except Exception:
        logger.warning(
            "Thread state materialization failed (best-effort)",
            run_id=job.identity.run_id,
            thread_id=job.identity.thread_id,
        )


def _build_run_config(job: RunJob) -> dict[str, Any]:
    """Assemble the LangGraph run config from a RunJob."""
    config = create_run_config(
        job.identity.run_id,
        job.identity.thread_id,
        job.user,
        additional_config=job.execution.config,
        checkpoint=job.execution.checkpoint,
    )
    if job.behavior.interrupt_before is not None:
        items = job.behavior.interrupt_before
        config["interrupt_before"] = items if isinstance(items, list) else [items]
    if job.behavior.interrupt_after is not None:
        items = job.behavior.interrupt_after
        config["interrupt_after"] = items if isinstance(items, list) else [items]
    return config


def _resolve_input(job: RunJob) -> Any:
    """Return graph input — either raw data or a LangGraph Command."""
    if job.execution.command is not None:
        return map_command_to_langgraph(job.execution.command)
    return job.execution.input_data


def _resolve_stream_modes(stream_mode: str | list[str] | None) -> list[str]:
    """Normalize stream_mode to a list."""
    if stream_mode is None:
        return _DEFAULT_STREAM_MODES.copy()
    if isinstance(stream_mode, str):
        return [stream_mode]
    return list(stream_mode)


# ------------------------------------------------------------------
# End event (tells SSE consumers the stream is finished)
# ------------------------------------------------------------------


async def _signal_end_event(run_id: str, status: str) -> None:
    """Publish an 'end' event to the broker so SSE consumers close cleanly.

    Without this, the SSE connection hangs after the last data event
    because the broker never signals completion on success/interrupt.
    Error and cancel paths already send end events via signal_run_error
    and signal_run_cancelled respectively.
    """
    broker = broker_manager.get_broker(run_id)
    if broker is None or broker.is_finished():
        return

    event_id = await broker_manager.allocate_event_id(run_id)
    await broker.put(event_id, ("end", {"status": status}))


# ------------------------------------------------------------------
# Done signal (Redis key for fast completion polling)
# ------------------------------------------------------------------

_DONE_KEY_TTL_SECONDS = 3600


async def _signal_run_done(run_id: str) -> None:
    """Set a Redis key indicating the run has finished.

    WorkerExecutor.wait_for_completion polls this key instead of
    subscribing to the broker's event channel. Simpler, no subscription
    race, no message parsing. Falls back silently if Redis unavailable.
    """
    try:
        client = redis_manager.get_client()
        done_key = f"{settings.redis.REDIS_CHANNEL_PREFIX}done:{run_id}"
        await client.set(done_key, "1", ex=_DONE_KEY_TTL_SECONDS)
    except Exception:
        logger.debug("Redis done-key set failed (non-critical)", run_id=run_id)
