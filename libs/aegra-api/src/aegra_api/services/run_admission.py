"""Atomic thread and organization admission for stateful runs."""

import enum
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import structlog
from observability.cloudwatch_emf import emit_metric
from sqlalchemy import exists, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Run as RunORM
from aegra_api.services import run_limits
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)

THREAD_ADVISORY_LOCK_NAMESPACE = 8472

_PROMOTABLE_CANDIDATES_SQL = text(
    """
    WITH eligible AS (
        SELECT candidate.run_id,
               candidate.org_id,
               candidate.queue_position,
               row_number() OVER (
                   PARTITION BY candidate.org_id
                   ORDER BY candidate.queue_position
               ) AS org_rank
          FROM runs AS candidate
         WHERE candidate.status = 'pending'
           AND candidate.claimed_by IS NULL
           AND (
               candidate.pending_reason IS NOT NULL
               OR candidate.updated_at > candidate.created_at
               OR candidate.created_at < :stuck_before
           )
           AND (
               candidate.multitask_strategy <> 'enqueue'
               OR NOT EXISTS (
                   SELECT 1
                     FROM runs AS predecessor
                    WHERE predecessor.thread_id = candidate.thread_id
                      AND predecessor.queue_position < candidate.queue_position
                      AND predecessor.status IN ('pending', 'running')
               )
           )
    )
    SELECT run_id, org_id
      FROM eligible
     ORDER BY org_rank, queue_position
     LIMIT :scan_limit
    """
)


class AdmissionOutcome(enum.Enum):
    """Result of attempting to move a run from pending to running."""

    CLAIMED = "claimed"
    ALREADY_TAKEN = "already_taken"
    THREAD_BLOCKED = "thread_blocked"
    ORG_AT_CAPACITY = "org_at_capacity"


@dataclass(frozen=True, slots=True)
class _AdmissionState:
    thread_id: str
    org_id: str | None
    status: str
    claimed_by: str | None
    multitask_strategy: str
    queue_position: int
    pending_reason: str | None

    @property
    def claimable(self) -> bool:
        return self.status == "pending" and self.claimed_by is None


async def lock_thread(session: AsyncSession, thread_id: str) -> None:
    """Serialize admission and creation for one thread until transaction end."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:namespace, hashtext(:thread_id))"),
        {
            "namespace": THREAD_ADVISORY_LOCK_NAMESPACE,
            "thread_id": thread_id,
        },
    )


async def try_start_run(
    session: AsyncSession,
    run_id: str,
    *,
    claimed_by: str | None = None,
    lease_expires_at: datetime | None = None,
) -> AdmissionOutcome:
    """Claim a pending run after thread FIFO and org-capacity checks."""
    state = await _load_state(session, run_id)
    if state is None or not state.claimable:
        return AdmissionOutcome.ALREADY_TAKEN

    await lock_thread(session, state.thread_id)
    state = await _load_state(session, run_id)
    if state is None or not state.claimable:
        return AdmissionOutcome.ALREADY_TAKEN

    if state.multitask_strategy == "enqueue" and await _has_older_active_run(session, state):
        if not await _mark_blocked(session, run_id, state.pending_reason, "thread"):
            return AdmissionOutcome.ALREADY_TAKEN
        return AdmissionOutcome.THREAD_BLOCKED

    if settings.run_limits.enabled and state.org_id is not None:
        await run_limits.lock_org(session, state.org_id)
        decision = await run_limits.evaluate(session, state.org_id)
        if decision.at_capacity:
            if settings.run_limits.enforcing:
                if not await _mark_blocked(session, run_id, state.pending_reason, "org"):
                    return AdmissionOutcome.ALREADY_TAKEN
                logger.info(
                    "Run held at org concurrency limit",
                    run_id=run_id,
                    org_id=state.org_id,
                    active=decision.active,
                    limit=decision.limit,
                )
                emit_metric(
                    "OrgRunQueued",
                    1,
                    properties={
                        "run_id": run_id,
                        "org_id": state.org_id,
                        "active": decision.active,
                        "limit": decision.limit,
                    },
                )
                return AdmissionOutcome.ORG_AT_CAPACITY
            emit_metric(
                "OrgRunWouldQueue",
                1,
                properties={
                    "run_id": run_id,
                    "org_id": state.org_id,
                    "active": decision.active,
                    "limit": decision.limit,
                },
            )

    result = await session.execute(
        update(RunORM)
        .where(
            RunORM.run_id == run_id,
            RunORM.status == "pending",
            RunORM.claimed_by.is_(None),
        )
        .values(
            claimed_by=claimed_by,
            lease_expires_at=lease_expires_at,
            status="running",
            pending_reason=None,
            pending_reason_at=None,
            updated_at=datetime.now(UTC),
        )
    )
    if result.rowcount == 0:  # type: ignore[union-attr]
        return AdmissionOutcome.ALREADY_TAKEN
    return AdmissionOutcome.CLAIMED


async def find_promotable_runs(
    session: AsyncSession,
    *,
    batch_size: int,
) -> list[str]:
    """Find durable queue heads, with projected org capacity and fairness."""
    active = await run_limits.active_counts_by_org(session) if settings.run_limits.enforcing else {}
    scan_multiplier = max(10, run_limits.max_limit())
    candidates = await session.execute(
        _PROMOTABLE_CANDIDATES_SQL,
        {
            "stuck_before": datetime.now(UTC) - timedelta(seconds=settings.worker.STUCK_PENDING_THRESHOLD_SECONDS),
            "scan_limit": batch_size * scan_multiplier,
        },
    )

    promotable: list[str] = []
    projected = dict(active)
    for run_id, org_id in candidates.all():
        if len(promotable) >= batch_size:
            break
        if settings.run_limits.enforcing and org_id is not None:
            if projected.get(org_id, 0) >= run_limits.limit_for(org_id):
                continue
            projected[org_id] = projected.get(org_id, 0) + 1
        promotable.append(run_id)
    return promotable


async def _load_state(session: AsyncSession, run_id: str) -> _AdmissionState | None:
    result = await session.execute(
        select(
            RunORM.thread_id,
            RunORM.org_id,
            RunORM.status,
            RunORM.claimed_by,
            RunORM.multitask_strategy,
            RunORM.queue_position,
            RunORM.pending_reason,
        ).where(RunORM.run_id == run_id)
    )
    row = result.first()
    if row is None:
        return None
    return _AdmissionState(*row)


async def _has_older_active_run(
    session: AsyncSession,
    state: _AdmissionState,
) -> bool:
    stmt = select(
        exists().where(
            RunORM.thread_id == state.thread_id,
            RunORM.queue_position < state.queue_position,
            RunORM.status.in_(("pending", "running")),
        )
    )
    return bool(await session.scalar(stmt))


async def _mark_blocked(
    session: AsyncSession,
    run_id: str,
    current_reason: str | None,
    reason: str,
) -> bool:
    values: dict[str, object] = {
        "pending_reason": reason,
        "updated_at": datetime.now(UTC),
    }
    if current_reason != reason:
        values["pending_reason_at"] = datetime.now(UTC)

    result = await session.execute(
        update(RunORM)
        .where(
            RunORM.run_id == run_id,
            RunORM.status == "pending",
            RunORM.claimed_by.is_(None),
        )
        .values(**values)
    )
    return bool(result.rowcount)  # type: ignore[union-attr]
