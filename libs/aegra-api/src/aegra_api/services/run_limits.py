"""Per-organization concurrent run limits.

One tenant may execute at most N runs at a time across every graph. Runs over
that ceiling are never rejected — they stay ``pending`` (the runs table is
already the queue of record) until a slot frees and the promoter dispatches
them.

The gate lives on the ``pending -> running`` transition, which is the only
place a run starts consuming capacity. Both executors route their claim
through :func:`try_start_run`, so the policy holds in Redis worker mode and
in-process dev mode alike.

Counting is derived from the runs table rather than an incrementing counter:
a crashed worker's lease expires and the reaper resets its row, so a derived
count self-heals where a counter would leak a slot permanently.

Dependency-light on purpose (orm + settings + metrics only) so it can be
imported from executors and API code without an import cycle.
"""

import enum
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from observability.cloudwatch_emf import emit_metric
from sqlalchemy import func, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Run as RunORM
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)

# Two-argument advisory locks occupy a namespace of their own, so this can
# never collide with the single-argument lock state_backfill takes.
_ADVISORY_LOCK_NAMESPACE = 8471


class ClaimOutcome(enum.Enum):
    """Result of attempting to move a run from ``pending`` to ``running``."""

    CLAIMED = "claimed"
    # Another worker got there first, or the run is gone / no longer pending.
    ALREADY_TAKEN = "already_taken"
    AT_CAPACITY = "at_capacity"


@dataclass(frozen=True, slots=True)
class LimitDecision:
    """Snapshot of one org's capacity at decision time."""

    org_id: str
    active: int
    limit: int

    @property
    def at_capacity(self) -> bool:
        """True when the org has no free slot."""
        return self.active >= self.limit


def resolve_org_id(config: Mapping[str, Any] | None, run_metadata: Mapping[str, Any] | None = None) -> str | None:
    """Extract the tenant key from a run's config, falling back to metadata.

    Returns None when no tenant can be determined; such runs are exempt from
    limits rather than sharing one anonymous bucket, which would let a legacy
    caller starve the quota for everyone.
    """
    key = settings.run_limits.ORG_ID_CONFIG_KEY

    configurable = (config or {}).get("configurable")
    if isinstance(configurable, Mapping):
        resolved = _coerce_org_id(configurable.get(key))
        if resolved is not None:
            return resolved

    return _coerce_org_id((run_metadata or {}).get(key))


def _coerce_org_id(value: Any) -> str | None:
    """Normalize an org identifier to a non-empty string, or None."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def limit_for(org_id: str) -> int:
    """Return the concurrent-run ceiling for one org."""
    return settings.run_limits.limit_overrides.get(org_id, settings.run_limits.ORG_MAX_CONCURRENT_RUNS)


def max_limit() -> int:
    """Return the largest ceiling any org can have.

    Used to bound the promoter's candidate scan: no org can ever need more
    candidates than its own ceiling.
    """
    overrides = settings.run_limits.limit_overrides
    if not overrides:
        return settings.run_limits.ORG_MAX_CONCURRENT_RUNS
    return max(settings.run_limits.ORG_MAX_CONCURRENT_RUNS, *overrides.values())


def _stale_run_cutoff() -> datetime:
    """Runs created before this can no longer legitimately be executing.

    Anything older is a wedged row; excluding it stops one stuck run from
    permanently holding a slot the reaper hasn't gotten to yet.
    """
    return datetime.now(UTC) - timedelta(seconds=settings.worker.BG_JOB_TIMEOUT_SECS)


async def lock_org(session: AsyncSession, org_id: str) -> None:
    """Take a transaction-scoped advisory lock for one org.

    Without it the capacity count is a phantom read: two workers can both
    observe ``limit - 1`` active runs and both start one. The lock releases on
    commit or rollback, and serializes only run *admission* for a single org.
    """
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:namespace, hashtext(:org_id))"),
        {"namespace": _ADVISORY_LOCK_NAMESPACE, "org_id": org_id},
    )


async def count_active_runs(session: AsyncSession, org_id: str) -> int:
    """Count runs currently occupying one org's capacity.

    Only ``running`` counts: ``pending`` is the queue, so counting it would
    deadlock an org against its own backlog. Rows whose worker lease has
    expired are excluded — the worker is gone and the reaper will reset them.
    """
    now = datetime.now(UTC)
    stmt = (
        select(func.count())
        .select_from(RunORM)
        .where(
            RunORM.org_id == org_id,
            RunORM.status == "running",
            or_(RunORM.lease_expires_at.is_(None), RunORM.lease_expires_at > now),
            RunORM.created_at > _stale_run_cutoff(),
        )
    )
    return await session.scalar(stmt) or 0


async def evaluate(session: AsyncSession, org_id: str) -> LimitDecision:
    """Measure one org's capacity. Caller must already hold the org lock."""
    active = await count_active_runs(session, org_id)
    return LimitDecision(org_id=org_id, active=active, limit=limit_for(org_id))


async def try_start_run(
    session: AsyncSession,
    run_id: str,
    *,
    claimed_by: str | None = None,
    lease_expires_at: datetime | None = None,
) -> ClaimOutcome:
    """Atomically move a run from ``pending`` to ``running`` if capacity allows.

    Does NOT commit — the caller owns the transaction boundary, and the org
    advisory lock must stay held until it closes.

    ``AT_CAPACITY`` means the run is still valid and still queued; the caller
    should leave it ``pending`` for the promoter rather than re-enqueueing it.

    With limits off this is exactly the single atomic UPDATE it has always
    been — the gate adds no round-trip to the default path.
    """
    if settings.run_limits.enabled:
        gated = await _apply_capacity_gate(session, run_id)
        if gated is not None:
            return gated

    result = await session.execute(
        update(RunORM)
        .where(RunORM.run_id == run_id, RunORM.status == "pending", RunORM.claimed_by.is_(None))
        .values(claimed_by=claimed_by, lease_expires_at=lease_expires_at, status="running")
    )
    if result.rowcount == 0:  # type: ignore[union-attr]
        return ClaimOutcome.ALREADY_TAKEN
    return ClaimOutcome.CLAIMED


async def _apply_capacity_gate(session: AsyncSession, run_id: str) -> ClaimOutcome | None:
    """Return a terminal outcome when the claim must not proceed.

    None means "carry on and attempt the claim". In shadow mode a full org
    still proceeds, but emits the metric that would have gated it.
    """
    row = (
        await session.execute(select(RunORM.org_id, RunORM.status, RunORM.claimed_by).where(RunORM.run_id == run_id))
    ).first()
    if row is None:
        return ClaimOutcome.ALREADY_TAKEN

    org_id, status, existing_claim = row
    if status != "pending" or existing_claim is not None:
        return ClaimOutcome.ALREADY_TAKEN
    if org_id is None:
        return None

    await lock_org(session, org_id)
    decision = await evaluate(session, org_id)
    if not decision.at_capacity:
        return None

    if not settings.run_limits.enforcing:
        emit_metric(
            "OrgRunWouldQueue",
            1,
            properties={
                "run_id": run_id,
                "org_id": org_id,
                "active": decision.active,
                "limit": decision.limit,
            },
        )
        return None

    logger.info(
        "Run held at org concurrency limit",
        run_id=run_id,
        org_id=org_id,
        active=decision.active,
        limit=decision.limit,
    )
    emit_metric(
        "OrgRunQueued",
        1,
        properties={
            "run_id": run_id,
            "org_id": org_id,
            "active": decision.active,
            "limit": decision.limit,
        },
    )
    return ClaimOutcome.AT_CAPACITY


async def active_counts_by_org(session: AsyncSession) -> dict[str, int]:
    """Return per-org active run counts in one pass, for the promoter."""
    now = datetime.now(UTC)
    stmt = (
        select(RunORM.org_id, func.count())
        .where(
            RunORM.org_id.isnot(None),
            RunORM.status == "running",
            or_(RunORM.lease_expires_at.is_(None), RunORM.lease_expires_at > now),
            RunORM.created_at > _stale_run_cutoff(),
        )
        .group_by(RunORM.org_id)
    )
    result = await session.execute(stmt)
    return {org_id: count for org_id, count in result.all() if org_id is not None}


# Ordering by rank-then-age hands every org its oldest queued run before any
# org gets a second, so a large backlog cannot crowd the scan window.
_PROMOTABLE_CANDIDATES_SQL = text(
    """
    SELECT run_id, org_id
      FROM (
           SELECT run_id,
                  org_id,
                  created_at,
                  row_number() OVER (PARTITION BY org_id ORDER BY created_at) AS rank
             FROM runs
            WHERE status = 'pending'
              AND claimed_by IS NULL
              AND org_id IS NOT NULL
           ) ranked
     WHERE rank <= :max_per_org
     ORDER BY rank, created_at
     LIMIT :scan_limit
    """
)


# Runs with no tenant are exempt from limits, so they never appear above — but
# the promoter replaces the reaper's stuck-pending sweep while enforcing, and
# without this they would have no recovery path at all if their queue entry is
# lost (Redis restart, or a crash between the run's commit and its enqueue).
# Age-gated so freshly enqueued runs aren't pushed twice.
_STUCK_UNSCOPED_CANDIDATES_SQL = text(
    """
    SELECT run_id
      FROM runs
     WHERE status = 'pending'
       AND claimed_by IS NULL
       AND org_id IS NULL
       AND created_at < :stuck_before
     ORDER BY created_at
     LIMIT :scan_limit
    """
)


async def find_promotable_runs(session: AsyncSession, *, batch_size: int) -> list[str]:
    """Return queued run_ids that are safe to dispatch, fairest-first.

    Purely advisory: the promoter re-enqueues these, and the claim re-checks
    capacity under the org lock, so an over-selection here is harmless.
    """
    active = await active_counts_by_org(session)
    candidates = await session.execute(
        _PROMOTABLE_CANDIDATES_SQL,
        {"max_per_org": max_limit(), "scan_limit": batch_size * 10},
    )

    promotable: list[str] = []
    projected = dict(active)
    for run_id, org_id in candidates.all():
        if len(promotable) >= batch_size:
            break
        if projected.get(org_id, 0) >= limit_for(org_id):
            continue
        projected[org_id] = projected.get(org_id, 0) + 1
        promotable.append(run_id)

    remaining = batch_size - len(promotable)
    if remaining > 0:
        promotable.extend(await _find_stuck_unscoped_runs(session, batch_size=remaining))

    return promotable


async def _find_stuck_unscoped_runs(session: AsyncSession, *, batch_size: int) -> list[str]:
    """Return long-pending runs that carry no tenant and so are never gated."""
    stuck_before = datetime.now(UTC) - timedelta(seconds=settings.worker.STUCK_PENDING_THRESHOLD_SECONDS)
    result = await session.execute(
        _STUCK_UNSCOPED_CANDIDATES_SQL,
        {"stuck_before": stuck_before, "scan_limit": batch_size},
    )
    return [row[0] for row in result.all()]


async def find_expired_queued_runs(session: AsyncSession, *, batch_size: int) -> list[RunORM]:
    """Return runs that have waited in the queue past the maximum wait.

    A run that starts hours late would answer a conversation that has long
    since moved on, so these are failed rather than eventually executed.
    """
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.run_limits.ORG_RUN_MAX_QUEUE_WAIT_SECONDS)
    stmt = (
        select(RunORM)
        .where(
            RunORM.status == "pending",
            RunORM.claimed_by.is_(None),
            RunORM.created_at < cutoff,
        )
        .order_by(RunORM.created_at.asc())
        .limit(batch_size)
    )
    result = await session.scalars(stmt)
    return list(result.all())
