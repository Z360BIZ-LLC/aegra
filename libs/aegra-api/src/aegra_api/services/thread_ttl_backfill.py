"""Arm pre-existing threads for the TTL sweep.

A ``thread_ttl`` row is written by ``POST /threads`` and nowhere else, so every
thread created before TTL was configured is invisible to the sweeper — enabling
the feature on a live database reclaims nothing until those rows exist.

Deliberately manual and tranche-able rather than a startup backfill: arming N
threads makes N threads immediately due, and on a large database that is a
sweep the operator wants to meter themselves. Insert a tranche, watch the sweep
drain it, insert the next.

``expires_at`` is derived from ``thread.updated_at``, so a thread idle longer
than the TTL is due at once and a recently active one is not touched until it
has been quiet for a full interval.
"""

from __future__ import annotations

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.getLogger(__name__)

# Threads without a thread_ttl row, oldest first, capped. The LEFT JOIN (rather
# than relying on ON CONFLICT alone) is what makes --limit count *new* rows;
# ON CONFLICT stays as the guard against a concurrent POST /threads arming the
# same thread between the SELECT and the INSERT.
# Every parameter is cast explicitly: an untyped `$1 IS NULL` gives Postgres
# nothing to infer from and fails with AmbiguousParameterError. TTL is applied
# as seconds because make_interval's `mins` is an integer and would truncate a
# fractional TTL to zero.
_AGE_FILTER = """
  AND ((:older_than_days)::int IS NULL
       OR t.updated_at < now() - make_interval(days => (:older_than_days)::int))
"""

_BACKFILL_SQL = f"""
INSERT INTO thread_ttl (thread_id, strategy, ttl_minutes, created_at, expires_at)
SELECT t.thread_id,
       (:strategy)::text,
       (:ttl_minutes)::double precision,
       now(),
       t.updated_at + make_interval(secs => (:ttl_minutes)::double precision * 60)
FROM thread t
LEFT JOIN thread_ttl x ON x.thread_id = t.thread_id
WHERE x.thread_id IS NULL
{_AGE_FILTER}
ORDER BY t.updated_at ASC
LIMIT (:limit)::int
ON CONFLICT (thread_id) DO NOTHING
"""

_COUNT_SQL = f"""
SELECT count(*) FROM thread t
LEFT JOIN thread_ttl x ON x.thread_id = t.thread_id
WHERE x.thread_id IS NULL
{_AGE_FILTER}
"""


async def count_unarmed_threads(session: AsyncSession, *, older_than_days: int | None = None) -> int:
    """Threads that have no thread_ttl row and would be armed by a backfill."""
    result = await session.execute(text(_COUNT_SQL), {"older_than_days": older_than_days})
    return int(result.scalar_one())


async def backfill_thread_ttl(
    session: AsyncSession,
    *,
    strategy: str = "keep_latest",
    ttl_minutes: float = 43200,
    limit: int = 1000,
    older_than_days: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Arm up to ``limit`` unarmed threads. Returns counts, commits on success.

    Idempotent: a re-run only ever sees threads that still have no row, so it
    can be repeated until ``remaining`` reaches zero.
    """
    if strategy not in ("keep_latest", "delete"):
        raise ValueError(f"unknown strategy {strategy!r}")
    if ttl_minutes <= 0:
        raise ValueError(f"ttl_minutes must be greater than 0, got {ttl_minutes}")
    if limit <= 0:
        raise ValueError(f"limit must be greater than 0, got {limit}")

    candidates = await count_unarmed_threads(session, older_than_days=older_than_days)
    if dry_run:
        logger.info(
            "Thread TTL backfill dry run",
            candidates=candidates,
            would_arm=min(candidates, limit),
            strategy=strategy,
        )
        return {"armed": 0, "candidates": candidates, "remaining": candidates}

    result = await session.execute(
        text(_BACKFILL_SQL),
        {
            "strategy": strategy,
            "ttl_minutes": ttl_minutes,
            "limit": limit,
            "older_than_days": older_than_days,
        },
    )
    armed = result.rowcount or 0
    await session.commit()

    remaining = await count_unarmed_threads(session, older_than_days=older_than_days)
    logger.info(
        "Thread TTL backfill tranche complete",
        armed=armed,
        remaining=remaining,
        strategy=strategy,
        ttl_minutes=ttl_minutes,
    )
    return {"armed": armed, "candidates": candidates, "remaining": remaining}
