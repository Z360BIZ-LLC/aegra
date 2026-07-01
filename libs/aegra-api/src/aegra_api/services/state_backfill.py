"""Backfill of materialized thread state (values / interrupts).

Threads created before state materialization have NULL ``thread.values``. This
walks threads that have a graph, computes ``aget_state`` (the graph's logical
state — correct even for middleware agents), and persists it so /threads/search
can project it without waiting for each thread's next run.

Two entry points:
- ``backfill_thread_state`` — the worker, used by the ``aegra db
  backfill-thread-state`` CLI command.
- ``run_startup_backfill`` — a one-time, single-runner startup pass (advisory
  lock + completion marker), so a deploy backfills automatically and never
  re-runs. Both are best-effort per thread: a failure is logged and skipped.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import select, update

from aegra_api.core.database import db_manager
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.auth import User
from aegra_api.services.langgraph_service import create_thread_config, get_langgraph_service
from aegra_api.services.thread_state_service import ThreadStateService

logger = structlog.getLogger(__name__)
_thread_state_service = ThreadStateService()

# Arbitrary constant key for pg_try_advisory_lock — one runner per database.
_BACKFILL_ADVISORY_LOCK_KEY = 4772393
# Completion marker (in the LangGraph store) so the startup pass runs at most once.
_BACKFILL_MARKER_NS: tuple[str, ...] = ("__system__",)
_BACKFILL_MARKER_KEY = "thread_state_backfill_v1"


async def backfill_thread_state(
    *, limit: int | None = None, only_missing: bool = True, batch_size: int = 50, sleep_s: float = 1.0
) -> dict[str, int]:
    """Materialize thread state for existing threads.

    Args:
        limit: cap the number of threads processed (None = all).
        only_missing: skip threads that already have materialized values.
        batch_size: threads per batch before pausing (rate-limit; <=0 disables).
        sleep_s: seconds to sleep between batches.
    """
    maker = _get_session_maker()
    async with maker() as session:
        stmt = select(ThreadORM.thread_id, ThreadORM.user_id, ThreadORM.metadata_json)
        if only_missing:
            stmt = stmt.where(ThreadORM.values_json.is_(None))
        if limit:
            stmt = stmt.limit(limit)
        rows = (await session.execute(stmt)).all()

    stats = {"total": len(rows), "materialized": 0, "skipped_no_graph": 0, "failed": 0}
    langgraph_service = get_langgraph_service()

    for index, (thread_id, user_id, metadata) in enumerate(rows):
        graph_id = (metadata or {}).get("graph_id")
        if not graph_id:
            stats["skipped_no_graph"] += 1
            continue
        try:
            await _materialize_one(langgraph_service, thread_id, user_id, graph_id, maker)
            stats["materialized"] += 1
        except Exception:
            logger.warning("Backfill failed for thread", thread_id=thread_id, graph_id=graph_id)
            stats["failed"] += 1
        # Rate-limit: pause between batches so a large backfill can't stampede the DB.
        if batch_size > 0 and (index + 1) % batch_size == 0 and index + 1 < len(rows):
            await asyncio.sleep(sleep_s)

    logger.info("Thread state backfill complete", **stats)
    return stats


async def run_startup_backfill(*, batch_size: int = 50, sleep_s: float = 1.0) -> None:
    """One-time backfill on startup. Best-effort — never blocks/crashes startup.

    An advisory lock ensures a single runner across pods; a completion marker in
    the store ensures it never runs again once finished (future deploys just do a
    quick marker check and skip).
    """
    pool = db_manager.lg_pool
    if pool is None:
        return
    try:
        async with pool.connection() as lock_conn:
            if not await _try_advisory_lock(lock_conn, _BACKFILL_ADVISORY_LOCK_KEY):
                logger.info("Startup thread state backfill: lock held elsewhere, skipping")
                return
            # Session-level locks persist on a pooled connection until explicitly
            # released — unlock in finally so a later boot can acquire it.
            try:
                if await _marker_exists():
                    logger.info("Startup thread state backfill: already completed, skipping")
                    return
                logger.info("Startup thread state backfill: starting one-time pass")
                stats = await backfill_thread_state(only_missing=True, batch_size=batch_size, sleep_s=sleep_s)
                await _set_marker(stats)
            finally:
                await _advisory_unlock(lock_conn, _BACKFILL_ADVISORY_LOCK_KEY)
    except Exception:
        logger.warning("Startup thread state backfill failed (best-effort)", exc_info=True)


async def _try_advisory_lock(conn: Any, key: int) -> bool:
    async with conn.cursor() as cur:
        await cur.execute("SELECT pg_try_advisory_lock(%s) AS locked", (key,))
        row = await cur.fetchone()
    return bool(row and row["locked"])


async def _advisory_unlock(conn: Any, key: int) -> None:
    async with conn.cursor() as cur:
        await cur.execute("SELECT pg_advisory_unlock(%s)", (key,))


async def _marker_exists() -> bool:
    item = await db_manager.get_store().aget(_BACKFILL_MARKER_NS, _BACKFILL_MARKER_KEY)
    return item is not None


async def _set_marker(stats: dict[str, int]) -> None:
    marker = {"completed_at": datetime.now(UTC).isoformat(), **stats}
    await db_manager.get_store().aput(_BACKFILL_MARKER_NS, _BACKFILL_MARKER_KEY, marker)


async def _materialize_one(langgraph_service: Any, thread_id: str, user_id: str, graph_id: str, maker: Any) -> None:
    user = User(identity=user_id)
    config = create_thread_config(thread_id, user)
    async with langgraph_service.get_graph(graph_id, config=config, access_context="threads.read", user=user) as agent:
        snapshot = await agent.with_config(config).aget_state(config)
    values, interrupts = _thread_state_service.materialize_state(snapshot)
    async with maker() as session:
        await session.execute(
            update(ThreadORM)
            .where(ThreadORM.thread_id == thread_id)
            .values(values_json=values, interrupts_json=interrupts, state_updated_at=datetime.now(UTC))
        )
        await session.commit()
