"""One-off backfill of materialized thread state (values / interrupts).

Threads created before state materialization have NULL ``thread.values``. This
walks threads that have a graph, computes ``aget_state`` (the graph's logical
state — correct even for middleware agents), and persists it so /threads/search
can project it without waiting for each thread's next run.

Best-effort per thread: a failure is logged and skipped, never aborting the run.
"""

from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import select, update

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.models.auth import User
from aegra_api.services.langgraph_service import create_thread_config, get_langgraph_service
from aegra_api.services.thread_state_service import ThreadStateService

logger = structlog.getLogger(__name__)
_thread_state_service = ThreadStateService()


async def backfill_thread_state(*, limit: int | None = None, only_missing: bool = True) -> dict[str, int]:
    """Materialize thread state for existing threads.

    Args:
        limit: cap the number of threads processed (None = all).
        only_missing: skip threads that already have materialized values.
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

    for thread_id, user_id, metadata in rows:
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

    logger.info("Thread state backfill complete", **stats)
    return stats


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
