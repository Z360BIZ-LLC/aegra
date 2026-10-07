"""PostgreSQL-backed regressions for concurrent stateful run creation."""

import asyncio
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy import delete, select, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.services.run_admission import lock_thread
from aegra_api.services.run_preparation import update_thread_metadata
from aegra_api.settings import settings


@pytest.mark.asyncio
async def test_concurrent_auto_create_does_not_cross_user_thread_ownership() -> None:
    """The lock loser must not mutate a thread created by another user."""
    engine = create_async_engine(settings.db.database_url)
    thread_id = f"ownership-race-{uuid4()}"
    release_first = asyncio.Event()

    try:
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
                thread_table = await conn.scalar(text("SELECT to_regclass('public.thread')"))
        except (DBAPIError, OperationalError, OSError) as exc:
            pytest.skip(f"PostgreSQL test database is unavailable: {exc}")

        if thread_table is None:
            pytest.skip("thread table is unavailable; run Alembic migrations before this DB regression test")

        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        first_has_created = asyncio.Event()
        async def create_for_user(user_id: str, assistant_id: str, *, hold_lock: bool = False) -> None:
            async with session_maker() as session:
                await lock_thread(session, thread_id)
                await update_thread_metadata(
                    session,
                    thread_id,
                    assistant_id,
                    "graph",
                    user_id=user_id,
                )
                if hold_lock:
                    first_has_created.set()
                    await release_first.wait()
                await session.commit()

        first = asyncio.create_task(create_for_user("first-user", "first-assistant", hold_lock=True))
        await asyncio.wait_for(first_has_created.wait(), timeout=2)
        second = asyncio.create_task(create_for_user("second-user", "second-assistant"))
        await asyncio.sleep(0.05)
        assert not second.done(), "second creator should be waiting on the thread advisory lock"

        release_first.set()
        await first
        with pytest.raises(HTTPException) as exc_info:
            await second
        assert exc_info.value.status_code == 404

        async with session_maker() as session:
            saved = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == thread_id))
            assert saved is not None
            assert saved.user_id == "first-user"
            assert saved.metadata_json["assistant_id"] == "first-assistant"
            await session.execute(delete(ThreadORM).where(ThreadORM.thread_id == thread_id))
            await session.commit()
    finally:
        release_first.set()
        await engine.dispose()
