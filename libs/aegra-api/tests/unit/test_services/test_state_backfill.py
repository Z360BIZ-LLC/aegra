"""Unit tests for the thread-state backfill: run-once guard + batching."""

import contextlib
from typing import Any
from unittest.mock import AsyncMock

import pytest

from aegra_api.services import state_backfill as sb


class _FakePool:
    @contextlib.asynccontextmanager
    async def connection(self) -> Any:
        yield object()


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows


class _FakeSession:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *_a: Any) -> bool:
        return False

    async def execute(self, _stmt: Any) -> _Result:
        return _Result(self._rows)

    async def commit(self) -> None:
        pass


def _maker(rows: list[Any]) -> Any:
    return lambda: _FakeSession(rows)


class TestRunOnceGuard:
    async def test_skips_when_marker_exists(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sb.db_manager, "lg_pool", _FakePool())
        monkeypatch.setattr(sb, "_try_advisory_lock", AsyncMock(return_value=True))
        monkeypatch.setattr(sb, "_marker_exists", AsyncMock(return_value=True))
        ran = AsyncMock()
        monkeypatch.setattr(sb, "backfill_thread_state", ran)
        await sb.run_startup_backfill(batch_size=10, sleep_s=0)
        ran.assert_not_awaited()

    async def test_skips_when_lock_not_acquired(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sb.db_manager, "lg_pool", _FakePool())
        monkeypatch.setattr(sb, "_try_advisory_lock", AsyncMock(return_value=False))
        marker = AsyncMock(return_value=False)
        monkeypatch.setattr(sb, "_marker_exists", marker)
        ran = AsyncMock()
        monkeypatch.setattr(sb, "backfill_thread_state", ran)
        await sb.run_startup_backfill(batch_size=10, sleep_s=0)
        ran.assert_not_awaited()
        marker.assert_not_awaited()  # lock failed first → never even checks the marker

    async def test_runs_then_sets_marker(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sb.db_manager, "lg_pool", _FakePool())
        monkeypatch.setattr(sb, "_try_advisory_lock", AsyncMock(return_value=True))
        monkeypatch.setattr(sb, "_marker_exists", AsyncMock(return_value=False))
        ran = AsyncMock(return_value={"total": 3, "materialized": 3, "skipped_no_graph": 0, "failed": 0})
        set_marker = AsyncMock()
        monkeypatch.setattr(sb, "backfill_thread_state", ran)
        monkeypatch.setattr(sb, "_set_marker", set_marker)
        await sb.run_startup_backfill(batch_size=10, sleep_s=0)
        ran.assert_awaited_once()
        set_marker.assert_awaited_once()

    async def test_no_pool_is_noop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sb.db_manager, "lg_pool", None)
        ran = AsyncMock()
        monkeypatch.setattr(sb, "backfill_thread_state", ran)
        await sb.run_startup_backfill(batch_size=10, sleep_s=0)
        ran.assert_not_awaited()


class TestBatching:
    async def test_sleeps_between_batches_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        rows = [(f"t{i}", "u", {"graph_id": "g"}) for i in range(5)]
        monkeypatch.setattr(sb, "_get_session_maker", lambda: _maker(rows))
        monkeypatch.setattr(sb, "get_langgraph_service", lambda: object())
        monkeypatch.setattr(sb, "_materialize_one", AsyncMock())
        sleeps: list[float] = []

        async def _sleep(s: float) -> None:
            sleeps.append(s)

        monkeypatch.setattr(sb.asyncio, "sleep", _sleep)
        stats = await sb.backfill_thread_state(batch_size=2, sleep_s=0.5)
        assert stats["materialized"] == 5
        assert sleeps == [0.5, 0.5]  # after threads 2 and 4, not after the final batch

    async def test_skips_threads_without_graph(self, monkeypatch: pytest.MonkeyPatch) -> None:
        rows = [("t1", "u", {"graph_id": "g"}), ("t2", "u", {}), ("t3", "u", None)]
        monkeypatch.setattr(sb, "_get_session_maker", lambda: _maker(rows))
        monkeypatch.setattr(sb, "get_langgraph_service", lambda: object())
        materialize = AsyncMock()
        monkeypatch.setattr(sb, "_materialize_one", materialize)
        stats = await sb.backfill_thread_state(batch_size=0, sleep_s=0)
        assert stats == {"total": 3, "materialized": 1, "skipped_no_graph": 2, "failed": 0}
        assert materialize.await_count == 1
