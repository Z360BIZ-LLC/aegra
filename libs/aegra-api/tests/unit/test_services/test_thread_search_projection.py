"""Unit tests for ThreadSearchProjectionService.

The DB-touching ``_fetch_projection`` is stubbed so these stay fast and assert the
projection logic: field projection, SQL-vs-Python extraction routing, and that
state is only fetched when referenced. SQL jsonpath extraction itself is covered
end-to-end against real Postgres in the e2e/verification layer.
"""

from typing import Any

import pytest

from aegra_api.services.thread_search_projection import ThreadSearchProjectionService, _to_jsonpath


def _base(thread_id: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "thread_id": thread_id,
        "status": "idle",
        "metadata": metadata or {},
        "user_id": "user-1",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }


def _stub(
    svc: ThreadSearchProjectionService, monkeypatch: pytest.MonkeyPatch, mapping: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Patch _fetch_projection to return `mapping`; capture the call kwargs."""
    seen: dict[str, Any] = {}

    async def _fake(
        thread_ids: list[str],
        *,
        want_values: bool,
        want_interrupts: bool,
        value_paths: dict[str, Any],
        interrupt_paths: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        seen.update(
            want_values=want_values,
            want_interrupts=want_interrupts,
            value_paths=dict(value_paths),
            interrupt_paths=dict(interrupt_paths),
            thread_ids=thread_ids,
        )
        return mapping

    monkeypatch.setattr(svc, "_fetch_projection", _fake)
    return seen


async def _boom(*_a: Any, **_k: Any) -> dict[str, Any]:
    raise AssertionError("_fetch_projection must not be called")


class TestToJsonPath:
    """Segment -> Postgres jsonpath translation (strict mode; verified vs PG e2e)."""

    def test_key_and_positive_index(self) -> None:
        assert _to_jsonpath(["messages", 0, "content"]) == 'strict $."messages"[0]."content"'

    def test_negative_one_is_last(self) -> None:
        assert _to_jsonpath(["messages", -1, "content"]) == 'strict $."messages"[last]."content"'

    def test_negative_n(self) -> None:
        assert _to_jsonpath(["a", -2]) == 'strict $."a"[last-1]'

    def test_hyphenated_key_quoted(self) -> None:
        assert _to_jsonpath(["task-1", 0, "value"]) == 'strict $."task-1"[0]."value"'


class TestProjectRouting:
    async def test_select_basic_never_fetches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        monkeypatch.setattr(svc, "_fetch_projection", _boom)
        out = await svc.project([_base("t1", {"a": 1})], select=["thread_id", "status"], extract=None)
        assert out == [{"thread_id": "t1", "status": "idle"}]

    async def test_metadata_extract_never_fetches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        monkeypatch.setattr(svc, "_fetch_projection", _boom)
        out = await svc.project([_base("t1", {"title": "Hi"})], select=["thread_id"], extract={"t": "metadata.title"})
        assert out == [{"thread_id": "t1", "extracted": {"t": "Hi"}}]

    async def test_values_extract_routes_to_sql(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        seen = _stub(svc, monkeypatch, {"t1": {"extracted": {"last": "bye"}}})
        out = await svc.project([_base("t1")], select=["thread_id"], extract={"last": "values.messages[-1].content"})
        assert seen["want_values"] is False and "last" in seen["value_paths"]
        assert out[0] == {"thread_id": "t1", "extracted": {"last": "bye"}}

    async def test_interrupts_extract_routes_to_sql(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        seen = _stub(svc, monkeypatch, {"t1": {"extracted": {"q": "confirm?"}}})
        out = await svc.project([_base("t1")], select=["thread_id"], extract={"q": "interrupts.task-1[0].value.q"})
        assert "q" in seen["interrupt_paths"]
        assert out[0]["extracted"] == {"q": "confirm?"}

    async def test_select_values_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        seen = _stub(svc, monkeypatch, {"t1": {"values": {"messages": [{"content": "hi"}]}}})
        out = await svc.project([_base("t1")], select=["thread_id", "values"], extract=None)
        assert seen["want_values"] is True
        assert out[0] == {"thread_id": "t1", "values": {"messages": [{"content": "hi"}]}}

    async def test_select_values_plus_extract_uses_python_not_sql(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        seen = _stub(svc, monkeypatch, {"t1": {"values": {"messages": [{"content": "hi"}, {"content": "bye"}]}}})
        out = await svc.project(
            [_base("t1")], select=["thread_id", "values"], extract={"last": "values.messages[-1].content"}
        )
        # Full column already fetched → extract in Python, no SQL path.
        assert seen["value_paths"] == {}
        assert out[0]["extracted"]["last"] == "bye"

    async def test_select_interrupts_full(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        interrupts = {"task-1": [{"value": {"q": "confirm?"}, "id": "i1"}]}
        seen = _stub(svc, monkeypatch, {"t1": {"interrupts": interrupts}})
        out = await svc.project([_base("t1")], select=["thread_id", "interrupts"], extract=None)
        assert seen["want_interrupts"] is True
        assert out[0] == {"thread_id": "t1", "interrupts": interrupts}

    async def test_thread_without_state_yields_empty_and_null(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        _stub(svc, monkeypatch, {})
        out = await svc.project([_base("t1")], select=["thread_id", "values"], extract={"x": "values.a.b"})
        assert out[0]["values"] == {}
        assert out[0]["extracted"] == {"x": None}

    async def test_extract_without_select_keeps_full_base(self, monkeypatch: pytest.MonkeyPatch) -> None:
        svc = ThreadSearchProjectionService()
        monkeypatch.setattr(svc, "_fetch_projection", _boom)
        out = await svc.project([_base("t1", {"title": "Hi"})], select=None, extract={"t": "metadata.title"})
        assert out[0]["user_id"] == "user-1"
        assert out[0]["extracted"] == {"t": "Hi"}

    async def test_config_and_malformed_paths_resolve_null_without_fetch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # config isn't materialized and a malformed sub-path is unparseable —
        # both resolve to null, and neither needs a state fetch.
        svc = ThreadSearchProjectionService()
        monkeypatch.setattr(svc, "_fetch_projection", _boom)
        out = await svc.project(
            [_base("t1")], select=["thread_id"], extract={"c": "config.foo", "bad": "values.a[xyz]"}
        )
        assert out[0] == {"thread_id": "t1", "extracted": {"c": None, "bad": None}}
