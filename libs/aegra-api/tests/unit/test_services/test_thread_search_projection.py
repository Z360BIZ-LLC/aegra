"""Unit tests for ThreadSearchProjectionService.

The DB-touching ``_fetch_values`` is stubbed so these stay fast and assert the
projection logic: field projection, channel pruning, and metadata-only extracts
that must never read checkpoints.
"""

from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from aegra_api.services.thread_search_projection import ThreadSearchProjectionService


def _base(thread_id: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "thread_id": thread_id,
        "status": "idle",
        "metadata": metadata or {},
        "user_id": "user-1",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    }


async def test_select_projects_only_requested_fields() -> None:
    svc = ThreadSearchProjectionService()
    out = await svc.project([_base("t1", {"a": 1})], select=["thread_id", "status"], extract=None)
    assert out == [{"thread_id": "t1", "status": "idle"}]


async def test_metadata_extract_never_reads_checkpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = ThreadSearchProjectionService()

    async def _boom(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("_fetch_values must not run for metadata-only extracts")

    monkeypatch.setattr(svc, "_fetch_values", _boom)
    out = await svc.project(
        [_base("t1", {"title": "Hello"})], select=["thread_id"], extract={"title": "metadata.title"}
    )
    assert out == [{"thread_id": "t1", "extracted": {"title": "Hello"}}]


async def test_values_extract_prunes_to_referenced_channels(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = ThreadSearchProjectionService()
    seen: dict[str, Any] = {}

    async def _fake(thread_ids: list[str], channels: list[str] | None) -> dict[str, dict[str, Any]]:
        seen["channels"] = channels
        return {"t1": {"messages": [{"content": "first"}, {"content": "last"}], "summary": "s"}}

    monkeypatch.setattr(svc, "_fetch_values", _fake)
    out = await svc.project(
        [_base("t1")],
        select=["thread_id"],
        extract={"a": "values.messages[0].content", "b": "values.summary"},
    )
    assert seen["channels"] == ["messages", "summary"]
    assert out[0]["extracted"] == {"a": "first", "b": "s"}


async def test_select_values_fetches_all_channels_and_attaches_state(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = ThreadSearchProjectionService()
    seen: dict[str, Any] = {}

    async def _fake(thread_ids: list[str], channels: list[str] | None) -> dict[str, dict[str, Any]]:
        seen["channels"] = channels
        return {"t1": {"messages": [{"content": "hi"}], "counter": 3}}

    monkeypatch.setattr(svc, "_fetch_values", _fake)
    out = await svc.project([_base("t1")], select=["thread_id", "values"], extract=None)
    assert seen["channels"] is None  # full state → no channel filter
    assert out[0] == {"thread_id": "t1", "values": {"messages": [{"content": "hi"}], "counter": 3}}


async def test_thread_without_checkpoint_yields_empty_values_and_null_extract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    svc = ThreadSearchProjectionService()

    async def _empty(thread_ids: list[str], channels: list[str] | None) -> dict[str, dict[str, Any]]:
        return {}

    monkeypatch.setattr(svc, "_fetch_values", _empty)
    out = await svc.project([_base("t1")], select=["thread_id", "values"], extract={"x": "values.a.b"})
    assert out[0]["values"] == {}
    assert out[0]["extracted"] == {"x": None}


async def test_extract_without_select_keeps_full_base(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = ThreadSearchProjectionService()

    async def _boom(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("metadata-only extract must not read checkpoints")

    monkeypatch.setattr(svc, "_fetch_values", _boom)
    out = await svc.project([_base("t1", {"title": "Hi"})], select=None, extract={"t": "metadata.title"})
    # No select → the full base survives, plus extracted.
    assert out[0]["user_id"] == "user-1"
    assert out[0]["extracted"] == {"t": "Hi"}


class TestDecodeRow:
    """_decode_row reconstructs state like the checkpointer: inline + blobs."""

    def test_merges_inline_values_and_decoded_blobs(self) -> None:
        svc = ThreadSearchProjectionService()
        serde = JsonPlusSerializer()
        # `messages` lives in a blob (complex objects); `summary` is inline JSON.
        blob_type, blob = serde.dumps_typed([HumanMessage("hi"), AIMessage("yo")])
        row = {
            "thread_id": "t1",
            "inline_values": {"summary": "s"},
            "channel_values": [(b"messages", blob_type.encode(), blob)],
        }
        state = svc._decode_row(row, serde)
        assert state["summary"] == "s"
        assert [m["content"] for m in state["messages"]] == ["hi", "yo"]

    def test_skips_empty_typed_blobs(self) -> None:
        svc = ThreadSearchProjectionService()
        serde = JsonPlusSerializer()
        row = {
            "thread_id": "t1",
            "inline_values": {"summary": "s"},
            "channel_values": [(b"__start__", b"empty", b"")],
        }
        assert svc._decode_row(row, serde) == {"summary": "s"}

    def test_no_state_yields_empty_dict(self) -> None:
        svc = ThreadSearchProjectionService()
        row = {"thread_id": "t1", "inline_values": None, "channel_values": None}
        assert svc._decode_row(row, JsonPlusSerializer()) == {}
