"""Integration tests for POST /threads/search select/extract/ids projection."""

from typing import Any
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from aegra_api.services.thread_search_projection import thread_search_projection
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.session_fixtures import ThreadSession, override_session_dependency
from tests.fixtures.test_helpers import DummyThread


def _thread_row(thread_id: str, status: str = "idle", metadata: dict[str, Any] | None = None) -> DummyThread:
    thread = DummyThread(thread_id, status, metadata, "test-user")
    thread.metadata_json = metadata or {}
    return thread


def _client(threads: list[DummyThread]) -> TestClient:
    app = create_test_app(include_runs=False, include_threads=True)
    override_session_dependency(app, ThreadSession, threads=threads)
    return make_client(app)


class TestBackwardCompat:
    """Requests without select/extract return the full thread shape, unchanged."""

    def test_no_projection_returns_full_thread(self) -> None:
        client = _client([_thread_row("t1", metadata={"env": "prod"})])
        resp = client.post("/threads/search", json={})
        assert resp.status_code == 200
        row = resp.json()[0]
        assert row["thread_id"] == "t1"
        assert row["user_id"] == "test-user"
        assert row["metadata"]["env"] == "prod"
        assert "values" not in row and "extracted" not in row


class TestSelectProjection:
    """select returns only the requested keys."""

    def test_select_projects_subset(self) -> None:
        client = _client([_thread_row("t1", status="busy", metadata={"env": "prod"})])
        resp = client.post("/threads/search", json={"select": ["thread_id", "status"]})
        assert resp.status_code == 200
        assert resp.json() == [{"thread_id": "t1", "status": "busy"}]

    def test_select_metadata_included(self) -> None:
        client = _client([_thread_row("t1", metadata={"env": "prod"})])
        resp = client.post("/threads/search", json={"select": ["thread_id", "metadata"]})
        assert resp.status_code == 200
        assert resp.json() == [{"thread_id": "t1", "metadata": {"env": "prod"}}]


class TestExtractMetadata:
    """metadata.* extract resolves from the thread row without a checkpoint read."""

    def test_extract_metadata_path(self) -> None:
        client = _client([_thread_row("t1", metadata={"title": "Hello world"})])
        with patch.object(thread_search_projection, "_fetch_values", new=AsyncMock()) as fetch:
            resp = client.post(
                "/threads/search",
                json={"select": ["thread_id"], "extract": {"title": "metadata.title"}},
            )
        assert resp.status_code == 200
        assert resp.json() == [{"thread_id": "t1", "extracted": {"title": "Hello world"}}]
        fetch.assert_not_called()


class TestExtractValues:
    """values.* extract decodes state (mocked) and resolves the path."""

    def test_extract_first_and_last_message(self) -> None:
        client = _client([_thread_row("t1")])
        decoded = {"t1": {"messages": [{"content": "first"}, {"content": "last"}]}}
        with patch.object(thread_search_projection, "_fetch_values", new=AsyncMock(return_value=decoded)) as fetch:
            resp = client.post(
                "/threads/search",
                json={
                    "select": ["thread_id"],
                    "extract": {
                        "title": "values.messages[0].content",
                        "last_msg": "values.messages[-1].content",
                    },
                },
            )
        assert resp.status_code == 200
        assert resp.json() == [{"thread_id": "t1", "extracted": {"title": "first", "last_msg": "last"}}]
        # Only the `messages` channel is needed, so the fetch is pruned to it.
        assert fetch.call_args.args[1] == ["messages"]


class TestUnsupportedParams:
    """SDK params Aegra can't honour fail loudly with 422."""

    def test_values_filter_returns_422(self) -> None:
        client = _client([_thread_row("t1")])
        resp = client.post("/threads/search", json={"values": {"foo": "bar"}})
        assert resp.status_code == 422

    def test_unsupported_select_field_returns_422(self) -> None:
        client = _client([_thread_row("t1")])
        resp = client.post("/threads/search", json={"select": ["interrupts"]})
        assert resp.status_code == 422

    def test_too_many_extract_paths_returns_422(self) -> None:
        client = _client([_thread_row("t1")])
        resp = client.post(
            "/threads/search",
            json={"extract": {f"a{i}": "values.x" for i in range(11)}},
        )
        assert resp.status_code == 422
