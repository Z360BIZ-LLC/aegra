"""E2E tests for POST /threads/search select/extract/values projection.

These run a real graph first so the thread has checkpoint state, then assert the
server projects/extracts it in a single search call (no N+1).
"""

import uuid

import pytest
from httpx import AsyncClient

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client


async def _thread_with_state(question: str) -> str:
    """Create a thread, run the 'agent' graph once, return the thread id."""
    client = get_e2e_client()
    assistant = await client.assistants.create(graph_id="agent", if_exists="do_nothing")
    thread = await client.threads.create()
    thread_id = thread["thread_id"]
    await client.runs.wait(
        thread_id,
        assistant["assistant_id"],
        input={"messages": [{"role": "user", "content": question}]},
    )
    elog("Seeded thread with state", {"thread_id": thread_id, "question": question})
    return thread_id


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_extract_first_and_last_message_e2e() -> None:
    """extract pulls message content out of state in one search call."""
    question = f"what is rust? {uuid.uuid4().hex[:8]}"
    thread_id = await _thread_with_state(question)

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.post(
            "/threads/search",
            json={
                "ids": [thread_id],
                "select": ["thread_id"],
                "extract": {
                    "first": "values.messages[0].content",
                    "last": "values.messages[-1].content",
                },
            },
        )
    assert resp.status_code == 200, resp.text
    rows = resp.json()
    elog("extract result", rows)
    row = next(r for r in rows if r["thread_id"] == thread_id)
    assert set(row.keys()) == {"thread_id", "extracted"}
    assert row["extracted"]["first"] == question
    assert row["extracted"]["last"]  # AI reply present


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_select_values_returns_full_state_e2e() -> None:
    """select=['values'] attaches the full decoded state values."""
    question = f"hello there {uuid.uuid4().hex[:8]}"
    thread_id = await _thread_with_state(question)

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.post(
            "/threads/search",
            json={"ids": [thread_id], "select": ["thread_id", "values"]},
        )
    assert resp.status_code == 200, resp.text
    row = next(r for r in resp.json() if r["thread_id"] == thread_id)
    assert set(row.keys()) == {"thread_id", "values"}
    messages = row["values"]["messages"]
    assert messages and messages[0]["content"] == question


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_metadata_extract_without_state_e2e() -> None:
    """metadata.* extract works on a stateless thread (no checkpoint)."""
    client = get_e2e_client()
    tag = f"proj-meta-{uuid.uuid4().hex[:8]}"
    thread = await client.threads.create(metadata={"thread_name": tag})
    thread_id = thread["thread_id"]

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.post(
            "/threads/search",
            json={
                "ids": [thread_id],
                "select": ["thread_id"],
                "extract": {"name": "metadata.thread_name"},
            },
        )
    assert resp.status_code == 200, resp.text
    row = next(r for r in resp.json() if r["thread_id"] == thread_id)
    assert row["extracted"]["name"] == tag


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_backward_compatible_default_shape_e2e() -> None:
    """A search without select/extract still returns the full thread shape."""
    client = get_e2e_client()
    tag = f"proj-compat-{uuid.uuid4().hex[:8]}"
    thread = await client.threads.create(metadata={"search_test_tag": tag})

    async with AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http:
        resp = await http.post("/threads/search", json={"metadata": {"search_test_tag": tag}})
    assert resp.status_code == 200, resp.text
    row = resp.json()[0]
    assert row["thread_id"] == thread["thread_id"]
    assert "metadata" in row and "status" in row and "created_at" in row
