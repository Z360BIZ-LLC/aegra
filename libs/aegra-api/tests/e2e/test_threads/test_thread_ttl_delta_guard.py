"""E2E: keep_latest compacts plain threads and refuses to touch delta-backed ones.

Runs against the `thread_ttl` harness (`make e2e-ttl-delta`), whose two graphs
are identical except for how `messages` is stored — `add_messages` vs
`DeltaChannel`. That makes the channel type the only variable.

Deterministic via POST /threads/prune rather than waiting on the background
sweep; both go through `_apply_strategy`, and the sweep loop is unit-tested.
"""

import os

import httpx
import psycopg
import pytest

from aegra_api.settings import settings
from tests.e2e._utils import elog, get_e2e_client

_TABLES = ("checkpoints", "checkpoint_writes", "checkpoint_blobs")
_RUNS_PER_THREAD = 3


def _dsn() -> str:
    dsn = os.environ.get("AEGRA_E2E_DSN") or settings.db.database_url_sync
    assert dsn, "no DSN available for row-count assertions"
    return dsn


def _row_counts(thread_id: str) -> dict[str, int]:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        counts = {}
        for table in _TABLES:
            cur.execute(f"SELECT count(*) FROM {table} WHERE thread_id = %s", (thread_id,))  # noqa: S608
            counts[table] = cur.fetchone()[0]
        return counts


def _has_delta_marker(thread_id: str) -> bool:
    """langgraph records this metadata key only while a delta channel awaits its
    next snapshot; a thread that never used one never carries it."""
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM checkpoints WHERE thread_id = %s "
            "AND jsonb_exists(metadata, 'counters_since_delta_snapshot')",
            (thread_id,),
        )
        return cur.fetchone()[0] > 0


def _expire(thread_id: str) -> None:
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE thread_ttl SET expires_at = now() - interval '1 day' WHERE thread_id = %s",
            (thread_id,),
        )
        assert cur.rowcount == 1, f"no thread_ttl row for {thread_id}; TTL config not applied at create"
        conn.commit()


def _prune() -> dict:
    resp = httpx.post(f"{settings.app.SERVER_URL}/threads/prune", timeout=30.0)
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _messages(thread_id: str) -> list[str]:
    client = get_e2e_client()
    state = await client.threads.get_state(thread_id)
    values = state.get("values") or {}
    return [
        m["content"] for m in values.get("messages", []) if isinstance(m, dict) and isinstance(m.get("content"), str)
    ]


async def _exercise(graph_id: str) -> str:
    """Create a thread and run it enough times to accumulate real history."""
    client = get_e2e_client()
    assistant = await client.assistants.create(graph_id=graph_id, if_exists="do_nothing")
    thread = await client.threads.create()
    thread_id = thread["thread_id"]
    for _ in range(_RUNS_PER_THREAD):
        run = await client.runs.create(
            thread_id=thread_id,
            assistant_id=assistant["assistant_id"],
            input={"messages": [], "turns": 0},
        )
        await client.runs.join(thread_id, run["run_id"])
        finished = await client.runs.get(thread_id, run["run_id"])
        assert finished["status"] == "success", f"run ended as {finished['status']}"
    return thread_id


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_keep_latest_compacts_plain_thread_without_losing_the_conversation() -> None:
    thread_id = await _exercise("plain_agent")
    assert not _has_delta_marker(thread_id)

    before_rows = _row_counts(thread_id)
    before_msgs = await _messages(thread_id)
    assert before_rows["checkpoints"] > 1, "no history to compact"
    assert len(before_msgs) == _RUNS_PER_THREAD * 3

    _expire(thread_id)
    result = _prune()
    elog("prune result", result)
    assert result["pruned"] >= 1

    after_rows = _row_counts(thread_id)
    after_msgs = await _messages(thread_id)
    elog("plain thread row counts", {"before": before_rows, "after": after_rows})

    assert after_rows["checkpoints"] == 1
    assert after_rows["checkpoints"] < before_rows["checkpoints"]
    # The whole point: storage collapses, the conversation does not.
    assert after_msgs == before_msgs


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_compacted_thread_still_accepts_a_new_run() -> None:
    thread_id = await _exercise("plain_agent")
    _expire(thread_id)
    _prune()

    before_msgs = await _messages(thread_id)
    client = get_e2e_client()
    assistant = await client.assistants.create(graph_id="plain_agent", if_exists="do_nothing")
    run = await client.runs.create(
        thread_id=thread_id, assistant_id=assistant["assistant_id"], input={"messages": [], "turns": 0}
    )
    await client.runs.join(thread_id, run["run_id"])
    finished = await client.runs.get(thread_id, run["run_id"])
    assert finished["status"] == "success"

    after_msgs = await _messages(thread_id)
    assert after_msgs[: len(before_msgs)] == before_msgs
    assert len(after_msgs) == len(before_msgs) + 3


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_keep_latest_refuses_to_prune_a_delta_backed_thread() -> None:
    """Without this guard the conversation reconstructs as empty, no error raised."""
    thread_id = await _exercise("delta_agent")
    assert _has_delta_marker(thread_id), "harness graph is not delta-backed; test proves nothing"

    before_rows = _row_counts(thread_id)
    before_msgs = await _messages(thread_id)
    assert len(before_msgs) == _RUNS_PER_THREAD * 3

    _expire(thread_id)
    result = _prune()
    elog("prune result", result)
    assert result["skipped"] >= 1

    after_rows = _row_counts(thread_id)
    after_msgs = await _messages(thread_id)
    elog("delta thread row counts", {"before": before_rows, "after": after_rows})

    assert after_rows == before_rows, "delta-backed thread must be left completely untouched"
    assert after_msgs == before_msgs
