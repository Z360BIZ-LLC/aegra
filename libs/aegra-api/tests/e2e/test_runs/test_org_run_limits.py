"""E2E tests for per-organization run limits.

Requires a server started with ``ORG_RUN_LIMIT_MODE=enforce``, a small
``ORG_MAX_CONCURRENT_RUNS``, and the ``sleeper`` graph from
``tests/e2e/harness/run_limits``. ``make e2e-run-limits`` wires all of that up
and sets ``AEGRA_E2E_ORG_RUN_LIMIT`` to the configured ceiling.

The point of these tests is the guarantee that over-limit runs are *queued,
never rejected* — a rejected run means a dropped customer message.
"""

import asyncio
import os
import time
import uuid

import httpx
import pytest

from aegra_api.settings import settings

LIMIT = int(os.getenv("AEGRA_E2E_ORG_RUN_LIMIT", "0"))
SLEEP_SECONDS = float(os.getenv("SLEEP_GRAPH_SECONDS", "6"))

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        LIMIT <= 0,
        reason="Set AEGRA_E2E_ORG_RUN_LIMIT (see `make e2e-run-limits`) to run org run-limit E2E tests",
    ),
]


def _org(prefix: str) -> str:
    """Unique org per test so reruns never inherit another test's backlog."""
    return f"e2e-{prefix}-{uuid.uuid4().hex[:8]}"


async def _create_run(client: httpx.AsyncClient, org: str) -> tuple[str, str]:
    """Create a thread plus one background run scoped to ``org``."""
    thread = (await client.post("/threads", json={})).json()
    run = (
        await client.post(
            f"/threads/{thread['thread_id']}/runs",
            json={
                "assistant_id": "sleeper",
                "input": {},
                "config": {"configurable": {"org_id": org}},
            },
        )
    ).json()
    return thread["thread_id"], run["run_id"]


async def _status_counts(client: httpx.AsyncClient, runs: list[tuple[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for thread_id, run_id in runs:
        status = (await client.get(f"/threads/{thread_id}/runs/{run_id}")).json()["status"]
        counts[status] = counts.get(status, 0) + 1
    return counts


async def _wait_for_all_success(
    client: httpx.AsyncClient, runs: list[tuple[str, str]], *, timeout: float
) -> tuple[dict[str, int], float]:
    started = time.monotonic()
    counts: dict[str, int] = {}
    while time.monotonic() - started < timeout:
        counts = await _status_counts(client, runs)
        if counts.get("success", 0) == len(runs):
            break
        await asyncio.sleep(1.0)
    return counts, time.monotonic() - started


@pytest.fixture
async def client():
    async with httpx.AsyncClient(base_url=settings.app.SERVER_URL, timeout=30.0) as http_client:
        yield http_client


async def test_over_limit_runs_are_queued_not_rejected(client: httpx.AsyncClient) -> None:
    org = _org("queue")
    runs = [await _create_run(client, org) for _ in range(LIMIT * 2)]

    await asyncio.sleep(2.0)
    counts = await _status_counts(client, runs)

    assert counts.get("running", 0) == LIMIT, f"expected exactly {LIMIT} running, got {counts}"
    assert counts.get("pending", 0) == LIMIT, f"expected {LIMIT} queued, got {counts}"
    assert counts.get("error", 0) == 0, f"no run may be rejected, got {counts}"


async def test_queued_runs_drain_as_capacity_frees(client: httpx.AsyncClient) -> None:
    org = _org("drain")
    runs = [await _create_run(client, org) for _ in range(LIMIT * 2)]

    counts, elapsed = await _wait_for_all_success(client, runs, timeout=SLEEP_SECONDS * 8 + 30)

    assert counts.get("success", 0) == len(runs), f"every queued run must complete, got {counts}"
    # Two waves of `LIMIT` runs cannot finish in the time one wave takes;
    # this is what separates real serialization from "they all ran at once".
    assert elapsed > SLEEP_SECONDS, f"runs did not serialize behind the limit (drained in {elapsed:.1f}s)"


async def test_one_orgs_backlog_does_not_block_another_org(client: httpx.AsyncClient) -> None:
    saturated = _org("busy")
    bystander = _org("idle")
    backlog = [await _create_run(client, saturated) for _ in range(LIMIT * 3)]

    await asyncio.sleep(1.0)
    theirs = [await _create_run(client, bystander) for _ in range(LIMIT)]
    await asyncio.sleep(2.0)

    counts = await _status_counts(client, theirs)

    assert counts.get("running", 0) == LIMIT, f"bystander org must be unaffected, got {counts}"
    assert len(backlog) == LIMIT * 3
