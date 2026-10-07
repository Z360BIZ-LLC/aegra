"""Deterministic E2E coverage for durable FIFO enqueue scheduling."""

import asyncio
import os
import time
import uuid
from collections.abc import Mapping
from typing import Any

import httpx
import pytest

from aegra_api.settings import settings

ENABLED = os.getenv("AEGRA_E2E_ENQUEUE") == "1"
TERMINAL = {"success", "error", "interrupted"}

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not ENABLED,
        reason="Run through `make e2e-enqueue-both`",
    ),
]


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        base_url=settings.app.SERVER_URL,
        timeout=30.0,
    ) as http_client:
        yield http_client


async def _thread(client: httpx.AsyncClient) -> str:
    response = await client.post("/threads", json={})
    response.raise_for_status()
    return response.json()["thread_id"]


async def _create(
    client: httpx.AsyncClient,
    thread_id: str,
    value: str,
    *,
    delay: float = 0,
    strategy: str | None = None,
    fail: bool = False,
    pause: bool = False,
    checkpoint: Mapping[str, Any] | None = None,
    org_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "assistant_id": "queue",
        "input": {
            "value": value,
            "delay": delay,
            "fail": fail,
            "pause": pause,
        },
    }
    if strategy is not None:
        payload["multitask_strategy"] = strategy
    if checkpoint is not None:
        payload["checkpoint"] = dict(checkpoint)
    if org_id is not None:
        payload["config"] = {"configurable": {"org_id": org_id}}
    response = await client.post(f"/threads/{thread_id}/runs", json=payload)
    response.raise_for_status()
    return response.json()


async def _get_run(
    client: httpx.AsyncClient,
    thread_id: str,
    run_id: str,
) -> dict[str, Any]:
    response = await client.get(f"/threads/{thread_id}/runs/{run_id}")
    response.raise_for_status()
    return response.json()


async def _wait_status(
    client: httpx.AsyncClient,
    thread_id: str,
    run_id: str,
    statuses: set[str],
    *,
    timeout: float = 20,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = await _get_run(client, thread_id, run_id)
        if latest["status"] in statuses:
            return latest
        await asyncio.sleep(0.1)
    pytest.fail(f"run {run_id} did not reach {statuses}; latest={latest}")


async def _wait_terminal(
    client: httpx.AsyncClient,
    thread_id: str,
    run_id: str,
) -> dict[str, Any]:
    return await _wait_status(client, thread_id, run_id, TERMINAL)


def _values(run: Mapping[str, Any]) -> list[str]:
    output = run.get("output")
    assert isinstance(output, Mapping)
    values = output.get("values")
    assert isinstance(values, list)
    return values


async def test_omitted_strategy_preserves_both_state_updates_fifo(
    client: httpx.AsyncClient,
) -> None:
    thread_id = await _thread(client)
    first = await _create(client, thread_id, "first", delay=1.5)
    second = await _create(client, thread_id, "second")

    await _wait_status(client, thread_id, first["run_id"], {"running"})
    queued = await _get_run(client, thread_id, second["run_id"])
    assert queued["status"] == "pending"

    assert (await _wait_terminal(client, thread_id, first["run_id"]))["status"] == "success"
    completed = await _wait_terminal(client, thread_id, second["run_id"])
    assert completed["status"] == "success"
    assert _values(completed) == ["first", "second"]


async def test_concurrent_http_creates_preserve_every_state_update(
    client: httpx.AsyncClient,
) -> None:
    """Simultaneous requests may commit in any order, but none may be lost."""
    thread_id = await _thread(client)
    labels = [f"concurrent-{index}" for index in range(6)]
    created = await asyncio.gather(*(_create(client, thread_id, label, delay=0.15) for label in labels))

    completed = await asyncio.gather(*(_wait_terminal(client, thread_id, run["run_id"]) for run in created))
    outputs = [_values(run) for run in completed]
    final_values = max(outputs, key=len)

    assert len(final_values) == len(labels)
    assert set(final_values) == set(labels)


async def test_explicit_enqueue_matches_default(client: httpx.AsyncClient) -> None:
    thread_id = await _thread(client)
    first = await _create(
        client,
        thread_id,
        "explicit-first",
        delay=0.8,
        strategy="enqueue",
    )
    second = await _create(
        client,
        thread_id,
        "explicit-second",
        strategy="enqueue",
    )

    await _wait_terminal(client, thread_id, first["run_id"])
    completed = await _wait_terminal(client, thread_id, second["run_id"])
    assert _values(completed) == ["explicit-first", "explicit-second"]


async def test_different_threads_execute_concurrently(client: httpx.AsyncClient) -> None:
    first_thread, second_thread = await asyncio.gather(
        _thread(client),
        _thread(client),
    )
    started = time.monotonic()
    first, second = await asyncio.gather(
        _create(client, first_thread, "a", delay=1.2),
        _create(client, second_thread, "b", delay=1.2),
    )
    await asyncio.gather(
        _wait_terminal(client, first_thread, first["run_id"]),
        _wait_terminal(client, second_thread, second["run_id"]),
    )
    assert time.monotonic() - started < 2.2


async def test_error_and_interrupt_release_successor(
    client: httpx.AsyncClient,
) -> None:
    error_thread = await _thread(client)
    failed = await _create(client, error_thread, "fail", delay=0.4, fail=True)
    after_error = await _create(client, error_thread, "after-error")
    assert (await _wait_terminal(client, error_thread, failed["run_id"]))["status"] == "error"
    assert (await _wait_terminal(client, error_thread, after_error["run_id"]))["status"] == "success"

    interrupt_thread = await _thread(client)
    interrupted = await _create(
        client,
        interrupt_thread,
        "interrupt",
        delay=0.4,
        pause=True,
    )
    after_interrupt = await _create(client, interrupt_thread, "after-interrupt")
    assert (await _wait_terminal(client, interrupt_thread, interrupted["run_id"]))["status"] == "interrupted"
    assert (await _wait_terminal(client, interrupt_thread, after_interrupt["run_id"]))["status"] == "success"


async def test_queued_cancellation_releases_following_run(
    client: httpx.AsyncClient,
) -> None:
    thread_id = await _thread(client)
    first = await _create(client, thread_id, "kept", delay=1.2)
    cancelled = await _create(client, thread_id, "cancelled")
    await _wait_status(client, thread_id, first["run_id"], {"running"})
    await _wait_status(client, thread_id, cancelled["run_id"], {"pending"})
    response = await client.post(f"/threads/{thread_id}/runs/{cancelled['run_id']}/cancel")
    response.raise_for_status()
    following = await _create(client, thread_id, "following")

    assert (await _wait_terminal(client, thread_id, cancelled["run_id"]))["status"] == "interrupted"
    completed = await _wait_terminal(client, thread_id, following["run_id"])
    assert _values(completed) == ["kept", "following"]


async def test_force_delete_waits_for_running_predecessor_before_promotion(
    client: httpx.AsyncClient,
) -> None:
    thread_id = await _thread(client)
    predecessor = await _create(client, thread_id, "deleted", delay=1.5)
    successor = await _create(client, thread_id, "after-delete")
    await _wait_status(client, thread_id, predecessor["run_id"], {"running"})
    await _wait_status(client, thread_id, successor["run_id"], {"pending"})

    response = await client.delete(
        f"/threads/{thread_id}/runs/{predecessor['run_id']}",
        params={"force": 1},
    )
    response.raise_for_status()

    completed = await _wait_terminal(client, thread_id, successor["run_id"])
    assert completed["status"] == "success"
    assert _values(completed) == ["after-delete"]


async def test_explicit_checkpoint_is_not_reinterpreted_while_queued(
    client: httpx.AsyncClient,
) -> None:
    thread_id = await _thread(client)
    baseline = await _create(client, thread_id, "baseline")
    await _wait_terminal(client, thread_id, baseline["run_id"])
    state_response = await client.get(f"/threads/{thread_id}/state")
    state_response.raise_for_status()
    checkpoint = state_response.json()["checkpoint"]

    blocker = await _create(client, thread_id, "current", delay=1.0)
    branch = await _create(
        client,
        thread_id,
        "branch",
        checkpoint=checkpoint,
    )
    await _wait_terminal(client, thread_id, blocker["run_id"])
    branched = await _wait_terminal(client, thread_id, branch["run_id"])

    assert _values(branched) == ["baseline", "branch"]


async def test_thread_fifo_composes_with_org_capacity(
    client: httpx.AsyncClient,
) -> None:
    org_id = f"enqueue-{uuid.uuid4().hex[:8]}"
    first_thread, second_thread, third_thread = await asyncio.gather(
        _thread(client),
        _thread(client),
        _thread(client),
    )
    first = await _create(
        client,
        first_thread,
        "org-first",
        delay=1.0,
        org_id=org_id,
    )
    same_thread = await _create(
        client,
        first_thread,
        "org-same-thread",
        org_id=org_id,
    )
    other_thread = await _create(
        client,
        second_thread,
        "org-other-thread",
        delay=1.0,
        org_id=org_id,
    )
    third_head = await _create(
        client,
        third_thread,
        "org-third-thread",
        delay=1.0,
        org_id=org_id,
    )

    await asyncio.sleep(0.3)
    states = await asyncio.gather(
        _get_run(client, first_thread, first["run_id"]),
        _get_run(client, first_thread, same_thread["run_id"]),
        _get_run(client, second_thread, other_thread["run_id"]),
        _get_run(client, third_thread, third_head["run_id"]),
    )
    heads = [states[0], states[2], states[3]]
    assert sum(run["status"] == "running" for run in heads) <= 2
    assert any(run["status"] == "pending" for run in heads)
    assert states[1]["status"] == "pending"

    completed = await asyncio.gather(
        _wait_terminal(client, first_thread, first["run_id"]),
        _wait_terminal(client, first_thread, same_thread["run_id"]),
        _wait_terminal(client, second_thread, other_thread["run_id"]),
        _wait_terminal(client, third_thread, third_head["run_id"]),
    )
    assert all(run["status"] == "success" for run in completed)
