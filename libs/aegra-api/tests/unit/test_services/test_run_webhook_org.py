"""The organisation a completion webhook names, and where it is read from.

`_send_run_webhook` reads the run's config to resolve the tenant for the
callback credential. `RunJob` nests that config under `execution`, and reaching
for `job.config` instead raises `AttributeError` *inside the run executor* —
which does not merely lose the webhook, it fails the run itself. That is how
this was found: on staging, every run finalised as `error` while the model had
already produced its answer.

These tests build a real `RunJob` so the attribute path is exercised rather
than assumed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from aegra_api.models.auth import User
from aegra_api.models.run_job import RunBehavior, RunExecution, RunIdentity, RunJob
from aegra_api.services.run_executor import _send_run_webhook


def _job(*, config: dict | None = None, run_metadata: dict | None = None) -> RunJob:
    return RunJob(
        identity=RunIdentity(run_id="run-1", thread_id="thread-1", graph_id="slow_agent"),
        user=User(identity="org:267"),
        execution=RunExecution(input_data={}, config=config or {}, context={}),
        behavior=RunBehavior(webhook_url="https://staging.z360.biz/webhooks/ai/267/run"),
        run_metadata=run_metadata or {},
    )


@pytest.fixture
def sent():
    with patch("aegra_api.services.run_executor.send_run_webhook", new_callable=AsyncMock) as mock:
        yield mock


async def test_the_org_is_read_from_the_nested_execution_config(sent):
    """The regression. `job.config` does not exist; `job.execution.config` does."""
    await _send_run_webhook(_job(config={"configurable": {"org_id": "267-z360-staging"}}), "success", {})
    assert sent.await_args.kwargs["org_id"] == "267-z360-staging"


async def test_run_metadata_is_the_fallback(sent):
    await _send_run_webhook(_job(run_metadata={"org_id": "99"}), "success", {})
    assert sent.await_args.kwargs["org_id"] == "99"


async def test_a_run_naming_no_org_still_sends(sent):
    """The app decides whether to accept it; dropping it would lose the result."""
    await _send_run_webhook(_job(), "success", {})
    assert sent.await_args.kwargs["org_id"] is None


async def test_the_webhook_url_still_comes_from_behavior(sent):
    """The sibling nesting that should have warned me off `job.config`."""
    await _send_run_webhook(_job(), "success", {})
    assert sent.await_args.kwargs["webhook_url"].endswith("/webhooks/ai/267/run")


async def test_the_error_path_resolves_the_org_too(sent):
    await _send_run_webhook(_job(config={"configurable": {"org_id": "267"}}), "error", {}, "boom")
    assert sent.await_args.kwargs["org_id"] == "267"
    assert sent.await_args.kwargs["status"] == "error"
