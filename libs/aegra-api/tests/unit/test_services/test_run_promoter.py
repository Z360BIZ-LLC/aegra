"""Unit tests for the queued-run promoter."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services import run_promoter as mod
from aegra_api.services.run_promoter import RunPromoter, _webhook_url_for

WEBHOOK = "https://app.example.test/webhooks/ai/run"


@pytest.fixture(autouse=True)
def _session_maker(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Give the promoter a mock session for its lookups."""
    session = AsyncMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(mod, "_get_session_maker", lambda: MagicMock(return_value=ctx))
    return session


@pytest.fixture
def promote_calls(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    """Capture executor.promote calls."""
    promote = AsyncMock()
    monkeypatch.setattr(mod.executor, "promote", promote)
    return promote


@pytest.fixture
def terminal_calls(monkeypatch: pytest.MonkeyPatch) -> tuple[AsyncMock, AsyncMock]:
    """Capture finalize_run and send_run_webhook calls."""
    finalize = AsyncMock()
    webhook = AsyncMock()
    monkeypatch.setattr(mod, "finalize_run", finalize)
    monkeypatch.setattr(mod, "send_run_webhook", webhook)
    return finalize, webhook


def _patch_lookups(
    monkeypatch: pytest.MonkeyPatch,
    *,
    promotable: list[str] | None = None,
    expired: list[object] | None = None,
) -> None:
    monkeypatch.setattr(mod.run_limits, "find_promotable_runs", AsyncMock(return_value=promotable or []))
    monkeypatch.setattr(mod.run_limits, "find_expired_queued_runs", AsyncMock(return_value=expired or []))


def _run_orm(*, run_id: str = "run-1", webhook_url: str | None = WEBHOOK) -> MagicMock:
    orm = MagicMock()
    orm.run_id = run_id
    orm.thread_id = "thread-1"
    orm.org_id = "7-z360"
    orm.created_at = datetime.now(UTC)
    orm.execution_params = {
        "graph_id": "slow_agent",
        "user": {"identity": "anonymous", "is_authenticated": True, "permissions": []},
        "execution": {},
        "behavior": {"webhook_url": webhook_url},
        "run_metadata": {},
    }
    return orm


class TestPromotion:
    async def test_dispatches_every_promotable_run(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        _patch_lookups(monkeypatch, promotable=["run-1", "run-2"])

        await RunPromoter().tick()

        assert [call.args[0] for call in promote_calls.await_args_list] == ["run-1", "run-2"]

    async def test_does_nothing_when_queue_is_empty(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        _patch_lookups(monkeypatch)

        await RunPromoter().tick()

        promote_calls.assert_not_awaited()

    async def test_one_failed_dispatch_does_not_abort_the_batch(
        self, monkeypatch: pytest.MonkeyPatch, terminal_calls: tuple
    ) -> None:
        """A run that fails to dispatch stays queued; the rest still go."""
        promote = AsyncMock(side_effect=[RuntimeError("redis down"), None])
        monkeypatch.setattr(mod.executor, "promote", promote)
        _patch_lookups(monkeypatch, promotable=["run-1", "run-2"])

        await RunPromoter().tick()

        assert promote.await_count == 2


class TestQueueExpiry:
    async def test_expired_run_is_failed_and_webhooked(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        finalize, webhook = terminal_calls
        _patch_lookups(monkeypatch, expired=[_run_orm()])

        await RunPromoter().tick()

        assert finalize.await_args.kwargs["status"] == "error"
        assert finalize.await_args.kwargs["thread_status"] == "error"
        assert webhook.await_args.kwargs["status"] == "error"
        assert webhook.await_args.kwargs["webhook_url"] == WEBHOOK
        assert webhook.await_args.kwargs["error_message"] == mod._QUEUE_EXPIRY_ERROR

    async def test_run_without_webhook_is_still_failed(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        finalize, webhook = terminal_calls
        _patch_lookups(monkeypatch, expired=[_run_orm(webhook_url=None)])

        await RunPromoter().tick()

        finalize.assert_awaited_once()
        webhook.assert_not_awaited()

    async def test_expires_every_overdue_run(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        finalize, _ = terminal_calls
        _patch_lookups(monkeypatch, expired=[_run_orm(run_id="a"), _run_orm(run_id="b")])

        await RunPromoter().tick()

        assert finalize.await_count == 2


class TestWebhookUrlLookup:
    def test_reads_url_from_execution_params(self) -> None:
        assert _webhook_url_for(_run_orm()) == WEBHOOK

    def test_returns_none_without_execution_params(self) -> None:
        orm = _run_orm()
        orm.execution_params = None
        assert _webhook_url_for(orm) is None

    def test_returns_none_for_unreadable_params(self) -> None:
        orm = _run_orm()
        orm.execution_params = {"behavior": {}}
        assert _webhook_url_for(orm) is None


class TestLifecycle:
    async def test_stop_is_safe_before_start(self) -> None:
        await RunPromoter().stop()

    async def test_start_then_stop_cancels_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_lookups(monkeypatch)
        promoter = RunPromoter()

        await promoter.start()
        await promoter.stop()

        assert promoter._task is None
