"""Unit tests for the queued-run promoter."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services import run_promoter as mod
from aegra_api.services.run_limits import ExpiredRun
from aegra_api.services.run_promoter import RunPromoter

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
    """Capture the expiry claim and the webhook it guards. Claim wins by default."""
    claim = AsyncMock(return_value=True)
    webhook = AsyncMock()
    monkeypatch.setattr(mod.run_limits, "claim_expired_run", claim)
    monkeypatch.setattr(mod, "set_thread_status", AsyncMock())
    monkeypatch.setattr(mod, "send_run_webhook", webhook)
    return claim, webhook


def _patch_lookups(
    monkeypatch: pytest.MonkeyPatch,
    *,
    promotable: list[str] | None = None,
    expired: list[object] | None = None,
) -> None:
    monkeypatch.setattr(mod.run_limits, "find_promotable_runs", AsyncMock(return_value=promotable or []))
    monkeypatch.setattr(mod.run_limits, "find_expired_queued_runs", AsyncMock(return_value=expired or []))


def _expired(*, run_id: str = "run-1", webhook_url: str | None = WEBHOOK) -> ExpiredRun:
    return ExpiredRun(run_id=run_id, thread_id="thread-1", org_id="7-z360", webhook_url=webhook_url)


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
        claim, webhook = terminal_calls
        _patch_lookups(monkeypatch, expired=[_expired()])

        await RunPromoter().tick()

        assert claim.await_args.kwargs["error"] == mod._QUEUE_EXPIRY_ERROR
        assert webhook.await_args.kwargs["status"] == "error"
        assert webhook.await_args.kwargs["webhook_url"] == WEBHOOK
        assert webhook.await_args.kwargs["error_message"] == mod._QUEUE_EXPIRY_ERROR

    async def test_run_without_webhook_is_still_failed(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        claim, webhook = terminal_calls
        _patch_lookups(monkeypatch, expired=[_expired(webhook_url=None)])

        await RunPromoter().tick()

        claim.assert_awaited_once()
        webhook.assert_not_awaited()

    async def test_expires_every_overdue_run(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        claim, _ = terminal_calls
        _patch_lookups(monkeypatch, expired=[_expired(run_id="a"), _expired(run_id="b")])

        await RunPromoter().tick()

        assert claim.await_count == 2

    async def test_no_webhook_when_the_run_started_before_we_expired_it(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        """A worker can claim a run between the expiry scan and the update.

        Losing the conditional claim must mean silence — otherwise a run that
        is still executing gets a failure webhook and then a success one.
        """
        claim, webhook = terminal_calls
        claim.return_value = False
        _patch_lookups(monkeypatch, expired=[_expired()])

        await RunPromoter().tick()

        webhook.assert_not_awaited()

    async def test_only_the_pod_that_wins_the_row_notifies(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        """Every pod runs a promoter, so the loser must not double-notify."""
        claim, webhook = terminal_calls
        claim.side_effect = [True, False]
        _patch_lookups(monkeypatch, expired=[_expired(run_id="a"), _expired(run_id="b")])

        await RunPromoter().tick()

        assert webhook.await_count == 1

    async def test_expiry_runs_before_promotion(
        self, monkeypatch: pytest.MonkeyPatch, promote_calls: AsyncMock, terminal_calls: tuple
    ) -> None:
        """Otherwise a doomed run can be dispatched moments before being killed."""
        order: list[str] = []
        claim, _ = terminal_calls
        claim.side_effect = lambda *a, **k: order.append("expire") or True
        monkeypatch.setattr(
            mod.run_limits,
            "find_promotable_runs",
            AsyncMock(side_effect=lambda *a, **k: order.append("promote") or []),
        )
        monkeypatch.setattr(mod.run_limits, "find_expired_queued_runs", AsyncMock(return_value=[_expired()]))

        await RunPromoter().tick()

        assert order == ["expire", "promote"]


class TestLifecycle:
    async def test_stop_is_safe_before_start(self) -> None:
        await RunPromoter().stop()

    async def test_start_then_stop_cancels_the_loop(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_lookups(monkeypatch)
        promoter = RunPromoter()

        await promoter.start()
        await promoter.stop()

        assert promoter._task is None
