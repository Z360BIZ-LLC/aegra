"""Unit tests for lease_reaper service."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aegra_api.observability.metrics import REAPER_RECOVERED_RUNS
from aegra_api.services.lease_reaper import LeaseReaper


def _recovered_count(outcome: str) -> float:
    """Read the current value of the reaper counter for one outcome label."""
    return REAPER_RECOVERED_RUNS.labels(outcome=outcome)._value.get()


def _make_session_maker(session: AsyncMock) -> MagicMock:
    """Wrap a mock session in a context-manager-returning maker."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    maker = MagicMock(return_value=ctx)
    return maker


class TestFindRecoverable:
    @pytest.mark.asyncio
    async def test_returns_only_expired_running_leases(self) -> None:
        session = AsyncMock()
        crashed_result = MagicMock()
        crashed_result.fetchall.return_value = [("run-1",)]
        session.execute = AsyncMock(return_value=crashed_result)
        maker = _make_session_maker(session)

        with patch("aegra_api.services.lease_reaper._get_session_maker", return_value=maker):
            crashed = await LeaseReaper._find_recoverable()

        assert crashed == ["run-1"]
        assert session.execute.await_count == 1

    @pytest.mark.asyncio
    async def test_returns_empty_when_nothing_to_recover(self) -> None:
        session = AsyncMock()
        empty_result = MagicMock()
        empty_result.fetchall.return_value = []
        session.execute = AsyncMock(return_value=empty_result)
        maker = _make_session_maker(session)

        with patch("aegra_api.services.lease_reaper._get_session_maker", return_value=maker):
            crashed = await LeaseReaper._find_recoverable()

        assert crashed == []
        assert session.execute.await_count == 1


class TestRecoverCrashedRuns:
    @pytest.mark.asyncio
    async def test_classifies_and_transitions_under_one_transaction(self) -> None:
        session = AsyncMock()
        locked = MagicMock()
        locked.fetchall.return_value = [
            ("run-1", "thread-1", "user-1", {"_retry_count": 0}),
            ("run-2", "thread-2", "user-2", {"_retry_count": 1}),
        ]
        updated_run_1 = MagicMock()
        updated_run_1.scalar_one_or_none.return_value = "run-1"
        updated_run_2 = MagicMock()
        updated_run_2.scalar_one_or_none.return_value = "run-2"
        session.execute = AsyncMock(side_effect=[locked, updated_run_1, updated_run_2])
        session.commit = AsyncMock()
        maker = _make_session_maker(session)

        with (
            patch("aegra_api.services.lease_reaper._get_session_maker", return_value=maker),
            patch("aegra_api.services.lease_reaper.settings") as mock_settings,
            patch(
                "aegra_api.services.lease_reaper.set_thread_status_if_no_active_runs",
                new_callable=AsyncMock,
            ) as mock_set_thread,
            patch("aegra_api.services.lease_reaper.run_queue_signal.notify") as notify,
        ):
            mock_settings.worker.BG_JOB_MAX_RETRIES = 1
            retryable, exhausted = await LeaseReaper._recover_crashed_runs(["run-1", "run-2"])

        assert retryable == ["run-1"]
        assert exhausted == ["run-2"]
        for call in session.execute.await_args_list[1:]:
            compiled = call.args[0].compile()
            assert "runs.user_id" in str(compiled)
            assert "queue_position" not in compiled.params
        mock_set_thread.assert_awaited_once_with(session, {"thread-2"}, "error", user_id="user-2")
        session.commit.assert_awaited_once()
        notify.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_empty_when_rows_are_no_longer_expired(self) -> None:
        session = AsyncMock()
        locked = MagicMock()
        locked.fetchall.return_value = []
        session.execute = AsyncMock(return_value=locked)
        session.commit = AsyncMock()
        maker = _make_session_maker(session)

        with (
            patch("aegra_api.services.lease_reaper._get_session_maker", return_value=maker),
            patch("aegra_api.services.lease_reaper.run_queue_signal.notify") as notify,
        ):
            retryable, exhausted = await LeaseReaper._recover_crashed_runs(["run-1"])

        assert retryable == []
        assert exhausted == []
        session.commit.assert_awaited_once()
        notify.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_run_when_guarded_transition_loses_race(self) -> None:
        session = AsyncMock()
        locked = MagicMock()
        locked.fetchall.return_value = [
            ("run-1", "thread-1", "user-1", {"_retry_count": 1}),
        ]
        unchanged = MagicMock()
        unchanged.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(side_effect=[locked, unchanged])
        session.commit = AsyncMock()
        maker = _make_session_maker(session)

        with (
            patch("aegra_api.services.lease_reaper._get_session_maker", return_value=maker),
            patch("aegra_api.services.lease_reaper.settings") as mock_settings,
            patch(
                "aegra_api.services.lease_reaper.set_thread_status_if_no_active_runs",
                new_callable=AsyncMock,
            ) as mock_set_thread,
            patch("aegra_api.services.lease_reaper.run_queue_signal.notify") as notify,
        ):
            mock_settings.worker.BG_JOB_MAX_RETRIES = 1
            retryable, exhausted = await LeaseReaper._recover_crashed_runs(["run-1"])

        assert retryable == []
        assert exhausted == []
        mock_set_thread.assert_not_awaited()
        session.commit.assert_awaited_once()
        notify.assert_not_called()


class TestReap:
    @pytest.mark.asyncio
    async def test_crashed_runs_are_classified_before_becoming_claimable(self) -> None:
        reaper = LeaseReaper()

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=["run-1", "run-2"],
            ),
            patch.object(
                LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock, return_value=(["run-1"], ["run-2"])
            ) as mock_recover,
        ):
            await reaper._reap()

        mock_recover.assert_awaited_once_with(["run-1", "run-2"])

    @pytest.mark.asyncio
    async def test_skips_when_nothing_to_recover(self) -> None:
        reaper = LeaseReaper()

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=[],
            ),
            patch.object(LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock) as mock_recover,
        ):
            await reaper._reap()

        mock_recover.assert_not_awaited()


class TestReapMetrics:
    @pytest.mark.asyncio
    async def test_increments_counters_per_outcome_on_crashed_recovery(self) -> None:
        """Retried and exhausted crashed runs each increment their own outcome series."""
        reaper = LeaseReaper()
        retried_before = _recovered_count("crashed_retried")
        exhausted_before = _recovered_count("crashed_exhausted")

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=["run-1", "run-2"],
            ),
            patch.object(
                LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock, return_value=(["run-1"], ["run-2"])
            ),
        ):
            await reaper._reap()

        assert _recovered_count("crashed_retried") == retried_before + 1
        assert _recovered_count("crashed_exhausted") == exhausted_before + 1

    @pytest.mark.asyncio
    async def test_no_increment_when_nothing_to_recover(self) -> None:
        reaper = LeaseReaper()
        before = {outcome: _recovered_count(outcome) for outcome in ("crashed_retried", "crashed_exhausted")}

        with patch.object(
            LeaseReaper,
            "_find_recoverable",
            new_callable=AsyncMock,
            return_value=[],
        ):
            await reaper._reap()

        for outcome, value in before.items():
            assert _recovered_count(outcome) == value

    @pytest.mark.asyncio
    async def test_no_increment_when_all_crashed_claimed_elsewhere(self) -> None:
        """Runs found crashed but re-claimed before reset must not count as recovered."""
        reaper = LeaseReaper()
        before = _recovered_count("crashed_retried")

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=["run-1"],
            ),
            patch.object(
                LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock, return_value=([], [])
            ) as mock_recover,
        ):
            await reaper._reap()

        mock_recover.assert_awaited_once_with(["run-1"])
        assert _recovered_count("crashed_retried") == before

    @pytest.mark.asyncio
    async def test_crashed_retried_counts_every_reset_row(self) -> None:
        reaper = LeaseReaper()
        before = _recovered_count("crashed_retried")

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=["run-1", "run-2"],
            ),
            patch.object(
                LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock, return_value=(["run-1", "run-2"], [])
            ),
        ):
            await reaper._reap()

        assert _recovered_count("crashed_retried") == before + 2

    @pytest.mark.asyncio
    async def test_crashed_exhausted_counts_only_rows_atomically_failed(self) -> None:
        reaper = LeaseReaper()
        before = _recovered_count("crashed_exhausted")

        with (
            patch.object(
                LeaseReaper,
                "_find_recoverable",
                new_callable=AsyncMock,
                return_value=["run-1"],
            ),
            patch.object(LeaseReaper, "_recover_crashed_runs", new_callable=AsyncMock, return_value=([], [])),
        ):
            await reaper._reap()

        assert _recovered_count("crashed_exhausted") == before


class TestStartStop:
    @pytest.mark.asyncio
    async def test_start_creates_background_task(self) -> None:
        reaper = LeaseReaper()

        with patch("aegra_api.services.lease_reaper.settings") as mock_settings:
            mock_settings.worker.REAPER_INTERVAL_SECONDS = 60

            await reaper.start()

        assert reaper._task is not None
        assert not reaper._task.done()

        # Cleanup
        await reaper.stop()

    @pytest.mark.asyncio
    async def test_stop_cancels_background_task(self) -> None:
        reaper = LeaseReaper()

        with patch("aegra_api.services.lease_reaper.settings") as mock_settings:
            mock_settings.worker.REAPER_INTERVAL_SECONDS = 60

            await reaper.start()
            task = reaper._task
            await reaper.stop()

        assert reaper._task is None
        assert task is not None
        assert task.done()

    @pytest.mark.asyncio
    async def test_stop_noop_when_not_started(self) -> None:
        reaper = LeaseReaper()
        # Should not raise
        await reaper.stop()
        assert reaper._task is None
