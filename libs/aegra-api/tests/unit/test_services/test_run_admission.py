"""Unit tests for durable thread and organization run admission."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services import run_admission
from aegra_api.services.run_admission import AdmissionOutcome, try_start_run
from aegra_api.services.run_limits import LimitDecision
from aegra_api.settings import settings

ORG = "org-1"
RUN_ID = "run-1"
THREAD_ID = "thread-1"


def _result(
    *,
    first: object = None,
    rows: list[tuple[str, str | None]] | None = None,
    rowcount: int = 1,
) -> MagicMock:
    result = MagicMock()
    result.first.return_value = first
    result.all.return_value = rows or []
    result.rowcount = rowcount
    return result


def _session(*results: MagicMock, high_water: int | None = 100) -> AsyncMock:
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=results)
    session.scalar = AsyncMock(return_value=high_water)
    return session


def _run_row(
    *,
    status: str = "pending",
    claimed_by: str | None = None,
    strategy: str = "enqueue",
    org_id: str | None = None,
    pending_reason: str | None = None,
) -> tuple[str, str | None, str, str | None, str, int, str | None]:
    return (
        THREAD_ID,
        org_id,
        status,
        claimed_by,
        strategy,
        2,
        pending_reason,
    )


@pytest.fixture(autouse=True)
def _limits_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "off")
    monkeypatch.setattr(run_admission, "_promotion_cursor", 0)
    monkeypatch.setattr(run_admission, "_promotion_high_water", None)


class TestTryStartRun:
    async def test_claims_enqueue_run_without_older_active_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = _session(_result(first=_run_row()), _result(first=_run_row()), _result(rowcount=1))
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=False))

        outcome = await try_start_run(session, RUN_ID, claimed_by="worker-1")

        assert outcome is AdmissionOutcome.CLAIMED
        claim = session.execute.await_args_list[-1].args[0]
        assert claim.compile().params["claimed_by"] == "worker-1"
        assert claim.compile().params["pending_reason"] is None
        assert claim.compile().params["pending_reason_at"] is None

    async def test_leaves_enqueue_run_pending_behind_older_active_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = _session(_result(first=_run_row()), _result(first=_run_row()), _result(rowcount=1))
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=True))

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.THREAD_BLOCKED
        blocked = session.execute.await_args_list[-1].args[0].compile().params
        assert blocked["pending_reason"] == "thread"
        assert isinstance(blocked["pending_reason_at"], datetime)

    async def test_same_block_reason_preserves_original_timestamp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        row = _run_row(pending_reason="thread")
        session = _session(_result(first=row), _result(first=row), _result(rowcount=1))
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=True))

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.THREAD_BLOCKED
        blocked = session.execute.await_args_list[-1].args[0].compile().params
        assert "pending_reason_at" not in blocked

    async def test_non_enqueue_strategy_does_not_check_predecessors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        row = _run_row(strategy="rollback")
        session = _session(_result(first=row), _result(first=row), _result(rowcount=1))
        older = AsyncMock(return_value=True)
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", older)

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.CLAIMED
        older.assert_not_awaited()

    @pytest.mark.parametrize(
        ("row", "expected"),
        [
            (None, AdmissionOutcome.ALREADY_TAKEN),
            (_run_row(status="running"), AdmissionOutcome.ALREADY_TAKEN),
            (_run_row(claimed_by="worker-1"), AdmissionOutcome.ALREADY_TAKEN),
        ],
    )
    async def test_rejects_missing_or_taken_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
        row: object,
        expected: AdmissionOutcome,
    ) -> None:
        session = _session(_result(first=row))
        lock = AsyncMock()
        monkeypatch.setattr(run_admission, "lock_thread", lock)

        assert await try_start_run(session, RUN_ID) is expected
        lock.assert_not_awaited()

    async def test_org_capacity_is_checked_after_thread_eligibility(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
        row = _run_row(org_id=ORG, pending_reason="thread")
        session = _session(_result(first=row), _result(first=row), _result(rowcount=1))
        order: list[str] = []
        monkeypatch.setattr(
            run_admission,
            "lock_thread",
            AsyncMock(side_effect=lambda *_: order.append("thread")),
        )
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=False))
        monkeypatch.setattr(
            run_admission.run_limits,
            "lock_org",
            AsyncMock(side_effect=lambda *_: order.append("org")),
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "evaluate",
            AsyncMock(return_value=LimitDecision(org_id=ORG, active=1, limit=1)),
        )

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.ORG_AT_CAPACITY
        assert order == ["thread", "org"]
        blocked = session.execute.await_args_list[-1].args[0].compile().params
        assert blocked["pending_reason"] == "org"
        assert isinstance(blocked["pending_reason_at"], datetime)

    async def test_shadow_org_limit_records_metric_but_claims(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "shadow")
        row = _run_row(org_id=ORG)
        session = _session(_result(first=row), _result(first=row), _result(rowcount=1))
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=False))
        monkeypatch.setattr(run_admission.run_limits, "lock_org", AsyncMock())
        monkeypatch.setattr(
            run_admission.run_limits,
            "evaluate",
            AsyncMock(return_value=LimitDecision(org_id=ORG, active=1, limit=1)),
        )
        metric = MagicMock()
        monkeypatch.setattr(run_admission, "emit_metric", metric)

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.CLAIMED
        metric.assert_called_once()

    async def test_org_with_free_capacity_claims_after_both_locks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
        row = _run_row(org_id=ORG)
        session = _session(_result(first=row), _result(first=row), _result(rowcount=1))
        order: list[str] = []
        monkeypatch.setattr(
            run_admission,
            "lock_thread",
            AsyncMock(side_effect=lambda *_: order.append("thread")),
        )
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=False))
        monkeypatch.setattr(
            run_admission.run_limits,
            "lock_org",
            AsyncMock(side_effect=lambda *_: order.append("org")),
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "evaluate",
            AsyncMock(return_value=LimitDecision(org_id=ORG, active=0, limit=1)),
        )

        outcome = await try_start_run(session, RUN_ID)

        assert outcome is AdmissionOutcome.CLAIMED
        assert order == ["thread", "org"]

    async def test_conditional_claim_race_returns_already_taken(self, monkeypatch: pytest.MonkeyPatch) -> None:
        session = _session(_result(first=_run_row()), _result(first=_run_row()), _result(rowcount=0))
        monkeypatch.setattr(run_admission, "lock_thread", AsyncMock())
        monkeypatch.setattr(run_admission, "_has_older_active_run", AsyncMock(return_value=False))

        assert await try_start_run(session, RUN_ID) is AdmissionOutcome.ALREADY_TAKEN


class TestThreadLock:
    async def test_uses_transaction_scoped_thread_namespace(self) -> None:
        session = AsyncMock()

        await run_admission.lock_thread(session, THREAD_ID)

        statement = str(session.execute.await_args.args[0])
        params = session.execute.await_args.args[1]
        assert "pg_advisory_xact_lock" in statement
        assert params == {
            "namespace": run_admission.THREAD_ADVISORY_LOCK_NAMESPACE,
            "thread_id": THREAD_ID,
        }


class TestPredecessorQuery:
    async def test_only_pending_and_running_older_rows_block(self) -> None:
        session = AsyncMock()
        session.scalar = AsyncMock(return_value=False)
        state = run_admission._AdmissionState(
            thread_id=THREAD_ID,
            org_id=None,
            status="pending",
            claimed_by=None,
            multitask_strategy="enqueue",
            queue_position=2,
            pending_reason=None,
        )

        assert not await run_admission._has_older_active_run(session, state)

        statement = str(session.scalar.await_args.args[0])
        assert "runs.queue_position <" in statement
        assert "runs.status IN" in statement


class TestFindPromotableRuns:
    async def test_excludes_saturated_orgs_inside_bounded_scan(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
        monkeypatch.setattr(settings.run_limits, "ORG_MAX_CONCURRENT_RUNS", 1)
        session = _session(_result(rows=[("eligible", "org-2", 11, True)]))
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={ORG: 1}),
        )

        result = await run_admission.find_promotable_runs(session, batch_size=1)

        assert result == ["eligible"]
        session.execute.assert_awaited_once()
        params = session.execute.await_args.args[1]
        assert params["scan_limit"] == 10
        assert params["after_queue_position"] == 0
        assert params["high_water_position"] == 100
        statement = str(session.execute.await_args.args[0])
        assert "row_number" not in statement
        bounded_page = statement.split("ORDER BY candidate.queue_position", 1)[0]
        assert "pending_reason IS NOT NULL" not in bounded_page
        assert run_admission._promotion_cursor == 11

    async def test_rotating_cursor_skips_bounded_blocked_pages(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
        monkeypatch.setattr(settings.run_limits, "ORG_MAX_CONCURRENT_RUNS", 1)
        session = _session(
            _result(rows=[("full", ORG, 10, True)]),
            _result(rows=[("eligible", "org-2", 20, True)]),
            high_water=20,
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={ORG: 1}),
        )

        assert await run_admission.find_promotable_runs(session, batch_size=1) == []
        assert await run_admission.find_promotable_runs(session, batch_size=1) == ["eligible"]
        assert [call.args[1]["after_queue_position"] for call in session.execute.await_args_list] == [0, 10]
        assert run_admission._promotion_cursor == 0
        assert run_admission._promotion_high_water is None

    async def test_high_water_wrap_revisits_skipped_rows_while_tail_grows(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = _session(
            _result(rows=[("blocked", ORG, 10, False)]),
            _result(rows=[("cycle-end", ORG, 20, False)]),
            _result(rows=[("now-eligible", ORG, 10, True)]),
            high_water=20,
        )
        # A new cycle captures a newer tail only after reaching the old bound.
        session.scalar = AsyncMock(side_effect=[20, 30])
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={}),
        )

        assert await run_admission.find_promotable_runs(session, batch_size=1) == []
        assert await run_admission.find_promotable_runs(session, batch_size=1) == []
        assert await run_admission.find_promotable_runs(session, batch_size=1) == ["now-eligible"]

        params = [call.args[1] for call in session.execute.await_args_list]
        assert [item["after_queue_position"] for item in params] == [0, 10, 0]
        assert [item["high_water_position"] for item in params] == [20, 20, 30]

    async def test_selects_enqueue_thread_heads_and_non_enqueue_runs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = _session(
            _result(
                rows=[
                    ("head-a", ORG, 1, True),
                    ("head-b", "org-2", 2, True),
                    ("rollback", None, 3, True),
                ]
            )
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={}),
        )

        result = await run_admission.find_promotable_runs(session, batch_size=10)

        assert result == ["head-a", "head-b", "rollback"]
        statement = str(session.execute.await_args.args[0])
        assert "NOT EXISTS" in statement
        assert "multitask_strategy" in statement
        assert "queue_position" in statement
        assert "pending_reason" in statement

    async def test_projects_org_capacity_and_preserves_cross_org_fairness(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
        monkeypatch.setattr(settings.run_limits, "ORG_MAX_CONCURRENT_RUNS", 2)
        session = _session(
            _result(
                rows=[
                    ("org-1-first", ORG, 1, True),
                    ("org-2-first", "org-2", 2, True),
                    ("org-1-second", ORG, 3, True),
                ]
            )
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={ORG: 1}),
        )

        result = await run_admission.find_promotable_runs(session, batch_size=10)

        assert result == ["org-1-first", "org-2-first"]

    async def test_limits_batch_and_recovers_stale_unscoped_runs(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session = _session(
            _result(rows=[("stale-unscoped", None, 1, True), ("other-thread", None, 2, True)])
        )
        monkeypatch.setattr(
            run_admission.run_limits,
            "active_counts_by_org",
            AsyncMock(return_value={}),
        )

        result = await run_admission.find_promotable_runs(session, batch_size=1)

        assert result == ["stale-unscoped"]
        params = session.execute.await_args.args[1]
        assert params["stuck_before"] < datetime.now(params["stuck_before"].tzinfo)
