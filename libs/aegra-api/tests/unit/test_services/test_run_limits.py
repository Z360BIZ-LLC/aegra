"""Unit tests for per-organization run limits."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services import run_limits
from aegra_api.services.run_limits import (
    ClaimOutcome,
    LimitDecision,
    find_promotable_runs,
    limit_for,
    max_limit,
    resolve_org_id,
    try_start_run,
)
from aegra_api.settings import settings

ORG = "7-z360"


@pytest.fixture
def limits(monkeypatch: pytest.MonkeyPatch) -> None:
    """Enforce limits with a ceiling of 2 and no overrides."""
    monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "enforce")
    monkeypatch.setattr(settings.run_limits, "ORG_MAX_CONCURRENT_RUNS", 2)
    monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_OVERRIDES", "")


def _result(*, first: object = None, rowcount: int = 1, rows: list | None = None) -> MagicMock:
    """Build a mock SQLAlchemy result."""
    result = MagicMock()
    result.first.return_value = first
    result.rowcount = rowcount
    result.all.return_value = rows or []
    return result


def _session(*, execute: list, scalar: object = 0) -> AsyncMock:
    """Build a mock session whose execute() returns the given results in order."""
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=execute)
    session.scalar = AsyncMock(return_value=scalar)
    return session


class TestResolveOrgId:
    def test_reads_org_id_from_configurable(self) -> None:
        assert resolve_org_id({"configurable": {"org_id": ORG}}) == ORG

    def test_falls_back_to_run_metadata(self) -> None:
        assert resolve_org_id({"configurable": {}}, {"org_id": ORG}) == ORG

    def test_configurable_wins_over_metadata(self) -> None:
        assert resolve_org_id({"configurable": {"org_id": ORG}}, {"org_id": "other"}) == ORG

    def test_returns_none_when_absent(self) -> None:
        assert resolve_org_id({"configurable": {}}, {}) is None

    def test_returns_none_for_missing_config(self) -> None:
        assert resolve_org_id(None, None) is None

    def test_returns_none_for_blank_value(self) -> None:
        assert resolve_org_id({"configurable": {"org_id": "   "}}) is None

    def test_strips_surrounding_whitespace(self) -> None:
        assert resolve_org_id({"configurable": {"org_id": f"  {ORG} "}}) == ORG

    def test_coerces_integer_org_id(self) -> None:
        assert resolve_org_id({"configurable": {"org_id": 7}}) == "7"

    def test_ignores_boolean_org_id(self) -> None:
        """bool is an int subclass — True must not become the org "1"."""
        assert resolve_org_id({"configurable": {"org_id": True}}) is None

    def test_ignores_non_mapping_configurable(self) -> None:
        assert resolve_org_id({"configurable": "nope"}) is None

    def test_honours_configured_key_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_ID_CONFIG_KEY", "tenant")
        assert resolve_org_id({"configurable": {"tenant": ORG}}) == ORG


class TestLimitLookup:
    def test_uses_default_when_no_override(self, limits: None) -> None:
        assert limit_for(ORG) == 2

    def test_override_wins(self, monkeypatch: pytest.MonkeyPatch, limits: None) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_OVERRIDES", f'{{"{ORG}": 40}}')
        assert limit_for(ORG) == 40
        assert limit_for("other-org") == 2

    def test_max_limit_spans_default_and_overrides(self, monkeypatch: pytest.MonkeyPatch, limits: None) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_OVERRIDES", f'{{"{ORG}": 40}}')
        assert max_limit() == 40

    def test_max_limit_without_overrides_is_the_default(self, limits: None) -> None:
        assert max_limit() == 2


class TestLimitDecision:
    def test_at_capacity_when_active_reaches_limit(self) -> None:
        assert LimitDecision(org_id=ORG, active=2, limit=2).at_capacity

    def test_not_at_capacity_below_limit(self) -> None:
        assert not LimitDecision(org_id=ORG, active=1, limit=2).at_capacity


class TestTryStartRun:
    async def test_claims_with_a_single_update_when_limits_are_off(self) -> None:
        """The default path must not pay for a capacity lookup."""
        session = _session(execute=[_result(rowcount=1)])

        assert await try_start_run(session, "run-1") is ClaimOutcome.CLAIMED
        assert session.execute.await_count == 1

    async def test_returns_already_taken_when_update_matches_nothing(self) -> None:
        session = _session(execute=[_result(rowcount=0)])

        assert await try_start_run(session, "run-1") is ClaimOutcome.ALREADY_TAKEN

    async def test_claims_when_org_has_free_capacity(self, limits: None) -> None:
        session = _session(
            execute=[_result(first=(ORG, "pending", None)), _result(), _result(rowcount=1)],
            scalar=1,
        )

        assert await try_start_run(session, "run-1") is ClaimOutcome.CLAIMED

    async def test_returns_at_capacity_when_org_is_full(self, limits: None) -> None:
        session = _session(execute=[_result(first=(ORG, "pending", None)), _result()], scalar=2)

        assert await try_start_run(session, "run-1") is ClaimOutcome.AT_CAPACITY

    async def test_full_org_still_claims_in_shadow_mode(self, monkeypatch: pytest.MonkeyPatch, limits: None) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_RUN_LIMIT_MODE", "shadow")
        session = _session(
            execute=[_result(first=(ORG, "pending", None)), _result(), _result(rowcount=1)],
            scalar=99,
        )

        assert await try_start_run(session, "run-1") is ClaimOutcome.CLAIMED

    async def test_run_without_org_is_exempt(self, limits: None) -> None:
        """A run with no tenant is never gated, and never takes an org lock."""
        session = _session(execute=[_result(first=(None, "pending", None)), _result(rowcount=1)])

        assert await try_start_run(session, "run-1") is ClaimOutcome.CLAIMED

    async def test_returns_already_taken_when_run_is_gone(self, limits: None) -> None:
        session = _session(execute=[_result(first=None)])

        assert await try_start_run(session, "run-1") is ClaimOutcome.ALREADY_TAKEN

    async def test_returns_already_taken_when_run_is_no_longer_pending(self, limits: None) -> None:
        session = _session(execute=[_result(first=(ORG, "running", "worker-1"))])

        assert await try_start_run(session, "run-1") is ClaimOutcome.ALREADY_TAKEN


class TestFindPromotableRuns:
    async def test_skips_orgs_that_are_at_capacity(self, limits: None) -> None:
        session = _session(
            execute=[
                _result(rows=[("full-org", 2), ("free-org", 1)]),
                _result(rows=[("run-full", "full-org"), ("run-free", "free-org")]),
            ]
        )

        assert await find_promotable_runs(session, batch_size=10) == ["run-free"]

    async def test_counts_promotions_against_remaining_capacity(self, limits: None) -> None:
        """Two free slots means two runs promoted, not the whole backlog."""
        session = _session(
            execute=[
                _result(rows=[]),
                _result(rows=[("run-1", ORG), ("run-2", ORG), ("run-3", ORG)]),
            ]
        )

        assert await find_promotable_runs(session, batch_size=10) == ["run-1", "run-2"]

    async def test_stops_at_batch_size(self, monkeypatch: pytest.MonkeyPatch, limits: None) -> None:
        monkeypatch.setattr(settings.run_limits, "ORG_MAX_CONCURRENT_RUNS", 50)
        session = _session(
            execute=[
                _result(rows=[]),
                _result(rows=[("run-1", ORG), ("run-2", ORG), ("run-3", ORG)]),
            ]
        )

        assert await find_promotable_runs(session, batch_size=2) == ["run-1", "run-2"]

    async def test_returns_empty_when_nothing_is_queued(self, limits: None) -> None:
        session = _session(execute=[_result(rows=[]), _result(rows=[])])

        assert await find_promotable_runs(session, batch_size=10) == []


class TestStaleRunsAreNotCounted:
    async def test_count_query_excludes_expired_leases_and_old_rows(self, limits: None) -> None:
        """The count must filter on lease expiry and age, not just status."""
        session = _session(execute=[], scalar=0)

        await run_limits.count_active_runs(session, ORG)

        rendered = str(session.scalar.await_args.args[0])
        assert "lease_expires_at" in rendered
        assert "created_at" in rendered
