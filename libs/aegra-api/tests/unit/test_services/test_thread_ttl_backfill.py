"""Unit tests for the thread_ttl backfill.

The SQL itself is exercised against a real Postgres by the e2e harness; these
cover the guard rails and the shape of what the command reports, which is what
an operator meters a large backfill by.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.services.thread_ttl_backfill import backfill_thread_ttl, count_unarmed_threads


def _session(counts: list[int], rowcount: int = 0) -> AsyncMock:
    """Session whose scalar_one() returns `counts` in order (before, after)."""
    session = AsyncMock()
    results = []
    for c in counts:
        r = MagicMock()
        r.scalar_one.return_value = c
        r.rowcount = rowcount
        results.append(r)
    insert_result = MagicMock()
    insert_result.rowcount = rowcount
    # count -> insert -> count
    session.execute = AsyncMock(side_effect=[results[0], insert_result, results[1]])
    return session


class TestValidation:
    @pytest.mark.asyncio
    async def test_rejects_unknown_strategy(self) -> None:
        with pytest.raises(ValueError, match="unknown strategy"):
            await backfill_thread_ttl(AsyncMock(), strategy="purge")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ttl", [0, -1])
    async def test_rejects_non_positive_ttl(self, ttl: float) -> None:
        with pytest.raises(ValueError, match="ttl_minutes"):
            await backfill_thread_ttl(AsyncMock(), ttl_minutes=ttl)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, -5])
    async def test_rejects_non_positive_limit(self, limit: int) -> None:
        with pytest.raises(ValueError, match="limit"):
            await backfill_thread_ttl(AsyncMock(), limit=limit)


class TestBackfill:
    @pytest.mark.asyncio
    async def test_dry_run_changes_nothing(self) -> None:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one.return_value = 4321
        session.execute = AsyncMock(return_value=result)

        stats = await backfill_thread_ttl(session, limit=1000, dry_run=True)

        assert stats == {"armed": 0, "candidates": 4321, "remaining": 4321}
        session.commit.assert_not_awaited()
        assert session.execute.await_count == 1  # the count probe only

    @pytest.mark.asyncio
    async def test_reports_armed_and_remaining(self) -> None:
        """`remaining` is what tells the operator whether to run another tranche."""
        session = _session([5000, 4000], rowcount=1000)

        stats = await backfill_thread_ttl(session, limit=1000)

        assert stats == {"armed": 1000, "candidates": 5000, "remaining": 4000}
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_passes_older_than_filter_through(self) -> None:
        session = _session([1, 0], rowcount=1)

        await backfill_thread_ttl(session, limit=10, older_than_days=30)

        for call in session.execute.await_args_list:
            assert call.args[1]["older_than_days"] == 30

    @pytest.mark.asyncio
    async def test_count_defaults_to_no_age_filter(self) -> None:
        session = AsyncMock()
        result = MagicMock()
        result.scalar_one.return_value = 7
        session.execute = AsyncMock(return_value=result)

        assert await count_unarmed_threads(session) == 7
        assert session.execute.await_args.args[1] == {"older_than_days": None}
