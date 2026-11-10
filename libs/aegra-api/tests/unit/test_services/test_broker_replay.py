"""Unit tests for in-memory broker replay functionality"""

import pytest

from aegra_api.services.broker import RunBroker


class TestRunBrokerReplay:
    """Test RunBroker replay buffer"""

    def _make_broker(self, run_id: str = "run-123") -> RunBroker:
        return RunBroker(run_id)

    @pytest.mark.asyncio
    async def test_put_stores_in_replay_buffer(self) -> None:
        """Test that put() stores events in the replay buffer by default"""
        broker = self._make_broker()

        await broker.put("evt-1", ("updates", {"a": 1}))
        await broker.put("evt-2", ("updates", {"a": 2}))

        events = await broker.replay(None)
        assert len(events) == 2
        assert events[0] == ("evt-1", ("updates", {"a": 1}))
        assert events[1] == ("evt-2", ("updates", {"a": 2}))

    @pytest.mark.asyncio
    async def test_put_non_resumable_skips_buffer(self) -> None:
        """Test that put() with resumable=False skips the replay buffer"""
        broker = self._make_broker()

        await broker.put("evt-1", ("updates", {"a": 1}), resumable=True)
        await broker.put("evt-2", ("updates", {"a": 2}), resumable=False)
        await broker.put("evt-3", ("updates", {"a": 3}), resumable=True)

        events = await broker.replay(None)
        assert len(events) == 2
        assert events[0] == ("evt-1", ("updates", {"a": 1}))
        assert events[1] == ("evt-3", ("updates", {"a": 3}))

    @pytest.mark.asyncio
    async def test_replay_all_when_no_last_event_id(self) -> None:
        broker = self._make_broker()

        await broker.put("evt-1", ("updates", {"a": 1}))
        await broker.put("evt-2", ("updates", {"a": 2}))

        events = await broker.replay(None)
        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_replay_after_last_event_id(self) -> None:
        broker = self._make_broker()

        await broker.put("evt-1", ("updates", {"a": 1}))
        await broker.put("evt-2", ("updates", {"a": 2}))
        await broker.put("evt-3", ("updates", {"a": 3}))

        events = await broker.replay("evt-1")
        assert len(events) == 2
        assert events[0] == ("evt-2", ("updates", {"a": 2}))
        assert events[1] == ("evt-3", ("updates", {"a": 3}))

    @pytest.mark.asyncio
    async def test_replay_from_last_event_returns_empty(self) -> None:
        """Test replay returns empty when last_event_id is the final event"""
        broker = self._make_broker()

        await broker.put("evt-1", ("values", {"a": 1}))
        await broker.put("evt-2", ("values", {"a": 2}))

        events = await broker.replay("evt-2")
        assert events == []

    @pytest.mark.asyncio
    async def test_replay_unknown_last_event_id_returns_all(self) -> None:
        """Test replay returns all events when last_event_id is not found"""
        broker = self._make_broker()

        await broker.put("evt-1", ("values", {"a": 1}))

        events = await broker.replay("evt-999")
        assert len(events) == 1
        assert events[0] == ("evt-1", ("values", {"a": 1}))

    @pytest.mark.asyncio
    async def test_replay_empty_buffer(self) -> None:
        broker = self._make_broker()

        events = await broker.replay(None)
        assert events == []

    @pytest.mark.asyncio
    async def test_replay_empty_buffer_with_last_event_id(self) -> None:
        broker = self._make_broker()

        events = await broker.replay("evt-1")
        assert events == []

    @pytest.mark.asyncio
    async def test_end_event_stored_in_replay_buffer(self) -> None:
        """Test that end events are also stored for replay"""
        broker = self._make_broker()

        await broker.put("evt-1", ("values", {"a": 1}))
        await broker.put("evt-2", ("end", {"status": "success"}))

        events = await broker.replay(None)
        assert len(events) == 2
        assert events[1] == ("evt-2", ("end", {"status": "success"}))


class TestRunBrokerLatestValuesOnly:
    """The replay buffer keeps only the newest legacy values event per namespace."""

    def _make_broker(self, run_id: str = "run-123") -> RunBroker:
        return RunBroker(run_id)

    @pytest.mark.asyncio
    async def test_newer_values_supersedes_older(self) -> None:
        broker = self._make_broker()

        await broker.put("run-123_event_1", ("values", {"step": 1}))
        await broker.put("run-123_event_2", ("messages", [{"content": "hi"}]))
        await broker.put("run-123_event_3", ("updates", {"node": {}}))
        await broker.put("run-123_event_4", ("values", {"step": 2}))

        events = await broker.replay(None)
        assert events == [
            ("run-123_event_2", ("messages", [{"content": "hi"}])),
            ("run-123_event_3", ("updates", {"node": {}})),
            ("run-123_event_4", ("values", {"step": 2})),
        ]

    @pytest.mark.asyncio
    async def test_subgraph_values_kept_per_namespace(self) -> None:
        broker = self._make_broker()

        await broker.put("run-123_event_1", ("values", {"root": 1}))
        await broker.put("run-123_event_2", ("values|sub:1", {"sub": 1}))
        await broker.put("run-123_event_3", ("values|sub:1", {"sub": 2}))

        events = await broker.replay(None)
        assert events == [
            ("run-123_event_1", ("values", {"root": 1})),
            ("run-123_event_3", ("values|sub:1", {"sub": 2})),
        ]

    @pytest.mark.asyncio
    async def test_protocol_v2_values_events_are_all_kept(self) -> None:
        """v2 sessions count buffered events for their seq cursor."""
        broker = self._make_broker()
        first = {"type": "event", "method": "values", "params": {"namespace": [], "data": {"a": 1}}}
        second = {"type": "event", "method": "values", "params": {"namespace": [], "data": {"a": 2}}}

        await broker.put("run-123_event_1", ("values", first))
        await broker.put("run-123_event_2", ("values", second))

        assert len(await broker.replay(None)) == 2

    @pytest.mark.asyncio
    async def test_non_resumable_values_does_not_evict_buffered_one(self) -> None:
        broker = self._make_broker()

        await broker.put("run-123_event_1", ("values", {"step": 1}))
        await broker.put("run-123_event_2", ("values", {"step": 2}), resumable=False)

        assert await broker.replay(None) == [("run-123_event_1", ("values", {"step": 1}))]

    @pytest.mark.asyncio
    async def test_resume_after_superseded_event_uses_sequence(self) -> None:
        """A client whose last event was a since-removed values event resumes after it."""
        broker = self._make_broker()

        await broker.put("run-123_event_1", ("values", {"step": 1}))
        await broker.put("run-123_event_2", ("messages", [{"content": "a"}]))
        await broker.put("run-123_event_3", ("values", {"step": 2}))
        await broker.put("run-123_event_4", ("end", {"status": "success"}))

        events = await broker.replay("run-123_event_1")
        assert events == [
            ("run-123_event_2", ("messages", [{"content": "a"}])),
            ("run-123_event_3", ("values", {"step": 2})),
            ("run-123_event_4", ("end", {"status": "success"})),
        ]

    @pytest.mark.asyncio
    async def test_unknown_id_of_other_run_returns_all(self) -> None:
        broker = self._make_broker()

        await broker.put("run-123_event_1", ("messages", []))
        await broker.put("run-123_event_2", ("end", {"status": "success"}))

        assert len(await broker.replay("run-999_event_1")) == 2
        assert len(await broker.replay("run-123_event_x")) == 2
