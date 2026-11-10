"""Abstract base classes for the broker system"""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any

from aegra_api.models.enums import RunCancellationAction


def values_replay_slot(payload: Any) -> str | None:
    """Return the replay slot a full-state ``values`` event occupies, or None.

    A ``values`` event is a complete snapshot of graph state, so each one
    supersedes every earlier one in the same namespace: ``"values"`` for the
    root graph, ``"values|<namespace>"`` for a subgraph. Replay buffers keep
    only the newest event per slot. Keeping all of them stored a full copy of
    the conversation for every step, which was most of a long run's buffer.

    Only legacy stream events qualify. A native v3 protocol event (an Agent
    Protocol v2 run) is left alone: ``ThreadEventSession`` derives its
    ``seq`` reconnect cursor by counting buffered events, so removing one
    would shift every later cursor.
    """
    if not isinstance(payload, (tuple, list)) or len(payload) < 2 or not isinstance(payload[0], str):
        return None
    mode, data = payload[0], payload[1]
    if mode != "values" and not mode.startswith("values|"):
        return None
    if isinstance(data, dict) and data.get("type") == "event" and "params" in data:
        return None
    return mode


def resume_sequence(run_id: str, last_event_id: str) -> int | None:
    """Return the sequence to resume after, or None if the id is not this run's.

    Replay first looks ``last_event_id`` up directly. This is the fallback for
    an id the buffer no longer holds, most often a ``values`` event that a newer
    one superseded: the client resumes after its sequence number instead of
    receiving the whole run again.
    """
    prefix = f"{run_id}_event_"
    if not last_event_id.startswith(prefix):
        return None
    sequence = last_event_id[len(prefix) :]
    return int(sequence) if sequence.isdigit() else None


class BaseRunBroker(ABC):
    """Abstract base class for a run-specific event broker.

    Handles both live event broadcast and replay storage for SSE reconnection.
    """

    @abstractmethod
    async def put(self, event_id: str, payload: Any, *, resumable: bool = True) -> None:
        """Publish an event to live subscribers and optionally store for replay.

        Args:
            event_id: Unique event identifier (format: {run_id}_event_{seq}).
            payload: The event data (typically a tuple like ("values", {...})).
            resumable: If True, store the event for replay on reconnect.
        """

    @abstractmethod
    def aiter(self) -> AsyncGenerator[tuple[str, Any], None]:
        """Closeable async generator yielding (event_id, payload) pairs."""
        ...

    @abstractmethod
    async def replay(self, last_event_id: str | None) -> list[tuple[str, Any]]:
        """Return stored events for replay on reconnect.

        Args:
            last_event_id: If provided, return events after this ID. An ID
                           of this run that the buffer no longer holds (a
                           superseded ``values`` event) resumes after its
                           sequence number. Any other unknown ID, or None,
                           returns all stored events.

        Returns:
            List of (event_id, payload) tuples in order.
        """

    @abstractmethod
    def mark_finished(self) -> None:
        """Mark this broker as finished."""

    @abstractmethod
    def is_finished(self) -> bool:
        """Check if this broker is finished."""


class BaseBrokerManager(ABC):
    """Abstract base class for managing multiple RunBroker instances"""

    @abstractmethod
    def get_or_create_broker(self, run_id: str) -> BaseRunBroker:
        """Get or create a broker for a run"""

    @abstractmethod
    def get_broker(self, run_id: str, *, for_replay: bool = False) -> BaseRunBroker | None:
        """Get an existing broker, or an uncached handle to remote replay data."""

    @abstractmethod
    def cleanup_broker(self, run_id: str) -> None:
        """Clean up a broker for a run"""

    @abstractmethod
    def remove_broker(self, run_id: str) -> None:
        """Remove a broker completely"""

    @abstractmethod
    async def start(self) -> None:
        """Start background tasks (cleanup loop, cancel listener, etc.)"""

    @abstractmethod
    async def stop(self) -> None:
        """Stop background tasks and release resources"""

    @abstractmethod
    async def request_cancel(
        self,
        run_id: str,
        action: RunCancellationAction = "cancel",
        *,
        emit_end_event: bool = True,
    ) -> None:
        """Request cancellation of a run.

        In single-instance mode, cancels the local asyncio task directly.
        In multi-instance mode, broadcasts via Redis pub/sub so the owning
        instance can act on it. ``emit_end_event`` is disabled when the API
        has already reconciled the database and owns terminal signaling.
        """

    @abstractmethod
    async def get_event_sequence(self, run_id: str) -> int:
        """Return the current (highest) event sequence number for a run.

        Used by signal methods (cancel, error) to generate the next event ID
        when they need to inject events into a run's stream.
        """

    async def allocate_event_id(self, run_id: str) -> str:
        """Atomically allocate the next event sequence number and return event_id.

        Default implementation uses get_event_sequence + 1 (not atomic).
        Redis backend overrides with atomic INCR.
        """
        from aegra_api.utils import generate_event_id

        counter = await self.get_event_sequence(run_id) + 1
        return generate_event_id(run_id, counter)
