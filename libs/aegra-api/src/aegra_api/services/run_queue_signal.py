"""In-process wakeup hint for the durable PostgreSQL run queue."""

import asyncio


class RunQueueSignal:
    """Wake the local promoter without making delivery correctness depend on it."""

    def __init__(self) -> None:
        self._event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def _current_event(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        if self._event is None or self._loop is not loop:
            self._event = asyncio.Event()
            self._loop = loop
        return self._event

    def notify(self) -> None:
        """Request a promotion pass."""
        self._current_event().set()

    def clear(self) -> None:
        """Acknowledge signals observed before the next promotion pass."""
        self._current_event().clear()

    async def wait(self, timeout: float) -> bool:
        """Wait for a signal, returning false when periodic fallback is due."""
        try:
            await asyncio.wait_for(self._current_event().wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True


run_queue_signal = RunQueueSignal()
