"""In-process wakeup hint for the durable PostgreSQL run queue."""

import asyncio


class RunQueueSignal:
    """Wake the local promoter without making delivery correctness depend on it."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def notify(self) -> None:
        """Request a promotion pass."""
        self._event.set()

    def clear(self) -> None:
        """Acknowledge signals observed before the next promotion pass."""
        self._event.clear()

    async def wait(self, timeout: float) -> bool:
        """Wait for a signal, returning false when periodic fallback is due."""
        try:
            await asyncio.wait_for(self._event.wait(), timeout=timeout)
        except TimeoutError:
            return False
        return True


run_queue_signal = RunQueueSignal()
