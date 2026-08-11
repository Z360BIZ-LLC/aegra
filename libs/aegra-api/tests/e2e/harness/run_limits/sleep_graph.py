"""Graph that only sleeps, so run-limit queueing is observable without an LLM.

Used by the ``e2e-run-limits`` harness: a run must occupy its org's capacity
long enough for a second run to be seen waiting behind it.
"""

import asyncio
import os
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph


class SleepState(TypedDict, total=False):
    slept: float


async def sleep_node(_state: SleepState) -> dict[str, Any]:
    """Hold the run open for SLEEP_GRAPH_SECONDS."""
    duration = float(os.getenv("SLEEP_GRAPH_SECONDS", "6"))
    await asyncio.sleep(duration)
    return {"slept": duration}


builder = StateGraph(SleepState)
builder.add_node("sleep", sleep_node)
builder.add_edge(START, "sleep")
builder.add_edge("sleep", END)
graph = builder.compile()
