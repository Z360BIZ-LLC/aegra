"""Deterministic stateful graph for enqueue strategy end-to-end tests."""

import asyncio
import operator
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class QueueState(TypedDict, total=False):
    values: Annotated[list[str], operator.add]
    value: str
    delay: float
    fail: bool
    pause: bool


async def append_value(state: QueueState) -> dict[str, Any]:
    """Delay if requested, optionally fail, then append one input value."""
    await asyncio.sleep(float(state.get("delay", 0)))
    if state.get("fail", False):
        raise RuntimeError(f"requested failure for {state.get('value', 'unknown')}")
    if state.get("pause", False):
        interrupt("requested pause")
    return {"values": [state["value"]]}


builder = StateGraph(QueueState)
builder.add_node("append", append_value)
builder.add_edge(START, "append")
builder.add_edge("append", END)
graph = builder.compile()
