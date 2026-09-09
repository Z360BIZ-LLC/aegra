"""Two toy graphs for the thread-TTL keep_latest E2E.

They are identical except for how `messages` is stored:

* ``plain_graph``   — ``add_messages``, the classic reducer. Every checkpoint
  materialises the whole message list, so compaction is lossless.
* ``delta_graph``   — ``DeltaChannel``, what deepagents puts `messages` on.
  Only a sentinel is stored per step plus a ``_DeltaSnapshot`` every
  ``snapshot_frequency`` updates; the value is reconstructed by walking the
  parent chain. Compacting it away silently empties the conversation.

No model calls: each node appends a deterministic AI message so the test can
assert on exact content.
"""

from typing import Annotated, Any, TypedDict

from langchain_core.messages import AIMessage, AnyMessage
from langgraph.channels.delta import DeltaChannel
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages


def _delta_reducer(state: list[Any] | None, writes: list[Any]) -> list[Any]:
    """Batching-invariant append, as DeltaChannel requires."""
    current = list(state or [])
    for write in writes:
        if isinstance(write, list):
            current.extend(write)
        else:
            current.append(write)
    return current


class PlainState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    turns: int


class DeltaState(TypedDict):
    messages: Annotated[list[AnyMessage], DeltaChannel(_delta_reducer, snapshot_frequency=50)]
    turns: int


def _step_a(state: dict[str, Any]) -> dict[str, Any]:
    n = state.get("turns", 0) + 1
    return {"messages": [AIMessage(content=f"step-a-{n}")], "turns": n}


def _step_b(state: dict[str, Any]) -> dict[str, Any]:
    n = state.get("turns", 0) + 1
    return {"messages": [AIMessage(content=f"step-b-{n}")], "turns": n}


def _step_c(state: dict[str, Any]) -> dict[str, Any]:
    n = state.get("turns", 0) + 1
    return {"messages": [AIMessage(content=f"step-c-{n}")], "turns": n}


def _build(state_schema: type) -> Any:
    """Three sequential nodes, so one run produces several checkpoints."""
    builder = StateGraph(state_schema)
    builder.add_node("a", _step_a)
    builder.add_node("b", _step_b)
    builder.add_node("c", _step_c)
    builder.add_edge(START, "a")
    builder.add_edge("a", "b")
    builder.add_edge("b", "c")
    builder.add_edge("c", END)
    return builder.compile()


plain_graph = _build(PlainState)
delta_graph = _build(DeltaState)
