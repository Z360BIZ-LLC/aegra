"""Graph that interrupts once, for checking HITL runs against run limits.

An interrupted run is terminal in Aegra's status model, so it must release
its organization's capacity — a human sitting on an approval for a day
cannot be allowed to hold a slot.
"""

from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class ApprovalState(TypedDict, total=False):
    approved: bool


async def ask_for_approval(_state: ApprovalState) -> dict[str, Any]:
    """Pause for a human decision, then record it."""
    answer = interrupt({"question": "approve?"})
    return {"approved": bool(answer)}


builder = StateGraph(ApprovalState)
builder.add_node("approval", ask_for_approval)
builder.add_edge(START, "approval")
builder.add_edge("approval", END)
graph = builder.compile()
