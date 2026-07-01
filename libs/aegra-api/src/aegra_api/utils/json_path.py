"""Tiny JSON-path parser/navigator for thread-search `extract` projection.

Supports dot keys and bracket integer indexing including negatives, e.g.
``messages[-1].content`` -> ``["messages", -1, "content"]``. No wildcards or
filters by design (see aegra issue #330).
"""

import re
from collections.abc import Sequence
from typing import Any

_INT_RE = re.compile(r"-?\d+")


def parse_path(path: str) -> list[str | int]:
    """Parse a dotted/bracketed path into ordered segments.

    Raises ValueError on malformed input; the request model surfaces that as a
    422 so callers fail loudly instead of silently matching nothing.
    """
    if not path or not isinstance(path, str):
        raise ValueError("path must be a non-empty string")

    segments: list[str | int] = []
    i = 0
    n = len(path)
    expect_key = True
    while i < n:
        ch = path[i]
        if ch == ".":
            if expect_key:
                raise ValueError(f"misplaced '.' in path: {path!r}")
            expect_key = True
            i += 1
        elif ch == "[":
            if expect_key:
                raise ValueError(f"misplaced '[' in path: {path!r}")
            end = path.find("]", i)
            if end == -1:
                raise ValueError(f"unclosed '[' in path: {path!r}")
            inner = path[i + 1 : end]
            if not _INT_RE.fullmatch(inner):
                raise ValueError(f"invalid index '{inner}' in path: {path!r}")
            segments.append(int(inner))
            i = end + 1
        else:
            if not expect_key:
                raise ValueError(f"missing separator in path: {path!r}")
            j = i
            while j < n and path[j] not in ".[":
                j += 1
            segments.append(path[i:j])
            expect_key = False
            i = j

    if expect_key:
        raise ValueError(f"path ends with a separator: {path!r}")
    return segments


def extract_value(data: Any, segments: Sequence[str | int]) -> Any:
    """Walk ``segments`` over ``data``; missing key / out-of-range / type
    mismatch resolves to None rather than raising (per issue #330)."""
    current = data
    for seg in segments:
        if isinstance(seg, int):
            if not isinstance(current, (list, tuple)):
                return None
            try:
                current = current[seg]
            except IndexError:
                return None
        else:
            if not isinstance(current, dict) or seg not in current:
                return None
            current = current[seg]
    return current
