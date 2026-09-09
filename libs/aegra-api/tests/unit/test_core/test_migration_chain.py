"""The alembic revision graph must stay linear with exactly one head.

This fork merges from upstream aegra, and both sides add migrations. When both
branch off the same parent the result is two heads, alembic refuses to run with
"Multiple head revisions are present for given argument 'head'", and the server
dies during lifespan startup — every pod crashloops on deploy. Nothing else in
the suite catches it, because a migration file is valid Python either way.
"""

from __future__ import annotations

import re
from pathlib import Path

_VERSIONS_DIR = Path(__file__).resolve().parents[3] / "alembic" / "versions"

# Match only the assignments, never the same tokens inside a docstring.
_REVISION_RE = re.compile(r"^revision(?::\s*str)?\s*=\s*[\"']([^\"']+)[\"']", re.MULTILINE)
_DOWN_RE = re.compile(r"^down_revision(?::\s*str\s*\|\s*None)?\s*=\s*(?:[\"']([^\"']+)[\"']|None)", re.MULTILINE)


def _load_chain() -> tuple[dict[str, str | None], dict[str, str]]:
    """Return (revision -> down_revision, revision -> filename)."""
    down_by_rev: dict[str, str | None] = {}
    file_by_rev: dict[str, str] = {}
    for path in sorted(_VERSIONS_DIR.glob("*.py")):
        text = path.read_text()
        rev_match = _REVISION_RE.search(text)
        assert rev_match is not None, f"{path.name} declares no revision"
        rev = rev_match.group(1)
        assert rev not in file_by_rev, f"duplicate revision {rev}: {path.name} and {file_by_rev[rev]}"
        down_match = _DOWN_RE.search(text)
        assert down_match is not None, f"{path.name} declares no down_revision"
        down_by_rev[rev] = down_match.group(1)
        file_by_rev[rev] = path.name
    return down_by_rev, file_by_rev


def test_exactly_one_head() -> None:
    """Two heads means `alembic upgrade head` fails and the server won't boot."""
    down_by_rev, file_by_rev = _load_chain()
    parents = {down for down in down_by_rev.values() if down is not None}
    heads = sorted(rev for rev in down_by_rev if rev not in parents)
    named = [f"{rev} ({file_by_rev[rev]})" for rev in heads]
    assert len(heads) == 1, f"expected a single alembic head, found {len(heads)}: {named}"


def test_exactly_one_base() -> None:
    down_by_rev, file_by_rev = _load_chain()
    bases = sorted(rev for rev, down in down_by_rev.items() if down is None)
    named = [f"{rev} ({file_by_rev[rev]})" for rev in bases]
    assert len(bases) == 1, f"expected a single alembic base, found {len(bases)}: {named}"


def test_every_down_revision_exists() -> None:
    """A dangling down_revision breaks the walk just as badly as a second head."""
    down_by_rev, file_by_rev = _load_chain()
    for rev, down in down_by_rev.items():
        if down is not None:
            assert down in down_by_rev, f"{file_by_rev[rev]} points at unknown down_revision {down!r}"


def test_chain_has_no_cycles_and_covers_every_revision() -> None:
    down_by_rev, _ = _load_chain()
    parents = {down for down in down_by_rev.values() if down is not None}
    head = next(rev for rev in down_by_rev if rev not in parents)

    walked: list[str] = []
    seen: set[str] = set()
    cursor: str | None = head
    while cursor is not None:
        assert cursor not in seen, f"cycle in alembic chain at {cursor}"
        seen.add(cursor)
        walked.append(cursor)
        cursor = down_by_rev[cursor]

    assert len(walked) == len(down_by_rev), (
        f"walking head->base reached {len(walked)} of {len(down_by_rev)} revisions; "
        "the chain is branched rather than linear"
    )
