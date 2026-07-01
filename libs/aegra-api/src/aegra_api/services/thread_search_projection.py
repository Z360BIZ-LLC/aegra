"""Projection of `select` / `extract` onto thread-search results.

Reads the **materialized** thread state — the `thread.values` / `thread.interrupts`
columns populated from `aget_state` on run completion + state update. That is the
graph's logical state, so it matches `GET /threads/{id}/state` for every agent,
including middleware agents (e.g. deep_agent) whose messages live in
`checkpoint_writes` and can't be reconstructed from raw checkpoint blobs without
the graph.

Because the state is real JSONB, `extract` paths are resolved **in Postgres** via
`jsonb_path_query_first` — the DB returns only the referenced scalars/subtrees, not
the whole state blob. The full column is fetched only when `select` asks for the
entire `values`/`interrupts`. Nothing is read at all for metadata-only or
base-field requests.
"""

from collections.abc import Sequence
from typing import Any

import structlog
from sqlalchemy import cast, func, literal, select, type_coerce
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import UserDefinedType

from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.orm import _get_session_maker
from aegra_api.utils.json_path import extract_value, parse_path

logger = structlog.getLogger(__name__)


class _JsonPath(UserDefinedType):
    """Postgres ``jsonpath`` type so extract paths bind as CAST(:p AS jsonpath)."""

    cache_ok = True

    def get_col_spec(self, **_kw: Any) -> str:
        return "JSONPATH"


def _to_jsonpath(segments: Sequence[str | int]) -> str:
    """Translate parsed path segments into a Postgres jsonpath string.

    Keys are always quoted (so hyphenated task ids etc. are safe); negative
    indices map to ``[last]`` / ``[last-N]``. Uses ``strict`` mode so the DB
    matches LangGraph's Python resolver: index only on arrays, key only on
    objects — any type mismatch resolves to null (via silent=true at the call).
    """
    parts = ["strict $"]
    for seg in segments:
        if isinstance(seg, int):
            if seg >= 0:
                parts.append(f"[{seg}]")
            elif seg == -1:
                parts.append("[last]")
            else:
                parts.append(f"[last{seg + 1}]")
        else:
            escaped = seg.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'."{escaped}"')
    return "".join(parts)


class ThreadSearchProjectionService:
    """Builds projected result dicts for POST /threads/search."""

    async def project(
        self,
        base_dicts: list[dict[str, Any]],
        *,
        select: list[str] | None,
        extract: dict[str, str] | None,
    ) -> list[dict[str, Any]]:
        """Project base thread dicts down to `select` and attach `extracted`.

        ``base_dicts`` are already-serialized full thread rows (JSON-safe), which
        keeps this decoupled from the ORM/handler.
        """
        select_set = set(select) if select is not None else None
        # Parse leniently: a malformed sub-path (root already validated) resolves
        # to null rather than erroring — matches LangGraph's resolver.
        parsed: dict[str, list[str | int] | None] = {}
        for alias, path in (extract or {}).items():
            try:
                parsed[alias] = parse_path(path)
            except ValueError:
                parsed[alias] = None
        wants_values = bool(select_set and "values" in select_set)
        wants_interrupts = bool(select_set and "interrupts" in select_set)

        # Paths resolved in SQL — only those whose full column isn't already fetched.
        sql_value_paths = {} if wants_values else {a: s for a, s in parsed.items() if s and s[0] == "values"}
        sql_interrupt_paths = (
            {} if wants_interrupts else {a: s for a, s in parsed.items() if s and s[0] == "interrupts"}
        )

        data: dict[str, dict[str, Any]] = {}
        if wants_values or wants_interrupts or sql_value_paths or sql_interrupt_paths:
            thread_ids = [base["thread_id"] for base in base_dicts]
            data = await self._fetch_projection(
                thread_ids,
                want_values=wants_values,
                want_interrupts=wants_interrupts,
                value_paths=sql_value_paths,
                interrupt_paths=sql_interrupt_paths,
            )

        results: list[dict[str, Any]] = []
        for base in base_dicts:
            row = data.get(base["thread_id"], {})
            out = dict(base)
            if wants_values:
                out["values"] = row.get("values") or {}
            if wants_interrupts:
                out["interrupts"] = row.get("interrupts") or {}
            if select_set is not None:
                out = {key: value for key, value in out.items() if key in select_set}
            if parsed:
                out["extracted"] = self._assemble_extracted(
                    parsed, base.get("metadata") or {}, row, wants_values, wants_interrupts
                )
            results.append(out)
        return results

    @staticmethod
    def _assemble_extracted(
        parsed: dict[str, list[str | int] | None],
        metadata: dict[str, Any],
        row: dict[str, Any],
        wants_values: bool,
        wants_interrupts: bool,
    ) -> dict[str, Any]:
        extracted: dict[str, Any] = {}
        sql_extracted = row.get("extracted", {})
        for alias, segments in parsed.items():
            if not segments:
                extracted[alias] = None  # unparseable path (e.g. bad index) -> null
                continue
            root = segments[0]
            if root == "metadata":
                extracted[alias] = extract_value(metadata, segments[1:])
            elif root == "values" and wants_values:
                extracted[alias] = extract_value(row.get("values") or {}, segments[1:])
            elif root == "interrupts" and wants_interrupts:
                extracted[alias] = extract_value(row.get("interrupts") or {}, segments[1:])
            else:
                extracted[alias] = sql_extracted.get(alias)
        return extracted

    async def _fetch_projection(
        self,
        thread_ids: list[str],
        *,
        want_values: bool,
        want_interrupts: bool,
        value_paths: dict[str, list[str | int]],
        interrupt_paths: dict[str, list[str | int]],
    ) -> dict[str, dict[str, Any]]:
        """One batched query: full columns only when selected, plus per-path
        jsonb_path_query_first for each extract path (DB returns just those)."""
        if not thread_ids:
            return {}

        columns: list[Any] = [ThreadORM.thread_id]
        if want_values:
            columns.append(ThreadORM.values_json)
        if want_interrupts:
            columns.append(ThreadORM.interrupts_json)

        labels: dict[str, str] = {}
        for offset, (alias, segments) in enumerate(list(value_paths.items()) + list(interrupt_paths.items())):
            source = ThreadORM.values_json if alias in value_paths else ThreadORM.interrupts_json
            jsonpath = _to_jsonpath(segments[1:])
            label = f"ext{offset}"
            # vars={} + silent=true → any strict-mode type mismatch yields null
            # (never errors), matching LangGraph's resolver.
            columns.append(
                type_coerce(
                    func.jsonb_path_query_first(source, cast(jsonpath, _JsonPath()), literal({}, JSONB), True),
                    JSONB,
                ).label(label)
            )
            labels[label] = alias

        maker = _get_session_maker()
        async with maker() as session:
            result = await session.execute(select(*columns).where(ThreadORM.thread_id.in_(thread_ids)))
            out: dict[str, dict[str, Any]] = {}
            for row in result:
                mapping = row._mapping
                out[row.thread_id] = {
                    "values": row.values_json if want_values else None,
                    "interrupts": row.interrupts_json if want_interrupts else None,
                    "extracted": {alias: mapping[label] for label, alias in labels.items()},
                }
            return out


thread_search_projection = ThreadSearchProjectionService()
