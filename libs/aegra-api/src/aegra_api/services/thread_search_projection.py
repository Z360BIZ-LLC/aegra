"""Projection of `select` / `extract` onto thread-search results.

State values are not queryable JSONB — they are msgpack BYTEA in
``checkpoint_blobs`` (see langgraph-checkpoint-postgres). So path extraction
happens in Python after decode. The optimisation is in *what we read*: one
batched query for the page's latest checkpoints, with the blob join pruned to
only the channels the request references, and no checkpoint read at all when a
request needs only metadata/base fields. Interrupts (SDK shape
``{task_id: [Interrupt]}``) come from the latest checkpoint's ``__interrupt__``
writes, fetched only when requested.
"""

from collections.abc import Iterable, Sequence
from typing import Any

import structlog
from psycopg.rows import dict_row

from aegra_api.core.database import db_manager
from aegra_api.core.serializers import LangGraphSerializer
from aegra_api.utils.json_path import extract_value, parse_path, top_level_channel

logger = structlog.getLogger(__name__)

# Latest root checkpoint per thread. State is reconstructed exactly like the
# checkpointer (langgraph v4): inline `checkpoint.channel_values` overlaid with
# the decoded blobs. The blob join is pruned to %(channels)s (NULL = all) so we
# only transfer/decode the (often large) channels a request actually references.
_LATEST_VALUES_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (thread_id)
           thread_id, checkpoint, checkpoint_ns, checkpoint_id
    FROM checkpoints
    WHERE checkpoint_ns = '' AND thread_id = ANY(%(ids)s::text[])
    ORDER BY thread_id, checkpoint_id DESC
)
SELECT
    l.thread_id AS thread_id,
    l.checkpoint -> 'channel_values' AS inline_values,
    (
        SELECT array_agg(ARRAY[bl.channel::bytea, bl.type::bytea, bl.blob])
        FROM jsonb_each_text(l.checkpoint -> 'channel_versions') AS cv(key, value)
        JOIN checkpoint_blobs bl
          ON bl.thread_id = l.thread_id
         AND bl.checkpoint_ns = l.checkpoint_ns
         AND bl.channel = cv.key
         AND bl.version = cv.value
        WHERE (%(channels)s::text[] IS NULL OR bl.channel = ANY(%(channels)s::text[]))
    ) AS channel_values
FROM latest l
"""

# Pending interrupts for each thread's latest checkpoint, grouped in Python into
# the SDK shape {task_id: [Interrupt]}. Interrupts live in checkpoint_writes on
# the reserved '__interrupt__' channel, not on the thread row.
_INTERRUPTS_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (thread_id)
           thread_id, checkpoint_id, checkpoint_ns
    FROM checkpoints
    WHERE checkpoint_ns = '' AND thread_id = ANY(%(ids)s::text[])
    ORDER BY thread_id, checkpoint_id DESC
)
SELECT cw.thread_id AS thread_id, cw.task_id AS task_id, cw.type AS type, cw.blob AS blob
FROM latest l
JOIN checkpoint_writes cw
  ON cw.thread_id = l.thread_id
 AND cw.checkpoint_ns = l.checkpoint_ns
 AND cw.checkpoint_id = l.checkpoint_id
 AND cw.channel = '__interrupt__'
ORDER BY cw.thread_id, cw.task_id, cw.idx
"""


class ThreadSearchProjectionService:
    """Builds projected result dicts for POST /threads/search."""

    def __init__(self) -> None:
        self.serializer = LangGraphSerializer()

    async def project(
        self,
        base_dicts: list[dict[str, Any]],
        *,
        select: list[str] | None,
        extract: dict[str, str] | None,
    ) -> list[dict[str, Any]]:
        """Project base thread dicts down to `select` and attach `extracted`.

        ``base_dicts`` are already-serialized full thread rows (JSON-safe); this
        keeps the service decoupled from the ORM/handler.
        """
        select_set = set(select) if select is not None else None
        parsed = {alias: parse_path(path) for alias, path in (extract or {}).items()}
        value_paths = {alias: segs for alias, segs in parsed.items() if segs and segs[0] == "values"}
        wants_values = bool(select_set and "values" in select_set)
        wants_interrupts = bool(select_set and "interrupts" in select_set)
        needs_interrupts = wants_interrupts or any(segs and segs[0] == "interrupts" for segs in parsed.values())

        thread_ids = [base["thread_id"] for base in base_dicts]
        decoded: dict[str, dict[str, Any]] = {}
        if wants_values or value_paths:
            channels = self._needed_channels(wants_values=wants_values, value_segments=value_paths.values())
            decoded = await self._fetch_values(thread_ids, channels)
        interrupts: dict[str, dict[str, Any]] = {}
        if needs_interrupts:
            interrupts = await self._fetch_interrupts(thread_ids)

        results: list[dict[str, Any]] = []
        for base in base_dicts:
            tid = base["thread_id"]
            values = decoded.get(tid, {})
            thread_interrupts = interrupts.get(tid, {})
            out = dict(base)
            if wants_values:
                out["values"] = values
            if wants_interrupts:
                out["interrupts"] = thread_interrupts
            if select_set is not None:
                out = {key: value for key, value in out.items() if key in select_set}
            if parsed:
                sources = {
                    "metadata": base.get("metadata") or {},
                    "values": values,
                    "interrupts": thread_interrupts,
                }
                out["extracted"] = {alias: self._extract_one(segs, sources) for alias, segs in parsed.items()}
            results.append(out)
        return results

    @staticmethod
    def _extract_one(segments: Sequence[str | int], sources: dict[str, Any]) -> Any:
        root = segments[0]
        source = sources.get(root) if isinstance(root, str) else None
        if source is None:
            return None
        return extract_value(source, segments[1:])

    @staticmethod
    def _needed_channels(*, wants_values: bool, value_segments: Iterable[Sequence[str | int]]) -> list[str] | None:
        """Channels to fetch: None = all (full `values` or a non-narrowable path)."""
        if wants_values:
            return None
        channels: set[str] = set()
        for segments in value_segments:
            channel = top_level_channel(segments)
            if channel is None:
                return None
            channels.add(channel)
        return sorted(channels)

    async def _fetch_values(self, thread_ids: list[str], channels: list[str] | None) -> dict[str, dict[str, Any]]:
        """Batched, channel-pruned fetch of the latest state values per thread."""
        if not thread_ids:
            return {}

        pool = db_manager.lg_pool
        if pool is None:
            raise RuntimeError("Database not initialized")
        serde = db_manager.get_checkpointer().serde

        async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_LATEST_VALUES_SQL, {"ids": thread_ids, "channels": channels})
            rows = await cur.fetchall()

        return {row["thread_id"]: self._decode_row(row, serde) for row in rows}

    def _decode_row(self, row: dict[str, Any], serde: Any) -> dict[str, Any]:
        """Reconstruct one thread's state values, matching the checkpointer:
        inline ``checkpoint.channel_values`` overlaid with the decoded blobs."""
        blob_values = row["channel_values"] or []
        blob_state = {
            k.decode(): serde.loads_typed((t.decode(), v)) for k, t, v in blob_values if t.decode() != "empty"
        }
        channel_state = {**(row["inline_values"] or {}), **blob_state}
        return self.serializer.serialize(channel_state)

    async def _fetch_interrupts(self, thread_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Batched fetch of pending interrupts, grouped as {task_id: [Interrupt]}."""
        if not thread_ids:
            return {}

        pool = db_manager.lg_pool
        if pool is None:
            raise RuntimeError("Database not initialized")
        serde = db_manager.get_checkpointer().serde

        async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_INTERRUPTS_SQL, {"ids": thread_ids})
            rows = await cur.fetchall()

        grouped: dict[str, dict[str, list[Any]]] = {}
        for row in rows:
            decoded = serde.loads_typed((row["type"], row["blob"]))
            values = list(decoded) if isinstance(decoded, (list, tuple)) else [decoded]
            per_thread = grouped.setdefault(row["thread_id"], {})
            per_thread.setdefault(row["task_id"], []).extend(values)
        return {tid: self.serializer.serialize(task_map) for tid, task_map in grouped.items()}


thread_search_projection = ThreadSearchProjectionService()
