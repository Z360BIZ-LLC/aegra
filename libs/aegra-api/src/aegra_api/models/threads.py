"""Thread-related Pydantic models for Agent Protocol"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from aegra_api.utils.json_path import parse_path
from aegra_api.utils.status_compat import validate_thread_status

# Subset of the LangGraph SDK's ThreadSelectField that this server can project.
SUPPORTED_SELECT_FIELDS: frozenset[str] = frozenset(
    {"thread_id", "status", "created_at", "updated_at", "metadata", "values", "interrupts"}
)
# In the SDK literal but not implemented here yet — rejected with a clear 422.
UNSUPPORTED_SELECT_FIELDS: frozenset[str] = frozenset({"config", "context"})
_EXTRACT_PREFIXES: tuple[str, ...] = ("values.", "metadata.", "interrupts.")
_MAX_EXTRACT_PATHS = 10


class ThreadCreate(BaseModel):
    """Request model for creating threads"""

    model_config = ConfigDict(populate_by_name=True)

    metadata: dict[str, Any] | None = Field(None, description="Thread metadata")
    initial_state: dict[str, Any] | None = Field(None, description="LangGraph initial state")
    thread_id: str | None = Field(
        None,
        alias="threadId",
        description="Optional client-provided thread ID for idempotent creation",
    )
    if_exists: str | None = Field(
        "raise",
        alias="ifExists",
        description="Behavior when thread exists: 'raise' (default) or 'do_nothing'",
    )


class ThreadUpdate(BaseModel):
    """Request model for updating threads"""

    metadata: dict[str, Any] | None = Field(None, description="Thread metadata to update")


class Thread(BaseModel):
    """Thread entity model

    Status values: idle, busy, interrupted, error
    """

    model_config = ConfigDict(from_attributes=True)

    thread_id: str = Field(..., description="Unique identifier for the thread.")
    status: str = Field("idle", description="Current thread status: idle, busy, interrupted, or error.")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Arbitrary metadata attached to the thread.")
    user_id: str = Field(..., description="Identifier of the user who owns this thread.")
    created_at: datetime = Field(..., description="Timestamp when the thread was created.")
    updated_at: datetime = Field(..., description="Timestamp when the thread was last updated.")

    @field_validator("status", mode="before")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status conforms to API specification."""
        if not isinstance(v, str):
            raise ValueError(f"Status must be a string, got {type(v)}")
        return validate_thread_status(v)


class ThreadList(BaseModel):
    """Response model for listing threads"""

    threads: list[Thread]
    total: int


class ThreadSearchRequest(BaseModel):
    """Request model for thread search"""

    metadata: dict[str, Any] | None = Field(None, description="Metadata filters")
    status: str | None = Field(None, description="Thread status filter (idle, busy, interrupted, error)")
    limit: int | None = Field(20, le=100, ge=1, description="Maximum results")
    offset: int | None = Field(0, ge=0, description="Results offset")
    order_by: str | None = Field(
        "created_at DESC",
        deprecated=True,
        description="DEPRECATED: use sort_by + sort_order. Legacy single-field form, e.g. 'updated_at ASC'.",
    )
    sort_by: Literal["thread_id", "status", "created_at", "updated_at"] | None = Field(
        None,
        description="Field to sort by (SDK-compatible). Takes precedence over order_by.",
    )
    sort_order: Literal["asc", "desc"] | None = Field(
        None,
        description="Sort direction (SDK-compatible). Defaults to 'desc' when sort_by is set.",
    )
    ids: list[str] | None = Field(None, description="Restrict results to these thread IDs.")
    select: list[str] | None = Field(
        None,
        description="Fields to include in each result. Supported: thread_id, status, "
        "created_at, updated_at, metadata, values, interrupts. Absent = full thread.",
    )
    extract: dict[str, str] | None = Field(
        None,
        description="Alias -> JSON path (e.g. 'values.messages[-1].content'). Paths must "
        "start with 'values.', 'metadata.', or 'interrupts.'; max 10. Results land in 'extracted'.",
    )
    values: dict[str, Any] | None = Field(
        None,
        description="SDK state-value filter — not supported; a non-empty value returns 422.",
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str | None) -> str | None:
        """Validate status filter conforms to API specification."""
        if v is not None:
            return validate_thread_status(v)
        return v

    @field_validator("select")
    @classmethod
    def validate_select(cls, v: list[str] | None) -> list[str] | None:
        """Reject unknown fields, and SDK fields we don't implement yet (422)."""
        if v is None:
            return v
        for field in v:
            if field in SUPPORTED_SELECT_FIELDS:
                continue
            if field in UNSUPPORTED_SELECT_FIELDS:
                raise ValueError(f"select field '{field}' is not supported yet")
            raise ValueError(f"unknown select field '{field}'")
        return v

    @field_validator("extract")
    @classmethod
    def validate_extract(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        """Enforce path count, allowed roots, and syntactic validity (422)."""
        if v is None:
            return v
        if len(v) > _MAX_EXTRACT_PATHS:
            raise ValueError(f"extract supports at most {_MAX_EXTRACT_PATHS} paths")
        for alias, path in v.items():
            if not isinstance(path, str) or not path.startswith(_EXTRACT_PREFIXES):
                raise ValueError(f"extract path for '{alias}' must start with 'values.', 'metadata.', or 'interrupts.'")
            try:
                parse_path(path)
            except ValueError as exc:
                raise ValueError(f"invalid extract path for '{alias}': {exc}") from exc
        return v

    @field_validator("values")
    @classmethod
    def reject_values_filter(cls, v: dict[str, Any] | None) -> dict[str, Any] | None:
        """State-value filtering would require decoding every candidate's blobs;
        decline explicitly instead of silently ignoring it."""
        if v:
            raise ValueError("filtering threads by state 'values' is not supported")
        return v


class ThreadSearchResponse(BaseModel):
    """Response model for thread search"""

    threads: list[Thread]
    total: int
    limit: int
    offset: int


class ThreadCheckpoint(BaseModel):
    """Checkpoint identifier for thread history"""

    checkpoint_id: str | None = None
    thread_id: str | None = None
    checkpoint_ns: str | None = ""


class ThreadCheckpointPostRequest(BaseModel):
    """Request model for fetching thread checkpoint"""

    checkpoint: ThreadCheckpoint = Field(description="Checkpoint to fetch")
    subgraphs: bool | None = Field(False, description="Include subgraph states")


class ThreadState(BaseModel):
    """Thread state model for history endpoint"""

    values: dict[str, Any] = Field(description="Channel values (messages, etc.)")
    next: list[str] = Field(default_factory=list, description="Next nodes to execute")
    tasks: list[dict[str, Any]] = Field(default_factory=list, description="Tasks to execute")
    interrupts: list[dict[str, Any]] = Field(default_factory=list, description="Interrupt data")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Checkpoint metadata")
    created_at: datetime | None = Field(None, description="Timestamp of state creation")
    checkpoint: ThreadCheckpoint = Field(description="Current checkpoint")
    parent_checkpoint: ThreadCheckpoint | None = Field(None, description="Parent checkpoint")
    checkpoint_id: str | None = Field(None, description="Checkpoint ID (for backward compatibility)")
    parent_checkpoint_id: str | None = Field(None, description="Parent checkpoint ID (for backward compatibility)")


class ThreadStateUpdate(BaseModel):
    """Request model for updating thread state"""

    values: dict[str, Any] | list[dict[str, Any]] | None = Field(
        None, description="The values to update the state with"
    )
    checkpoint: dict[str, Any] | None = Field(None, description="The checkpoint to update the state of")
    checkpoint_id: str | None = Field(None, description="Optional checkpoint ID to update from")
    as_node: str | None = Field(None, description="Update the state as if this node had just executed")
    # Also support query-like parameters for GET-like behavior via POST
    subgraphs: bool | None = Field(False, description="Include states from subgraphs")
    checkpoint_ns: str | None = Field(None, description="Checkpoint namespace")


class ThreadStateUpdateResponse(BaseModel):
    """Response model for thread state update"""

    checkpoint: dict[str, Any] = Field(description="The checkpoint that was created/updated")


class ThreadHistoryRequest(BaseModel):
    """Request model for thread history endpoint"""

    limit: int | None = Field(10, ge=1, le=1000, description="Number of states to return")
    before: dict[str, Any] | str | None = Field(
        None,
        description="Return states before this checkpoint (checkpoint ID string, raw checkpoint dict, or RunnableConfig with 'configurable' key)",
    )
    metadata: dict[str, Any] | None = Field(None, description="Filter by metadata")
    checkpoint: dict[str, Any] | None = Field(None, description="Checkpoint for subgraph filtering")
    subgraphs: bool | None = Field(False, description="Include states from subgraphs")
    checkpoint_ns: str | None = Field(None, description="Checkpoint namespace")
