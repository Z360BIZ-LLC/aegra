"""add materialized state columns to thread

Stores the graph's logical state (aget_state output) on the thread row so
POST /threads/search can project values/interrupts without graph-aware
checkpoint replay (LangGraph Platform parity).

Revision ID: b5c6d7e8f9a0
Revises: a4b5c6d7e8f9
Create Date: 2026-07-01 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision: str = "b5c6d7e8f9a0"
down_revision: str | None = "a4b5c6d7e8f9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("thread", sa.Column("values", JSONB(), nullable=True))
    op.add_column("thread", sa.Column("interrupts", JSONB(), nullable=True))
    op.add_column("thread", sa.Column("state_updated_at", sa.TIMESTAMP(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column("thread", "state_updated_at")
    op.drop_column("thread", "interrupts")
    op.drop_column("thread", "values")
