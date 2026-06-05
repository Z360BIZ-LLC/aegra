"""Add composite indexes on thread(user_id, created_at DESC) and (user_id, updated_at DESC)

POST /threads/search resolves to
``WHERE user_id = ? [AND status = ?] [AND metadata_json @> ?] ORDER BY <sort_col> DESC LIMIT N``.
With only ``idx_thread_user (user_id)``, Postgres must fetch every row for the
user and sort in memory before applying LIMIT. On a low-ACU Aurora Serverless
v2 floor this turned a default-sort search into a 130–260 s query during the
2026-06-05 incident (see also ``idx_thread_metadata_gin``, added in
``b7c8d9e0f123``, which only helps the metadata predicate).

The composite indexes let Postgres satisfy WHERE+ORDER BY+LIMIT as a single
index-range scan with early exit, regardless of how many threads a user has.
Two indexes because the frontend already sorts by ``updated_at`` in
use-agent-threads.ts even though the API default is ``created_at``.

``CREATE INDEX CONCURRENTLY`` only holds SHARE UPDATE EXCLUSIVE, so writes
keep flowing during the build. Must run outside a transaction; the
``autocommit_block`` exits the wrapping Alembic transaction for the duration.

Recovery: if CONCURRENTLY is interrupted, Postgres leaves an INVALID index
behind that won't satisfy queries. The IF NOT EXISTS guards turn re-runs into
no-ops if the build already succeeded; for an INVALID leftover, drop it and
re-run::

    DROP INDEX IF EXISTS idx_thread_user_created_at;
    DROP INDEX IF EXISTS idx_thread_user_updated_at;

Revision ID: a4b5c6d7e8f9
Revises: c7d1f2a4b6e8
Create Date: 2026-06-05 00:00:00.000000

"""

from alembic import op

revision = "a4b5c6d7e8f9"
down_revision = "c7d1f2a4b6e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_user_created_at ON thread (user_id, created_at DESC)"
        )
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_thread_user_updated_at ON thread (user_id, updated_at DESC)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_thread_user_updated_at")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_thread_user_created_at")
