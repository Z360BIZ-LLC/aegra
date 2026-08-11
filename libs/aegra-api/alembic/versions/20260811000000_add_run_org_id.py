"""Add runs.org_id for per-organization concurrency limits

Adds the tenant key the run-limit policy counts on, backfills it from the
run config the callers already send (``config.configurable.org_id``), and
creates a partial index covering only active runs.

The index is built CONCURRENTLY inside an ``autocommit_block``: the
transactional build takes a SHARE lock on ``runs`` for its duration, which
would stall every run insert on a deploy.

Recovery: an interrupted CONCURRENTLY build leaves an INVALID index behind.
The ``DROP INDEX CONCURRENTLY IF EXISTS`` below makes a re-run idempotent.

Revision ID: c6d7e8f9a0b1
Revises: b5c6d7e8f9a0
Create Date: 2026-08-11 00:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

revision = "c6d7e8f9a0b1"
down_revision = "b5c6d7e8f9a0"
branch_labels = None
depends_on = None


INDEX_NAME = "idx_runs_org_active"
# Bound each backfill statement so a large runs table can't hold a single
# long-running UPDATE against production writes.
BACKFILL_BATCH_SIZE = 5000


def upgrade() -> None:
    op.add_column("runs", sa.Column("org_id", sa.Text(), nullable=True))

    connection = op.get_bind()
    while True:
        result = connection.execute(
            sa.text(
                """
                UPDATE runs SET org_id = config -> 'configurable' ->> 'org_id'
                 WHERE run_id IN (
                       SELECT run_id FROM runs
                        WHERE org_id IS NULL
                          AND config -> 'configurable' ->> 'org_id' IS NOT NULL
                        LIMIT :batch
                 )
                """
            ),
            {"batch": BACKFILL_BATCH_SIZE},
        )
        if result.rowcount < BACKFILL_BATCH_SIZE:
            break

    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
        op.execute(
            f"CREATE INDEX CONCURRENTLY {INDEX_NAME} ON runs (org_id, created_at) "
            "WHERE status IN ('pending', 'running')"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
    op.drop_column("runs", "org_id")
