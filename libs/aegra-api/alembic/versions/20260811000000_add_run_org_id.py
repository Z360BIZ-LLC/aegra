"""Add runs.org_id for per-organization concurrency limits

Adds the tenant key the run-limit policy counts on, backfills it for runs
that are still active, and creates a partial index covering only those.

The backfill is deliberately scoped to ``pending``/``running`` rows. Alembic
wraps the whole migration in one transaction, so a full-history backfill would
hold a row lock on every run ever created until the migration commits — on a
large table that stalls live runs updating their own status, during startup.
Only active rows are ever read by the policy, so historical rows gain nothing
from being populated; backfill them out-of-band if they are ever wanted.

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

# Mirrors resolve_org_id: config wins, run metadata is the fallback.
BACKFILL_ACTIVE_RUNS = sa.text(
    """
    UPDATE runs
       SET org_id = COALESCE(
               config -> 'configurable' ->> 'org_id',
               execution_params -> 'run_metadata' ->> 'org_id'
           )
     WHERE status IN ('pending', 'running')
       AND org_id IS NULL
    """
)


def upgrade() -> None:
    op.add_column("runs", sa.Column("org_id", sa.Text(), nullable=True))
    op.get_bind().execute(BACKFILL_ACTIVE_RUNS)

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
