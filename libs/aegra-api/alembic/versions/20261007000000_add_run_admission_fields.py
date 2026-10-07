"""add run admission fields

Revision ID: d4e8f2a1b6c9
Revises: a3f7c1d9e2b4
Create Date: 2026-10-07 00:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e8f2a1b6c9"
down_revision = "a3f7c1d9e2b4"
branch_labels = None
depends_on = None

_QUEUE_SEQUENCE = "runs_queue_position_seq"


def upgrade() -> None:
    op.execute(sa.schema.CreateSequence(sa.Sequence(_QUEUE_SEQUENCE)))
    op.add_column(
        "runs",
        sa.Column(
            "multitask_strategy",
            sa.Text(),
            server_default=sa.text("'enqueue'"),
            nullable=False,
        ),
    )
    # Preserve the strategy that created historical rows when execution
    # parameters contain it. Rows from older releases have no such value and
    # adopt the new Agent Protocol default.
    op.execute(
        sa.text(
            """
            UPDATE runs
               SET multitask_strategy = CASE
                   WHEN execution_params #>> '{behavior,multitask_strategy}'
                        IN ('reject', 'interrupt', 'rollback', 'enqueue')
                   THEN execution_params #>> '{behavior,multitask_strategy}'
                   ELSE 'enqueue'
               END
            """
        )
    )
    op.add_column("runs", sa.Column("queue_position", sa.BigInteger(), nullable=True))
    op.add_column("runs", sa.Column("pending_reason", sa.Text(), nullable=True))
    op.add_column(
        "runs",
        sa.Column("pending_reason_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )

    op.execute(
        sa.text(
            """
            WITH ordered AS (
                SELECT run_id,
                       row_number() OVER (ORDER BY created_at, run_id) AS position
                  FROM runs
            )
            UPDATE runs
               SET queue_position = ordered.position
              FROM ordered
             WHERE runs.run_id = ordered.run_id
               AND runs.queue_position IS NULL
            """
        )
    )
    op.execute(
        sa.text(
            """
            SELECT setval(
                'runs_queue_position_seq',
                COALESCE((SELECT MAX(queue_position) FROM runs), 0) + 1,
                false
            )
            """
        )
    )
    op.alter_column(
        "runs",
        "queue_position",
        existing_type=sa.BigInteger(),
        server_default=sa.text("nextval('runs_queue_position_seq')"),
        nullable=False,
    )
    op.execute("ALTER SEQUENCE runs_queue_position_seq OWNED BY runs.queue_position")

    op.create_check_constraint(
        "ck_runs_multitask_strategy",
        "runs",
        "multitask_strategy IN ('reject', 'interrupt', 'rollback', 'enqueue')",
    )
    op.create_check_constraint(
        "ck_runs_pending_reason",
        "runs",
        "pending_reason IS NULL OR pending_reason IN ('thread', 'org')",
    )
    op.create_index(
        "idx_runs_thread_active_queue",
        "runs",
        ["thread_id", "queue_position"],
        unique=False,
        postgresql_where=sa.text("status IN ('pending', 'running')"),
    )


def downgrade() -> None:
    op.drop_index("idx_runs_thread_active_queue", table_name="runs")
    op.drop_constraint("ck_runs_pending_reason", "runs", type_="check")
    op.drop_constraint("ck_runs_multitask_strategy", "runs", type_="check")
    op.drop_column("runs", "pending_reason_at")
    op.drop_column("runs", "pending_reason")
    op.alter_column(
        "runs",
        "queue_position",
        existing_type=sa.BigInteger(),
        server_default=None,
    )
    op.execute("ALTER SEQUENCE runs_queue_position_seq OWNED BY NONE")
    op.drop_column("runs", "queue_position")
    op.drop_column("runs", "multitask_strategy")
    op.execute(sa.schema.DropSequence(sa.Sequence(_QUEUE_SEQUENCE)))
