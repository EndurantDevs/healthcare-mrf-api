"""Add import run table"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260610120000_add_import_run_table"
down_revision = "20260319193000_extend_geo_zip_census_profile_metrics"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "import_run",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("engine", sa.String(length=64), nullable=False),
        sa.Column("node_id", sa.String(length=64), nullable=True),
        sa.Column("importer", sa.String(length=64), nullable=False),
        sa.Column("family", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("phase_detail", sa.String(length=128), nullable=True),
        sa.Column("params", sa.JSON(), nullable=True),
        sa.Column("idempotency_key", sa.String(length=160), nullable=True),
        sa.Column("triggered_by", sa.String(length=32), nullable=True),
        sa.Column("schedule_id", sa.String(length=64), nullable=True),
        sa.Column("subscription_id", sa.String(length=64), nullable=True),
        sa.Column("source_file_import_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("started_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("finished_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("heartbeat_at", sa.TIMESTAMP(), nullable=True),
        sa.Column("progress", sa.JSON(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.Column("error", sa.JSON(), nullable=True),
        sa.Column("snapshot_id", sa.String(length=96), nullable=True),
        sa.Column("import_id", sa.String(length=64), nullable=True),
        sa.Column("retry_of_run_id", sa.String(length=64), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
        schema=schema,
    )
    op.create_index("import_run_status_heartbeat_idx", "import_run", ["status", "heartbeat_at"], schema=schema)
    op.create_index("import_run_importer_created_idx", "import_run", ["importer", "created_at"], schema=schema)
    op.create_index(
        "import_run_active_idempotency_idx",
        "import_run",
        ["idempotency_key"],
        unique=True,
        schema=schema,
        postgresql_where=sa.text("status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')"),
    )
    op.create_index("import_run_schedule_idx", "import_run", ["schedule_id"], schema=schema)
    op.create_index("import_run_subscription_idx", "import_run", ["subscription_id"], schema=schema)
    op.create_index("import_run_source_file_import_idx", "import_run", ["source_file_import_id"], schema=schema)


def downgrade():
    schema = _schema()

    op.drop_index("import_run_source_file_import_idx", table_name="import_run", schema=schema)
    op.drop_index("import_run_subscription_idx", table_name="import_run", schema=schema)
    op.drop_index("import_run_schedule_idx", table_name="import_run", schema=schema)
    op.drop_index("import_run_active_idempotency_idx", table_name="import_run", schema=schema)
    op.drop_index("import_run_importer_created_idx", table_name="import_run", schema=schema)
    op.drop_index("import_run_status_heartbeat_idx", table_name="import_run", schema=schema)
    op.drop_table("import_run", schema=schema)
