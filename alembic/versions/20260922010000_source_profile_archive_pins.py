# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seal locally pinned completed source-profile rows against concurrent writes."""

import os

import sqlalchemy as sa

from alembic import op
from process.source_profile_result_pins import PAYLOAD_TABLES, TABLE, pin_guard_statements, pin_policy_statements

revision = "20260922010000_source_profile_archive_pins"
down_revision = "20260922000000_custom_import_durable_parquet_capture"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schemas differ")
    schema = runtime or legacy or "mrf"
    tuple(pin_guard_statements(schema))
    return schema


def upgrade():
    """Install local pins and the native guards that make their rows immutable."""
    schema = _schema()
    op.execute("SET LOCAL lock_timeout='500ms'")
    op.create_table(
        TABLE,
        sa.Column("pin_id", sa.String(36), nullable=False),
        sa.Column("source_key", sa.String(96), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("purpose", sa.String(16), nullable=False),
        sa.Column("authority_json", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("pin_id", "run_id"),
        schema=schema,
        if_not_exists=True,
    )
    op.create_index("provider_profile_source_pin_run_idx", TABLE, ["run_id"], schema=schema, if_not_exists=True)
    for statement in pin_guard_statements(schema):
        op.execute(sa.text(statement))
    for statement in pin_policy_statements(schema):
        op.execute(sa.text(statement))


def downgrade():
    """Remove unused guards only when no retained local pin remains."""
    schema = _schema()
    op.execute("SET LOCAL lock_timeout='500ms'")
    op.execute(sa.text(f'LOCK TABLE "{schema}".{TABLE} IN ACCESS EXCLUSIVE MODE'))
    retained = op.get_bind().execute(sa.text(f'SELECT EXISTS(SELECT 1 FROM "{schema}".{TABLE})')).scalar_one()
    if retained:
        raise RuntimeError("source profile archive pins remain")
    for name in PAYLOAD_TABLES:
        op.execute(sa.text(f'DROP TRIGGER provider_profile_pinned_run_guard ON "{schema}".{name}'))
        op.execute(sa.text(f'DROP TRIGGER provider_profile_pinned_truncate_guard ON "{schema}".{name}'))
    op.execute(sa.text(f'DROP FUNCTION "{schema}".provider_profile_pinned_run_guard() RESTRICT'))
    op.execute(sa.text(f'DROP FUNCTION "{schema}".provider_profile_pinned_truncate_guard() RESTRICT'))
    op.drop_table(TABLE, schema=schema)
