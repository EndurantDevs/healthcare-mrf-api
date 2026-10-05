# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check retained-run protection once per payload statement, not per row."""

import os

import sqlalchemy as sa

from alembic import op
from process.source_profile_result_pins import (
    PAYLOAD_TABLES,
    TABLE,
    pin_guard_statements,
    statement_pin_guard_statements,
)

revision = "20261005030000_source_profile_statement_pins"
down_revision = "20261005020000_custom_import_sealed_append_plans"
branch_labels = None
depends_on = None


def _schema():
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("database schemas differ")
    schema = runtime or legacy or "mrf"
    tuple(statement_pin_guard_statements(schema))
    return schema


def _lock_payload(schema):
    op.execute("SET LOCAL lock_timeout='500ms'")
    tables = ", ".join(f'"{schema}".{name}' for name in (*PAYLOAD_TABLES, TABLE))
    op.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))


def upgrade():
    """Replace guards atomically without changing table, pin or function ownership."""
    schema = _schema()
    _lock_payload(schema)
    op.execute(
        sa.text(f"""DO $preflight$ BEGIN
        IF (SELECT count(*) FROM pg_catalog.pg_proc guard
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=guard.pronamespace
            WHERE namespace.nspname='{schema}'
                AND guard.proname='provider_profile_pinned_run_guard' AND guard.pronargs=0)<>1 THEN
            RAISE EXCEPTION 'source profile historical pin guard missing' USING ERRCODE='55000';
        END IF;
        END $preflight$""")
    )
    for statement in statement_pin_guard_statements(schema):
        op.execute(sa.text(statement))
    op.execute(
        sa.text(f"""DO $owner$ DECLARE guard_owner pg_catalog.name;
        BEGIN
            SELECT pg_catalog.pg_get_userbyid(guard.proowner) INTO STRICT guard_owner
            FROM pg_catalog.pg_proc guard
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=guard.pronamespace
            WHERE namespace.nspname='{schema}'
                AND guard.proname='provider_profile_pinned_run_guard' AND guard.pronargs=0;
            EXECUTE pg_catalog.format(
                'ALTER FUNCTION %I.provider_profile_attached_pin_guard() OWNER TO %I',
                '{schema}', guard_owner);
        END $owner$""")
    )


def downgrade():
    """Restore historical guards in the same transaction without unsealing pins."""
    schema = _schema()
    _lock_payload(schema)
    names = ", ".join(f"'{name}'" for name in PAYLOAD_TABLES)
    op.execute(
        sa.text(f"""DO $guard$ BEGIN
        IF EXISTS(SELECT 1 FROM pg_catalog.pg_inherits inherited
            JOIN pg_catalog.pg_class parent ON parent.oid=inherited.inhparent
            JOIN pg_catalog.pg_namespace namespace ON namespace.oid=parent.relnamespace
            WHERE namespace.nspname='{schema}' AND parent.relname IN ({names})) THEN
            RAISE EXCEPTION 'source profile attached tables remain' USING ERRCODE='55000';
        END IF;
        END $guard$""")
    )
    for operation in ("update", "delete", "truncate"):
        op.execute(sa.text(f'DROP TRIGGER provider_profile_attached_pin_guard_{operation} ON "{schema}".{TABLE}'))
    op.execute(sa.text(f'DROP FUNCTION "{schema}".provider_profile_attached_pin_guard() RESTRICT'))
    for name in PAYLOAD_TABLES:
        for operation in ("insert", "update", "delete"):
            op.execute(sa.text(f'DROP TRIGGER provider_profile_pinned_run_guard_{operation} ON "{schema}".{name}'))
    for statement in pin_guard_statements(schema):
        op.execute(sa.text(statement))
