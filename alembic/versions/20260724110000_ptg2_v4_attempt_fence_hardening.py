"""Harden already-stamped PTG V4 attempt authority catalogs.

Revision ID: 20260724110000_ptg2_v4_attempt_fence_hardening
Revises: 20260724103000_ptg2_v4_attempt_indexes
"""

from __future__ import annotations

import os

from alembic import op
from db.migration_ptg2_v4_attempt_audit import (
    install_attempt_audit_guard,
)
from db.migration_ptg2_v4_attempt_catalog import (
    adopt_or_validate_attempt_tables,
    validate_current_attempt_authority,
)
from db.migration_ptg2_v4_attempt_fence import (
    install_lifecycle_lock_layer,
    validate_lifecycle_lock_layer,
)
from db.migration_ptg2_v4_import_job import (
    adopt_or_create_import_job,
    install_import_job_attempt_guards,
    validate_import_job_attempt_guards,
    validate_import_job_catalog,
)
from db.ptg2_v4_attempt_schema import LIFECYCLE_GUARDED_TABLES


revision = "20260724110000_ptg2_v4_attempt_fence_hardening"
down_revision = "20260724103000_ptg2_v4_attempt_indexes"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _offline_refusal_sql() -> str:
    """Emit a script that cannot stamp hardening without online validation."""

    return """
        DO $block$
        BEGIN
            RAISE EXCEPTION
                'ptg2_v4_attempt_hardening_requires_online_catalog'
                USING ERRCODE = '55000';
        END;
        $block$
    """


def upgrade() -> None:
    """Adopt the legacy fence in place and install nonce-bound audit."""

    schema = _schema()
    import_job_status = adopt_or_create_import_job(
        op,
        schema,
        emit_offline=False,
    )
    if import_job_status == "offline":
        op.execute(_offline_refusal_sql())
        return
    adoption_status = adopt_or_validate_attempt_tables(
        op,
        schema,
        require_existing=True,
    )
    if adoption_status == "offline":
        op.execute(_offline_refusal_sql())
        return
    install_attempt_audit_guard(op, schema)
    install_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )
    install_import_job_attempt_guards(op, schema)
    validate_current_attempt_authority(op, schema)
    validate_import_job_catalog(op, schema)
    validate_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )
    validate_import_job_attempt_guards(op, schema)


def downgrade() -> None:
    """Preserve monotonic nonce and immutable-audit hardening."""

    schema = _schema()
    validate_current_attempt_authority(op, schema)
    validate_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )
