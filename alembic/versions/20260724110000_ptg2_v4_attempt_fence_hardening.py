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


def upgrade() -> None:
    """Adopt the legacy fence in place and install nonce-bound audit."""

    schema = _schema()
    adoption_status = adopt_or_validate_attempt_tables(
        op,
        schema,
        require_existing=True,
    )
    if adoption_status == "offline":
        raise RuntimeError(
            "ptg2_v4_attempt_hardening_requires_online_catalog"
        )
    install_attempt_audit_guard(op, schema)
    install_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )
    validate_current_attempt_authority(op, schema)
    validate_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )


def downgrade() -> None:
    """Preserve monotonic nonce and immutable-audit hardening."""

    schema = _schema()
    validate_current_attempt_authority(op, schema)
    validate_lifecycle_lock_layer(
        op,
        schema,
        LIFECYCLE_GUARDED_TABLES,
    )
