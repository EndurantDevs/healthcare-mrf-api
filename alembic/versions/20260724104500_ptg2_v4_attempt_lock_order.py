"""Acquire the PTG lifecycle lock before low-volume attachment DML.

Revision ID: 20260724104500_ptg2_v4_attempt_lock_order
Revises: 20260724100000_ptg2_v4_attempt_fence
"""

from __future__ import annotations

import os

from alembic import op
from db.migration_ptg2_v4_attempt_fence import (
    install_lifecycle_lock_layer,
)
from db.ptg2_v4_attempt_schema import LIFECYCLE_GUARDED_TABLES


revision = "20260724104500_ptg2_v4_attempt_lock_order"
down_revision = "20260724100000_ptg2_v4_attempt_fence"
branch_labels = None
depends_on = None

_GUARDED_TABLES = LIFECYCLE_GUARDED_TABLES


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def upgrade() -> None:
    """Install a before-statement lifecycle lock ahead of all row locks."""

    install_lifecycle_lock_layer(op, _schema(), _GUARDED_TABLES)


def downgrade() -> None:
    """Restore the predecessor revision's lifecycle-lock layer."""

    install_lifecycle_lock_layer(op, _schema(), _GUARDED_TABLES)
