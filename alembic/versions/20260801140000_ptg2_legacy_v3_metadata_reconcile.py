"""Add durable source-attempt fencing for legacy PTG V3 repair.

Revision ID: 20260801140000_ptg2_legacy_v3_metadata_reconcile
Revises: 20260801130000_provider_directory_capacity_lease_v2
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

from alembic import op

from db.migration_ptg2_legacy_v3_metadata_reconcile import (
    AUDIT_TABLE,
    CAPABILITY_TABLE,
    EVENT_TABLE,
    install_legacy_v3_reconcile_contract,
    refuse_legacy_v3_downgrade,
)


revision = "20260801140000_ptg2_legacy_v3_metadata_reconcile"
down_revision = "20260801130000_provider_directory_capacity_lease_v2"
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


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table_name: str) -> str:
    return f"{_q(schema)}.{_q(table_name)}"


def upgrade() -> None:
    """Install immutable action and reconciliation evidence."""

    install_legacy_v3_reconcile_contract(op, _schema())


def _restore_predecessor_guard(schema: str) -> None:
    """Load the exact predecessor revision by path and restore its guard."""

    migration_path = Path(__file__).with_name(
        "20260724100000_ptg2_v4_attempt_fence.py"
    )
    module_name = "_ptg2_v4_attempt_fence_predecessor"
    module_spec = importlib.util.spec_from_file_location(
        module_name,
        migration_path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("PTG V4 predecessor guard revision is unavailable")
    previous_migration = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = previous_migration
    try:
        module_spec.loader.exec_module(previous_migration)
        previous_migration._create_guard_function(schema)
    finally:
        sys.modules.pop(module_name, None)


def downgrade() -> None:
    """Remove an unused contract, refusing any evidence loss."""

    schema = _schema()
    refuse_legacy_v3_downgrade(op, schema)
    capability = _qt(schema, CAPABILITY_TABLE)
    op.execute(
        f"DELETE FROM {capability} "
        "WHERE service_name = 'healthcare-mrf-api'"
    )
    _restore_predecessor_guard(schema)
    op.execute(
        f"DROP FUNCTION {_q(schema)}."
        f"{_q('guard_ptg_source_attempt')}(text)"
    )
    for table_name in (AUDIT_TABLE, EVENT_TABLE):
        op.execute(f"DROP TABLE {_qt(schema, table_name)}")
    op.execute(f"DROP TABLE {capability}")
    op.execute(
        f"DROP FUNCTION {_q(schema)}."
        f"{_q('guard_ptg_source_attempt_append_only')}()"
    )
