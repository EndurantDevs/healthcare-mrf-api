# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Avoid recursive SQL calls for scalar canonical JSON values."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261006010000_custom_import_canonical_scalar_fastpath"
down_revision = "20261005080000_custom_import_writer_cutover"
branch_labels = None
depends_on = None


def _bulk():
    path = Path(__file__).with_name("20261005040000_custom_import_bulk_snapshot_writers.py")
    spec = importlib.util.spec_from_file_location("canonical_scalar_bulk_prerequisite", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema() -> str:
    return _bulk()._schema()


def _statement(*, fastpath: bool) -> str:
    bulk = _bulk()
    statement = next(
        statement
        for statement in bulk._resource("source_control.sql")
        if statement.startswith("CREATE FUNCTION __CONTROL__.source_bulk_canonical(")
    )
    call = "__CONTROL__.source_bulk_canonical(value)"
    if statement.count(call) != 2:
        raise RuntimeError("canonical scalar prerequisite body changed")
    if fastpath:
        statement = statement.replace(
            call,
            f"CASE WHEN jsonb_typeof(value) IN ('array','object') THEN {call} ELSE value::text END",
        )
    rendered = bulk._control_sql(bulk._storage(), _schema(), statement)
    return rendered.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)


def _replace(*, fastpath: bool) -> None:
    statement = _statement(fastpath=fastpath)
    storage = _bulk()._storage()
    identity = storage._literal(storage._quote(_schema()) + ".source_bulk_canonical(jsonb)")
    op.execute(f"""DO $guard$ BEGIN
        IF pg_catalog.to_regprocedure({identity}) IS NULL THEN
            RAISE EXCEPTION 'custom_import_canonical_prerequisite_missing' USING ERRCODE = 'P0001';
        END IF;
    END; $guard$""")
    op.execute(statement)


def upgrade() -> None:
    """Replace the existing helper body while preserving its identity and authority."""
    _replace(fastpath=True)


def downgrade() -> None:
    """Restore the original recursive body without recreating the helper."""
    _replace(fastpath=False)
