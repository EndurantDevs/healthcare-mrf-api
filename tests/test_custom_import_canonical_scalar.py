# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Schema-only rendering and prerequisite drift checks for canonical scalars."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic/versions/20261006010000_custom_import_canonical_scalar_fastpath.py"
    )
    spec = importlib.util.spec_from_file_location("canonical_scalar_migration", path)
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def test_canonical_scalar_migration_is_schema_only(monkeypatch):
    migration = _migration()
    schema = "synthetic\"schema\\'name"
    monkeypatch.setattr(migration, "_schema", lambda: schema)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    migration.downgrade()
    assert len(statements) == 4 and statements[0] == statements[2]
    assert statements[0].startswith("DO $guard$")
    assert "custom_import_canonical_prerequisite_missing" in statements[0]
    bulk = migration._bulk()
    storage = bulk._storage()
    identity = storage._literal(storage._quote(schema) + ".source_bulk_canonical(jsonb)")
    assert f"pg_catalog.to_regprocedure({identity}) IS NULL" in statements[0]
    original = next(
        sql
        for sql in bulk._resource("source_control.sql")
        if sql.startswith("CREATE FUNCTION __CONTROL__.source_bulk_canonical(")
    )
    original = bulk._control_sql(storage, schema, original).replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
    call = storage._quote(schema) + ".source_bulk_canonical(value)"
    fastpath = f"CASE WHEN jsonb_typeof(value) IN ('array','object') THEN {call} ELSE value::text END"
    assert statements[1] == original.replace(call, fastpath)
    assert statements[3] == original
    assert not any(
        word in "\n".join(statements)
        for word in ("DROP ", "GRANT ", "REVOKE ", "ALTER ", "INSERT ", "UPDATE ", "DELETE ")
    )


def test_canonical_scalar_migration_rejects_resource_drift(monkeypatch):
    migration = _migration()
    bulk = migration._bulk()
    statements = bulk._resource("source_control.sql")
    monkeypatch.setattr(
        bulk,
        "_resource",
        lambda _: [sql.replace("__CONTROL__.source_bulk_canonical(value)", "value::text", 1) for sql in statements],
    )
    monkeypatch.setattr(migration, "_bulk", lambda: bulk)
    executed_statements = []
    monkeypatch.setattr(migration.op, "execute", executed_statements.append)
    for direction in (migration.upgrade, migration.downgrade):
        with pytest.raises(RuntimeError, match="canonical scalar prerequisite body changed"):
            direction()
    assert executed_statements == []
