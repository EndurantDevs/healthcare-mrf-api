# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic schema checks; no database, source or credentials are used."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects import postgresql

from db.models import CustomImportCaptureRawObject
from db.models.custom_import_raw_capture import MAX_RAW_OBJECT_BYTES, RAW_OBJECT_CONTRACT
from tests import custom_import_postgres_support


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261017010000_custom_import_raw_objects.py"
    spec = importlib.util.spec_from_file_location("raw_capture_schema", path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _operations(monkeypatch, *, retained=False, isolation="read committed"):
    migration = _migration()
    statements = []
    grantees = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["writer", 'odd"role', "PUBLIC"]))
    connection = SimpleNamespace(
        dialect=postgresql.dialect(),
        execute=lambda *_args: grantees,
        scalar=lambda statement: isolation if "transaction_isolation" in str(statement) else retained,
    )
    operations = SimpleNamespace(
        get_bind=lambda: connection,
        execute=lambda statement: statements.append(str(statement.compile(dialect=connection.dialect))),
        drop_table=lambda name, schema: statements.append(f"DROP {schema}.{name}"),
    )
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_capture")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    return migration, statements


def test_raw_capture_model_has_explicit_bounded_original_identity():
    table = CustomImportCaptureRawObject.__table__
    assert CustomImportCaptureRawObject.__runtime_schema_sync__ is False
    assert tuple(table.primary_key.columns.keys()) == ("capture_bundle_id", "stream_slot")
    assert all(not column.nullable for column in table.columns)
    assert not table.foreign_keys
    checks = " ".join(
        str(constraint.sqltext) for constraint in table.constraints if isinstance(constraint, CheckConstraint)
    )
    assert RAW_OBJECT_CONTRACT in checks
    assert f"byte_count BETWEEN 0 AND {MAX_RAW_OBJECT_BYTES}" in checks
    assert "octet_length(payload) = byte_count" in checks
    assert "payload_sha256 = pg_catalog.sha256(payload)" in checks
    assert {tuple(column.name for column in index.columns) for index in table.indexes} == {("dataset_id",)}


def test_raw_capture_upgrade_is_additive_and_closes_default_grants(monkeypatch):
    migration, statements = _operations(monkeypatch)
    migration.upgrade()
    sql = "\n".join(statements)
    assert "CREATE TABLE synthetic_capture.custom_import_capture_raw_object" in sql
    assert "CREATE INDEX custom_import_capture_raw_object_dataset_idx" in sql
    assert "REVOKE ALL ON TABLE synthetic_capture.custom_import_capture_raw_object FROM PUBLIC" in sql
    assert "REVOKE ALL ON TABLE synthetic_capture.custom_import_capture_raw_object FROM writer" in sql
    assert 'FROM "odd""role"' in sql
    assert 'FROM "PUBLIC"' in sql
    assert "current_setting('lock_timeout')::interval < interval '2 seconds'" in sql
    assert all(denied not in sql.upper() for denied in ("CREATE FUNCTION", "CREATE TRIGGER", "INSERT INTO", "UPDATE "))
    assert "custom_import_capture_parquet_part" not in sql
    assert migration.down_revision == "20261017210000_provider_directory_readiness_indexes"


@pytest.mark.parametrize("retained", [False, True])
def test_raw_capture_downgrade_preserves_retained_objects(monkeypatch, retained):
    migration, statements = _operations(monkeypatch, retained=retained)
    if retained:
        with pytest.raises(RuntimeError, match="custom_import_raw_objects_downgrade_blocked"):
            migration.downgrade()
        assert not any(statement.startswith("DROP ") for statement in statements)
    else:
        migration.downgrade()
        assert statements[-1] == "DROP synthetic_capture.custom_import_capture_raw_object"
    assert any("ACCESS EXCLUSIVE" in statement for statement in statements)


def test_raw_capture_migration_rejects_schema_disagreement(monkeypatch):
    migration, statements = _operations(monkeypatch)
    monkeypatch.setenv("DB_SCHEMA", "different_schema")
    with pytest.raises(RuntimeError, match="must match"):
        migration.upgrade()
    assert statements == []


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
def test_raw_capture_downgrade_rejects_a_stale_transaction_snapshot(monkeypatch, isolation):
    migration, statements = _operations(monkeypatch, isolation=isolation)
    with pytest.raises(RuntimeError, match="custom_import_raw_objects_downgrade_isolation"):
        migration.downgrade()
    assert statements == []


def test_native_capture_fixture_installs_the_upgraded_retention_schema(monkeypatch):
    calls = []

    def load_migration(path, _module_name):
        migration = SimpleNamespace()
        migration.upgrade = lambda: calls.append((path.name, migration._schema()))
        return migration

    monkeypatch.setattr(custom_import_postgres_support, "_migration", load_migration)
    monkeypatch.setattr(custom_import_postgres_support, "Operations", lambda context: context)
    monkeypatch.setattr(
        custom_import_postgres_support, "MigrationContext", SimpleNamespace(configure=lambda connection: connection)
    )
    custom_import_postgres_support.install_segmented_capture_migration(object(), "synthetic_capture")
    assert calls == [
        ("20261002000000_custom_import_segmented_capture.py", "synthetic_capture"),
        ("20261017010000_custom_import_raw_objects.py", "synthetic_capture"),
    ]
