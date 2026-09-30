# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Schema-only registration authority pins and retained history are guarded."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_PATH = Path(__file__).resolve().parents[1] / "alembic/versions/20260930030000_custom_import_registration_authority.py"


def _migration():
    """Load the version directly without mutating the configured database."""

    spec = importlib.util.spec_from_file_location("registration_authority_migration", _PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_empty_migration_preserves_pins_and_terminal_history(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", 'synthetic"schema')
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    sql = "\n".join(statements)
    assert migration.down_revision == "20260930020000_cms_npd_serving_coverage"
    assert 'CREATE TABLE "synthetic""schema"."custom_import_registration_authority"' in sql
    assert "FOREIGN KEY" not in sql and "INSERT INTO" not in sql
    assert "clock_timestamp()" in sql and "BETWEEN 2 AND 4096" in sql
    for field in ("authority_id", "input_sha256", "token_sha256", "expires_at", "created_at"):
        assert f"NEW.{field} IS DISTINCT FROM OLD.{field}" in sql
    assert "OLD.revoked_at IS NOT NULL" in sql and "OLD.result_receipt IS NOT NULL" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert sql.count("ENABLE ALWAYS TRIGGER") == 2
    assert "SET search_path = pg_catalog" in sql
    assert "SECURITY DEFINER" not in sql
    assert "REVOKE ALL ON TABLE" in sql and "REVOKE ALL ON FUNCTION" in sql


def test_downgrade_locks_and_refuses_retained_rows_before_dropping(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_authority")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.downgrade()
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "custom_import_registration_authority_downgrade_blocked" in statements[0]
    assert statements[1].startswith("DROP TABLE") and statements[2].startswith("DROP FUNCTION")


def test_schema_disagreement_stops_before_ddl(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "synthetic_one")
    monkeypatch.setenv("DB_SCHEMA", "synthetic_two")
    monkeypatch.setattr(migration.op, "execute", lambda _: pytest.fail("no DDL after schema disagreement"))
    with pytest.raises(RuntimeError, match="must match"):
        migration.upgrade()
