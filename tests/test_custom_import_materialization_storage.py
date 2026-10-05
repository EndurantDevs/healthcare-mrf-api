# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Installation and bounded routing contracts; live authority has native tests."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import_storage import CustomImportRevisionHome


def _migration():
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261005070000_custom_import_materialization_storage.py"
    )
    spec = importlib.util.spec_from_file_location("materialization_storage_contract", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _statements(monkeypatch, *, downgrade=False):
    migration = _migration()
    statements = []
    monkeypatch.setattr(migration, "_schema", lambda: 'synthetic "schema')
    monkeypatch.setattr(migration.op, "execute", statements.append)
    (migration.downgrade if downgrade else migration.upgrade)()
    return statements


def test_installation_renders_every_fixed_function_and_closes_native_acls(monkeypatch):
    statements = _statements(monkeypatch)
    functions = [sql for sql in statements if sql.startswith("CREATE FUNCTION")]
    revokes = [sql for sql in statements if sql.startswith("REVOKE ALL ON FUNCTION")]
    assert len(functions) == len(revokes) == 15
    assert all("SECURITY DEFINER SET search_path=pg_catalog" in sql for sql in functions)
    assert any("finish_custom_import_materialization_page() FROM PUBLIC" in sql for sql in revokes)
    assert any("append_custom_import_revision_home(bigint,bigint[],bigint[]) FROM PUBLIC" in sql for sql in revokes)
    assert all("__CONTROL" not in sql and "__LEAF_DDL__" not in sql for sql in statements)
    assert any('"synthetic ""schema".custom_import_revision_home' in sql for sql in statements)
    installer = next(
        sql for sql in functions if ".install_custom_import_materialization_writers(" in sql.splitlines()[0]
    )
    assert installer.count("$materialization_install$") == 2
    assert not any(sql.startswith(("GRANT", "INSERT", "UPDATE", "DELETE")) for sql in statements)
    assert not any("DROP TRIGGER" in sql or "DISABLE TRIGGER" in sql for sql in statements)


def test_revision_home_keeps_native_exact_sets_and_separate_kind_domains():
    ddl = str(CreateTable(CustomImportRevisionHome.__table__).compile(dialect=postgresql.dialect()))
    assert "INT8MULTIRANGE NOT NULL" in ddl
    assert ddl.count("EXCLUDE USING gist (revision_ids WITH &&)") == 2
    assert "PRIMARY KEY (revision_kind, first_revision_id)" in ddl
    assert CustomImportRevisionHome.__runtime_schema_sync__ is False
    resources = _migration()._resource("homes.sql")
    lookup = next(sql for sql in resources if "CREATE FUNCTION __CONTROL__.lookup_" in sql)
    assert "matched AS MATERIALIZED" in lookup and "h.revision_ids*w.revision_ids" in lookup
    assert "m.revision_ids@>r.id" in lookup and "h.revision_ids@>r.id" not in lookup
    append = resources[-1]
    assert "cardinality(p_root_ids)+cardinality(p_child_ids)>100000" in append
    assert "FROM fresh GROUP BY revision_kind" in append and "ON CONFLICT" not in append
    assert append.count("lock_custom_import_materialization_storage(p_family_id,true)") == 2


def test_deferred_completion_follows_storage_without_new_commit_time_lease_check():
    resources = _migration()._resource("control.sql")
    completion = next(sql for sql in resources if "CREATE FUNCTION __CONTROL__.finish_" in sql)
    assert "NEW.transaction_id<>pg_current_xact_id()" in completion
    assert "check_custom_import_materialization_completion" in completion
    assert "verify_custom_import_materialization_writers" in completion
    assert "lock_custom_import_materialization_set" not in completion
    assert "clock_timestamp" not in completion and "expires_at" not in completion
    assert "DELETE FROM __CONTROL__.custom_import_materialization_page" in completion


def test_downgrade_handles_trigger_signature_and_refuses_retained_homes(monkeypatch):
    statements = _statements(monkeypatch, downgrade=True)
    assert "ACCESS EXCLUSIVE MODE" in statements[0]
    assert "custom_import_materialization_storage_downgrade_blocked" in statements[0]
    assert any("finish_custom_import_materialization_page()" in sql for sql in statements)
    assert not any("CASCADE" in sql or "DROP SCHEMA" in sql for sql in statements)


def test_packaged_resources_are_allowlisted():
    migration = _migration()
    assert migration.down_revision == "20261005060000_custom_import_snapshot_finality"
    with pytest.raises(ValueError, match="unknown"):
        migration._resource("../other.sql")
