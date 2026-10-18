# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen notification storage, configured schema and retained-history downgrade."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

REVISION = "20261017010000_registry_cms_source_notifications"


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions" / (REVISION + ".py")
    specification = importlib.util.spec_from_file_location(REVISION, path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_notification_migration_freezes_native_ddl_and_exact_predecessor():
    from process.registry_cms_source_notifications import CREATE_TABLE_SQL

    migration = _migration()
    statements = migration._ddl("example_registry")
    assert migration.down_revision == "20261016070000_registry_allowed_plan_scope"
    assert statements == (
        CREATE_TABLE_SQL.format(namespace='"example_registry"'),
        'REVOKE ALL ON "example_registry".registry_cms_source_notification FROM PUBLIC',
    )
    source = Path(migration.__file__).read_text()
    assert "from process" not in source and "CREATE_TABLE_SQL" not in source
    assert all(
        token not in " ".join(statements).upper()
        for token in ("CREATE FUNCTION", "CREATE TRIGGER", "REFERENCES", "GRANT ")
    )


@pytest.mark.parametrize("schema", [None, 17, "", 'bad"name', "a.b", "a" * 64, "x\n"])
def test_notification_migration_rejects_invalid_schema_before_sql(schema):
    with pytest.raises(ValueError):
        _migration()._ddl(schema)


@pytest.mark.parametrize("configured", [None, "example_registry"])
def test_notification_upgrade_preserves_schema_selection(monkeypatch, configured):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fallback_schema")
    if configured is None:
        monkeypatch.delenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", configured)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert statements == list(migration._ddl(configured or "fallback_schema"))


@pytest.mark.parametrize("populated", [False, True])
def test_notification_downgrade_locks_and_preserves_retained_history(monkeypatch, populated):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "example_registry")
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setattr(migration.op, "get_bind", lambda: SimpleNamespace(scalar=lambda _query: populated))
    if populated:
        with pytest.raises(RuntimeError, match="retained-data migration"):
            migration.downgrade()
    else:
        migration.downgrade()
    assert statements[0] == 'LOCK TABLE "example_registry".registry_cms_source_notification IN ACCESS EXCLUSIVE MODE'
    assert len(statements) == (1 if populated else 2)
