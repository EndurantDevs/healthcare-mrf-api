# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recovery storage follows the mandatory overlay, including before Address exists."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


def _migration():
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261023100000_provider_directory_serving_recovery.py"
    )
    spec = importlib.util.spec_from_file_location("serving_recovery_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _operations(owner):
    queries, statements = [], []

    def execute(statement, parameters):
        queries.append((str(statement), parameters))
        return SimpleNamespace(scalar_one_or_none=lambda: owner)

    return (
        SimpleNamespace(get_bind=lambda: SimpleNamespace(execute=execute), execute=statements.append),
        queries,
        statements,
    )


@pytest.mark.parametrize("owner", ["ordinary_builder", "protected_owner"])
def test_recovery_migration_follows_only_existing_overlay_owner(monkeypatch, owner):
    migration = _migration()
    operations, queries, statements = _operations(owner)
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setenv("DB_SCHEMA", "fixture")

    migration.upgrade()

    query, parameters = queries[0]
    assert parameters == {"overlay": '"fixture"."provider_directory_address_overlay"'}
    assert "entity_address_unified" not in query and "rolcanlogin" not in query
    assert "role.oid=relation.relowner" in query
    assert "relation.relkind='r'" in query and "relation.relpersistence='p'" in query
    assert "NOT relation.relispartition" in query
    assert statements[1] == f'ALTER TABLE "fixture"."provider_directory_serving_recovery" OWNER TO "{owner}"'
    assert statements[2] == 'REVOKE ALL ON "fixture"."provider_directory_serving_recovery" FROM PUBLIC'
    assert len(statements) == 3


def test_recovery_migration_refuses_missing_or_invalid_overlay_before_creating_storage(monkeypatch):
    migration = _migration()
    operations, queries, statements = _operations(None)
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setenv("DB_SCHEMA", "fixture")

    with pytest.raises(RuntimeError, match="provider_directory_serving_recovery_overlay_required"):
        migration.upgrade()

    assert len(queries) == 1
    assert statements == []
