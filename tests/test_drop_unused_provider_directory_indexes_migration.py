# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryPractitionerRole,
    ProviderDirectorySourceResource,
)

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20261110010000_drop_unused_provider_directory_indexes.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location("drop_unused_provider_directory_indexes", MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _executed(operation: MagicMock) -> list[str]:
    return [call.args[0] for call in operation.get_bind.return_value.exec_driver_sql.call_args_list]


def test_upgrade_drops_each_index_concurrently_and_idempotently(monkeypatch) -> None:
    migration = _load_migration()
    operation = MagicMock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "pd")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.down_revision == "20261108010000_custom_import_family_child_reference_aggregate"
    operation.get_context.return_value.autocommit_block.assert_called_once()
    assert _executed(operation) == [
        f'DROP INDEX CONCURRENTLY IF EXISTS "pd"."{name}"' for name, _table, _definition in migration.INDEXES
    ]


def test_downgrade_restores_the_frozen_definitions(monkeypatch) -> None:
    migration = _load_migration()
    operation = MagicMock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "pd")
    monkeypatch.setattr(migration, "op", operation)

    migration.downgrade()

    statements = _executed(operation)
    assert len(statements) == len(migration.INDEXES)
    assert all(sql.startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS ") for sql in statements)
    assert 'ON "pd"."provider_directory_practitioner_role" USING gin ((code_codes::jsonb))' in statements[-1]


def test_models_no_longer_declare_dropped_indexes() -> None:
    # db.maintenance recreates any model-declared index, so models and the migration must agree.
    dropped_names = {name for name, _table, _definition in _load_migration().INDEXES}
    declared_names = {
        spec["name"]
        for model in (
            ProviderDirectoryCanonicalResource,
            ProviderDirectoryPractitionerRole,
            ProviderDirectorySourceResource,
        )
        for spec in model.__my_additional_indexes__
    }
    assert dropped_names.isdisjoint(declared_names)
    assert "provider_directory_role_network_refs_gin_idx" in declared_names
