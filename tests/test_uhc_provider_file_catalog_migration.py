# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import sqlalchemy as sa
import pytest

from process import uhc_provider_file_catalog_store as catalog_store


def _load_catalog_migration():
    migration_path = Path(
        "alembic/versions/20260720110000_uhc_provider_file_catalog.py"
    )
    migration_spec = importlib.util.spec_from_file_location(
        "uhc_catalog_migration",
        migration_path,
    )
    assert migration_spec is not None and migration_spec.loader is not None
    migration = importlib.util.module_from_spec(migration_spec)
    migration_spec.loader.exec_module(migration)
    return migration


class _MigrationRecorder:
    def __init__(self):
        self.tables_by_name = {}
        self.indexes_by_name = {}
        self.dropped_tables = []

    def create_table(self, name, *table_elements, schema=None):
        if name in self.tables_by_name:
            raise RuntimeError(f"table already exists: {name}")
        self.tables_by_name[name] = {
            "table_elements": table_elements,
            "schema": schema,
        }

    def create_index(self, name, table_name, columns, **index_options):
        self.indexes_by_name[name] = {
            "table_name": table_name,
            "columns": tuple(columns),
            **index_options,
        }

    def drop_table(self, name, **_drop_options):
        self.dropped_tables.append(name)
        self.tables_by_name.pop(name, None)


def _constraint_columns(constraint):
    return tuple(
        column if isinstance(column, str) else column.name
        for column in constraint._pending_colargs
    )


def _check_expressions(migration_recorder, table_name):
    return {
        str(table_element.sqltext)
        for table_element in migration_recorder.tables_by_name[table_name][
            "table_elements"
        ]
        if isinstance(table_element, sa.CheckConstraint)
    }


def test_catalog_migration_upgrade_downgrade_is_additive(monkeypatch):
    migration = _load_catalog_migration()
    migration_recorder = _MigrationRecorder()
    monkeypatch.setattr(migration, "op", migration_recorder)

    migration.upgrade()

    assert migration.down_revision == (
        "20260720100000_provider_directory_bulk_output_resume"
    )
    assert set(migration_recorder.tables_by_name) == {
        catalog_store.CATALOG_SET_TABLE,
        catalog_store.CATALOG_FILE_TABLE,
        catalog_store.RAW_OBSERVATION_TABLE,
    }
    assert any(
        "catalog_set_sha256 ~ '^[0-9a-f]{64}$'" in expression
        for expression in _check_expressions(
            migration_recorder,
            catalog_store.CATALOG_SET_TABLE,
        )
    )
    file_checks = _check_expressions(
        migration_recorder,
        catalog_store.CATALOG_FILE_TABLE,
    )
    assert any("file_id ~ '^[0-9a-f]{64}$'" in expression for expression in file_checks)
    assert any(
        "catalog_entry_sha256 ~ '^[0-9a-f]{64}$'" in expression
        for expression in file_checks
    )
    assert any(
        "size_bytes<=9223372036854775807" in expression
        for expression in file_checks
    )
    assert any(
        "raw_set_sha256 ~ '^[0-9a-f]{64}$'" in expression
        and "catalog_set_sha256 ~ '^[0-9a-f]{64}$'" in expression
        for expression in _check_expressions(
            migration_recorder,
            catalog_store.RAW_OBSERVATION_TABLE,
        )
    )
    assert len(migration_recorder.indexes_by_name) == 3
    migration.downgrade()
    assert migration_recorder.dropped_tables == [
        catalog_store.RAW_OBSERVATION_TABLE,
        catalog_store.CATALOG_FILE_TABLE,
        catalog_store.CATALOG_SET_TABLE,
    ]
    migration.upgrade()
    assert set(migration_recorder.tables_by_name) == {
        catalog_store.CATALOG_SET_TABLE,
        catalog_store.CATALOG_FILE_TABLE,
        catalog_store.RAW_OBSERVATION_TABLE,
    }


def test_catalog_migration_rejects_preexisting_lookalike_table(monkeypatch):
    migration = _load_catalog_migration()
    migration_recorder = _MigrationRecorder()
    migration_recorder.tables_by_name[catalog_store.CATALOG_SET_TABLE] = {
        "table_elements": (),
        "schema": "mrf",
    }
    monkeypatch.setattr(migration, "op", migration_recorder)

    with pytest.raises(RuntimeError, match="table already exists"):
        migration.upgrade()
