# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260731190000_tiger_zcta5_zip_index.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "tiger_zcta5_zip_index_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration_module)
    return migration_module


class _QueryResult:
    def __init__(self, *, scalar_value=None, first_value=None):
        self.scalar_value = scalar_value
        self.first_value = first_value

    def scalar(self):
        return self.scalar_value

    def first(self):
        return self.first_value


class _BindRecorder:
    def __init__(
        self,
        *,
        table_exists: bool,
        index_state=None,
        usable_index: bool = False,
    ):
        self.table_exists = table_exists
        self.index_state = self._index_row(index_state)
        self.usable_index = usable_index
        self.driver_statements: list[str] = []

    @staticmethod
    def _index_row(index_state):
        if index_state is None:
            return None
        return SimpleNamespace(
            _mapping=dict(
                zip(
                    (
                        "table_schema",
                        "table_name",
                        "indisvalid",
                        "indisready",
                        "indislive",
                    ),
                    index_state,
                )
            )
        )

    def execute(self, _statement, parameters):
        if parameters == {"table_name": "tiger.zcta5"}:
            return _QueryResult(scalar_value=self.table_exists)
        if parameters == {
            "schema": "tiger",
            "index_name": "zcta5_zcta5ce_idx",
        }:
            return _QueryResult(first_value=self.index_state)
        assert parameters == {
            "column_name": "zcta5ce",
            "table_name": "tiger.zcta5",
        }
        return _QueryResult(scalar_value=self.usable_index)

    def exec_driver_sql(self, statement: str):
        self.driver_statements.append(statement)


class _MigrationContext:
    def __init__(self, *, as_sql: bool = False):
        self.as_sql = as_sql

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _OperationsRecorder:
    def __init__(self, *, as_sql: bool = False, **bind_options):
        self.bind_recorder = _BindRecorder(**bind_options)
        self.migration_context = _MigrationContext(as_sql=as_sql)

    def get_bind(self):
        return self.bind_recorder

    def get_context(self):
        return self.migration_context


def _run_upgrade(monkeypatch, operations_recorder, *, index_matches=True):
    migration = _load_migration()
    monkeypatch.setattr(migration, "op", operations_recorder)
    monkeypatch.setattr(
        migration,
        "has_matching_index",
        lambda *_args, **_options: index_matches,
    )
    migration.upgrade()
    return migration


def test_upgrade_builds_required_concurrent_index(monkeypatch):
    operations = _OperationsRecorder(table_exists=True)

    migration = _run_upgrade(monkeypatch, operations)

    assert migration.down_revision == (
        "20260801140000_ptg2_legacy_v3_metadata_reconcile"
    )
    assert operations.bind_recorder.driver_statements == [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "zcta5_zcta5ce_idx" '
        'ON "tiger"."zcta5" ("zcta5ce");'
    ]


def test_upgrade_rebuilds_interrupted_canonical_index(monkeypatch):
    operations = _OperationsRecorder(
        table_exists=True,
        index_state=("tiger", "zcta5", False, False, True),
    )

    _run_upgrade(monkeypatch, operations)

    assert operations.bind_recorder.driver_statements == [
        'DROP INDEX CONCURRENTLY IF EXISTS "tiger"."zcta5_zcta5ce_idx";',
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "zcta5_zcta5ce_idx" '
        'ON "tiger"."zcta5" ("zcta5ce");',
    ]


def test_upgrade_adopts_equivalent_first_key_index(monkeypatch):
    operations = _OperationsRecorder(
        table_exists=True,
        usable_index=True,
    )

    _run_upgrade(monkeypatch, operations)

    assert operations.bind_recorder.driver_statements == []


def test_upgrade_rejects_canonical_name_owned_by_another_table(monkeypatch):
    operations = _OperationsRecorder(
        table_exists=True,
        index_state=("tiger", "another_table", True, True, True),
    )
    migration = _load_migration()
    monkeypatch.setattr(migration, "op", operations)

    with pytest.raises(
        RuntimeError,
        match="existing_schema_index_mismatch:tiger.zcta5_zcta5ce_idx",
    ):
        migration.upgrade()

    assert operations.bind_recorder.driver_statements == []


@pytest.mark.parametrize("as_sql,table_exists", [(True, False), (False, False)])
def test_upgrade_skips_when_index_cannot_be_managed(
    monkeypatch,
    as_sql,
    table_exists,
):
    operations = _OperationsRecorder(
        as_sql=as_sql,
        table_exists=table_exists,
    )

    _run_upgrade(monkeypatch, operations)

    assert operations.bind_recorder.driver_statements == []
