# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from db.models import EntityAddressUnified


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260827210000_entity_address_geo_taxonomy.py"
)
MIGRATION_SPEC = spec_from_file_location(
    "entity_address_geo_taxonomy_migration",
    MIGRATION_PATH,
)
migration = module_from_spec(MIGRATION_SPEC)
assert MIGRATION_SPEC and MIGRATION_SPEC.loader
MIGRATION_SPEC.loader.exec_module(migration)


class _Context:
    as_sql = False

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


class _Bind:
    def __init__(self):
        self.statements = []

    def exec_driver_sql(self, statement):
        self.statements.append(statement)


class _Operations:
    def __init__(self):
        self.bind = _Bind()
        self.executed = []

    def execute(self, statement):
        self.executed.append(statement)

    def get_bind(self):
        return self.bind

    def get_context(self):
        return _Context()


def test_migration_matches_model_contract():
    runtime_indexes = [
        index
        for index in EntityAddressUnified.__my_additional_indexes__
        if index.get("name") == "geo_taxonomy"
    ]

    assert runtime_indexes == [
        {
            "index_elements": migration.INDEX_EXPRESSIONS,
            "using": "gist",
            "name": "geo_taxonomy",
            "where": migration.INDEX_PREDICATE,
        }
    ]
    assert migration.down_revision == (
        "20260828090000_npi_search_taxonomy_projection"
    )


def test_migration_uses_concurrent_gist_ddl_and_analyze():
    create_sql = migration._create_index_sql("fixture")

    assert create_sql.startswith(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        '"entity_address_unified_idx_geo_taxonomy" '
        'ON "fixture"."entity_address_unified" USING gist '
    )
    assert all(expression in create_sql for expression in migration.INDEX_EXPRESSIONS)
    assert create_sql.endswith(f"WHERE {migration.INDEX_PREDICATE}")
    assert migration._drop_index_sql("fixture") == (
        "DROP INDEX CONCURRENTLY IF EXISTS "
        '"fixture"."entity_address_unified_idx_geo_taxonomy"'
    )
    assert migration._analyze_sql("fixture") == (
        'ANALYZE "fixture"."entity_address_unified"'
    )


@pytest.mark.parametrize(
    ("is_valid", "is_ready", "is_live"),
    (
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ),
)
def test_migration_rebuilds_interrupted_index(
    monkeypatch,
    is_valid,
    is_ready,
    is_live,
):
    monkeypatch.setattr(
        migration,
        "_index_catalog_record",
        lambda *_args: {
            "indisvalid": is_valid,
            "indisready": is_ready,
            "indislive": is_live,
        },
    )

    assert migration._matching_index_record("fixture", "expected") is False


def test_migration_rejects_wrong_valid_index_shape(monkeypatch):
    monkeypatch.setattr(
        migration,
        "_index_catalog_record",
        lambda *_args: {
            "indisvalid": True,
            "indisready": True,
            "indislive": True,
        },
    )
    monkeypatch.setattr(migration, "_shape_from_catalog", lambda _record: "wrong")

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration._matching_index_record("fixture", "expected")


def test_migration_rejects_same_name_owned_by_another_relation(monkeypatch):
    monkeypatch.setattr(migration, "_index_catalog_record", lambda *_args: None)
    monkeypatch.setattr(migration, "_same_name_relation_exists", lambda _schema: True)

    with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
        migration._matching_index_record("fixture", "expected")


def test_upgrade_adopts_manual_index_and_analyzes(monkeypatch):
    operations = _Operations()
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setattr(migration, "_table_exists", lambda _schema: True)
    monkeypatch.setattr(migration, "_expected_index_shape", lambda _schema: "shape")
    monkeypatch.setattr(
        migration,
        "_matching_index_record",
        lambda _schema, _shape: {"indisvalid": True},
    )

    migration.upgrade()

    assert operations.executed == [
        migration.ENSURE_EXTENSION_SQL,
        'ANALYZE "mrf"."entity_address_unified"',
    ]
    assert operations.bind.statements == []


def test_upgrade_skips_missing_table_after_ensuring_extension(monkeypatch):
    operations = _Operations()
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setattr(migration, "_table_exists", lambda _schema: False)

    migration.upgrade()

    assert operations.executed == [migration.ENSURE_EXTENSION_SQL]
    assert operations.bind.statements == []


@pytest.mark.parametrize("initial_record", (None, False))
def test_upgrade_builds_or_repairs_then_analyzes(monkeypatch, initial_record):
    operations = _Operations()
    records = iter((initial_record, {"indisvalid": True}))
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setattr(migration, "_table_exists", lambda _schema: True)
    monkeypatch.setattr(migration, "_expected_index_shape", lambda _schema: "shape")
    monkeypatch.setattr(
        migration,
        "_matching_index_record",
        lambda _schema, _shape: next(records),
    )

    migration.upgrade()

    expected_statements = [migration._create_index_sql("mrf")]
    if initial_record is False:
        expected_statements.insert(0, migration._drop_index_sql("mrf"))
    assert operations.bind.statements == expected_statements
    assert operations.executed[-1] == 'ANALYZE "mrf"."entity_address_unified"'


def test_downgrade_drops_only_the_canonical_index(monkeypatch):
    operations = _Operations()
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    assert operations.executed == [migration._drop_index_sql("mrf")]
    assert operations.bind.statements == []
