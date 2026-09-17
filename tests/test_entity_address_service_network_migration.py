# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from db.models import EntityAddressUnified

MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260917120000_entity_address_service_network.py"
)
MIGRATION_SPEC = spec_from_file_location(
    "entity_address_service_network_migration",
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


def test_migration_matches_serving_model_contract():
    runtime_indexes = [
        index
        for index in EntityAddressUnified.__my_additional_indexes__
        if index.get("name") == "service_plans_network_array"
    ]

    assert runtime_indexes == [
        {
            "index_elements": migration.INDEX_EXPRESSIONS,
            "using": "gin",
            "name": "service_plans_network_array",
            "where": migration.INDEX_PREDICATE,
        }
    ]
    assert migration.down_revision == "20260917130000_custom_import_generation_finality"
    assert migration._create_index_sql("fixture") == (
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        '"entity_address_unified_idx_service_plans_network_array" '
        'ON "fixture"."entity_address_unified" USING gin '
        "(plans_network_array gin__int_ops) "
        "WHERE type IN ('primary', 'secondary', 'practice', 'site')"
    )


@pytest.mark.parametrize("initial_record", (None, False))
def test_upgrade_builds_or_repairs_then_analyzes(monkeypatch, initial_record):
    operations = _Operations()
    records = iter((initial_record, {"indisvalid": True}))
    monkeypatch.setattr(migration, "op", operations)
    monkeypatch.setattr(migration, "_has_table", lambda _schema: True)
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
    assert operations.executed == [
        migration.ENSURE_EXTENSION_SQL,
        'ANALYZE "mrf"."entity_address_unified"',
    ]


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


def test_downgrade_drops_only_the_service_network_index(monkeypatch):
    operations = _Operations()
    monkeypatch.setattr(migration, "op", operations)

    migration.downgrade()

    assert operations.executed == [migration._drop_index_sql("mrf")]
