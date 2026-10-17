# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen additive DDL, current head and exact immutable assertion privileges."""

import importlib.util
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

from db.models.company_registry_assertions import CompanyRegistryGroupAssertion
from process import registry_management_permissions as permissions

ROOT = Path(__file__).parents[1]
REVISION = "20261011010000_company_registry_group_assertions"
PREDECESSOR = "20261010030000_registry_source_admission_receipt"


def _migration():
    path = ROOT / "alembic/versions" / (REVISION + ".py")
    specification = importlib.util.spec_from_file_location(REVISION, path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_additive_group_assertion_migration_matches_frozen_model_and_index():
    migration = _migration()
    statements = migration._ddl("example_registry")
    assert len(statements) == 3
    normalized = "".join(statements[0].split())
    table = CompanyRegistryGroupAssertion.__table__
    for column in table.columns:
        expected = str(CreateColumn(column).compile(dialect=postgresql.dialect()))
        assert "".join(expected.split()) in normalized
    for constraint in table.constraints:
        if isinstance(constraint, CheckConstraint):
            expected = f"CONSTRAINT{constraint.name}CHECK({constraint.sqltext})"
            assert "".join(expected.split()) in normalized
    assert "PRIMARYKEY(assertion_id,company_id,company_revision)" in normalized
    assert statements[1] == (
        'CREATE INDEX company_group_assertion_revision_idx ON "example_registry".'
        "company_registry_group_assertion (company_id, company_revision)"
    )
    assert statements[2] == 'REVOKE ALL ON "example_registry".company_registry_group_assertion FROM PUBLIC'
    assert all(
        word not in " ".join(statements).upper()
        for word in (
            "CREATE FUNCTION",
            "CREATE TRIGGER",
            "REFERENCES",
            "ALTER TABLE",
            "INSERT INTO",
            "UPDATE ",
            "GRANT ",
        )
    )
    assert not table.foreign_keys
    with pytest.raises(RuntimeError, match="retained-data migration"):
        migration.downgrade()


@pytest.mark.parametrize("schema", [None, 17, "", 'bad"name', "a.b", "a" * 64, "x\n"])
def test_group_migration_rejects_invalid_schema_before_sql(schema):
    with pytest.raises(ValueError):
        _migration()._ddl(schema)


def test_group_assertions_share_exact_insert_only_management_policy():
    model = CompanyRegistryGroupAssertion
    name = model.__tablename__
    assert model in permissions._MODELS and model not in permissions._HEAD_MODELS
    assert permissions._TABLES[name] == tuple(model.__table__.columns.keys())
    assert permissions._INSERT_COLUMNS[name] == tuple(
        column for column in model.__table__.columns.keys() if column != "created_at"
    )
    assert name not in permissions._UPDATE_COLUMNS


def test_group_migration_extends_only_the_actual_current_receipt_head():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == ["20261016070000_registry_allowed_plan_scope"]
    assert script.get_revision("20261016070000_registry_allowed_plan_scope").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision(REVISION).down_revision == PREDECESSOR
    assert script.get_revision(PREDECESSOR).down_revision == "20261010020000_registry_ptg_office_retention"
    assert script._upgrade_revs("head", PREDECESSOR)[0].revision.revision == REVISION
