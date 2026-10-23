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


def _migration(revision=REVISION):
    path = ROOT / "alembic/versions" / (revision + ".py")
    specification = importlib.util.spec_from_file_location(revision, path)
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
    assert script.get_heads() == ["20261023220000_merge_directory_pricing_controls"]
    assert (
        script.get_revision("20261019030000_provider_directory_role_schema").down_revision
        == "20261019020000_registry_continuation_join"
    )
    assert script.get_revision("20261019020000_registry_continuation_join").down_revision == (
        "20261019010000_registry_raw_object_join",
        "20261019010000_custom_import_build_continuation",
    )
    assert script.get_revision("20261019010000_custom_import_build_continuation").down_revision == (
        "20261017010000_custom_import_raw_objects"
    )
    assert script.get_revision("20261017020000_source_registry_snapshot_join").down_revision == (
        "20261016060000_uhc_official_cohort_snapshots",
        "20261017010000_registry_cms_source_notifications",
    )
    assert script.get_revision("20261017010000_registry_cms_source_notifications").down_revision == (
        "20261016070000_registry_allowed_plan_scope"
    )
    assert script.get_revision("20261016070000_registry_allowed_plan_scope").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261016060000_uhc_official_cohort_snapshots").down_revision == (
        "20261016050000_acquisition_validator_argument_types"
    )
    assert script.get_revision("20261015010000_plan_release_binding_cohorts").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261015020000_custom_import_query_projections").down_revision == (
        "20261013010000_registry_custom_history_events"
    )
    assert script.get_revision(REVISION).down_revision == PREDECESSOR
    assert script.get_revision(PREDECESSOR).down_revision == "20261010020000_registry_ptg_office_retention"
    assert script._upgrade_revs("head", PREDECESSOR)[0].revision.revision == REVISION


def test_snapshot_branch_join_preserves_both_parents_without_database_operations(monkeypatch):
    migration = _migration("20261017020000_source_registry_snapshot_join")
    statements = []
    monkeypatch.setattr("alembic.op.execute", statements.append)
    assert migration.revision == "20261017020000_source_registry_snapshot_join"
    assert migration.down_revision == (
        "20261016060000_uhc_official_cohort_snapshots",
        "20261017010000_registry_cms_source_notifications",
    )
    assert migration.branch_labels is None and migration.depends_on is None
    assert migration.upgrade() is None
    assert migration.downgrade() is None
    assert statements == []


@pytest.mark.parametrize(
    "installed_revision,remaining_revisions",
    [
        (
            "20261016060000_uhc_official_cohort_snapshots",
            [
                "20261016070000_registry_allowed_plan_scope",
                "20261017010000_registry_cms_source_notifications",
                "20261017020000_source_registry_snapshot_join",
            ],
        ),
        (
            "20261017010000_registry_cms_source_notifications",
            [
                "20261015010000_plan_release_binding_cohorts",
                "20261015015000_generic_source_candidates",
                "20261016010000_cms_registry_source_pair_slot",
                "20261016030000_cms_source_guard_argument_types",
                "20261016040000_source_validator_argument_types",
                "20261016050000_acquisition_validator_argument_types",
                "20261016060000_uhc_official_cohort_snapshots",
                "20261017020000_source_registry_snapshot_join",
            ],
        ),
    ],
)
def test_snapshot_join_applies_only_the_missing_branch(installed_revision, remaining_revisions):
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert [
        step.revision.revision
        for step in script._upgrade_revs("20261017020000_source_registry_snapshot_join", installed_revision)
    ] == (remaining_revisions)


def test_raw_object_join_preserves_both_parents_without_database_operations(monkeypatch):
    migration = _migration("20261019010000_registry_raw_object_join")
    statements = []
    monkeypatch.setattr("alembic.op.execute", statements.append)
    assert migration.down_revision == (
        "20261018040000_registry_aca_source_notifications",
        "20261017010000_custom_import_raw_objects",
    )
    assert migration.branch_labels is None and migration.depends_on is None
    assert migration.upgrade() is None
    assert migration.downgrade() is None
    assert statements == []


@pytest.mark.parametrize(
    "installed_revision",
    (
        "20261018040000_registry_aca_source_notifications",
        "20261017010000_custom_import_raw_objects",
    ),
)
def test_raw_object_join_upgrades_either_installed_branch_once(installed_revision):
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    revisions = [
        step.revision.revision
        for step in script._upgrade_revs("20261019010000_registry_raw_object_join", installed_revision)
    ]
    assert len(revisions) == len(set(revisions))
    assert revisions[-1] == "20261019010000_registry_raw_object_join"
    assert installed_revision not in revisions
    assert "20261017210000_provider_directory_readiness_indexes" not in revisions
    if installed_revision == "20261018040000_registry_aca_source_notifications":
        assert revisions == [
            "20261017010000_custom_import_raw_objects",
            "20261019010000_registry_raw_object_join",
        ]
    else:
        assert revisions[-2] == "20261018040000_registry_aca_source_notifications"
        assert "20261017020000_source_registry_snapshot_join" in revisions
