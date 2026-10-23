# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen additive origin DDL, physical column order and retained-data refusal."""

import importlib.util
import re
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import CheckConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

from db.models.company_registry import CompanyRegistry

ROOT = Path(__file__).parents[1]
REVISION = "20261012010000_company_registry_name_origin"
PREDECESSOR = "20261011010000_company_registry_group_assertions"
CURRENT_HEAD = "20261023220000_merge_directory_pricing_controls"
_ORIGIN_UPGRADE_REVISIONS = [
    REVISION,
    "20261013010000_registry_custom_history_events",
    "20261015020000_custom_import_query_projections",
    "20261016070000_registry_allowed_plan_scope",
    "20261017010000_registry_cms_source_notifications",
    "20261017210000_provider_directory_readiness_indexes",
    "20261017010000_custom_import_raw_objects",
    "20261019010000_custom_import_build_continuation",
    "20261020150000_custom_import_staged_verification",
    "20261020180000_custom_import_verification_arms",
    "20261020190000_custom_import_verification_root_ranges",
    "20261020200000_custom_import_manual_acceptance",
    "20261021110000_ptg_membership_import_idempotency",
    "20261015010000_plan_release_binding_cohorts",
    "20261015015000_generic_source_candidates",
    "20261016010000_cms_registry_source_pair_slot",
    "20261016030000_cms_source_guard_argument_types",
    "20261016040000_source_validator_argument_types",
    "20261016050000_acquisition_validator_argument_types",
    "20261016060000_uhc_official_cohort_snapshots",
    "20261017020000_source_registry_snapshot_join",
    "20261017030000_registry_ptg_office_recipe_selection",
    "20261017040000_nppes_evidence_candidates",
    "20261018011000_network_candidate_custody",
    "20261018040000_registry_aca_source_notifications",
    "20261019010000_registry_raw_object_join",
    "20261019020000_registry_continuation_join",
    "20261019030000_provider_directory_role_schema",
    "20261019130000_cms_archive_transaction_guard",
    "20261019140000_plan_pricing_attribution_idempotency",
    "20261019150000_registry_source_check_idempotency",
    "20261020160000_registry_staged_verification_join",
    "20261020210000_registry_manual_acceptance_join",
    "20261022110000_ptg_checker_execution",
    "20261022120000_provider_directory_first_retirement",
    "20261023110000_code_pricing_import_idempotency",
    "20261023100000_provider_directory_retirement_parent_updates",
    CURRENT_HEAD,
]


def _migration(revision=REVISION):
    path = ROOT / "alembic/versions" / (revision + ".py")
    specification = importlib.util.spec_from_file_location(revision, path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


def test_origin_migration_matches_frozen_model_without_rewriting_retained_data():
    migration = _migration()
    statements = migration._ddl("example_registry")
    assert len(statements) == 2
    assert statements[0] == (
        'ALTER TABLE "example_registry".company_registry '
        "ADD COLUMN display_name_origin VARCHAR(32) DEFAULT 'unknown' NOT NULL"
    )
    column = CompanyRegistry.__table__.c.display_name_origin
    compiled = str(CreateColumn(column).compile(dialect=postgresql.dialect()))
    assert "".join(compiled.split()) in "".join(statements[0].split())
    constraint = next(
        constraint
        for constraint in CompanyRegistry.__table__.constraints
        if isinstance(constraint, CheckConstraint) and constraint.name == "registry_company_name_origin"
    )
    assert statements[1] == (
        f'ALTER TABLE "example_registry".company_registry ADD CONSTRAINT {constraint.name} CHECK ({constraint.sqltext})'
    )
    assert "('manual','source_company','source_issuer','identifier','unknown')" in statements[1]
    assert all(
        keyword not in " ".join(statements).upper()
        for keyword in ("UPDATE ", "INSERT ", "DROP ", "CREATE FUNCTION", "CREATE TRIGGER", "GRANT ")
    )
    assert "registry_record_history" not in " ".join(statements)
    assert "registry_approved_record" not in " ".join(statements)
    with pytest.raises(RuntimeError, match="retained-data migration"):
        migration.downgrade()


def test_origin_column_is_appended_after_the_original_physical_company_columns():
    original = _migration("20261007010000_managed_network_registry")
    definition = original._TABLE_DEFINITIONS_BY_NAME["company_registry"]
    original_columns = re.findall(
        r"(?:^|,)\s*([a-z_]+)\s+(?:uuid|varchar|boolean|bigint|jsonb|timestamptz)\b", definition, re.M
    )
    assert original_columns == ["company_id", "display_name", "roles", "aliases", "archived", "revision", "created_at"]
    assert list(CompanyRegistry.__table__.columns.keys()) == [*original_columns, "display_name_origin"]


@pytest.mark.parametrize("schema", [None, 17, "", 'bad"name', "a.b", "a" * 64, "x\n"])
def test_origin_migration_rejects_invalid_schema_before_sql(schema):
    with pytest.raises(ValueError):
        _migration()._ddl(schema)


@pytest.mark.parametrize(
    "registry_schema,database_schema,expected",
    [
        ("example_registry", "example_database", "example_registry"),
        (None, "example_database", "example_database"),
        (None, None, "mrf"),
    ],
)
def test_origin_upgrade_preserves_existing_schema_selection(monkeypatch, registry_schema, database_schema, expected):
    migration = _migration()
    for key, setting in (("HLTHPRT_NETWORK_REGISTRY_SCHEMA", registry_schema), ("HLTHPRT_DB_SCHEMA", database_schema)):
        if setting is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, setting)
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    assert statements == migration._ddl(expected)


def test_invalid_origin_schema_refuses_upgrade_before_any_statement(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", 'bad"name')
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    with pytest.raises(ValueError):
        migration.upgrade()
    assert statements == []


def _assert_custom_import_manual_acceptance_lineage(script):
    """Preserve the joined custom-import verification and acceptance ancestry."""
    assert script.get_revision("20261020210000_registry_manual_acceptance_join").down_revision == (
        "20261020160000_registry_staged_verification_join",
        "20261021110000_ptg_membership_import_idempotency",
    )
    assert script.get_revision("20261021110000_ptg_membership_import_idempotency").down_revision == (
        "20261020200000_custom_import_manual_acceptance"
    )
    assert script.get_revision("20261020200000_custom_import_manual_acceptance").down_revision == (
        "20261020190000_custom_import_verification_root_ranges"
    )
    assert script.get_revision("20261020190000_custom_import_verification_root_ranges").down_revision == (
        "20261020180000_custom_import_verification_arms"
    )
    assert script.get_revision("20261020180000_custom_import_verification_arms").down_revision == (
        "20261020150000_custom_import_staged_verification"
    )
    assert script.get_revision("20261020150000_custom_import_staged_verification").down_revision == (
        "20261019010000_custom_import_build_continuation"
    )
    assert script.get_revision("20261019010000_custom_import_build_continuation").down_revision == (
        "20261017010000_custom_import_raw_objects"
    )
    assert script.get_revision("20261017010000_custom_import_raw_objects").down_revision == (
        "20261017210000_provider_directory_readiness_indexes"
    )
    assert script.get_revision("20261017210000_provider_directory_readiness_indexes").down_revision == (
        "20261015020000_custom_import_query_projections"
    )


def test_origin_migration_extends_the_sole_existing_head():
    """Preserve both migration histories and their exact joined upgrade order."""
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    assert script.get_heads() == [CURRENT_HEAD]
    _assert_custom_import_manual_acceptance_lineage(script)
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
    assert script.get_revision("20261017010000_custom_import_raw_objects").down_revision == (
        "20261017210000_provider_directory_readiness_indexes"
    )
    assert script.get_revision("20261017210000_provider_directory_readiness_indexes").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    history = "20261013010000_registry_custom_history_events"
    assert script.get_revision("20261016060000_uhc_official_cohort_snapshots").down_revision == (
        "20261016050000_acquisition_validator_argument_types"
    )
    assert script.get_revision("20261015010000_plan_release_binding_cohorts").down_revision == (
        "20261015020000_custom_import_query_projections"
    )
    assert script.get_revision("20261015020000_custom_import_query_projections").down_revision == history
    assert script.get_revision(history).down_revision == REVISION
    assert script.get_revision(REVISION).down_revision == PREDECESSOR
    assert script.get_revision(PREDECESSOR).down_revision == "20261010030000_registry_source_admission_receipt"
    assert [step.revision.revision for step in script._upgrade_revs("head", PREDECESSOR)] == _ORIGIN_UPGRADE_REVISIONS
