# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Projection metadata is additive and preserves immutable historical seals."""

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.custom_import import CustomImportGenerationSeal
from db.models.custom_import_query_projection import CustomImportQueryProjection
from tests.custom_import_postgres_support import _migration


def migration():
    return _migration(
        Path(__file__).parents[1] / "alembic" / "versions" / "20261015020000_custom_import_query_projections.py",
        "query_projection_migration_test",
    )


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


def _assert_projection_upgrade_order(script, module):
    """Preserve the exact projection upgrade order through both joined histories."""
    assert [
        step.revision.revision
        for step in script._upgrade_revs("20261025020000_company_affiliation_join", module.down_revision)
    ] == [
        module.revision,
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
        "20261023220000_merge_directory_pricing_controls",
        "20261025010000_company_affiliation_states",
        "20261024050000_provider_directory_first_retirement_owner_lineage",
        "20261023100000_provider_directory_serving_recovery",
        "20261025020000_company_affiliation_join",
    ]


def test_projection_migration_extends_the_single_installed_head():
    """Keep projection metadata in the sole chain beneath registry admission."""
    root = Path(__file__).parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(config)
    module = migration()
    assert module.down_revision == "20261013010000_registry_custom_history_events"
    assert script.get_heads() == ["20261031030000_profile_initial_inverse_guard"]
    assert script.get_revision("20261029010000_registry_source_inventory").down_revision == (
        "20261028020000_registry_membership_refresh"
    )
    assert script.get_revision("20261028020000_registry_membership_refresh").down_revision == (
        "20261028010000_registry_fhir_catalog"
    )
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
    assert script.get_revision("20261016070000_registry_allowed_plan_scope").down_revision == module.revision
    _assert_projection_upgrade_order(script, module)


def test_migration_creates_only_additive_native_metadata_in_the_selected_schema():
    module = migration()
    table = module._manifest_table("synthetic_control")
    sql = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CREATE TABLE synthetic_control.custom_import_query_projection" in sql
    assert "REFERENCES synthetic_control.custom_import_generation" in sql
    assert "REFERENCES synthetic_control.custom_import_snapshot_family" in sql
    assert "mrf." not in sql
    source = Path(module.__file__).read_text()
    assert all(term not in source for term in ("CREATE FUNCTION", "CREATE TRIGGER", "UPDATE ", "SET ROLE"))
    assert not CustomImportQueryProjection.__runtime_schema_sync__


def test_historical_nullable_seal_fields_require_complete_projection_evidence():
    module = migration()
    seal = CustomImportGenerationSeal.__table__
    assert all(seal.c[name].nullable and seal.c[name].server_default is None for name in module._SEAL_COLUMNS)
    check = next(constraint for constraint in seal.constraints if constraint.name == module._CHECK)
    sql = str(check.sqltext)
    assert all(f"{name} IS NULL" in sql for name in module._SEAL_COLUMNS)
    assert all(f"{name} IS NOT NULL" in sql for name in module._SEAL_COLUMNS[1:])
    assert "query_projection_root_count >= 0" in sql and "query_projection_child_count >= 0" in sql
