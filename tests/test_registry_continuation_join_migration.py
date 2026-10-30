# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Join both installed histories without replaying their common predecessor."""

import importlib.util
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

ROOT = Path(__file__).parents[1]
REVISION = "20261019020000_registry_continuation_join"
REGISTRY = "20261019010000_registry_raw_object_join"
CONTINUATION = "20261019010000_custom_import_build_continuation"
RAW_OBJECTS = "20261017010000_custom_import_raw_objects"

SOURCE_CHECK = "20261019150000_registry_source_check_idempotency"
STAGED_VERIFICATION = "20261020150000_custom_import_staged_verification"
STAGED_JOIN = "20261020160000_registry_staged_verification_join"
MANUAL_ACCEPTANCE = "20261020200000_custom_import_manual_acceptance"
MANUAL_JOIN = "20261020210000_registry_manual_acceptance_join"
CHECKER_EXECUTION = "20261022110000_ptg_checker_execution"
FIRST_RETIREMENT = "20261022120000_provider_directory_first_retirement"
DIRECTORY_PRICING_JOIN = "20261023220000_merge_directory_pricing_controls"
CURRENT_HEAD = "20261024050000_provider_directory_first_retirement_owner_lineage"
SERVING_RECOVERY = "20261023100000_provider_directory_serving_recovery"
COMPANY_AFFILIATION = "20261025010000_company_affiliation_states"
COMPANY_JOIN = "20261025020000_company_affiliation_join"
CATALOG_HEAD = "20261028010000_registry_fhir_catalog"
REFRESH_HEAD = "20261028020000_registry_membership_refresh"
INVENTORY_HEAD = "20261029010000_registry_source_inventory"
PTG_MEMBERSHIP = "20261021110000_ptg_membership_import_idempotency"


def _script():
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    return ScriptDirectory.from_config(config)


def test_join_preserves_both_completed_parent_histories():
    script = _script()
    parents = (
        "20261023100000_provider_directory_retirement_parent_updates",
        "20261023110000_code_pricing_import_idempotency",
    )
    assert script.get_revision(CURRENT_HEAD).down_revision == DIRECTORY_PRICING_JOIN
    assert script.get_revision(DIRECTORY_PRICING_JOIN).down_revision == parents
    assert all(script.get_revision(parent).down_revision == FIRST_RETIREMENT for parent in parents)
    assert script.get_revision(FIRST_RETIREMENT).down_revision == CHECKER_EXECUTION
    assert script.get_revision(SERVING_RECOVERY).down_revision == CURRENT_HEAD
    assert script.get_heads() == [INVENTORY_HEAD]
    assert script.get_revision(INVENTORY_HEAD).down_revision == REFRESH_HEAD
    assert script.get_revision(REFRESH_HEAD).down_revision == CATALOG_HEAD
    assert script.get_revision(CATALOG_HEAD).down_revision == COMPANY_JOIN
    assert script.get_revision(COMPANY_JOIN).down_revision == (SERVING_RECOVERY, COMPANY_AFFILIATION)
    assert script.get_revision(COMPANY_AFFILIATION).down_revision == DIRECTORY_PRICING_JOIN
    assert script.get_revision("20261019030000_provider_directory_role_schema").down_revision == REVISION
    assert script.get_revision(REVISION).down_revision == (REGISTRY, CONTINUATION)
    assert script.get_revision(REGISTRY).down_revision == (
        "20261018040000_registry_aca_source_notifications",
        RAW_OBJECTS,
    )
    assert script.get_revision(CONTINUATION).down_revision == RAW_OBJECTS


@pytest.mark.parametrize("installed", [REGISTRY, CONTINUATION])
def test_join_upgrade_from_either_parent_keeps_common_history_installed(installed):
    revisions = [step.revision.revision for step in _script()._upgrade_revs(REVISION, installed)]
    assert revisions[-1] == REVISION
    assert installed not in revisions
    assert RAW_OBJECTS not in revisions
    if installed == REGISTRY:
        assert revisions == [CONTINUATION, REVISION]
    else:
        assert REGISTRY in revisions


def test_join_upgrade_and_downgrade_require_no_database_operations():
    path = ROOT / "alembic" / "versions" / f"{REVISION}.py"
    spec = importlib.util.spec_from_file_location("registry_continuation_join", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    assert migration.upgrade() is None
    assert migration.downgrade() is None


def test_staged_join_preserves_parent_histories():
    script = _script()
    assert script.get_revision(SERVING_RECOVERY).down_revision == CURRENT_HEAD
    assert script.get_heads() == [INVENTORY_HEAD]
    assert script.get_revision(STAGED_JOIN).down_revision == (SOURCE_CHECK, STAGED_VERIFICATION)
    assert script.get_revision(STAGED_VERIFICATION).down_revision == CONTINUATION
    assert script.get_revision(SOURCE_CHECK).down_revision == "20261019140000_plan_pricing_attribution_idempotency"


@pytest.mark.parametrize(
    "installed, expected",
    [
        (SOURCE_CHECK, [STAGED_VERIFICATION, STAGED_JOIN]),
        (
            STAGED_VERIFICATION,
            [
                "20261016070000_registry_allowed_plan_scope",
                "20261017010000_registry_cms_source_notifications",
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
                REVISION,
                "20261019030000_provider_directory_role_schema",
                "20261019130000_cms_archive_transaction_guard",
                "20261019140000_plan_pricing_attribution_idempotency",
                SOURCE_CHECK,
                STAGED_JOIN,
            ],
        ),
        ((SOURCE_CHECK, STAGED_VERIFICATION), [STAGED_JOIN]),
    ],
    ids=["registry-parent", "verification-parent", "both-parents"],
)
def test_staged_join_preserves_upgrade_paths(installed, expected):
    revisions = [step.revision.revision for step in _script()._upgrade_revs(STAGED_JOIN, installed)]
    assert revisions == expected
    assert CONTINUATION not in revisions
    assert RAW_OBJECTS not in revisions


def test_staged_join_uses_no_database():
    migration = _script().get_revision(STAGED_JOIN).module
    assert migration.branch_labels is None
    assert migration.depends_on is None
    assert migration.upgrade() is None
    assert migration.downgrade() is None


def test_manual_join_preserves_parent_histories_without_database_operations():
    script = _script()
    assert script.get_revision(SERVING_RECOVERY).down_revision == CURRENT_HEAD
    assert script.get_heads() == [INVENTORY_HEAD]
    revision = script.get_revision(MANUAL_JOIN)
    assert revision.down_revision == (STAGED_JOIN, PTG_MEMBERSHIP)
    assert script.get_revision(PTG_MEMBERSHIP).down_revision == MANUAL_ACCEPTANCE
    migration = revision.module
    assert migration.branch_labels is None
    assert migration.depends_on is None
    assert migration.upgrade() is None
    assert migration.downgrade() is None


@pytest.mark.parametrize("installed", [(STAGED_JOIN, MANUAL_ACCEPTANCE), MANUAL_ACCEPTANCE, STAGED_JOIN])
def test_manual_join_does_not_replay_completed_parent_histories(installed):
    script = _script()
    revisions = [step.revision.revision for step in script._upgrade_revs(MANUAL_JOIN, installed)]
    assert revisions[-1] == MANUAL_JOIN
    assert RAW_OBJECTS not in revisions
    assert STAGED_VERIFICATION not in revisions
    if isinstance(installed, tuple):
        assert revisions == [PTG_MEMBERSHIP, MANUAL_JOIN]
    elif installed == STAGED_JOIN:
        assert revisions == [
            "20261020180000_custom_import_verification_arms",
            "20261020190000_custom_import_verification_root_ranges",
            MANUAL_ACCEPTANCE,
            PTG_MEMBERSHIP,
            MANUAL_JOIN,
        ]
    else:
        assert STAGED_JOIN in revisions
        assert MANUAL_ACCEPTANCE not in revisions


@pytest.mark.parametrize("installed", [(STAGED_JOIN, PTG_MEMBERSHIP), PTG_MEMBERSHIP])
def test_join_preserves_completed_membership_migration(installed):
    revisions = [step.revision.revision for step in _script()._upgrade_revs(MANUAL_JOIN, installed)]
    assert revisions[-1] == MANUAL_JOIN
    assert PTG_MEMBERSHIP not in revisions
    assert MANUAL_ACCEPTANCE not in revisions
    if isinstance(installed, tuple):
        assert revisions == [MANUAL_JOIN]
