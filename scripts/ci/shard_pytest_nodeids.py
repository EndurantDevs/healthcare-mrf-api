"""Write one deterministic exact-once pytest node-id shard."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path
from typing import Sequence

TEST_PROCESS_TIMEOUT_SECONDS = "295s"
MAIN_TEST_SHARDS = {
    "tests/test_ptg2_v3_capacity_gate_request_boundaries.py::test_resource_measurements_are_bound_to_fresh_observation_windows": 5,
    "tests/test_custom_import_snowflake_operator_cli.py::test_registration_entry_point_redacts_rejected_stdin": 5,
    "tests/test_custom_import_geo_receiver_fixture.py::test_geo_receiver_hydrates_every_npi_in_a_separate_process": 5,
    "tests/test_migration_adoption_offline.py::test_online_candidate_migrations_reject_offline_sql_without_stamping[20261005120000_practitioner_set_validation:20261005130000_provider_dataset_candidates-provider_dataset_migration_requires_online_connection]": 5,
    "tests/test_ptg2_tax_identity_source_publication_parser.py::test_publication_parser_import_orders_in_fresh_interpreter[validation-first]": 0,
    "tests/test_ptg2_tax_identity_source_publication_parser.py::test_publication_parser_import_orders_in_fresh_interpreter[parser-first]": 2,
    "tests/test_ptg2_v3_capacity_gate_trust.py::test_import_and_audit_rows_require_unique_joined_ordered_stages": 3,
    "tests/test_ci_shard_plugin.py::test_xdist_shards_execute_the_legacy_sorted_assignment_once[1-loadscope-6]": 5,
    "tests/test_control_imports_api.py::test_provider_directory_runtime_contract_preflight_passes": 1,
}


REGISTRY_TEST_SHARDS = {
    "tests/test_cms_publication_source_session_postgres.py::test_default_pool_and_reader_privileges_are_preserved": 5,
    "tests/test_cms_publication_source_session_postgres.py::test_owner_handle_stays_out_of_prepared_manifest[initial]": 4,
    "tests/test_company_network_link_store_postgres.py::test_concurrent_identical_commands_return_one_immutable_batch": 3,
    "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[recipe-isolation]": 4,
    "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[recipe-manifest]": 4,
    "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[selection-approved]": 4,
    "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[selection-candidate]": 4,
    "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[custody-timeout]": 1,
    "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair": 2,
    "tests/test_registry_approval_preview_postgres.py::test_complete_imported_counts_above_copy_page[rebind-1-10000-0]": 6,
    "tests/test_registry_approval_preview_postgres.py::test_complete_imported_counts_above_copy_page[close-1-5000-5000]": 6,
    "tests/test_cms_publication_source_session_postgres.py::test_prepared_owner_handle_is_optional_and_private": 0,
    "tests/test_network_custom_address_source_postgres.py::test_bulk_approved_pairs_have_fixed_native_query_count": 2,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_wal_attribution_preserves_original_refusal": 5,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_dependency_refusal_rolls_back_both_drops[external_view]": 5,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_tampering_refuses_removal[address_acl]": 1,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_tampering_refuses_removal[raw_acl]": 0,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_catalog_identity_preserves_search_path_and_rejects_index_loss": 5,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_cleanup_requires_caller_isolation_and_uncontended_locks": 5,
    "tests/test_registry_source_recipe_composition_postgres.py::test_generic_source_cannot_enter_production_stage": 2,
    "tests/test_registry_source_import_postgres.py::test_module_entrypoint_admits_through_actual_configured_database": 4,
    "tests/test_registry_approval_preview_postgres.py::test_cms_preview_counts_all_source_rows_above_both_native_page_limits[close-retained_cms_preview1]": 3,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_tampering_refuses_removal[comment]": 5,
    "tests/test_network_membership_serving_indexes_postgres.py::test_large_candidate_uses_same_statement_count[20000]": 6,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds0]": 3,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved": 1,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_prepared_decode_and_exact_removal[tagged]": 3,
}


EXCLUSIVE_REGISTRY_TEST_SHARDS = {
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds0]": 0,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds1]": 1,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_source_notification_rollback_preserves_incumbent": 2,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_signed_retention_budget_refuses_before_copy[copy_bounds0]": 3,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_signed_retention_budget_refuses_before_copy[copy_bounds1]": 4,
    "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved": 5,
    "tests/test_registry_cms_source_consumer_postgres.py::test_committed_notification_refresh_replay_review_and_owned_lock": 6,
    "tests/test_registry_cms_source_consumer_initial_postgres.py::test_initial_source_approval_publication_and_replay_after_later_head": 7,
    "tests/test_registry_cms_source_consumer_race_postgres.py::test_checkpoint_race_retries_real_publication_with_latest_custom[serialization]": 8,
    "tests/test_registry_cms_source_consumer_race_postgres.py::test_checkpoint_race_retries_real_publication_with_latest_custom[cas]": 9,
    "tests/test_registry_cms_source_mixed_postgres.py::test_mixed_cms_refresh_retains_nonmanual_ptg_office_and_latest_custom": 10,
    "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair": 11,
    "tests/test_registry_publication_consumer_postgres.py::test_provider_custody_timeout_retains_lease_and_resumes_exact_request": 12,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retention_preparation_failures_preserve_source[copy_bounds0]": 13,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retention_preparation_failures_preserve_source[copy_bounds1]": 14,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retained_pair_survives_receipt_head_refresh[copy_bounds0]": 15,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retained_pair_survives_receipt_head_refresh[copy_bounds1]": 16,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retention_address_copy_interruptions_preserve_source[copy_bounds0]": 17,
    "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_retention_address_copy_interruptions_preserve_source[copy_bounds1]": 18,
}


def pytest_addoption(parser) -> None:
    """Allow CI workers to select their shard during the execution collection."""

    parser.addoption("--ci-shard-count", type=int, default=4)
    parser.addoption("--ci-shard-index", type=int)


def pytest_collection_modifyitems(config, items) -> None:
    """Keep the same sorted, exact-once assignment without a collect-only run."""

    import pytest

    shard_index = config.getoption("ci_shard_index")
    if shard_index is None:
        return
    shard_count = config.getoption("ci_shard_count")
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise pytest.UsageError("CI shard index must be in [0, positive shard count)")
    nodeids = [item.nodeid for item in items]
    if len(nodeids) != len(set(nodeids)):
        raise pytest.UsageError("pytest collection returned duplicate node IDs")
    selected_ids = set(select_nodeids(nodeids, shard_count=shard_count, shard_index=shard_index))
    if not selected_ids:
        raise pytest.UsageError("selected shard has no node IDs")
    config.hook.pytest_deselected(items=[item for item in items if item.nodeid not in selected_ids])
    items[:] = sorted((item for item in items if item.nodeid in selected_ids), key=lambda item: item.nodeid)


def parse_arguments(arguments: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse and validate one deterministic shard request."""

    parser = argparse.ArgumentParser(description="Collect pytest node IDs and write one deterministic shard.")
    parser.add_argument("--shard-count", type=int, required=True)
    parser.add_argument("--shard-index", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("pytest_arguments", nargs=argparse.REMAINDER)
    parsed = parser.parse_args(arguments)
    if parsed.shard_count < 1:
        parser.error("--shard-count must be positive")
    if not 0 <= parsed.shard_index < parsed.shard_count:
        parser.error("--shard-index must be in [0, --shard-count)")
    if parsed.pytest_arguments[:1] == ["--"]:
        parsed.pytest_arguments = parsed.pytest_arguments[1:]
    return parsed


def collection_command(pytest_arguments: Sequence[str]) -> list[str]:
    """Build the bounded pytest collection command."""

    return [
        "timeout",
        "--foreground",
        TEST_PROCESS_TIMEOUT_SECONDS,
        sys.executable,
        "-m",
        "pytest",
        "--collect-only",
        "-q",
        *pytest_arguments,
    ]


def collect_nodeids(pytest_arguments: Sequence[str]) -> list[str]:
    """Collect unique sorted repository test node IDs."""

    completed = subprocess.run(
        collection_command(pytest_arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    nodeids = sorted(line for line in completed.stdout.splitlines() if line.startswith("tests/") and "::" in line)
    if not nodeids:
        raise ValueError("pytest collection returned no node IDs")
    if len(nodeids) != len(set(nodeids)):
        raise ValueError("pytest collection returned duplicate node IDs")
    return nodeids


def shard_index_for_nodeid(nodeid: str, shard_count: int) -> int:
    """Map one node ID to a stable shard index."""
    if shard_count == 6 and nodeid in MAIN_TEST_SHARDS:
        return MAIN_TEST_SHARDS[nodeid]
    if shard_count in (32, 48):
        exclusive = EXCLUSIVE_REGISTRY_TEST_SHARDS.get(nodeid)
        if exclusive is not None:
            return exclusive
        digest = hashlib.sha256(nodeid.encode("utf-8")).digest()
        reserved = len(EXCLUSIVE_REGISTRY_TEST_SHARDS)
        return reserved + int.from_bytes(digest, byteorder="big") % (shard_count - reserved)
    if shard_count == 16 and nodeid in (
        "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair",
        "tests/test_registry_publication_consumer_postgres.py::test_provider_custody_timeout_retains_lease_and_resumes_exact_request",
    ):
        # Balance expensive publication proofs under the existing process limit.
        return 15
    if shard_count == 16:
        if nodeid == (
            "tests/test_network_registry_cms_prepared_pair_postgres.py"
            "::test_prepared_retention_native_binding[copy_bounds1]"
        ):
            return 12
        if nodeid == (
            "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved"
        ):
            return 9
    digest = hashlib.sha256(nodeid.encode("utf-8")).digest()
    shard_index = int.from_bytes(digest, byteorder="big") % shard_count
    if shard_count == 8:
        # Balance expensive proofs without changing the remaining hash assignment.
        if nodeid in REGISTRY_TEST_SHARDS:
            return REGISTRY_TEST_SHARDS[nodeid]
        if shard_index == 1 and nodeid.split("::", 1)[0] == (
            "tests/test_network_registry_cms_prepared_cleanup_postgres.py"
        ):
            return 7
        if shard_index == 0 and nodeid.split("::", 1)[0] == (
            "tests/test_network_registry_cms_prepared_cleanup_postgres.py"
        ):
            return 5
        if shard_index == 2 and nodeid.split("::", 1)[0] in (
            "tests/test_registry_source_selection_receipt_postgres.py",
            "tests/test_network_initial_source_office_bindings_postgres.py",
            "tests/test_network_legacy_alias_adoption_postgres.py",
        ):
            return 1
        if shard_index == 2 and nodeid.split("::", 1)[0] in (
            "tests/test_registry_publication_execution_postgres.py",
            "tests/test_provider_directory_cms_resource_batch_postgres.py",
            "tests/test_network_registry_cms_prepared_cleanup_postgres.py",
            "tests/test_registry_source_recipe_composition_postgres.py",
        ):
            return 7
        if shard_index == 6 and nodeid.split("::", 1)[0] in (
            "tests/test_network_registry_cms_prepared_pair_postgres.py",
            "tests/test_cms_publication_source_session_postgres.py",
        ):
            return 5
    return shard_index


def select_nodeids(
    nodeids: Sequence[str],
    *,
    shard_count: int,
    shard_index: int,
) -> list[str]:
    """Return the sorted node IDs assigned to one shard."""

    return [nodeid for nodeid in sorted(nodeids) if shard_index_for_nodeid(nodeid, shard_count) == shard_index]


def write_nodeids(output: Path, nodeids: Sequence[str]) -> None:
    """Write one pytest response file with one node ID per line."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("".join(f"{nodeid}\n" for nodeid in nodeids), encoding="utf-8")


def main(arguments: Sequence[str] | None = None) -> int:
    """Collect, select, and write one requested shard."""

    parsed = parse_arguments(arguments)
    nodeids = collect_nodeids(parsed.pytest_arguments)
    selected = select_nodeids(
        nodeids,
        shard_count=parsed.shard_count,
        shard_index=parsed.shard_index,
    )
    if not selected:
        raise ValueError("selected shard has no node IDs")
    write_nodeids(parsed.output, selected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
