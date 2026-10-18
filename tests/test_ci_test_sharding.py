"""Contracts for deterministic CI pytest sharding."""

from __future__ import annotations

import hashlib
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "ci" / "shard_pytest_nodeids.py"
SPEC = importlib.util.spec_from_file_location("shard_pytest_nodeids", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
SHARDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SHARDER
SPEC.loader.exec_module(SHARDER)


def test_shards_are_sorted_disjoint_and_exact_once() -> None:
    nodeids = [
        "tests/test_alpha.py::test_one",
        "tests/test_alpha.py::test_two",
        "tests/test_beta.py::test_three",
        "tests/test_beta.py::test_four",
        "tests/test_gamma.py::test_five",
    ]

    for count in (2, 6):
        shards = [SHARDER.select_nodeids(nodeids, shard_count=count, shard_index=index) for index in range(count)]
        assert all(shard == sorted(shard) for shard in shards)
        assert sorted(node for shard in shards for node in shard) == sorted(nodeids)


def test_collection_command_has_the_hard_test_process_limit() -> None:
    command = SHARDER.collection_command(["--ignore", "tests/capacity.py"])

    assert command[:3] == ["timeout", "--foreground", "295s"]
    assert command[3:7] == [sys.executable, "-m", "pytest", "--collect-only"]
    assert command[-2:] == ["--ignore", "tests/capacity.py"]


def test_cli_collects_and_assigns_each_temporary_test_once(tmp_path: Path) -> None:
    test_root = tmp_path / "tests"
    test_root.mkdir()
    (test_root / "test_sample.py").write_text(
        "def test_one():\n    assert True\n\ndef test_two():\n    assert True\n",
        encoding="utf-8",
    )
    outputs = [tmp_path / f"shard-{index}.txt" for index in range(2)]

    for index, output in enumerate(outputs):
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--shard-count",
                "2",
                "--shard-index",
                str(index),
                "--output",
                str(output),
                "--",
                str(test_root),
            ],
            check=True,
            cwd=tmp_path,
        )

    assigned_nodeids = [nodeid for output in outputs for nodeid in output.read_text(encoding="utf-8").splitlines()]
    assert sorted(assigned_nodeids) == [
        "tests/test_sample.py::test_one",
        "tests/test_sample.py::test_two",
    ]


def test_registry_cost_moves_preserve_all_other_assignments_and_exact_once() -> None:
    """Move expensive proofs while preserving every other assignment and the hard limit."""
    publication = (
        "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair"
    )
    preview = (
        "tests/test_registry_approval_preview_postgres.py"
        "::test_complete_imported_counts_above_copy_page[rebind-1-10000-0]"
    )
    assignment_by_nodeid = {publication: 2, preview: 6}
    swapped_files = (
        "tests/test_network_registry_cms_prepared_pair_postgres.py",
        "tests/test_cms_publication_source_session_postgres.py",
    )
    balanced_files = (
        "tests/test_registry_publication_execution_postgres.py",
        "tests/test_provider_directory_cms_resource_batch_postgres.py",
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py",
        "tests/test_registry_source_recipe_composition_postgres.py",
    )
    nodeids = [publication, preview, "tests/test_unrelated.py::test_one"]
    nodeids.extend(f"{path}::test_case[{index}]" for path in (*swapped_files, *balanced_files) for index in range(100))
    shards = [SHARDER.select_nodeids(nodeids, shard_count=8, shard_index=index) for index in range(8)]
    assert publication in shards[2]
    assert preview in shards[6]
    assert sorted(node for shard in shards for node in shard) == sorted(nodeids)
    for node in nodeids:
        legacy = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % 8
        expected = assignment_by_nodeid.get(node, legacy)
        if legacy == 6 and node.split("::", 1)[0] in swapped_files:
            expected = 5
        if legacy == 2 and node.split("::", 1)[0] in balanced_files:
            expected = 7
        if legacy == 0 and node.split("::", 1)[0] == "tests/test_network_registry_cms_prepared_cleanup_postgres.py":
            expected = 5
        if legacy == 1 and node.split("::", 1)[0] == "tests/test_network_registry_cms_prepared_cleanup_postgres.py":
            expected = 7
        assert SHARDER.shard_index_for_nodeid(node, 8) == expected
        for count in (1, 2, 4, 6):
            assert SHARDER.shard_index_for_nodeid(node, count) == (
                int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % count
            )


def test_registry_rebalance_preserves_unmoved_nodeids_and_all_cases() -> None:
    """Spread measured native cohorts while keeping every other hash and smaller matrix stable."""
    preview = "tests/test_registry_approval_preview_postgres.py"
    cleanup = "tests/test_network_registry_cms_prepared_cleanup_postgres.py"
    moves_by_scope = {
        (0, cleanup): 5,
        (1, cleanup): 7,
        (2, "tests/test_registry_source_selection_receipt_postgres.py"): 1,
        (2, "tests/test_network_initial_source_office_bindings_postgres.py"): 1,
        (2, "tests/test_network_legacy_alias_adoption_postgres.py"): 1,
    }
    assignment_by_nodeid = {
        "tests/test_network_custom_address_source_postgres.py::test_bulk_approved_pairs_have_fixed_native_query_count": 2,
        "tests/test_cms_publication_source_session_postgres.py::test_default_pool_and_reader_privileges_are_preserved": 5,
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_wal_attribution_preserves_original_refusal": 5,
        "tests/test_cms_publication_source_session_postgres.py::test_prepared_owner_handle_is_optional_and_private": 0,
        f"{cleanup}::test_native_dependency_refusal_rolls_back_both_drops[external_view]": 0,
        f"{cleanup}::test_native_tampering_refuses_removal[address_acl]": 1,
        f"{cleanup}::test_native_catalog_identity_preserves_search_path_and_rejects_index_loss": 5,
        f"{cleanup}::test_native_cleanup_requires_caller_isolation_and_uncontended_locks": 5,
        "tests/test_registry_source_recipe_composition_postgres.py::test_generic_source_cannot_enter_production_stage": 2,
        f"{cleanup}::test_native_tampering_refuses_removal[raw_acl]": 0,
        "tests/test_company_network_link_store_postgres.py::test_concurrent_identical_commands_return_one_immutable_batch": 3,
        "tests/test_registry_source_import_postgres.py::test_module_entrypoint_admits_through_actual_configured_database": 4,
        "tests/test_cms_publication_source_session_postgres.py::test_owner_handle_stays_out_of_prepared_manifest[initial]": 4,
        f"{preview}::test_complete_imported_counts_above_copy_page[close-1-5000-5000]": 6,
    }
    for boundary in (
        "recipe-isolation",
        "recipe-manifest",
        "selection-approved",
        "selection-candidate",
        "custody-timeout",
    ):
        assignment_by_nodeid[
            "tests/test_registry_publication_consumer_postgres.py"
            f"::test_retained_recipe_database_failure_resumes_exact_request[{boundary}]"
        ] = 4
    assignment_by_nodeid.update(_timing_move_expectations())
    nodeids = [*assignment_by_nodeid, "tests/test_unrelated.py::test_one"]
    nodeids.extend(
        f"{path}::test_case[{index}]"
        for path in {preview, *(path for _, path in moves_by_scope)}
        for index in range(100)
    )
    shards = [SHARDER.select_nodeids(nodeids, shard_count=8, shard_index=index) for index in range(8)]
    assert sorted(node for shard in shards for node in shard) == sorted(nodeids)
    for node in nodeids:
        legacy = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % 8
        expected = moves_by_scope.get((legacy, node.split("::", 1)[0]), legacy)
        if legacy == 2 and node.split("::", 1)[0] == cleanup:
            expected = 7
        expected = assignment_by_nodeid.get(node, expected)
        assert node in shards[expected]
        assert SHARDER.shard_index_for_nodeid(node, 8) == expected
        for count in (1, 2, 4):
            assert SHARDER.shard_index_for_nodeid(node, count) == (
                int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big") % count
            )


def _timing_move_expectations() -> dict[str, int]:
    """Keep the measured case fixture separate from the existing scope assertions."""
    return {
        "tests/test_registry_approval_preview_postgres.py::test_cms_preview_counts_all_source_rows_above_both_native_page_limits[close-retained_cms_preview1]": 3,
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_tampering_refuses_removal[comment]": 5,
        "tests/test_registry_publication_consumer_postgres.py::test_retained_recipe_database_failure_resumes_exact_request[custody-timeout]": 1,
        "tests/test_network_membership_serving_indexes_postgres.py::test_large_candidate_uses_same_statement_count[20000]": 6,
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds0]": 3,
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved": 1,
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_prepared_decode_and_exact_removal[tagged]": 3,
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_dependency_refusal_rolls_back_both_drops[external_view]": 5,
    }


def test_sixteen_shard_publication_parity() -> None:
    publication = (
        "tests/test_network_cms_registry_complete_publication_postgres.py::test_complete_publication_and_retained_pair"
    )
    custody_retry = (
        "tests/test_registry_publication_consumer_postgres.py"
        "::test_provider_custody_timeout_retains_lease_and_resumes_exact_request"
    )
    assignment_by_nodeid = {
        publication: 15,
        custody_retry: 15,
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds1]": 12,
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved": 9,
    }
    nodeids = [
        *assignment_by_nodeid,
        custody_retry + "[variant]",
        custody_retry.replace("tests/", "other/", 1),
        publication,
        publication + "[variant]",
        publication.replace("tests/", "other/", 1),
        publication.replace("::test_complete", "::TestPublication::test_complete"),
        "tests/test_network_cms_registry_complete_publication_postgres.py"
        "::test_native_retention_input_keeps_independent_source_and_profile_dates",
        *SHARDER.REGISTRY_TEST_SHARDS,
        *(f"tests/test_unrelated.py::test_case[{index}]" for index in range(5000)),
    ]
    nodeids = sorted(set(nodeids))
    shards = [SHARDER.select_nodeids(nodeids, shard_count=16, shard_index=index) for index in range(16)]

    assert publication in shards[15]
    assert publication not in shards[5]
    assert custody_retry in shards[15]
    assert custody_retry not in shards[1]
    assert sorted(node for shard in shards for node in shard) == nodeids
    assert all(shard == sorted(shard) for shard in shards)
    for node in nodeids:
        digest = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big")
        assert SHARDER.shard_index_for_nodeid(node, 16) == assignment_by_nodeid.get(node, digest % 16)
        for count in (1, 2, 4, 6, 12, 64):
            assert SHARDER.shard_index_for_nodeid(node, count) == digest % count
    assert SHARDER.shard_index_for_nodeid(publication, 8) == 2
    assert SHARDER.shard_index_for_nodeid(custody_retry, 8) == 1


def test_expensive_native_publication_cases_use_distinct_shards() -> None:
    """Keep real publication proofs apart without dropping cases or raising the process limit."""
    nodeids = [
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds0]",
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_prepared_retention_native_binding[copy_bounds1]",
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_source_notification_rollback_preserves_incumbent",
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_signed_retention_budget_refuses_before_copy[copy_bounds0]",
        "tests/test_network_registry_cms_prepared_pair_postgres.py::test_native_signed_retention_budget_refuses_before_copy[copy_bounds1]",
        "tests/test_network_registry_cms_prepared_cleanup_postgres.py::test_native_finalized_pair_is_preserved",
        "tests/test_registry_cms_source_consumer_postgres.py::test_committed_notification_refresh_replay_review_and_owned_lock",
        "tests/test_registry_cms_source_consumer_initial_postgres.py::test_initial_source_approval_publication_and_replay_after_later_head",
        "tests/test_registry_cms_source_consumer_race_postgres.py::test_checkpoint_race_retries_real_publication_with_latest_custom[serialization]",
        "tests/test_registry_cms_source_consumer_race_postgres.py::test_checkpoint_race_retries_real_publication_with_latest_custom[cas]",
        "tests/test_registry_cms_source_mixed_postgres.py::test_mixed_cms_refresh_retains_nonmanual_ptg_office_and_latest_custom",
    ]
    assignments = [SHARDER.shard_index_for_nodeid(node, 16) for node in nodeids]
    assert len(set(assignments)) == len(nodeids)
    shards = [SHARDER.select_nodeids(nodeids, shard_count=16, shard_index=index) for index in range(16)]
    assert sorted(node for shard in shards for node in shard) == sorted(nodeids)
    assert SHARDER.TEST_PROCESS_TIMEOUT_SECONDS == "295s"


@pytest.mark.parametrize("shard_count", (32, 48))
def test_registry_shards_isolate_long_publications_and_preserve_every_case(shard_count) -> None:
    """Long proofs keep their process budget without sharing it with ordinary cases."""
    heavy_nodeids = list(SHARDER.EXCLUSIVE_REGISTRY_TEST_SHARDS)
    ordinary_nodeids = [
        *(node + "[variant]" for node in heavy_nodeids),
        *(node.replace("tests/", "other/", 1) for node in heavy_nodeids),
        *(f"tests/test_unrelated.py::test_case[{index}]" for index in range(5000)),
    ]
    nodeids = heavy_nodeids + ordinary_nodeids
    shards = [
        SHARDER.select_nodeids(nodeids, shard_count=shard_count, shard_index=index) for index in range(shard_count)
    ]
    assert len(heavy_nodeids) == 19
    assert shards[:19] == [[node] for node in heavy_nodeids]
    assert all(shards)
    assert sorted(node for shard in shards for node in shard) == sorted(nodeids)
    assert all(shard == sorted(shard) for shard in shards)
    for node in ordinary_nodeids:
        digest = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big")
        assert SHARDER.shard_index_for_nodeid(node, shard_count) == 19 + digest % (shard_count - 19)
    assert SHARDER.TEST_PROCESS_TIMEOUT_SECONDS == "295s"


def _main_cost_moves() -> dict[str, tuple[int, int]]:
    """Bind exact measured cases to their original and balanced six-shard assignments."""
    return {
        "tests/test_ptg2_v3_capacity_gate_request_boundaries.py::test_resource_measurements_are_bound_to_fresh_observation_windows": (
            0,
            5,
        ),
        "tests/test_custom_import_snowflake_operator_cli.py::test_registration_entry_point_redacts_rejected_stdin": (
            1,
            5,
        ),
        "tests/test_custom_import_geo_receiver_fixture.py::test_geo_receiver_hydrates_every_npi_in_a_separate_process": (
            1,
            5,
        ),
        "tests/test_migration_adoption_offline.py::test_online_candidate_migrations_reject_offline_sql_without_stamping[20261005120000_practitioner_set_validation:20261005130000_provider_dataset_candidates-provider_dataset_migration_requires_online_connection]": (
            1,
            5,
        ),
        "tests/test_ptg2_tax_identity_source_publication_parser.py::test_publication_parser_import_orders_in_fresh_interpreter[validation-first]": (
            1,
            0,
        ),
        "tests/test_ptg2_tax_identity_source_publication_parser.py::test_publication_parser_import_orders_in_fresh_interpreter[parser-first]": (
            1,
            2,
        ),
        "tests/test_ptg2_v3_capacity_gate_trust.py::test_import_and_audit_rows_require_unique_joined_ordered_stages": (
            1,
            3,
        ),
        "tests/test_ci_shard_plugin.py::test_xdist_shards_execute_the_legacy_sorted_assignment_once[1-loadscope-6]": (
            2,
            5,
        ),
        "tests/test_control_imports_api.py::test_provider_directory_runtime_contract_preflight_passes": (3, 1),
    }


def test_main_cost_moves_preserve_other_layouts() -> None:
    moves_by_nodeid = _main_cost_moves()
    assert SHARDER.MAIN_TEST_SHARDS == {node: destination for node, (_original, destination) in moves_by_nodeid.items()}
    nodeids = [*moves_by_nodeid, *SHARDER.REGISTRY_TEST_SHARDS, *SHARDER.EXCLUSIVE_REGISTRY_TEST_SHARDS]
    nodeids.extend(node + "[variant]" for node in moves_by_nodeid)
    nodeids.extend(node.replace("tests/", "other/", 1) for node in moves_by_nodeid)
    nodeids.extend(f"tests/test_unrelated.py::test_case[{index}]" for index in range(5000))
    nodeids = sorted(set(nodeids))
    shards = [SHARDER.select_nodeids(nodeids, shard_count=6, shard_index=index) for index in range(6)]
    assert sorted(node for shard in shards for node in shard) == nodeids
    assert all(shard == sorted(shard) for shard in shards)
    for node in nodeids:
        digest = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big")
        original, destination = moves_by_nodeid.get(node, (digest % 6, digest % 6))
        assert original == digest % 6
        assert node in shards[destination]
        for count in (1, 2, 3, 4, 5, 7, 12, 64):
            assert SHARDER.shard_index_for_nodeid(node, count) == digest % count
    for node, (_original, _destination) in moves_by_nodeid.items():
        digest = int.from_bytes(hashlib.sha256(node.encode()).digest(), byteorder="big")
        for count in (8, 16):
            assert SHARDER.shard_index_for_nodeid(node, count) == digest % count
        for count in (32, 48):
            assert SHARDER.shard_index_for_nodeid(node, count) == 19 + digest % (count - 19)
    assert SHARDER.TEST_PROCESS_TIMEOUT_SECONDS == "295s"
