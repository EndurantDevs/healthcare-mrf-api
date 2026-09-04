"""Contracts for deterministic CI pytest sharding."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPOSITORY_ROOT / "scripts" / "ci" / "shard_pytest_nodeids.py"
SPEC = importlib.util.spec_from_file_location("shard_pytest_nodeids", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
SHARDER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SHARDER
SPEC.loader.exec_module(SHARDER)


def test_two_shards_are_sorted_disjoint_and_exact_once() -> None:
    nodeids = [
        "tests/test_alpha.py::test_one",
        "tests/test_alpha.py::test_two",
        "tests/test_beta.py::test_three",
        "tests/test_beta.py::test_four",
        "tests/test_gamma.py::test_five",
    ]

    first = SHARDER.select_nodeids(nodeids, shard_count=2, shard_index=0)
    second = SHARDER.select_nodeids(nodeids, shard_count=2, shard_index=1)

    assert first == sorted(first)
    assert second == sorted(second)
    assert set(first).isdisjoint(second)
    assert sorted((*first, *second)) == sorted(nodeids)


def test_collection_command_has_the_hard_test_process_limit() -> None:
    command = SHARDER.collection_command(["--ignore", "tests/capacity.py"])

    assert command[:3] == ["timeout", "--foreground", "295s"]
    assert command[3:7] == [sys.executable, "-m", "pytest", "--collect-only"]
    assert command[-2:] == ["--ignore", "tests/capacity.py"]


def test_slow_envelope_suites_run_once_across_the_parallel_main_lanes() -> None:
    """Shard every bounded envelope suite across the existing main lanes."""

    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
    )
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    expected = (
        "tests/test_plan_pricing_projection_v3_census_envelope_admission.py",
        "tests/test_plan_pricing_projection_v3_census_envelope_interrupts.py",
        "tests/test_plan_pricing_projection_v3_census_envelope_runtime_safety.py",
        "tests/test_plan_pricing_projection_v3_census_envelope_safety.py",
    )
    base_capacity_paths = tuple(workflow["env"]["CAPACITY_TEST_PATHS"].splitlines())
    expected_definition = "envelope_test_paths=$'" + "\\n".join(expected) + "'"
    python_main = prepush.split("run_python_main() {", 1)[1].split(
        "run_capacity() {", 1
    )[0]
    capacity = prepush.split("run_capacity() {", 1)[1].split(
        "run_python_coverage() {", 1
    )[0]
    assert set(expected).isdisjoint(base_capacity_paths)
    assert "base_capacity_test_paths=$capacity_test_paths" in prepush
    assert expected_definition in prepush
    assert "capacity_test_paths+=$'\\n'\"$envelope_test_paths\"" in prepush
    assert 'collection_args+=(--ignore "$test_path")' in python_main
    assert 'mapfile -t envelope_tests <<< "$envelope_test_paths"' in python_main
    assert "python scripts/ci/shard_pytest_nodeids.py" not in python_main
    assert python_main.count(
        '-p scripts.ci.shard_pytest_nodeids --ci-shard-count 4 --ci-shard-index "$shard"'
    ) == 2
    assert '"${#envelope_tests[@]}" -eq 4' in python_main
    assert python_main.count('    "${collection_args[@]}"') == 1
    assert python_main.count('    "${envelope_tests[@]}"') == 1
    assert python_main.count("--cov-append") == 1
    assert python_main.count("python -m pytest -q -n 4 --dist worksteal") == 1
    assert python_main.count("export COVERAGE_FILE=") == 1
    assert python_main.index('    "${envelope_tests[@]}"') < python_main.index(
        "write-shard-provenance"
    )
    assert 'mapfile -t capacity_tests <<< "$base_capacity_test_paths"' in capacity
    assert capacity.count("python -m pytest -q -n 4 --dist worksteal") == 1
    assert "--cov-append" not in capacity
    assert '"${capacity_tests[@]}"' in capacity
    for test_path in expected:
        assert prepush.count(test_path) == 1


def test_capacity_lane_publishes_one_complete_coverage_artifact() -> None:
    """Publish the original bounded capacity lane as one exact artifact."""

    workflow = yaml.safe_load(
        (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
            encoding="utf-8"
        )
    )
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    capacity = prepush.split("run_capacity() {", 1)[1].split(
        "run_python_coverage() {", 1
    )[0]
    capacity_job = workflow["jobs"]["capacity-evidence"]
    capacity_uploads = [
        step
        for step in capacity_job["steps"]
        if step.get("uses", "").startswith("actions/upload-artifact@")
    ]

    assert capacity.count("export COVERAGE_FILE=") == 1
    assert capacity.count("write-shard-provenance") == 1
    assert capacity.index('"${capacity_tests[@]}"') < capacity.index(
        "write-shard-provenance"
    )
    assert capacity_job["timeout-minutes"] == 10
    assert len(capacity_uploads) == 1
    assert capacity_uploads[0]["with"]["name"] == "mrf-python-coverage-capacity"
    assert capacity_uploads[0]["with"]["path"].splitlines() == [
        ".coverage.capacity",
        ".coverage-provenance.capacity.json",
    ]


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

    assigned_nodeids = [
        nodeid
        for output in outputs
        for nodeid in output.read_text(encoding="utf-8").splitlines()
    ]
    assert sorted(assigned_nodeids) == [
        "tests/test_sample.py::test_one",
        "tests/test_sample.py::test_two",
    ]


def _assert_single_lifecycle_test(
    prepush: str,
    lifecycle_step: str,
    test_path: str,
) -> None:
    """Require one owned PostgreSQL test path in the lifecycle command."""

    assert test_path in lifecycle_step
    assert prepush.count(test_path) == 1


def test_v13_terminal_compat_postgres_runs_exactly_once() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    core_step = prepush.split("run_core_postgres() {", 1)[1].split(
        "run_provider_directory_postgres() {", 1
    )[0]
    _assert_single_lifecycle_test(
        prepush, core_step, "tests/test_ptg_wave_v13_terminal_compat_postgres.py"
    )


def test_v13_json_null_guard_postgres_runs_exactly_once() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    core_step = prepush.split("run_core_postgres() {", 1)[1].split(
        "run_provider_directory_postgres() {", 1
    )[0]
    _assert_single_lifecycle_test(
        prepush,
        core_step,
        "tests/test_ptg_wave_v13_json_null_guard_migration.py",
    )


def test_plan_pricing_projection_postgres_runs_in_core_with_its_dsn() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    core_step = prepush.split("run_core_postgres() {", 1)[1].split(
        "run_provider_directory_postgres() {", 1
    )[0]

    assert "HLTHPRT_PLAN_PRICING_PROJECTION_POSTGRES_DSN=$dsn" in core_step
    for test_path in (
        "tests/test_plan_pricing_projection_postgres.py",
        "tests/test_plan_pricing_projection_v3_postgres.py",
        "tests/test_plan_pricing_projection_v3_staging_postgres.py",
        "tests/test_plan_pricing_projection_v3_differential_postgres.py",
        "tests/test_plan_pricing_projection_v3_census_postgres.py",
        "tests/test_plan_pricing_projection_v3_census_signal_postgres.py",
        "tests/test_plan_pricing_projection_v3_work_postgres.py",
        "tests/test_ptg2_factorized_card_postgres.py",
    ):
        _assert_single_lifecycle_test(prepush, core_step, test_path)


def test_plan_pricing_idempotency_postgres_runs_in_core_once() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    core_step = prepush.split("run_core_postgres() {", 1)[1].split(
        "run_provider_directory_postgres() {", 1
    )[0]
    _assert_single_lifecycle_test(
        prepush,
        core_step,
        "tests/test_plan_pricing_idempotency_postgres.py",
    )


def test_workflow_uses_four_unique_main_coverage_artifacts_and_timeouts() -> None:
    """Keep coverage artifacts, lifecycle proofs, and command deadlines closed."""

    workflow = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )

    assert "shard-index: [0, 1, 2, 3]" in workflow
    assert "--ci-shard-count 4" in prepush
    assert "scripts.ci.shard_pytest_nodeids" in prepush
    assert "mrf-python-coverage-main-${{ matrix.shard-index }}" in workflow
    assert "pattern: mrf-python-coverage-main-*" in workflow
    workflow_jobs = yaml.safe_load(workflow)["jobs"]
    assert workflow_jobs["python-tests"]["timeout-minutes"] == 20
    assert workflow_jobs["address-canonical-db-tests"]["timeout-minutes"] == 15
    assert "mrf-python-coverage-postgres-${{ matrix.shard }}" in workflow
    assert "pattern: mrf-python-coverage-postgres-*" in workflow
    assert 'scripts/ci/prepush postgres "${{ matrix.shard }}"' in workflow
    assert "python -m pytest -q -n 1 --dist loadscope" in prepush
    assert (
        "cargo build --locked --bins --manifest-path "
        "support/ptg2_scanner/Cargo.toml &"
    ) in prepush
    assert "timeout --foreground 295s python -m pytest" in prepush
    assert "timeout --foreground 295s cargo llvm-cov" in prepush
    lifecycle_step = prepush.split("run_provider_directory_postgres() {", 1)[1].split(
        "run_provider_profile_postgres() {", 1
    )[0]
    assert "timeout --foreground 295s python -m pytest -q" in lifecycle_step
    assert "tests/test_tin_npi_connector_postgres.py" in lifecycle_step
    assert (
        "tests/test_provider_directory_subset_completion_postgres.py" in lifecycle_step
    )
    for test_path in (
        "tests/test_provider_directory_endpoint_dataset_admission_seal_migration_postgres.py",
        "tests/test_provider_directory_terminal_root_retirement_postgres.py",
        "tests/test_provider_directory_terminal_root_retirement_repair_postgres.py",
        "tests/test_provider_directory_terminal_root_retirement_v2_postgres.py",
        "tests/test_provider_directory_terminal_root_retirement_v2_topology_postgres.py",
        "tests/test_provider_directory_reviewed_subset_terminal_window_postgres.py",
    ):
        _assert_single_lifecycle_test(prepush, lifecycle_step, test_path)
    bounded_selection_step = lifecycle_step
    _assert_single_lifecycle_test(
        prepush,
        bounded_selection_step,
        "tests/test_provider_directory_dataset_selection_sealed_db.py",
    )
    for workflow_line in prepush.splitlines():
        if "python -m pytest -q -n 1 --dist loadscope" in workflow_line:
            assert "timeout --foreground 420s" in workflow_line
        elif (
            "python -m pytest" in workflow_line
            or "cargo llvm-cov --manifest-path" in workflow_line
        ):
            assert "timeout --foreground 295s" in workflow_line


def test_distance_scope_postgres_proof_runs_once() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    lifecycle_step = prepush.split("run_provider_directory_postgres() {", 1)[1].split(
        "run_provider_profile_postgres() {", 1
    )[0]
    _assert_single_lifecycle_test(
        prepush,
        lifecycle_step,
        "tests/test_ptg2_distance_exact_scope_postgres.py",
    )


def test_provider_directory_enrichment_postgres_proofs_run_exactly_once() -> None:
    """Keep the lifecycle and Profile database proofs in their owned shards."""

    workflow = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    lifecycle_tests = (
        "tests/test_provider_directory_source_local_outcomes_postgres.py",
        "tests/test_provider_directory_michigan_generation_retirement_migration.py",
        "tests/test_uhc_flex_practitioner_registration_postgres.py",
        "tests/test_provider_directory_uhc_flex_npi_cohort_postgres.py",
        "tests/test_provider_directory_uhc_flex_practitioner_acquisition_postgres.py",
        "tests/test_provider_directory_uhc_flex_practitioner_twin_postgres.py",
        "tests/test_provider_directory_uhc_flex_practitioner_publication_postgres.py",
        "tests/test_provider_directory_rooted_graph_adoption_postgres.py",
        "tests/test_provider_directory_rooted_graph_acquisition_postgres.py",
        "tests/test_provider_directory_rooted_graph_single_root_postgres.py",
        "tests/test_provider_directory_rooted_graph_publication_guards_postgres.py",
        "tests/test_provider_directory_rooted_graph_publication_postgres.py",
    )
    profile_tests = (
        "tests/test_provider_directory_profile_capacity_adoption_postgres.py",
        "tests/test_provider_directory_profile_uhc_flex_postgres.py",
    )
    lifecycle_step = prepush.split("run_provider_directory_postgres() {", 1)[1].split(
        "run_provider_profile_postgres() {", 1
    )[0]
    profile_step = prepush.split("run_provider_profile_postgres() {", 1)[1].split(
        "run_postgres() {", 1
    )[0]

    assert 'scripts/ci/prepush postgres "${{ matrix.shard }}"' in workflow
    assert "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN=$dsn" in lifecycle_step
    assert "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN=$dsn" in profile_step
    for test_path in (*lifecycle_tests, *profile_tests):
        assert prepush.count(test_path) == 1
    for test_path in lifecycle_tests:
        assert test_path in lifecycle_step
    for test_path in profile_tests:
        assert test_path in profile_step


RETAINED_PROJECTION_TEST_PATHS = (
    "tests/test_provider_directory_retained_campaign_contract.py",
    "tests/test_provider_directory_retained_private_locator_contract.py",
    "tests/test_provider_directory_retained_contract_keys.py",
    "tests/test_provider_directory_retained_private_key_binding.py",
    "tests/test_provider_directory_retained_sealed_contract.py",
    "tests/test_provider_directory_retained_source_neutrality.py",
    "tests/test_provider_directory_retained_blob_store.py",
    "tests/test_provider_directory_retained_reader_postgres.py",
    "tests/test_provider_directory_retained_reader_parallel_postgres.py",
    "tests/test_provider_directory_retained_reader_close_races_postgres.py",
    "tests/test_provider_directory_retained_core_postgres.py",
    "tests/test_provider_directory_retained_producer_store_postgres.py",
    "tests/test_provider_directory_retained_producer_store_faults_postgres.py",
    "tests/test_provider_directory_retained_producer_store_boundaries_postgres.py",
    "tests/test_provider_directory_retained_core_state_guards.py",
    "tests/test_provider_directory_retained_seal_claim_guards.py",
    "tests/test_provider_directory_retained_binding_guards.py",
    "tests/test_provider_directory_retained_root_ledger_guards.py",
    "tests/test_provider_directory_retained_retry_gc_guards.py",
    "tests/test_provider_directory_retained_key_fence_status.py",
    "tests/test_provider_directory_projection_foundation_postgres.py",
    "tests/test_provider_directory_projection_child_read_postgres.py",
    "tests/test_provider_directory_projection_child_read_cleanup_postgres.py",
    "tests/test_provider_directory_projection_materializer_postgres.py",
    "tests/test_provider_directory_projection_materializer_census_postgres.py",
    "tests/test_provider_directory_projection_finalizer_postgres.py",
    "tests/test_provider_directory_projection_decoded_census_migration_postgres.py",
    "tests/test_uhc_organization_evidence_schema.py",
)


def test_retained_projection_tail_keeps_its_environment_on_profile_lane() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    directory_step, profile_step = prepush.split(
        "run_provider_directory_postgres() {", 1
    )[1].split("run_provider_profile_postgres() {", 1)
    profile_step = profile_step.split("run_postgres() {", 1)[0]
    retained_step, original_profile_step = profile_step.split(
        "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN=$dsn", 1
    )
    assert directory_step.split("local common_env=(", 1)[1].split(
        "\n  )", 1
    )[0] == retained_step.split("local common_env=(", 1)[1].split("\n  )", 1)[0]
    assert 'env "${common_env[@]}" timeout --foreground 295s' in retained_step
    for scope_option in (
        "asyncio_default_test_loop_scope=session",
        "asyncio_default_fixture_loop_scope=session",
    ):
        assert scope_option in retained_step
        assert scope_option in original_profile_step
    assert profile_step.count("timeout --foreground 295s python -m pytest -q") == 2
    assert "--durations=20" in retained_step
    assert "-n " not in profile_step
    for test_path in RETAINED_PROJECTION_TEST_PATHS:
        _assert_single_lifecycle_test(prepush, retained_step, test_path)


def test_container_proves_rooted_graph_operator_is_packaged_and_dormant() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    operator_step = prepush.split("run_container_package() {", 1)[1]

    assert "/opt/scripts/smoke/provider_directory_rooted_graph_operator.py" in (
        operator_step
    )
    for gate_name in (
        "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_REGISTRATION_ENABLED",
        "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_ACQUISITION_ENABLED",
        "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ACQUISITION_ENABLED",
        "HLTHPRT_PROVIDER_DIRECTORY_ROOTED_GRAPH_PUBLICATION_ENABLED",
    ):
        assert f"-u {gate_name}" in operator_step
    assert 'test "$operation_status" -eq 1' in operator_step
    assert "acquire-single-root" in operator_step
    assert '{"code":"disabled","status":"error"}' in operator_step


def test_container_proves_exact_cohort_operator_is_packaged_and_dormant() -> None:
    prepush = (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )
    operator_step = prepush.split("run_container_package() {", 1)[1]

    assert "/opt/scripts/smoke/uhc_flex_practitioner_operator.py" in operator_step
    assert "HLTHPRT_UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ACQUISITION_ENABLED" in (
        operator_step
    )
    assert "acquire-admit-single-root" in operator_step
    assert 'test "$operation_status" -eq 1' in operator_step
    assert '{"code":"disabled","status":"error"}' in operator_step
