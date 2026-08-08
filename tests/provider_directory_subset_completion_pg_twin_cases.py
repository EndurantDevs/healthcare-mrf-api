# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Twin-root and source-fence PostgreSQL cases for subset completion."""

from __future__ import annotations

from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    replace_subset_source,
)
from tests.provider_directory_subset_completion_pg_source_cases import (
    prove_publish_source_fence,
)
from tests.provider_directory_subset_completion_pg_support import (
    ALTERNATE_RESOURCE_ROWS,
    terminal_metadata,
    terminal_parameters,
    terminal_sql,
    valid_evidence_pairs,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


async def prove_initial_source_fence(scenario, evidence_pairs):
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    cases = (
        ("dataset-source-absent", "root-source-absent", False),
        ("dataset-source-nonmanual", "root-source-nonmanual", True),
    )
    for dataset_id, root_run_id, use_nonmanual_source in cases:
        await insert_subset_candidate(
            scenario,
            dataset_id=dataset_id,
            root_run_id=root_run_id,
        )
        await insert_valid_subset_resources(scenario, dataset_id)
        if use_nonmanual_source:
            await replace_subset_source(
                scenario,
                "pending_two_matching_reviewed_subset_acquisitions",
                provider_directory_manual_only=False,
            )
        metadata_by_field = terminal_metadata(
            proof_by_field,
            proof_sha256,
            replay_by_field,
            replay_sha256,
            root_run_id,
        )
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_terminal_source_invalid",
            terminal_sql(scenario, dataset_id),
            *terminal_parameters(
                proof_by_field,
                proof_sha256,
                metadata_by_field,
                "verification_baseline",
            ),
        )
    await replace_subset_source(
        scenario,
        "pending_two_matching_reviewed_subset_acquisitions",
    )


async def _expect_baseline_metadata_rejected(
    scenario,
    evidence_pairs,
    *,
    dataset_id,
    root_run_id,
    mutate_metadata,
    make_current=False,
):
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(scenario, dataset_id)
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
    )
    mutate_metadata(metadata_by_field)
    statement = terminal_sql(scenario, dataset_id)
    if make_current:
        statement = statement.replace(
            "SET dataset_hash = $1,",
            "SET is_current = true, dataset_hash = $1,",
        )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_baseline_twin_invalid",
        statement,
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_baseline",
        ),
    )


async def _prove_malformed_baselines(scenario, evidence_pairs):
    cases = (
        (
            "dataset-baseline-missing-root",
            "root-baseline-missing-root",
            lambda metadata: metadata.pop("acquisition_root_run_id"),
            False,
        ),
        (
            "dataset-baseline-forged-role",
            "root-baseline-forged-role",
            lambda metadata: metadata.update(
                verification_role="verification_candidate"
            ),
            False,
        ),
        (
            "dataset-baseline-campaign-drift",
            "root-baseline-campaign-drift",
            lambda metadata: metadata.update(
                verification_campaign_id="different-campaign"
            ),
            False,
        ),
        (
            "dataset-baseline-current",
            "root-baseline-current",
            lambda metadata: None,
            True,
        ),
    )
    for dataset_id, root_run_id, mutation, make_current in cases:
        await _expect_baseline_metadata_rejected(
            scenario,
            evidence_pairs,
            dataset_id=dataset_id,
            root_run_id=root_run_id,
            mutate_metadata=mutation,
            make_current=make_current,
        )


async def _prove_duplicate_baseline_rejected(scenario, evidence_pairs):
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    dataset_id = "dataset-baseline-duplicate"
    root_run_id = "root-baseline-duplicate"
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(scenario, dataset_id)
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
    )
    await expect_postgres_error(
        scenario.connection,
        "pd_endpoint_dataset_subset_baseline_generation_key",
        terminal_sql(scenario, dataset_id),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_baseline",
        ),
    )


async def _prove_false_mismatch_rejected(scenario, evidence_pairs):
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    dataset_id = "dataset-false-mismatch"
    root_run_id = "root-false-mismatch"
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(scenario, dataset_id)
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
        mismatch_fields=["dataset_hash"],
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_mismatch_twin_invalid",
        terminal_sql(scenario, dataset_id),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_mismatch",
        ),
    )


async def _prove_real_mismatch_succeeds(scenario):
    evidence_pairs = valid_evidence_pairs(rows=ALTERNATE_RESOURCE_ROWS)
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    dataset_id = "dataset-real-mismatch"
    root_run_id = "root-real-mismatch"
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(
        scenario,
        dataset_id,
        resource_rows=ALTERNATE_RESOURCE_ROWS,
    )
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
        mismatch_fields=[
            "dataset_hash",
            "resource_hashes",
            "completion_proof",
            "completion_proof_sha256",
        ],
    )
    await scenario.connection.execute(
        terminal_sql(scenario, dataset_id),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_mismatch",
        ),
    )


async def _prove_mismatch_source_drift_rejected(scenario):
    evidence_pairs = valid_evidence_pairs(rows=ALTERNATE_RESOURCE_ROWS)
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    dataset_id = "dataset-mismatch-source-drift"
    root_run_id = "root-mismatch-source-drift"
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(
        scenario,
        dataset_id,
        resource_rows=ALTERNATE_RESOURCE_ROWS,
    )
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
        mismatch_fields=[
            "source_ids",
            "dataset_hash",
            "resource_hashes",
            "completion_proof",
            "completion_proof_sha256",
        ],
    )
    metadata_by_field["source_ids"] = ["different-source"]
    metadata_by_field["twin_root_verification_v1"]["proof"][
        "source_ids"
    ] = ["different-source"]
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_mismatch_twin_invalid",
        terminal_sql(scenario, dataset_id),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_mismatch",
        ),
    )


async def prove_terminal_twin_semantics(scenario, evidence_pairs):
    """Prove exact baseline generation and real mismatch semantics."""

    await _prove_malformed_baselines(scenario, evidence_pairs)
    await _prove_duplicate_baseline_rejected(scenario, evidence_pairs)
    await _prove_false_mismatch_rejected(scenario, evidence_pairs)
    await _prove_mismatch_source_drift_rejected(scenario)
    await _prove_real_mismatch_succeeds(scenario)


async def _expect_matched_transition_rejected(
    scenario,
    dataset_id,
    root_run_id,
    evidence_pairs,
    baseline_dataset_id,
    baseline_root_run_id,
    *,
    status="validated",
    marker="provider_directory_subset_matched_twin_invalid",
):
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id=root_run_id,
    )
    await insert_valid_subset_resources(scenario, dataset_id)
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
        baseline_dataset_id=baseline_dataset_id,
        baseline_root_run_id=baseline_root_run_id,
    )
    await expect_postgres_error(
        scenario.connection,
        marker,
        terminal_sql(scenario, dataset_id),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            status,
        ),
    )


async def _prove_matched_rejections(scenario, evidence_pairs):
    rejected_cases = (
        (
            "dataset-no-baseline",
            "root-no-baseline",
            "dataset-absent",
            "root-absent",
        ),
        (
            "dataset-different-baseline-root",
            "root-different-baseline-root",
            "dataset-subset",
            "root-not-the-baseline",
        ),
        (
            "dataset-same-root",
            "root-subset",
            "dataset-subset",
            "root-subset",
        ),
    )
    for dataset_id, root_run_id, baseline_id, baseline_root in rejected_cases:
        await _expect_matched_transition_rejected(
            scenario,
            dataset_id,
            root_run_id,
            evidence_pairs,
            baseline_id,
            baseline_root,
        )
    await _expect_matched_transition_rejected(
        scenario,
        "dataset-direct-published",
        "root-direct-published",
        evidence_pairs,
        "dataset-subset",
        "root-subset",
        status="published",
    )


async def prove_matched_twin_transition_gate(scenario, evidence_pairs):
    """Prove exact distinct-root validation and verified-source publication."""

    await _prove_matched_rejections(scenario, evidence_pairs)
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-matched",
        root_run_id="root-matched",
    )
    await insert_valid_subset_resources(scenario, "dataset-matched")
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        "root-matched",
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
    )
    await scenario.connection.execute(
        terminal_sql(scenario, "dataset-matched"),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "validated",
        ),
    )
    await prove_publish_source_fence(scenario)
