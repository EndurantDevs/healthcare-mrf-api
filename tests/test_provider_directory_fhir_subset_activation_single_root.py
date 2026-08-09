# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Single-root reviewed subset activation tests."""

from __future__ import annotations

import json

import pytest

from process import provider_directory_fhir_subset_activation as activation
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from tests import test_provider_directory_fhir_subset_activation as shared
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)


def test_single_root_manifest_and_marker_are_closed_v2(tmp_path):
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    manifest = activation.reviewed_subset_activation_manifest(
        shared._write_manifest(
            tmp_path,
            shared._manifest_for_evidence(evidence),
        )
    )

    assert manifest.root_policy == evidence.root_policy
    assert manifest.require_verified_evidence() == evidence
    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    marker_by_field = selection.metadata_marker()
    assert set(marker_by_field) == {
        "contract_version",
        "root_policy",
        "source_contract_sha256",
        "cutoff",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "source_id",
        "endpoint_id",
        "verification_campaign_id",
        "candidate",
    }
    assert "baseline" not in marker_by_field
    assert marker_by_field["root_policy"] == evidence.root_policy.document()
    assert marker_by_field["contract_version"] == (
        activation.ACTIVATION_CONTRACT_VERSION_V2
    )


def test_manifest_rejects_direct_policy_evidence_mismatch():
    manifest = activation.ReviewedSubsetActivationManifest(
        desired_candidate_status="verified_reviewed_subset_acquisition",
        evidence=activation.ReviewedSubsetActivationEvidence(
            source_contract_sha256="1" * 64,
            cutoff="2026-08-09T00:00:00.000000Z",
            verification_source_scope_sha256="2" * 64,
            completion_proof_sha256="3" * 64,
            root_policy=ReviewedRootPolicy(2),
        ),
        root_policy=ReviewedRootPolicy(1),
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        manifest.require_verified_evidence()

    assert error.value.code == "disabled"


def _authorized_database(monkeypatch, tmp_path):
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    database = shared._ActivationDatabase(source_record, dataset_rows)
    shared._authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )
    return source_record, dataset_rows, evidence, database


@pytest.mark.asyncio
async def test_single_root_sync_uses_v2_status_marker_and_closed_cas(
    monkeypatch,
    tmp_path,
):
    _, _, _, database = _authorized_database(monkeypatch, tmp_path)

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database,
    )

    assert activation_result.activated is True
    update_call = next(
        call
        for call in database.calls
        if call[0] == "status" and "UPDATE" in call[1]
    )
    parameters = update_call[2]
    assert parameters["pending_status"] == (
        "pending_reviewed_subset_acquisition"
    )
    assert parameters["verified_status"] == (
        "verified_reviewed_subset_acquisition"
    )
    assert parameters["activation_key"] == (
        activation.ACTIVATION_METADATA_KEY_V2
    )
    assert set(parameters["activation_keys"]) == {
        activation.ACTIVATION_METADATA_KEY,
        activation.ACTIVATION_METADATA_KEY_V2,
    }
    assert "?|" in update_call[1]
    marker_by_field = json.loads(parameters["activation_marker"])
    assert marker_by_field["root_policy"]["required_root_count"] == 1
    assert "baseline" not in marker_by_field


@pytest.mark.asyncio
async def test_single_root_sync_replays_exact_v2_marker(
    monkeypatch,
    tmp_path,
):
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    selection = activation.validated_reviewed_subset_activation_selection(
        source_rows=[source_record],
        dataset_rows=dataset_rows,
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    source_record["metadata_json"].update(
        provider_directory_candidate_status=(
            "verified_reviewed_subset_acquisition"
        ),
        provider_directory_reviewed_subset_activation_v2=(
            selection.metadata_marker()
        ),
    )
    database = shared._ActivationDatabase(source_record, dataset_rows)
    shared._authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    activation_result = await activation.sync_reviewed_subset_verified_state(
        database=database,
    )

    assert activation_result.is_already_applied is True
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in database.calls
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_replay", (False, True))
async def test_single_root_sync_rejects_mixed_activation_versions(
    monkeypatch,
    tmp_path,
    is_replay,
):
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    if is_replay:
        selection = activation.validated_reviewed_subset_activation_selection(
            source_rows=[source_record],
            dataset_rows=dataset_rows,
            expected_source_id=source_record["source_id"],
            evidence=evidence,
        )
        source_record["metadata_json"].update(
            provider_directory_candidate_status=(
                "verified_reviewed_subset_acquisition"
            ),
            provider_directory_reviewed_subset_activation_v2=(
                selection.metadata_marker()
            ),
        )
    source_record["metadata_json"][activation.ACTIVATION_METADATA_KEY] = {
        "version": "unexpected"
    }
    database = shared._ActivationDatabase(source_record, dataset_rows)
    shared._authorize_sync(monkeypatch, tmp_path, evidence)
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: source_record["source_id"],
    )

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        await activation.sync_reviewed_subset_verified_state(database=database)

    assert error.value.code == "state"
    assert not any(
        call[0] == "status" and "UPDATE" in call[1]
        for call in database.calls
    )


@pytest.mark.parametrize(
    "mutation",
    (
        lambda row: row.update(dataset_hash="f" * 64),
        lambda row: row.update(resource_count=999),
        lambda row: row["publication_metadata_json"].update(
            verification_campaign_id="different-campaign"
        ),
        lambda row: row["publication_metadata_json"][
            "provider_directory_content_proof_v1"
        ].update(dataset_hash="f" * 64),
    ),
)
def test_single_root_activation_rejects_proof_binding_drift(mutation):
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    mutation(dataset_rows[0])

    with pytest.raises(activation.ReviewedSubsetActivationError) as error:
        activation.validated_reviewed_subset_activation_selection(
            source_rows=[source_record],
            dataset_rows=dataset_rows,
            expected_source_id=source_record["source_id"],
            evidence=evidence,
        )

    assert error.value.code == "evidence"
