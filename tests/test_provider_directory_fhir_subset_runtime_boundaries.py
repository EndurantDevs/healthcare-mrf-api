# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Runtime identity and terminalization boundaries for reviewed subsets."""

from __future__ import annotations

import copy
from dataclasses import replace
import json

import pytest

from process import provider_directory_fhir_manual_catalog as manual_catalog
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
)
from tests.provider_directory_fhir_subset_completion_support import (
    CUTOFF,
    build_persisted_subset_inputs,
    build_subset_contract,
    build_transport_coordinate_rows,
    importer,
)


def _reviewed_source_record():
    document = json.loads(
        manual_catalog.DEFAULT_MANUAL_SOURCE_MANIFEST.read_text(
            encoding="utf-8"
        )
    )
    entry = next(
        candidate
        for candidate in document["entries"]
        if candidate.get("classification")
        == manual_catalog.MANUAL_ACQUISITION_CLASSIFICATION
    )
    seed = manual_catalog.reviewed_manual_census_seed_rows(
        entry["source_ids"][0]
    )[0]
    source_record = importer._source_row_from_seed(seed)
    source_record["endpoint_id"] = "synthetic-endpoint"
    return source_record


def test_artifact_reconstructs_reviewed_contract_and_neutral_scope():
    source_record = _reviewed_source_record()
    scoped_source = importer._artifact_source_with_subset_contract(
        source_record,
        CUTOFF,
    )
    contract = scoped_source[CURRENT_VERSION_CENSUS_CONTRACT_FIELD]
    source_ids = (source_record["source_id"],)

    direct_scope = importer._server_issued_subset_source_scope_hash(
        [scoped_source],
        list(source_ids),
    )
    checkpoint_scope, artifact_scope = (
        importer._artifact_current_source_scope_hashes(
            source_record,
            source_ids,
            CUTOFF,
        )
    )
    twin_scope = importer._twin_root_scope_hash(
        [scoped_source],
        contract.campaign_id,
        None,
    )

    assert contract.is_server_issued_subset_v3 is True
    assert checkpoint_scope != artifact_scope
    assert artifact_scope == direct_scope == twin_scope
    assert importer._candidate_subset_contract([scoped_source]) == (contract, 3)
    selected = importer._endpoint_dataset_selected_resources(
        [scoped_source],
        ["Organization", "Organization"],
    )
    assert selected == ["Organization"]


def test_artifact_reconstruction_rejects_invalid_bound_contract(monkeypatch):
    source_record = _reviewed_source_record()
    monkeypatch.setattr(
        importer,
        "_artifact_subset_contract_from_metadata",
        lambda *_args: build_subset_contract(campaign_id=None),
    )

    with pytest.raises(RuntimeError, match="artifact_subset_contract_invalid"):
        importer._artifact_source_with_subset_contract(source_record, CUTOFF)


def test_subset_source_scope_rejects_missing_base_and_invalid_payload(
    monkeypatch,
):
    source_record = importer._artifact_source_with_subset_contract(
        _reviewed_source_record(),
        CUTOFF,
    )
    source_ids = [source_record["source_id"]]
    missing_base_source_by_field = {
        **source_record,
        "api_base": None,
        "canonical_api_base": None,
    }
    with pytest.raises(RuntimeError, match="verification_scope_required"):
        importer._server_issued_subset_source_scope_hash(
            [missing_base_source_by_field],
            source_ids,
        )

    def reject(*_args, **_kwargs):
        raise ValueError("synthetic invalid source scope")

    monkeypatch.setattr(
        importer,
        "server_issued_subset_source_scope_payload",
        reject,
    )
    with pytest.raises(RuntimeError, match="verification_scope_required"):
        importer._server_issued_subset_source_scope_hash(
            [source_record],
            source_ids,
        )


def test_artifact_profile_rejects_subset_identity_and_unknown_marker():
    source_record = _reviewed_source_record()
    metadata = source_record["metadata_json"]
    metadata["provider_directory_candidate_status"] = (
        importer.PROVIDER_DIRECTORY_SUBSET_TWIN_ROOT_VERIFIED
    )
    campaign = metadata[
        importer.PROVIDER_DIRECTORY_VERIFICATION_CAMPAIGN_METADATA_KEY
    ]
    metadata["provider_directory_manual_only"] = False

    with pytest.raises(RuntimeError, match="artifact_subset_contract_invalid"):
        importer._has_validated_artifact_verification_profile(
            source_record,
            "synthetic-dataset",
            campaign,
            3,
        )
    with pytest.raises(RuntimeError, match="artifact_subset_contract_invalid"):
        importer._has_validated_artifact_verification_profile(
            {"metadata_json": {}},
            "synthetic-dataset",
            None,
            4,
        )


def test_completion_serialization_rejects_marker_and_contract_drift():
    candidate, _, content, _, _, _, _ = build_persisted_subset_inputs()
    proof_pair = (
        content.completion_proof,
        content.completion_proof_sha256,
    )
    legacy = replace(
        candidate,
        completion_proof_required_version=None,
        subset_contract=None,
    )

    with pytest.raises(RuntimeError, match="completion_proof_unexpected"):
        importer._serialized_subset_completion_proof_pair(legacy, proof_pair)
    with pytest.raises(RuntimeError, match="completion_proof_required"):
        importer._serialized_subset_completion_proof_pair(candidate, None)
    with pytest.raises(RuntimeError, match="completion_proof_invalid"):
        importer._serialized_subset_completion_proof_pair(
            candidate,
            ({}, "bad"),
        )

    contract = replace(candidate.subset_contract, cutoff="2026-08-01T12:00:01Z")
    with pytest.raises(RuntimeError, match="completion_contract_mismatch"):
        importer._serialized_subset_completion_proof_pair(
            replace(candidate, subset_contract=contract),
            proof_pair,
        )


def test_content_completion_rejects_identity_diagnostic_and_proof_drift():
    candidate, diagnostics, content, _, _, _, _ = (
        build_persisted_subset_inputs()
    )
    raw_content = replace(
        content,
        completion_proof=None,
        completion_proof_sha256=None,
    )
    with pytest.raises(RuntimeError, match="proof_identity_invalid"):
        importer._content_proof_with_subset_completion(
            replace(candidate, completion_proof_required_version=2),
            diagnostics,
            raw_content,
        )
    with pytest.raises(RuntimeError, match="proof_incomplete"):
        importer._content_proof_with_subset_completion(
            candidate,
            {},
            raw_content,
        )

    invalid_diagnostics = copy.deepcopy(diagnostics)
    resource_type = next(iter(invalid_diagnostics))
    invalid_diagnostics[resource_type][
        "server_issued_subset_completeness"
    ]["advertised_post"] += 1
    with pytest.raises(RuntimeError, match="completion_proof_invalid"):
        importer._content_proof_with_subset_completion(
            candidate,
            invalid_diagnostics,
            raw_content,
        )


def test_subset_replay_rejects_identity_missing_and_invalid_evidence():
    candidate, diagnostics, content, _, _, _, _ = (
        build_persisted_subset_inputs()
    )
    with pytest.raises(RuntimeError, match="replay_evidence_invalid"):
        importer._subset_replay_metadata(
            replace(candidate, completion_proof_required_version=2),
            diagnostics,
            content,
        )

    missing = copy.deepcopy(diagnostics)
    resource_type = next(iter(missing))
    missing[resource_type].pop(
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
    )
    with pytest.raises(RuntimeError, match="replay_evidence_invalid"):
        importer._subset_replay_metadata(candidate, missing, content)

    invalid = copy.deepcopy(diagnostics)
    invalid[resource_type][
        importer._SERVER_ISSUED_SUBSET_INTERNAL_REPLAY_KEY
    ]["continuation_hop_sha256"] = ["bad"]
    with pytest.raises(RuntimeError, match="replay_evidence_invalid"):
        importer._subset_replay_metadata(candidate, invalid, content)


def test_twin_content_proof_rejects_invalid_pair_and_contract_drift():
    candidate, _, content, _, _, _, _ = build_persisted_subset_inputs()
    with pytest.raises(RuntimeError, match="completion_proof_invalid"):
        importer._twin_root_content_proof(
            candidate,
            replace(content, completion_proof={}),
        )

    mismatched_contract = replace(
        candidate.subset_contract,
        cutoff="2026-08-01T12:00:01Z",
    )
    with pytest.raises(RuntimeError, match="completion_contract_mismatch"):
        importer._twin_root_content_proof(
            replace(candidate, subset_contract=mismatched_contract),
            content,
        )


def test_subset_projection_helpers_reject_nonmapping_replay_shapes():
    assert importer._sanitized_server_issued_subset_execution_proof(None) is None
    assert importer._server_issued_subset_internal_replay_evidence(None) is None
    assert importer._server_issued_subset_internal_replay_evidence({}) is None


def test_subset_resource_rows_reject_invalid_raw_digest():
    model, row_by_field, _continuation, _changed = (
        build_transport_coordinate_rows()
    )
    row_by_field["_acquired_resource_sha256"] = "bad"

    with pytest.raises(ValueError, match="acquired_content_invalid"):
        importer._endpoint_dataset_resource_rows(
            model,
            [row_by_field],
            dataset_id="synthetic-dataset",
        )


def test_subset_page_parse_retains_raw_resource_commitment():
    source_record = importer._artifact_source_with_subset_contract(
        _reviewed_source_record(),
        CUTOFF,
    )
    raw_resource_by_field = {
        "resourceType": "Organization",
        "id": "synthetic-organization",
        "name": "Synthetic Network",
    }
    model, _parsed = importer.parse_fhir_resource(
        source_record["source_id"],
        raw_resource_by_field,
    )

    observed = importer._parsed_current_version_census_entries(
        source_record,
        "Organization",
        model,
        [{"resource": raw_resource_by_field}],
        "https://directory.example.test/fhir/Organization",
        "synthetic-run",
        normalize_location_contacts=False,
    )

    assert observed[0][1]["_acquired_resource_sha256"] == (
        importer.subset_canonical_sha256(raw_resource_by_field)
    )


def test_subset_baseline_marker_mismatch_is_incompatible(monkeypatch):
    candidate, _, _, dataset, _, _, _ = build_persisted_subset_inputs()
    monkeypatch.setattr(
        importer,
        "_twin_root_baseline_proof",
        lambda _dataset: {},
    )

    with pytest.raises(RuntimeError, match="baseline_incompatible"):
        importer._assert_compatible_twin_root_baseline(
            candidate,
            {**dataset, "completion_proof_required_version": 2},
        )
