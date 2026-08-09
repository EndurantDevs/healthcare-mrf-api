# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner
from process import provider_directory_dataset_rehydrate as rehydrate
from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import (
    DEFAULT_RESOURCE_HASH_CONTRACT,
    LEGACY_RESOURCE_HASH_CONTRACT,
    RESOURCE_HASH_CONTRACT_METADATA_KEY,
    RESOURCE_TRANSPORT_PAYLOAD_FIELDS,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    legacy_resource_payload_sha256,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _observation_by_field(**changes_by_field):
    """Return one semantic resource observation with transport provenance."""

    return {
        "source_id": "source-a",
        "resource_id": "practitioner-1",
        "full_name": "Example Practitioner",
        "npi": 1215387113,
        "resource_url": "https://directory.example.test/fhir/Practitioner/practitioner-1",
        "fhir_self_url": "https://directory.example.test/fhir/Practitioner/practitioner-1",
        "fhir_fetch_url": "https://directory.example.test/fhir/Practitioner?page=1",
        "fhir_fetch_mode": "rest_bundle",
        **changes_by_field,
    }


def _artifact_rows(
    observation_by_field,
    resource_hash_contract=DEFAULT_RESOURCE_HASH_CONTRACT,
):
    """Build the canonical and immutable dataset rows for one observation."""

    canonical_row_by_field = importer._canonical_resource_rows(
        ProviderDirectoryPractitioner,
        [observation_by_field],
        canonical_api_base="https://directory.example.test/fhir",
        run_id="run-1",
        resource_hash_contract=resource_hash_contract,
    )[0]
    dataset_row_by_field = importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [observation_by_field],
        dataset_id="dataset-1",
        resource_hash_contract=resource_hash_contract,
    )[0]
    return canonical_row_by_field, dataset_row_by_field


def _assert_transport_provenance(artifact_by_field, observation_by_field):
    """Require every transport coordinate to remain in the stored payload."""

    for transport_field in RESOURCE_TRANSPORT_PAYLOAD_FIELDS:
        assert (
            artifact_by_field["payload_json"][transport_field]
            == observation_by_field[transport_field]
        )


def _transport_variant_by_field():
    """Return the same semantic resource observed through another page."""

    return _observation_by_field(
        resource_url="urn:uuid:synthetic-practitioner",
        fhir_self_url="urn:uuid:synthetic-practitioner",
        fhir_fetch_url="https://directory.example.test/fhir/Practitioner?page=2",
        fhir_fetch_mode="rest_bundle_replay",
    )


def _merged_resource_summary(tmp_path, dataset_rows):
    """Merge proof records with the same spool path as durable shards."""

    record_directory = tmp_path / "records"
    npi_directory = tmp_path / "npis"
    record_directory.mkdir()
    npi_directory.mkdir()
    record_spool = proof_store._RecordSpool(record_directory)
    for dataset_row_by_field in dataset_rows:
        proof_record = proof_store._proof_record(dataset_row_by_field)
        record_spool.add(
            json.dumps(
                proof_record,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
    return proof_store._merged_resource_proof(
        record_spool,
        proof_store._RecordSpool(npi_directory),
    )


def test_transport_coordinates_do_not_change_content_hash():
    """Treat replay URLs as provenance while retaining their exact values."""

    first_by_field = _observation_by_field()
    continuation_by_field = _transport_variant_by_field()
    first_canonical, first_dataset = _artifact_rows(first_by_field)
    continuation_canonical, continuation_dataset = _artifact_rows(
        continuation_by_field
    )

    assert first_canonical["payload_hash"] == continuation_canonical["payload_hash"]
    assert first_dataset["payload_hash"] == continuation_dataset["payload_hash"]
    assert first_canonical["payload_hash"] == first_dataset["payload_hash"]
    _assert_transport_provenance(first_canonical, first_by_field)
    _assert_transport_provenance(continuation_dataset, continuation_by_field)


def test_semantic_content_still_changes_hash():
    """Keep meaningful resource changes visible to immutable content proofs."""

    _baseline_canonical, baseline_dataset = _artifact_rows(
        _observation_by_field()
    )
    changed_canonical, changed_dataset = _artifact_rows(
        _transport_variant_by_field() | {"full_name": "Changed Practitioner"}
    )

    assert changed_canonical["payload_hash"] == changed_dataset["payload_hash"]
    assert changed_dataset["payload_hash"] != baseline_dataset["payload_hash"]


def test_persisted_contract_fences_legacy_and_neutral_writes():
    """Keep resumed roots legacy while making fresh roots transport-neutral."""

    first_by_field = _observation_by_field()
    continuation_by_field = _transport_variant_by_field()
    first_legacy = _artifact_rows(
        first_by_field,
        LEGACY_RESOURCE_HASH_CONTRACT,
    )
    continuation_legacy = _artifact_rows(
        continuation_by_field,
        LEGACY_RESOURCE_HASH_CONTRACT,
    )
    first_neutral = _artifact_rows(
        first_by_field,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    continuation_neutral = _artifact_rows(
        continuation_by_field,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )

    assert first_legacy[0]["payload_hash"] == first_legacy[1]["payload_hash"]
    assert continuation_legacy[0]["payload_hash"] == continuation_legacy[1]["payload_hash"]
    assert first_legacy[1]["payload_hash"] != continuation_legacy[1]["payload_hash"]
    assert first_neutral[1]["payload_hash"] == continuation_neutral[1]["payload_hash"]


def test_transport_variants_merge_as_one_proof_resource(tmp_path):
    """Merge repeated transport observations without weakening deduplication."""

    _first_canonical, first_dataset = _artifact_rows(_observation_by_field())
    _next_canonical, continuation_dataset = _artifact_rows(
        _transport_variant_by_field()
    )

    merged_summary = _merged_resource_summary(
        tmp_path,
        [first_dataset, continuation_dataset],
    )
    assert merged_summary[1] == 1
    assert merged_summary[3] == {"Practitioner": 1}


def test_semantic_variants_remain_a_proof_conflict(tmp_path):
    """Reject repeated resource identities whose semantic payload changed."""

    _first_canonical, first_dataset = _artifact_rows(_observation_by_field())
    _changed_canonical, changed_dataset = _artifact_rows(
        _observation_by_field(full_name="Changed Practitioner")
    )

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="proof shards conflict",
    ):
        _merged_resource_summary(tmp_path, [first_dataset, changed_dataset])


def test_legacy_transport_hash_remains_readable():
    """Read exact historical ordinary rows while new writes use semantic hashes."""

    observation_by_field = _observation_by_field()
    mapped_payload_by_field = importer._canonical_resource_payload(
        observation_by_field
    )
    legacy_hash = legacy_resource_payload_sha256(mapped_payload_by_field)
    _canonical_row, dataset_row_by_field = _artifact_rows(observation_by_field)

    assert legacy_hash != dataset_row_by_field["payload_hash"]
    importer._assert_endpoint_dataset_resource_payload_hash(
        {"payload_json": mapped_payload_by_field, "payload_hash": legacy_hash}
    )
    assert (
        rehydrate._validate_payload(
            ProviderDirectoryPractitioner,
            "practitioner-1",
            legacy_hash,
            mapped_payload_by_field,
        )
        is None
    )


def test_legacy_hash_does_not_hide_semantic_changes():
    """Reject changed semantics even when the retained hash uses legacy rules."""

    mapped_payload_by_field = importer._canonical_resource_payload(
        _observation_by_field()
    )
    legacy_hash = legacy_resource_payload_sha256(mapped_payload_by_field)
    changed_payload_by_field = {
        **mapped_payload_by_field,
        "full_name": "Changed Practitioner",
    }

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="payload_hash_mismatch",
    ):
        importer._assert_endpoint_dataset_resource_payload_hash(
            {
                "payload_json": changed_payload_by_field,
                "payload_hash": legacy_hash,
            }
        )
    assert (
        rehydrate._validate_payload(
            ProviderDirectoryPractitioner,
            "practitioner-1",
            legacy_hash,
            changed_payload_by_field,
        )
        == "payload_hash_mismatch"
    )


def _candidate_for_contract(resource_hash_contract):
    """Return a neutral synthetic candidate for contract-boundary tests."""

    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        import_run_id="run-1",
        previous_dataset_id=None,
        expected_resources=("Practitioner",),
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-1",
        verification_source_scope_hash="scope-1",
        resource_hash_contract=resource_hash_contract,
    )


def test_dataset_contract_defaults_fresh_and_normalizes_legacy_metadata():
    """Version fresh roots while preserving marker-less historical roots."""

    assert (
        importer._dataset_resource_hash_contract({})
        == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    )
    legacy_state_by_field = {
        "publication_metadata_json": {"source_ids": ["source-1"]}
    }
    assert (
        importer._dataset_resource_hash_contract(legacy_state_by_field)
        == LEGACY_RESOURCE_HASH_CONTRACT
    )
    candidate = _candidate_for_contract(LEGACY_RESOURCE_HASH_CONTRACT)
    metadata = importer._endpoint_dataset_candidate_metadata(candidate)
    assert (
        metadata[RESOURCE_HASH_CONTRACT_METADATA_KEY]
        == LEGACY_RESOURCE_HASH_CONTRACT
    )


def test_dataset_contract_accepts_known_marker_and_rejects_unknown_marker():
    """Fail closed before an acquisition can mix an unrecognized hash policy."""

    neutral_state_by_field = {
        "publication_metadata_json": {
            RESOURCE_HASH_CONTRACT_METADATA_KEY: (
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            )
        }
    }
    assert (
        importer._dataset_resource_hash_contract(neutral_state_by_field)
        == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    )
    invalid_state_by_field = {
        "publication_metadata_json": {
            RESOURCE_HASH_CONTRACT_METADATA_KEY: "unknown-contract"
        }
    }
    with pytest.raises(RuntimeError, match="resource_hash_contract_invalid"):
        importer._dataset_resource_hash_contract(invalid_state_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_state_by_field", "expected_contract"),
    [
        ({}, TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT),
        (
            {"publication_metadata_json": {"source_ids": ["source-1"]}},
            LEGACY_RESOURCE_HASH_CONTRACT,
        ),
        (
            {
                "publication_metadata_json": {
                    RESOURCE_HASH_CONTRACT_METADATA_KEY: (
                        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
                    )
                }
            },
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
        ),
    ],
)
async def test_resumable_selection_preserves_dataset_contract(
    monkeypatch,
    existing_state_by_field,
    expected_contract,
):
    """Select the root's stored contract before any candidate write."""

    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_previous_endpoint_dataset_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_should_repair_empty_endpoint_dataset_orphan",
        lambda *_args: False,
    )
    selection = await importer._select_resumable_endpoint_dataset_candidate(
        existing_state_by_field,
        "dataset-1",
        "endpoint-1",
        ("Practitioner",),
        "root-1",
        None,
        None,
        importer.EndpointDatasetVerificationProfile(False),
    )

    assert selection.resource_hash_contract == expected_contract


def test_dataset_write_requires_explicit_contract():
    """Detect any future dataset writer that bypasses candidate propagation."""

    with pytest.raises(ValueError, match="resource_hash_contract_required"):
        importer._endpoint_dataset_write_scope(
            {"_endpoint_dataset_id": "dataset-1"}
        )


@pytest.mark.asyncio
async def test_prepared_source_propagates_candidate_contract(monkeypatch):
    """Carry the selected root contract into every downstream write path."""

    candidate = _candidate_for_contract(LEGACY_RESOURCE_HASH_CONTRACT)
    monkeypatch.setattr(
        importer,
        "_prepare_endpoint_dataset_candidate",
        AsyncMock(return_value=candidate),
    )
    prepared_sources, prepared_candidate = (
        await importer._prepare_resource_import_source_group(
            [{"source_id": "source-1"}],
            ["Practitioner"],
            run_id="run-1",
            retry_of_run_id=None,
            pagination_root_run_id=None,
            is_checkpointing_enabled=False,
        )
    )

    assert prepared_candidate is candidate
    assert prepared_sources[0]["_endpoint_dataset_id"] == "dataset-1"
    assert (
        prepared_sources[0]["_resource_hash_contract"]
        == LEGACY_RESOURCE_HASH_CONTRACT
    )


def test_fresh_twin_successor_inherits_markerless_baseline(monkeypatch):
    """Keep both twin roots on the baseline's historical hash contract."""

    candidate = _candidate_for_contract(
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    )
    baseline_by_field = {
        "dataset_id": "dataset-baseline",
        "publication_metadata_json": {},
    }
    monkeypatch.setattr(
        importer,
        "_compatible_twin_root_baseline",
        lambda _candidate, _state: baseline_by_field,
    )

    admitted = importer._candidate_with_locked_twin_root_admission(
        candidate,
        baseline_by_field,
    )

    assert admitted.resource_hash_contract == LEGACY_RESOURCE_HASH_CONTRACT
    assert admitted.verification_baseline_dataset_id == "dataset-baseline"


def test_resumed_twin_successor_rejects_contract_mismatch(monkeypatch):
    """Reject a resumed successor that differs from its immutable baseline."""

    candidate = _candidate_for_contract(
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    )
    candidate = importer.replace(
        candidate,
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        verification_baseline_dataset_id="dataset-baseline",
    )
    baseline_by_field = {
        "dataset_id": "dataset-baseline",
        "publication_metadata_json": {},
    }
    monkeypatch.setattr(
        importer,
        "_compatible_twin_root_baseline",
        lambda _candidate, _state: baseline_by_field,
    )

    with pytest.raises(RuntimeError, match="baseline_incompatible"):
        importer._candidate_with_locked_twin_root_admission(
            candidate,
            baseline_by_field,
        )


def test_finalized_replay_normalizes_markerless_contract():
    """Allow exact legacy replay but reject a neutral reinterpretation."""

    metadata = {
        "acquisition_root_run_id": "root-1",
        "selected_resources": ["Practitioner"],
        "expected_resources": ["Practitioner"],
        "source_ids": ["source-1"],
        "resource_diagnostics": {"Practitioner": {"complete": True}},
    }
    legacy_candidate = importer.replace(
        _candidate_for_contract(LEGACY_RESOURCE_HASH_CONTRACT),
        requires_twin_root_verification=False,
        already_validated=True,
        validated_metadata=metadata,
    )
    importer._assert_finalized_endpoint_dataset_replay(legacy_candidate)
    neutral_candidate = importer.replace(
        legacy_candidate,
        resource_hash_contract=TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    with pytest.raises(RuntimeError, match="validated_identity_mismatch"):
        importer._assert_finalized_endpoint_dataset_replay(neutral_candidate)
