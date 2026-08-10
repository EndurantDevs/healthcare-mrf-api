# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed FHIR runtime edges for the semantic-content dataset contract."""

from __future__ import annotations

from contextlib import asynccontextmanager
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_semantic_v3_coverage_support import (
    LEGACY_CONTRACT,
    NEUTRAL_CONTRACT,
    PROJECTION_DATE,
    SEMANTIC_CONTRACT,
    ZERO_HASH,
    candidate,
    generic_dataset_row,
    semantic_parent_metadata,
)


importer = importlib.import_module("process.provider_directory_fhir")
resource_hash = importlib.import_module("process.provider_directory_resource_hash")
_candidate = candidate
_generic_dataset_row = generic_dataset_row
_semantic_parent_metadata = semantic_parent_metadata


def test_fhir_acquisition_context_rejects_nondate_projection() -> None:
    with pytest.raises(ValueError, match="semantic_projection_as_of_invalid"):
        importer.FHIRAcquisitionContext(semantic_projection_as_of="2026-08-09")


def test_artifact_content_identity_translates_invalid_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importer,
        "_dataset_resource_hash_contract",
        lambda _dataset: (_ for _ in ()).throw(RuntimeError("invalid")),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="artifact_content_proof_invalid",
    ):
        importer._artifact_content_proof_identity({}, (), "dataset-edge")


def test_artifact_content_source_scope_rejects_invalid_identity() -> None:
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="artifact_content_proof_invalid",
    ):
        importer._artifact_content_proof_source_ids(
            {"source_ids": ["source-other"]},
            "source-edge",
            "dataset-edge",
        )


def test_semantic_artifact_requires_stored_content_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importer,
        "_artifact_content_proof_identity",
        lambda *_args: importer._ArtifactContentProofIdentity(
            resource_hash_contract=SEMANTIC_CONTRACT,
            semantic_projection_as_of=PROJECTION_DATE,
            proof_resource_scope=("Practitioner",),
        ),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="artifact_content_proof_invalid",
    ):
        importer._artifact_dataset_retained_resources(
            source_id="source-edge",
            endpoint_id="endpoint-edge",
            dataset_id="dataset-edge",
            evidence_run_id="root-edge",
            selected_resources=("Practitioner",),
            publication_metadata={},
        )


def test_nonpractitioner_semantic_payloads_merge_by_content() -> None:
    payload = {"resource_id": "resource-edge", "active": True}

    assert importer._merge_resource_payload_for_contract(
        object,
        payload,
        dict(payload),
        SEMANTIC_CONTRACT,
    ) == payload


def test_dataset_payload_index_rejects_raw_hash_conflict() -> None:
    rows = [
        {
            "resource_id": "resource-edge",
            "active": True,
            "_acquired_resource_sha256": ZERO_HASH,
        },
        {
            "resource_id": "resource-edge",
            "active": True,
            "_acquired_resource_sha256": "1" * 64,
        },
    ]

    with pytest.raises(ValueError, match="acquired_content_conflict"):
        importer._dataset_payloads_by_resource_id(object, rows, LEGACY_CONTRACT)


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (datetime.date(2026, 8, 9), datetime.date(2026, 8, 9)),
        (7, None),
        ("2026-13-01", None),
        ("2020-W01-1", None),
    ),
)
def test_source_projection_date_accepts_only_exact_iso_date(value, expected) -> None:
    if expected is not None:
        assert importer._source_semantic_projection_as_of(
            {"_semantic_projection_as_of": value}
        ) == expected
        return

    with pytest.raises(ValueError, match="semantic_projection_as_of_invalid"):
        importer._source_semantic_projection_as_of(
            {"_semantic_projection_as_of": value}
        )


@pytest.mark.asyncio
async def test_proof_persistence_requires_hash_contract() -> None:
    with pytest.raises(RuntimeError, match="resource_hash_contract_required"):
        await importer._upsert_dataset_resource_rows_on_connection(
            SimpleNamespace(),
            [_generic_dataset_row()],
            persist_content_proof=True,
        )


def test_resumed_source_proof_scope_accepts_only_canonical_resource_scope() -> None:
    assert importer._source_proof_resource_scope(
        {"_proof_resource_scope": ["Practitioner"]}
    ) == frozenset({"Practitioner"})

    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._source_proof_resource_scope(
            {"_proof_resource_scope": ["UnknownResource"]}
        )


@pytest.mark.asyncio
async def test_linked_import_skips_resource_outside_proof_scope() -> None:
    result = await importer._import_linked_resource_rows(
        {
            "source_id": "source-edge",
            "api_base": "https://example.invalid/fhir",
            "_proof_resource_scope": ["PractitionerRole"],
        },
        {
            "PractitionerRole": [
                {
                    "resource_id": "role-edge",
                    "practitioner_ref": "Practitioner/practitioner-edge",
                }
            ]
        },
        per_source_limit=1,
        timeout=1,
        run_id="root-edge",
    )

    assert result == {}


@pytest.mark.asyncio
async def test_dataset_proof_parent_must_remain_mutable() -> None:
    connection = SimpleNamespace(first=AsyncMock(return_value={}))

    with pytest.raises(RuntimeError, match="proof_parent_not_mutable"):
        await importer._lock_endpoint_dataset_proof_parent(
            connection,
            "dataset-edge",
        )


def test_endpoint_resource_scope_rejects_unknown_resource() -> None:
    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._validated_endpoint_dataset_resource_scope(["UnknownResource"])


def test_endpoint_parent_metadata_decodes_json_and_rejects_invalid_values() -> None:
    assert importer._endpoint_dataset_parent_metadata(
        {"publication_metadata_json": '{"selected_resources": ["Practitioner"]}'}
    ) == {"selected_resources": ["Practitioner"]}

    for metadata in ("{", []):
        with pytest.raises(RuntimeError, match="resource_scope_invalid"):
            importer._endpoint_dataset_parent_metadata(
                {"publication_metadata_json": metadata}
            )


def test_parent_proof_scope_rejects_contract_and_subset_mismatches() -> None:
    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._endpoint_dataset_parent_proof_resource_scope(
            {"publication_metadata_json": {"proof_resource_scope": None}},
            LEGACY_CONTRACT,
        )

    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._endpoint_dataset_parent_proof_resource_scope(
            {
                "publication_metadata_json": _semantic_parent_metadata(
                    proof_resource_scope=["Location"]
                )
            },
            SEMANTIC_CONTRACT,
        )


@pytest.mark.asyncio
async def test_existing_dataset_resources_decode_payload_or_fail_closed() -> None:
    empty_connection = SimpleNamespace(all=AsyncMock())
    assert await importer._existing_endpoint_dataset_resources(
        empty_connection,
        "dataset-edge",
        "Organization",
        [],
    ) == {}
    empty_connection.all.assert_not_awaited()

    valid_connection = SimpleNamespace(
        all=AsyncMock(
            return_value=[
                {
                    "resource_id": "resource-edge",
                    "payload_json": '{"resource_id": "resource-edge"}',
                }
            ]
        )
    )
    decoded = await importer._existing_endpoint_dataset_resources(
        valid_connection,
        "dataset-edge",
        "Organization",
        ["resource-edge"],
    )
    assert decoded["resource-edge"]["payload_json"] == {
        "resource_id": "resource-edge"
    }

    for record_by_field in (
        {"resource_id": "resource-edge", "payload_json": "{"},
        {"resource_id": None, "payload_json": {}},
    ):
        connection = SimpleNamespace(
            all=AsyncMock(return_value=[record_by_field])
        )
        with pytest.raises(RuntimeError, match="semantic_.*payload_invalid"):
            await importer._existing_endpoint_dataset_resources(
                connection,
                "dataset-edge",
                "Organization",
                ["resource-edge"],
            )


def test_v3_dataset_payload_requires_object_subset_absence_and_exact_hash() -> None:
    cases = (
        {"payload_json": []},
        {
            "payload_json": {"resource_id": "resource-edge"},
            "acquired_resource_sha256": ZERO_HASH,
        },
        {
            "payload_json": {"resource_id": "resource-edge"},
            "payload_hash": ZERO_HASH,
        },
    )

    for dataset_row in cases:
        with pytest.raises(RuntimeError, match="semantic_resource"):
            importer._validated_v3_dataset_payload(dataset_row)


def test_endpoint_batch_identity_rejects_mixed_or_missing_scope() -> None:
    with pytest.raises(RuntimeError, match="dataset_scope_invalid"):
        importer._endpoint_dataset_batch_identity(
            [_generic_dataset_row(dataset_id=None)],
            None,
        )

    with pytest.raises(RuntimeError, match="resource_type_scope_invalid"):
        importer._endpoint_dataset_batch_identity(
            [_generic_dataset_row(resource_type=None)],
            "dataset-edge",
        )


def test_incoming_semantic_batch_rejects_invalid_or_duplicate_identity() -> None:
    with pytest.raises(RuntimeError, match="resource_identity_invalid"):
        importer._incoming_semantic_dataset_rows_by_id(
            [_generic_dataset_row(resource_id=None)],
            "Organization",
            PROJECTION_DATE,
        )

    row = _generic_dataset_row()
    with pytest.raises(RuntimeError, match="batch_duplicate"):
        importer._incoming_semantic_dataset_rows_by_id(
            [row, dict(row)],
            "Organization",
            PROJECTION_DATE,
        )


@pytest.mark.asyncio
async def test_dataset_accumulator_rejects_unknown_hash_contract() -> None:
    with pytest.raises(RuntimeError, match="resource_hash_contract_required"):
        await importer._accumulated_endpoint_dataset_rows(
            SimpleNamespace(),
            [_generic_dataset_row()],
            resource_hash_contract="unknown-contract",
        )


def test_materialization_requires_matching_observation_and_payload() -> None:
    with pytest.raises(RuntimeError, match="materialization_invalid"):
        importer._materialized_resource_rows_from_dataset_rows(
            [],
            [_generic_dataset_row()],
        )


def test_resource_write_identity_fences_dataset_contract_and_date() -> None:
    legacy_scope = importer.EndpointDatasetWriteScope(
        dataset_id="dataset-edge",
        resource_hash_contract=LEGACY_CONTRACT,
    )
    semantic_scope = importer.EndpointDatasetWriteScope(
        dataset_id="dataset-edge",
        resource_hash_contract=SEMANTIC_CONTRACT,
        semantic_projection_as_of=PROJECTION_DATE,
    )
    cases = (
        importer.ProviderDirectoryResourceWriteOptions(
            run_id="root-edge",
            track_seen=False,
            dataset_scope=legacy_scope,
            resource_hash_contract=NEUTRAL_CONTRACT,
        ),
        importer.ProviderDirectoryResourceWriteOptions(
            run_id="root-edge",
            track_seen=False,
            dataset_scope=semantic_scope,
            semantic_projection_as_of="2026-08-10",
        ),
        importer.ProviderDirectoryResourceWriteOptions(
            run_id="root-edge",
            track_seen=False,
            resource_hash_contract="unknown-contract",
        ),
    )

    for options in cases:
        with pytest.raises(ValueError, match="scope_mismatch|contract_invalid"):
            importer._resolved_resource_write_identity(options)


@pytest.mark.parametrize("value", ("2026-13-01", "2020-W01-1"))
def test_semantic_projection_date_rejects_invalid_or_noncanonical_iso(value) -> None:
    with pytest.raises(RuntimeError, match="semantic_projection_as_of_invalid"):
        importer._validated_semantic_projection_as_of(value)


def test_legacy_contract_has_no_semantic_proof_scope() -> None:
    assert (
        importer._resource_hash_contract_proof_scope(
            ["Practitioner"],
            LEGACY_CONTRACT,
        )
        is None
    )


def test_candidate_proof_scope_rejects_contract_and_subset_mismatches() -> None:
    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._candidate_proof_resource_scope(
            _candidate(
                resource_hash_contract=LEGACY_CONTRACT,
                proof_resource_scope=("Practitioner",),
            )
        )

    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._candidate_proof_resource_scope(
            _candidate(proof_resource_scope=("Location",))
        )


def test_dataset_proof_scope_rejects_selected_resource_drift() -> None:
    with pytest.raises(RuntimeError, match="resource_scope_invalid"):
        importer._dataset_proof_resource_scope(
            {"publication_metadata_json": _semantic_parent_metadata()},
            ["Organization"],
            SEMANTIC_CONTRACT,
        )


def test_optional_twin_proof_requires_mapping_shapes() -> None:
    assert not importer._is_finalized_semantic_twin_proof_exact(
        {importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: []},
        {},
    )

    proof_state = importer._TwinRootDatasetProofState(
        metadata=None,
        verification=None,
        proof=None,
        semantic_projection_as_of=None,
        stored_proof_resource_scope=None,
        proof_resource_count=0,
        dataset_resource_count=0,
    )
    assert not importer._is_twin_root_proof_hash_exact({}, proof_state)


@pytest.mark.asyncio
async def test_uncheckpointed_cleanup_requires_locked_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_executor = SimpleNamespace(
        first=AsyncMock(return_value=None),
        status=AsyncMock(),
    )

    @asynccontextmanager
    async def mutation_scope(_dataset_ids):
        yield database_executor

    monkeypatch.setattr(
        importer,
        "_mutable_endpoint_dataset_resource_mutation",
        mutation_scope,
    )

    with pytest.raises(RuntimeError, match="cleanup_parent_changed"):
        await importer._clear_uncheckpointed_endpoint_dataset_candidate(
            _candidate()
        )
    database_executor.status.assert_not_awaited()


def test_payload_hash_validator_covers_subset_and_explicit_contract_paths() -> None:
    payload = {"resource_id": "resource-edge"}
    importer._assert_endpoint_dataset_resource_payload_hash(
        {
            "payload_json": payload,
            "payload_hash": importer.subset_payload_sha256(
                resource_hash.resource_content_hash_payload(payload)
            ),
            "acquired_resource_sha256": ZERO_HASH,
        }
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="resource_hash_contract_invalid",
    ):
        importer._assert_endpoint_dataset_resource_payload_hash(
            {
                "payload_json": payload,
                "payload_hash": ZERO_HASH,
                "acquired_resource_sha256": None,
            },
            resource_hash_contract="unknown-contract",
        )


def test_locked_candidate_translates_invalid_persisted_identity() -> None:
    with pytest.raises(RuntimeError, match="candidate_stale"):
        importer._assert_locked_candidate_identity(
            {"publication_metadata_json": "{"},
            _candidate(),
        )
