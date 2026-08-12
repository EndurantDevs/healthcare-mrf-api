# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import importlib
import json
from datetime import datetime

import pytest

from db.connection import Database
from tests.test_provider_directory_artifact_eligibility_db import (
    DATASET_HASH,
    _artifact_options,
    _assert_candidate_eligibility,
    _baseline_metadata,
    _candidate_database,
    _compact_candidate_ids,
    _matched_metadata,
    _option_ids,
    _seal_core_datasets,
    _set_all_source_profiles,
    _set_source_metadata,
    _set_twin_metadata,
    _source_metadata,
)
from tests.provider_directory_fhir_subset_activation_support import activation_inputs


importer = importlib.import_module("process.provider_directory_fhir")


def _metadata_with_hash_identity(
    metadata: dict[str, object],
    resource_hash_contract: str,
) -> dict[str, object]:
    """Attach the exact resource-hash lineage used by this contract matrix."""

    updated_metadata = copy.deepcopy(metadata)
    updated_metadata[importer.RESOURCE_HASH_CONTRACT_METADATA_KEY] = (
        resource_hash_contract
    )
    if (
        resource_hash_contract
        == importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    ):
        proof_resource_types = list(
            importer._provider_directory_proof_resource_scope(
                updated_metadata["selected_resources"]
            )
        )
        semantic_projection_as_of = "2026-08-09"
        updated_metadata[
            importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
        ] = proof_resource_types
        updated_metadata[importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY] = (
            semantic_projection_as_of
        )
        proof = updated_metadata[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY][
            "proof"
        ]
        proof[importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY] = (
            proof_resource_types
        )
        proof[importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY] = (
            semantic_projection_as_of
        )
    return updated_metadata


async def _assert_metadata_case(
    database: Database,
    schema: str,
    *,
    baseline_metadata: dict[str, object],
    candidate_metadata: dict[str, object],
    expected: bool,
    case_name: str,
) -> None:
    """Persist one twin pair and assert both eligibility implementations."""
    await _set_twin_metadata(
        database,
        schema,
        baseline_metadata=baseline_metadata,
        candidate_metadata=candidate_metadata,
    )
    await _assert_candidate_eligibility(
        database,
        schema,
        expected=expected,
        case_name=case_name,
    )


async def _assert_legacy_hash_compatibility(
    database: Database,
    schema: str,
) -> dict[str, object]:
    """Accept a markerless legacy baseline with an explicit-v1 candidate."""
    explicit_v1_candidate = _metadata_with_hash_identity(
        _matched_metadata(),
        importer.LEGACY_RESOURCE_HASH_CONTRACT,
    )
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=_baseline_metadata(),
        candidate_metadata=explicit_v1_candidate,
        expected=True,
        case_name="markerless-v1 baseline with explicit-v1 candidate",
    )
    return explicit_v1_candidate


async def _assert_transport_neutral_hash_compatibility(
    database: Database,
    schema: str,
) -> dict[str, object]:
    """Accept matching explicit-v2 parent contracts."""
    explicit_v2_baseline = _metadata_with_hash_identity(
        _baseline_metadata(),
        importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    explicit_v2_candidate = _metadata_with_hash_identity(
        _matched_metadata(),
        importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=explicit_v2_baseline,
        candidate_metadata=explicit_v2_candidate,
        expected=True,
        case_name="matching explicit-v2 parents",
    )
    return explicit_v2_baseline


async def _assert_semantic_hash_compatibility(
    database: Database,
    schema: str,
) -> tuple[dict[str, object], dict[str, object]]:
    """Accept matching semantic-v3 parents and embedded proofs."""
    semantic_baseline = _metadata_with_hash_identity(
        _baseline_metadata(),
        importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    semantic_candidate = _metadata_with_hash_identity(
        _matched_metadata(),
        importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=semantic_baseline,
        candidate_metadata=semantic_candidate,
        expected=True,
        case_name="matching semantic-v3 parents and proofs",
    )
    return semantic_baseline, semantic_candidate


async def _assert_embedded_semantic_drift_rejected(
    database: Database,
    schema: str,
    semantic_baseline: dict[str, object],
    semantic_candidate: dict[str, object],
) -> None:
    """Reject projection-date and resource-scope drift inside proof objects."""
    candidate_proof_drift = copy.deepcopy(semantic_candidate)
    candidate_proof_drift[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["proof"][
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY
    ] = "2026-08-10"
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=semantic_baseline,
        candidate_metadata=candidate_proof_drift,
        expected=False,
        case_name="candidate proof projection date drift",
    )

    baseline_proof_drift = copy.deepcopy(semantic_baseline)
    baseline_proof_drift[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["proof"][
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    ] = ["Practitioner"]
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=baseline_proof_drift,
        candidate_metadata=semantic_candidate,
        expected=False,
        case_name="baseline proof resource scope drift",
    )


async def _assert_semantic_parent_proof_drift(
    database: Database,
    schema: str,
    semantic_baseline: dict[str, object],
    semantic_candidate: dict[str, object],
) -> None:
    """Reject matching drift applied to both parent metadata and its proof."""
    candidate_scope_drift = copy.deepcopy(semantic_candidate)
    candidate_scope_drift[
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    ] = ["Organization", "Practitioner"]
    candidate_scope_drift[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["proof"][
        importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    ] = ["Organization", "Practitioner"]
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=semantic_baseline,
        candidate_metadata=candidate_scope_drift,
        expected=False,
        case_name="candidate parent and proof scope drift",
    )

    baseline_projection_drift = copy.deepcopy(semantic_baseline)
    baseline_projection_drift[
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY
    ] = "2026-08-10"
    baseline_projection_drift[importer.TWIN_ROOT_VERIFICATION_METADATA_KEY]["proof"][
        importer.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY
    ] = "2026-08-10"
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=baseline_projection_drift,
        candidate_metadata=semantic_candidate,
        expected=False,
        case_name="baseline parent and proof projection date drift",
    )


async def _assert_hash_contract_drift_rejected(
    database: Database,
    schema: str,
    explicit_v1_candidate: dict[str, object],
    explicit_v2_baseline: dict[str, object],
) -> None:
    """Reject mismatched, null, and unknown resource-hash contracts."""
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=explicit_v2_baseline,
        candidate_metadata=explicit_v1_candidate,
        expected=False,
        case_name="candidate and baseline hash contract mismatch",
    )

    invalid_contract_candidate = copy.deepcopy(explicit_v1_candidate)
    invalid_contract_candidate[importer.RESOURCE_HASH_CONTRACT_METADATA_KEY] = None
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=_baseline_metadata(),
        candidate_metadata=invalid_contract_candidate,
        expected=False,
        case_name="explicit-null hash contract",
    )
    invalid_contract_candidate[
        importer.RESOURCE_HASH_CONTRACT_METADATA_KEY
    ] = "unknown_contract"
    await _assert_metadata_case(
        database,
        schema,
        baseline_metadata=_baseline_metadata(),
        candidate_metadata=invalid_contract_candidate,
        expected=False,
        case_name="unknown hash contract",
    )


@pytest.mark.asyncio
async def test_twin_hash_identity_is_exact_in_options_and_compact_fence(
    monkeypatch,
):
    """Prove v1/v2/v3 identity parity and fail-closed drift handling."""
    async with _candidate_database(monkeypatch) as (database, schema):
        await _set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        explicit_v1_candidate = await _assert_legacy_hash_compatibility(
            database, schema
        )
        explicit_v2_baseline = await _assert_transport_neutral_hash_compatibility(
            database, schema
        )
        semantic_baseline, semantic_candidate = await _assert_semantic_hash_compatibility(
            database, schema
        )
        await _assert_embedded_semantic_drift_rejected(
            database, schema, semantic_baseline, semantic_candidate
        )
        await _assert_semantic_parent_proof_drift(
            database, schema, semantic_baseline, semantic_candidate
        )
        await _assert_hash_contract_drift_rejected(
            database, schema, explicit_v1_candidate, explicit_v2_baseline
        )


@pytest.mark.asyncio
async def test_verified_gate_rejects_config_and_profile_drift(monkeypatch):
    async with _candidate_database(monkeypatch) as (database, schema):
        await _set_all_source_profiles(
            database,
            schema,
            importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
        )
        await _set_source_metadata(
            database,
            schema,
            "source_a",
            _source_metadata(
                status=importer.PROVIDER_DIRECTORY_TWIN_ROOT_VERIFIED,
                configured_endpoint_id="replacement_endpoint",
            ),
        )
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]
        for invalid_metadata in (
            _source_metadata(status="unknown"),
            _source_metadata(status=None),
        ):
            await _set_source_metadata(
                database, schema, "source_a", invalid_metadata
            )
            assert _option_ids(await _artifact_options(database, schema)) == [
                "dataset_current"
            ]
        await _set_source_metadata(database, schema, "source_a", _source_metadata())
        await _set_source_metadata(database, schema, "source_b", {})
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]


@pytest.mark.asyncio
async def test_removed_review_profile_cannot_reclassify_twin_datasets(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        profile_absent_metadata = _source_metadata(
            status=None,
            campaign_id=None,
        )
        for source_id in ("source_a", "source_b"):
            await _set_source_metadata(
                database,
                schema,
                source_id,
                profile_absent_metadata,
            )
        assert _option_ids(await _artifact_options(database, schema)) == [
            "dataset_current"
        ]


@pytest.mark.asyncio
async def test_genuine_established_candidate_keeps_profile_absent_path(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_source (
                source_id, endpoint_id, metadata_json
            ) VALUES (
                'established_source', 'established_endpoint',
                CAST(:source_metadata AS jsonb)
            );
            """,
            source_metadata=json.dumps(
                {
                    "provider_directory_supported_resources": ["Organization"],
                    "provider_directory_fully_enumerable_resources": [
                        "Organization"
                    ],
                }
            ),
        )
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                dataset_hash, status, is_current, resource_count,
                publication_metadata_json
            ) VALUES
                ('established_current', 'established_endpoint', 'root_current',
                 :dataset_hash, :published, true, 1, '{{}}'::jsonb),
                ('established_candidate', 'established_endpoint', 'root_candidate',
                 :dataset_hash, :validated, false, 1,
                 CAST(:candidate_metadata AS jsonb));
            """,
            dataset_hash=DATASET_HASH,
            published=importer.ENDPOINT_DATASET_PUBLISHED,
            validated=importer.ENDPOINT_DATASET_VALIDATED,
            candidate_metadata=json.dumps(
                {
                    "requires_twin_root_verification": False,
                    "source_ids": ["established_source"],
                }
            ),
        )
        await _seal_core_datasets(database, schema)
        options = await _artifact_options(
            database,
            schema,
            "established_endpoint",
        )
        assert _option_ids(options) == [
            "established_candidate",
            "established_current",
        ]


async def _insert_policy_two_dataset(
    database: Database,
    schema: str,
    dataset_row: dict[str, object],
    policy_document: dict[str, object],
) -> dict[str, object]:
    explicit_policy_row = copy.deepcopy(dataset_row)
    explicit_policy_row["publication_metadata_json"][
        importer.REVIEWED_ROOT_POLICY_METADATA_KEY
    ] = policy_document
    await database.status(
        f"""
        INSERT INTO {schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, validated_at, publication_metadata_json,
            completion_proof_required_version, completion_proof_json,
            completion_proof_sha256
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id, :root_run_id, NULL,
            :dataset_hash, :status, false, :resource_count, :validated_at,
            CAST(:metadata AS jsonb), :required_version,
            CAST(:completion_proof AS jsonb), :completion_sha256
        );
        """,
        dataset_id=explicit_policy_row["dataset_id"],
        endpoint_id=explicit_policy_row["endpoint_id"],
        root_run_id=explicit_policy_row["acquisition_root_run_id"],
        dataset_hash=explicit_policy_row["dataset_hash"],
        status=explicit_policy_row["status"],
        resource_count=explicit_policy_row["resource_count"],
        validated_at=(
            datetime.fromisoformat(
                explicit_policy_row["validated_at"]
            ).replace(tzinfo=None)
            if explicit_policy_row["validated_at"]
            else None
        ),
        metadata=json.dumps(explicit_policy_row["publication_metadata_json"]),
        required_version=explicit_policy_row["completion_proof_required_version"],
        completion_proof=json.dumps(explicit_policy_row["completion_proof_json"]),
        completion_sha256=explicit_policy_row["completion_proof_sha256"],
    )
    return explicit_policy_row


async def _insert_policy_two_subset_pair(
    candidate_store: Database,
    schema: str,
) -> tuple[dict[str, object], dict[str, object]]:
    await candidate_store.status(
        f"ALTER TABLE {schema}.provider_directory_endpoint_dataset "
        "ADD COLUMN import_run_id varchar(64), ADD COLUMN previous_dataset_id varchar(96), "
        "ADD COLUMN validated_at timestamp, "
        "ADD COLUMN published_at timestamp;"
    )
    source_record, dataset_rows, _evidence = activation_inputs()
    policy_document = importer.ReviewedRootPolicy(2).document()
    source_metadata = copy.deepcopy(source_record["metadata_json"])
    source_metadata["provider_directory_candidate_status"] = importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED
    source_metadata[importer.REVIEWED_ROOT_POLICY_METADATA_KEY] = policy_document
    await candidate_store.status(
        f"""
        INSERT INTO {schema}.provider_directory_source (
            source_id, endpoint_id, metadata_json
        ) VALUES (:source_id, :endpoint_id, CAST(:metadata AS jsonb));
        """,
        source_id=source_record["source_id"],
        endpoint_id=source_record["endpoint_id"],
        metadata=json.dumps(source_metadata),
    )
    for dataset_row in dataset_rows:
        candidate = await _insert_policy_two_dataset(candidate_store, schema, dataset_row, policy_document)
    return source_record, candidate


@pytest.mark.asyncio
async def test_explicit_policy_two_candidate_remains_eligible_and_publishable(
    monkeypatch,
):
    async with _candidate_database(monkeypatch) as (database, schema):
        source_record, candidate = await _insert_policy_two_subset_pair(database, schema)
        endpoint_id = candidate["endpoint_id"]
        candidate_id = candidate["dataset_id"]
        assert _option_ids(await _artifact_options(database, schema, endpoint_id)) == [
            candidate_id
        ]
        assert await _compact_candidate_ids(
            database, endpoint_id, source_record["source_id"]
        ) == [candidate_id]

        monkeypatch.setattr(importer, "db", database)
        await importer._publish_validated_artifact_dataset(
            importer.ProviderDirectoryArtifactDataset(
                source_id=source_record["source_id"],
                endpoint_id=endpoint_id,
                dataset_id=candidate_id,
                evidence_run_id=candidate["acquisition_root_run_id"],
                status=importer.ENDPOINT_DATASET_VALIDATED,
                is_current=False,
                dataset_hash=candidate["dataset_hash"],
                resource_count=candidate["resource_count"],
                reviewed_root_policy=importer.ReviewedRootPolicy(2),
                completion_proof_required_version=3,
            )
        )
        published = await database.first(
            f"""SELECT status, is_current, published_at
                  FROM {schema}.provider_directory_endpoint_dataset
                 WHERE dataset_id = :dataset_id""",
            dataset_id=candidate_id,
        )
        assert published is not None
        assert published._mapping["status"] == importer.ENDPOINT_DATASET_PUBLISHED
        assert published._mapping["is_current"] is True
        assert published._mapping["published_at"] is not None
