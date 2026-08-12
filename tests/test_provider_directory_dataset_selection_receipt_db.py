# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for bounded Provider Directory selection receipts."""

from __future__ import annotations

import json

import pytest

from api.provider_directory_source_catalog_outcomes import (
    _canonical_validated_datasets_by_source_id,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
    importer,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _all_source_projected_rows,
    _large_metadata_by_field,
    _set_shared_semantic_proof,
)


_ZERO_SUMMARY_COUNTS = {
    "address_records": 0,
    "addressed_locations": 0,
    "distinct_npis": 0,
    "geocoded_locations": 0,
    "individual_practitioners": 0,
    "network_plan_links": 0,
    "organization_affiliation_links": 0,
    "organization_resources": 0,
    "practitioner_role_resources": 0,
}


def _receipt_candidate_and_proof():
    metadata = _large_metadata_by_field(
        dataset_id="dataset_candidate",
        root_run_id="root-candidate",
    )
    proof = metadata[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_shared",
        dataset_id="dataset_candidate",
        acquisition_root_run_id="root-candidate",
        source_ids=("source_primary", "source_sibling"),
        selected_resources=("Location",),
        expected_resources=("Location",),
        import_run_id=None,
        previous_dataset_id="dataset_shared",
        resource_hash_contract=(
            importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        semantic_projection_as_of="2026-08-09",
        proof_resource_scope=("Location",),
    )
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash=proof["dataset_hash"],
        resource_count=proof["resource_count"],
        resource_hashes=dict(proof["resource_hashes"]),
        resource_counts=dict(proof["resource_counts"]),
        source_metrics=dict(proof["source_metrics"]),
        proof_metadata=proof,
    )
    return candidate, content_proof


def _large_metadata_with_normalized_receipt() -> dict[str, object]:
    candidate, content_proof = _receipt_candidate_and_proof()
    summary = importer._build_endpoint_dataset_source_summary(
        candidate,
        content_proof,
        _ZERO_SUMMARY_COUNTS,
        "root-candidate",
    )
    return importer._dataset_validation_metadata(
        candidate,
        {},
        content_proof,
        {},
        {},
        {},
        {importer.SOURCE_SUMMARY_METADATA_KEY: summary},
    )


async def _install_selected_hash_sentinel(database, schema: str) -> None:
    await database.status(
        f"ALTER FUNCTION {schema}.provider_directory_subset_payload_sha256(jsonb) "
        "RENAME TO provider_directory_subset_payload_sha256_original;"
    )
    proof_key = importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    await database.status(
        f"""CREATE FUNCTION {schema}.provider_directory_subset_payload_sha256(candidate jsonb)
        RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        AS $function$ BEGIN
            IF jsonb_typeof(candidate #> '{{{proof_key},shards}}') = 'array'
            THEN RAISE EXCEPTION 'selected_large_proof_hashed'; END IF;
            RETURN {schema}.provider_directory_subset_payload_sha256_original(candidate);
        END; $function$;"""
    )


async def _install_selected_validator_sentinel(database, schema: str) -> None:
    signature = "jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb"
    await database.status(
        f"ALTER FUNCTION {schema}.provider_directory_subset_content_proof_valid("
        f"{signature}) RENAME TO provider_directory_subset_content_proof_valid_original;"
    )
    await database.status(
        f"""CREATE FUNCTION {schema}.provider_directory_subset_content_proof_valid(
            candidate jsonb, expected_dataset_id text, expected_endpoint_id text,
            expected_root_run_id text, expected_source_ids jsonb,
            expected_selected_resources jsonb, expected_dataset_hash text,
            expected_resource_count bigint, expected_resource_hashes jsonb,
            expected_resource_counts jsonb
        ) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE
        AS $function$ BEGIN
            IF expected_dataset_id IN ('dataset_candidate', 'dataset_shared') THEN
                RAISE EXCEPTION 'selected_large_proof_validated'; END IF;
            RETURN {schema}.provider_directory_subset_content_proof_valid_original(
                candidate, expected_dataset_id, expected_endpoint_id,
                expected_root_run_id, expected_source_ids,
                expected_selected_resources, expected_dataset_hash,
                expected_resource_count, expected_resource_hashes,
                expected_resource_counts
            );
        END; $function$;"""
    )


def _out_of_scope_receipt_metadata() -> dict[str, object]:
    metadata = _large_metadata_with_normalized_receipt()
    candidate, content_proof = _receipt_candidate_and_proof()
    expanded_proof = importer.EndpointDatasetContentProof(
        dataset_hash=content_proof.dataset_hash,
        resource_count=content_proof.resource_count,
        resource_hashes={
            **content_proof.resource_hashes,
            "Practitioner": "0" * 64,
        },
        resource_counts={**content_proof.resource_counts, "Practitioner": 0},
    )
    metadata[importer.SOURCE_SUMMARY_METADATA_KEY] = (
        importer._build_endpoint_dataset_source_summary(
            candidate,
            expanded_proof,
            _ZERO_SUMMARY_COUNTS,
            "root-candidate",
        )
    )
    metadata[
        importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
    ] = importer._outcome_resource_count_proof(candidate, expanded_proof)
    return metadata


async def _install_receipt_candidate(database, schema: str) -> None:
    await _insert_validated_shared_dataset(database, schema)
    metadata = _large_metadata_with_normalized_receipt()
    await _set_shared_semantic_proof(
        database,
        schema,
        metadata,
        dataset_id="dataset_candidate",
    )
    await _set_selection_receipt(
        database, schema, metadata, dataset_id="dataset_candidate"
    )


async def _set_selection_receipt(
    database, schema: str, metadata, *, dataset_id: str
) -> None:
    receipt = importer._artifact_selection_receipt(metadata)
    assert receipt is not None
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
        "artifact_selection_receipt_json = CAST(:receipt_json AS jsonb) "
        "WHERE dataset_id = :dataset_id;",
        receipt_json=json.dumps(receipt, sort_keys=True),
        dataset_id=dataset_id,
    )


def _current_receipt_metadata() -> dict[str, object]:
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_shared",
        dataset_id="dataset_shared",
        acquisition_root_run_id="root-shared",
        source_ids=("source_primary", "source_sibling"),
        selected_resources=("Location",),
        expected_resources=("Location",),
        import_run_id=None,
        previous_dataset_id=None,
        resource_hash_contract=(
            importer.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        semantic_projection_as_of="2026-08-09",
        proof_resource_scope=("Location",),
    )
    metadata = _large_metadata_by_field()
    proof = metadata[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    content_proof = importer.EndpointDatasetContentProof(
        dataset_hash=proof["dataset_hash"],
        resource_count=proof["resource_count"],
        resource_hashes=dict(proof["resource_hashes"]),
        resource_counts=dict(proof["resource_counts"]),
        source_metrics=dict(proof["source_metrics"]),
        proof_metadata=proof,
    )
    summary = importer._build_endpoint_dataset_source_summary(
        candidate, content_proof, _ZERO_SUMMARY_COUNTS, "root-shared"
    )
    receipt_metadata = importer._dataset_validation_metadata(
        candidate,
        {},
        content_proof,
        {},
        {},
        {},
        {importer.SOURCE_SUMMARY_METADATA_KEY: summary},
    )
    return receipt_metadata


async def _install_current_receipt(database, schema: str) -> None:
    receipt_metadata = _current_receipt_metadata()
    await _set_shared_semantic_proof(
        database,
        schema,
        receipt_metadata,
    )
    await _set_selection_receipt(
        database, schema, receipt_metadata, dataset_id="dataset_shared"
    )


def test_non_null_receipt_is_authoritative_and_fail_closed():
    sql = importer._provider_directory_artifact_dataset_selection_sql(
        ["source_primary"]
    )
    assert "dataset.artifact_selection_receipt_json IS NOT NULL" in sql
    assert "selected.receipt_stored" in sql
    assert "selected.receipt_jsonb" in sql


@pytest.mark.asyncio
async def test_non_null_malformed_receipt_never_uses_legacy_proof(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_current_receipt(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "artifact_selection_receipt_json = '{}'::jsonb "
            "WHERE dataset_id = 'dataset_shared';"
        )
        await _install_selected_hash_sentinel(database, schema)
        await _install_selected_validator_sentinel(database, schema)

        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
            await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"],
                should_select_validated_candidates=False,
            )


@pytest.mark.asyncio
async def test_all_source_projection_keeps_large_proof_server_side(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field()
        assert len(json.dumps(metadata, sort_keys=True)) > 200_000
        await _set_shared_semantic_proof(database, schema, metadata)

        rows = await _all_source_projected_rows(database)

        assert len(rows) == 2
        assert all(
            importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
            not in row["publication_metadata_json"]
            and row["content_proof_valid"] is True
            and row["content_proof_resources"] == ["Location"]
            for row in rows
        )
        assert max(len(json.dumps(row, default=str).encode()) for row in rows) < 8192


@pytest.mark.asyncio
async def test_normalized_receipt_bounds_selected_large_proof(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_receipt_candidate(database, schema)
        await _install_selected_hash_sentinel(database, schema)
        await _install_selected_validator_sentinel(database, schema)

        catalog = await _canonical_validated_datasets_by_source_id(
            ["source_primary"]
        )
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=True,
        )
        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(
                fence, database
            )

        candidate = fence.datasets[0]
        assert catalog["source_primary"].dataset_id == "dataset_candidate"
        assert candidate.dataset_id == "dataset_candidate"
        assert candidate.expected_incumbent_dataset_id == "dataset_shared"
        assert candidate.promote_on_cutover is True
        assert candidate.artifact_resources == ("Location",)


@pytest.mark.asyncio
async def test_normalized_receipt_bounds_selected_large_current(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_current_receipt(database, schema)
        await _install_selected_hash_sentinel(database, schema)
        await _install_selected_validator_sentinel(database, schema)

        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=False,
        )
        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(
                fence, database
            )

        assert fence.datasets[0].dataset_id == "dataset_shared"
        assert fence.datasets[0].promote_on_cutover is False
        assert fence.datasets[0].artifact_resources == ("Location",)


@pytest.mark.asyncio
async def test_current_repair_records_one_guarded_receipt(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _current_receipt_metadata()
        await _set_shared_semantic_proof(database, schema, metadata)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"],
            should_select_validated_candidates=False,
        )

        await importer._record_current_dataset_selection_receipt(
            fence.datasets[0],
            metadata,
        )

        stored_receipt = await database.scalar(
            f"SELECT artifact_selection_receipt_json FROM {schema}."
            "provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared';"
        )
        assert stored_receipt == importer._artifact_selection_receipt(metadata)

        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET status = 'validated' WHERE dataset_id = 'dataset_shared';"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="provider_directory_endpoint_dataset_metadata_changed",
        ):
            await importer._record_current_dataset_selection_receipt(
                fence.datasets[0], metadata
            )


@pytest.mark.asyncio
async def test_normalized_receipt_rejects_resource_outside_proof_scope(
    monkeypatch,
):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        metadata = _out_of_scope_receipt_metadata()
        await _set_shared_semantic_proof(
            database,
            schema,
            metadata,
            dataset_id="dataset_candidate",
        )
        await _set_selection_receipt(
            database, schema, metadata, dataset_id="dataset_candidate"
        )

        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
            await importer._resolve_provider_directory_artifact_datasets(
                ["source_primary"],
                should_select_validated_candidates=True,
            )


def _receipt_tamper_sql(schema: str, mutation: str) -> str:
    proof_key = importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
    path_and_value = {
        "summary": (
            f"{importer.SOURCE_SUMMARY_METADATA_KEY},summary_sha256",
            "to_jsonb(repeat('0', 64))",
        ),
        "outcome": (
            f"{importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY},resource_count",
            "to_jsonb(999)",
        ),
        "proof_sha": (
            f"{proof_key},proof_sha256",
            f"to_jsonb(' '::text || (artifact_selection_receipt_json #>> '{{{proof_key},proof_sha256}}'))",
        ),
        "proof_contract": (
            f"{proof_key},contract_id",
            f"to_jsonb(' '::text || (artifact_selection_receipt_json #>> '{{{proof_key},contract_id}}'))",
        ),
    }[mutation]
    path, value = path_and_value
    return (
        f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
        "artifact_selection_receipt_json = jsonb_set("
        f"artifact_selection_receipt_json, '{{{path}}}', {value}) "
        "WHERE dataset_id = 'dataset_candidate';"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["summary", "outcome", "proof_sha", "proof_contract"]
)
async def test_normalized_receipt_fence_rejects_drift(monkeypatch, mutation):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_receipt_candidate(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"], should_select_validated_candidates=True
        )
        await database.status(_receipt_tamper_sql(schema, mutation))

        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
            async with database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(
                    fence, database
                )


@pytest.mark.asyncio
async def test_normalized_receipt_does_not_reopen_audit_shards(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _install_receipt_candidate(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"], should_select_validated_candidates=True
        )
        proof_key = importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "publication_metadata_json = CAST(jsonb_set("
            "publication_metadata_json::jsonb, "
            f"'{{{proof_key},shards,0,artifact_byte_count}}', '2') AS json) "
            "WHERE dataset_id = 'dataset_candidate';"
        )

        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(
                fence, database
            )


@pytest.mark.asyncio
async def test_partial_receipt_uses_legacy_validation(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        metadata = _large_metadata_with_normalized_receipt()
        metadata.pop(
            importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY
        )
        await _set_shared_semantic_proof(
            database, schema, metadata, dataset_id="dataset_candidate"
        )

        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"], should_select_validated_candidates=True
        )

        assert fence.datasets[0].normalized_receipt_present is False
