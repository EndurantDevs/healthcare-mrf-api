# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded selection contracts for sealed candidate datasets."""

from __future__ import annotations

import json

import pytest

from api.provider_directory_source_catalog_outcomes import (
    _canonical_validated_datasets_by_source_id,
)
from process.provider_directory_admission_seal import (
    admission_seal_from_validated_metadata,
    backfill_provider_directory_admission_seal,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
from process.uhc_canonical_proof import UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _insert_validated_shared_dataset,
    importer,
)
from tests.provider_directory_dataset_artifact_pg_support import (
    seal_validated_dataset as _seal_validated_dataset,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _HASH_CASES,
    _all_source_projected_rows,
    _install_unrelated_large_proof_hash_sentinel,
    _large_metadata_by_field,
    _proof_line_hash,
    _replace_shared_metadata,
    _resealed_proof_mutation,
    _set_shared_semantic_proof,
)


def _fresh_acquisition_candidate():
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint_shared",
        dataset_id="dataset_fresh",
        acquisition_root_run_id="root-fresh",
        source_ids=("source_primary",),
        selected_resources=("Location",),
        expected_resources=("Location",),
        import_run_id="run-fresh",
        previous_dataset_id="dataset_shared",
        resource_hash_contract=(
            importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        ),
    )


async def _invalidate_legacy_candidate_proof(database, schema: str) -> None:
    metadata = _large_metadata_by_field(
        1,
        dataset_id="dataset_legacy_invalid",
        root_run_id="root-legacy-invalid",
    )
    proof = metadata[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    proof["shards"][0]["resource_count"] = 2
    proof["shards"][0]["resource_counts"] = {"Location": 2}
    proof["shard_set_sha256"] = _proof_line_hash(proof["shards"])
    proof.pop("proof_sha256")
    proof["proof_sha256"] = importer._identity_hash(proof)
    await _set_shared_semantic_proof(
        database,
        schema,
        metadata,
        dataset_id="dataset_legacy_invalid",
    )


async def _assert_sealed_metadata_closed(
    database,
    schema: str,
) -> None:
    await _install_metadata_open_sentinel(database, schema)
    predicate = importer._validated_endpoint_candidate_blocks_admission_sql()
    assert await database.scalar(
        f"""
        WITH raw_metadata AS MATERIALIZED (
            SELECT {schema}.fail_if_metadata_is_opened() AS metadata
        )
        SELECT ({predicate})
          FROM (
              SELECT 'dataset_legacy_invalid'::varchar AS dataset_id,
                     'endpoint_shared'::varchar AS endpoint_id,
                     'root-legacy-invalid'::varchar AS acquisition_root_run_id,
                     NULL::varchar AS import_run_id,
                     repeat('e', 64)::varchar AS dataset_hash,
                     1::bigint AS resource_count,
                     (SELECT metadata FROM raw_metadata)
                         AS publication_metadata_json,
                     NULL::jsonb AS artifact_selection_receipt_json,
                     '{{}}'::jsonb AS publication_metadata_summary_json,
                     {schema}.provider_directory_endpoint_dataset_admission_metadata_sha256(
                         '{{}}'::jsonb, 1::smallint, 'generic'::text,
                         repeat('a', 64), ARRAY['Location']::varchar[]
                     ) AS publication_metadata_sha256,
                     1::smallint AS content_proof_admission_version,
                     'generic'::varchar AS content_proof_admission_kind,
                     repeat('a', 64)::varchar AS content_proof_admission_sha256,
                     ARRAY['Location']::varchar[] AS content_proof_resource_types
          ) AS dataset;
        """
    ) is True


@pytest.mark.asyncio
async def test_invalid_validated_candidate_does_not_block_fresh_acquisition(
    monkeypatch,
):
    """Ignore a proven-invalid legacy proof but retain valid blockers."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(
            database,
            schema,
            dataset_id="dataset_legacy_invalid",
            root_run_id="root-legacy-invalid",
            seal=False,
        )
        candidate = _fresh_acquisition_candidate()
        async with database.acquire() as connection:
            with pytest.raises(RuntimeError, match="active_conflict"):
                await importer._assert_no_conflicting_endpoint_candidate(
                    connection, candidate
                )

        await _invalidate_legacy_candidate_proof(database, schema)
        async with database.acquire() as connection:
            assert await importer._assert_no_conflicting_endpoint_candidate(
                connection, candidate
            ) == candidate

        await _seal_validated_dataset(
            database, schema, "dataset_legacy_invalid"
        )
        await _assert_sealed_metadata_closed(database, schema)
        async with database.acquire() as connection:
            locked = await importer._locked_endpoint_verification_state(
                connection, candidate
            )
            assert locked["publication_metadata_json"] is None
            with pytest.raises(RuntimeError, match="active_conflict"):
                await importer._assert_no_conflicting_endpoint_candidate(
                    connection, candidate
                )


@pytest.mark.asyncio
async def test_sealed_canonical_proof_uses_bounded_receipt_over_legacy_cap(
    monkeypatch,
):
    """Treat a sealed non-generic proof as valid without opening its body."""

    async with _dataset_database(monkeypatch) as (database, schema):
        receipt = await _seal_oversized_canonical_metadata(database, schema)
        await _install_metadata_open_sentinel(database, schema)
        legacy_bounded_sql = importer._artifact_legacy_metadata_is_bounded_sql(
            f"{schema}.fail_if_metadata_is_opened()",
            "dataset",
        )
        assert await database.scalar(
            f"""
            SELECT ({legacy_bounded_sql})
              FROM {schema}.provider_directory_endpoint_dataset AS dataset
             WHERE dataset_id = 'dataset_shared';
            """
        ) is False

        _assert_canonical_projection(
            await _all_source_projected_rows(database),
            receipt.metadata_sha256,
        )


async def _seal_oversized_canonical_metadata(database, schema: str):
    metadata_by_field = {
        "source_ids": ["source_primary", "source_sibling"],
        "selected_resources": ["Location"],
        "expected_resources": ["Location"],
        UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: {
            "proof_sha256": "a" * 64,
            "resource_counts": {"Location": 1},
            "synthetic_large_body": "x" * 1_100_000,
        },
    }
    receipt = admission_seal_from_validated_metadata(metadata_by_field)
    assert receipt is not None and receipt.admission_kind == "uhc_canonical"
    assert len(json.dumps(metadata_by_field).encode()) > 1024 * 1024
    await database.status(
        f"""
        UPDATE {schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json = CAST(:metadata AS json),
               publication_metadata_summary_json = CAST(:summary AS jsonb),
               publication_metadata_sha256 = :metadata_sha256,
               content_proof_admission_version = :admission_version,
               content_proof_admission_kind = :admission_kind,
               content_proof_admission_sha256 = :proof_sha256,
               content_proof_resource_types = CAST(:resource_types AS varchar[])
         WHERE dataset_id = 'dataset_shared';
        """,
        metadata=json.dumps(metadata_by_field),
        summary=json.dumps(receipt.metadata_summary),
        metadata_sha256=receipt.metadata_sha256,
        admission_version=receipt.admission_version,
        admission_kind=receipt.admission_kind,
        proof_sha256=receipt.proof_sha256,
        resource_types=list(receipt.resource_types),
    )
    return receipt


async def _install_metadata_open_sentinel(database, schema: str) -> None:
    await database.status(
        f"""CREATE FUNCTION {schema}.fail_if_metadata_is_opened()
        RETURNS jsonb LANGUAGE plpgsql VOLATILE AS $function$ BEGIN
            RAISE EXCEPTION 'sealed metadata was opened';
        END $function$;"""
    )


def _assert_canonical_projection(projected_rows, metadata_sha256: str) -> None:
    assert len(projected_rows) == 2
    assert all(
        projected["publication_metadata_hash"] == metadata_sha256
        and UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY
        not in projected["publication_metadata_json"]
        and projected["content_proof_present"] is False
        and projected["content_proof_valid"] is True
        and projected["content_proof_resources"] is None
        for projected in projected_rows
    )


@pytest.mark.asyncio
async def test_final_publish_uses_sealed_summary_over_legacy_cap(monkeypatch):
    """Publish a sealed generic candidate without reopening its shard array."""

    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        metadata_by_field = _large_metadata_by_field(
            2_048,
            dataset_id="dataset_candidate",
            endpoint_id="endpoint_shared",
            root_run_id="root-candidate",
        )
        serialized_metadata = json.dumps(metadata_by_field)
        assert len(serialized_metadata.encode()) > 1024 * 1024
        receipt = admission_seal_from_validated_metadata(metadata_by_field)
        assert receipt is not None
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = CAST(:metadata AS json),
                   publication_metadata_summary_json = CAST(:summary AS jsonb),
                   publication_metadata_sha256 = :metadata_sha256,
                   content_proof_admission_version = :admission_version,
                   content_proof_admission_kind = :admission_kind,
                   content_proof_admission_sha256 = :proof_sha256,
                   content_proof_resource_types = CAST(:resource_types AS varchar[]),
                   dataset_hash = :dataset_hash,
                   resource_count = :resource_count
             WHERE dataset_id = 'dataset_candidate';
            """,
            metadata=serialized_metadata,
            summary=json.dumps(receipt.metadata_summary),
            metadata_sha256=receipt.metadata_sha256,
            admission_version=receipt.admission_version,
            admission_kind=receipt.admission_kind,
            proof_sha256=receipt.proof_sha256,
            resource_types=list(receipt.resource_types),
            dataset_hash="e" * 64,
            resource_count=2_048,
        )
        candidate = importer.ProviderDirectoryArtifactDataset(
            source_id="source_primary",
            endpoint_id="endpoint_shared",
            serving_endpoint_id="endpoint_shared",
            dataset_id="dataset_candidate",
            evidence_run_id="root-candidate",
            expected_incumbent_dataset_id="dataset_shared",
            status=importer.ENDPOINT_DATASET_VALIDATED,
            is_current=False,
            dataset_hash="e" * 64,
            resource_count=2_048,
            promote_on_cutover=True,
        )

        await importer._publish_validated_artifact_dataset(candidate)

        published_status = await database.scalar(
            f"SELECT status FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_candidate';"
        )
        assert published_status == importer.ENDPOINT_DATASET_PUBLISHED


@pytest.mark.asyncio
async def test_explicit_selection_does_not_evaluate_unrelated_metadata(monkeypatch):
    """Scope the candidate-aware catalog path before full proof work."""
    async with _dataset_database(monkeypatch) as (database, schema):
        await _insert_validated_shared_dataset(database, schema)
        metadata_by_field = _large_metadata_by_field(
            2,
            dataset_id="dataset_candidate",
            endpoint_id="endpoint_shared",
            root_run_id="root-candidate",
        )
        await database.status(
            f"""
            UPDATE {schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = CAST(:metadata AS json),
                   dataset_hash = :dataset_hash,
                   resource_count = 2
             WHERE dataset_id = 'dataset_candidate';
            """,
            metadata=json.dumps(metadata_by_field),
            dataset_hash="e" * 64,
        )
        await backfill_provider_directory_admission_seal(
            "dataset_candidate",
            database=database,
        )
        await _install_unrelated_large_proof_hash_sentinel(database, schema)
        selected = await _canonical_validated_datasets_by_source_id(
            ["source_primary"]
        )
        assert selected["source_primary"].dataset_id == "dataset_candidate"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation_name",
    [
        "shard_descriptor",
        "source_metrics",
        "resource_keyset",
        "dataset_hash",
        "cross_contract",
        "projection_date",
        "selected_outside_scope",
    ],
)
async def test_all_source_projection_rejects_resealed_semantic_proof_mutation(
    monkeypatch,
    mutation_name,
):
    """Require full server-side proof validation, not only its outer seal."""

    async with _dataset_database(monkeypatch) as (database, schema):
        mutated_metadata = _resealed_proof_mutation(
            _large_metadata_by_field(),
            mutation_name,
        )
        await _set_shared_semantic_proof(database, schema, mutated_metadata)

        projected_row_list = await _all_source_projected_rows(database)

        assert len(projected_row_list) == 2
        assert all(
            projected_row["content_proof_valid"] is False
            for projected_row in projected_row_list
        )


@pytest.mark.asyncio
async def test_all_source_projection_normalizes_expected_source_scope(monkeypatch):
    """Preserve sorted proof lineage when parent source order is arbitrary."""

    async with _dataset_database(monkeypatch) as (database, schema):
        metadata_by_field = _large_metadata_by_field()
        metadata_by_field["source_ids"] = ["source_sibling", "source_primary"]
        await _set_shared_semantic_proof(database, schema, metadata_by_field)

        projected_row_list = await _all_source_projected_rows(database)

        assert len(projected_row_list) == 2
        assert all(
            projected_row["content_proof_valid"] is True
            for projected_row in projected_row_list
        )


@pytest.mark.asyncio
async def test_payload_hash_documents_python_identity_boundary(monkeypatch):
    """Pin exact parity for ordinary JSON and safe divergence at edge values."""

    async with _dataset_database(monkeypatch) as (database, schema):
        hash_function = f'"{schema}"."provider_directory_subset_payload_sha256"'
        for hash_input, has_python_identity_parity in _HASH_CASES:
            server_hash = await database.scalar(
                f"SELECT {hash_function}(CAST(:hash_input AS jsonb));",
                hash_input=json.dumps(hash_input, ensure_ascii=False),
            )
            assert server_hash == canonical_payload_sha256(hash_input)
            assert (
                server_hash == importer._identity_hash(hash_input)
            ) is has_python_identity_parity


@pytest.mark.asyncio
async def test_payload_hash_fences_unknown_metadata_drift(monkeypatch):
    """Keep same-version lock identity exact across Unicode and float metadata."""

    async with _dataset_database(monkeypatch) as (database, schema):
        metadata_by_field = {
            "selected_resources": ["Location"],
            "synthetic_label": "synthetic-ž",
            "synthetic_weight": 0.0,
        }
        await _replace_shared_metadata(database, schema, metadata_by_field)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        assert fence.datasets[0].publication_metadata_hash == (
            canonical_payload_sha256(metadata_by_field)
        )
        assert fence.datasets[0].publication_metadata_hash != (
            importer._identity_hash(metadata_by_field)
        )
        async with database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(fence, database)

        metadata_by_field["synthetic_future_field"] = "changed"
        await _replace_shared_metadata(database, schema, metadata_by_field)
        async with database.transaction():
            with pytest.raises(
                importer.ProviderDirectoryArtifactBuildStale,
                match="provider_directory_endpoint_dataset_metadata_changed",
            ):
                await importer._lock_and_verify_artifact_dataset_fence(
                    fence,
                    database,
                )
