# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for one-row streaming receipt backfill."""

from __future__ import annotations

import hashlib
import importlib
import json

import pytest

from process.provider_directory_admission_seal import (
    AdmissionSealError,
    backfill_provider_directory_admission_seal,
)
from process.uhc_canonical_proof import (
    UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY,
)
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
    _set_shared_semantic_proof,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_backfill_streams_full_proof_and_is_idempotent(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(64)
        await _set_shared_semantic_proof(database, schema, metadata)
        seal_result = await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )

        assert seal_result["status"] == "sealed"
        assert seal_result["admission_kind"] == "generic"
        assert seal_result["resource_types"] == ["Location"]
        receipt_row = await database.first(
            f"""
            SELECT publication_metadata_summary_json,
                   publication_metadata_sha256,
                   content_proof_admission_version,
                   content_proof_admission_kind,
                   content_proof_admission_sha256,
                   content_proof_resource_types
              FROM {schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset_shared'
            """
        )
        assert receipt_row is not None
        row_map = receipt_row._mapping
        assert row_map["content_proof_admission_version"] == 1
        assert row_map["content_proof_admission_kind"] == "generic"
        assert row_map["content_proof_resource_types"] == ["Location"]
        summary = row_map["publication_metadata_summary_json"]
        assert "provider_directory_content_proof_v1" not in summary
        assert summary["provider_directory_content_proof_admission_summary_v1"] == {
            "dataset_hash": "e" * 64,
            "resource_count": 64,
            "resource_hashes": {"Location": "f" * 64},
            "resource_counts": {"Location": 64},
        }

        assert await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        ) == {
            "dataset_id": "dataset_shared",
            "status": "already_sealed",
            "admission_kind": "generic",
        }


async def _assert_bounded_followup_projections(database) -> None:
    """Verify sealed source and profile reads project only bounded metadata."""

    summary_row = await database.first(
        importer._artifact_source_summary_candidate_sql(),
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        previous_dataset_id=None,
        dataset_hash="e" * 64,
        is_current=True,
        expected_status=importer.ENDPOINT_DATASET_PUBLISHED,
    )
    profile_row = await importer._current_profile_dataset_map("source_primary")
    assert summary_row is not None
    for projected_metadata in (
        dict(summary_row._mapping)["publication_metadata_json"],
        profile_row["publication_metadata_json"],
    ):
        assert "provider_directory_content_proof_v1" not in projected_metadata
        assert projected_metadata["source_ids"] == [
            "source_primary",
            "source_sibling",
        ]


async def _assert_additive_summary_preserves_raw_proof(database, schema: str) -> None:
    """Verify an additive summary write leaves the large raw proof unchanged."""

    raw_before = await database.first(
        f"SELECT md5(publication_metadata_json::text) AS digest, "
        f"octet_length(publication_metadata_json::text) AS byte_count "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared'"
    )
    fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"]
    )
    selected_dataset = next(
        dataset_option
        for dataset_option in fence.datasets
        if dataset_option.dataset_id == "dataset_shared"
    )
    await importer._record_current_dataset_publication_proof(
        selected_dataset,
        "synthetic_additive_proof",
        {"verified": True},
    )
    raw_after = await database.first(
        f"SELECT md5(publication_metadata_json::text) AS digest, "
        f"octet_length(publication_metadata_json::text) AS byte_count, "
        "publication_metadata_summary_json -> "
        "'synthetic_additive_proof' AS additive_proof, "
        "publication_metadata_sha256 = "
        f"{schema}.provider_directory_endpoint_dataset_admission_metadata_sha256("
        "publication_metadata_summary_json, "
        "content_proof_admission_version, "
        "content_proof_admission_kind, "
        "content_proof_admission_sha256, "
        "content_proof_resource_types) AS digest_valid "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared'"
    )
    assert raw_before is not None and raw_after is not None
    assert raw_after._mapping["digest"] == raw_before._mapping["digest"]
    assert raw_after._mapping["byte_count"] == raw_before._mapping["byte_count"]
    assert raw_after._mapping["additive_proof"] == {"verified": True}
    assert raw_after._mapping["digest_valid"] is True


@pytest.mark.asyncio
async def test_sealed_followup_reads_do_not_return_large_raw_proof(monkeypatch):
    """Keep follow-up reads bounded and raw proof bytes immutable after sealing."""

    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(2048)
        await _set_shared_semantic_proof(database, schema, metadata)
        assert await database.scalar(
            f"SELECT octet_length(publication_metadata_json::text) > 1048576 "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) is True
        await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )
        await _assert_bounded_followup_projections(database)
        await _assert_additive_summary_preserves_raw_proof(database, schema)


@pytest.mark.asyncio
async def test_backfill_rejects_resealed_inner_shard_drift(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(2)
        proof = metadata["provider_directory_content_proof_v1"]
        proof["shards"][0]["resource_count"] = 2
        await _set_shared_semantic_proof(database, schema, metadata)

        with pytest.raises(AdmissionSealError, match="shard"):
            await backfill_provider_directory_admission_seal(
                "dataset_shared",
                database=database,
            )
        assert await database.scalar(
            f"SELECT content_proof_admission_sha256 IS NULL "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) is True


@pytest.mark.asyncio
async def test_backfill_binds_completion_resource_summary(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(2)
        proof = metadata["provider_directory_content_proof_v1"]
        assert isinstance(proof, dict)
        proof["resource_hashes"] = {"Location": "a" * 64}
        unsigned_proof_by_field = dict(proof)
        unsigned_proof_by_field.pop("proof_sha256")
        proof["proof_sha256"] = hashlib.sha256(
            json.dumps(
                unsigned_proof_by_field,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        await _set_shared_semantic_proof(database, schema, metadata)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET completion_proof_required_version = 3, "
            "completion_proof_json = CAST(:proof AS jsonb) "
            "WHERE dataset_id = 'dataset_shared';",
            proof=json.dumps({"dataset": {}}),
        )
        with pytest.raises(AdmissionSealError, match="completion_summary"):
            await backfill_provider_directory_admission_seal(
                "dataset_shared",
                database=database,
            )

        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET completion_proof_json = CAST(:proof AS jsonb) "
            "WHERE dataset_id = 'dataset_shared';",
            proof=json.dumps(
                {
                    "dataset": {
                        "resource_hashes": {"Location": "f" * 64},
                        "resource_counts": {"Location": 2},
                    }
                }
            ),
        )

        with pytest.raises(AdmissionSealError, match="completion_summary"):
            await backfill_provider_directory_admission_seal(
                "dataset_shared",
                database=database,
            )
        assert await database.scalar(
            f"SELECT content_proof_admission_sha256 IS NULL "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) is True


@pytest.mark.asyncio
async def test_backfill_rejects_unsupported_legacy_canonical_proof(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = {
            "source_ids": ["source_primary", "source_sibling"],
            "selected_resources": ["Location"],
            UHC_CANONICAL_CONTENT_PROOF_METADATA_KEY: {
                "proof_sha256": "a" * 64,
                "resource_counts": {"Location": 1},
            },
        }
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(:metadata AS jsonb), "
            "dataset_hash = :dataset_hash, resource_count = 1 "
            "WHERE dataset_id = 'dataset_shared';",
            metadata=json.dumps(metadata),
            dataset_hash="e" * 64,
        )

        with pytest.raises(AdmissionSealError, match="uhc_backfill_unsupported"):
            await backfill_provider_directory_admission_seal(
                "dataset_shared",
                database=database,
            )
        assert await database.scalar(
            f"SELECT content_proof_admission_sha256 IS NULL "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) is True
