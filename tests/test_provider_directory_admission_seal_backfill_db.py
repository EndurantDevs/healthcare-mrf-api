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
from tests.uhc_final_publication_test_support import final_publication_fixture


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_backfill_streams_full_proof_and_is_idempotent(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(64)
        await _set_shared_semantic_proof(database, schema, metadata)
        result = await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )

        assert result["status"] == "sealed"
        assert result["admission_kind"] == "generic"
        assert result["resource_types"] == ["Location"]
        row = await database.first(
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
        assert row is not None
        row_map = row._mapping
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


@pytest.mark.asyncio
async def test_sealed_followup_reads_do_not_return_large_raw_proof(monkeypatch):
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
        for projected in (
            dict(summary_row._mapping)["publication_metadata_json"],
            profile_row["publication_metadata_json"],
        ):
            assert "provider_directory_content_proof_v1" not in projected
            assert projected["source_ids"] == [
                "source_primary",
                "source_sibling",
            ]

        raw_before = await database.first(
            f"SELECT md5(publication_metadata_json::text) AS digest, "
            f"octet_length(publication_metadata_json::text) AS byte_count "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        )
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        dataset = next(
            item
            for item in fence.datasets
            if item.dataset_id == "dataset_shared"
        )
        await importer._record_current_dataset_publication_proof(
            dataset,
            importer.PROVIDER_DIRECTORY_OUTCOME_RESOURCE_COUNTS_METADATA_KEY,
            {"verified": True},
        )
        raw_after = await database.first(
            f"SELECT md5(publication_metadata_json::text) AS digest, "
            f"octet_length(publication_metadata_json::text) AS byte_count, "
            "publication_metadata_summary_json -> "
            "'outcome_resource_counts_v1' AS additive_proof, "
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
        assert raw_after._mapping["byte_count"] == (
            raw_before._mapping["byte_count"]
        )
        assert raw_after._mapping["additive_proof"] == {"verified": True}
        assert raw_after._mapping["digest_valid"] is True


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
        unsigned = dict(proof)
        unsigned.pop("proof_sha256")
        proof["proof_sha256"] = hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
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
async def test_backfill_validates_bounded_legacy_canonical_proof(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        state, _expectation = final_publication_fixture(
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            acquisition_root_run_id="root-shared",
        )
        metadata = state["publication_metadata_json"]
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset "
            "SET publication_metadata_json = CAST(:metadata AS jsonb), "
            "dataset_hash = :dataset_hash, resource_count = :resource_count "
            "WHERE dataset_id = 'dataset_shared';",
            metadata=json.dumps(metadata),
            dataset_hash=state["dataset_hash"],
            resource_count=state["resource_count"],
        )

        result = await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )

        assert result["status"] == "sealed"
        assert result["admission_kind"] == "uhc_canonical"
        assert await database.scalar(
            f"SELECT content_proof_admission_kind "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) == "uhc_canonical"
