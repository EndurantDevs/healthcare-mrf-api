# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable PostgreSQL proof for one-row streaming receipt backfill."""

from __future__ import annotations

import hashlib
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import db.models as db_models
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
backfill = importlib.import_module("process.provider_directory_admission_backfill")


@pytest.mark.asyncio
async def test_backfill_streams_full_proof_and_is_idempotent(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        metadata = _large_metadata_by_field(64)
        await _set_shared_semantic_proof(database, schema, metadata)
        backfill_status_by_field = await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )

        assert backfill_status_by_field["status"] == "sealed"
        assert backfill_status_by_field["admission_kind"] == "generic"
        assert backfill_status_by_field["resource_types"] == ["Location"]
        dataset_record = await database.first(
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
        assert dataset_record is not None
        row_map = dataset_record._mapping
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
        monkeypatch.setattr(db_models, "db", database)
        with pytest.raises(AdmissionSealError, match="dataset_missing"):
            await backfill_provider_directory_admission_seal("dataset_missing")


def _unsealed_locked_row(**updates):
    row_by_field = {
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "completion_proof_required_version": None,
        "completion_resource_hashes": None,
        "completion_resource_counts": None,
        "raw_metadata_bytes": 1,
        "evidence_run_id": "root",
        "dataset_hash": "e" * 64,
        "resource_count": 1,
    }
    row_by_field.update(updates)
    return row_by_field


@pytest.mark.parametrize(
    ("updates", "error"),
    [
        ({"status": "incomplete"}, "status_invalid"),
        ({"completion_proof_required_version": 2}, "completion_summary_invalid"),
        ({"raw_metadata_bytes": 0}, "metadata_size_invalid"),
        (
            {"raw_metadata_bytes": backfill.ADMISSION_RAW_METADATA_MAX_BYTES + 1},
            "metadata_size_invalid",
        ),
        ({"evidence_run_id": None}, "parent_identity_invalid"),
        ({"dataset_hash": None}, "parent_identity_invalid"),
        ({"resource_count": True}, "parent_identity_invalid"),
        ({"resource_count": "1"}, "parent_identity_invalid"),
    ],
)
def test_backfill_rejects_invalid_locked_row(updates, error):
    """Reject each unsafe legacy-row boundary before streaming metadata."""

    with pytest.raises(AdmissionSealError, match=error):
        backfill._validated_row_metadata_size(_unsealed_locked_row(**updates))


def test_backfill_rejects_partial_seal():
    """Never treat a partially written admission receipt as reusable."""

    dataset_row = dict.fromkeys(backfill._SEAL_FIELDS)
    dataset_row["publication_metadata_summary_json"] = {}
    with pytest.raises(AdmissionSealError, match="partial_seal"):
        backfill._existing_seal_result("dataset", dataset_row)


def test_backfill_rejects_invalid_schema(monkeypatch):
    """Keep the dynamic schema identifier outside generated SQL."""

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-schema")
    with pytest.raises(AdmissionSealError, match="schema_invalid"):
        backfill._qualified_dataset_table()


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_id", ["", " dataset", "dataset "])
async def test_backfill_rejects_invalid_dataset_id(dataset_id):
    """Reject blank or whitespace-shifted dataset identities."""

    with pytest.raises(AdmissionSealError, match="dataset_id_invalid"):
        await backfill_provider_directory_admission_seal(dataset_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "copy_status", "error"),
    [
        (b"x" * 133, "COPY 1", "copy_size_invalid"),
        (b"x", "COPY 0", "copy_lost"),
    ],
)
async def test_backfill_rejects_invalid_copy(
    monkeypatch,
    tmp_path,
    payload,
    copy_status,
    error,
):
    """Fail closed when bounded COPY output grows or loses its source row."""

    async def copy_from_query(*_args, output, **_kwargs):
        await output(payload)
        return copy_status

    monkeypatch.setattr(backfill, "ADMISSION_RAW_METADATA_MAX_BYTES", 4)
    connection = SimpleNamespace(copy_from_query=copy_from_query)
    dataset_row_by_field = {
        "dataset_id": "dataset",
        "row_ctid": "(0,1)",
        "row_xmin": "1",
    }
    with pytest.raises(AdmissionSealError, match=error):
        await backfill._copy_metadata(
            connection,
            '"mrf"."provider_directory_endpoint_dataset"',
            dataset_row_by_field,
            tmp_path,
        )


@pytest.mark.asyncio
async def test_backfill_rejects_lost_seal_update():
    """Reject a seal write whose row-version fence no longer matches."""

    connection = SimpleNamespace(execute=AsyncMock(return_value="UPDATE 0"))
    dataset_row_by_field = {
        "dataset_id": "dataset",
        "row_ctid": "(0,1)",
        "row_xmin": "1",
    }
    seal = SimpleNamespace(
        metadata_summary={},
        metadata_sha256="a" * 64,
        admission_version=1,
        admission_kind="generic",
        proof_sha256="b" * 64,
        resource_types=("Location",),
    )
    with pytest.raises(AdmissionSealError, match="backfill_lost"):
        await backfill._store_seal(
            connection,
            '"mrf"."provider_directory_endpoint_dataset"',
            dataset_row_by_field,
            seal,
        )


@pytest.mark.asyncio
async def test_sealed_followup_reads_do_not_return_large_raw_proof(monkeypatch):
    """Read and update only bounded summaries after a row is sealed."""

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
        await _assert_additive_proof_preserves_raw_metadata(database, schema)


async def _assert_bounded_followup_projections(database) -> None:
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


async def _assert_additive_proof_preserves_raw_metadata(database, schema: str) -> None:
    fence = await importer._resolve_provider_directory_artifact_datasets(
        ["source_primary"]
    )
    dataset = next(
        candidate_dataset
        for candidate_dataset in fence.datasets
        if candidate_dataset.dataset_id == "dataset_shared"
    )
    await database.status(
        f"UPDATE {schema}.provider_directory_endpoint_dataset "
        "SET artifact_selection_receipt_json = CAST(:receipt AS jsonb) "
        "WHERE dataset_id = 'dataset_shared'",
        receipt=json.dumps({"stale": True}),
    )
    raw_before = await database.first(
        f"SELECT md5(publication_metadata_json::text) AS digest, "
        f"octet_length(publication_metadata_json::text) AS byte_count "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared'"
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
        "publication_metadata_summary_json, content_proof_admission_version, "
        "content_proof_admission_kind, content_proof_admission_sha256, "
        "content_proof_resource_types) AS digest_valid "
        ", artifact_selection_receipt_json IS NULL "
        "AS selection_receipt_invalidated "
        f"FROM {schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset_shared'"
    )
    assert raw_before is not None and raw_after is not None
    assert raw_after._mapping["digest"] == raw_before._mapping["digest"]
    assert raw_after._mapping["byte_count"] == raw_before._mapping["byte_count"]
    assert raw_after._mapping["additive_proof"] == {"verified": True}
    assert raw_after._mapping["digest_valid"] is True
    assert raw_after._mapping["selection_receipt_invalidated"] is True


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

        backfill_status_by_field = await backfill_provider_directory_admission_seal(
            "dataset_shared",
            database=database,
        )

        assert backfill_status_by_field["status"] == "sealed"
        assert backfill_status_by_field["admission_kind"] == "uhc_canonical"
        assert await database.scalar(
            f"SELECT content_proof_admission_kind "
            f"FROM {schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared'"
        ) == "uhc_canonical"
