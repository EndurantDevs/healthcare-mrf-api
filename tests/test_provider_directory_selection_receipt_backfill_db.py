# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded repair coverage for one legacy selection receipt."""

from __future__ import annotations

import importlib
import json
from unittest.mock import AsyncMock

from click.testing import CliRunner
import pytest

import process
from tests.test_provider_directory_dataset_artifact_db import _dataset_database
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _set_shared_semantic_proof,
)
from tests.test_provider_directory_dataset_selection_receipt_db import (
    _current_receipt_metadata,
    _install_selected_hash_sentinel,
    _install_selected_validator_sentinel,
    importer,
)


backfill = importlib.import_module(
    "process.provider_directory_selection_receipt_backfill"
)


async def _install_repairable_current(database, schema: str, *, short: int = 0):
    metadata = _current_receipt_metadata()
    await _set_shared_semantic_proof(database, schema, metadata)
    await database.status(
        f"DELETE FROM {schema}.provider_directory_dataset_resource "
        "WHERE dataset_id = 'dataset_shared' AND resource_type <> 'Location';"
    )
    proof = metadata[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY]
    expected_count = proof["resource_counts"]["Location"]
    existing_count = await database.scalar(
        f"SELECT count(*) FROM {schema}.provider_directory_dataset_resource "
        "WHERE dataset_id = 'dataset_shared' AND resource_type = 'Location';"
    )
    missing_count = expected_count - int(existing_count or 0) - short
    if missing_count > 0:
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_dataset_resource (
                dataset_id, resource_type, resource_id, payload_hash, payload_json
            )
            SELECT 'dataset_shared', 'Location',
                   'repair-location-' || generate_series,
                   repeat('f', 64), '{{}}'::json
              FROM generate_series(1, :missing_count);
            """,
            missing_count=missing_count,
        )
    return metadata


@pytest.mark.asyncio
async def test_backfill_dry_run_apply_and_idempotent_replay(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        metadata = await _install_repairable_current(database, schema)

        dry_run = await backfill.backfill_provider_directory_selection_receipt(
            "dataset_shared",
            database=database,
        )

        assert dry_run["status"] == "validated"
        assert dry_run["receipt_bytes"] < 64 * 1024
        assert dry_run["resource_count"] == 512
        assert (
            await database.scalar(
                f"SELECT artifact_selection_receipt_json IS NULL FROM {schema}."
                "provider_directory_endpoint_dataset "
                "WHERE dataset_id = 'dataset_shared';"
            )
            is True
        )

        applied = await backfill.backfill_provider_directory_selection_receipt(
            "dataset_shared",
            apply=True,
            database=database,
        )
        assert applied["status"] == "stored"
        assert await database.scalar(
            f"SELECT artifact_selection_receipt_json FROM {schema}."
            "provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset_shared';"
        ) == importer._artifact_selection_receipt(metadata)
        assert (
            await database.scalar(
                f"SELECT publication_metadata_summary_json IS NULL "
                f"AND content_proof_admission_version IS NULL FROM {schema}."
                "provider_directory_endpoint_dataset "
                "WHERE dataset_id = 'dataset_shared';"
            )
            is True
        )
        await _install_selected_hash_sentinel(database, schema)
        await _install_selected_validator_sentinel(database, schema)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary", "source_sibling"],
            should_select_validated_candidates=False,
        )
        assert [dataset.dataset_id for dataset in fence.datasets] == [
            "dataset_shared",
            "dataset_shared",
        ]

        replay = await backfill.backfill_provider_directory_selection_receipt(
            "dataset_shared",
            apply=True,
            database=database,
        )
        assert replay["status"] == "already_stored"


@pytest.mark.asyncio
async def test_backfill_rejects_retained_count_drift(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _install_repairable_current(database, schema, short=1)

        with pytest.raises(
            backfill.ProviderDirectorySelectionReceiptBackfillError,
            match="retained_counts_changed",
        ):
            await backfill.backfill_provider_directory_selection_receipt(
                "dataset_shared",
                database=database,
            )


@pytest.mark.asyncio
async def test_backfill_rejects_unproven_retained_resource_type(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _install_repairable_current(database, schema)
        await database.status(
            f"INSERT INTO {schema}.provider_directory_dataset_resource ("
            "dataset_id, resource_type, resource_id, payload_hash, payload_json"
            ") VALUES ('dataset_shared', 'Organization', 'unexpected', "
            "repeat('a', 64), '{}'::json);"
        )

        with pytest.raises(
            backfill.ProviderDirectorySelectionReceiptBackfillError,
            match="retained_counts_changed",
        ):
            await backfill.backfill_provider_directory_selection_receipt(
                "dataset_shared",
                database=database,
            )


@pytest.mark.asyncio
async def test_backfill_rejects_invalid_or_conflicting_receipt(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _install_repairable_current(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "artifact_selection_receipt_json = '{}'::jsonb "
            "WHERE dataset_id = 'dataset_shared';"
        )

        with pytest.raises(
            backfill.ProviderDirectorySelectionReceiptBackfillError,
            match="stored_receipt_invalid",
        ):
            await backfill.backfill_provider_directory_selection_receipt(
                "dataset_shared",
                apply=True,
                database=database,
            )


@pytest.mark.asyncio
async def test_backfill_rejects_any_admission_seal_state(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        metadata = await _install_repairable_current(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "artifact_selection_receipt_json = CAST(:receipt AS jsonb), "
            "content_proof_admission_version = 1 "
            "WHERE dataset_id = 'dataset_shared';",
            receipt=json.dumps(importer._artifact_selection_receipt(metadata)),
        )

        with pytest.raises(
            backfill.ProviderDirectorySelectionReceiptBackfillError,
            match="admission_state_invalid",
        ):
            await backfill.backfill_provider_directory_selection_receipt(
                "dataset_shared",
                apply=True,
                database=database,
            )


@pytest.mark.asyncio
async def test_backfill_rejects_source_summary_drift(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _install_repairable_current(database, schema)
        await database.status(
            f"UPDATE {schema}.provider_directory_endpoint_dataset SET "
            "publication_metadata_json = jsonb_set("
            "publication_metadata_json::jsonb, "
            "'{source_summary_v1,summary_sha256}', to_jsonb(repeat('0', 64))) "
            "WHERE dataset_id = 'dataset_shared';"
        )

        with pytest.raises(
            backfill.ProviderDirectorySelectionReceiptBackfillError,
            match="metadata_invalid",
        ):
            await backfill.backfill_provider_directory_selection_receipt(
                "dataset_shared",
                database=database,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset_id", ["", " dataset", "dataset "])
async def test_backfill_rejects_invalid_dataset_id(dataset_id):
    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="dataset_id_invalid",
    ):
        await backfill.backfill_provider_directory_selection_receipt(dataset_id)


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema"),
    [("mrf", "other"), ("invalid-name", None)],
)
def test_backfill_rejects_invalid_schema(monkeypatch, runtime_schema, legacy_schema):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", runtime_schema)
    if legacy_schema is None:
        monkeypatch.delenv("DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("DB_SCHEMA", legacy_schema)

    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="schema_invalid",
    ):
        backfill._schema()


@pytest.mark.parametrize("raw_metadata", ["{", []])
def test_backfill_rejects_invalid_json_object(raw_metadata):
    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="metadata_invalid",
    ):
        backfill._json_object(raw_metadata)


@pytest.mark.asyncio
async def test_backfill_rejects_invalid_retained_summary():
    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="metadata_invalid",
    ):
        await backfill._require_retained_resource_counts(
            AsyncMock(), '"mrf"."resource"', "dataset-synthetic", {}
        )


def test_backfill_rejects_invalid_current_row():
    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="dataset_state_invalid",
    ):
        backfill._validated_current_row({})


def test_backfill_rejects_invalid_receipt_structure():
    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="metadata_invalid",
    ):
        backfill._validated_receipt({"bounded_publication_metadata_json": {}})


@pytest.mark.asyncio
async def test_backfill_rejects_lost_receipt_update():
    connection = AsyncMock()
    connection.execute.return_value = "UPDATE 0"
    dataset_by_field = {
        "dataset_id": "dataset-synthetic",
        "row_ctid": "(1,1)",
        "row_xmin": "1",
        "endpoint_id": "endpoint-synthetic",
        "evidence_run_id": "run-synthetic",
        "previous_dataset_id": None,
        "dataset_hash": "a" * 64,
        "resource_count": 1,
        "status": "published",
    }

    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="backfill_lost",
    ):
        await backfill._store_receipt(
            connection, '"mrf"."dataset"', dataset_by_field, {}
        )


@pytest.mark.asyncio
async def test_backfill_rejects_missing_dataset():
    connection = AsyncMock()
    connection.fetchrow.return_value = None

    with pytest.raises(
        backfill.ProviderDirectorySelectionReceiptBackfillError,
        match="dataset_missing",
    ):
        await backfill._load_dataset_by_field(
            connection,
            '"mrf"."dataset"',
            "dataset-synthetic",
            lock=False,
        )


def test_backfill_sql_projects_only_bounded_metadata():
    sql = backfill._dataset_row_sql('"mrf"."dataset"', lock=False)

    assert "WITH selected_dataset AS MATERIALIZED" in sql
    assert "jsonb_to_record" in sql
    assert sql.count("publication_metadata_json::jsonb") == 2
    assert "proofless_publication_metadata_json -> 'source_summary_v1'" in sql
    assert "proofless_publication_metadata_json -> 'outcome_resource_counts_v1'" in sql
    assert "SELECT dataset.publication_metadata_json" not in sql
    assert "FOR UPDATE" not in sql
    assert "FOR UPDATE OF raw_dataset" in backfill._dataset_row_sql(
        '"mrf"."dataset"', lock=True
    )


def test_process_cli_defaults_to_validation_and_requires_apply(monkeypatch):
    operation = AsyncMock(return_value={"status": "validated"})
    monkeypatch.setattr(
        process,
        "backfill_provider_directory_selection_receipt",
        operation,
    )

    result = CliRunner().invoke(
        process.process_group,
        [
            "provider-directory-selection-receipt-backfill",
            "--dataset-id",
            "dataset-synthetic",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {"status": "validated"}
    operation.assert_awaited_once_with(
        "dataset-synthetic",
        apply=False,
    )
