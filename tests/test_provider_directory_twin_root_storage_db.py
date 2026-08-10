# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import uuid

from tests.test_provider_directory_twin_root_verification_db import (
    _candidate,
    _create_dataset_table,
    _disposable_database,
)


importer = importlib.import_module("process.provider_directory_fhir")


async def test_postgres_stores_verification_baseline_without_status_parameter_ambiguity(
    monkeypatch,
):
    """Execute the terminal dataset update through asyncpg's real type inference."""
    schema = f"provider_directory_status_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = await _disposable_database()

    is_schema_created = False
    candidate = _candidate()
    try:
        await _create_dataset_table(database, schema)
        is_schema_created = True
        await database.status(
            f"""
            INSERT INTO {schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                status, is_current, resource_count
            ) VALUES (:dataset_id, :endpoint_id, :acquisition_root_run_id,
                      :status, false, 0);
            """,
            dataset_id=candidate.dataset_id,
            endpoint_id=candidate.endpoint_id,
            acquisition_root_run_id=candidate.acquisition_root_run_id,
            status=importer.ENDPOINT_DATASET_ACQUIRING,
        )
        await importer._store_validated_endpoint_dataset(
            database,
            candidate,
            candidate.previous_dataset_id,
            "d" * 64,
            288_056,
            {"verification": "baseline"},
            status=importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        )
        stored_dataset = await database.first(
            f"""
            SELECT previous_dataset_id, dataset_hash, status, resource_count,
                   validated_at, publication_metadata_json
            FROM {schema}.provider_directory_endpoint_dataset
            WHERE dataset_id = :dataset_id;
            """,
            dataset_id=candidate.dataset_id,
        )
        assert stored_dataset is not None
        stored_dataset_by_field = stored_dataset._mapping
        assert stored_dataset_by_field["previous_dataset_id"] == candidate.previous_dataset_id
        assert stored_dataset_by_field["dataset_hash"] == "d" * 64
        assert stored_dataset_by_field["status"] == importer.ENDPOINT_DATASET_VERIFICATION_BASELINE
        assert stored_dataset_by_field["resource_count"] == 288_056
        assert stored_dataset_by_field["validated_at"] is None
        assert stored_dataset_by_field["publication_metadata_json"] == {
            "verification": "baseline"
        }
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()
