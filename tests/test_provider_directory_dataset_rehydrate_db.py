# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import getpass
import hashlib
import importlib
import json
import os
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetRehydrationCheckpoint,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
    ProviderDirectoryLocation,
    ProviderDirectorySource,
    ProviderDirectorySourceResource,
)
from process.provider_directory_dataset_rehydrate import (
    DatasetRehydrationError,
    DatasetScope,
    RehydrationRequest,
    RehydrationRuntime,
    rehydrate_current_dataset,
)


importer = importlib.import_module("process.provider_directory_fhir")
rehydration = importlib.import_module(
    "process.provider_directory_dataset_rehydrate"
)

SOURCE_ID = "pdfhir_humana_fixture"
ENDPOINT_ID = "endpoint_humana_fixture"
DATASET_ID = "pdds_humana_fixture"
ROOT_RUN_ID = "run_humana_fixture_root"
CANONICAL_BASE = "https://fhir.humana.fixture.test/api"


async def _require_disposable_postgres(database: Database) -> None:
    """Skip unless the configured PostgreSQL database is explicitly a test DB."""
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("dataset rehydration DB test needs disposable PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("dataset rehydration DB test refuses a non-test database")


def _fixture_payloads() -> tuple[dict[str, object], ...]:
    """Return deterministic retained mapped Location payloads."""
    return tuple(
        {
            "resource_id": f"location-{location_number}",
            "status": "active",
            "name": f"Humana Fixture {location_number}",
            "city_name": "Louisville",
            "state_code": "KY",
            "postal_code": "40202",
        }
        for location_number in range(1, 4)
    )


def _payload_hash(mapped_payload: dict[str, object]) -> str:
    """Reproduce the retained mapped-payload hash."""
    encoded_payload = json.dumps(mapped_payload, sort_keys=True)
    return hashlib.sha256(encoded_payload.encode()).hexdigest()


def _dataset_hash(mapped_payloads: tuple[dict[str, object], ...]) -> str:
    """Build the ordered dataset identity digest."""
    dataset_digest = hashlib.sha256()
    for payload_index, mapped_payload in enumerate(mapped_payloads):
        if payload_index:
            dataset_digest.update(b"\n")
        identity_parts = (
            "Location",
            str(mapped_payload["resource_id"]),
            _payload_hash(mapped_payload),
        )
        dataset_digest.update(
            json.dumps(identity_parts, separators=(",", ":")).encode()
        )
    return dataset_digest.hexdigest()


def _rehydration_models() -> tuple[type, ...]:
    """Return fixture tables in dependency order."""
    return (
        ProviderDirectoryAPIEndpoint,
        ProviderDirectorySource,
        ProviderDirectoryEndpointDataset,
        ProviderDirectoryDatasetResource,
        ProviderDirectoryCanonicalResource,
        ProviderDirectorySourceResource,
        ProviderDirectoryDatasetRehydrationCheckpoint,
        ProviderDirectoryLocation,
    )


async def _create_fixture_tables(database: Database, schema: str) -> None:
    """Create only the relations exercised by rehydration."""
    for model in _rehydration_models():
        await database.status(
            importer._provider_directory_artifact_scope_table_sql(
                model, schema, model.__tablename__
            )
        )


async def _insert_fixture(database: Database, schema: str) -> None:
    """Insert one current published dataset with three retained resources."""
    mapped_payloads = _fixture_payloads()
    await database.status(
        f"INSERT INTO {schema}.provider_directory_api_endpoint ("
        "endpoint_id, canonical_api_base, credential_descriptor_hash, "
        "endpoint_signature_hash) VALUES ("
        ":endpoint_id, :canonical_base, repeat('c', 64), repeat('s', 64));",
        endpoint_id=ENDPOINT_ID,
        canonical_base=CANONICAL_BASE,
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_source ("
        "source_id, org_name, api_base, canonical_api_base, endpoint_id, "
        "requires_registration, requires_api_key) VALUES ("
        ":source_id, 'Humana Fixture', :canonical_base, :canonical_base, "
        ":endpoint_id, false, false);",
        source_id=SOURCE_ID,
        canonical_base=CANONICAL_BASE,
        endpoint_id=ENDPOINT_ID,
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, import_run_id, acquisition_root_run_id, "
        "dataset_hash, status, is_current, resource_count, created_at, "
        "validated_at, published_at, publication_metadata_json) VALUES ("
        ":dataset_id, :endpoint_id, 'run_fixture_terminal', :root_run_id, "
        ":dataset_hash, 'published', true, :resource_count, now(), now(), "
        "now(), CAST(:metadata_json AS json));",
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        root_run_id=ROOT_RUN_ID,
        dataset_hash=_dataset_hash(mapped_payloads),
        resource_count=len(mapped_payloads),
        metadata_json=json.dumps(
            {"selected_resources": ["Location"], "source_ids": [SOURCE_ID]}
        ),
    )
    for mapped_payload in mapped_payloads:
        await _insert_retained_payload(database, schema, mapped_payload)


async def _insert_retained_payload(
    database: Database,
    schema: str,
    mapped_payload: dict[str, object],
) -> None:
    """Insert one retained mapped resource with its canonical hash."""
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource ("
        "dataset_id, resource_type, resource_id, payload_hash, payload_json) "
        "VALUES (:dataset_id, 'Location', :resource_id, :payload_hash, "
        "CAST(:payload_json AS json));",
        dataset_id=DATASET_ID,
        resource_id=mapped_payload["resource_id"],
        payload_hash=_payload_hash(mapped_payload),
        payload_json=json.dumps(mapped_payload, sort_keys=True),
    )


@asynccontextmanager
async def _fixture_database(monkeypatch):
    """Yield an isolated schema in local disposable PostgreSQL."""
    database_defaults_by_variable = {
        "HLTHPRT_DB_DRIVER": "asyncpg",
        "HLTHPRT_DB_HOST": "127.0.0.1",
        "HLTHPRT_DB_PORT": "5440",
        "HLTHPRT_DB_USER": getpass.getuser(),
        "HLTHPRT_DB_PASSWORD": "",
        "HLTHPRT_DB_DATABASE": "healthporta_test",
    }
    for variable_name, default_value in database_defaults_by_variable.items():
        monkeypatch.setenv(
            variable_name,
            os.getenv(variable_name, default_value),
        )
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "4")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT_MIN_ROWS", "1")
    schema = f"provider_directory_rehydrate_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f"CREATE SCHEMA {schema};")
        is_schema_created = True
        await _create_fixture_tables(database, schema)
        await _insert_fixture(database, schema)
        for model in _rehydration_models():
            monkeypatch.setattr(model.__table__, "schema", schema)
        monkeypatch.setattr(importer, "db", database)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        await database.disconnect()


async def _normal_upsert(
    model: type,
    typed_rows: list[dict[str, object]],
    scope: DatasetScope,
) -> int:
    """Write typed rows through the production provenance path."""
    return await importer._upsert_resource_rows(
        model,
        typed_rows,
        run_id=scope.acquisition_root_run_id,
        track_seen=False,
        canonical_api_base=scope.canonical_api_base,
        source_ids=[scope.source_id],
    )


def _runtime(database: Database, schema: str) -> RehydrationRuntime:
    """Build a runtime using production Location upserts."""
    return RehydrationRuntime(
        database=database,
        schema=schema,
        models_by_type={"Location": ProviderDirectoryLocation},
        upsert_batch=_normal_upsert,
    )


def _request(owner_run_id: str) -> RehydrationRequest:
    """Build one exact fixture request with single-row transactions."""
    return RehydrationRequest(
        source_id=SOURCE_ID,
        dataset_id=DATASET_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        owner_run_id=owner_run_id,
        batch_size=1,
    )


async def _persistence_counts(database: Database, schema: str) -> tuple[int, ...]:
    """Count typed, canonical, and source-edge rows for the fixture root."""
    count_record = await database.first(
        f"SELECT "
        f"(SELECT count(*) FROM {schema}.provider_directory_location "
        "WHERE source_id=:source_id AND last_seen_run_id=:root_run_id), "
        f"(SELECT count(*) FROM {schema}.provider_directory_canonical_resource "
        "WHERE canonical_api_base=:canonical_base "
        "AND last_seen_run_id=:root_run_id), "
        f"(SELECT count(*) FROM {schema}.provider_directory_source_resource "
        "WHERE source_id=:source_id AND last_seen_run_id=:root_run_id);",
        source_id=SOURCE_ID,
        root_run_id=ROOT_RUN_ID,
        canonical_base=CANONICAL_BASE,
    )
    return tuple(int(count_value) for count_value in count_record)


async def _checkpoint_state(database: Database, schema: str) -> tuple[object, ...]:
    """Read the durable Location checkpoint state and counts."""
    checkpoint_record = await database.first(
        f"SELECT state, input_count, mapped_count, last_resource_id "
        f"FROM {schema}.provider_directory_dataset_rehydration_checkpoint "
        "WHERE resource_type='Location';"
    )
    return tuple(checkpoint_record)


async def _insert_extra_membership(database: Database) -> None:
    """Add one same-root typed/canonical/source-edge identity outside dataset."""
    extra_typed_rows = [
        {
            "source_id": SOURCE_ID,
            "resource_id": "location-outside-dataset",
            "status": "active",
            "name": "Outside Dataset",
            "last_seen_run_id": ROOT_RUN_ID,
        }
    ]
    await _normal_upsert(
        ProviderDirectoryLocation,
        extra_typed_rows,
        DatasetScope(
            source_id=SOURCE_ID,
            dataset_id=DATASET_ID,
            acquisition_root_run_id=ROOT_RUN_ID,
            endpoint_id=ENDPOINT_ID,
            canonical_api_base=CANONICAL_BASE,
            dataset_hash="unused",
            resource_count=3,
            resource_types=("Location",),
            publication_metadata_hash="unused",
            published_at=None,
        ),
    )


@pytest.mark.asyncio
async def test_postgres_rolls_back_resumes_and_proves_exact_membership(
    monkeypatch,
):
    async with _fixture_database(monkeypatch) as (database, schema):
        values_upsert = AsyncMock(
            side_effect=AssertionError("COPY upsert unexpectedly fell back")
        )
        monkeypatch.setattr(importer, "_upsert_rows_values", values_upsert)
        original_save = rehydration._save_checkpoint

        async def fail_second_checkpoint(context, checkpoint_record):
            """Fail after second-batch writes but before checkpoint commit."""
            if (
                checkpoint_record.state == "running"
                and checkpoint_record.input_count == 2
            ):
                raise RuntimeError("simulated checkpoint failure")
            await original_save(context, checkpoint_record)

        monkeypatch.setattr(rehydration, "_save_checkpoint", fail_second_checkpoint)
        with pytest.raises(RuntimeError, match="simulated checkpoint failure"):
            await rehydrate_current_dataset(
                _runtime(database, schema), _request("run_fixture_first")
            )
        assert await _persistence_counts(database, schema) == (1, 1, 1)
        assert await _checkpoint_state(database, schema) == (
            "interrupted", 1, 1, "location-1"
        )

        monkeypatch.setattr(rehydration, "_save_checkpoint", original_save)
        completed_summary = await rehydrate_current_dataset(
            _runtime(database, schema), _request("run_fixture_resume")
        )
        assert await _persistence_counts(database, schema) == (3, 3, 3)
        assert completed_summary["resources"]["Location"] == {
            "state": "complete",
            "input": 3,
            "mapped": 3,
            "rejected": 0,
            "typed": 3,
            "typed_extra": 0,
            "canonical_hash_matched": 3,
            "canonical_extra": 0,
            "source_edges": 3,
            "source_edge_extra": 0,
            "reused_complete_checkpoint": False,
        }

        await _insert_extra_membership(database)
        with pytest.raises(DatasetRehydrationError, match="proof_failed"):
            await rehydrate_current_dataset(
                _runtime(database, schema), _request("run_fixture_extra")
            )
        assert await _persistence_counts(database, schema) == (4, 4, 4)
        assert (await _checkpoint_state(database, schema))[0] == "proof_failed"
        assert values_upsert.await_count == 0
