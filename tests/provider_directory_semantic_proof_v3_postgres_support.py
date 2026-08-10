# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared PostgreSQL fixtures for semantic Provider Directory proof tests."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import datetime
import importlib
import json
from types import SimpleNamespace
import uuid
from unittest.mock import AsyncMock
import zlib

import pytest
from sqlalchemy.exc import OperationalError

from tests.provider_directory_semantic_proof_v3_schema import (
    _create_tables,
    _fixture_models,
)

from db.connection import Database
from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryOrganization,
    ProviderDirectoryPractitioner,
    ProviderDirectorySourceResource,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ProviderDirectoryStoredProofOptions,
    build_stored_dataset_proof,
)
from process.provider_directory_resource_hash import (
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    resource_payload_sha256_for_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


SOURCE_ID = "source-semantic-proof"
RESOURCE_ID = "practitioner-semantic-proof"
CANONICAL_BASE = "https://directory.example.test/fhir"
PROJECTION_AS_OF = "2026-08-09"
V3_DATASET_FORWARD = "dataset-semantic-forward"
V3_DATASET_REVERSE = "dataset-semantic-reverse"
V3_DATASET_CONCURRENT = "dataset-semantic-concurrent"
V3_DATASET_ROLLBACK = "dataset-semantic-rollback"


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("semantic proof tests need disposable PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("semantic proof tests refuse a non-test database")


@asynccontextmanager
async def _semantic_database(monkeypatch):
    schema = f"provider_directory_semantic_{uuid.uuid4().hex[:12]}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "6")
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        for model in _fixture_models():
            monkeypatch.setattr(model.__table__, "schema", schema)
        await _create_tables(database, schema)
        is_schema_created = True
        monkeypatch.setattr(importer, "db", database)
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


async def _insert_parent(
    database: Database,
    schema: str,
    dataset_id: str,
    *,
    resource_hash_contract: str = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    semantic_projection_as_of: str | None = PROJECTION_AS_OF,
    selected_resources: tuple[str, ...] = ("Practitioner",),
) -> None:
    metadata = {
        "source_ids": [SOURCE_ID],
        "selected_resources": list(selected_resources),
        "resource_hash_contract": resource_hash_contract,
        **(
            {"semantic_projection_as_of": semantic_projection_as_of}
            if semantic_projection_as_of is not None
            else {}
        ),
        **(
            {
                "proof_resource_scope": list(
                    importer._provider_directory_proof_resource_scope(
                        selected_resources
                    )
                )
            }
            if resource_hash_contract
            == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            else {}
        ),
    }
    await database.status(
        f"INSERT INTO \"{schema}\".provider_directory_endpoint_dataset ("
        "dataset_id, endpoint_id, acquisition_root_run_id, status, "
        "is_current, publication_metadata_json) VALUES ("
        ":dataset_id, :endpoint_id, :root_run_id, :status, false, "
        "CAST(:metadata_json AS jsonb));",
        dataset_id=dataset_id,
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        root_run_id=f"root-{dataset_id}"[:64],
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps(metadata),
    )


def _observation(
    full_name: str,
    *,
    page_number: int,
) -> dict[str, object]:
    family_name = full_name.split()[-1]
    given_name = full_name.split()[0]
    practitioner_payload_by_field = canonical_practitioner_payload(
        {
            "resource_id": RESOURCE_ID,
            "npi": 1000000000,
            "active": True,
            "names": [
                {
                    "use": "official",
                    "family": family_name,
                    "given": [given_name],
                    "text": full_name,
                }
            ],
            "fhir_meta": {
                "versionId": "1",
                "lastUpdated": (
                    f"2026-08-09T12:00:0{page_number}Z"
                ),
                "source": CANONICAL_BASE,
            },
            "resource_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_self_url": f"{CANONICAL_BASE}/Practitioner/{RESOURCE_ID}",
            "fhir_fetch_url": (
                f"{CANONICAL_BASE}/Practitioner?page={page_number}"
            ),
            "fhir_fetch_mode": "rest_bundle",
        }
    )
    observed_at = datetime.datetime(
        2026,
        8,
        9,
        12,
        0,
        page_number,
    )
    return {
        **practitioner_payload_by_field,
        "source_id": SOURCE_ID,
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


def _organization_observation(
    transport_host: str,
    *,
    page_number: int,
) -> dict[str, object]:
    resource_id = "organization-semantic-proof"
    observed_at = datetime.datetime(2026, 8, 9, 13, 0, page_number)
    return {
        "source_id": SOURCE_ID,
        "resource_id": resource_id,
        "name": "Example Organization",
        "active": True,
        "resource_url": f"https://{transport_host}/{resource_id}",
        "fhir_self_url": f"https://{transport_host}/self/{resource_id}",
        "fhir_fetch_url": f"https://{transport_host}/page/{page_number}",
        "fhir_fetch_mode": "rest_bundle",
        "fhir_meta": {
            "versionId": "1",
            "lastUpdated": f"2026-08-09T13:00:0{page_number}Z",
            "source": CANONICAL_BASE,
        },
        "last_seen_run_id": "run-semantic-proof",
        "observed_at": observed_at,
        "updated_at": observed_at,
    }


def _write_scope(dataset_id: str) -> importer.EndpointDatasetWriteScope:
    return importer.EndpointDatasetWriteScope(
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )


async def _write_page(dataset_id: str, row: dict[str, object]) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryPractitioner,
        [row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


async def _write_organization_page(
    dataset_id: str,
    row: dict[str, object],
) -> int:
    return await importer._upsert_resource_rows(
        ProviderDirectoryOrganization,
        [row],
        options=importer.ProviderDirectoryResourceWriteOptions(
            run_id="run-semantic-proof",
            track_seen=False,
            canonical_api_base=CANONICAL_BASE,
            source_ids=[SOURCE_ID],
            dataset_scope=_write_scope(dataset_id),
        ),
    )


def _dataset_row(
    dataset_id: str,
    row: dict[str, object],
) -> dict[str, object]:
    return importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [row],
        dataset_id=dataset_id,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )[0]


async def _write_partition_page(
    monkeypatch,
    dataset_id: str,
    resource_row_by_field: dict[str, object],
) -> None:
    dataset_row = _dataset_row(dataset_id, resource_row_by_field)
    stage = importer.LastUpdatedPartitionStage(
        rows=(dataset_row,),
        candidate_hashes_by_id={
            str(dataset_row["resource_id"]): str(
                dataset_row["payload_hash"]
            )
        },
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
    )
    state = SimpleNamespace(
        plan=SimpleNamespace(failure=None),
        context=object(),
        census=object(),
        pages_fetched=1,
        rows_fetched=1,
    )
    window = SimpleNamespace(passes={1: object()})
    monkeypatch.setattr(
        importer,
        "_store_partition_pass_proof",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_save_last_updated_partition_plan",
        AsyncMock(),
    )
    await importer._persist_partition_pass(
        "Practitioner",
        object(),
        state,
        window,
        2,
        stage,
    )


async def _stored_dataset_row(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[str, dict[str, object]]:
    record = await database.first(
        f"SELECT payload_hash, payload_json FROM \"{schema}\"."
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id "
        "AND resource_type='Practitioner' AND resource_id=:resource_id;",
        dataset_id=dataset_id,
        resource_id=RESOURCE_ID,
    )
    payload = record[1]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return str(record[0]), payload


async def _shard_records(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[list[str], list[list[object]]]:
    records = await database.all(
        f"SELECT input_sha256, payload_bytes FROM \"{schema}\"."
        f'"{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id ORDER BY shard_id;",
        dataset_id=dataset_id,
    )
    input_hashes: list[str] = []
    proof_records: list[list[object]] = []
    for input_hash, payload_bytes in records:
        input_hashes.append(str(input_hash))
        proof_records.extend(
            json.loads(line)
            for line in zlib.decompress(bytes(payload_bytes)).splitlines()
        )
    return sorted(input_hashes), proof_records


async def _proof(
    database: Database,
    schema: str,
    dataset_id: str,
):
    return await build_stored_dataset_proof(
        database,
        schema,
        dataset_id=dataset_id,
        endpoint_id=f"endpoint-{dataset_id}"[:64],
        acquisition_root_run_id=f"root-{dataset_id}"[:64],
        source_ids=[SOURCE_ID],
        selected_resources=["Practitioner"],
        options=ProviderDirectoryStoredProofOptions(
            proof_resource_scope=["Practitioner"],
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        ),
    )


async def _dataset_and_shard_counts(
    database: Database,
    schema: str,
    dataset_id: str,
) -> tuple[int, int]:
    counts = await database.first(
        f"SELECT (SELECT count(*) FROM \"{schema}\"."
        "provider_directory_dataset_resource WHERE dataset_id=:dataset_id), "
        f'(SELECT count(*) FROM "{schema}".'
        f'"{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}" '
        "WHERE dataset_id=:dataset_id);",
        dataset_id=dataset_id,
    )
    return int(counts[0]), int(counts[1])


async def _compatibility_counts(
    database: Database,
    schema: str,
) -> tuple[int, int, int]:
    counts = await database.first(
        f'SELECT (SELECT count(*) FROM "{schema}".'
        "provider_directory_canonical_resource), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_source_resource), "
        f'(SELECT count(*) FROM "{schema}".'
        "provider_directory_practitioner);"
    )
    return int(counts[0]), int(counts[1]), int(counts[2])
