# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL support for exact current-version census tests."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib
import json
import uuid
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ensure_dataset_proof_shard_table,
)


importer = importlib.import_module("process.provider_directory_fhir")

BASE_URL = "https://directory.example.test/fhir"
SOURCE_ID = "synthetic-current-census"
DATASET_ID = "dataset-current-census"
ENDPOINT_ID = "endpoint-current-census"
ROOT_RUN_ID = "run-current-census-root"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Practitioner"
NEXT_URL = (
    f"{BASE_URL}?_pretty=true&_getpages=opaque-token"
    "&_getpagesoffset=1&_count=1&_bundletype=searchset"
)

FetchResponse = tuple[int | None, dict[str, Any] | None, str | None, int]
FetchCallback = Callable[..., Awaitable[FetchResponse]]


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(
            await database.scalar("SELECT current_database();") or ""
        )
    except (OSError, OperationalError):
        pytest.skip("current-version census tests need disposable PostgreSQL")
    if "test" not in database_name.lower():
        pytest.skip("current-version census tests need a test database")


def census_contract() -> CurrentVersionCensusContract:
    return CurrentVersionCensusContract(
        source_id=SOURCE_ID,
        cutoff=CUTOFF,
        resources=(RESOURCE_TYPE,),
        expected_nonempty_resources=(RESOURCE_TYPE,),
        start_urls=(
            (
                RESOURCE_TYPE,
                f"{BASE_URL}/{RESOURCE_TYPE}?active=true",
            ),
        ),
        continuation_strategy=(
            CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
        ),
    )


def census_source_record(
    contract: CurrentVersionCensusContract,
) -> dict[str, Any]:
    return {
        "source_id": SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "api_base": BASE_URL,
        "canonical_api_base": BASE_URL,
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": True,
            "provider_directory_supported_resources": [RESOURCE_TYPE],
            "provider_directory_fully_enumerable_resources": [
                RESOURCE_TYPE
            ],
        },
        CURRENT_VERSION_CENSUS_CONTRACT_FIELD: contract,
    }


def count_bundle() -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 2,
    }


def practitioner_bundle(
    resource_id: str,
    *,
    next_url: str | None = None,
) -> dict[str, Any]:
    return {
        "resourceType": "Bundle",
        "type": "searchset",
        "link": (
            []
            if next_url is None
            else [{"relation": "next", "url": next_url}]
        ),
        "entry": [
            {
                "fullUrl": f"{BASE_URL}/{RESOURCE_TYPE}/{resource_id}",
                "resource": {
                    "resourceType": RESOURCE_TYPE,
                    "id": resource_id,
                    "active": True,
                },
            }
        ],
    }


async def _create_endpoint_tables(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_api_endpoint (
            endpoint_id varchar(64) PRIMARY KEY,
            canonical_api_base text NOT NULL,
            credential_descriptor_hash varchar(64) NOT NULL,
            endpoint_signature_hash varchar(64) NOT NULL
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_endpoint_dataset (
            dataset_id varchar(96) PRIMARY KEY,
            endpoint_id varchar(64) NOT NULL REFERENCES
                "{schema}".provider_directory_api_endpoint(endpoint_id),
            import_run_id varchar(64),
            acquisition_root_run_id varchar(64),
            previous_dataset_id varchar(96),
            dataset_hash varchar(64),
            status varchar(32) NOT NULL,
            is_current boolean NOT NULL DEFAULT false,
            resource_count bigint NOT NULL DEFAULT 0,
            created_at timestamp DEFAULT now(),
            validated_at timestamp,
            published_at timestamp,
            superseded_at timestamp,
            publication_metadata_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            publication_metadata_summary_json jsonb,
            publication_metadata_sha256 varchar(64),
            content_proof_admission_version smallint,
            content_proof_admission_kind varchar(32),
            content_proof_admission_sha256 varchar(64),
            content_proof_resource_types varchar(64)[],
            completion_proof_required_version integer,
            completion_proof_json jsonb,
            completion_proof_sha256 varchar(64)
        );
        """
    )
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_dataset_resource (
            dataset_id varchar(96) NOT NULL REFERENCES
                "{schema}".provider_directory_endpoint_dataset(dataset_id),
            resource_type varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            payload_hash varchar(64) NOT NULL,
            payload_json jsonb NOT NULL,
            acquired_resource_sha256 varchar(64),
            PRIMARY KEY (dataset_id, resource_type, resource_id)
        );
        """
    )


async def _create_checkpoint_table(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_pagination_checkpoint (
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_scope_hash varchar(64) NOT NULL,
            dataset_id varchar(96) REFERENCES
                "{schema}".provider_directory_endpoint_dataset(dataset_id),
            source_ids jsonb NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            owner_run_id varchar(64) NOT NULL,
            retry_of_run_id varchar(64),
            start_url_hash varchar(64) NOT NULL,
            next_url text,
            state varchar(32) NOT NULL,
            pages_processed bigint NOT NULL DEFAULT 0,
            rows_processed bigint NOT NULL DEFAULT 0,
            recent_cursor_hashes jsonb NOT NULL DEFAULT '[]'::jsonb,
            completeness_json jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            created_at timestamp NOT NULL DEFAULT now(),
            updated_at timestamp NOT NULL DEFAULT now(),
            completed_at timestamp,
            PRIMARY KEY (
                canonical_api_base,
                resource_type,
                source_scope_hash,
                acquisition_root_run_id
            )
        );
        """
    )


async def _create_bulk_checkpoint_table(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        CREATE TABLE "{schema}".provider_directory_bulk_acquisition_checkpoint (
            checkpoint_id varchar(64) PRIMARY KEY,
            canonical_api_base text NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_scope_hash varchar(64) NOT NULL,
            strategy_version varchar(64) NOT NULL,
            acquisition_root_run_id varchar(64) NOT NULL,
            owner_run_id varchar(64) NOT NULL,
            retry_of_run_id varchar(64),
            endpoint_id varchar(64) NOT NULL,
            dataset_id varchar(96) NOT NULL,
            manifest_json jsonb,
            state varchar(32) NOT NULL,
            updated_at timestamp NOT NULL DEFAULT now()
        );
        """
    )


async def _seed_endpoint_dataset(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_api_endpoint (
            endpoint_id, canonical_api_base,
            credential_descriptor_hash, endpoint_signature_hash
        ) VALUES (
            :endpoint_id, :canonical_api_base,
            :credential_hash, :signature_hash
        );
        """,
        endpoint_id=ENDPOINT_ID,
        canonical_api_base=BASE_URL,
        credential_hash="0" * 64,
        signature_hash="1" * 64,
    )


async def _seed_direct_fetch_dataset(
    database: Database,
    schema: str,
) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            status, is_current, publication_metadata_json
        ) VALUES (
            :dataset_id, :endpoint_id, :root_run_id, :root_run_id,
            :status, false, CAST(:metadata_json AS jsonb)
        );
        """,
        dataset_id=DATASET_ID,
        endpoint_id=ENDPOINT_ID,
        root_run_id=ROOT_RUN_ID,
        status=importer.ENDPOINT_DATASET_ACQUIRING,
        metadata_json=json.dumps(
            {
                "source_ids": [SOURCE_ID],
                "selected_resources": [RESOURCE_TYPE],
                "resource_hash_contract": (
                    importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
                ),
            }
        ),
    )


async def _create_census_tables(
    database: Database,
    schema: str,
    *,
    seed_dataset: bool,
) -> None:
    await _create_endpoint_tables(database, schema)
    await _create_checkpoint_table(database, schema)
    await _create_bulk_checkpoint_table(database, schema)
    await _seed_endpoint_dataset(database, schema)
    if seed_dataset:
        await _seed_direct_fetch_dataset(database, schema)
    await ensure_dataset_proof_shard_table(database, schema)


@asynccontextmanager
async def census_database(
    monkeypatch: pytest.MonkeyPatch,
    *,
    seed_dataset: bool = True,
) -> AsyncIterator[tuple[Database, str]]:
    schema = f"provider_current_census_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    dataset_table = importer.ProviderDirectoryDatasetResource.__table__
    original_model_schema = dataset_table.schema
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        await database.status(f'CREATE SCHEMA "{schema}";')
        is_schema_created = True
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setattr(dataset_table, "schema", schema)
        monkeypatch.setattr(importer, "db", database)
        await _create_census_tables(
            database,
            schema,
            seed_dataset=seed_dataset,
        )
        yield database, schema
    finally:
        dataset_table.schema = original_model_schema
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()


def checkpoint_context(
    source_record: dict[str, Any],
    *,
    owner_run_id: str,
    retry_of_run_id: str | None,
) -> importer.PaginationCheckpointContext:
    scope_identity = importer._pagination_checkpoint_scope_identity(
        source_record,
        [SOURCE_ID],
    )
    assert scope_identity is not None
    canonical_api_base, source_scope_hash = scope_identity
    return importer.PaginationCheckpointContext(
        canonical_api_base=canonical_api_base,
        source_scope_hash=source_scope_hash,
        source_ids=(SOURCE_ID,),
        owner_run_id=owner_run_id,
        retry_of_run_id=retry_of_run_id,
        acquisition_root_run_id=ROOT_RUN_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        lineage_verified=True,
    )


def fetch_sequence(
    responses: list[FetchResponse],
    requested_urls: list[str],
) -> FetchCallback:
    pending_responses = list(responses)

    async def fetch(
        _source_record: dict[str, Any],
        request_url: str,
        *,
        timeout: int,
    ) -> FetchResponse:
        del timeout
        requested_urls.append(request_url)
        if not pending_responses:
            raise AssertionError(
                f"unexpected current-version census request: {request_url}"
            )
        return pending_responses.pop(0)

    return fetch


async def fetch_practitioners(
    source_record: dict[str, Any],
    context: importer.PaginationCheckpointContext,
) -> importer.ResourceFetchResult:
    async def persist_page(
        model: type,
        resource_rows: list[dict[str, Any]],
    ) -> int:
        dataset_rows = await importer._persist_endpoint_dataset_rows(
            model,
            resource_rows,
            DATASET_ID,
            resource_hash_contract=(
                importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            ),
            semantic_projection_as_of=None,
        )
        return len(dataset_rows)

    fetch_result = await importer._fetch_resource_rows(
        source_record,
        RESOURCE_TYPE,
        per_resource_limit=0,
        page_limit=0,
        page_count=1,
        timeout=3,
        run_id=context.owner_run_id,
        row_batch_handler=persist_page,
        row_batch_size=1,
        retain_rows=False,
        pagination_checkpoint=context,
    )
    assert fetch_result is not None
    return fetch_result


async def checkpoint_record(
    database: Database,
    schema: str,
) -> dict[str, Any]:
    checkpoint_row = await database.first(
        f'SELECT * FROM "{schema}".'
        "provider_directory_pagination_checkpoint;"
    )
    return importer._pagination_checkpoint_row_mapping(checkpoint_row)


async def endpoint_dataset_record(
    database: Database,
    schema: str,
) -> dict[str, Any]:
    dataset_row = await database.first(
        f"""
        SELECT dataset_id, endpoint_id, import_run_id,
               acquisition_root_run_id, status, is_current,
               resource_count, dataset_hash, validated_at, published_at,
               publication_metadata_json
          FROM "{schema}".provider_directory_endpoint_dataset;
        """
    )
    return importer._pagination_checkpoint_row_mapping(dataset_row)


async def candidate_resource_ids(
    database: Database,
    schema: str,
    dataset_id: str = DATASET_ID,
) -> list[str]:
    rows = await database.all(
        f"""
        SELECT resource_id
          FROM "{schema}".provider_directory_dataset_resource
         WHERE dataset_id=:dataset_id
           AND resource_type=:resource_type
         ORDER BY resource_id;
        """,
        dataset_id=dataset_id,
        resource_type=RESOURCE_TYPE,
    )
    return [row[0] for row in rows]


async def proof_shard_counts(
    database: Database,
    schema: str,
    dataset_id: str = DATASET_ID,
) -> tuple[int, int]:
    row = await database.first(
        f"""
        SELECT count(*), COALESCE(sum(resource_count), 0)
          FROM "{schema}"."{PROVIDER_DIRECTORY_PROOF_SHARD_TABLE}"
         WHERE dataset_id=:dataset_id;
        """,
        dataset_id=dataset_id,
    )
    assert row is not None
    return int(row[0]), int(row[1])
