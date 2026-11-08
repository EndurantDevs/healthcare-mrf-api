# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL support for exact current-version census tests."""

from __future__ import annotations

import importlib
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process import provider_directory_generic_source_policy as source
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_CONTRACT_FIELD,
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS
from process.provider_directory_generic_source_completion import _active_rows
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_PROOF_SHARD_TABLE,
    ensure_dataset_proof_shard_table,
)
from process.provider_directory_source_checkpoint_catalog import capture_checkpoint_controls
from tests.cms_npd_admission_postgres_support import _owned_database
from tests.provider_directory_source_postgres_support import (
    bootstrap_source_component,
    native_source_component,
    provision_source_component,
)

importer = importlib.import_module("process.provider_directory_fhir")

BASE_URL = "https://directory.example.test/fhir"
SOURCE_ID = "synthetic-current-census"
DATASET_ID = "dataset-current-census"
ENDPOINT_ID = "endpoint-current-census"
ROOT_RUN_ID = "run-current-census-root"
CUTOFF = "2026-08-01T12:00:00.000000Z"
RESOURCE_TYPE = "Practitioner"
NEXT_URL = f"{BASE_URL}?_pretty=true&_getpages=opaque-token&_getpagesoffset=1&_count=1&_bundletype=searchset"

FetchResponse = tuple[int | None, dict[str, Any] | None, str | None, int]
FetchCallback = Callable[..., Awaitable[FetchResponse]]


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except OSError, OperationalError:
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
        continuation_strategy=(CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY),
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
            "provider_directory_fully_enumerable_resources": [RESOURCE_TYPE],
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
        "link": ([] if next_url is None else [{"relation": "next", "url": next_url}]),
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


async def _seed_direct_fetch_dataset(database: Database, schema: str) -> None:
    """Prepare the same private intake binding used by the real importer."""
    assert database is importer.db and schema == importer._schema()
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id=ENDPOINT_ID,
        dataset_id=DATASET_ID,
        acquisition_root_run_id=ROOT_RUN_ID,
        source_ids=(SOURCE_ID,),
        selected_resources=(RESOURCE_TYPE,),
        import_run_id=ROOT_RUN_ID,
        previous_dataset_id=None,
        resource_hash_contract=importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    await importer._ensure_endpoint_dataset_candidate(candidate)


@asynccontextmanager
async def census_database(
    monkeypatch: pytest.MonkeyPatch,
    *,
    seed_dataset: bool = True,
) -> AsyncIterator[tuple[Database, str]]:
    """Own a complete current Source corpus, actors and candidate storage."""
    schema = f"provider_current_census_{uuid.uuid4().hex[:12]}"
    administrator = Database()
    try:
        await administrator.connect()
        await _require_disposable_postgres(administrator)
        async with _owned_database(administrator.engine.url) as (database_url, _admin):
            engine = create_async_engine(database_url)
            try:
                async with engine.begin() as connection:
                    await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
                async with _prepared_census_component(
                    engine, schema, monkeypatch, database_url, seed_dataset=seed_dataset
                ) as database:
                    yield database, schema
            finally:
                await engine.dispose()
    finally:
        await administrator.disconnect()


@asynccontextmanager
async def _prepared_census_component(engine, schema, monkeypatch, database_url, *, seed_dataset):
    """Provision genuine Source custody and restore scoped settings on every exit."""
    with monkeypatch.context() as settings:
        for key, setting_value in {
            "HOST": database_url.host,
            "PORT": database_url.port or 5432,
            "DATABASE": database_url.database,
            "DATABASE_OVERRIDE": database_url.database,
            "USER": database_url.username,
            "PASSWORD": database_url.password or "",
            "SCHEMA": schema,
        }.items():
            settings.setenv("HLTHPRT_DB_" + key, str(setting_value))
        settings.setenv("DB_SCHEMA", schema)
        async with native_source_component(engine, schema, settings, legacy_builder=True) as component:
            await bootstrap_source_component(engine, component.policy)
            await provision_source_component(engine, component.policy)
            database = component.database
            settings.setattr(importer, "db", database)
            settings.setattr(importer.ProviderDirectoryDatasetResource.__table__, "schema", schema)
            async with database.transaction():
                publisher = source.bound_publisher()
                await capture_checkpoint_controls(publisher.connection, publisher.policy)
                await ensure_dataset_proof_shard_table(database, schema)
                await _seed_endpoint_dataset(database, schema)
            if seed_dataset:
                await _seed_direct_fetch_dataset(database, schema)
            yield database


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
            raise AssertionError(f"unexpected current-version census request: {request_url}")
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
            resource_hash_contract=(importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT),
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
    checkpoint_row = await database.first(f'SELECT * FROM "{schema}".provider_directory_pagination_checkpoint;')
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
    """Observe real pending/reset input or its exact finished resource heap."""
    async with source.source_transaction(database):
        publisher = source.bound_publisher()
        assert publisher.policy.schema == schema
        header = await publisher.connection.fetchrow(
            f'SELECT candidate_storage_json FROM "{schema}".provider_directory_endpoint_dataset WHERE dataset_id=$1',
            dataset_id,
        )
        binding = storage._json(header["candidate_storage_json"])
        identity = binding["identity"]
        assert identity["dataset_id"] == dataset_id
        phase = binding["phase"]
        assert phase in {"loading", "prepared", "published"}
        binding = await storage._locked(publisher.connection, publisher.policy, identity, phases=(phase,))
        await storage._verify_tables(publisher.connection, publisher.policy, binding)
        if binding["phase"] == "loading":
            resource_rows = _active_rows(
                publisher.connection, publisher.policy, binding, "resource_input", RESOURCE_COLUMNS
            )
            return sorted(
                {
                    resource_row["resource_id"]
                    async for resource_row in resource_rows
                    if resource_row["resource_type"] == RESOURCE_TYPE
                }
            )
        table = binding["tables"][storage.RESOURCE]["name"]
        resource_rows = await publisher.connection.fetch(
            f'SELECT resource_id FROM "{schema}"."{table}" WHERE dataset_id=$1 AND resource_type=$2 ORDER BY resource_id',
            dataset_id,
            RESOURCE_TYPE,
        )
        return [resource_row["resource_id"] for resource_row in resource_rows]


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
