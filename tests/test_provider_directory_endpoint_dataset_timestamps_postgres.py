# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual candidate timestamp writes; complete publication custody is tested separately."""

import importlib
import os
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateColumn

from db.connection import ConnectionProxy, Database
from db.models import ProviderDirectoryEndpointDataset
from tests.cms_npd_admission_postgres_support import _owned_database

importer = importlib.import_module("process.provider_directory_fhir")
pytestmark = pytest.mark.asyncio(loop_scope="module")
_SCHEMA = "candidate_timestamp_fixture"
_HEADER = _SCHEMA + ".provider_directory_endpoint_dataset"
_METADATA = {"source_ids": ["timestamp-source"]}


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def timestamp_connection():
    """Use the existing owned-database lifecycle with only two ordinary tables."""
    raw = os.getenv("HLTHPRT_CMS_LAYOUT_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_CMS_LAYOUT_TEST_DSN for the native candidate timestamp proof")
    database_url = make_url(raw)
    assert database_url.host in {"127.0.0.1", "localhost"} and database_url.port and not database_url.query
    async with _owned_database(database_url) as (owned, _admin):
        engine = create_async_engine(owned.set(drivername="postgresql+asyncpg"))
        try:
            async with engine.connect() as connection:
                async with connection.begin():
                    columns = ",".join(
                        str(
                            CreateColumn(Column(column.name, column.type, nullable=column.nullable)).compile(
                                dialect=dialect()
                            )
                        )
                        for column in ProviderDirectoryEndpointDataset.__table__.columns
                    )
                    await connection.exec_driver_sql(f"CREATE SCHEMA {_SCHEMA}")
                    await connection.exec_driver_sql(f"CREATE TABLE {_HEADER}({columns},PRIMARY KEY(dataset_id))")
                    await connection.exec_driver_sql(
                        f"CREATE TABLE {_SCHEMA}.provider_directory_source(source_id varchar(64) PRIMARY KEY)"
                    )
                    await connection.exec_driver_sql(
                        f"INSERT INTO {_SCHEMA}.provider_directory_source VALUES ('timestamp-source')"
                    )
                yield connection
        finally:
            await engine.dispose()


@pytest_asyncio.fixture(loop_scope="module")
async def candidate_store(timestamp_connection, monkeypatch):
    """Keep each production write on one real transaction and roll it back afterward."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    transaction = await timestamp_connection.begin()
    try:
        yield ConnectionProxy(Database(), timestamp_connection, None)
    finally:
        await transaction.rollback()


def _candidate():
    return importer.EndpointDatasetCandidate(
        endpoint_id="timestamp-endpoint",
        dataset_id="timestamp-candidate",
        acquisition_root_run_id="timestamp-root",
        source_ids=("timestamp-source",),
        selected_resources=("Organization",),
        expected_resources=("Organization",),
        import_run_id="timestamp-run",
        previous_dataset_id=None,
        resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
    )


async def _validate_candidate(store, candidate, status=importer.ENDPOINT_DATASET_VALIDATED):
    return await importer._store_validated_endpoint_dataset(
        store, candidate, None, "d" * 64, 2, _METADATA, status=status
    )


async def _header(store):
    native_header = await store.first(f"SELECT * FROM {_HEADER}")
    assert native_header is not None
    return dict(native_header._mapping)


@pytest.mark.parametrize("time_zone", ["UTC", "Pacific/Kiritimati", "America/Los_Angeles"])
async def test_production_creation_and_validation_store_utc(candidate_store, time_zone):
    """Compare both actual SQL writes to the server instant across positive and negative offsets."""
    store, candidate = candidate_store, _candidate()
    await store.scalar("SELECT set_config('TimeZone',:zone,true)", zone=time_zone)
    instant = await store.scalar("SELECT transaction_timestamp()")
    expected = instant.astimezone(timezone.utc).replace(tzinfo=None)
    await importer._upsert_endpoint_dataset_candidate(store, candidate, _METADATA)
    acquiring = await _header(store)
    assert acquiring["created_at"] == expected
    assert acquiring["validated_at"] is None
    await _validate_candidate(store, candidate)
    validated = await _header(store)
    assert validated["created_at"] == validated["validated_at"] == expected
    publication_instant = await store.scalar("SELECT statement_timestamp()")
    assert validated["validated_at"] <= publication_instant.astimezone(timezone.utc).replace(tzinfo=None)
    assert validated["status"] == importer.ENDPOINT_DATASET_VALIDATED
    assert validated["is_current"] is False
    assert validated["published_at"] is None and validated["superseded_at"] is None
    assert validated["dataset_hash"] == "d" * 64 and validated["resource_count"] == 2
    assert await store.scalar("SHOW TimeZone") == time_zone


async def test_existing_created_at_and_terminal_replay_remain_immutable(candidate_store):
    """Keep a pre-existing creation value and reject any second terminal validation write."""
    store, candidate = candidate_store, _candidate()
    await store.scalar("SELECT set_config('TimeZone','Pacific/Kiritimati',true)")
    await importer._upsert_endpoint_dataset_candidate(store, candidate, _METADATA)
    previous_created_at = datetime(2000, 1, 2, 3, 4, 5)
    await store.status(f"UPDATE {_HEADER} SET created_at=:created_at", created_at=previous_created_at)
    await importer._upsert_endpoint_dataset_candidate(store, candidate, _METADATA)
    assert (await _header(store))["created_at"] == previous_created_at
    await _validate_candidate(store, candidate)
    before_by_field = await _header(store)
    await store.scalar("SELECT set_config('TimeZone','America/Los_Angeles',true)")
    await importer._upsert_endpoint_dataset_candidate(store, candidate, {"source_ids": ["changed-source"]})
    assert await _header(store) == before_by_field
    with pytest.raises(RuntimeError, match="endpoint_dataset_validation_lost"):
        await _validate_candidate(store, candidate)
    assert await _header(store) == before_by_field


@pytest.mark.parametrize(
    "status", [importer.ENDPOINT_DATASET_VERIFICATION_BASELINE, importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH]
)
async def test_verification_terminal_status_keeps_null_validation_time(candidate_store, status):
    store, candidate = candidate_store, _candidate()
    await store.scalar("SELECT set_config('TimeZone','Pacific/Kiritimati',true)")
    await importer._upsert_endpoint_dataset_candidate(store, candidate, _METADATA)
    await _validate_candidate(store, candidate, status)
    terminal = await _header(store)
    assert terminal["status"] == status and terminal["is_current"] is False
    assert terminal["validated_at"] is None and terminal["published_at"] is None
