# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native header types and publication/cleanup lock order for catalog retention."""

import asyncio
import importlib
import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from db.connection import Database
from tests.test_provider_directory_profile_selection_db import _profile_test_database

fhir = importlib.import_module("process.provider_directory_fhir")


@asynccontextmanager
async def _catalog_database(monkeypatch, metadata_type):
    database = await _profile_test_database()
    schema = "catalog_retention_test_" + uuid.uuid4().hex[:16]
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        await database.status(f"CREATE TABLE {schema}.provider_directory_source (source_id varchar(64) PRIMARY KEY)")
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_practitioner_role "
            "(source_id varchar(64) NOT NULL, resource_id varchar(64) NOT NULL, "
            "PRIMARY KEY(source_id, resource_id))"
        )
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_address_overlay "
            "(source_id varchar(64) NOT NULL, resource_id varchar(64) NOT NULL, "
            "PRIMARY KEY(source_id, resource_id))"
        )
        await database.status(
            f"CREATE TABLE {schema}.provider_directory_endpoint_dataset "
            "(dataset_id varchar(96) PRIMARY KEY, published_at timestamp, "
            f"publication_metadata_json {metadata_type}, publication_metadata_summary_json jsonb)"
        )
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(fhir, "_schema", lambda: schema)
        monkeypatch.setattr(
            fhir,
            "SOURCE_CATALOG_STALE_TABLE_MODELS",
            (SimpleNamespace(__tablename__="provider_directory_practitioner_role"),),
        )
        for source_id in ("source_current", "source_overlay", "source_zero_office", "source_old", "source_unused"):
            await database.status(f"INSERT INTO {schema}.provider_directory_source VALUES (:source)", source=source_id)
            await database.status(
                f"INSERT INTO {schema}.provider_directory_practitioner_role VALUES (:source,'role')", source=source_id
            )
        yield database, schema
    finally:
        try:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _header(database, schema, dataset, source_ids, *, published=True, compact=None):
    await database.status(
        f"INSERT INTO {schema}.provider_directory_endpoint_dataset "
        "(dataset_id,published_at,publication_metadata_json,publication_metadata_summary_json) "
        "VALUES (:dataset,CASE WHEN :published THEN CURRENT_TIMESTAMP ELSE NULL END,"
        "CAST(:metadata AS jsonb),CAST(:compact AS jsonb))",
        dataset=dataset,
        published=published,
        metadata=json.dumps({"source_ids": source_ids}),
        compact=json.dumps({"source_ids": compact}) if compact is not None else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_type", ["json", "jsonb"])
async def test_native_catalog_retains_overlay_and_all_published_header_aliases(monkeypatch, metadata_type):
    async with _catalog_database(monkeypatch, metadata_type) as (database, schema):
        await database.status(
            f"INSERT INTO {schema}.provider_directory_address_overlay VALUES ('source_overlay','office')"
        )
        await _header(database, schema, "retained-r1", ["source_zero_office"])
        await _header(database, schema, "current-r2", [], compact=["source_old"])
        await _header(database, schema, "failed-r3", ["source_unused"], published=False)
        deleted = await fhir._delete_source_catalog_rows_except(["source_current"])
        assert deleted == {"provider_directory_practitioner_role": 1, "provider_directory_source": 1}
        expected_source_ids = ["source_current", "source_old", "source_overlay", "source_zero_office"]
        for relation in ("provider_directory_source", "provider_directory_practitioner_role"):
            rows = await database.all(f"SELECT source_id FROM {schema}.{relation} ORDER BY source_id")
            assert [row._mapping["source_id"] for row in rows] == expected_source_ids


@pytest.mark.asyncio
@pytest.mark.parametrize("membership", [None, {}, [], [3], [""], [" "], [" source_old"], ["source_old "]])
async def test_native_catalog_unknown_published_membership_keeps_every_alias(monkeypatch, membership):
    async with _catalog_database(monkeypatch, "json") as (database, schema):
        await _header(database, schema, "legacy", membership)
        assert await fhir._delete_source_catalog_rows_except(["source_current"]) == {}
        assert await database.scalar(f"SELECT count(*) FROM {schema}.provider_directory_source") == 5
        assert await database.scalar(f"SELECT count(*) FROM {schema}.provider_directory_practitioner_role") == 5


async def _wait_for_alias_lock(publisher, publisher_pid, cleanup_pid):
    async with asyncio.timeout(5):
        while publisher_pid not in await publisher.scalar(
            "SELECT pg_blocking_pids(CAST(:pid AS integer))", pid=cleanup_pid
        ):
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_type", ["json", "jsonb"])
async def test_native_catalog_rechecks_membership_after_waiting_for_publication(monkeypatch, metadata_type):
    async with _catalog_database(monkeypatch, metadata_type) as (database, schema):
        publisher = Database()
        cleanup_task = None
        try:
            await publisher.connect()
            await _header(database, schema, "candidate-r1", ["source_old"], published=False)
            original_scalar = database.scalar
            cleanup_backend = asyncio.get_running_loop().create_future()

            async def tracked_scalar(statement, **parameters):
                result = await original_scalar(statement, **parameters)
                if statement == "SHOW transaction_isolation":
                    cleanup_backend.set_result(await original_scalar("SELECT pg_backend_pid()"))
                return result

            monkeypatch.setattr(database, "scalar", tracked_scalar)
            async with publisher.transaction():
                publisher_pid = await publisher.scalar("SELECT pg_backend_pid()")
                await publisher.all(
                    f"SELECT source_id FROM {schema}.provider_directory_source WHERE source_id='source_old' FOR SHARE"
                )
                await publisher.status(
                    f"UPDATE {schema}.provider_directory_endpoint_dataset "
                    "SET published_at=CURRENT_TIMESTAMP WHERE dataset_id='candidate-r1'"
                )
                cleanup_task = asyncio.create_task(fhir._delete_source_catalog_rows_except(["source_current"]))
                await _wait_for_alias_lock(publisher, publisher_pid, await asyncio.wait_for(cleanup_backend, 5))
                assert not cleanup_task.done()
            async with asyncio.timeout(5):
                await cleanup_task
            assert (
                await database.scalar(
                    f"SELECT count(*) FROM {schema}.provider_directory_source WHERE source_id='source_old'"
                )
                == 1
            )
            assert (
                await database.scalar(
                    f"SELECT count(*) FROM {schema}.provider_directory_practitioner_role WHERE source_id='source_old'"
                )
                == 1
            )
        finally:
            if cleanup_task is not None and not cleanup_task.done():
                cleanup_task.cancel()
                await asyncio.gather(cleanup_task, return_exceptions=True)
            await publisher.disconnect()
