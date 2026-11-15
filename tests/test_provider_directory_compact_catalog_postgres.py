# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog exports preserve their bounds and semantics over compact editions."""

import json

import pytest

from process import provider_directory_compact_edition as edition
from process import provider_directory_compact_payload as compact
from process import registry_fhir_catalog_extraction as catalog
from tests.test_provider_directory_compact_edition_postgres import _published_tables
from tests.test_provider_directory_compact_payload import resource
from tests.test_provider_directory_compact_payload_postgres import append, storage


async def _catalog_relations(connection, loading, rows):
    await append(connection, loading, rows, 1)
    tables = await _published_tables(connection)
    await connection.execute(
        "CREATE VIEW compact_test.provider_directory_dataset_resource AS " + compact.compact_resource_sql(tables)
    )
    await connection.execute("CREATE SCHEMA plain_test")
    await connection.execute(
        "CREATE TABLE plain_test.provider_directory_dataset_resource("
        "dataset_id text,resource_type text,resource_id text,payload_hash text,payload_json json)"
    )
    await connection.copy_records_to_table(
        "provider_directory_dataset_resource",
        schema_name="plain_test",
        records=[
            (
                row["dataset_id"],
                row["resource_type"],
                row["resource_id"],
                row["payload_hash"],
                json.dumps(row["payload_json"]),
            )
            for row in rows
        ],
    )


async def _pages(connection, schema, **bounds):
    return [
        page
        async for page in catalog.iter_fhir_catalog_pages(
            connection, schema_name=schema, dataset_id="dataset", **bounds
        )
    ]


@pytest.mark.asyncio
async def test_catalog_exports_match_plain_rows_and_resume_by_original_key():
    rows = [
        resource(
            str(index),
            healthcare_service_refs=["HealthcareService/" + "a" * 32] * 4,
            npi=None,
            values=[-0.0, 1e20, 1e-20],
        )
        for index in range(7)
    ]
    rows[0]["resource_type"] = "Organization"
    async with storage() as (connection, loading):
        async with connection.transaction():
            await _catalog_relations(connection, loading, rows)
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            compact_pages = await _pages(connection, "compact_test", page_rows=2, page_bytes=2048)
            plain_pages = await _pages(connection, "plain_test", page_rows=2, page_bytes=2048)
            compact_rows = [row for page in compact_pages for row in page]
            assert compact_rows == [row for page in plain_pages for row in page]
            assert len(compact_rows) == len(rows)
            assert all(len(page) <= 2 and len(catalog.fhir_catalog_json_bytes(page)) <= 2048 for page in compact_pages)
            cursor_values = tuple(compact_rows[2][field] for field in ("resource_type", "resource_id"))
            resumed = await _pages(
                connection, "compact_test", after_resource=cursor_values, page_rows=2, page_bytes=2048
            )
            assert [row for page in resumed for row in page] == compact_rows[3:]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_format", [None, "unsupported-format"])
async def test_catalog_exports_refuse_missing_or_unknown_compact_format(changed_format):
    async with storage() as (connection, loading):
        async with connection.transaction():
            await _catalog_relations(connection, loading, [resource()])
            if changed_format is None:
                await connection.execute(f"DROP TABLE compact_test.{edition.FORMAT_TABLE}")
            else:
                await connection.execute(f"UPDATE compact_test.{edition.FORMAT_TABLE} SET format_id=$1", changed_format)
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="format_(missing|changed)"):
                await _pages(connection, "compact_test")


@pytest.mark.asyncio
async def test_catalog_exports_refuse_an_oversized_first_row_without_false_eof():
    async with storage() as (connection, loading):
        async with connection.transaction():
            await _catalog_relations(connection, loading, [resource(text="x" * 4096)])
        for schema in ("plain_test", "compact_test"):
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                with pytest.raises(ValueError, match="page_bounds|row_too_large"):
                    await _pages(connection, schema, page_bytes=1024)


@pytest.mark.asyncio
async def test_catalog_exports_refuse_missing_payload_without_false_eof():
    async with storage() as (connection, loading):
        async with connection.transaction():
            await _catalog_relations(connection, loading, [resource()])
            await connection.execute(f"DELETE FROM compact_test.{edition.BODY_BLOCKS}")
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="read_rows_missing"):
                await _pages(connection, "compact_test")
