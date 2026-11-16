# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Connected native overlay parity and failed-candidate fixtures."""

import importlib
from contextlib import asynccontextmanager

import pytest

from process.ext import address_batch
from tests.test_address_format_db import _load_migration
from tests.test_provider_directory_practitioner_address_overlay_db import (
    _create_fixture_tables,
    _insert_practitioner_rows,
    _install_canonical_functions,
    _temporary_schema,
)

directory = importlib.import_module("process.provider_directory_fhir")
batch = importlib.import_module("process.provider_directory_address_batch")


@asynccontextmanager
async def overlay_fixture(monkeypatch):
    if not address_batch.is_native_available():
        pytest.skip("native address batch extension is required")
    async with _temporary_schema(monkeypatch) as (database, schema):
        await _install_canonical_functions(database, schema)
        display = _load_migration()
        await database.status(display._humanize_component_function_sql(schema))
        await database.status(display._formatted_address_function_sql(schema))
        await _create_fixture_tables(database, schema, "sql_labels")
        await database.status(directory.provider_directory_address_overlay_table_sql(schema, "native_labels"))
        await _insert_practitioner_rows(database, schema)
        await database.status(
            f'UPDATE "{schema}".provider_directory_practitioner SET addresses=CAST(:addresses AS jsonb) '
            "WHERE source_id='source-target' AND resource_id='practitioner-old-run'",
            addresses='[{"line":["200 Caf\u00e9 Road"],"city":"Austin","state":"TX","postalCode":"78702"}]',
        )
        monkeypatch.setattr(directory, "db", database)
        yield database, schema


async def insert_practitioner_labels(database, schema, table, run_id=None):
    return await database.status(
        directory._address_overlay_component_insert_sql(
            schema, table, component="practitioner_address", source_ids=["source-target"], run_id=run_id
        ),
        source_ids=["source-target"],
        **({"run_id": run_id} if run_id is not None else {}),
    )


async def label_rows(database, schema, table):
    columns = ",".join(
        directory._q(column)
        for column in directory._provider_directory_address_overlay_columns()
        if column != "published_at"
    )
    return list(map(tuple, await database.all(f'SELECT {columns} FROM "{schema}"."{table}" ORDER BY source_record_id')))


async def remaining_batch_tables(database, schema):
    return await database.scalar(
        "SELECT count(*) FROM pg_tables WHERE schemaname=:schema AND tablename LIKE :pattern",
        schema=schema,
        pattern="provider_directory_address_overlay_stage_%",
    )


@asynccontextmanager
async def location_overlay_fixture(monkeypatch):
    from tests.test_provider_directory_address_overlay_keys_db import _seed_sql_keyed_stage

    if not address_batch.is_native_available():
        pytest.skip("native address batch extension is required")
    async with _temporary_schema(monkeypatch) as (database, schema):
        components = ("practitioner_role", "organization_affiliation")
        await _seed_sql_keyed_stage(database, schema, components, {"source_ids": ["cms-npd"]})
        display = _load_migration()
        await database.status(display._humanize_component_function_sql(schema))
        await database.status(display._formatted_address_function_sql(schema))
        monkeypatch.setattr(directory, "db", database)
        yield database, schema, components
