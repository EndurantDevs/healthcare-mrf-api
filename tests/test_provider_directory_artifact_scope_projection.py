# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json

import pytest

from tests.test_provider_directory_practitioner_address_overlay_db import _temporary_schema

importer = importlib.import_module("process.provider_directory_fhir")
scope_projection = importlib.import_module("process.provider_directory_artifact_scope_projection")

PAYLOADS = (
    {"names": [{"family": "Stone"}], "full_name": "Ada Stone", "active": True, "npi": 1234567890},
    {"names": None, "full_name": None, "active": None},
    {"full_name": {"nested": ["kept", 1]}, "telecom": "not-a-list", "npi": "1234567891"},
    {},
)


@pytest.mark.parametrize(
    "select_sql",
    [importer._artifact_resource_batch_select_sql, importer._provider_directory_artifact_resource_select_sql],
)
def test_scope_projection_parses_each_payload_once(select_sql):
    """Every typed column must read one parsed payload, not re-parse the JSON text."""

    projection_sql = select_sql(importer.ProviderDirectoryLocation, "mrf")

    assert projection_sql.count("payload_json::jsonb") == 1
    assert projection_sql.count("parsed.payload_jsonb ->") == len(
        [
            column
            for column in importer.ProviderDirectoryLocation.__table__.columns
            if column.name not in {"source_id", "resource_id", "last_seen_run_id", "observed_at", "updated_at"}
        ]
    )
    assert scope_projection.PARSED_PAYLOAD_JOIN in projection_sql


@pytest.mark.asyncio
async def test_parsed_payload_columns_equal_per_column_casts(monkeypatch):
    """Null, missing, scalar and nested values must project exactly as before."""

    columns = [
        column
        for column in importer.ProviderDirectoryPractitioner.__table__.columns
        if column.name not in {"source_id", "resource_id", "last_seen_run_id", "observed_at", "updated_at"}
    ]
    per_column_sql = ", ".join(
        f"{importer._provider_directory_artifact_payload_column_sql(column)} AS {importer._q(column.name)}"
        for column in columns
    )
    parsed_sql = ", ".join(
        f"{importer._provider_directory_artifact_payload_column_sql(column, 'parsed.payload_jsonb')} "
        f"AS {importer._q(column.name)}"
        for column in columns
    )
    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await database.status(f'CREATE TABLE "{schema_name}"."resource" (resource_id varchar, payload_json json)')
        for position, practitioner_payload in enumerate(PAYLOADS):
            await database.status(
                f'INSERT INTO "{schema_name}"."resource" VALUES (:resource_id, CAST(:payload AS json))',
                resource_id=str(position),
                payload=json.dumps(practitioner_payload),
            )
        relation = f'"{schema_name}"."resource" AS resource'
        differing_rows = await database.scalar(
            f"""
            WITH per_column AS (SELECT resource.resource_id, {per_column_sql} FROM {relation}),
                 parsed_once AS (
                    SELECT resource.resource_id, {parsed_sql}
                      FROM {relation} {scope_projection.PARSED_PAYLOAD_JOIN}
                 )
            SELECT count(*)
              FROM per_column
              FULL JOIN parsed_once USING (resource_id)
             WHERE to_jsonb(per_column)::text IS DISTINCT FROM to_jsonb(parsed_once)::text;
            """
        )
        assert differing_rows == 0
        assert await database.scalar(f'SELECT count(*) FROM "{schema_name}"."resource"') == len(PAYLOADS)
