# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact task-owned native catalogue schemas and minimal publication data."""

import json
import os
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from db.registry_fhir_catalog import table_sql

GENERATION = "a" * 64
SOURCE_GENERATION = "b" * 64
MANIFEST = "c" * 64
BINDING_ID = UUID(int=1)


async def _tables(connection, control, catalog):
    await connection.execute(f"""CREATE SCHEMA {control}; CREATE SCHEMA {catalog};
      CREATE TABLE {control}.registry_approved_record(approved_revision bigint,record_kind text,record_key text,
        record_revision bigint,custom_revision bigint,record_json jsonb,PRIMARY KEY(approved_revision,record_kind,record_key));
      CREATE TABLE {control}.registry_fhir_catalog_snapshot(metadata_generation text PRIMARY KEY,schema_name text,
        metadata_approved_revision bigint,published_at timestamptz,network_generation bigint,manifest_sha256 text);
      CREATE TABLE {control}.network_serving_manifest(generation_id bigint PRIMARY KEY,approved_custom_revision bigint,
        source_generations jsonb,manifest_sha256 text);
      CREATE TABLE {control}.network_serving_control(id integer PRIMARY KEY,generation_id bigint,metadata_generation text);
      CREATE TABLE {control}.network_membership(network_id integer,provider_system text,provider_id text,location_id uuid);
      CREATE TABLE {control}.provider_location_binding(provider_system text,provider_id text,location_id uuid,
        location_key text,entity_type text,entity_id text);
      CREATE TABLE {control}.entity_address_unified(location_key text,entity_type text,entity_id text)""")
    for name in ("source", "network", "binding", "relationship"):
        await connection.execute(table_sql(catalog, "registry_fhir_catalog_" + name)[0])


async def _seed(connection, control, catalog):
    await connection.execute(
        f"INSERT INTO {control}.registry_fhir_catalog_snapshot VALUES($1,$2,0,clock_timestamp(),9,$3);",
        GENERATION,
        catalog[1:-1],
        MANIFEST,
    )
    await connection.execute(f"INSERT INTO {control}.network_serving_control VALUES(1,9,$1)", GENERATION)
    await connection.execute(
        f"INSERT INTO {control}.network_serving_manifest VALUES(9,0,$1::jsonb,$2)",
        json.dumps({"fhir_catalog": GENERATION}),
        MANIFEST,
    )
    await connection.execute(
        f"INSERT INTO {catalog}.registry_fhir_catalog_network VALUES($1,7,'Imported network','[]',false,1,ARRAY['medical'],'imported network')",
        GENERATION,
    )
    await connection.execute(
        f"INSERT INTO {catalog}.registry_fhir_catalog_source VALUES($1,'source-one',$2::jsonb,'published_normalized',1,1,clock_timestamp(),clock_timestamp())",
        SOURCE_GENERATION,
        json.dumps({"source_aliases": ["source-alias"]}),
    )
    binding_by_field = {
        "namespace": "source_binding",
        "binding_id": str(BINDING_ID),
        "revision": 1,
        "source_system": "fhir",
        "source_id": "source-one",
        "dataset_schema": "source_data",
        "dataset_id": "dataset-one",
        "producer_id": "registry-fhir-catalog-v1",
        "edition_id": "dataset-one",
        "source_key": "network-one",
        "source_scope": {"organization_id": "network-one", "source_generation": SOURCE_GENERATION},
        "evidence_id": SOURCE_GENERATION + ":Organization/network-one",
        "evidence_sha256": "d" * 64,
        "metadata_sha256": "e" * 64,
        "accepted_at": "2026-01-01T00:00:00Z",
        "status": "current",
    }
    await connection.execute(
        f"INSERT INTO {catalog}.registry_fhir_catalog_binding VALUES($1,7,$2,'source-one',$3::jsonb)",
        GENERATION,
        BINDING_ID,
        json.dumps(binding_by_field),
    )


@pytest.fixture
async def effective_catalog_db(tmp_path):
    dsn = os.environ.get("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schemas = ["effective_catalog_test_" + uuid4().hex, "fhir_catalog_" + uuid4().hex]
    manifest = tmp_path / "catalogue-test-resources.json"
    manifest.write_text(json.dumps({"registered_before_creation": schemas}) + "\n")
    try:
        control, catalog = '"' + schemas[0] + '"', '"' + schemas[1] + '"'
        await _tables(connection, control, catalog)
        await _seed(connection, control, catalog)
        pin = SimpleNamespace(
            source_generations={"fhir_catalog": GENERATION},
            approved_custom_revision=0,
            generation_id=9,
            manifest_sha256=MANIFEST,
            schema_name=schemas[0],
            compact_layout=None,
        )
        async with connection.transaction():
            yield SimpleNamespace(connection=connection, control=schemas[0], catalog=schemas[1], manifest=pin)
    finally:
        for schema in reversed(schemas):
            await connection.execute('DROP SCHEMA IF EXISTS "' + schema + '" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        manifest.write_text(json.dumps({"cleaned_exact_schemas": schemas}) + "\n")
        await connection.close()


async def manual_record(db, kind, key, document, *, revision=1, custom=1):
    await db.connection.execute(
        f'INSERT INTO "{db.control}".registry_approved_record VALUES($1,$2,$3,$4,$5,$6::jsonb)',
        db.manifest.approved_custom_revision,
        kind,
        str(key),
        revision,
        custom,
        json.dumps(document),
    )
