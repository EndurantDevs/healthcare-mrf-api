# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared graph scopes require full raw-input identity, never matching provider counts alone."""

import json
from copy import deepcopy
from uuid import uuid4

import pytest

from process import registry_native_provider_source as providers
from process.ptg_parts.ptg2_shared_source_set import shared_source_set_metadata
from process.registry_native_network_catalog import native_catalog_descriptor
from tests.test_registry_fhir_catalog_store_postgres import storage_database as storage_database
from tests.test_registry_imported_source_inventory import native_ptg_source


async def source_scope(fixture, version):
    schema = "native_ptg_scope_" + uuid4().hex
    fixture.schemas.append(schema)
    await fixture.connection.execute('CREATE SCHEMA "' + schema + '"')
    namespace = '"' + schema + '"'
    await fixture.connection.execute(f"""CREATE TABLE {namespace}.ptg2_snapshot(snapshot_id text,status text,manifest jsonb);
      CREATE TABLE {namespace}.ptg2_v3_snapshot_binding(snapshot_id text,snapshot_key bigint);
      CREATE TABLE {namespace}.ptg2_v3_snapshot_layout(snapshot_key bigint,state text,generation text,layout_manifest jsonb);
      CREATE TABLE {namespace}.ptg2_v3_snapshot_source(snapshot_id text,source_key integer,raw_container_sha256 text);
      CREATE TABLE {namespace}.ptg2_v3_npi_scope(snapshot_key bigint,npi bigint);
      CREATE TABLE {namespace}.ptg2_v4_npi_scope(snapshot_key bigint,npi bigint)""")
    raw_set = shared_source_set_metadata(["a" * 64])
    source_document = native_ptg_source()
    source_document["schema_name"] = schema
    source_document["graph"].update(source_set=raw_set, npi_scope_table=f"ptg2_{version}_npi_scope")
    source_document["graph"]["provider_graph"]["npi_count"] = 2
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.ptg2_snapshot VALUES('snapshot-one','published',$1::jsonb)",
        json.dumps({"serving_index": {"source_set": raw_set}}),
    )
    await fixture.connection.execute(f"INSERT INTO {namespace}.ptg2_v3_snapshot_binding VALUES('snapshot-one',31)")
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.ptg2_v3_snapshot_layout VALUES(31,'sealed',$1,$2::jsonb)",
        "shared_blocks_" + version,
        json.dumps(
            {
                "serving_index": {
                    "source_witness": {"source_count": 1, "source_set_digest": raw_set["raw_container_sha256_digest"]}
                }
            }
        ),
    )
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.ptg2_v3_snapshot_source VALUES('snapshot-one',0,$1)", "a" * 64
    )
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.ptg2_{version}_npi_scope VALUES(31,1234567893),(31,1999999984)"
    )
    return native_catalog_descriptor(source_document), namespace


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v3", "v4"])
async def test_complete_input_equivalence_allows_explicit_whole_source_set(storage_database, version):
    fixture = storage_database
    descriptor, _ = await source_scope(fixture, version)
    async with fixture.connection.transaction():
        await providers.require_ptg_complete_provider_scope(fixture.connection, descriptor)
        query, parameters = providers.native_provider_query(descriptor)
        rows = await fixture.connection.fetch(query, *parameters)
        assert [row["npi"] for row in rows] == [1234567893, 1999999984]
        assert {row["source_key"] for row in rows} == {"PTGProviderSet/source-graph"}


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v3", "v4"])
async def test_same_count_different_raw_inputs_refuses_shared_scope(storage_database, version):
    fixture = storage_database
    descriptor, namespace = await source_scope(fixture, version)
    different = shared_source_set_metadata(["b" * 64])
    descriptor["input"]["graph"]["source_set"] = different
    await fixture.connection.execute(
        f"UPDATE {namespace}.ptg2_snapshot SET manifest=$1::jsonb",
        json.dumps({"serving_index": {"source_set": different}}),
    )
    await fixture.connection.execute(
        f"UPDATE {namespace}.ptg2_v3_snapshot_source SET raw_container_sha256=$1", "b" * 64
    )
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="scope_changed"):
            await providers.require_ptg_complete_provider_scope(fixture.connection, descriptor)


@pytest.mark.asyncio
async def test_scope_requires_correct_layout_and_preserves_raw_providers(storage_database):
    fixture = storage_database
    descriptor, namespace = await source_scope(fixture, "v4")
    wrong = deepcopy(descriptor)
    wrong["input"]["graph"]["npi_scope_table"] = "ptg2_v3_npi_scope"
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="scope_changed"):
            await providers.require_ptg_complete_provider_scope(fixture.connection, wrong)
        await fixture.connection.execute(
            f"UPDATE {namespace}.ptg2_v4_npi_scope SET npi=3333333333 WHERE npi=1234567893"
        )
        await providers.require_ptg_complete_provider_scope(fixture.connection, descriptor)
        query, parameters = providers.native_provider_query(descriptor)
        rows = await fixture.connection.fetch(query, *parameters)
        assert [row["npi"] for row in rows] == [1999999984, 3333333333]
