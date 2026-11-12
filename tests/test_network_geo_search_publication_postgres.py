# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The optional search heap follows the existing publication and retained-read lifecycle."""

import json
from dataclasses import asdict, replace
from uuid import UUID

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.endpoint import npi
from api.network_address_scope import (
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.network_read_execution import execute_network_read, network_read_name_inventory, network_read_relations
from api.provider_geo_sql import build_native_nearby_count_sql, build_native_nearby_sql
from process import network_geo_search
from process.network_compact_layout import CompactAddressRelation, bind_compact_geo_search
from process.network_serving_read import resolve_network_serving_manifest
from tests import test_network_compact_publication_postgres as publication
from tests.test_network_compact_address_lookup import _layout
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db


def test_search_sources_allow_only_the_authenticated_base_name_to_rotate():
    original = _layout()
    sources = json.loads(json.dumps(asdict(original)))
    search = CompactAddressRelation(original.ref_map.schema_name, "network_geo_search", 6)
    rotated_base = replace(original.base, table_name="retained_address_result")
    rotated = replace(original, base=rotated_base)
    assert bind_compact_geo_search(rotated, search, sources=sources).geo_search == search
    for base in (
        replace(rotated_base, table_oid=7),
        replace(rotated_base, schema_name="other_schema"),
    ):
        with pytest.raises(ValueError, match="inputs differ"):
            bind_compact_geo_search(replace(original, base=base), search, sources=sources)
    with pytest.raises(ValueError, match="inputs differ"):
        bind_compact_geo_search(replace(original, custom=None), search, sources=sources)


async def _geo_rows(fixture, generation_id, *, projected):
    async with AsyncSession(fixture.reader_engine) as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        connection = await session.connection()
        native = (await connection.get_raw_connection()).driver_connection
        original_path = await native.fetchval("SHOW search_path")
        await native.execute("SELECT set_config('search_path',$1,true)", fixture.metadata_path)
        manifest = await resolve_network_serving_manifest(
            native, generation_id=generation_id, control_schema=fixture.control_schema, api_read=True
        )
        assert await native.fetchval("SHOW search_path") == fixture.metadata_path
        await native.execute("SELECT set_config('search_path',$1,true)", original_path)
        search = manifest.compact_layout.geo_search
        assert (search is not None) is projected
        if projected:
            assert search in manifest.candidate_relation_pins
        with network_read_name_inventory(native, manifest.candidate_relation_pins):
            with network_address_read_scope(manifest, (42,)):
                relation = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
                nearby = npi._nearby_sql_query("1=1", "", "", use_taxonomy_filter=False, address_table_sql=relation)
                page_sql = build_native_nearby_sql(nearby, use_search_page=True)
                count_sql = build_native_nearby_count_sql(nearby)
                assert (nearby.search_table_sql is not None) is projected
                if projected:
                    assert network_read_relations(count_sql) == (search,)
                    assert search in network_read_relations(page_sql)
                parameters = scoped_address_parameters({"in_lat": 40.75, "in_long": -73.5, "radius": 10.0, "limit": 28})
                count = (await execute_network_read(session, text(count_sql), parameters)).scalar_one()
                page_rows = (await execute_network_read(session, text(page_sql), parameters)).all()
                assert count == 1
                return [page_row._mapping["location_key"] for page_row in page_rows]


async def _provider_tables(fixture):
    await fixture.connection.execute("CREATE SCHEMA mrf")
    await fixture.connection.execute(
        f'CREATE TABLE mrf.npi AS SELECT DISTINCT npi FROM "{fixture.address.schema}".entity_address_unified'
    )
    await fixture.connection.execute("CREATE TABLE mrf.npi_taxonomy(npi bigint,healthcare_provider_taxonomy_code text)")
    await fixture.connection.execute("CREATE TABLE mrf.nucc_taxonomy(code text,display_name text)")
    reader = fixture.address.roles["reader"]
    await fixture.connection.execute(f'GRANT USAGE ON SCHEMA mrf TO "{reader}"')
    await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf TO "{reader}"')


async def _publish_legacy_profile(fixture, monkeypatch):
    async def legacy_profile(*_arguments, **_options):
        return None

    first = await publication._candidate(fixture, (42, 88), 0)
    with monkeypatch.context() as patch:
        patch.setattr(network_geo_search, "prepare_network_geo_search", legacy_profile)
        await publication._prepare(fixture, first)
    await publication._seal(fixture, first)
    await publication._publish(fixture, first)
    return await publication._read(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "producer_path,metadata_path", [("pg_catalog", "public,pg_catalog"), ("public,pg_catalog", "pg_catalog")]
)
async def test_geo_search_preparation_custody_publication_and_retained_fallback(
    compact_publication_db, monkeypatch, producer_path, metadata_path
):
    """Different catalog paths preserve publication identity, retained reads, and rollback."""
    fixture = compact_publication_db
    fixture.metadata_path = metadata_path
    monkeypatch.setattr(npi, "_should_include_geo_service_locations", lambda: True)
    await _provider_tables(fixture)
    original_prepare = network_geo_search.prepare_network_geo_search

    old_manifest, old_rows = await _publish_legacy_profile(fixture, monkeypatch)
    old_geo_rows = await _geo_rows(fixture, old_manifest.generation_id, projected=False)
    second = await publication._candidate(fixture, (42, 99), old_manifest.generation_id)
    await fixture.execution_connection.execute("SELECT set_config('search_path',$1,false)", producer_path)
    await fixture.custodian.execute("SELECT set_config('search_path',$1,false)", metadata_path)

    async def fail_after_search(*arguments, **options):
        await original_prepare(*arguments, **options)
        raise RuntimeError("synthetic failure after search verification")

    with monkeypatch.context() as patch:
        patch.setattr(network_geo_search, "prepare_network_geo_search", fail_after_search)
        with pytest.raises(RuntimeError, match="after search verification"):
            await publication._prepare(fixture, second)
    assert not fixture.execution_connection.is_in_transaction()
    assert await fixture.execution_connection.fetchval("SHOW search_path") == producer_path
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", second.schema_name + ".network_geo_search") is None
    )
    assert (await publication._read(fixture))[0].generation_id == old_manifest.generation_id

    await publication._prepare(fixture, second)
    assert await fixture.execution_connection.fetchval("SHOW search_path") == producer_path
    candidate = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(second.candidate_id),
    )
    descriptor = network_geo_search.geo_search_receipt(dict(candidate))
    assert candidate["state"] == "ready" and candidate["index_ready"] is True
    assert descriptor["row_count"] == 2
    closure = json.loads(candidate["validation_json"])["writer_closure"]
    assert closure["relation_oids"][network_geo_search.TABLE] == descriptor["table_oid"]
    async with fixture.connection.transaction():
        await network_geo_search.verify_geo_search_indexes(fixture.connection, descriptor)
    seal = await publication._seal(fixture, second)
    assert await fixture.custodian.fetchval("SHOW search_path") == metadata_path
    assert seal["receipt"]["closure"]["relation_oids"][network_geo_search.TABLE] == descriptor["table_oid"]
    for connection in (fixture.address.reader, fixture.execution_connection):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(f'UPDATE "{second.schema_name}".network_geo_search SET npi=npi')
    await publication._assert_rollback_then_publish(fixture, second, old_manifest, old_rows)
    assert await fixture.execution_connection.fetchval("SHOW search_path") == producer_path
    current, _rows = await publication._read(fixture)
    assert current.manifest_sha256 != old_manifest.manifest_sha256
    assert await _geo_rows(fixture, current.generation_id, projected=True) == old_geo_rows
    assert await _geo_rows(fixture, old_manifest.generation_id, projected=False) == old_geo_rows
