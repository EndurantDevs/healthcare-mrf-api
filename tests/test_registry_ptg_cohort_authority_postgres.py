# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native set-join proof; dictionary coordinates are not graph edges."""

import struct

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_ptg_cohort_authority as authority
from process import registry_ptg_producer_scope as producer
from tests import test_registry_ptg_cohort_authority as fixture
from tests.test_result_archive_published_authority_postgres import _database, _seed


async def _relations(connection, schema):
    columns_by_table = {
        "office_assertion": "ordinal bigint PRIMARY KEY,binding_source_key text,company_key text,cohort_id text,"
        "snapshot_id text,provider_system text,provider_id text,dense_source_key integer,"
        "source_record_ordinal bigint,provider_group_ref text",
        "ptg2_provider_group_tax_identity_source": "snapshot_key bigint,source_key integer,source_record_ordinal bigint,"
        "provider_group_global_id_128 bytea,PRIMARY KEY(snapshot_key,source_key,source_record_ordinal)",
        "ptg2_v3_provider_group": "snapshot_key bigint,provider_group_key integer,provider_group_global_id_128 bytea,"
        "PRIMARY KEY(snapshot_key,provider_group_key),UNIQUE(snapshot_key,provider_group_global_id_128)",
        "ptg2_v4_npi_scope": "snapshot_key bigint,npi_key integer,npi bigint,"
        "PRIMARY KEY(snapshot_key,npi_key),UNIQUE(snapshot_key,npi)",
    }
    for relation, columns in columns_by_table.items():
        await connection.execute(text(f"CREATE TABLE {schema}.{relation}({columns})"))
    await connection.execute(
        text(f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_source(snapshot_id,source_key) VALUES ('synthetic-snapshot',0),('synthetic-snapshot',1);
    """)
    )
    await connection.execute(
        text(f"""
        INSERT INTO {schema}.ptg2_provider_group_tax_identity_source VALUES
          (31,0,0,decode(repeat('a',32),'hex')),(31,1,0,decode(repeat('b',32),'hex'))
    """)
    )
    await connection.execute(
        text(f"""
        INSERT INTO {schema}.ptg2_v3_provider_group VALUES
          (31,4,decode(repeat('a',32),'hex')),(31,5,decode(repeat('b',32),'hex'))
    """)
    )
    await connection.execute(text(f"INSERT INTO {schema}.ptg2_v4_npi_scope VALUES(31,9,1999999901)"))
    await connection.execute(
        text(f"""
        INSERT INTO {schema}.office_assertion
        SELECT series,'synthetic-binding','synthetic-company','synthetic-cohort','synthetic-snapshot',
          'npi','1999999901',0,0,repeat('a',32) FROM generate_series(1,4100) series
    """)
    )


def _parameters(**changes):
    return {
        "after": 0,
        "page_rows": authority._PAGE_ROWS,
        "snapshot_key": 31,
        "snapshot_id": "synthetic-snapshot",
        "payload_snapshot_id": "synthetic-snapshot",
        "binding_source_key": "synthetic-binding",
        "company_key": "synthetic-company",
        "cohort_id": "synthetic-cohort",
        "selected_sources": (0,),
    } | changes


@pytest.mark.asyncio
async def test_native_pages_resolve_exact_occurrences_with_bounded_edge_requests():
    async with _database() as (engine, schema_name):
        schema = f'"{schema_name}"'
        async with engine.begin() as connection:
            await _relations(connection, schema)
            query = text(authority._WITNESS_PAGE_SQL.format(schema=schema, office=f"{schema}.office_assertion"))
            first = (await connection.execute(query, _parameters())).mappings().one()
            assert authority._checked_page(first, 0) == (4096, struct.pack(">II", 4, 9))
            last = (await connection.execute(query, _parameters(after=4096))).mappings().one()
            assert authority._checked_page(last, 4096) == (4, struct.pack(">II", 4, 9))
            empty = (await connection.execute(query, _parameters(after=4100))).mappings().one()
            assert authority._checked_page(empty, 4100) == (0, b"")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("assignment", "error"),
    [
        ("provider_group_ref=repeat('b',32)", "registry_ptg_provider_unresolved"),
        ("source_record_ordinal=1", "registry_ptg_provider_unresolved"),
        ("dense_source_key=1", "registry_ptg_scope_changed"),
        ("company_key='other-company'", "registry_ptg_scope_changed"),
        ("cohort_id='other-cohort'", "registry_ptg_scope_changed"),
        ("provider_id='1000000004'", "registry_ptg_provider_unresolved"),
        ("provider_system='other'", "registry_ptg_provider_unresolved"),
        ("provider_group_ref='malformed'", "registry_ptg_provider_unresolved"),
        ("snapshot_id='other-snapshot'", "registry_ptg_scope_changed"),
    ],
)
async def test_native_page_rejects_cross_source_group_and_scope_substitution(assignment, error):
    async with _database() as (engine, schema_name):
        schema = f'"{schema_name}"'
        async with engine.begin() as connection:
            await _relations(connection, schema)
            await connection.execute(text(f"UPDATE {schema}.office_assertion SET {assignment} WHERE ordinal=1"))
            query = text(authority._WITNESS_PAGE_SQL.format(schema=schema, office=f"{schema}.office_assertion"))
            page = (await connection.execute(query, _parameters())).mappings().one()
            assert page["mismatch_count"] == 1 or page["scope_mismatch_count"] == 1
            with pytest.raises(authority.RegistryPTGCohortAuthorityError, match=error):
                authority._checked_page(page, 0)


@pytest.mark.asyncio
async def test_native_published_pin_and_missing_projection_refusal():
    async with _database() as (database, name):
        snapshot = await _seed(database, name)
        async with database.begin() as connection:
            await connection.execute(text(f'UPDATE "{name}".ptg2_v3_snapshot_source SET source_key=0'))
        specification = fixture._specification(
            ptg_schema_name=name, snapshot_id=snapshot, binding_source_key="source_a"
        )
        async with async_sessionmaker(database)() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            receipt = await authority.capture_registry_ptg_source(session, specification)
            assert receipt["contract"] == authority.PTG_PUBLISHED_RESULT_SOURCE_AUTHORITY_CONTRACT
            assert await authority._require_frozen_source(session, specification, receipt) == receipt
            snapshot_by_field, source_records, binding = await authority._resolved_source_state(session, specification)
            assert snapshot_by_field["binding_payload"] is None and binding is None and len(source_records) == 1
            identity = receipt["identity"]
            graph_by_field = {
                "snapshot_key": identity["snapshot_key"],
                "layout_generation": "shared_blocks_v4",
                "layout_mapping_sha256": identity["layout_mapping_digest"],
                "map_sha256": identity["map_digest"],
                "finalizer_map_sha256": identity["finalizer_map_digest"],
                "source_assignments_sha256": identity["source_assignments_sha256"],
            }
            with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_unavailable"):
                await authority._source_state(session, specification, graph_by_field)
            with pytest.raises(producer.RegistryPTGProducerScopeError, match="published_scope_unavailable"):
                await producer._evidence(session, specification, {}, receipt, graph_by_field)
            assert await authority.release_registry_ptg_source(session, specification, authority=receipt) == 1
            assert (await session.execute(text(f'SELECT count(*) FROM "{name}".ptg2_snapshot_pin'))).scalar_one() == 0
            with pytest.raises(authority.RegistryPTGCohortAuthorityError, match="source_changed"):
                await authority._require_frozen_source(session, specification, receipt)
