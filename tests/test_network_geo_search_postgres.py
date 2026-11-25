# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify bounded narrow search preparation against independently read compact offices."""

import copy
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from api.network_compact_address import compact_address_native_sql, compact_city_page, compact_city_search_relation
from db.models import EntityAddressUnified
from process import network_geo_search as search
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    bind_compact_geo_search_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from tests.test_network_compact_address_postgres import compact_db as compact_db
from tests.test_network_membership_slices_postgres import slice_db as slice_db


@pytest.mark.asyncio
async def test_search_keyset_reuses_direct_range_shape_after_first_batch():
    candidate_id = str(UUID(int=1))
    namespace = "network_candidate_" + UUID(candidate_id).hex
    layout = CompactNetworkLayout(
        CompactAddressRelation(namespace, "network_slice_ref"),
        CompactAddressRelation("address_source", "entity_address_unified"),
        (CompactEdgeArtifact(candidate_id, namespace),),
    )
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            side_effect=[
                {"last_key": "office-a", "row_count": 2, "differs": False},
                {"last_key": "office-b", "row_count": 1, "differs": False},
                {"last_key": None, "row_count": 0, "differs": False},
            ]
        )
    )
    assert await search._insert_projection(connection, '"candidate".network_geo_search', layout) == 3
    first, second, final = connection.fetchrow.await_args_list
    assert "WHERE $1::text IS NULL" in first.args[0]
    assert "WHERE edge.location_key>$1::text" in second.args[0]
    assert "a.location_key=networks.location_key" in second.args[0]
    assert "IS NULL OR" not in second.args[0] and second.args[0] == final.args[0]
    assert first.args[1:] == (None, UUID(candidate_id))
    assert second.args[1:] == ("office-a", UUID(candidate_id))


@pytest.mark.asyncio
async def test_search_generic_plan_uses_edge_range_and_exact_address_lookup(search_db):
    db, layout = search_db, search_db.second_layout
    base = search._qualified(layout.base)
    fields = ",".join(
        "'seek-'||lpad(sequence::text,5,'0')" if column.name == "location_key" else 'a."' + column.name + '"'
        for column in EntityAddressUnified.__table__.columns
    )
    await db.connection.execute(
        f"INSERT INTO {base} SELECT {fields} FROM generate_series(1,6000) sequence "
        f"CROSS JOIN (SELECT * FROM ONLY {base} ORDER BY location_key LIMIT 1) a"
    )
    await db.connection.execute(f"ANALYZE {base}")
    artifact = layout.artifacts[0]
    edge = '"' + artifact.schema_name + '".network_office_edge'
    await db.connection.execute(
        f"INSERT INTO {edge} SELECT slice_id,'seek-'||lpad(sequence::text,5,'0') "
        f"FROM (SELECT slice_id FROM {edge} ORDER BY slice_id LIMIT 1) selected "
        "CROSS JOIN generate_series(1,6000) sequence"
    )
    await db.connection.execute(f"ANALYZE {edge}")
    await db.connection.execute("SET LOCAL plan_cache_mode='force_generic_plan'")
    ctes = search._batch_ctes(layout, has_after=True)
    kinds = ",".join(("text", *("uuid" for _ in layout.artifacts)))
    await db.connection.execute(f"PREPARE search_keyset({kinds}) AS WITH {ctes} SELECT location_key FROM expected")
    prepared_arguments = ",".join(
        ("'seek-04000'", *("'" + artifact.candidate_id + "'" for artifact in layout.artifacts))
    )
    explained = json.loads(
        await db.connection.fetchval(
            f"EXPLAIN(ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) EXECUTE search_keyset({prepared_arguments})"
        )
    )
    pending_nodes, nodes = [explained[0]["Plan"]], []
    while pending_nodes:
        plan_node = pending_nodes.pop()
        nodes.append(plan_node)
        pending_nodes.extend(plan_node.get("Plans", ()))
    assert any(
        node.get("Relation Name") == "network_office_edge" and "> $1" in node.get("Index Cond", "") for node in nodes
    ), json.dumps(explained)
    assert any(
        node.get("Schema") == layout.base.schema_name
        and node.get("Relation Name") == layout.base.table_name
        and "location_key" in node.get("Index Cond", "")
        and " = " in node["Index Cond"]
        for node in nodes
    ), json.dumps(explained)
    await db.connection.execute("DEALLOCATE search_keyset")


@pytest.fixture
async def search_db(compact_db):
    db = compact_db
    layout = db.second_layout
    entry = await db.connection.fetchrow(
        f'SELECT * FROM "{db.source_schema}".network_membership_candidate WHERE schema_name=$1',
        layout.ref_map.schema_name,
    )
    db.search_target = MembershipCopyTarget(**{field: str(entry[field]) for field in search._SCOPE_FIELDS})
    async with db.connection.transaction():
        await db.connection.execute("SET LOCAL work_mem='64MB'")
        await db.connection.execute("SET LOCAL temp_file_limit='256MB'")
        await db.connection.execute("SET LOCAL statement_timeout='60s'")
        await db.connection.execute("SET LOCAL jit=off")
        for relation in (layout.base, layout.custom):
            await db.connection.execute(
                f"UPDATE {search._qualified(relation)} SET lat=0,long=0,address_key='00000000-0000-0000-0000-000000000001',taxonomy_array=ARRAY[1],state_name='AA'"
            )
        await db.connection.execute(
            f"UPDATE {search._qualified(layout.custom)} SET lat=70,address_precision='city_zip',state_name='BB' WHERE location_key=$1",
            db.keys[0],
        )
        yield db


def _candidate(target, layout, descriptor):
    """Shape the published candidate row: its scope and the named search and edge inputs."""
    return {
        **{field: getattr(target, field) for field in search._SCOPE_FIELDS},
        "validation_json": {
            "serving_readiness": {
                "geo_search": descriptor,
                "edge_artifacts": [asdict(artifact) for artifact in layout.artifacts],
            },
        },
    }


@pytest.mark.asyncio
async def test_bounded_search_matches_full_compact_rows_and_custom_precedence(search_db, monkeypatch):
    db = search_db
    monkeypatch.setattr(search, "_BATCH_ROWS", 1)
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    columns = ",".join("a." + column for column in search._LOCATION_COLUMNS)
    relation, arguments = compact_address_native_sql(db.second_layout, first_parameter=1, visible_membership_sql="TRUE")
    expected = await db.connection.fetch(
        f"SELECT {columns},{search._GEO} AS geo,a.canonical_network_ids FROM {relation} a WHERE cardinality(a.canonical_network_ids)>0 ORDER BY a.location_key",
        *arguments,
    )
    actual = await db.connection.fetch(
        f'SELECT * FROM "{db.search_target.schema_name}".{search.TABLE} ORDER BY location_key'
    )
    assert actual == expected and descriptor["row_count"] == len(expected) == 2
    assert actual[0]["lat"] == 70 and actual[0]["state_name"] == "BB"
    assert actual[0]["canonical_network_ids"] == [7, 42]
    assert len(actual[0]) == len(search._LOCATION_COLUMNS) + 2
    candidate = _candidate(db.search_target, db.second_layout, descriptor)
    assert search.geo_search_receipt(candidate) == descriptor
    assert search.geo_search_relation(candidate) == CompactAddressRelation(db.search_target.schema_name, search.TABLE)
    assert await db.connection.fetchval("SHOW work_mem") == "64MB"
    assert await db.connection.fetchval("SHOW temp_file_limit") == "256MB"


@pytest.mark.asyncio
async def test_published_city_search_preserves_inferred_and_non_geocoded_offices(search_db):
    db, layout = search_db, search_db.second_layout
    await db.connection.execute(f"UPDATE {search._qualified(layout.base)} SET city_name='EXAMPLE CITY'")
    await db.connection.execute(
        f"""UPDATE {search._qualified(layout.custom)}
        SET city_name='EXAMPLE CITY',npi=NULL,inferred_npi=2222222222,lat=NULL,long=NULL,type='secondary'
        WHERE location_key=$1""",
        db.keys[2],
    )
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, layout)
    candidate = _candidate(db.search_target, layout, descriptor)
    assert bind_compact_geo_search_candidate(layout, candidate).city_search is None
    candidate["validation_json"]["serving_readiness"]["city_search"] = True
    projected = bind_compact_geo_search_candidate(layout, candidate)
    relation_name = search._qualified(projected.city_search)
    index_names = {index["name"] for index in await search._index_records(db.connection, relation_name)}
    assert search._CITY_INDEX in index_names
    assert search._CITY_INDEX not in {index["name"] for index in descriptor["indexes"]}

    for network_ids, expected_npis in (((42,), []), ((42, 99), [2222222222]), ((99,), [2222222222]), ((88,), [])):
        await _assert_published_city_search(db.connection, projected, network_ids, expected_npis)


async def _assert_published_city_search(connection, layout, network_ids, expected_npis):
    address_type = "c.type IN ('primary','secondary','practice','site')"
    ordinary_sql, ordinary_binds = compact_address_native_sql(layout, first_parameter=1, network_ids=network_ids)
    reference_rows = await connection.fetch(
        f"SELECT DISTINCT coalesce(c.npi,c.inferred_npi) npi FROM {ordinary_sql} c "
        f"WHERE city_name='EXAMPLE CITY' AND state_name='AA' AND {address_type} ORDER BY npi",
        *ordinary_binds,
    )
    assert [reference_row["npi"] for reference_row in reference_rows] == expected_npis
    key_ctes, page_sql, fallback_filter = compact_city_page(layout, network_ids, address_type)
    assert key_ctes is None and fallback_filter == "FALSE"
    relation_sql = compact_city_search_relation(layout, network_ids)
    count_sql = (
        f"SELECT count(DISTINCT coalesce(c.npi,c.inferred_npi)) FROM {relation_sql} c "
        f"WHERE city_name=:city AND state_name=:state AND {address_type}"
    )
    for statement, offset, expected_values in (
        (count_sql, 0, [len(expected_npis)]),
        (page_sql, 0, expected_npis),
        (page_sql, 1, []),
    ):
        compiled = text(statement).compile(dialect=PGDialect_asyncpg())
        binds_by_name = {
            "_canonical_network_ids": list(network_ids),
            "city": "EXAMPLE CITY",
            "state": "AA",
            "limit": 1,
            "start": offset,
        }
        observed_rows = await connection.fetch(str(compiled), *(binds_by_name[name] for name in compiled.positiontup))
        assert [observed_row[0] for observed_row in observed_rows] == expected_values


@pytest.mark.asyncio
async def test_search_rejects_changed_index_identity_and_receipt_scope(search_db):
    db = search_db
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    candidate = _candidate(db.search_target, db.second_layout, descriptor)
    for change in ("schema_name", "artifacts"):
        altered = copy.deepcopy(candidate)
        if change == "schema_name":
            altered["schema_name"] = "network_candidate_" + "0" * 32
        else:
            altered["validation_json"]["serving_readiness"]["edge_artifacts"] = []
        with pytest.raises(search.NetworkGeoSearchError):
            search.geo_search_relation(altered)
    relation = search._qualified(CompactAddressRelation(db.search_target.schema_name, search.TABLE))
    await db.connection.execute(f'DROP INDEX "{db.search_target.schema_name}".{search.TABLE}_gn')
    with pytest.raises(search.NetworkGeoSearchError):
        await search.verify_geo_search_indexes(db.connection, descriptor)
    await db.connection.execute(
        f'CREATE INDEX {search.TABLE}_gn ON "{db.search_target.schema_name}".{search.TABLE} USING gin(canonical_network_ids)'
    )
    descriptor["indexes"] = [
        {field: index[field] for field in ("name", *search._INDEX_FIELDS)}
        for index in await search._index_records(db.connection, relation)
        if index["name"] != search._CITY_INDEX
    ]
    descriptor["sha256"] = search._digest({key: entry for key, entry in descriptor.items() if key != "sha256"})
    with pytest.raises(search.NetworkGeoSearchError):
        await search.verify_geo_search_indexes(db.connection, descriptor)
    # Receipt content changes come last: a changed digest is refused only while the receipt digest is verified.
    for field in ("table_name", "row_count", "sha256"):
        altered = copy.deepcopy(candidate)
        altered["validation_json"]["serving_readiness"]["geo_search"][field] = "invalid"
        with pytest.raises(search.NetworkGeoSearchError):
            search.geo_search_receipt(altered)


@pytest.mark.asyncio
async def test_duplicate_office_fails_the_native_key_and_rolls_back_the_projection(search_db):
    db = search_db
    custom = search._qualified(db.second_layout.custom)
    primary_key = await db.connection.fetchval(
        "SELECT conname FROM pg_constraint WHERE conrelid=to_regclass($1::text) AND contype='p'", custom
    )
    await db.connection.execute(f"ALTER TABLE {custom} DROP CONSTRAINT {search.compact_identifier(primary_key)}")
    await db.connection.execute(f"INSERT INTO {custom} SELECT * FROM {custom} WHERE location_key=$1", db.keys[0])
    with pytest.raises(asyncpg.UniqueViolationError):
        await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)", db.search_target.schema_name + "." + search.TABLE)
        is None
    )
    assert await db.connection.fetchval("SELECT to_regclass($1)", search._qualified(db.second_layout.base)) is not None


@pytest.mark.asyncio
async def test_old_incompatible_profile_has_no_search_receipt_or_heap(search_db):
    db = search_db
    await db.connection.execute(f"ALTER TABLE {search._qualified(db.second_layout.base)} DROP COLUMN aca_plan_array")
    assert await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout) is None
    assert search.geo_search_receipt({"validation_json": json.dumps({"serving_readiness": {}})}) is None
    assert search.geo_search_relation({"validation_json": {}}) is None
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)", db.search_target.schema_name + "." + search.TABLE)
        is None
    )
