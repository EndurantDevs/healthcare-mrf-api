# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify bounded narrow search preparation against independently read compact offices."""

import copy
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api.network_compact_address import compact_address_native_sql
from db.models import EntityAddressUnified
from process import network_geo_search as search
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
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
    assert (
        await search._source_pass(
            connection, '"candidate".network_geo_search', layout, layout.base, is_base=True, verify=True
        )
        == 3
    )
    first, second, final = connection.fetchrow.await_args_list
    assert "WHERE $1::text IS NULL" in first.args[0]
    assert "WHERE a.location_key>$1::text" in second.args[0]
    assert "IS NULL OR" not in second.args[0] and second.args[0] == final.args[0]
    assert first.args[1:] == (None, UUID(candidate_id))
    assert second.args[1:] == ("office-a", UUID(candidate_id))


@pytest.mark.asyncio
async def test_search_later_batch_has_index_range_under_generic_plan(search_db):
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
    await db.connection.execute("SET LOCAL plan_cache_mode='force_generic_plan'")
    ctes = search._batch_ctes(layout, layout.base, is_base=True, has_after=True)
    kinds = ",".join(("text", *("uuid" for _ in layout.artifacts)))
    await db.connection.execute(f"PREPARE search_keyset({kinds}) AS WITH {ctes} SELECT location_key FROM source_page")
    values = ",".join(("'seek-04000'", *("'" + artifact.candidate_id + "'" for artifact in layout.artifacts)))
    explained = json.loads(await db.connection.fetchval(f"EXPLAIN(FORMAT JSON) EXECUTE search_keyset({values})"))
    pending_nodes, index_conditions = [explained[0]["Plan"]], []
    while pending_nodes:
        plan_node = pending_nodes.pop()
        index_conditions.append(plan_node.get("Index Cond", ""))
        pending_nodes.extend(plan_node.get("Plans", ()))
    assert any("location_key" in condition and "> $1" in condition for condition in index_conditions)
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
    indexes = descriptor["indexes"]
    return {
        **{field: getattr(target, field) for field in search._SCOPE_FIELDS},
        "validation_json": {
            "candidate_readiness": {"ref_map_oid": layout.ref_map.table_oid},
            "serving_readiness": {
                "geo_search": descriptor,
                "address_artifact": {"table": {"oid": layout.base.table_oid}},
                "edge_artifacts": [asdict(artifact) for artifact in layout.artifacts],
            },
            "writer_closure": {
                "relation_oids": {search.TABLE: descriptor["table_oid"]},
                "index_oids": {index["name"]: index["index_oid"] for index in indexes},
                "index_table_oids": {index["name"]: index["table_oid"] for index in indexes},
            },
        },
    }


@pytest.mark.asyncio
async def test_bounded_search_matches_full_compact_rows_and_custom_precedence(search_db, monkeypatch):
    db = search_db
    monkeypatch.setattr(search, "_BATCH_ROWS", 1)
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    columns = ",".join("a." + column for column in search.SEARCH_COLUMNS)
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
    assert len(actual[0]) == len(search.SEARCH_COLUMNS) + 2
    candidate = _candidate(db.search_target, db.second_layout, descriptor)
    assert search.geo_search_receipt(candidate) == descriptor
    assert search.geo_search_relation(candidate).table_oid == descriptor["table_oid"]
    assert await db.connection.fetchval("SHOW work_mem") == "64MB"
    assert await db.connection.fetchval("SHOW temp_file_limit") == "256MB"


@pytest.mark.asyncio
async def test_search_rejects_changed_index_identity_and_receipt_scope(search_db):
    db = search_db
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    candidate = _candidate(db.search_target, db.second_layout, descriptor)
    for field in ("table_oid", "row_count", "sha256"):
        altered = copy.deepcopy(candidate)
        altered["validation_json"]["serving_readiness"]["geo_search"][field] = "invalid"
        with pytest.raises(search.NetworkGeoSearchError):
            search.geo_search_receipt(altered)
    altered = copy.deepcopy(candidate)
    altered["validation_json"]["writer_closure"]["index_table_oids"][search.TABLE + "_gn"] += 1
    with pytest.raises(search.NetworkGeoSearchError):
        search.geo_search_relation(altered)
    await db.connection.execute(f'DROP INDEX "{db.search_target.schema_name}".{search.TABLE}_gn')
    with pytest.raises(search.NetworkGeoSearchError):
        await search.verify_geo_search_indexes(db.connection, descriptor)
    await db.connection.execute(
        f'CREATE INDEX {search.TABLE}_gn ON "{db.search_target.schema_name}".{search.TABLE} USING gin(canonical_network_ids)'
    )
    descriptor["indexes"] = [
        {field: index[field] for field in ("name", *search._INDEX_FIELDS)}
        for index in await search._index_records(db.connection, descriptor["table_oid"])
    ]
    descriptor["sha256"] = search._digest({key: value for key, value in descriptor.items() if key != "sha256"})
    with pytest.raises(search.NetworkGeoSearchError):
        await search.verify_geo_search_indexes(db.connection, descriptor)


@pytest.mark.asyncio
async def test_search_parity_failure_rolls_back_only_its_new_projection(search_db, monkeypatch):
    db = search_db
    original = search._source_pass
    mutation_events = []

    async def corrupt_before_verify(connection, table, layout, source, *, is_base, verify):
        if verify and not mutation_events:
            await connection.execute(f"UPDATE {table} SET state_name='changed'")
            mutation_events.append("changed")
        return await original(connection, table, layout, source, is_base=is_base, verify=verify)

    monkeypatch.setattr(search, "_source_pass", corrupt_before_verify)
    with pytest.raises(search.NetworkGeoSearchError):
        await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    assert mutation_events == ["changed"]
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)", db.search_target.schema_name + "." + search.TABLE)
        is None
    )
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)::oid", search._qualified(db.second_layout.base))
        == db.second_layout.base.table_oid
    )


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
