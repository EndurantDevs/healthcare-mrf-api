# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise sparse edge-selected search parity and complete final comparison."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID

import pytest

from api.network_compact_address import compact_address_native_sql
from db.models import EntityAddressUnified
from process import network_geo_search as search
from tests.test_network_compact_address_postgres import compact_db as compact_db
from tests.test_network_geo_search_postgres import search_db as search_db
from tests.test_network_membership_slices_postgres import slice_db as slice_db


def _bucket_key(prefix, bucket):
    return next(
        prefix + str(index)
        for index in range(10000)
        if hashlib.sha256((prefix + str(index)).encode()).digest()[0] & 63 == bucket
    )


async def _clone_rows(connection, relation, key_sql, *, count=1, changes=None):
    table = search._qualified(relation)
    expression_by_field = {"location_key": key_sql, **(changes or {})}
    fields = ",".join(
        expression_by_field.get(column.name, 'a."' + column.name + '"')
        for column in EntityAddressUnified.__table__.columns
    )
    await connection.execute(
        f"INSERT INTO {table} SELECT {fields} FROM generate_series(1,{count}) sequence "
        f"CROSS JOIN (SELECT * FROM ONLY {table} ORDER BY location_key LIMIT 1) a"
    )


@pytest.mark.asyncio
async def test_sparse_selected_edges_match_full_rows_with_missing_and_manual_addresses(search_db, monkeypatch):
    db, layout = search_db, search_db.second_layout
    monkeypatch.setattr(search, "_BATCH_ROWS", 1)
    for relation in (layout.base, layout.custom):
        await _clone_rows(db.connection, relation, "'unrelated-'||lpad(sequence::text,6,'0')", count=20000)
        await db.connection.execute(f"ANALYZE {search._qualified(relation)}")
    await db.connection.execute(
        f"UPDATE {search._qualified(layout.custom)} SET lat=NULL,long=NULL WHERE location_key=$1", db.keys[2]
    )
    artifact = layout.artifacts[0]
    reference = await db.connection.fetchrow(
        f"SELECT slice_id,office_bucket FROM {search._qualified(layout.ref_map)} WHERE artifact_candidate_id=$1 ORDER BY network_id LIMIT 1",
        UUID(artifact.candidate_id),
    )
    missing_key = _bucket_key("aaa-missing-", reference["office_bucket"])
    manual_key = _bucket_key("selected-manual-", reference["office_bucket"])
    filtered_key = _bucket_key("selected-filtered-", reference["office_bucket"])
    await _clone_rows(
        db.connection,
        layout.base,
        "'" + manual_key + "'",
        changes={"entity_type": "'organization'", "entity_id": "'manual-provider'", "npi": "NULL"},
    )
    await _clone_rows(db.connection, layout.base, "'" + filtered_key + "'", changes={"row_origin": "'network_custom'"})
    await db.connection.executemany(
        f'INSERT INTO "{artifact.schema_name}".network_office_edge VALUES($1,$2)',
        [(reference["slice_id"], key) for key in (missing_key, manual_key, filtered_key)]
        + [(UUID(int=999), db.keys[1])],
    )
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, layout)
    columns = ",".join("a." + column for column in search._LOCATION_COLUMNS)
    relation, arguments = compact_address_native_sql(layout, first_parameter=1, visible_membership_sql="TRUE")
    expected = await db.connection.fetch(
        f"SELECT {columns},{search._GEO} AS geo,a.canonical_network_ids FROM {relation} a WHERE cardinality(a.canonical_network_ids)>0 ORDER BY a.location_key",
        *arguments,
    )
    actual = await db.connection.fetch(
        f'SELECT * FROM "{db.search_target.schema_name}".{search.TABLE} ORDER BY location_key'
    )
    assert actual == expected and descriptor["row_count"] == len(expected) == 3
    manual_row = next(address_row for address_row in actual if address_row["location_key"] == manual_key)
    assert manual_row["npi"] is None
    assert next(address_row for address_row in actual if address_row["location_key"] == db.keys[2])["geo"] is None
    actual_keys = [address_row["location_key"] for address_row in actual]
    assert missing_key not in actual_keys and filtered_key not in actual_keys


@pytest.mark.asyncio
async def test_shared_slice_id_in_another_bucket_adds_no_network(search_db):
    db, layout = search_db, search_db.second_layout
    relation, arguments = compact_address_native_sql(layout, first_parameter=1, visible_membership_sql="TRUE")
    selected_columns = "location_key,canonical_network_ids"
    expected = await db.connection.fetch(
        f"SELECT {selected_columns} FROM {relation} a WHERE cardinality(a.canonical_network_ids)>0 ORDER BY location_key",
        *arguments,
    )
    refs = search._qualified(layout.ref_map)
    await db.connection.execute(f"""INSERT INTO {refs}
      (network_id,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256)
      SELECT 1701,(office_bucket+1)%64,artifact_candidate_id,slice_id,row_count,content_sha256
      FROM {refs} ORDER BY network_id LIMIT 1""")
    descriptor = await search.prepare_network_geo_search(db.connection, db.search_target, layout)
    actual = await db.connection.fetch(
        f'SELECT {selected_columns} FROM "{db.search_target.schema_name}".{search.TABLE} ORDER BY location_key'
    )
    assert actual == expected and descriptor["row_count"] == len(expected) == 2


@pytest.mark.asyncio
async def test_no_active_edge_artifacts_builds_an_empty_verified_projection(search_db):
    db = search_db
    await db.connection.execute(f"DELETE FROM {search._qualified(db.second_layout.ref_map)}")
    descriptor = await search.prepare_network_geo_search(
        db.connection, db.search_target, replace(db.second_layout, artifacts=())
    )
    assert descriptor["row_count"] == 0
    assert await db.connection.fetchval(f'SELECT count(*) FROM "{db.search_target.schema_name}".{search.TABLE}') == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "extra"])
async def test_insert_accounting_rejects_missing_or_extra_rows(search_db, monkeypatch, fault):
    db, original = search_db, search._insert_projection
    differences = []

    async def change_insert(connection, table, layout):
        async def fetchrow(statement, *arguments):
            columns = ",".join(search.compact_identifier(name) for name in search._COLUMNS)
            selected = f"SELECT {columns} FROM expected"
            changed = selected + " WHERE FALSE" if fault == "missing" else selected + " UNION ALL " + selected
            assert selected in statement
            result = await connection.fetchrow(statement.replace(selected, changed, 1), *arguments)
            if result["differs"]:
                differences.append(result["row_count"])
            return result

        return await original(SimpleNamespace(fetchrow=fetchrow), table, layout)

    monkeypatch.setattr(search, "_insert_projection", change_insert)
    with pytest.raises(search.NetworkGeoSearchError):
        await search.prepare_network_geo_search(db.connection, db.search_target, db.second_layout)
    assert differences == [0 if fault == "missing" else 4]
    assert (
        await db.connection.fetchval("SELECT to_regclass($1)", db.search_target.schema_name + "." + search.TABLE)
        is None
    )
