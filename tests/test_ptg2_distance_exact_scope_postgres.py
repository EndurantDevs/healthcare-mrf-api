# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL proof for the bounded exact-distance taxonomy path."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text

from api import ptg2_serving as serving
from api import ptg_membership_reader
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_coverage import (
    _invalid_fixture_npis,
)
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables

_CREATE_SQL = (
    """CREATE TABLE {schema}.npi (
        npi bigint PRIMARY KEY,
        entity_type_code smallint NOT NULL,
        search_taxonomy_codes varchar[] NOT NULL
    )""",
    """CREATE TABLE {schema}.npi_taxonomy (
        npi bigint NOT NULL,
        healthcare_provider_taxonomy_code varchar NOT NULL,
        healthcare_provider_primary_taxonomy_switch varchar
    )""",
)
_INSERT_NPI_SQL = """INSERT INTO {schema}.npi (
        npi, entity_type_code, search_taxonomy_codes
    )
    SELECT npi, 1, ARRAY['207R00000X']::varchar[]
      FROM unnest(CAST(:projection_negative_npis AS bigint[])) npi
    UNION ALL
    SELECT npi, 1, ARRAY['207Q00000X']::varchar[]
      FROM unnest(CAST(:projection_positive_npis AS bigint[])) npi"""
_INSERT_SCOPE_SQL = """INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
    SELECT 41, npi FROM {schema}.npi"""
_INSERT_TAXONOMY_SQL = """INSERT INTO {schema}.npi_taxonomy (
        npi, healthcare_provider_taxonomy_code,
        healthcare_provider_primary_taxonomy_switch
    )
    SELECT npi, '207Q00000X', 'Y'
      FROM unnest(CAST(:exact_npis AS bigint[])) npi"""
_INSERT_PROJECTION_NEGATIVE_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    )
    SELECT
        'projection-negative-' || ordinal, npi,
        md5('projection-negative-address-' || ordinal)::uuid,
        md5('projection-negative-premise-' || ordinal)::uuid,
        1, ARRAY['nppes']::varchar[], 1, 1, 'practice', ordinal,
        ordinal || ' SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
        '00001', '00001', 'US', 'street', 42.0, -83.0 + ordinal * 0.000001
      FROM unnest(CAST(:projection_negative_npis AS bigint[]))
           WITH ORDINALITY fixture(npi, ordinal)"""
_INSERT_EXACT_NEGATIVE_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    )
    SELECT
        'exact-negative-' || ordinal, npi,
        md5('exact-negative-address-' || ordinal)::uuid,
        md5('exact-negative-premise-' || ordinal)::uuid,
        1, ARRAY['nppes']::varchar[], 1, 1, 'practice', 1000 + ordinal,
        ordinal || ' SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
        '00001', '00001', 'US', 'street', 42.0, -83.0 + ordinal * 0.00001
      FROM unnest(CAST(:exact_negative_npis AS bigint[]))
           WITH ORDINALITY fixture(npi, ordinal)"""
_INSERT_EXACT_SQL = """INSERT INTO {schema}.entity_address_unified (
        location_key, npi, address_key, premise_key,
        address_source_mask, address_sources, source_count, source_mask,
        type, checksum, first_line, city_name, state_name, state_code,
        postal_code, zip5, country_code, address_precision, lat, long
    ) VALUES
        (
            'exact-geocoded', :exact_geo_npi,
            md5('exact-geocoded-address')::uuid,
            md5('exact-geocoded-premise')::uuid,
            1, ARRAY['nppes']::varchar[], 1, 1,
            'practice', 2001, 'FARTHER SYNTHETIC WAY',
            'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
            'street', 42.0, -83.1
        ),
        (
            'exact-zip', :exact_zip_npi,
            md5('exact-zip-address')::uuid,
            md5('exact-zip-premise')::uuid,
            1, ARRAY['nppes']::varchar[], 1, 1,
            'practice', 2002, 'EXACT ZIP SYNTHETIC WAY',
            'TEST CITY', 'TS', 'TS', '00001', '00001', 'US',
            'street', NULL, NULL
        )"""
_INSERT_ADDRESS_LINK_SQL = """INSERT INTO {schema}.npi_address (
        npi, address_key, type, checksum, date_added
    )
    SELECT npi, address_key, type, checksum, '2026-08-31'::date
      FROM {schema}.entity_address_unified"""


async def _prepare_rows(database, schema: str) -> tuple[int, int]:
    fixture_npis = _invalid_fixture_npis(137)
    projection_negative_npis = fixture_npis[:69]
    exact_negative_npis = fixture_npis[69:135]
    exact_npis = fixture_npis[135:]
    for statement in _CREATE_SQL:
        await database.status(statement.format(schema=schema))
    await database.status(
        _INSERT_NPI_SQL.format(schema=schema),
        projection_negative_npis=list(projection_negative_npis),
        projection_positive_npis=[*exact_negative_npis, *exact_npis],
    )
    await database.status(_INSERT_SCOPE_SQL.format(schema=schema))
    await database.status(_INSERT_TAXONOMY_SQL.format(schema=schema), exact_npis=list(exact_npis))
    await database.status(
        _INSERT_PROJECTION_NEGATIVE_SQL.format(schema=schema),
        projection_negative_npis=list(projection_negative_npis),
    )
    await database.status(
        _INSERT_EXACT_NEGATIVE_SQL.format(schema=schema),
        exact_negative_npis=list(exact_negative_npis),
    )
    await database.status(
        _INSERT_EXACT_SQL.format(schema=schema),
        exact_geo_npi=exact_npis[0],
        exact_zip_npi=exact_npis[1],
    )
    await database.status(_INSERT_ADDRESS_LINK_SQL.format(schema=schema))
    return exact_npis


def _request_by_field() -> dict[str, object]:
    return {
        "taxonomy_codes": ["207Q00000X"],
        "zip5": "00001",
        "lat": 42.0,
        "long": -83.0,
        "radius_miles": 30,
        "order_by": "distance",
        "order": "asc",
    }


def _schema_location_executor(schema: str):
    execute_location_sql = serving._execute_membership_location_sql

    async def execute(session, query_context, location_sql, *, offset):
        return await execute_location_sql(
            session,
            query_context,
            _schema_sql(location_sql, schema),
            offset=offset,
        )

    return execute


def _recording_location_reader(location_probes: list[tuple[int, list[dict]]]):
    membership_location_rows = serving._membership_location_rows

    async def read(*args, limit, **kwargs):
        assert kwargs["candidate_npis"] is None
        assert kwargs["coarse_taxonomy_knn"] is True
        locations = await membership_location_rows(*args, limit=limit, **kwargs)
        location_probes.append((limit, locations))
        return locations

    return read


def _recording_v4_reader(membership_probes):
    async def read(_session, _tables, npis, **_kwargs):
        normalized_npis = tuple(int(npi) for npi in npis)
        if normalized_npis:
            membership_probes.append(normalized_npis)
        return {npi: (10, 20) for npi in normalized_npis}

    return read


def _recording_forward_reader(code_scope_probes):
    async def read(
        _session,
        _tables,
        code_rows,
        *,
        provider_set_keys,
        scan_budget,
    ):
        del scan_budget
        code_scope_probes.append((tuple(row["code_key"] for row in code_rows), provider_set_keys))
        return [SimpleNamespace(provider_set_key=10)]

    return read


def _assert_scan_result(exact_npis, location_probes, candidates) -> None:
    assert [limit for limit, _locations in location_probes] == [1, 4]
    first_location = location_probes[0][1][0]
    assert first_location["npi"] is None
    assert first_location["_ptg_source_exhausted"] is False
    assert first_location["_ptg_probe_empty"] is True
    assert [location["npi"] for location in candidates.location_rows] == list(exact_npis)
    assert candidates.location_rows[0]["distance_miles"] is not None
    assert candidates.location_rows[1]["distance_miles"] is None
    assert all(location["_ptg_source_exhausted"] for location in candidates.location_rows)
    assert candidates.provider_set_keys_by_npi == {
        exact_npis[0]: {10},
        exact_npis[1]: {10},
    }
    assert candidates.taxonomy_filtered is True


def _configure_serving(
    monkeypatch,
    schema: str,
    location_probes,
    membership_probes,
    code_scope_probes,
) -> AsyncMock:
    """Isolate real KNN/ZIP SQL from graph and optional-index storage."""

    provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
    unavailable_index = AsyncMock(return_value=None)
    monkeypatch.setattr(ptg_membership_reader, "indexed_memberships", unavailable_index)
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
    monkeypatch.setattr(serving, "_MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT", 1)
    monkeypatch.setattr(
        serving,
        "_ptg2_npi_scope_table",
        lambda *_args, **_kwargs: f"{schema}.ptg2_v3_npi_scope",
    )
    monkeypatch.setattr(
        serving,
        "_membership_address_table_for_request",
        AsyncMock(return_value=f"{schema}.entity_address_unified"),
    )
    monkeypatch.setattr(
        serving,
        "_execute_membership_location_sql",
        _schema_location_executor(schema),
    )
    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        _recording_location_reader(location_probes),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_code_scope_rows",
        AsyncMock(return_value=[{"code_key": 7}]),
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=SimpleNamespace(representation="pattern_v1")),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", _recording_v4_reader(membership_probes))
    monkeypatch.setattr(
        serving,
        "_shared_forward_entries_for_code_rows",
        _recording_forward_reader(code_scope_probes),
    )
    monkeypatch.setattr(serving, "_graph_location_probe_batch_size", lambda *_args, **_kwargs: 1)
    return unavailable_index


@pytest.mark.asyncio
async def test_coarse_taxonomy_knn_executes_prefix_and_zip_recovery(monkeypatch):
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        exact_npis = await _prepare_rows(database, schema)
        location_probes = []
        membership_probes = []
        code_scope_probes = []
        unavailable_index = _configure_serving(
            monkeypatch,
            schema,
            location_probes,
            membership_probes,
            code_scope_probes,
        )
        request_by_field = _request_by_field()
        tables = _local_distance_tables()
        async with database.transaction() as session:
            candidates = await serving._local_inferred_distance_graph_candidates(
                session,
                tables,
                request_by_field,
                plan_id="synthetic-plan",
                requested_code="73721",
                requested_system="CPT",
                candidate_limit=2,
            )

    assert candidates is not None
    unavailable_index.assert_awaited_once()
    assert unavailable_index.await_args.args[1] is tables
    assert tuple(unavailable_index.await_args.args[2]) == tuple(exact_npis)
    assert unavailable_index.await_args.args[3].code_system == "CPT"
    assert unavailable_index.await_args.args[3].code == "73721"
    assert membership_probes == [tuple(exact_npis)]
    assert code_scope_probes == [((7,), (10, 20))]
    _assert_scan_result(exact_npis, location_probes, candidates)


async def _prepare_projection_probe(database, schema):
    await database.status(_CREATE_SQL[0].format(schema=schema))
    await database.status(
        f"""INSERT INTO {schema}.npi VALUES
                (101, 1, ARRAY['207Q00000X']::varchar[]),
                (102, 1, ARRAY['207Q00000X', '207Q00000X', NULL]::varchar[]),
                (103, 1, ARRAY['207X00000X']::varchar[]),
                (104, 1, ARRAY[]::varchar[]),
                (105, 1, ARRAY[NULL]::varchar[]);
            """
    )
    await database.status(
        f"""INSERT INTO {schema}.npi
                SELECT npi, 1, ARRAY['207Q00000X']::varchar[]
                FROM generate_series(1000, 21000) npi"""
    )
    await database.status(
        f"""CREATE TABLE {schema}.nucc_taxonomy (
                code varchar, classification varchar, specialization varchar
            )"""
    )
    await database.status(
        f"""INSERT INTO {schema}.nucc_taxonomy VALUES
                ('207Q00000X', 'Fixture specialty', NULL),
                ('207X00000X', 'Fixture specialty', 'Subspecialty')"""
    )
    await database.status(
        f"""INSERT INTO {schema}.entity_address_unified(location_key, npi) VALUES
                ('a', 101), ('b', 101), ('c', 102), ('d', 103),
                ('empty', 104), ('null-codes', 105),
                ('missing', 999999), ('null-npi', NULL)"""
    )
    await database.status(f"ANALYZE {schema}.npi")


@pytest.mark.asyncio
async def test_knn_projection_probes_local_npis_without_global_materialization(monkeypatch):
    """Keep projection results equal while probing local NPIs through an index."""
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    async with _temporary_schema() as (database, schema):
        await _prepare_projection_probe(database, schema)
        cases = (
            ({"taxonomy_codes": ["207Q00000X"]}, ["a", "b", "c"]),
            ({"taxonomy_codes": ["207Q00000X", "207X00000X"]}, ["a", "b", "c", "d"]),
            ({"classification": "Fixture specialty"}, ["a", "b", "c"]),
            ({"classification": "Fixture specialty", "include_subspecialties": True}, ["a", "b", "c", "d"]),
            ({"code": "29888", "code_system": "CPT"}, ["d"]),
            ({"taxonomy_codes": ["207Q00000X"], "code": "29888", "code_system": "CPT"}, []),
        )
        for request, expected in cases:
            parameters_by_name = {}
            projection = serving._membership_scope_projection_sql(request, parameters_by_name)
            old = (
                f"addr.npi IN (SELECT scope_provider.npi FROM {serving.PTG2_SCHEMA}.npi "
                f"scope_provider WHERE {projection})"
            )
            candidate_parameters_by_name = {}
            candidate = serving._membership_projection_knn_prefilter_sql(
                request, candidate_parameters_by_name, "distance"
            )
            assert candidate_parameters_by_name == parameters_by_name
            for predicate in (old, candidate):
                sql = _schema_sql(
                    f"SELECT location_key FROM {serving.PTG2_SCHEMA}.entity_address_unified addr "
                    f"WHERE {predicate} ORDER BY location_key",
                    schema,
                )
                location_rows = await database.all(sql, **parameters_by_name)
                assert [location[0] for location in location_rows] == expected, request
        plan = await database.scalar(f"EXPLAIN (FORMAT JSON) {sql}", **parameters_by_name)
        if isinstance(plan, str):
            plan = json.loads(plan)
        pending_nodes = [plan[0]["Plan"]]
        nodes = []
        while pending_nodes:
            node = pending_nodes.pop()
            nodes.append(node)
            pending_nodes.extend(node.get("Plans", ()))
        assert any(
            node.get("Relation Name") == "npi"
            and node.get("Node Type") in {"Index Scan", "Index Only Scan"}
            and "addr.npi" in node.get("Index Cond", "")
            for node in nodes
        )


def _projection_predicate_pair(request):
    parameters_by_name = {}
    projection = serving._membership_scope_projection_sql(request, parameters_by_name)
    old = f"addr.npi IN (SELECT scope_provider.npi FROM {serving.PTG2_SCHEMA}.npi scope_provider WHERE {projection})"
    candidate_parameters_by_name = {}
    candidate = serving._membership_projection_knn_prefilter_sql(request, candidate_parameters_by_name, "distance")
    assert candidate_parameters_by_name == parameters_by_name
    return parameters_by_name, old, candidate


@asynccontextmanager
async def _projection_reader(database, schema, monkeypatch):
    reader = "knn_projection_reader_" + uuid4().hex
    password = uuid4().hex
    reader_database = Database()
    is_reader_created = False
    try:
        await database.status(
            f"""CREATE ROLE "{reader}" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB
                NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"""
        )
        is_reader_created = True
        await database.status(f'GRANT USAGE ON SCHEMA {schema} TO "{reader}"')
        await database.status(f'GRANT SELECT ON {schema}.npi, {schema}.entity_address_unified TO "{reader}"')
        monkeypatch.setenv("HLTHPRT_DB_READER_USER", reader)
        monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", password)
        yield reader_database, reader
    finally:
        try:
            await reader_database.disconnect()
        finally:
            if is_reader_created:
                await database.status(f"DROP POLICY IF EXISTS projection_visibility ON {schema}.npi")
                await database.status(f'REVOKE SELECT ON {schema}.npi, {schema}.entity_address_unified FROM "{reader}"')
                await database.status(f'REVOKE USAGE ON SCHEMA {schema} FROM "{reader}"')
                await database.status(f'DROP ROLE "{reader}"')
                assert not await database.scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :reader)", reader=reader
                )


async def _assert_projection_reader(session, schema, reader):
    assert session.info["api_reader_verified"] is True
    identity_fields = tuple(
        (
            await session.execute(
                text(
                    "SELECT session_user, current_user, rolsuper, rolbypassrls, "
                    "current_setting('transaction_read_only'), current_setting('transaction_isolation') "
                    "FROM pg_roles WHERE rolname = current_user"
                )
            )
        ).one()
    )
    assert identity_fields == (reader, reader, False, False, "on", "repeatable read")
    assert await session.scalar(
        text("SELECT row_security_active(CAST(:relation AS regclass))"), {"relation": f"{schema}.npi"}
    )
    for child in ("npi_low", "npi_high"):
        assert not await session.scalar(
            text("SELECT has_table_privilege(:relation, 'SELECT')"), {"relation": f"{schema}.{child}"}
        )


@pytest.mark.asyncio
async def test_knn_projection_partition_parent_preserves_reader_rls(monkeypatch):
    """Keep parent RLS filtering and child isolation for an admitted Reader."""
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
        await database.status(_CREATE_SQL[0].format(schema=schema) + " PARTITION BY RANGE (npi)")
        await database.status(
            f"CREATE TABLE {schema}.npi_low PARTITION OF {schema}.npi FOR VALUES FROM (MINVALUE) TO (200)"
        )
        await database.status(
            f"CREATE TABLE {schema}.npi_high PARTITION OF {schema}.npi FOR VALUES FROM (200) TO (MAXVALUE)"
        )
        await database.status(
            f"""INSERT INTO {schema}.npi VALUES
                (101, 1, ARRAY['207Q00000X']::varchar[]),
                (102, 1, ARRAY['207Q00000X']::varchar[]),
                (201, 1, ARRAY['207Q00000X', '207Q00000X', NULL]::varchar[]),
                (202, 1, ARRAY['207X00000X']::varchar[]),
                (203, 1, ARRAY[]::varchar[])"""
        )
        await database.status(
            f"""INSERT INTO {schema}.entity_address_unified (location_key, npi) VALUES
                ('a', 101), ('b', 101), ('hidden', 102), ('other-partition', 201),
                ('nonmatching', 202), ('empty', 203), ('missing', 999), ('null-npi', NULL)"""
        )
        parameters, old, candidate = _projection_predicate_pair({"taxonomy_codes": ["207Q00000X"]})
        sql = f"SELECT location_key FROM {schema}.entity_address_unified addr WHERE {{predicate}} ORDER BY location_key"
        assert [location[0] for location in await database.all(sql.format(predicate=old), **parameters)] == [
            "a",
            "b",
            "hidden",
            "other-partition",
        ]
        async with _projection_reader(database, schema, monkeypatch) as (reader_database, reader):
            await database.status(f"ALTER TABLE {schema}.npi ENABLE ROW LEVEL SECURITY")
            await database.status(
                f'CREATE POLICY projection_visibility ON {schema}.npi FOR SELECT TO "{reader}" USING (npi <> 102)'
            )
            async with reader_database.reader_session(independent=True) as session:
                await _assert_projection_reader(session, schema, reader)
                for predicate in (old, candidate):
                    location_result = await session.execute(text(sql.format(predicate=predicate)), parameters)
                    assert list(location_result.scalars()) == ["a", "b", "other-partition"]


@pytest.mark.asyncio
@pytest.mark.parametrize("broad_scope", [False, True], ids=["sparse", "broad"])
async def test_knn_projection_preserves_complete_prefix_and_zip_rows(monkeypatch, broad_scope):
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    async with _temporary_schema() as (database, schema):
        await _insert_spatial_reference_rows(database, schema)
        exact_npis = await _prepare_rows(database, schema)
        if broad_scope:
            extra_npis = _invalid_fixture_npis(serving._MEMBERSHIP_KNN_SPARSE_SCOPE_LIMIT + 2)[137:]
            await database.status(
                f"INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi) "
                "SELECT 41, npi FROM unnest(CAST(:npis AS bigint[])) npi",
                npis=list(extra_npis),
            )
        scope_count = await database.scalar(f"SELECT count(*) FROM {schema}.ptg2_v3_npi_scope")
        assert (scope_count > serving._MEMBERSHIP_KNN_SPARSE_SCOPE_LIMIT) is broad_scope
        monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
        monkeypatch.setattr(
            serving,
            "_membership_address_table_for_request",
            AsyncMock(return_value=f"{schema}.entity_address_unified"),
        )
        monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda *_args, **_kwargs: f"{schema}.ptg2_v3_npi_scope")
        request = _request_by_field()
        _, old, candidate = _projection_predicate_pair(request)
        async with database.transaction() as session:
            for limit in (1, 4):
                query = await serving._membership_location_query(
                    session,
                    _local_distance_tables(),
                    request,
                    candidate_npis=None,
                    coarse_taxonomy_knn=True,
                    limit=limit,
                    offset=0,
                )
                assert query is not None and query.knn_prefilter_sql == candidate
                candidate_sql = _schema_sql(serving._membership_location_sql(query, limit=limit, offset=0), schema)
                assert candidate_sql.count(candidate) == 3
                old_sql = candidate_sql.replace(candidate, old)
                old_rows = await serving._execute_membership_location_sql(session, query, old_sql, offset=0)
                candidate_rows = await serving._execute_membership_location_sql(session, query, candidate_sql, offset=0)
                assert candidate_rows == old_rows
                if limit == 1:
                    assert len(candidate_rows) == 1 and candidate_rows[0]["npi"] is None
                    assert candidate_rows[0]["_ptg_probe_empty"] is True
                    assert candidate_rows[0]["_ptg_source_exhausted"] is False
                else:
                    assert [location["npi"] for location in candidate_rows] == list(exact_npis)
                    assert [json.loads(location["address_payload"])["location_key"] for location in candidate_rows] == [
                        "exact-geocoded",
                        "exact-zip",
                    ]
                    assert candidate_rows[0]["distance_miles"] is not None
                    assert candidate_rows[1]["distance_miles"] is None
                    assert all(location["_ptg_source_exhausted"] for location in candidate_rows)
                    assert not any(location["_ptg_probe_empty"] for location in candidate_rows)
