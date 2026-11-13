# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build, serve, replace and roll back the all-address geo search projection on PostgreSQL."""

import asyncio
import contextlib
import os
import time
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from api.provider_geo_search import ProviderGeoSearch, current_provider_geo_search
from api.provider_geo_sql import NearbySqlQuery, build_native_nearby_count_sql, build_native_nearby_sql
from process import provider_geo_search as builder

pytestmark = pytest.mark.provider_geo_search

_CHICAGO = {"in_long": -87.6298, "in_lat": 41.8781}
_TYPE_CLAUSE = "AND a.type IN ('primary', 'secondary', 'practice', 'site')"
_PRECISION_CLAUSE = "AND COALESCE(a.address_precision, '') <> 'city_zip'"
_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS intarray;
CREATE SCHEMA mrf;
CREATE ROLE {reader} NOLOGIN;
CREATE TABLE mrf.entity_address_unified (
    location_key varchar(64) PRIMARY KEY, npi bigint, address_key uuid, lat numeric(11,8), long numeric(11,8),
    state_name varchar, type varchar(32), address_precision varchar(32), taxonomy_array integer[], zip5 varchar(5));
CREATE TABLE mrf.npi (npi bigint PRIMARY KEY, entity_type_code integer, provider_last_name varchar);
CREATE TABLE mrf.npi_taxonomy (npi bigint, healthcare_provider_taxonomy_code varchar,
    healthcare_provider_primary_taxonomy_switch varchar);
CREATE TABLE mrf.nucc_taxonomy (code varchar PRIMARY KEY, int_code integer, classification varchar, display_name varchar);
CREATE TABLE mrf.entity_address_result_generation (local_lineage_id uuid, local_generation bigint,
    origin_lineage_id uuid, origin_generation bigint);
CREATE TABLE mrf.npi_result_generation (local_lineage_id uuid, local_generation bigint,
    origin_lineage_id uuid, origin_generation bigint);
GRANT USAGE ON SCHEMA mrf TO {reader};
GRANT SELECT ON mrf.entity_address_unified TO {reader};
INSERT INTO mrf.nucc_taxonomy VALUES ('207Q00000X', 1, 'Family Medicine', 'Family'),
    ('208D00000X', 2, 'General Practice', 'General'), ('363L00000X', 3, 'Nurse Practitioner', 'NP');
INSERT INTO mrf.npi SELECT n, 1, 'Provider ' || n FROM generate_series(1, 40) AS n;
INSERT INTO mrf.npi_taxonomy SELECT n, CASE WHEN n % 2 = 0 THEN '207Q00000X' ELSE '363L00000X' END, 'Y'
    FROM generate_series(1, 40) AS n;
INSERT INTO mrf.npi_taxonomy SELECT n, '208D00000X', 'N' FROM generate_series(1, 40) AS n;
INSERT INTO mrf.entity_address_result_generation VALUES ('{lineage}', 1, '{lineage}', 1);
INSERT INTO mrf.npi_result_generation VALUES ('{lineage}', 1, '{lineage}', 1);
"""


def _address_rows():
    """Providers on a grid around Chicago, with duplicate-type rows, excluded rows and differing taxonomy."""
    rows = []
    for npi_value in range(1, 46):
        address_key = UUID(int=npi_value)
        lat = 41.8781 + (npi_value % 9) * 0.03
        long = -87.6298 + (npi_value // 9) * 0.04
        taxonomy = [1] if npi_value % 2 == 0 else [3]
        rows.append((f"loc-{npi_value}-a", npi_value, address_key, lat, long, "IL", "practice", None, taxonomy))
        if npi_value % 3 == 0:
            # Same identity and point: the projection keeps one row with the union of both taxonomy arrays.
            rows.append((f"loc-{npi_value}-b", npi_value, address_key, lat, long, "IL", "primary", None, [2]))
        if npi_value % 5 == 0:
            rows.append(
                (f"loc-{npi_value}-c", npi_value, UUID(int=1000 + npi_value), lat, long, "IL", "site", "city_zip", [1])
            )
    # One provider address recorded at two points in two states: counted once, found from either point.
    rows.append(("loc-8-other-state", 8, UUID(int=8), 41.70, -87.52, "IN", "practice", None, [1]))
    rows.append(("loc-null-npi", None, UUID(int=999), 41.88, -87.63, "IL", "primary", None, [1]))
    rows.append(("loc-mailing", 7, UUID(int=777), 41.88, -87.63, "IL", "mailing", None, [1]))
    return rows


@pytest.fixture
async def geo_db():
    admin_dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not admin_dsn:
        pytest.skip("provider geo search proof requires NETWORK_REGISTRY_TEST_DSN")
    admin_url = make_url(admin_dsn)
    assert admin_url.host in {"127.0.0.1", "localhost", "postgres"}
    database = "provider_geo_search_" + uuid4().hex
    reader = "provider_geo_reader_" + uuid4().hex[:12]
    native_admin_dsn = admin_url.set(drivername="postgresql").render_as_string(hide_password=False)
    admin = await asyncpg.connect(native_admin_dsn)
    await admin.execute(f'CREATE DATABASE "{database}"')
    database_url = admin_url.set(database=database)
    native_dsn = database_url.set(drivername="postgresql").render_as_string(hide_password=False)
    connection = await asyncpg.connect(native_dsn)
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    try:
        await connection.execute(_SCHEMA_SQL.format(reader=reader, lineage=uuid4()))
        await connection.executemany(
            "INSERT INTO mrf.entity_address_unified (location_key, npi, address_key, lat, long, state_name, type, "
            "address_precision, taxonomy_array) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            _address_rows(),
        )
        # Ineligible rows (no NPI) spread the address table over many blocks, so block ranges split.
        await connection.execute(
            "INSERT INTO mrf.entity_address_unified (location_key, type, zip5) "
            "SELECT 'padding-' || n || repeat('x', 40), 'mailing', '00000' FROM generate_series(1, 3000) AS n"
        )
        yield SimpleNamespace(connection=connection, engine=engine, dsn=native_dsn, reader=reader)
    finally:
        await engine.dispose()
        await connection.close()
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        await admin.execute(f'DROP ROLE IF EXISTS "{reader}"')
        await admin.close()


class _ReadConnection:
    """The API's `connection.all(statement, **parameters)` shape over one SQLAlchemy connection."""

    def __init__(self, connection):
        self.connection = connection

    async def all(self, statement, **parameters):
        return (await self.connection.execute(statement, parameters)).all()


def _query(search: ProviderGeoSearch | None, **options) -> NearbySqlQuery:
    return NearbySqlQuery(
        taxonomy_conditions=options.get("taxonomy_conditions", "1=1"),
        extra_clause=options.get("extra_clause", ""),
        ilike_clause="",
        use_taxonomy_filter="taxonomy_conditions" in options,
        primary_only=options.get("primary_only", False),
        address_table_sql="mrf.entity_address_unified",
        geo_precision_clause=_PRECISION_CLAUSE,
        geo_type_clause=_TYPE_CLAUSE,
        search_table_sql=search.table_sql if search else None,
        search_provider_facts=search is not None,
    )


async def _lookup(read):
    return await current_provider_geo_search(read, "mrf", include_service_locations=True)


async def _answers(read, search, parameters, **options):
    nearby = _query(search, **options)
    count = (await read.all(text(build_native_nearby_count_sql(nearby)), **parameters))[0][0]
    page = await read.all(text(build_native_nearby_sql(nearby, use_search_page=True)), limit=200, **parameters)
    shown_by_identity = {}
    for page_row in page:
        mapping = page_row._mapping
        shown_by_identity.setdefault(
            (mapping["npi_code"], mapping["address_key"]), (mapping["location_key"], mapping["type"])
        )
    return count, list(shown_by_identity.items())


@pytest.mark.asyncio
async def test_projection_answers_exactly_like_the_address_table(geo_db):
    first_build = await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    assert first_build["status"] == "published" and first_build["generation"] == 1
    assert await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True) == {
        "status": "current",
        "include_service_locations": True,
    }
    has_reader_grant = await geo_db.connection.fetchval(
        "SELECT has_table_privilege($1, 'mrf.provider_geo_search_g1', 'SELECT')", geo_db.reader
    )
    assert has_reader_grant is True
    async with geo_db.engine.connect() as connection:
        read = _ReadConnection(connection)
        search = await _lookup(read)
        assert search == ProviderGeoSearch('"mrf".provider_geo_search_g1')
        shared_rows = await geo_db.connection.fetch(
            "SELECT npi, state_name FROM mrf.provider_geo_search_g1 WHERE shares_identity ORDER BY state_name"
        )
        shared_points = [(shared_row["npi"], shared_row["state_name"]) for shared_row in shared_rows]
        # Provider 8 has a point in two states; provider 3 has two taxonomy sets at one point.
        assert {(8, "IL"), (8, "IN")} <= set(shared_points) and shared_points.count((3, "IL")) == 2
        cases = (
            ({"radius": 10.0}, {}),
            ({"radius": 3.0}, {}),
            (
                {"radius": 10.0, "classification": "Family Medicine"},
                {"taxonomy_conditions": "classification = :classification"},
            ),
            (
                {"radius": 10.0, "classification": "General Practice"},
                {"taxonomy_conditions": "classification = :classification"},
            ),
            (
                {"radius": 10.0, "classification": "Family Medicine"},
                {"taxonomy_conditions": "classification = :classification", "primary_only": True},
            ),
            (
                {"radius": 10.0, "classification": "General Practice"},
                {"taxonomy_conditions": "classification = :classification", "primary_only": True},
            ),
            ({"radius": 10.0, "state": "IL"}, {"extra_clause": "\n AND a.state_name = :state"}),
        )
        for case_parameters, options in cases:
            parameters_by_name = {**_CHICAGO, **case_parameters}
            live_answers = await _answers(read, None, parameters_by_name, **options)
            projected_answers = await _answers(read, search, parameters_by_name, **options)
            assert projected_answers == live_answers, (parameters_by_name, options)
            assert live_answers[0] > 0 or "General Practice" in str(parameters_by_name)


@pytest.mark.asyncio
async def test_new_inputs_need_a_new_generation_and_two_are_kept(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    async with geo_db.engine.connect() as connection:
        read = _ReadConnection(connection)
        await geo_db.connection.execute("UPDATE mrf.npi_result_generation SET local_generation = local_generation + 1")
        await connection.commit()
        assert await _lookup(read) is None
        assert (await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))[
            "generation"
        ] == 2
        await connection.commit()
        assert (await _lookup(read)).table_sql.endswith("provider_geo_search_g2")
        assert await current_provider_geo_search(read, "mrf", include_service_locations=False) is None

        await geo_db.connection.execute("UPDATE mrf.entity_address_result_generation SET origin_generation = 2")
        assert (await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))[
            "generation"
        ] == 3
    # The generation that stopped serving one publish ago stays until the next call retires it.
    assert await geo_db.connection.fetchval("SELECT to_regclass('mrf.provider_geo_search_g1')") is not None
    assert (await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))[
        "status"
    ] == "current"
    tables = await geo_db.connection.fetch(
        "SELECT table_name, is_current FROM mrf.provider_geo_search_state ORDER BY generation"
    )
    assert [(state_row["table_name"], state_row["is_current"]) for state_row in tables] == [
        ("provider_geo_search_g2", False),
        ("provider_geo_search_g3", True),
    ]
    assert await geo_db.connection.fetchval("SELECT to_regclass('mrf.provider_geo_search_g1')") is None


@pytest.mark.asyncio
async def test_rollback_serves_the_previous_generation_when_its_inputs_return(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    await geo_db.connection.execute("UPDATE mrf.entity_address_result_generation SET local_generation = 2")
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    await geo_db.connection.execute("UPDATE mrf.entity_address_result_generation SET local_generation = 1")
    assert await builder.rollback_provider_geo_search(geo_db.connection) == {
        "status": "rolled_back",
        "from": 2,
        "to": 1,
    }
    async with geo_db.engine.connect() as connection:
        assert (await _lookup(_ReadConnection(connection))).table_sql.endswith("provider_geo_search_g1")


@pytest.mark.asyncio
async def test_failed_build_leaves_the_current_generation_serving(geo_db, monkeypatch):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    await geo_db.connection.execute("UPDATE mrf.npi_result_generation SET local_generation = 5")
    original = builder._projection_sql
    monkeypatch.setattr(builder, "_projection_sql", lambda *_args, **_kwargs: "SELECT 1 / 0")
    with pytest.raises(asyncpg.DivisionByZeroError):
        await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    state = await geo_db.connection.fetch("SELECT generation, is_current FROM mrf.provider_geo_search_state")
    assert [(row["generation"], row["is_current"]) for row in state] == [(1, True)]
    leftovers = await geo_db.connection.fetchval(
        "SELECT count(*) FROM pg_class WHERE relname LIKE 'provider\\_geo\\_search\\_g2%'"
    )
    assert leftovers == 0
    assert await geo_db.connection.fetchval("SELECT pg_try_advisory_lock($1)", builder._BUILD_LOCK_ID) is True
    monkeypatch.setattr(builder, "_projection_sql", original)
    assert (await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))[
        "generation"
    ] == 2


@pytest.mark.asyncio
async def test_restored_input_without_published_origin_is_never_served(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    await geo_db.connection.execute(
        "UPDATE mrf.entity_address_result_generation SET origin_lineage_id = NULL, origin_generation = NULL"
    )
    async with geo_db.engine.connect() as connection:
        assert await _lookup(_ReadConnection(connection)) is None
    with pytest.raises(builder.ProviderGeoSearchError, match="must be published"):
        await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)


@pytest.mark.asyncio
async def test_input_published_during_the_load_is_loaded_again(geo_db, monkeypatch):
    original = builder._load_rows
    loads = []

    async def load_then_publish(connection, *args, **kwargs):
        await original(connection, *args, **kwargs)
        loads.append(True)
        if len(loads) == 1:
            await connection.execute("UPDATE mrf.entity_address_result_generation SET local_generation = 7")

    monkeypatch.setattr(builder, "_BATCH_START_BLOCKS", 1)
    monkeypatch.setattr(builder, "_load_rows", load_then_publish)
    published = await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    assert published["generation"] == 1 and len(loads) == 2
    current_input = await geo_db.connection.fetchval(
        "SELECT address_input->>1 FROM mrf.provider_geo_search_state WHERE is_current"
    )
    assert current_input == "7"
    async with geo_db.engine.connect() as connection:
        read = _ReadConnection(connection)
        search = await _lookup(read)
        parameters_by_name = {**_CHICAGO, "radius": 10.0}
        assert await _answers(read, search, parameters_by_name) == await _answers(read, None, parameters_by_name)


@pytest.mark.asyncio
async def test_indexing_holds_no_lock_on_the_live_inputs(geo_db, monkeypatch):
    original = builder._finish_generation
    held_locks = []

    async def finish_after_checking_locks(connection, *args, **kwargs):
        builder_pid = await connection.fetchval("SELECT pg_backend_pid()")
        observer = await asyncpg.connect(geo_db.dsn)
        try:
            held_locks.append(
                await observer.fetchval(
                    "SELECT count(*) FROM pg_locks AS l JOIN pg_class AS c ON c.oid = l.relation "
                    "WHERE l.pid = $1 AND c.relname IN ('entity_address_unified', 'npi', 'npi_taxonomy')",
                    builder_pid,
                )
            )
        finally:
            await observer.close()
        return await original(connection, *args, **kwargs)

    monkeypatch.setattr(builder, "_finish_generation", finish_after_checking_locks)
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    assert held_locks == [0]


@pytest.mark.asyncio
async def test_publication_does_not_wait_for_or_break_an_open_read(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    parameters_by_name = {**_CHICAGO, "radius": 10.0}
    async with geo_db.engine.connect() as connection:
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        read = _ReadConnection(connection)
        search = await _lookup(read)
        answers_before = await _answers(read, search, parameters_by_name)

        writer = await asyncpg.connect(geo_db.dsn)
        try:
            await writer.execute("UPDATE mrf.entity_address_result_generation SET local_generation = 9")
            published = await asyncio.wait_for(
                builder.rebuild_provider_geo_search(writer, uses_service_locations=True), timeout=60
            )
        finally:
            await writer.close()
        assert published["generation"] == 2
        assert await _lookup(read) == search
        assert await _answers(read, search, parameters_by_name) == answers_before
    async with geo_db.engine.connect() as connection:
        read = _ReadConnection(connection)
        assert (await _lookup(read)).table_sql.endswith("provider_geo_search_g2")
        assert await _answers(read, await _lookup(read), parameters_by_name) == answers_before


@pytest.mark.asyncio
async def test_a_read_keeps_its_generation_through_two_later_publications(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    parameters_by_name = {**_CHICAGO, "radius": 10.0}
    async with geo_db.engine.connect() as connection:
        read = _ReadConnection(connection)
        search = await _lookup(read)
        assert search.table_sql.endswith("provider_geo_search_g1")
        for generation in (2, 3):
            await geo_db.connection.execute(
                "UPDATE mrf.entity_address_result_generation SET local_generation = $1", generation
            )
            published = await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
            assert published["generation"] == generation
        # A third call would retire g1, but this open read holds the shared pin, so retirement waits and skips it.
        assert (await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))[
            "status"
        ] == "current"
        # The first projection read comes only now; its table is still there.
        assert (await _answers(read, search, parameters_by_name))[0] > 0
    assert await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    assert await geo_db.connection.fetchval("SELECT to_regclass('mrf.provider_geo_search_g1')") is None


@pytest.mark.asyncio
async def test_a_cutover_is_never_starved_by_a_slow_batch(geo_db, monkeypatch):
    original = builder._block_range
    widths = []

    def slow_when_wide(alias, lower, upper):
        widths.append(upper - lower)
        delay = 1.0 if upper - lower > 1 else 0
        return f"{original(alias, lower, upper)} AND (SELECT 1 FROM pg_sleep({delay})) = 1"

    class _CutoverRefused(Exception):
        pass

    async def is_cutover_admitted(connection):
        # The address publisher's production pattern: its cutover key, then NOWAIT locks; on failure it retries
        # after 25 ms, doubling to 100 ms, four attempts in all.
        for attempt in range(1, 5):
            try:
                async with connection.transaction():
                    if not await connection.fetchval(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))", "entity-address-unified:mrf"
                    ):
                        raise _CutoverRefused
                    await connection.execute("LOCK TABLE mrf.entity_address_unified IN ACCESS EXCLUSIVE MODE NOWAIT")
                return True
            except asyncpg.LockNotAvailableError, _CutoverRefused:
                await asyncio.sleep(min(25 * 2 ** (attempt - 1), 100) / 1000)
        return False

    cutover = await asyncpg.connect(geo_db.dsn)
    started = time.monotonic()
    with contextlib.suppress(asyncpg.QueryCanceledError):
        async with cutover.transaction():
            await cutover.execute("SET LOCAL statement_timeout = '15ms'")
            await cutover.execute("SELECT pg_sleep(1)")
    cancel_ms = (time.monotonic() - started) * 1000
    if cancel_ms > 25:
        await cutover.close()
        pytest.skip(f"this server cancels a 15 ms statement after {cancel_ms:.0f} ms, past the publisher's 25 ms retry")
    monkeypatch.setattr(builder, "_block_range", slow_when_wide)
    failed_cutovers = []
    build = asyncio.create_task(builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True))
    try:
        while not build.done():
            if not await is_cutover_admitted(cutover):
                failed_cutovers.append(True)
            await asyncio.sleep(0.02)
        assert (await build)["status"] == "published"
    finally:
        await cutover.close()
    assert failed_cutovers == []
    assert max(widths) > 1 and min(widths) == 1


@pytest.mark.asyncio
async def test_a_lookup_falls_back_while_a_retirement_holds_the_pin(geo_db):
    await builder.rebuild_provider_geo_search(geo_db.connection, uses_service_locations=True)
    retirer = await asyncpg.connect(geo_db.dsn)
    try:
        async with retirer.transaction():
            await retirer.execute("SELECT pg_advisory_xact_lock($1)", builder.RETIRE_LOCK_ID)
            async with geo_db.engine.connect() as connection:
                # The pin is taken before the selected table is checked, so a retirement in progress means fallback.
                assert await _lookup(_ReadConnection(connection)) is None
    finally:
        await retirer.close()
    async with geo_db.engine.connect() as connection:
        assert (await _lookup(_ReadConnection(connection))).table_sql.endswith("provider_geo_search_g1")
