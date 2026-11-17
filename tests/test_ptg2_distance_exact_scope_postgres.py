# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real-PostgreSQL proof for the bounded exact-distance taxonomy path."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api import ptg2_serving as serving
from api import ptg_membership_distance as distance_membership
from api import ptg_membership_reader
from api.billing_search_cursor import BillingSearchCursorGenerationExpired
from api.network_read_execution import has_network_read_inventory
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_coverage import (
    _invalid_fixture_npis,
)
from tests.ptg2_serving_address_evidence_postgres_geo import (
    _insert_spatial_reference_rows,
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _require_disposable_postgres,
    _schema_sql,
    _temporary_schema,
)
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables
from tests.test_tiger_snapshot_inheritance_postgres import _owned_database


async def _create_distance_input_generations(session, roles, *, inherited):
    """Install synthetic native headers and genuine protected read-only leaves."""
    npi = distance_membership.npi_generation
    address = distance_membership.address_generation
    reference = distance_membership.reference_generation
    names_by_family = {
        "npi": npi.RELATION_NAMES,
        "address": address.RELATION_NAMES,
        **{
            family: reference.RELATION_NAMES_BY_IMPORTER[family]
            for family in ("cms-doctors", "mrf-address", "geo", "nucc")
        },
    }
    await session.execute(text("CREATE SCHEMA mrf"))
    await session.execute(text("CREATE SCHEMA tiger"))
    tables = set(distance_membership._DISTANCE_DATA_NAMES) | {
        name for names in names_by_family.values() for name in names
    }
    for name in sorted(tables):
        await session.execute(text(f'CREATE TABLE mrf."{name}" (record_key integer)'))
    for name in ("zip_state", "zcta5"):
        await session.execute(text(f'CREATE TABLE tiger."{name}" (record_key integer)'))
        if not inherited:
            await session.execute(text(f'INSERT INTO tiger."{name}" VALUES (1)'))
    columns = (
        "local_lineage_id uuid NOT NULL,local_generation bigint NOT NULL,origin_lineage_id uuid,"
        "origin_generation bigint,published_at timestamptz,relation_oids bigint[]"
    )
    for table in (npi.TABLE_NAME, address.TABLE_NAME):
        await session.execute(text(f"CREATE TABLE mrf.{table} (singleton boolean PRIMARY KEY,{columns})"))
    await session.execute(
        text(
            "ALTER TABLE mrf.npi_result_generation ADD COLUMN canonical_publication_ref text,"
            "ADD COLUMN canonical_publication_generation bigint,ADD COLUMN canonical_chain_ref text,"
            "ADD COLUMN canonical_import_date date"
        )
    )
    await session.execute(text(f"CREATE TABLE mrf.{reference.TABLE_NAME} (importer_id text PRIMARY KEY,{columns})"))
    names_by_family["tiger"] = ("zip_state", "zcta5")
    for family, names in names_by_family.items():
        await _insert_distance_generation_header(session, family, names)
    await session.execute(text(f'GRANT USAGE ON SCHEMA mrf,tiger TO "{roles[2]}"'))
    await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf,tiger TO "{roles[2]}"'))
    if inherited:
        await _attach_distance_tiger_leaves(session, roles)
        await session.execute(
            text(
                "UPDATE mrf.reference_family_result_generation SET origin_lineage_id=NULL,"
                "origin_generation=NULL,published_at=NULL,relation_oids=NULL WHERE importer_id='tiger'"
            )
        )


async def _insert_distance_generation_header(session, family, names):
    """Attach a real synthetic native identity to every fixture family member."""
    namespace = "tiger" if family == "tiger" else "mrf"
    oids = [
        (
            await session.execute(text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"{namespace}.{name}"})
        ).scalar_one()
        for name in names
    ]
    table = {"npi": "npi_result_generation", "address": "entity_address_result_generation"}.get(
        family, "reference_family_result_generation"
    )
    key = "singleton" if family in {"npi", "address"} else "importer_id"
    await session.execute(
        text(
            f"INSERT INTO mrf.{table} ({key},local_lineage_id,local_generation,origin_lineage_id,"
            "origin_generation,published_at,relation_oids) VALUES (:key,:lineage,1,:lineage,1,"
            "'2026-01-01T00:00:00Z',CAST(:oids AS bigint[]))"
        ),
        {"key": True if key == "singleton" else family, "lineage": uuid4(), "oids": oids},
    )


async def _attach_distance_tiger_leaves(session, roles):
    namespace = "reference_family_archive_" + uuid4().hex
    await session.execute(text(f'CREATE SCHEMA "{namespace}" AUTHORIZATION "{roles[0]}"'))
    for name in ("zip_state", "zcta5"):
        await session.execute(
            text(f'CREATE TABLE "{namespace}"."{name}" (record_key integer) INHERITS (tiger."{name}")')
        )
        await session.execute(text(f'INSERT INTO "{namespace}"."{name}" VALUES (1)'))
        await session.execute(text(f'ALTER TABLE "{namespace}"."{name}" OWNER TO "{roles[0]}"'))
    await session.execute(text(f'GRANT USAGE ON SCHEMA "{namespace}" TO "{roles[2]}"'))
    await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{namespace}" TO "{roles[2]}"'))


@asynccontextmanager
async def _distance_input_database(*, inherited=False):
    _require_disposable_postgres()
    database = Database()
    await database.connect()
    try:
        async with _owned_database(source_url=database.engine.url) as (_, sessions, roles):
            async with sessions[0].begin() as admin:
                await _create_distance_input_generations(admin, roles, inherited=inherited)
            yield sessions, roles
    finally:
        await database.disconnect()


@asynccontextmanager
async def _distance_reader(sessions):
    async with sessions[2].begin() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        async with distance_membership.distance_input_snapshot(session, "mrf", {}) as identity:
            assert identity is not None
            yield session, identity
        assert not has_network_read_inventory(session)


@pytest.mark.asyncio
async def test_distance_ordinary_inheritance_keeps_legacy_capability_and_reader_scope():
    from process.reference_family_archive import ReferenceFamilyArchiveError

    async with _distance_input_database() as (sessions, _):
        async with sessions[0].begin() as admin:
            await admin.execute(text("CREATE SCHEMA ordinary_tiger"))
            for name in ("zip_state", "zcta5"):
                await admin.execute(text(f'CREATE TABLE ordinary_tiger."{name}" () INHERITS(tiger."{name}")'))
            await admin.execute(text("CREATE TABLE ordinary_tiger.zip_leaf () INHERITS(ordinary_tiger.zip_state)"))
        async with sessions[2].begin() as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with distance_membership.distance_input_snapshot(reader, "mrf", {}) as identity:
                assert identity is None
            assert not has_network_read_inventory(reader)
            with pytest.raises(ReferenceFamilyArchiveError):
                await distance_membership.current_tiger_snapshot_children(reader)


@pytest.mark.asyncio
async def test_distance_native_generation_changes_same_oid_and_restores_read_scope():
    async with _distance_input_database() as (sessions, _):
        async with _distance_reader(sessions) as (reader, before):
            with pytest.raises(DBAPIError):
                async with reader.begin_nested():
                    await reader.execute(text("SELECT * FROM missing_distance_fixture"))
            assert has_network_read_inventory(reader)
        for table, predicate in (
            ("npi_result_generation", "singleton IS TRUE"),
            ("entity_address_result_generation", "singleton IS TRUE"),
            ("reference_family_result_generation", "importer_id='mrf-address'"),
            ("reference_family_result_generation", "importer_id='cms-doctors'"),
            ("reference_family_result_generation", "importer_id='geo'"),
            ("reference_family_result_generation", "importer_id='tiger'"),
        ):
            async with sessions[0].begin() as admin:
                await admin.execute(
                    text(f"UPDATE mrf.{table} SET local_generation=local_generation+1 WHERE {predicate}")
                )
            async with _distance_reader(sessions) as (_, after):
                assert before["relations"] == after["relations"]
                assert before != after
                before = after


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["npi", "address", "mrf-address"])
async def test_published_input_drift_is_not_downgraded_when_another_generation_is_unavailable(family):
    async with _distance_input_database() as (sessions, _):
        async with sessions[0].begin() as admin:
            table = {"npi": "npi_result_generation", "address": "entity_address_result_generation"}.get(
                family, "reference_family_result_generation"
            )
            predicate = "importer_id='mrf-address'" if family == "mrf-address" else "singleton IS TRUE"
            await admin.execute(
                text(f"UPDATE mrf.{table} SET relation_oids[1]=relation_oids[1]+100000000 WHERE {predicate}")
            )
            if family in {"npi", "address"}:
                other = "entity_address_result_generation" if family == "npi" else "npi_result_generation"
                await admin.execute(
                    text(
                        f"UPDATE mrf.{other} SET origin_lineage_id=NULL,origin_generation=NULL,"
                        "published_at=NULL,relation_oids=NULL"
                    )
                )
        async with sessions[2].begin() as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            with pytest.raises(BillingSearchCursorGenerationExpired):
                async with distance_membership.distance_input_snapshot(reader, "mrf", {}):
                    pytest.fail("Published input drift must not select legacy continuation")
            assert not has_network_read_inventory(reader)


async def _install_distance_geo_state(session, reader_role):
    """Use the real stored-projection signature with retained fixture heaps."""
    await session.execute(text("CREATE SCHEMA distance_geo_held"))
    binding_by_name = {}
    for namespace, name in serving.geo_projection._PROJECTION_DEPENDENCIES:
        await session.execute(text(f'CREATE TABLE distance_geo_held."{name}" (record_key integer)'))
        oid, filenode = (
            await session.execute(
                text("SELECT to_regclass(:name)::oid::bigint,pg_relation_filenode(to_regclass(:name))::bigint"),
                {"name": f"distance_geo_held.{name}"},
            )
        ).one()
        binding_by_name[f"{namespace or 'mrf'}.{name}"] = {
            "schema_name": "distance_geo_held",
            "table_name": name,
            "relation_oid": oid,
            "relfilenode": filenode,
        }
    await session.execute(
        text(
            "CREATE TABLE mrf.entity_address_geo_assurance_state (singleton boolean PRIMARY KEY,"
            "active_geo_assurance_version integer,active_table_oid oid,active_relation_signature jsonb,"
            "active_dependency_bindings jsonb,candidate_progress integer DEFAULT 0)"
        )
    )
    signature_by_name = {
        name: [binding["relation_oid"], binding["relfilenode"]] for name, binding in binding_by_name.items()
    }
    await session.execute(
        text(
            "INSERT INTO mrf.entity_address_geo_assurance_state (singleton,active_geo_assurance_version,active_table_oid,"
            "active_relation_signature,active_dependency_bindings) VALUES (true,1,to_regclass('mrf.entity_address_unified')::oid,"
            "CAST(:signature AS jsonb),CAST(:bindings AS jsonb))"
        ),
        {"signature": json.dumps(signature_by_name), "bindings": json.dumps(binding_by_name)},
    )
    await session.execute(text(f'GRANT USAGE ON SCHEMA distance_geo_held TO "{reader_role}"'))
    await session.execute(text(f'GRANT SELECT ON mrf.entity_address_geo_assurance_state TO "{reader_role}"'))


@pytest.mark.asyncio
async def test_distance_geo_identity_ignores_candidate_progress_and_tracks_retained_heap_replacement():
    async with _distance_input_database() as (sessions, roles):
        async with sessions[0].begin() as admin:
            await _install_distance_geo_state(admin, roles[2])
        async with _distance_reader(sessions) as (_, before):
            assert before["geo"]["ready"]
        async with sessions[0].begin() as admin:
            await admin.execute(text("UPDATE mrf.entity_address_geo_assurance_state SET candidate_progress=1"))
        async with _distance_reader(sessions) as (_, same_active):
            assert before == same_active
        async with sessions[0].begin() as admin:
            await admin.execute(text("DROP TABLE distance_geo_held.npi_address"))
            await admin.execute(text("CREATE TABLE distance_geo_held.npi_address (record_key integer)"))
        async with _distance_reader(sessions) as (_, after):
            assert before["generations"] == after["generations"]
            assert before["relations"] == after["relations"]
            assert before["geo"]["bindings"] == after["geo"]["bindings"]
            assert before["geo"]["observed_signature"] != after["geo"]["observed_signature"]
            assert not after["geo"]["ready"]


@pytest.mark.asyncio
async def test_distance_inherited_tiger_pins_survive_savepoint_and_expire_after_leaf_exchange():
    async with _distance_input_database(inherited=True) as (sessions, roles):
        async with _distance_reader(sessions) as (reader, before):
            assert len(before["tiger"]) == 2
            assert "tiger" not in before["generations"]["reference"]
            with pytest.raises(DBAPIError):
                async with reader.begin_nested():
                    await reader.execute(text("SELECT * FROM missing_distance_fixture"))
            async with sessions[0].begin() as admin:
                with pytest.raises(DBAPIError) as blocked:
                    await admin.execute(text("LOCK TABLE ONLY tiger.zip_state IN ACCESS EXCLUSIVE MODE NOWAIT"))
                assert blocked.value.orig.sqlstate == "55P03"
        async with sessions[0].begin() as admin:
            await admin.execute(text("LOCK TABLE ONLY tiger.zip_state,tiger.zcta5 IN ACCESS EXCLUSIVE MODE NOWAIT"))
            for relation in before["tiger"]:
                await admin.execute(
                    text(
                        f'ALTER TABLE "{relation["schema_name"]}"."{relation["relation_name"]}" '
                        f'NO INHERIT tiger."{relation["relation_name"]}"'
                    )
                )
            await _attach_distance_tiger_leaves(admin, roles)
        async with _distance_reader(sessions) as (_, after):
            assert before["relations"] == after["relations"]
            assert before["tiger"] != after["tiger"]


@pytest.mark.asyncio
async def test_distance_input_capability_declines_absent_control_without_changing_session():
    async with _distance_input_database() as (sessions, _):
        async with sessions[0].begin() as admin:
            await admin.execute(text("DROP TABLE mrf.npi_result_generation"))
        async with sessions[2].begin() as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            async with distance_membership.distance_input_snapshot(reader, "mrf", {}) as identity:
                assert identity is None
            assert not has_network_read_inventory(reader)


@pytest.mark.asyncio
async def test_distance_tiger_exchange_before_probe_refuses_an_old_reader_snapshot():
    async with _distance_input_database(inherited=True) as (sessions, roles):
        async with sessions[2].begin() as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            old_oid_by_child = dict(await distance_membership.current_tiger_snapshot_children(reader))
            async with sessions[0].begin() as admin:
                await admin.execute(text("LOCK TABLE ONLY tiger.zip_state,tiger.zcta5 IN ACCESS EXCLUSIVE MODE NOWAIT"))
                old_rows = (
                    await admin.execute(
                        text(
                            "SELECT namespace.nspname,child.relname FROM pg_catalog.pg_class child "
                            "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=child.relnamespace "
                            "WHERE child.oid=ANY(CAST(:oids AS oid[]))"
                        ),
                        {"oids": list(old_oid_by_child.values())},
                    )
                ).all()
                for namespace, name in old_rows:
                    await admin.execute(text(f'ALTER TABLE "{namespace}"."{name}" NO INHERIT tiger."{name}"'))
                await _attach_distance_tiger_leaves(admin, roles)
            with pytest.raises(BillingSearchCursorGenerationExpired):
                async with distance_membership.distance_input_snapshot(reader, "mrf", {}):
                    pytest.fail("A changed inherited input must not return an accepted identity")
            assert not has_network_read_inventory(reader)


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_distance_input_body_failure_and_cancellation_restore_prior_inventory(error_type):
    async with _distance_input_database(inherited=True) as (sessions, _):
        async with sessions[2].begin() as reader:
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            with pytest.raises(error_type):
                async with distance_membership.distance_input_snapshot(reader, "mrf", {}) as identity:
                    assert identity is not None
                    raise error_type()
            assert not has_network_read_inventory(reader)
            assert (await reader.execute(text("SELECT 1"))).scalar_one() == 1


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

_SPARSE_NPI_BASE = 1_000_000_000
_SPARSE_PREFIX = 4975
_SPARSE_POPULATION = 5150


async def _load_sparse_distance_fixture(database, schema):
    """Populate synthetic nearest offices, a sparse code tail and one farther office."""
    await _insert_spatial_reference_rows(database, schema)
    await database.status(f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key, address_source_mask, address_sources,
            source_count, source_mask, type, checksum, first_line, city_name, state_name,
            state_code, postal_code, zip5, country_code, address_precision, lat, long
        )
        SELECT 'sparse-' || office, {_SPARSE_NPI_BASE} + provider,
               md5('sparse-address-' || office)::uuid, md5('sparse-premise-' || office)::uuid,
               1, ARRAY['nppes']::varchar[], 1, 1, 'practice', office,
               office || ' SYNTHETIC WAY', 'TEST CITY', 'TS', 'TS',
               '00001', '00001', 'US', 'street', 42.0, -83.0 + office * 0.00001
        FROM (
            SELECT ordinal AS provider, ordinal AS office FROM generate_series(1, {_SPARSE_POPULATION}) ordinal
            UNION ALL SELECT {_SPARSE_PREFIX}, {_SPARSE_POPULATION + 1}
        ) offices
    """)
    await database.status(f"""
        INSERT INTO {schema}.ptg2_v3_npi_scope (snapshot_key, npi)
        SELECT 41, {_SPARSE_NPI_BASE} + ordinal FROM generate_series(1, {_SPARSE_POPULATION}) ordinal
    """)
    await database.status(_INSERT_ADDRESS_LINK_SQL.format(schema=schema))
    await database.status(f"CREATE INDEX ON {schema}.npi_address (npi, address_key, type, checksum)")
    await database.status(f"ANALYZE {schema}.entity_address_unified")


def _configure_sparse_distance_reader(monkeypatch, schema, probe_limits):
    """Use real PostgreSQL address reads with bounded synthetic graph membership."""
    original_reader = serving._membership_distance_page

    async def read_page(*args, **options):
        if options["candidate_npis"] is None:
            probe_limits.append(options["limit"])
        return await original_reader(*args, **options)

    async def read_memberships(_session, _tables, npis, _request, _state):
        return {npi: (10 if npi < _SPARSE_NPI_BASE + 5000 or npi >= _SPARSE_NPI_BASE + 5060 else 11,) for npi in npis}

    async def classify(_session, _tables, _memberships, _request, state):
        state.code_sets.add(10)
        state.noncode_sets.add(11)

    provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
    monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda *_: f"{schema}.ptg2_v3_npi_scope")
    monkeypatch.setattr(
        serving, "_membership_address_table_for_request", AsyncMock(return_value=f"{schema}.entity_address_unified")
    )
    monkeypatch.setattr(serving, "_execute_membership_location_sql", _schema_location_executor(schema))
    monkeypatch.setattr(serving, "_membership_distance_page", read_page)
    monkeypatch.setattr(serving, "_local_v4_memberships", read_memberships)
    monkeypatch.setattr(serving, "_classify_local_code_sets", classify)


@pytest.mark.asyncio
async def test_snapshot_distance_seek_batches_sparse_tail_and_retains_next_provider(monkeypatch):
    """Continue a snapshot-wide native query past a 4,975-provider prefix."""
    async with _temporary_schema() as (database, schema):
        await _load_sparse_distance_fixture(database, schema)
        probe_limits: list[int] = []
        _configure_sparse_distance_reader(monkeypatch, schema, probe_limits)
        monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "5000")
        request_by_field = {"lat": 42.0, "long": -83.0, "radius_miles": 10, "order_by": "distance"}
        tables = _local_distance_tables()
        limits = serving._v4_geo_rate_forward_limits(tables)
        request = serving._LocalDistanceGraphRequest(26, [{"code_key": 7}], limits)
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            anchor = await serving._membership_distance_page(
                session, tables, request_by_field, candidate_npis=(_SPARSE_NPI_BASE + _SPARSE_PREFIX,), limit=1
            )
            assert anchor is not None and anchor.next_position is not None
            first = await serving._seek_local_distance_graph(
                session, tables, request_by_field, request, 416, 5000, after=anchor.next_position
            )
            assert first is not None and first.next_position is not None
            assert probe_limits == [26, 182]
            assert first.scanned_count == 175
            observed_npis = [location["npi"] - _SPARSE_NPI_BASE for location in first.candidates.location_rows]
            assert observed_npis == [*range(4976, 5000), 5060, 5061]
            second = await serving._seek_local_distance_graph(
                session, tables, request_by_field, request, 416, 5000, after=first.next_position
            )
            assert second is not None
            assert [location["npi"] - _SPARSE_NPI_BASE for location in second.candidates.location_rows] == list(
                range(5062, 5088)
            )


@pytest.mark.asyncio
async def test_sparse_first_match_beyond_5000_preserves_complete_native_traversal(monkeypatch):
    async with _temporary_schema() as (database, schema):
        await _load_sparse_distance_fixture(database, schema)
        probes = []
        _configure_sparse_distance_reader(monkeypatch, schema, probes)

        async def memberships(_session, _tables, npis, _request, _state):
            return {npi: (10 if npi >= _SPARSE_NPI_BASE + 5060 else 11,) for npi in npis}

        monkeypatch.setattr(serving, "_local_v4_memberships", memberships)
        monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "5000")
        request_by_field = {"lat": 42.0, "long": -83.0, "radius_miles": 10, "order_by": "distance"}
        tables = _local_distance_tables()
        request = serving._LocalDistanceGraphRequest(26, [{"code_key": 7}], serving._v4_geo_rate_forward_limits(tables))
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            oracle = await serving._membership_distance_page(
                session,
                tables,
                request_by_field,
                candidate_npis=tuple(range(_SPARSE_NPI_BASE + 5060, _SPARSE_NPI_BASE + _SPARSE_POPULATION + 1)),
                limit=100,
            )
            assert oracle is not None
            observed, after = [], None
            for _ in range(4):
                page = await serving._seek_local_distance_graph(
                    session,
                    tables,
                    request_by_field,
                    request,
                    416,
                    serving._graph_location_scan_limit(416),
                    after=after,
                )
                assert page is not None
                observed.extend(page.candidates.location_rows)
                after = page.next_position
                if after is None:
                    break
            assert after is None
            assert observed == oracle.rows
            assert len(observed) == 91
            assert max(probes) > 5000


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_setting", ["on", "off"])
async def test_ranked_address_planner_restores_settings_after_success_and_error(prior_setting):
    async with _temporary_schema() as (database, _schema):
        async with database.transaction() as session:
            await session.execute(
                text("SELECT set_config('enable_nestloop',:setting,true), set_config('jit',:setting,true)"),
                {"setting": prior_setting},
            )
            context = SimpleNamespace(parameter_map={}, knn_order_sql=None)
            address_rows = await serving._execute_ranked_membership_sql(
                session, context, "SELECT current_setting('enable_nestloop') AS nestloop, current_setting('jit') AS jit"
            )
            assert address_rows == [{"nestloop": "off", "jit": "off"}]
            for raises_error in (False, True):
                if raises_error:
                    await _require_ranked_sql_error(session, context)
                restored = (
                    await session.execute(text("SELECT current_setting('enable_nestloop'), current_setting('jit')"))
                ).one()
                assert tuple(restored) == (prior_setting, prior_setting)


async def _require_ranked_sql_error(session, context):
    with pytest.raises(DBAPIError):
        await serving._execute_ranked_membership_sql(session, context, "SELECT 1/0")


async def _wait_ranked_query_sleep(observer, backend_pid):
    while not await observer.scalar(
        text("SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid = :pid"), {"pid": backend_pid}
    ):
        await observer.rollback()
        await asyncio.sleep(0.01)


async def _cancel_ranked_native_query(session, observer):
    backend_pid = await session.scalar(text("SELECT pg_backend_pid()"))
    task = asyncio.create_task(
        serving._execute_ranked_membership_sql(
            session, SimpleNamespace(parameter_map={}, knn_order_sql=None), "SELECT pg_sleep(30)"
        )
    )
    try:
        await asyncio.wait_for(_wait_ranked_query_sleep(observer, backend_pid), 5)
    except BaseException:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        raise
    task.cancel()
    await task


@pytest.mark.asyncio
async def test_ranked_address_planner_cancellation_does_not_leak_settings():
    async with _temporary_schema() as (database, _schema):
        assert database.engine is not None
        async with database.engine.connect() as observer:
            original_settings = tuple(
                (
                    await observer.execute(text("SELECT current_setting('enable_nestloop'), current_setting('jit')"))
                ).one()
            )
            await observer.rollback()
            with pytest.raises(asyncio.CancelledError):
                async with database.transaction() as session:
                    await _cancel_ranked_native_query(session, observer)
            async with database.transaction() as recovered:
                settings = tuple(
                    (
                        await recovered.execute(
                            text("SELECT current_setting('enable_nestloop'), current_setting('jit')")
                        )
                    ).one()
                )
                assert settings == original_settings


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


async def _prepare_ranked_address_seek(monkeypatch, database, schema, candidate_scope):
    await _insert_spatial_reference_rows(database, schema)
    exact_npis = await _prepare_rows(database, schema)
    candidate_npis = exact_npis if candidate_scope == "explicit" else None
    if candidate_npis is None:
        await database.status(
            f"ALTER TABLE {schema}.entity_address_unified ADD COLUMN taxonomy_array integer[] NOT NULL DEFAULT '{{}}'"
        )
        await database.status(f"CREATE TABLE {schema}.nucc_taxonomy (code varchar, int_code integer)")
        await database.status(f"INSERT INTO {schema}.nucc_taxonomy VALUES ('207Q00000X', 1)")
        await database.status(
            f"UPDATE {schema}.entity_address_unified SET taxonomy_array = ARRAY[1] WHERE npi = :npi",
            npi=exact_npis[0],
        )
        await database.status(
            f"DELETE FROM {schema}.ptg2_v3_npi_scope WHERE npi <> ALL(CAST(:eligible AS bigint[]))",
            eligible=list(exact_npis),
        )
    await database.status(f"""
        INSERT INTO {schema}.entity_address_unified (
            location_key, npi, address_key, premise_key, address_source_mask, address_sources,
            source_count, source_mask, type, checksum, first_line, city_name, state_name,
            state_code, postal_code, zip5, country_code, address_precision, lat, long
        )
        SELECT 'farther-alternative', npi, md5('alternate-address')::uuid,
               md5('alternate-premise')::uuid, address_source_mask, address_sources,
               source_count, source_mask, type, 2100, 'SECOND SYNTHETIC WAY', city_name,
               state_name, state_code, postal_code, zip5, country_code,
               address_precision, lat, long - 0.01
          FROM {schema}.entity_address_unified
         WHERE location_key = 'exact-geocoded'
    """)
    await database.status(f"""
        INSERT INTO {schema}.npi_address (npi, address_key, type, checksum, date_added)
        SELECT npi, address_key, type, checksum, DATE '2026-08-31'
          FROM {schema}.entity_address_unified
         WHERE location_key = 'farther-alternative'
    """)
    provenance_sql = _schema_sql(serving._ADDRESS_PROVENANCE_SQL, schema)
    monkeypatch.setattr(serving, "PTG2_SCHEMA", schema)
    monkeypatch.setattr(serving, "_ADDRESS_PROVENANCE_SQL", provenance_sql)
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda *_: f"{schema}.ptg2_v3_npi_scope")
    monkeypatch.setattr(
        serving,
        "_membership_address_table_for_request",
        AsyncMock(return_value=f"{schema}.entity_address_unified"),
    )
    monkeypatch.setattr(serving, "_execute_membership_location_sql", _schema_location_executor(schema))
    monkeypatch.setenv("HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED", "1")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "1")
    return exact_npis, candidate_npis


@pytest.mark.asyncio
@pytest.mark.parametrize("candidate_scope", ["explicit", "snapshot"])
async def test_ranked_distance_page_skips_farther_office_and_resumes_null_tail(monkeypatch, candidate_scope):
    """Keep nearest-office and null-tail ranks stable across seeks."""
    async with _temporary_schema() as (database, schema):
        exact_npis, candidate_npis = await _prepare_ranked_address_seek(monkeypatch, database, schema, candidate_scope)
        args = _request_by_field()
        tables = _local_distance_tables()
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))

            first = await serving._membership_distance_page(
                session, tables, args, candidate_npis=candidate_npis, limit=1
            )
            assert first is not None and first.scanned_count == 1
            assert [location["npi"] for location in first.rows] == [exact_npis[0]]
            assert first.rows[0]["location_hash"].endswith(":exact-geocoded")
            assert first.next_position == (first.rows[0]["distance_miles"], exact_npis[0])
            assert first.next_position[0] is not None

            second = await serving._membership_distance_page(
                session, tables, args, candidate_npis=candidate_npis, limit=1, after=first.next_position
            )
            assert second is not None and second.scanned_count == 1
            assert [location["npi"] for location in second.rows] == [exact_npis[1]]
            assert second.next_position == (None, exact_npis[1])

            for anchor in (first, second):
                resumed = await serving._membership_distance_page(
                    session,
                    tables,
                    args,
                    candidate_npis=candidate_npis,
                    limit=1,
                    after=anchor.next_position,
                    include_anchor=True,
                )
                assert resumed == anchor

            end = await serving._membership_distance_page(
                session, tables, args, candidate_npis=candidate_npis, limit=1, after=second.next_position
            )
            assert end == serving._MembershipDistancePage([], 0, None)

            with pytest.raises(serving.PTG2ManifestArtifactError, match="configured exactness bound"):
                await serving._membership_distance_page(
                    session,
                    tables,
                    args,
                    candidate_npis=candidate_npis,
                    limit=serving._graph_location_scan_limit(1) + 1,
                )


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
