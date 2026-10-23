# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native protected code membership precedes geographic address admission."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api import ptg2_serving as serving
from api import ptg_membership_reader as reader
from api import ptg_provider_membership_build as build
from tests.ptg2_serving_address_evidence_postgres_geo import _insert_spatial_reference_rows
from tests.ptg2_serving_address_evidence_postgres_support import _create_tables
from tests.test_ptg2_distance_exact_scope_postgres import _schema_location_executor
from tests.test_ptg2_geo_rate_prefix import _production_tables, _rate_row
from tests.test_ptg_membership_projection_postgres import _build, _database, _source

_GEO_TABLES = (
    "entity_address_unified",
    "entity_address_geo_assurance_state",
    "entity_address_evidence",
    "mrf_address",
    "npi_address",
    "doctor_clinician_address",
    "ptg2_v3_npi_scope",
    "ptg2_v4_npi_scope",
    "geo_zip_lookup",
    "zip_state",
    "zcta5",
    "npi",
    "npi_taxonomy",
)
_GEO_NPI, _ZIP_NPI, _EMPTY_NPI = 1000000001, 1000000002, 1000000003
_NOISE = tuple(range(1000000100, 1000000237))
_NPIS = (_GEO_NPI, _ZIP_NPI, _EMPTY_NPI, *_NOISE)
_ARGS = {
    "code_system": "CPT",
    "code": "99213",
    "zip5": "00001",
    "lat": 42.0,
    "long": -83.0,
    "radius_miles": 10,
    "include_allowed_amounts": False,
}


async def _insert_geo_addresses(status):
    """Seed noise, duplicate premises and the independent eligible-location truth."""
    await status(
        """INSERT INTO mrf.entity_address_unified
        (location_key,npi,address_key,premise_key,address_source_mask,address_sources,source_count,source_mask,
         type,checksum,first_line,city_name,state_name,state_code,postal_code,zip5,country_code,address_precision,lat,long)
        SELECT 'noise-'||ordinal,npi,md5('noise-address-'||ordinal)::uuid,md5('noise-premise-'||ordinal)::uuid,
               1,ARRAY['nppes']::varchar[],1,1,'practice',ordinal,ordinal||' SYNTHETIC WAY',
               'TEST CITY','TS','TS','00001','00001','US','street',42.0,-83.0+ordinal*0.000001
          FROM unnest(CAST(:npis AS bigint[])) WITH ORDINALITY wanted(npi,ordinal)""",
        npis=list(_NOISE),
    )
    for name, npi, lon, zip5, mask in (
        ("eligible-geo", _GEO_NPI, -83.04, "00001", 1),
        ("eligible-geo-farther", _GEO_NPI, -83.06, "00001", 1),
        ("eligible-zip", _ZIP_NPI, None, "00001", 1),
        ("empty-code", _EMPTY_NPI, -83.00001, "00001", 1),
        ("unassured", _GEO_NPI, -83.00001, "00001", 0),
        ("incoherent", _ZIP_NPI, -83.00001, "00003", 1),
    ):
        await status(
            """INSERT INTO mrf.entity_address_unified
            (location_key,npi,address_key,premise_key,address_source_mask,address_sources,source_count,source_mask,
             type,checksum,first_line,city_name,state_name,state_code,postal_code,zip5,country_code,address_precision,lat,long)
            VALUES(CAST(:name AS text),:npi,md5(CAST(:name AS text))::uuid,md5(CAST(:name AS text)||'-premise')::uuid,:mask,ARRAY['nppes']::varchar[],1,:mask,
                   'practice',9000,'SYNTHETIC WAY','TEST CITY','TS','TS',:zip5,:zip5,'US','street',:lat,:lon)""",
            name=name,
            npi=npi,
            lon=lon,
            lat=None if lon is None else 42.0,
            zip5=zip5,
            mask=mask,
        )
    await status(
        "INSERT INTO mrf.npi_address SELECT npi,address_key,type,checksum,'2026-01-01'::date "
        "FROM mrf.entity_address_unified WHERE address_source_mask=1"
    )


async def _assert_geo_dispatch_parity(session, tables, identity, rates, expected_memberships_by_npi, monkeypatch):
    """Keep ordinary aggregate routing, taxonomy and distance tied to complete membership."""
    # Exercise ordinary aggregate dispatch with a smaller sealed fixture
    # rate budget; the complete three-row code must take the oversized path.
    smaller_tables = replace(
        tables,
        provider_graph_v4_hot_prefix=tables.provider_graph_v4_hot_prefix
        | {
            "max_online_provider_expansion_rate_rows": 2,
            "provider_expansion_rate_page_rows": 2,
        },
    )
    routed = await serving._select_geo_filtered_rate_prefix(
        session,
        smaller_tables,
        code_rows=[{"code_key": 7, "rate_count": 3}],
        args=dict(_ARGS),
        network_names=["synthetic network"],
        target_count=1,
        descending=False,
        source_trace_set_hash="ab" * 16,
    )
    assert routed == serving._GeoRateSelection((rates[0],), False)
    found = await reader.locate(session, identity)
    locations = await serving._indexed_geo_location_rows(session, tables, dict(_ARGS), found[0], (7,), limit=4)
    assert [location_row["npi"] for location_row in locations] == list(expected_memberships_by_npi)
    assert locations[0]["distance_miles"] is not None and locations[1]["distance_miles"] is None
    assert locations[0]["_ptg_source_exhausted"] is True
    taxonomy_locations = await serving._indexed_geo_location_rows(
        session, tables, dict(_ARGS, taxonomy_codes=["207Q00000X"]), found[0], (7,), limit=4
    )
    assert [location_row["npi"] for location_row in taxonomy_locations] == [_GEO_NPI]
    # Distance keeps its own ordinary stable-prefix reader and native index.
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 20)
    candidates = await serving._local_inferred_distance_graph_candidates(
        session,
        tables,
        dict(_ARGS, order_by="distance", order="asc"),
        plan_id="synthetic-plan",
        requested_code="99213",
        requested_system="CPT",
        candidate_limit=3,
    )
    assert [location_row["npi"] for location_row in candidates.location_rows] == list(expected_memberships_by_npi)
    assert candidates.provider_set_keys_by_npi == {npi: set(keys) for npi, keys in expected_memberships_by_npi.items()}


def _complete_source(monkeypatch, representation):
    tables, identity = _source(monkeypatch, representation=representation)
    manifest, members = build.load_v4_relation_manifest, build.lookup_v4_relation_members
    first = "npi_patterns" if representation == "pattern_v1" else "npi_groups_exact"

    async def complete_manifest(*args, relation, **kwargs):
        row = await manifest(*args, relation=relation, **kwargs)
        return replace(row, owner_count=len(_NPIS)) if relation == first else row

    async def complete_members(*args, relation, owner_keys, **kwargs):
        if relation != first:
            return await members(*args, relation=relation, owner_keys=owner_keys, **kwargs)
        selected_keys = tuple(key for key in owner_keys if key < 3)
        result = await members(*args, relation=relation, owner_keys=selected_keys, **kwargs)
        return {key: result.get(key, ()) for key in owner_keys}

    monkeypatch.setattr(build, "load_v4_relation_manifest", complete_manifest)
    monkeypatch.setattr(build, "lookup_v4_relation_members", complete_members)

    # Only immutable source metadata is synthetic; publication, custody, dictionary
    # lookup, membership joins and geographic SQL below use actual PostgreSQL.
    async def source_identity(_session, source_key, snapshot_id, system, code, **_kwargs):
        return (
            tables,
            None,
            identity
            | {
                "source_key": source_key,
                "snapshot_id": snapshot_id,
                "code_system": system,
                "code": code,
            },
        )

    monkeypatch.setattr(reader, "source_identity", source_identity)
    return replace(
        _production_tables(), source_key=tables.source_key, snapshot_id=tables.snapshot_id, shared_snapshot_key=1
    ), identity


async def _prepare_geo(db):
    """Create exact native address and taxonomy tables for the protected reader."""

    async def status(sql, **params):
        async with db.admin.begin() as session:
            await session.execute(text(sql), params)

    database = SimpleNamespace(status=status)
    await _create_tables(database, "mrf")
    await _insert_spatial_reference_rows(database, "mrf")
    await status(
        "CREATE TABLE mrf.ptg2_v4_npi_scope(snapshot_key bigint,npi_key integer,npi bigint,"
        "PRIMARY KEY(snapshot_key,npi_key),UNIQUE(snapshot_key,npi))"
    )
    await status(
        "INSERT INTO mrf.ptg2_v4_npi_scope SELECT 1,ordinal-1,npi "
        "FROM unnest(CAST(:npis AS bigint[])) WITH ORDINALITY wanted(npi,ordinal)",
        npis=list(_NPIS),
    )
    await status(
        "CREATE TABLE mrf.npi(npi bigint PRIMARY KEY,entity_type_code smallint,search_taxonomy_codes varchar[])"
    )
    await status(
        "CREATE TABLE mrf.npi_taxonomy(npi bigint,healthcare_provider_taxonomy_code varchar,"
        "healthcare_provider_primary_taxonomy_switch varchar)"
    )
    await status("INSERT INTO mrf.npi SELECT npi,1,ARRAY['207Q00000X']::varchar[] FROM mrf.ptg2_v4_npi_scope")
    await status("INSERT INTO mrf.npi_taxonomy VALUES(:npi,'207Q00000X','Y')", npi=_GEO_NPI)
    await _insert_geo_addresses(status)
    await status(f'GRANT SELECT ON ALL TABLES IN SCHEMA mrf TO "{db.reader}"')


@asynccontextmanager
async def _indexed_geo_database(monkeypatch, representation):
    tables, identity = _complete_source(monkeypatch, representation)
    async with _database(monkeypatch) as db:

        async def principals(*_args):
            return ['"' + db.reader + '"']

        monkeypatch.setattr(build, "_reader_principals", principals)
        receipt = await _build(db)
        assert receipt["state"] == "ready"
        engine = create_async_engine(db.reader_url)
        try:
            await _prepare_geo(db)
            monkeypatch.setattr(
                serving, "_membership_address_table_for_request", AsyncMock(return_value="mrf.entity_address_unified")
            )
            monkeypatch.setattr(serving, "_execute_membership_location_sql", _schema_location_executor("mrf"))
            monkeypatch.setattr(
                serving, "_shared_rate_code_scope_rows", AsyncMock(return_value=[{"code_key": 7, "rate_count": 3}])
            )
            monkeypatch.setattr(
                serving, "load_v4_graph_root", AsyncMock(return_value=SimpleNamespace(representation=representation))
            )
            monkeypatch.setattr(
                serving, "_v4_sets_by_npi", AsyncMock(side_effect=AssertionError("raw graph must not be read"))
            )
            monkeypatch.setattr(
                serving,
                "_scoped_rate_provider_set_keys",
                AsyncMock(side_effect=AssertionError("code coverage is indexed")),
            )
            yield db, async_sessionmaker(engine), tables, identity
        finally:
            await engine.dispose()
            async with db.admin.begin() as session:
                for name in _GEO_TABLES:
                    await session.execute(text(f'DROP TABLE IF EXISTS mrf."{name}" RESTRICT'))


@pytest.mark.asyncio
@pytest.mark.parametrize("representation", ["pattern_v1", "direct_v1"])
async def test_native_aggregate_and_distance_match_complete_code_geo_truth(monkeypatch, representation):
    """Both graph representations must match complete code and geographic truth."""
    async with _indexed_geo_database(monkeypatch, representation) as (_db, sessions, tables, _identity):
        # Independent flattened truth from the fixture's complete factors and code.
        expected_memberships_by_npi = {_GEO_NPI: (10, 11), _ZIP_NPI: (11,)}
        rates = [
            _rate_row(9) | {"term": "one", "source_key": 0},
            _rate_row(9) | {"price_key": 22, "term": "two", "source_key": 1},
            _rate_row(10) | {"term": "three", "source_key": 0},
        ]

        async def read_rates(_session, _tables, **kwargs):
            assert kwargs["provider_set_keys"] == (10, 11)
            assert kwargs["code_rows"] == [{"code_key": 7, "rate_count": 3}]
            assert kwargs["source_trace_set_hash"] == "ab" * 16
            assert kwargs["network_names"] == ["synthetic network"]
            ordered = list(reversed(rates)) if kwargs["descending"] else rates
            return ordered[kwargs["offset"] : kwargs["offset"] + kwargs["limit"]]

        monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", read_rates)
        monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 4)
        async with sessions() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET TRANSACTION READ ONLY"))
            raw = await serving._membership_location_rows(session, tables, dict(_ARGS), candidate_npis=None, limit=4)
            assert raw and raw[0]["_ptg_source_exhausted"] is False
            assert not {_GEO_NPI, _ZIP_NPI}.intersection(location_row.get("npi") for location_row in raw)
            for target_count, descending, exact in ((2, False, False), (4, True, True)):
                budget = serving._geo_rate_selection_budget(tables)
                request = serving._GeoRateSelectionRequest(
                    [{"code_key": 7, "rate_count": 3}],
                    dict(_ARGS),
                    ["synthetic network"],
                    target_count,
                    descending,
                    "ab" * 16,
                )
                selected = await serving._select_oversized_geo_rate_scope(session, tables, request, budget)
                assert selected.row_data == tuple((list(reversed(rates)) if descending else rates)[:target_count])
                assert selected.exhausted is exact
                assert budget.reverse_provider_set_keys_by_npi == expected_memberships_by_npi
                assert budget.reverse_geo_scope == (tuple(expected_memberships_by_npi), True, 4)
            await _assert_geo_dispatch_parity(
                session, tables, _identity, rates, expected_memberships_by_npi, monkeypatch
            )


@pytest.mark.asyncio
async def test_native_ready_attachment_cannot_escape_coverage_or_reader_custody(monkeypatch):
    async with _indexed_geo_database(monkeypatch, "pattern_v1") as (db, sessions, tables, identity):
        address = AsyncMock(side_effect=AssertionError("denied attachment must not read addresses"))
        monkeypatch.setattr(serving, "_indexed_geo_location_rows", address)
        request = serving._GeoRateSelectionRequest([{"code_key": 8, "rate_count": 1}], dict(_ARGS), [], 1, False)
        async with sessions() as session, session.begin():
            with pytest.raises(serving.PTG2ManifestArtifactError, match="complete requested physical code scope"):
                await serving._select_oversized_geo_rate_scope(
                    session, tables, request, serving._geo_rate_selection_budget(tables)
                )
        async with db.admin.begin() as session:
            await session.execute(
                text(f'REVOKE SELECT ON "{db.stages[0].schema_name}".ptg_membership_manifest FROM "{db.reader}"')
            )
        async with sessions() as session, session.begin():
            with pytest.raises(DBAPIError):
                await serving._select_oversized_geo_rate_scope(
                    session,
                    tables,
                    replace(request, code_rows=[{"code_key": 7}]),
                    serving._geo_rate_selection_budget(tables),
                )
        address.assert_not_awaited()
