# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verified narrow geo reads preserve scope, fallback, and wide hydration."""

import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from uuid import UUID

import pytest
from sanic.request.parameters import RequestParameters
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api import provider_list_sql
from api.endpoint import npi
from api.network_address_scope import (
    network_address_read_scope,
    scoped_address_cache_key,
    scoped_address_parameters,
    scoped_address_relation_sql,
    scoped_geo_search_relation_sql,
)
from api.network_compact_address import compact_address_parameters, compact_address_relation_sql
from api.network_read_execution import qualified_relation_sql
from api.provider_geo_sql import (
    build_imported_geo_statements,
    build_native_nearby_count_sql,
    build_native_nearby_sql,
)
from db.models import EntityAddressUnified
from process.network_compact_layout import (
    CompactAddressRelation,
    bind_compact_geo_search,
)
from process.network_geo_search import SEARCH_COLUMNS, prepare_network_geo_search
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import _manifest_digest
from tests import test_custom_import_provider_geo_sql as geo_fixture
from tests.network_compact_address_postgres_support import _layout, _lookup_manifest, _manifest
from tests.network_compact_address_postgres_support import compact_db as compact_db
from tests.test_custom_import_provider_geo_sql import _context, _imported_geo_query
from tests.test_network_membership_slices_postgres import slice_db as slice_db


def _search_layout():
    layout = _layout()
    projection = CompactAddressRelation(layout.ref_map.schema_name, "network_geo_search")
    return bind_compact_geo_search(layout, projection), projection


def _search_manifest(layout, projection):
    assert layout.geo_search == projection
    return _lookup_manifest(layout)


def test_search_binding_keeps_the_named_layout_serialization():
    layout = _layout()
    original = asdict(layout)
    projection = CompactAddressRelation(layout.ref_map.schema_name, "network_geo_search")
    bind_compact_geo_search(layout, projection)
    assert asdict(layout) == original
    assert layout.geo_search == projection
    assert projection in _search_manifest(layout, projection).candidate_relation_pins


@pytest.mark.parametrize("mode", ["old-layout", "visible-policy", "legacy", "flat"])
def test_search_falls_back_for_existing_or_incompatible_scope(mode):
    layout, _projection = _search_layout()
    if mode == "old-layout":
        layout = _layout()
    manifest = _lookup_manifest(layout)
    if mode == "flat":
        manifest = replace(
            _manifest(layout),
            address_relation=CompactAddressRelation(layout.ref_map.schema_name, "entity_address_unified"),
        )
    network_ids = None if mode == "visible-policy" else (42,)
    scope_options_by_name = (
        {"excluded_network_ids": (7,), "access_scope_sha256": "a" * 64}
        if mode == "visible-policy"
        else ({"legacy_checksums": (11,)} if mode == "legacy" else {})
    )
    with network_address_read_scope(manifest, network_ids, **scope_options_by_name):
        relation = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        assert scoped_geo_search_relation_sql(relation, "a.npi = :npi") is None
        assert "network_geo_search" not in relation


def test_search_requires_the_original_scoped_relation_and_supported_fields():
    layout, projection = _search_layout()
    assert scoped_geo_search_relation_sql("mrf.entity_address_unified", "a.npi=:npi") is None
    with network_address_read_scope(_search_manifest(layout, projection), (7, 42)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        assert scoped_geo_search_relation_sql("mrf.entity_address_unified", "a.npi=:npi") is None
        assert scoped_geo_search_relation_sql(relation + " ", "a.npi=:npi") is None
        unsupported_columns = {column.name for column in EntityAddressUnified.__table__.columns} - set(SEARCH_COLUMNS)
        assert unsupported_columns, "The search projection must remain narrower than the address heap"
        assert scoped_geo_search_relation_sql(relation, "a." + sorted(unsupported_columns)[0] + " IS NOT NULL") is None
        for alias in ('a."{}"', '"a"."{}"', "A . {}"):
            predicate = alias.format(sorted(unsupported_columns)[0]) + " IS NOT NULL"
            assert scoped_geo_search_relation_sql(relation, predicate) is None
        search = scoped_geo_search_relation_sql(relation, "a.npi=:npi AND a.state_name=:state")
        assert qualified_relation_sql(projection.schema_name, projection.table_name) in search
        assert "FROM ONLY" in search
        assert "canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])" in search
        assert scoped_address_parameters({})["_canonical_network_ids"] == [7, 42]


@pytest.mark.parametrize("network_ids", [(42,), (7, 42)])
def test_native_search_preserves_knn_page_and_filters_counts_and_cursors(monkeypatch, network_ids):
    monkeypatch.setattr(npi, "_should_include_geo_service_locations", lambda: True)
    layout, projection = _search_layout()
    with network_address_read_scope(_search_manifest(layout, projection), network_ids):
        relation = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        nearby = npi._nearby_sql_query(
            "code = ANY(:codes)",
            "AND a.state_name = :state",
            "AND d.provider_first_name LIKE :q",
            use_taxonomy_filter=True,
            primary_only=True,
            address_table_sql=relation,
            geo_precision_clause=npi._exact_geo_precision_clause(relation),
        )
        assert nearby.search_table_sql is not None
        page = build_native_nearby_sql(nearby)
        count = build_native_nearby_count_sql(nearby)
        assert page == build_native_nearby_sql(replace(nearby, search_table_sql=None))
        assert relation in page and relation not in count
        projection_sql = qualified_relation_sql(projection.schema_name, projection.table_name)
        assert projection_sql in count
        search_page = build_native_nearby_sql(nearby, use_search_page=True)
        assert "a.geo <->" in search_page
        assert search_page.index("LIMIT :limit") < search_page.index("hydrated_geo AS MATERIALIZED")
        assert projection_sql in search_page
        assert "a.state_name = :state" in page and "a.state_name = :state" in count
        assert "healthcare_provider_primary_taxonomy_switch" in page
        assert "COUNT(DISTINCT (a.npi, a.address_key))" in count
        cursor_clause, _parameters = npi._nearby_cursor_filter(
            (0.0, 1000000001, "00000000-0000-0000-0000-000000000001")
        )
        continued = build_native_nearby_sql(nearby, cursor_clause=cursor_clause)
        assert continued.index("SELECT DISTINCT ON") < continued.index("page_geo AS MATERIALIZED")
        assert nearby.search_table_sql in continued and relation in continued
        assert continued.index(cursor_clause) < continued.index("LIMIT :limit")


@pytest.mark.parametrize(
    "direction,required", [(None, True), ("asc", False), ("desc", False), ("asc", True), ("desc", True)]
)
def test_imported_search_keeps_eligible_scope_before_score_filter_and_order(monkeypatch, direction, required):
    monkeypatch.setattr(npi, "_should_include_geo_service_locations", lambda: True)
    layout, projection = _search_layout()
    with network_address_read_scope(_search_manifest(layout, projection), (7, 42)):
        relation = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        nearby = npi._nearby_sql_query(
            "1=1", "AND a.state_name=:state", "", use_taxonomy_filter=False, address_table_sql=relation
        )
        query = replace(_imported_geo_query(), nearby=nearby)
        statements = build_imported_geo_statements(_context(direction=direction, require_match=required), query)
        sql = str(statements.page)
        materialization = " MATERIALIZED" if required else ""
        eligible, remainder = sql.split("custom_import_provider_relation AS" + materialization + " (", 1)
        assert ("custom_import_provider_relation AS MATERIALIZED" in sql) is required
        assert nearby.search_table_sql in eligible and "SELECT DISTINCT ON" in eligible
        assert "LIMIT" not in eligible
        assert sql.index("LIMIT") < sql.index("hydrated_geo AS MATERIALIZED")
        assert relation in remainder
        assert qualified_relation_sql(projection.schema_name, projection.table_name) in sql
        assert ("WHERE imported.entity_value = native_eligible_geo.npi_code::text" in sql) is required
        assert ("LEFT JOIN custom_import_provider_relation AS imported" in sql) is (direction is not None)


def test_search_binding_is_idempotent_and_cannot_be_replaced_or_cleared():
    layout, projection = _search_layout()
    assert bind_compact_geo_search(layout, projection) is layout
    for changed in (None, replace(projection, table_name="other_search")):
        with pytest.raises(ValueError, match="already bound"):
            bind_compact_geo_search(layout, changed)
    assert layout.geo_search == projection


@pytest.mark.parametrize("field,changed", [("schema_name", "other_generation"), ("table_name", "other_search")])
def test_search_relation_must_match_its_published_candidate(field, changed):
    layout = _layout()
    projection = CompactAddressRelation(layout.ref_map.schema_name, "network_geo_search")
    with pytest.raises(ValueError, match="differs from its candidate"):
        bind_compact_geo_search(layout, replace(projection, **{field: changed}))


def test_changed_search_generation_changes_cache_and_native_cursor_scope():
    layout, projection = _search_layout()
    scope_report_by_field = {
        "candidate_id": "00000000-0000-0000-0000-000000000002",
        "schema_revision": 2,
        "source_generations": {},
        "approved_custom_revision": 7,
    }
    digests = [
        _manifest_digest(
            generation,
            scope_report_by_field,
            {"membership_rows": 1, "distinct_memberships": 1, "projected_locations": 1},
            {"index_profile": "network_geo_search"},
            {"schema_name": layout.ref_map.schema_name, "tables": [asdict(projection)]},
        )
        for generation in (1, 2, 3)
    ]
    assert len(set(digests)) == 3
    cache_keys, cursor_scopes = [], []
    for digest in digests:
        manifest = _search_manifest(layout, projection)
        object.__setattr__(manifest, "manifest_sha256", digest)
        with network_address_read_scope(manifest, (42,)):
            cache_keys.append(scoped_address_cache_key("same-query"))
            cursor_scopes.append(npi._nearby_cursor_scope({"lat": "0", "long": "0", "radius": "10"}))
    assert len(set(cache_keys)) == len(set(cursor_scopes)) == 3


def _install_api_boundary(case, session, monkeypatch):
    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", geo_fixture._PostgresGeoProxy)
    monkeypatch.setattr(npi, "_plan_release_npi_scope", geo_fixture._empty_plan_scope)
    monkeypatch.setattr(npi, "_should_include_geo_service_locations", lambda: True)

    async def address(*_args, **_kwargs):
        return "mrf.entity_address_unified"

    async def metadata(rows, **_kwargs):
        return rows

    monkeypatch.setattr(npi, "_address_serving_table_sql", address)
    monkeypatch.setattr(npi, "_project_provider_network_metadata", metadata)

    @asynccontextmanager
    async def same_session_connection(_database, _import_context, request_session):
        assert request_session is case.api_session
        yield geo_fixture._PostgresGeoProxy(_database, request_session, None)

    async def sequential_reads(_database, *operations):
        return [await operation for operation in operations]

    # Fixture DDL is rollback-only; all query bodies use that original local session.
    monkeypatch.setattr(npi, "_provider_list_connection", same_session_connection)
    monkeypatch.setattr(npi, "gather_reader_calls", sequential_reads)

    @asynccontextmanager
    async def snapshot(*_args, **_kwargs):
        yield session

    monkeypatch.setattr(npi, "provider_geo_read_snapshot", snapshot)


@pytest.fixture
async def projection_api_case(compact_db, monkeypatch):
    case = compact_db
    await case.connection.execute(
        "CREATE EXTENSION IF NOT EXISTS postgis; CREATE EXTENSION IF NOT EXISTS intarray; CREATE EXTENSION IF NOT EXISTS btree_gist"
    )
    layout = case.second_layout
    for relation in (layout.base, layout.custom):
        await case.connection.execute(
            f'''UPDATE "{relation.schema_name}"."{relation.table_name}"
            SET lat=CASE WHEN location_key=$1 THEN 0.02 ELSE 0 END,long=0,type='primary',
                npi=CASE WHEN location_key=ANY($2::text[]) THEN 1000000003 ELSE 1000000002 END,
                address_key=CASE WHEN location_key=ANY($2::text[]) THEN $3::uuid ELSE $4::uuid END,
                state_name=CASE WHEN location_key=$1 THEN 'NY' ELSE 'CA' END,
                zip5=CASE WHEN location_key=$1 THEN '00001' ELSE '00002' END,
                postal_code=CASE WHEN location_key=$1 THEN '00001' ELSE '00002' END,
                taxonomy_array=CASE WHEN location_key=ANY($2::text[]) THEN ARRAY[2] ELSE ARRAY[1] END,
                address_precision='street',first_line=entity_name''',
            case.keys[0],
            case.keys[:2],
            UUID(geo_fixture._THIRD_ADDRESS_KEY),
            UUID(geo_fixture._SECOND_ADDRESS_KEY),
        )
    async with AsyncSession(case.engine) as session:
        await geo_fixture._seed_geo_tables(session)
        await session.execute(
            text(
                "ALTER TABLE mrf.npi ADD COLUMN provider_last_name text,ADD COLUMN provider_organization_name text,ADD COLUMN provider_other_organization_name text,ADD COLUMN do_business_as_text text,ADD COLUMN provider_sex_code text,ADD COLUMN entity_type_code integer"
            )
        )
        await session.execute(
            text(
                "UPDATE mrf.npi SET provider_first_name=CASE WHEN npi=1000000003 THEN 'Taylor' ELSE 'Synthetic' END,provider_sex_code=CASE WHEN npi=1000000003 THEN 'F' ELSE 'M' END,entity_type_code=1"
            )
        )
        relation = compact_address_relation_sql(layout, visible_membership_sql="TRUE")
        await session.execute(
            text("CREATE TABLE mrf.entity_address_unified AS SELECT * FROM " + relation + " addresses"),
            compact_address_parameters(layout),
        )

        case.api_session = session
        _install_api_boundary(case, session, monkeypatch)
        yield case


async def _native_page(case, **changes):
    args = RequestParameters(
        {
            key: [value]
            for key, value in {
                "lat": "0",
                "long": "0",
                "radius": "10",
                "include_total": "true",
                "limit": "50",
                **changes,
            }.items()
        }
    )
    request = SimpleNamespace(args=args, ctx=SimpleNamespace(sa_session=case.api_session))
    reply = await npi.get_near_npi(request)
    assert reply.status == 200
    return json.loads(reply.body)


async def _route_observations(case, manifest):
    observations = []
    with network_address_read_scope(manifest, (7, 42, 99)):
        for changes in (
            {},
            {"state": "CA"},
            {"state": "ny"},
            {"codes": "222", "primary_only": "false"},
            {"q": "taylor"},
            {"provider_sex_code": "F"},
            {"exclude_npi": "1000000002"},
            {"radius": "0"},
            {"lat": "30", "long": "30"},
        ):
            page = await _native_page(case, **changes)
            observations.append(page)
        assert observations[0]["total_count"] == 2
        assert {provider_card["npi"] for provider_card in observations[0]["items"]} == {1000000002, 1000000003}
        assert observations[7]["total_count"] == 1 and observations[7]["items"][0]["npi"] == 1000000002
        initial = await _native_page(case, limit="1")
        continued = await _native_page(case, limit="1", cursor=initial["next_cursor"])
        assert initial["has_more"] and not continued["has_more"]
        assert initial["items"][0]["address_key"] != continued["items"][0]["address_key"]
        observations.extend((initial, continued))
        for direction, required in ((None, True), ("asc", False), ("desc", False), ("asc", True), ("desc", True)):
            page = await geo_fixture._read_postgres_geo_page(case.api_session, direction, require_match=required)
            first = geo_fixture._provider_identities(page)[0]
            continued = await geo_fixture._read_postgres_geo_page(
                case.api_session, direction, (str(first[0]), first[1]), require_match=required
            )
            assert geo_fixture._provider_identities(continued) == geo_fixture._provider_identities(page)[1:]
            observations.extend((page, continued))
    return observations


async def _page_only_observations(case, manifest):
    observations = []
    with network_address_read_scope(manifest, (7, 42, 99)):
        for filters in ({}, {"codes": "222", "primary_only": "false"}):
            first_call = len(case.api_session.info["geo_calls"])
            page = await _native_page(case, include_total="false", **filters)
            statements = case.api_session.info["geo_calls"][first_call:]
            assert statements and not any("COUNT(DISTINCT" in sql for sql, _parameters in statements)
            projected = manifest.compact_layout.geo_search is not None and bool(filters)
            assert any("network_geo_search" in sql for sql, _parameters in statements) is projected
            observations.append((page, statements))
    return observations


@pytest.mark.asyncio
async def test_postgres_search_projection_preserves_routes_and_unselected_fallback(projection_api_case):
    case = projection_api_case
    layout = case.second_layout
    manifest = _lookup_manifest(layout)
    without_projection = await _route_observations(case, manifest)
    page_only_before = await _page_only_observations(case, manifest)
    no_network_before = await _native_page(case)
    no_network_sql_before = case.api_session.info["geo_calls"][-2:]
    copy_target = MembershipCopyTarget(
        str(UUID(int=10)),
        str(UUID(int=11)),
        str(UUID(int=12)),
        str(UUID(layout.ref_map.schema_name.removeprefix("network_candidate_"))),
        layout.ref_map.schema_name,
    )
    async with case.connection.transaction():
        descriptor = await prepare_network_geo_search(case.connection, copy_target, layout)
    assert descriptor is not None and descriptor["row_count"] > 0
    projection = CompactAddressRelation(copy_target.schema_name, "network_geo_search")
    bind_compact_geo_search(layout, projection)
    projected_manifest = _search_manifest(layout, projection)
    case.api_session.info["geo_calls"] = []
    with_projection = await _route_observations(case, projected_manifest)
    assert with_projection == without_projection
    sql_calls = case.api_session.info["geo_calls"]
    assert any("network_geo_search" in sql for sql, _parameters in sql_calls)
    assert any("a.geo" in sql and "COUNT(DISTINCT" in sql for sql, _parameters in sql_calls)
    assert any("a.geo <->" in sql for sql, _parameters in sql_calls)
    page_only_after = await _page_only_observations(case, projected_manifest)
    assert [page for page, _statements in page_only_after] == [page for page, _statements in page_only_before]
    assert page_only_after[0][1] == page_only_before[0][1]
    no_network_after = await _native_page(case)
    assert no_network_after == no_network_before
    assert case.api_session.info["geo_calls"][-2:] == no_network_sql_before


@pytest.mark.parametrize(
    "taxonomy_entries,expected",
    [
        (
            [
                {
                    "healthcare_provider_taxonomy_code": "222",
                    "display_name": "Two",
                    "healthcare_provider_primary_taxonomy_switch": "Y",
                },
                {
                    "healthcare_provider_taxonomy_code": "111",
                    "display_name": "One",
                    "healthcare_provider_primary_taxonomy_switch": "Y",
                },
            ],
            {"taxonomy_code": "111", "display": "One"},
        ),
        (
            [{"taxonomy_code": "222", "display": "Two"}, {"taxonomy_code": "111", "display": "One"}],
            {"taxonomy_code": "111", "display": "One"},
        ),
        (
            [
                {"taxonomy_code": "111", "primary": True},
                {"taxonomy_code": "111", "display": "Zed"},
                {"taxonomy_code": "111", "display": "One"},
            ],
            {"taxonomy_code": "111", "display": "One"},
        ),
        (
            [
                {"taxonomy_code": "111", "display": "One"},
                {"taxonomy_code": "222", "display": "Two", "healthcare_provider_primary_taxonomy_switch": "Y"},
            ],
            {"taxonomy_code": "222", "display": "Two"},
        ),
    ],
    ids=["multiple-primary", "no-primary", "display-fallback", "single-primary-preference"],
)
def test_card_specialty_is_independent_of_taxonomy_row_order(taxonomy_entries, expected):
    assert npi._provider_card_primary_specialty(taxonomy_entries) == expected
    assert npi._provider_card_primary_specialty(list(reversed(taxonomy_entries))) == expected
