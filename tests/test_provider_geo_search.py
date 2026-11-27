# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Route nearby SQL through the all-address projection only when it can answer exactly."""

from dataclasses import replace

import pytest

from api.endpoint import npi
from api.provider_geo_search import (
    ProviderGeoSearch,
    current_provider_geo_search,
    nearby_search_fields,
    nearest_first_pages,
    provider_geo_search_sql,
    request_geo_search,
)
from api.provider_geo_sql import (
    NearbySqlQuery,
    build_native_nearby_count_sql,
    build_native_nearby_sql,
    build_nearby_page_edge_sql,
)
from process import provider_geo_search

_PROJECTION = ProviderGeoSearch('"mrf".provider_geo_search_g7')


def _nearby(*, extra_clause="", ilike_clause="", primary_only=False, taxonomy=False):
    return NearbySqlQuery(
        taxonomy_conditions="classification = :classification",
        extra_clause=extra_clause,
        ilike_clause=ilike_clause,
        use_taxonomy_filter=taxonomy,
        primary_only=primary_only,
        address_table_sql="mrf.entity_address_unified",
        geo_precision_clause="AND COALESCE(a.address_precision, '') <> 'city_zip'",
        geo_type_clause="AND a.type IN ('primary', 'secondary', 'practice', 'site')",
        search_table_sql=_PROJECTION.table_sql,
        search_provider_facts=True,
    )


def test_service_location_switch_matches_the_api_switch():
    assert provider_geo_search.GEO_SERVICE_LOCATIONS_ENV == npi.GEO_SERVICE_LOCATIONS_ENV


def test_projection_is_used_only_for_its_own_columns():
    assert provider_geo_search_sql(_PROJECTION, "AND a.state_name = :state", "") == _PROJECTION.table_sql
    assert provider_geo_search_sql(_PROJECTION, "AND a.npi <> :exclude_npi", "AND d.x ILIKE :q") == (
        _PROJECTION.table_sql
    )
    assert provider_geo_search_sql(_PROJECTION, "AND a.plans_network_array && (:plan_network_array)", "") is None
    zip_clause = "AND COALESCE(a.zip5, LEFT(COALESCE(a.postal_code, ''), 5)) = ANY(:z)"
    assert provider_geo_search_sql(_PROJECTION, zip_clause) == _PROJECTION.table_sql
    legacy = replace(_PROJECTION, layout=provider_geo_search.LEGACY_LAYOUT)
    assert provider_geo_search_sql(legacy, zip_clause) is None
    assert provider_geo_search_sql(legacy, "AND a.state_name = :state") == legacy.table_sql
    assert provider_geo_search_sql(None, "", "") is None


def test_projection_counts_rows_without_the_provider_join():
    sql = build_native_nearby_count_sql(_nearby())
    assert (
        "SELECT COUNT(*) FILTER (WHERE NOT a.shares_identity) "
        "+ COUNT(DISTINCT (a.npi, a.address_key)) FILTER (WHERE a.shares_identity) AS total_count"
    ) in sql
    assert 'FROM "mrf".provider_geo_search_g7 AS a' in sql
    assert "JOIN mrf.npi" not in sql


def test_provider_predicates_keep_the_join():
    sql = build_native_nearby_count_sql(_nearby(extra_clause="\n AND d.entity_type_code = :entity_type_code"))
    assert "JOIN mrf.npi AS d ON d.npi = a.npi" in sql
    assert "FILTER (WHERE a.shares_identity)" in sql


def test_projected_entity_type_filters_count_and_both_page_paths():
    filter_clause = "\n AND d.entity_type_code = :entity_type_code"
    nearby = replace(
        _nearby(extra_clause=filter_clause, taxonomy=True, primary_only=True), search_has_entity_type_code=True
    )
    statements = (
        build_native_nearby_count_sql(nearby),
        build_native_nearby_sql(nearby, use_search_page=True).split("hydrated_geo AS")[0],
        build_native_nearby_sql(nearby, cursor_clause="\n AND a.cursor_distance_meters > :cursor").split("page_geo AS")[
            0
        ],
    )
    for sql in statements:
        assert "a.entity_type_code = :entity_type_code" in sql
        assert "d.entity_type_code" not in sql and "JOIN mrf.npi AS d" not in sql
        assert "a.taxonomy_array &&" in sql and "a.primary_taxonomy_codes &&" in sql
    assert nearby.extra_clause == filter_clause
    named = replace(nearby, ilike_clause="\n AND d.provider_last_name ILIKE :q")
    named_count = build_native_nearby_count_sql(named)
    assert "WITH matched_npis AS MATERIALIZED" in named_count
    assert "a.npi IN (SELECT npi FROM matched_npis)" in named_count
    assert "d.provider_last_name ILIKE :q" in named_count and "JOIN mrf.npi AS d" not in named_count
    needs_live_provider = build_native_nearby_count_sql(replace(named, search_has_entity_type_code=False))
    assert "JOIN mrf.npi AS d" in needs_live_provider and "matched_npis" not in needs_live_provider


def test_primary_taxonomy_reads_the_projection_column_not_npi_taxonomy():
    nearby = _nearby(taxonomy=True, primary_only=True)
    count_sql = build_native_nearby_count_sql(nearby)
    page_sql = build_native_nearby_sql(nearby, use_search_page=True)
    for sql in (count_sql, page_sql.split("hydrated_geo AS")[0]):
        assert "a.primary_taxonomy_codes && ARRAY(SELECT CAST(code AS text) FROM mrf.nucc_taxonomy" in sql
        assert "provider_taxonomy" not in sql
    assert "SELECT a.npi AS npi_code" in page_sql


def test_cursor_pages_read_the_projection_without_the_provider_join():
    sql = build_native_nearby_sql(
        _nearby(), cursor_clause="\n AND a.cursor_distance_meters > :cursor", use_search_page=True
    )
    eligible = sql.split("page_geo AS")[0]
    assert 'FROM "mrf".provider_geo_search_g7 AS a' in eligible
    assert "SELECT DISTINCT ON (a.npi, a.address_key)" in eligible
    assert "JOIN mrf.npi" not in eligible


def test_live_address_reads_are_unchanged_without_a_projection():
    nearby = _nearby(taxonomy=True, primary_only=True)
    live = NearbySqlQuery(
        **{**{field: getattr(nearby, field) for field in nearby.__dataclass_fields__}, "search_table_sql": None}
    )
    sql = build_native_nearby_count_sql(live)
    assert "COUNT(DISTINCT (a.npi, a.address_key))" in sql and "JOIN mrf.npi AS d" in sql
    assert "provider_taxonomy.healthcare_provider_taxonomy_code" in sql
    assert "primary_taxonomy_codes" not in sql


def test_native_zip_page_keeps_exact_postal_fallback_with_zip_prefilter():
    address_table = "mrf.entity_address_unified"
    zip_filter = npi._address_zip5_filter("a", address_table, any_array=True)
    nearby = replace(_nearby(extra_clause=f"\n          AND {zip_filter}"), search_table_sql=None)
    prefilter = "AND (cardinality(:zip_codes) <> 1 OR a.zip5 = ANY (:zip_codes) OR a.zip5 IS NULL)"

    page = build_native_nearby_sql(nearby)
    assert f"AND {zip_filter}" in page
    assert prefilter in page
    assert prefilter not in build_native_nearby_count_sql(nearby)
    assert prefilter not in build_native_nearby_sql(nearby, cursor_clause="\n AND a.cursor_distance_meters > :cursor")
    scoped = replace(nearby, address_table_sql=f"(SELECT * FROM {address_table} WHERE FALSE)")
    assert prefilter not in build_native_nearby_sql(scoped)
    assert prefilter not in build_native_nearby_sql(replace(nearby, extra_clause=""))


def test_nearby_query_prefers_the_network_projection(monkeypatch):
    typed = replace(_PROJECTION, has_entity_type_code=True)
    assert nearby_search_fields("(network projection)", typed) == {"search_table_sql": "(network projection)"}
    assert nearby_search_fields(None, _PROJECTION, "AND a.zip5 = :z") == {
        "search_table_sql": _PROJECTION.table_sql,
        "search_provider_facts": True,
        "search_has_entity_type_code": False,
    }
    assert nearby_search_fields(None, _PROJECTION) == {
        "search_table_sql": _PROJECTION.table_sql,
        "search_provider_facts": True,
        "search_has_entity_type_code": False,
    }
    assert nearby_search_fields(None, typed, "AND a.entity_type_code = :type") == {
        "search_table_sql": typed.table_sql,
        "search_provider_facts": True,
        "search_has_entity_type_code": True,
    }
    assert nearby_search_fields(None, typed, "AND a.procedures_array && :codes") == {"search_table_sql": None}
    monkeypatch.setattr(npi, "scoped_geo_search_relation_sql", lambda *_args: None)
    nearby = npi._nearby_sql_query(
        "1=1",
        "",
        "",
        use_taxonomy_filter=False,
        address_table_sql="mrf.entity_address_unified",
        provider_search=_PROJECTION,
    )
    assert nearby.search_table_sql == _PROJECTION.table_sql and nearby.search_provider_facts is True


class _Rows:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.statements = []

    async def all(self, statement, **parameters):
        self.statements.append((str(statement), parameters))
        return self.answers.pop(0)


@pytest.mark.provider_geo_search
@pytest.mark.asyncio
async def test_lookup_serves_only_a_fresh_current_generation(caplog):
    missing = _Rows([(False,)])
    assert await current_provider_geo_search(missing, "mrf", include_service_locations=True) is None

    fresh = _Rows([(True,)], [("provider_geo_search_g3", provider_geo_search.LAYOUT, True, True)])
    assert await current_provider_geo_search(fresh, "mrf", include_service_locations=True) == ProviderGeoSearch(
        '"mrf".provider_geo_search_g3', has_entity_type_code=True
    )
    guard_sql, guard_parameters = fresh.statements[1]
    assert "entity_address_result_generation" in guard_sql and "npi_result_generation" in guard_sql
    assert guard_parameters == {
        "include_service_locations": True,
        "schema": "mrf",
        "layout": provider_geo_search.LAYOUT,
        "legacy_layout": provider_geo_search.LEGACY_LAYOUT,
    }
    pin_sql, pin_parameters = fresh.statements[0]
    assert pin_sql.startswith("SELECT pg_try_advisory_xact_lock_shared(:retire_lock) AND")
    assert pin_parameters["retire_lock"] == provider_geo_search.RETIRE_LOCK_ID
    assert "s.layout IN (:layout, :legacy_layout)" in guard_sql
    assert "has_table_privilege(" in guard_sql

    prior = _Rows([(True,)], [("provider_geo_search_g5", provider_geo_search.LAYOUT, True, False)])
    assert await current_provider_geo_search(prior, "mrf", include_service_locations=True) == ProviderGeoSearch(
        '"mrf".provider_geo_search_g5'
    )
    legacy = _Rows([(True,)], [("provider_geo_search_g2", provider_geo_search.LEGACY_LAYOUT, True, False)])
    assert await current_provider_geo_search(legacy, "mrf", include_service_locations=True) == ProviderGeoSearch(
        '"mrf".provider_geo_search_g2', provider_geo_search.LEGACY_LAYOUT
    )

    stale = _Rows([(True,)], [("provider_geo_search_g4", provider_geo_search.LAYOUT, False, True)])
    assert await current_provider_geo_search(stale, "mrf", include_service_locations=True) is None
    assert "provider_geo_search_g4 is not readable for the published inputs" in caplog.text

    unreadable = _Rows([(False,)])
    assert await current_provider_geo_search(unreadable, "mrf", include_service_locations=True) is None
    readable_sql, readable_parameters = unreadable.statements[0]
    assert readable_sql.count("has_table_privilege(to_regclass(") == 3
    assert readable_parameters["state"] == '"mrf"."provider_geo_search_state"'

    with pytest.raises(ValueError, match="provider_geo_search_table_invalid"):
        await current_provider_geo_search(
            _Rows([(True,)], [("other_table", provider_geo_search.LAYOUT, True, False)]),
            "mrf",
            include_service_locations=True,
        )


@pytest.mark.provider_geo_search
@pytest.mark.asyncio
async def test_lookup_checks_running_rebuild():
    """A generation behind the published inputs stays servable only while the builder holds its lock."""
    lookup = _Rows([(True,)], [("provider_geo_search_g4", provider_geo_search.LAYOUT, True, True)])
    await current_provider_geo_search(lookup, "mrf", include_service_locations=True)
    guard_sql = lookup.statements[1][0]
    high_half, low_half = divmod(provider_geo_search.BUILD_LOCK_ID, 2**32)
    assert "FROM pg_catalog.pg_locks AS held" in guard_sql and "held.granted" in guard_sql
    assert f"CAST('{high_half}' AS oid)" in guard_sql and f"CAST('{low_half}' AS oid)" in guard_sql


@pytest.mark.provider_geo_search
@pytest.mark.asyncio
async def test_refresh_after_publication_never_fails_the_import(monkeypatch, caplog):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    calls = []

    @asynccontextmanager
    async def acquire_driver(**options):
        calls.append(options)
        yield object()

    async def failing_rebuild(_connection, **_options):
        raise RuntimeError("disk full")

    import db.connection

    monkeypatch.setattr(db.connection, "db", SimpleNamespace(acquire_driver=acquire_driver))
    monkeypatch.setattr(provider_geo_search, "rebuild_provider_geo_search", failing_rebuild)
    await provider_geo_search.refresh_after_publication("npi", test_mode=True)
    assert calls == []
    await provider_geo_search.refresh_after_publication("npi")
    assert calls == [{"detached": True}]
    assert "rebuild after npi publication failed" in caplog.text


@pytest.mark.provider_geo_search
@pytest.mark.asyncio
async def test_first_pages_turn_off_bitmap_scans_and_restore_the_setting():
    answers = _Rows([("on",)], [("off",)], [("on",)])
    async with nearest_first_pages(answers, _PROJECTION):
        answers.statements.append(("page", {}))
    assert [statement for statement, _ in answers.statements] == [
        "SELECT current_setting('enable_bitmapscan')",
        "SELECT set_config('enable_bitmapscan', 'off', true)",
        "page",
        "SELECT set_config('enable_bitmapscan', :previous, true)",
    ]
    assert answers.statements[-1][1] == {"previous": "on"}
    untouched = _Rows()
    async with nearest_first_pages(untouched, None):
        assert untouched.statements == []
    assert untouched.statements == []


@pytest.mark.provider_geo_search
@pytest.mark.asyncio
async def test_projection_is_looked_up_only_for_unscoped_unified_reads(monkeypatch):
    from contextlib import asynccontextmanager

    import api.network_address_scope as network_scope

    opened_scopes = []

    @asynccontextmanager
    async def connection_scope():
        opened_scopes.append(True)
        yield _Rows([(True,)], [("provider_geo_search_g2", provider_geo_search.LAYOUT, True, False)])

    lookup_by_name = {"schema": "mrf", "include_service_locations": True}
    assert await request_geo_search(connection_scope(), "mrf.npi_address", **lookup_by_name) is None
    monkeypatch.setattr(network_scope, "current_network_address_scope", lambda: object())
    assert await request_geo_search(connection_scope(), "mrf.entity_address_unified", **lookup_by_name) is None
    assert opened_scopes == []
    monkeypatch.setattr(network_scope, "current_network_address_scope", lambda: None)
    assert await request_geo_search(connection_scope(), "mrf.entity_address_unified", **lookup_by_name) == (
        ProviderGeoSearch('"mrf".provider_geo_search_g2')
    )


def test_bounded_continuation_reads_only_the_disc_up_to_the_batch_edge():
    nearby = _nearby()
    cursor = "\n AND a.cursor_distance_meters > :cursor"
    page = build_native_nearby_sql(nearby, cursor_clause=cursor)
    bounded = build_native_nearby_sql(nearby, cursor_clause=cursor, cursor_bounded=True)
    radius = ":radius * 1609.34"
    edge = "CAST(:page_edge_meters AS double precision)"

    # The disc is wider than the edge because <-> is spherical and ST_DWithin spheroidal; the page stops at the edge.
    assert bounded == page.replace(f", {radius})", f", LEAST({radius}, {edge} * 1.01))", 1).replace(
        cursor, f"{cursor}\n       AND a.cursor_distance_meters <= {edge}", 1
    )
    assert ":page_edge_meters" not in page

    probe = build_nearby_page_edge_sql(nearby)
    eligible = page.split("page_geo AS")[0]
    filters = eligible[eligible.index("AND a.lat IS NOT NULL") : eligible.index("ORDER BY")].strip()
    assert filters in probe and f"{radius} * :page_edge_share)" in probe
    assert "a.npi, a.address_key) > (" in probe and ":cursor_distance_meters" in probe
    assert probe.rstrip().endswith("OFFSET :limit - 1 LIMIT 1") and "DISTINCT" not in probe
