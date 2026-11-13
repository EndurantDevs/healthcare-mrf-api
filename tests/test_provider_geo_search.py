# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Route nearby SQL through the all-address projection only when it can answer exactly."""

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
from api.provider_geo_sql import NearbySqlQuery, build_native_nearby_count_sql, build_native_nearby_sql
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
    assert provider_geo_search_sql(_PROJECTION, "AND COALESCE(a.zip5, LEFT(a.postal_code, 5)) = ANY(:z)", "") is None
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


def test_nearby_query_prefers_the_network_projection(monkeypatch):
    assert nearby_search_fields("(network projection)", _PROJECTION) == {"search_table_sql": "(network projection)"}
    assert nearby_search_fields(None, _PROJECTION, "AND a.zip5 = :z") == {"search_table_sql": None}
    assert nearby_search_fields(None, _PROJECTION) == {
        "search_table_sql": _PROJECTION.table_sql,
        "search_provider_facts": True,
    }
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

    fresh = _Rows([(True,)], [("provider_geo_search_g3", True)])
    assert await current_provider_geo_search(fresh, "mrf", include_service_locations=True) == ProviderGeoSearch(
        '"mrf".provider_geo_search_g3'
    )
    guard_sql, guard_parameters = fresh.statements[1]
    assert "entity_address_result_generation" in guard_sql and "npi_result_generation" in guard_sql
    assert guard_parameters == {"include_service_locations": True, "schema": "mrf", "layout": 3}
    pin_sql, pin_parameters = fresh.statements[0]
    assert pin_sql.startswith("SELECT pg_try_advisory_xact_lock_shared(:retire_lock) AND")
    assert pin_parameters["retire_lock"] == provider_geo_search.RETIRE_LOCK_ID
    assert "s.layout = :layout" in guard_sql
    assert "has_table_privilege(" in guard_sql

    stale = _Rows([(True,)], [("provider_geo_search_g4", False)])
    assert await current_provider_geo_search(stale, "mrf", include_service_locations=True) is None
    assert "provider_geo_search_g4 is not readable for the published inputs" in caplog.text

    unreadable = _Rows([(False,)])
    assert await current_provider_geo_search(unreadable, "mrf", include_service_locations=True) is None
    readable_sql, readable_parameters = unreadable.statements[0]
    assert readable_sql.count("has_table_privilege(to_regclass(") == 3
    assert readable_parameters["state"] == '"mrf"."provider_geo_search_state"'

    with pytest.raises(ValueError, match="provider_geo_search_table_invalid"):
        await current_provider_geo_search(
            _Rows([(True,)], [("other_table", True)]), "mrf", include_service_locations=True
        )


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
        yield _Rows([(True,)], [("provider_geo_search_g2", True)])

    lookup_by_name = {"schema": "mrf", "include_service_locations": True}
    assert await request_geo_search(connection_scope(), "mrf.npi_address", **lookup_by_name) is None
    monkeypatch.setattr(network_scope, "current_network_address_scope", lambda: object())
    assert await request_geo_search(connection_scope(), "mrf.entity_address_unified", **lookup_by_name) is None
    assert opened_scopes == []
    monkeypatch.setattr(network_scope, "current_network_address_scope", lambda: None)
    assert await request_geo_search(connection_scope(), "mrf.entity_address_unified", **lookup_by_name) == (
        ProviderGeoSearch('"mrf".provider_geo_search_g2')
    )
