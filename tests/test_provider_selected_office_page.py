# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify selected office discovery through the provider list entry point."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi
from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql
from tests.network_compact_address_postgres_support import _layout, _lookup_manifest, _manifest
from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("supports_location_lookup", [True, False])
@pytest.mark.parametrize(
    "size,extra,staged,computed",
    [
        (2, {}, False, False),
        (3, {}, True, False),
        (4, {}, True, True),
        (5, {}, True, True),
        (6, {}, False, False),
        (50, {}, False, False),
        (3, {"primary_only": "false"}, False, False),
        (4, {"primary_only": "false"}, False, True),
        (3, {"state": "NY"}, False, True),
        (3, {"zip_code": "60601"}, False, True),
        (3, {"zip_code": "60601", "codes": ""}, False, False),
        (3, {"zip_code": "60601", "state": "IL", "codes": ""}, False, False),
        (3, {"city": "Chicago", "state": "IL"}, False, True),
        (3, {"codes": "207Q00000X,208D00000X"}, False, False),
        (5, {"codes": "207Q00000X,208D00000X"}, False, True),
        (3, {"q": "clinic"}, False, False),
        (4, {"q": "clinic"}, False, False),
        (3, {"classification": "Family Medicine"}, False, False),
        (4, {"classification": "Family Medicine"}, False, True),
        (4, {"plan_network": "123"}, False, True),
    ],
)
async def test_selected_office_page(monkeypatch, size, extra, staged, computed, supports_location_lookup):
    conn = RecordingConnection()
    monkeypatch.setattr(npi, "_provider_list_connection", lambda *args, **kwargs: FakeAcquire(conn))
    manifest = _lookup_manifest(_layout()) if supports_location_lookup else _manifest(_layout())
    with network_address_read_scope(manifest, tuple(range(1, size + 1))):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        exact_place = bool(extra.get("zip_code"))
        location_source = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=exact_place)
        monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value=ordinary))
        await npi.get_all(SimpleNamespace(args={"codes": "207Q00000X", "include_total": "1", **extra}))
    page = next(sql for sql, _ in conn.sql_calls if "page_npis AS" in sql)
    count = next(sql for sql, _ in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql)
    bounded = size == 3 and staged and supports_location_lookup
    assert ("selected_office_keys AS MATERIALIZED" in page) == (bounded or staged and size != 3)
    assert "selected_office_keys" not in count
    selection, hydration = page.split("sub_s AS (", 1)
    assert location_source in hydration and "selected_office_keys" not in hydration
    if exact_place:
        assert location_source in selection and location_source in count
    bare_indexed_zip = supports_location_lookup and set(extra) == {"zip_code", "codes"} and extra["codes"] == ""
    assert ("ORDER BY npi + 0" in selection) == (computed or bare_indexed_zip)
    assert ("taxonomy_prefix AS MATERIALIZED" in selection) == bounded
    if bounded:
        assert "bucket_members OFFSET 0" in selection
        assert "COUNT(*) FROM prefix_matches" in selection and "COUNT(*) FROM taxonomy_prefix" in selection
        budget = "GREATEST(2048, LEAST(64, (CAST(:limit AS bigint) + CAST(:start AS bigint))) * 128)"
        assert f"LIMIT {budget}" in selection and f"< {budget}" in selection
        assert "SUM(ref.row_count)" in selection and "FROM selected_membership_bound" in selection
        assert "WHERE NOT (SELECT sufficient FROM prefix_complete)" in selection
    elif staged and size != 3:
        assert "SELECT DISTINCT location_key" in selection
        assert selection.count("ref.artifact_candidate_id=") == 2
        assert "native.location_key=key.location_key OFFSET 0" in selection
        assert "custom.location_key=base.location_key" in selection
        assert "c.taxonomy_array && q.int_codes" in selection


@pytest.mark.asyncio
async def test_plain_city_page_stages_state_offices_and_keeps_large_selection_fallback(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi, "_provider_list_connection", lambda *args, **kwargs: FakeAcquire(conn))
    with network_address_read_scope(_lookup_manifest(_layout()), (42,)):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        indexed = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        await npi.get_all(SimpleNamespace(args={"city": "Chicago", "state": "IL", "include_total": "1"}))
    page = next(sql for sql, _ in conn.sql_calls if "page_npis AS" in sql)
    count = next(sql for sql, _ in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql)
    selection, hydration = page.split("sub_s AS (", 1)
    assert "selected_state_offices AS MATERIALIZED" in selection
    assert "NOT (SELECT total <= 500000 FROM selected_office_bound)" in selection
    assert ordinary in selection and ordinary in count
    assert indexed in hydration


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lookup", "extra", "indexed_page"),
    [
        (True, {}, True),
        (True, {"start": "1"}, True),
        (True, {"start": "64"}, False),
        (True, {"phone": "3125550100"}, False),
        (False, {}, False),
    ],
)
async def test_small_network_provider_page_uses_ordered_office_lookup(monkeypatch, lookup, extra, indexed_page):
    conn = RecordingConnection()
    monkeypatch.setattr(npi, "_provider_list_connection", lambda *args, **kwargs: FakeAcquire(conn))
    manifest = _lookup_manifest(_layout()) if lookup else _manifest(_layout())
    with network_address_read_scope(manifest, (42,)):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        indexed = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        response = await npi.get_all(SimpleNamespace(args={"limit": "2", "include_total": "0", **extra}))
    assert response.status == 200
    assert not any("SELECT COUNT(DISTINCT" in sql for sql, _ in conn.sql_calls)
    page = next(sql for sql, _ in conn.sql_calls if "page_npis AS" in sql)
    selection, hydration = page.split("sub_s AS (", 1)
    assert ordinary in selection
    assert (indexed if indexed_page else ordinary) in hydration
    assert ("prefix_page AS MATERIALIZED" in selection) is indexed_page
