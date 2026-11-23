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
        monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value=ordinary))
        await npi.get_all(SimpleNamespace(args={"codes": "207Q00000X", "include_total": "1", **extra}))
    page = next(sql for sql, _ in conn.sql_calls if "page_npis AS" in sql)
    count = next(sql for sql, _ in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql)
    bounded = size == 3 and staged and supports_location_lookup
    assert ("selected_office_keys AS MATERIALIZED" in page) == (bounded or staged and size != 3)
    assert "selected_office_keys" not in count
    selection, hydration = page.split("sub_s AS (", 1)
    assert ordinary in hydration and "selected_office_keys" not in hydration
    assert ("ORDER BY npi + 0" in selection) == computed
    assert ("taxonomy_prefix AS MATERIALIZED" in selection) == bounded
    assert ("selected_slice_buckets_0 AS MATERIALIZED" in selection) == bounded
    assert "selected_slice_buckets" not in hydration and "selected_slice_buckets" not in count
    if bounded:
        assert "bucket_members OFFSET 0" in selection
        assert "FROM selected_slice_buckets_0" in selection
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
