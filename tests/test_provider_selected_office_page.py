# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify selected office discovery through the provider list entry point."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi
from api.network_address_scope import network_address_read_scope, scoped_address_relation_sql
from tests.network_compact_address_postgres_support import _layout, _manifest
from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size,extra,staged",
    [
        (4, {}, True),
        (5, {}, True),
        (3, {}, False),
        (6, {}, False),
        (4, {"primary_only": "false"}, False),
        (4, {"state": "NY"}, False),
        (5, {"codes": "207Q00000X,208D00000X"}, False),
        (4, {"q": "clinic"}, False),
        (4, {"classification": "Family Medicine"}, False),
        (4, {"plan_network": "123"}, False),
    ],
)
async def test_selected_office_page(monkeypatch, size, extra, staged):
    conn = RecordingConnection()
    monkeypatch.setattr(npi, "_provider_list_connection", lambda *args, **kwargs: FakeAcquire(conn))
    with network_address_read_scope(_manifest(_layout()), tuple(range(1, size + 1))):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value=ordinary))
        await npi.get_all(SimpleNamespace(args={"codes": "207Q00000X", "include_total": "1", **extra}))
    page = next(sql for sql, _ in conn.sql_calls if "page_npis AS" in sql)
    count = next(sql for sql, _ in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql)
    assert ("selected_office_keys AS MATERIALIZED" in page) == staged
    assert "selected_office_keys" not in count
    selection, hydration = page.split("sub_s AS (", 1)
    assert ordinary in hydration and "selected_office_keys" not in hydration
    if staged:
        assert "SELECT DISTINCT location_key" in selection
        assert selection.count("ref.artifact_candidate_id=") == 2
        assert "native.location_key=key.location_key OFFSET 0" in selection
        assert "custom.location_key=base.location_key" in selection
        assert "c.taxonomy_array && q.int_codes" in selection and "ORDER BY npi + 0" in selection
