# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types

import pytest

from api.endpoint import npi as npi_module
from api.endpoint.npi import (
    active_pharmacists,
    pharmacists_per_pharmacy,
    pharmacists_in_pharmacies,
)


class FakeConnection:
    def __init__(self, first_result=None, all_result=None):
        self._first = first_result
        self._all = all_result
        self._all_calls = 0

    async def first(self, *_args, **_kwargs):
        return self._first

    async def all(self, *_args, **_kwargs):
        self._all_calls += 1
        if isinstance(self._all, list):
            if self._all_calls <= len(self._all):
                return self._all[self._all_calls - 1]
            return self._all[-1] if self._all else []
        return self._all


class FakeAcquire:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


def test_nearby_sql_filters_to_geocoded_rows_for_geo_index():
    sql = npi_module._build_nearby_sql(
        "taxonomy_group = 'x'",
        "",
        "",
        use_taxonomy_filter=False,
        address_table_sql="mrf.entity_address_unified",
    )

    assert "AND a.lat IS NOT NULL" in sql
    assert "AND a.long IS NOT NULL" in sql
    assert ") <-> Geography(" in sql
    assert "a.npi ASC" in sql
    assert "a.address_key ASC" in sql
    assert "a.location_key ASC" in sql
    assert sql.index(") <-> Geography(") < sql.index("LIMIT :limit")


def test_nearby_count_sql_matches_page_filters_and_identity():
    sql = npi_module._build_nearby_count_sql(
        "classification = :classification",
        "\n          AND a.plans_network_array && :plan_network_array",
        "\n            AND d.provider_organization_name ILIKE :q",
        use_taxonomy_filter=True,
        address_table_sql="mrf.entity_address_unified",
        geo_precision_clause="AND COALESCE(a.address_precision, '') <> 'city_zip'",
        bbox_clause=(
            "AND a.lat BETWEEN :min_lat AND :max_lat "
            "AND a.long BETWEEN :min_long AND :max_long"
        ),
    )

    assert "COUNT(DISTINCT (a.npi, a.address_key))" in sql
    assert "a.address_key IS NOT NULL" in sql
    assert "classification = :classification" in sql
    assert "a.taxonomy_array && g.codes" in sql
    assert "a.plans_network_array && :plan_network_array" in sql
    assert "d.provider_organization_name ILIKE :q" in sql
    assert "a.lat BETWEEN :min_lat AND :max_lat" in sql
    assert "LIMIT" not in sql
    assert "ORDER BY" not in sql


def test_npi_address_contact_fallback_derives_canonical_digits_from_raw_fields():
    address = {
        "telephone_number": "1 (312) 555-1212 ext. 34",
        "fax_number": "(312) 555-0199",
        "country_code": "US",
    }

    normalized = npi_module._add_canonical_contact_fields_to_address(address)

    assert normalized["telephone_number"] == "1 (312) 555-1212 ext. 34"
    assert normalized["phone_number"] == "3125551212"
    assert normalized["phone_extension"] == "34"
    assert normalized["fax_number"] == "(312) 555-0199"
    assert normalized["fax_number_digits"] == "3125550199"


def test_npi_address_contact_fallback_does_not_overwrite_canonical_digits():
    address = {
        "telephone_number": "(312) 555-1212",
        "phone_number": "2175550100",
        "fax_number": "(312) 555-0199",
        "fax_number_digits": "2175550199",
        "country_code": "US",
    }

    normalized = npi_module._add_canonical_contact_fields_to_address(address)

    assert normalized["phone_number"] == "2175550100"
    assert normalized["fax_number_digits"] == "2175550199"


def test_npi_address_dedupe_preserves_duplicate_contact_before_fallback():
    addresses = [
        {
            "address_key": "same",
            "type": "primary",
            "first_line": "1 Main St",
            "telephone_number": None,
        },
        {
            "address_key": "same",
            "type": "practice",
            "first_line": "1 Main St",
            "telephone_number": "(217) 555-0100",
        },
    ]

    deduped = npi_module._dedupe_addresses_by_key(addresses)

    assert len(deduped) == 1
    assert deduped[0]["telephone_number"] == "(217) 555-0100"
    assert deduped[0]["phone_number"] == "2175550100"


@pytest.mark.asyncio
async def test_active_pharmacists(monkeypatch):
    fake_conn = FakeConnection(first_result=(10,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "TX"})
    response = await active_pharmacists(request)
    assert json.loads(response.body) == {"count": 10}


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy(monkeypatch):
    fake_conn = FakeConnection(all_result=[[("1", 5)], [("101", "Pharm A", 5), ("102", "Pharm B", 2)]])
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"detailed": "1"})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)
    assert payload["histogram"][0]["pharmacist_group"] == "1"
    assert payload["rows"][0]["pharmacy_npi"] == "101"


@pytest.mark.asyncio
async def test_pharmacists_in_pharmacies(monkeypatch):
    fake_conn = FakeConnection(first_result=(7,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"name_like": "clinic"})
    response = await pharmacists_in_pharmacies(request)
    assert json.loads(response.body) == {"count": 7}


@pytest.mark.asyncio
async def test_pharmacists_in_pharmacies_blank():
    request = types.SimpleNamespace(args={"name_like": ""})
    response = await pharmacists_in_pharmacies(request)
    assert json.loads(response.body) == {"count": 0}


@pytest.mark.asyncio
async def test_active_pharmacists_invalid_state(monkeypatch):
    fake_conn = FakeConnection(first_result=(0,))
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "Texas"})
    response = await active_pharmacists(request)
    assert json.loads(response.body) == {"count": 0}


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_invalid_state(monkeypatch):
    fake_conn = FakeConnection(all_result=[[("1", 5)], [("101", "Pharm A", 5)]])
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={"state": "California"})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)
    assert payload["histogram"][0]["pharmacy_count"] == 5


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_state_and_name(monkeypatch):
    captured = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            # first call histogram, second detail
            if "pharmacist_group" in str(sql):
                captured['sql'] = sql
                captured['params'] = params
                return [("1", 2)]
            return [("201", "Clinic", 2)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))

    request = types.SimpleNamespace(args={'state': 'ny', 'name_like': 'clinic'})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)

    assert payload["histogram"][0]['pharmacist_group'] == '1'
    # name params now use indexed placeholders
    assert captured['params']['state'] == 'NY'
    assert any(v == '%clinic%' for k, v in captured['params'].items() if k.startswith('name_like'))
    sql_text = str(captured['sql'])
    assert 'ph.state_name = :state' in sql_text
    assert 'LIKE :name_like_0' in sql_text


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_uses_unified_address_table_when_compatible(monkeypatch):
    captured = {}

    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return {"npi", "type", "state_name", "telephone_number", "taxonomy_array"}
        return set()

    class RecordingConnection:
        async def all(self, sql, **params):
            captured.setdefault("sql", str(sql))
            captured.setdefault("params", params)
            if "pharmacist_group" in str(sql):
                return [("1", 2)]
            return []

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))

    request = types.SimpleNamespace(args={'state': 'ny'})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)

    assert payload["histogram"][0]['pharmacist_group'] == '1'
    sql_text = captured["sql"]
    assert "FROM mrf.entity_address_unified AS a" in sql_text
    assert "FROM mrf.npi_address AS a" not in sql_text


@pytest.mark.asyncio
async def test_pharmacists_per_pharmacy_full_groups(monkeypatch):
    expected_groups = ['25+'] + [str(i) for i in range(25, 0, -1)]
    fake_rows = [(group, idx) for idx, group in enumerate(expected_groups, start=1)]

    fake_conn = FakeConnection(all_result=[fake_rows, [("999", "Any", 1)]])
    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(fake_conn))

    request = types.SimpleNamespace(args={})
    response = await pharmacists_per_pharmacy(request)
    payload = json.loads(response.body)

    assert [entry['pharmacist_group'] for entry in payload["histogram"]] == expected_groups
    assert [entry['pharmacy_count'] for entry in payload["histogram"]] == list(range(1, len(expected_groups) + 1))
