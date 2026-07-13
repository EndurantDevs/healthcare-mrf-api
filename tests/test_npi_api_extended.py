# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
import types
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from db.models import NPIAddress, NPIData, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIDataTaxonomyGroup
from db.models import AddressArchive


class FakeConnection:
    def __init__(self, responses):
        self._responses = list(responses)

    async def all(self, *_args, **_kwargs):
        return self._responses.pop(0)

    async def first(self, *_args, **_kwargs):
        return self._responses.pop(0)


class FakeAcquire:
    def __init__(self, connection):
        self._connection = connection

    async def __aenter__(self):
        return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_compute_npi_counts(monkeypatch):
    class ScalarDB:
        def __init__(self):
            self.values = [10, 20]

        async def scalar(self, *_args, **_kwargs):
            return self.values.pop(0)

    monkeypatch.setattr(npi_module, "db", ScalarDB())
    counts = await npi_module._compute_npi_counts()
    assert tuple(counts) == (10, 20)


@pytest.mark.asyncio
async def test_get_all_count_only(monkeypatch):
    fake_conn = FakeConnection([[(5,)]] )

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(args={"count_only": "1"}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    assert json.loads(response.body) == {"rows": 5}


@pytest.mark.asyncio
async def test_fast_has_insurance_count_global_uses_count_star_and_cache(monkeypatch):
    class FakeResult:
        def scalar(self):
            return 42

    calls = []

    class FakeSession:
        async def execute(self, stmt):
            calls.append(str(stmt))
            return FakeResult()

    class FakeSessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(npi_module, "db", types.SimpleNamespace(session=lambda: FakeSessionContext()))
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi_module, "_NPI_HAS_INSURANCE_TOTAL_CACHE", {})

    first = await npi_module._fast_has_insurance_count(None, None)
    second = await npi_module._fast_has_insurance_count(None, None)

    assert first == 42
    assert second == 42
    assert len(calls) == 1
    assert "count(*)" in calls[0].lower()


@pytest.mark.asyncio
async def test_fast_has_insurance_count_city_uses_distinct(monkeypatch):
    class FakeResult:
        def scalar(self):
            return 7

    calls = []

    class FakeSession:
        async def execute(self, stmt):
            calls.append(str(stmt))
            return FakeResult()

    class FakeSessionContext:
        async def __aenter__(self):
            return FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(npi_module, "db", types.SimpleNamespace(session=lambda: FakeSessionContext()))
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(npi_module, "_NPI_HAS_INSURANCE_TOTAL_CACHE", {})

    value = await npi_module._fast_has_insurance_count("MIAMI", None)
    assert value == 7
    assert len(calls) == 1
    assert "count(distinct" in calls[0].lower()


def _build_result_row(npi_value: int) -> list:
    row = [npi_value]
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "do_business_as":
            row.append(['DBA'])
        else:
            row.append(f"data_{column.key}")
    for column in NPIAddress.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "checksum":
            row.append(1)
        else:
            row.append(f"addr_{column.key}")
    for column in NPIDataTaxonomy.__table__.columns:
        if column.key in {"npi", "checksum"}:
            row.append(None)
        else:
            row.append(f"tax_{column.key}")
    return row


def _set_result_address_column(row: list, key: str, value):
    for idx, column in enumerate(NPIAddress.__table__.columns):
        if column.key == key:
            row[1 + len(NPIData.__table__.columns) + idx] = value
            return
    raise AssertionError(f"Unknown NPIAddress column: {key}")


def _set_near_address_column(row: list, key: str, value):
    for idx, column in enumerate(NPIAddress.__table__.columns):
        if column.key == key:
            row[2 + idx] = value
            return
    raise AssertionError(f"Unknown NPIAddress column: {key}")


def test_dedupe_addresses_keeps_distinct_address_sites():
    addresses = [
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000002",
            "first_line": "1 Main St",
            "type": "primary",
        },
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000003",
            "first_line": "1 Main St",
            "type": "primary",
        },
        {
            "address_key": "00000000-0000-0000-0000-000000000001",
            "premise_key": "00000000-0000-0000-0000-000000000002",
            "first_line": "1 Main St",
            "type": "secondary",
            "telephone_number": "3125551212",
        },
    ]

    deduped = npi_module._dedupe_addresses_by_key(addresses)

    assert len(deduped) == 2
    assert {
        address.get("premise_key")
        for address in deduped
    } == {
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
    }
    merged = next(
        address
        for address in deduped
        if address.get("premise_key") == "00000000-0000-0000-0000-000000000002"
    )
    assert merged["telephone_number"] == "3125551212"


@pytest.mark.asyncio
async def test_get_all_returns_rows(monkeypatch):
    result_row = _build_result_row(1234567890)
    _set_result_address_column(result_row, "telephone_number", "1 (312) 555-1212 ext. 44")
    _set_result_address_column(result_row, "fax_number", "(312) 555-0199")
    _set_result_address_column(result_row, "phone_number", None)
    _set_result_address_column(result_row, "phone_extension", None)
    _set_result_address_column(result_row, "fax_number_digits", None)
    _set_result_address_column(result_row, "country_code", "US")
    connections = [
        FakeConnection([[(2,)]]),
        FakeConnection([[result_row]]),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "limit": "1",
            "classification": "Test",
            "q": "Doc",
            "plan_network": "10,20",
            "has_insurance": "1",
            "city": "Chicago",
            "state": "il",
            "codes": "1,2",
            "section": "A",
            "display_name": "Display",
            "specialization": "Spec",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload["total"] == 2
    assert payload["rows"][0]["taxonomy_list"]
    assert payload["rows"][0]["do_business_as"] == ['DBA']
    assert payload["rows"][0]["address_key"] == "addr_address_key"
    assert payload["rows"][0]["phone_number"] == "3125551212"
    assert payload["rows"][0]["phone_extension"] == "44"
    assert payload["rows"][0]["fax_number_digits"] == "3125550199"


def _build_near_row(npi_value: int) -> list:
    row = [npi_value, 0.5]
    for column in NPIAddress.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "checksum":
            row.append(1)
        else:
            row.append(f"addr_{column.key}")
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row.append(npi_value)
        elif column.key == "do_business_as":
            row.append(['DBA'])
        else:
            row.append(f"data_{column.key}")
    for column in NPIDataTaxonomy.__table__.columns:
        if column.key in {"npi", "checksum"}:
            row.append(None)
        else:
            row.append(f"tax_{column.key}")
    return row


@pytest.mark.asyncio
async def test_get_near_npi(monkeypatch):
    near_row = _build_near_row(1112223334)
    _set_near_address_column(near_row, "telephone_number", "(217) 555-0100")
    _set_near_address_column(near_row, "phone_number", None)
    _set_near_address_column(near_row, "country_code", "US")
    responses = [
        [{"intptlat": "41.0", "intptlon": "-87.0"}],
        [near_row, _build_near_row(1112223334)],
    ]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={"zip_codes": "60601", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    payload = json.loads(response.body)
    assert len(payload) == 1
    assert payload[0]["distance"] == 0.5
    assert payload[0]["taxonomy_list"]
    assert payload[0]["do_business_as"] == ['DBA']
    assert payload[0]["phone_number"] == "2175550100"


@pytest.mark.asyncio
async def test_get_near_npi_with_lat_long_includes_bbox_params(monkeypatch):
    captured = {}

    class RecordingConnection:
        async def all(self, _sql, **params):
            captured.update(params)
            return [_build_near_row(1112223334)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "radius": "10", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    payload = json.loads(response.body)
    assert len(payload) == 1
    assert "min_lat" in captured
    assert "max_lat" in captured
    assert "min_long" in captured
    assert "max_long" in captured


@pytest.mark.asyncio
async def test_get_near_npi_uses_unified_address_table_when_compatible(monkeypatch):
    captured = {}

    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys()
        return set()

    class RecordingConnection:
        async def all(self, sql, **params):
            captured["sql"] = str(sql)
            captured["params"] = params
            return []

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "radius": "10", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    assert json.loads(response.body) == []
    assert "FROM mrf.entity_address_unified AS a" in captured["sql"]
    assert "FROM mrf.npi_address AS a" not in captured["sql"]
    assert "COALESCE(a.address_precision, '') <> 'city_zip'" in captured["sql"]
    assert "min_lat" in captured["params"]


@pytest.mark.asyncio
async def test_address_serving_table_falls_back_to_legacy_when_unified_incompatible(monkeypatch):
    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return {"npi"}
        return set()

    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)

    table_name = await npi_module._address_serving_table_sql({"npi", "type"})
    assert table_name == "mrf.npi_address"


@pytest.mark.asyncio
async def test_address_serving_table_uses_legacy_when_explicit(monkeypatch):
    async def fake_table_columns(*_args, **_kwargs):
        raise AssertionError("legacy mode must not probe unified table columns")

    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)

    table_name = await npi_module._address_serving_table_sql({"npi", "type"})
    assert table_name == "mrf.npi_address"


@pytest.mark.asyncio
async def test_get_full_taxonomy_list(monkeypatch):
    class FakeTaxonomy:
        def to_json_dict(self):
            return {"healthcare_provider_taxonomy_code": "123"}

    class FakeNucc:
        def to_json_dict(self):
            return {"code": "123"}

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return types.SimpleNamespace(all=lambda: [(FakeTaxonomy(), FakeNucc())])

    monkeypatch.setattr(npi_module, "db", FakeDB())
    response = await npi_module.get_full_taxonomy_list(types.SimpleNamespace(), "123")
    payload = json.loads(response.body)
    assert payload[0]["nucc_taxonomy"]["code"] == "123"


@pytest.mark.asyncio
async def test_get_plans_by_npi(monkeypatch):
    class FakeQuery:
        def __init__(self, rows):
            self._rows = rows

        def where(self, *_args, **_kwargs):
            return self

        def order_by(self, *_args, **_kwargs):
            return self

        async def iterate(self):
            for row in self._rows:
                yield row

    plan_entry = types.SimpleNamespace(to_json_dict=lambda: {"plan": 1})
    issuer_entry = types.SimpleNamespace(to_json_dict=lambda: {"issuer": 2})
    monkeypatch.setattr(
        npi_module,
        "db",
        types.SimpleNamespace(select=lambda *_args, **_kwargs: FakeQuery([(plan_entry, issuer_entry)])),
    )
    response = await npi_module.get_plans_by_npi(types.SimpleNamespace(), "123")
    payload = json.loads(response.body)
    assert payload["npi_data"][0]["issuer_info"]["issuer"] == 2


def test_public_nested_taxonomy_rows_hide_internal_identity_and_dedupe():
    rows = [
        {
            "npi": 1194956268,
            "checksum": -1,
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "npi": 1194956268,
            "checksum": -2,
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "npi": 1194956268,
            "checksum": -3,
            "healthcare_provider_taxonomy_code": "2085R0202X",
            "healthcare_provider_primary_taxonomy_switch": "N",
        },
    ]

    payload = npi_module._public_nested_taxonomy_rows(rows)

    assert payload == [
        {
            "healthcare_provider_taxonomy_code": "261QM1200X",
            "healthcare_provider_primary_taxonomy_switch": "Y",
        },
        {
            "healthcare_provider_taxonomy_code": "2085R0202X",
            "healthcare_provider_primary_taxonomy_switch": "N",
        },
    ]
    assert all("npi" not in row and "checksum" not in row for row in payload)


@pytest.mark.asyncio
async def test_build_npi_details(monkeypatch):
    row_values = []
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row_values.append(1234567890)
        else:
            row_values.append(f"data_{column.key}")
    row_values.append([{"npi": 1234567890, "checksum": 11, "taxonomy": 1}])
    row_values.append([{"npi": 1234567890, "checksum": 12, "group": 2}])
    row_values.append([{"checksum": 1, "type": "primary"}])

    class FakeSelect:
        def __init__(self, rows):
            self._rows = rows

        def select_from(self, *_args, **_kwargs):
            return self

        def where(self, *_args, **_kwargs):
            return self

        def group_by(self, *_args, **_kwargs):
            return self

        async def all(self):
            return [self._rows]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(None)

        def select(self, *_args, **_kwargs):
            return FakeSelect(row_values)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    payload = await npi_module._build_npi_details(1234567890)
    assert payload["taxonomy_list"]
    assert payload["taxonomy_list"] == [{"taxonomy": 1}]
    assert payload["taxonomy_group_list"]
    assert payload["taxonomy_group_list"] == [{"group": 2}]
    assert payload["address_list"]


@pytest.mark.asyncio
async def test_fetch_other_names(monkeypatch):
    class FakeRow:
        def to_json_dict(self):
            return {
                "npi": 1,
                "checksum": 2,
                "other_provider_identifier": "ALT",
            }

    class FakeDB:
        async def execute(self, *_args, **_kwargs):
            return types.SimpleNamespace(scalars=lambda: [FakeRow()])

    monkeypatch.setattr(npi_module, "db", FakeDB())
    rows = await npi_module._fetch_other_names(1)
    assert rows == [{"other_provider_identifier": "ALT"}]


class FakeUpdate:
    def where(self, *_args, **_kwargs):
        return self

    def values(self, *_args, **_kwargs):
        return self

    async def status(self):
        return None


class FakeInsert:
    def values(self, obj):
        self.payload = obj
        return self

    def on_conflict_do_update(self, **_kwargs):
        return self

    async def status(self):
        return None


def _make_address_row():
    data = {}
    for column in AddressArchive.__table__.columns:
        if column.key == "checksum":
            data[column.key] = 1
        elif column.key == "lat":
            data[column.key] = 41.0
        elif column.key == "long":
            data[column.key] = -87.0
        else:
            data[column.key] = f"archive_{column.key}"
    return types.SimpleNamespace(**data)


def _make_v2_archive_row(
    *,
    lat=42.0,
    long=-88.0,
    formatted_address="v2 address",
    place_id="v2-place",
    geo_source=None,
):
    return types.SimpleNamespace(
        _mapping={
            "lat": lat,
            "long": long,
            "formatted_address": formatted_address,
            "place_id": place_id,
            "geo_source": geo_source,
        }
    )


@pytest.mark.asyncio
async def test_get_npi_geocode_mapbox(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 1,
                    "first_line": "10 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "606011234",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    download_responses = [
        json.dumps(
            {
                "features": [
                    {
                        "geometry": {"coordinates": [-87.1, 41.1]},
                        "place_name": "Chicago, IL",
                    }
                ]
            }
        )
    ]

    async def fake_download(*_args, **_kwargs):
        return download_responses.pop(0)

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row()]

        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, *_args, **_kwargs):
            return self.scalar_values.pop(0)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, "db", fake_db)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {
                "NPI_API_UPDATE_GEOCODE": True,
                "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
                "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
                "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
                "GEOCODE_MAPBOX_STYLE_ADDITIONAL_QUERY_PARAMS": "language=en",
            }

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(
        args={"force_address_update": "1"},
        app=FakeApp(),
    )
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    assert payload["address_list"][0]["lat"] == 41.1
    assert payload["address_list"][0]["geo_source"] == "mapbox"
    await tasks[0]
    assert hasattr(insert, "payload")


@pytest.mark.asyncio
async def test_get_npi_geocode_omits_null_address_parts(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 1,
                    "first_line": None,
                    "second_line": None,
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": None,
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    requested_urls = []

    async def fake_download(url, *_args, **_kwargs):
        requested_urls.append(url)
        return json.dumps(
            {
                "features": [
                    {
                        "geometry": {"coordinates": [-87.1, 41.1]},
                        "place_name": "Chicago, IL",
                    }
                ]
            }
        )

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    class FakeDB:
        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            return FakeInsert()

        async def scalar(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module.random, "choice", lambda seq: seq[0])

    class FakeApp:
        def __init__(self):
            self.config = {
                "NPI_API_UPDATE_GEOCODE": True,
                "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
                "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
                "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
            }

        def add_task(self, coro):
            if asyncio.iscoroutine(coro):
                coro.close()

    request = types.SimpleNamespace(args={"force_address_update": "1"}, app=FakeApp())
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["address_list"][0]["lat"] == 41.1
    assert requested_urls
    assert "None" not in requested_urls[0]
    assert "Chicago" in requested_urls[0]


@pytest.mark.asyncio
async def test_get_npi_geocode_google(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 2,
                    "first_line": "20 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))

    download_responses = [
        json.dumps({"features": []}),
        json.dumps(
            {
                "results": [
                    {
                        "geometry": {"location": {"lat": 41.2, "lng": -87.2}},
                        "formatted_address": "Chicago, IL",
                        "place_id": "abc",
                    }
                ]
            }
        ),
    ]

    async def fake_download(*_args, **_kwargs):
        return download_responses.pop(0)

    monkeypatch.setattr(npi_module, "download_it", fake_download)

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row()]

        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, *_args, **_kwargs):
            return self.scalar_values.pop(0)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, "db", fake_db)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {
                "NPI_API_UPDATE_GEOCODE": True,
                "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
                "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
                "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
                "GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM": "address",
                "GEOCODE_GOOGLE_STYLE_KEY_PARAM": "key",
                "GEOCODE_GOOGLE_STYLE_KEY": "secret",
                "GEOCODE_GOOGLE_STYLE_URL": "https://mock-google",
                "GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS": "region=us",
            }

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(
        args={"force_address_update": "1"},
        app=FakeApp(),
    )
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    assert payload["address_list"][0]["lat"] == 41.2
    assert payload["address_list"][0]["geo_source"] == "google"
    await tasks[0]
    assert hasattr(insert, "payload")


@pytest.mark.asyncio
async def test_get_npi_geocode_openaddresses_before_paid_providers(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 4,
                    "first_line": "30 Main Street",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "download_it",
        AsyncMock(side_effect=AssertionError("paid geocoder should not be called")),
    )

    update = FakeUpdate()
    insert = FakeInsert()

    class FakeDB:
        def update(self, *_args, **_kwargs):
            return update

        def insert(self, *_args, **_kwargs):
            return insert

        async def scalar(self, query, **_kwargs):
            if isinstance(query, str) and "to_regclass" in query:
                return "mrf.openaddresses_geocode"
            return _make_address_row()

        async def first(self, sql, **_kwargs):
            assert "openaddresses_geocode" in sql
            return types.SimpleNamespace(
                _mapping={
                    "long": -87.3,
                    "lat": 41.3,
                    "formatted_address": "30 Main Street, Chicago, IL 60601",
                    "place_id": "oa-1",
                    "geo_source": "openaddresses",
                    "geocode_source": "openaddresses_exact",
                    "geocode_quality": "rooftop",
                }
            )

    monkeypatch.setattr(npi_module, "db", FakeDB())

    tasks = []

    class FakeApp:
        config = {
            "NPI_API_UPDATE_GEOCODE": True,
            "GEOCODE_MAPBOX_STYLE_KEY_PARAM": "access_token",
            "GEOCODE_MAPBOX_STYLE_KEY": "[\"token\"]",
            "GEOCODE_MAPBOX_STYLE_URL": "https://mock-map/",
            "GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM": "address",
            "GEOCODE_GOOGLE_STYLE_KEY_PARAM": "key",
            "GEOCODE_GOOGLE_STYLE_KEY": "secret",
            "GEOCODE_GOOGLE_STYLE_URL": "https://mock-google",
            "GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS": "region=us",
        }

        def add_task(self, coro):
            tasks.append(coro)
            if asyncio.iscoroutine(coro):
                coro.close()

    request = types.SimpleNamespace(
        args={"force_address_update": "1"},
        app=FakeApp(),
    )
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    address = payload["address_list"][0]
    assert address["lat"] == 41.3
    assert address["geo_source"] == "openaddresses"
    assert address["geocode_source"] == "openaddresses_exact"
    assert tasks


@pytest.mark.asyncio
async def test_get_all_full_taxonomy(monkeypatch):
    connections = [FakeConnection([[(1234, 7)]])]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "response_format": "full_taxonomy",
            "classification": "Pharmacy",
            "codes": "1234",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload["rows"] == {"1234": 7}


@pytest.mark.asyncio
async def test_get_all_response_format_default(monkeypatch):
    connections = [FakeConnection([[("Pharmacist", 3)]])]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "response_format": "classification",
            "codes": "1234",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload["rows"] == {"Pharmacist": 3}


@pytest.mark.asyncio
async def test_get_near_npi_with_filters(monkeypatch):
    row = _build_near_row(5556667778)
    responses = [[row]]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "exclude_npi": "1234567890",
            "plan_network": "1,2",
            "q": "Clinic",
            "classification": "Pharmacy",
            "section": "Sec",
            "display_name": "Name",
            "codes": "1,2",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    payload = json.loads(response.body)
    assert payload[0]["npi"] == 5556667778


@pytest.mark.asyncio
async def test_get_near_npi_rejects_name_like_legacy_alias():
    request = types.SimpleNamespace(
        args={"name_like": "Clinic", "zip_codes": "60601"},
        app=types.SimpleNamespace(),
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_near_npi(request)


@pytest.mark.asyncio
async def test_get_near_npi_applies_procedure_and_medication_filters(monkeypatch):
    captured = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            captured["sql"] = str(sql)
            captured["params"] = dict(params)
            return [_build_near_row(1112223334)]

        async def first(self, *_args, **_kwargs):
            return None

    class FakeDB:
        def acquire(self):
            return FakeAcquire(RecordingConnection())

    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        lambda: asyncio.sleep(0, result={
            "npi_procedures_array_available": True,
            "npi_medications_array_available": True,
            "pricing_provider_procedure_available": False,
            "pricing_provider_prescription_available": False,
        }),
    )

    request = types.SimpleNamespace(
        args={
            "long": "-87.0",
            "lat": "41.0",
            "procedure_codes": "1001,1002",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001,2002",
            "medication_code_system": "HP_RX_CODE",
            "year": "2023",
            "limit": "1",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    payload = json.loads(response.body)

    assert len(payload) == 1
    assert "a.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" in captured["sql"]
    assert "a.procedures_array @> ARRAY[:procedure_code_1]::INTEGER[]" in captured["sql"]
    assert "a.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" in captured["sql"]
    assert "a.medications_array @> ARRAY[:medication_code_1]::INTEGER[]" in captured["sql"]
    assert captured["params"]["procedure_code_0"] == 1001
    assert captured["params"]["procedure_code_1"] == 1002
    assert captured["params"]["medication_code_0"] == 2001
    assert captured["params"]["medication_code_1"] == 2002
    assert captured["params"]["filter_year"] == 2023


@pytest.mark.asyncio
async def test_get_near_npi_rejects_invalid_medication_code_system():
    request = types.SimpleNamespace(
        args={"lat": "41.0", "long": "-87.0", "medication_codes": "2001", "medication_code_system": "ATC"},
        app=types.SimpleNamespace(),
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_near_npi(request)


@pytest.mark.asyncio
async def test_get_near_npi_does_not_crash_on_short_positional_rows(monkeypatch):
    short_row = _build_near_row(1112223334)[:-3]
    responses = [[short_row]]
    fake_conn = FakeConnection(responses)

    class FakeDB:
        def acquire(self):
            return FakeAcquire(fake_conn)

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={"long": "-87.0", "lat": "41.0", "limit": "1"},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_near_npi(request)
    payload = json.loads(response.body)
    assert isinstance(payload, list)
    assert len(payload) == 1
    assert payload[0]["npi"] == 1112223334


@pytest.mark.asyncio
async def test_get_npi_uses_cached_address(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 3,
                    "first_line": "30 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected call")))

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            return types.SimpleNamespace(long=-80.0, lat=40.0, formatted_address="Cached", place_id="pid")

    monkeypatch.setattr(npi_module, "db", FakeDB())

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False})
    )
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)
    assert payload["address_list"][0]["lat"] == 40.0


@pytest.mark.asyncio
async def test_get_npi_sync_geocode_disabled_skips_live_geocode_and_caches_latless(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_SYNC_GEOCODE", "false")
    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE", "false")
    calls = 0

    async def fake_build(_npi, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 4,
                    "first_line": "10 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError("stored geocode lookup should not execute")

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected geocode call")))

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": True}),
    )
    first = await npi_module.get_npi(request, "1518379601")
    second = await npi_module.get_npi(request, "1518379601")

    payload = json.loads(first.body)
    assert payload["address_list"][0]["lat"] is None
    assert first.body == second.body
    assert calls == 1


@pytest.mark.asyncio
async def test_get_npi_query_flags_disable_stored_and_live_geocode(monkeypatch):
    captured_kwargs = {}

    async def fake_build(_npi, **kwargs):
        captured_kwargs.update(kwargs)
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "checksum": 4,
                    "first_line": "10 Main",
                    "second_line": "",
                    "city_name": "Chicago",
                    "state_name": "IL",
                    "postal_code": "60601",
                    "lat": None,
                    "long": None,
                }
            ],
        }

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError("stored geocode lookup should not execute")

    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_SYNC_GEOCODE", "true")
    monkeypatch.setenv("HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE", "true")
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_detail",
        AsyncMock(return_value={"summary": None, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "db", FakeDB())
    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=AssertionError("unexpected geocode call")))

    request = types.SimpleNamespace(
        args={
            "view": "summary",
            "sync_geocode": "0",
            "lookup_stored_geocode": "0",
            "include_address_total": "0",
        },
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": True}),
    )
    response = await npi_module.get_npi(request, "1518379601")
    payload = json.loads(response.body)

    assert payload["address_list"][0]["lat"] is None
    assert captured_kwargs["include_address_total"] is False


@pytest.mark.asyncio
async def test_get_npi_uses_response_cache(monkeypatch):
    calls = 0

    async def fake_build(_npi, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [{"checksum": 3, "lat": 40.0, "long": -80.0}],
        }

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    first = await npi_module.get_npi(request, "1518379601")
    second = await npi_module.get_npi(request, "1518379601")

    assert json.loads(first.body)["npi"] == 1518379601
    assert first.body == second.body
    assert calls == 1


def test_npi_detail_cache_key_tracks_exact_address_key():
    common_options_by_name = {
        "npi": 1518379601,
        "view": "full",
        "include_chain": False,
        "extra_info": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
    }

    unfiltered_key = npi_module._npi_detail_cache_key(
        **common_options_by_name,
    )
    exact_address_key = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        address_key="00000000-0000-0000-0000-000000000002",
    )

    assert unfiltered_key != exact_address_key


def _address_key_detail_payloads(address_key):
    """Return deterministic base and overlay fixtures for exact-address detail."""
    return (
        {
            "npi": 1518379601,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [
                {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "address_precision": "street",
                    "type": "primary",
                    "lat": 41.0,
                    "long": -87.0,
                },
                {
                    "address_key": address_key,
                    "address_precision": "street",
                    "type": "practice",
                    "lat": 42.0,
                    "long": -88.0,
                },
            ],
        },
        [
            {
                "address_key": address_key,
                "address_precision": "street",
                "type": "practice",
                "lat": 42.0,
                "long": -88.0,
                "address_sources": ["provider_directory_fhir"],
                "source_record_ids": ["provider_directory_fhir:role:source:role-1"],
                "provider_directory_sources": [
                    {
                        "source_ids": ["pdfhir_example"],
                        "practitioner_roles": [
                            {
                                "resource_type": "PractitionerRole",
                                "resource_id": "role-1",
                            }
                        ],
                    }
                ],
            },
            {
                "address_key": "00000000-0000-0000-0000-000000000003",
                "address_precision": "street",
                "type": "practice",
                "lat": 43.0,
                "long": -89.0,
            },
        ],
    )


@pytest.mark.asyncio
async def test_get_npi_address_key_returns_exact_address_with_typed_evidence(
    monkeypatch,
):
    address_key = "00000000-0000-0000-0000-000000000002"
    detail_payload_map, overlay_payload_list = _address_key_detail_payloads(address_key)
    build_details = AsyncMock(
        side_effect=[deepcopy(detail_payload_map), deepcopy(detail_payload_map)]
    )
    fetch_overlay = AsyncMock(
        side_effect=[deepcopy(overlay_payload_list), deepcopy(overlay_payload_list)]
    )
    monkeypatch.setattr(npi_module, "_build_npi_details", build_details)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay", fetch_overlay)
    monkeypatch.setattr(npi_module, "_attach_provider_directory_source_details", AsyncMock())
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0.0)

    app = types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False})
    common_arg_map = {
        "include_evidence": "true",
        "include_profile": "false",
        "sync_geocode": "false",
        "lookup_stored_geocode": "false",
    }
    default_response = await npi_module.get_npi(
        types.SimpleNamespace(args=common_arg_map, app=app),
        "1518379601",
    )
    exact_response = await npi_module.get_npi(
        types.SimpleNamespace(
            args={**common_arg_map, "address_key": address_key},
            app=app,
        ),
        "1518379601",
    )

    default_addresses = json.loads(default_response.body)["address_list"]
    exact_addresses = json.loads(exact_response.body)["address_list"]
    assert [address["address_key"] for address in default_addresses] == [
        "00000000-0000-0000-0000-000000000001",
        address_key,
        "00000000-0000-0000-0000-000000000003",
    ]
    assert [address["address_key"] for address in exact_addresses] == [address_key]
    assert exact_addresses[0]["provider_directory_sources"][0]["practitioner_roles"] == [
        {"resource_type": "PractitionerRole", "resource_id": "role-1"}
    ]
    assert [
        call.kwargs["address_key"] for call in build_details.await_args_list
    ] == [None, address_key]
    assert [
        call.kwargs["address_key"] for call in fetch_overlay.await_args_list
    ] == [None, address_key]


@pytest.mark.asyncio
async def test_get_npi_force_address_update_bypasses_response_cache(monkeypatch):
    calls = 0

    async def fake_build(_npi, **_kwargs):
        nonlocal calls
        calls += 1
        return {
            "npi": _npi,
            "taxonomy_list": [],
            "taxonomy_group_list": [],
            "do_business_as": [],
            "address_list": [{"checksum": 3, "lat": 40.0, "long": -80.0}],
        }

    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE", npi_module.OrderedDict())
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(npi_module, "_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", 8)
    monkeypatch.setattr(npi_module, "_build_npi_details", fake_build)
    monkeypatch.setattr(npi_module, "_fetch_other_names", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {}, "ffs_visibility": {}}),
    )

    request = types.SimpleNamespace(
        args={},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    await npi_module.get_npi(request, "1518379601")

    force_request = types.SimpleNamespace(
        args={"force_address_update": "1"},
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    await npi_module.get_npi(force_request, "1518379601")

    assert calls == 2


@pytest.mark.asyncio
async def test_get_npi_not_found(monkeypatch):
    monkeypatch.setattr(npi_module, "_build_npi_details", AsyncMock(return_value={}))
    request = types.SimpleNamespace(args={})
    with pytest.raises(sanic.exceptions.NotFound):
        await npi_module.get_npi(request, "123")


@pytest.mark.asyncio
async def test_get_all_count_only_filters(monkeypatch):
    connections = [FakeConnection([[(5,)]] )]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "count_only": "1",
            "plan_network": "1,2",
            "q": "doc",
            "has_insurance": "1",
            "city": "Chicago",
            "state": "il",
            "classification": "Test",
            "section": "B",
            "display_name": "Display",
            "specialization": "Spec",
            "codes": "1,2",
        },
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload["rows"] == 5


@pytest.mark.asyncio
async def test_get_all_format_full_taxonomy(monkeypatch):
    calls = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            calls['sql'] = sql
            calls['params'] = params
            return [("123", 4)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(args={'count_only': '1', 'format': 'full_taxonomy'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload == {'rows': {'123': 4}}
    assert 'ARRAY[int_code]' in str(calls['sql'])


@pytest.mark.asyncio
async def test_get_all_format_classification(monkeypatch):
    calls = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            calls['sql'] = sql
            calls['params'] = params
            return [("Spec", 7)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(args={'count_only': '1', 'format': 'classification'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload == {'rows': {'Spec': 7}}
    assert 'classification' in str(calls['sql'])


@pytest.mark.asyncio
async def test_get_all_format_all_returns_classification_map(monkeypatch):
    calls = {}

    class RecordingConnection:
        async def all(self, sql, **params):
            calls['sql'] = sql
            calls['params'] = params
            return [("Pharmacy", 12), ("Pharmacist", 33)]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module.db, 'acquire', lambda: FakeAcquire(RecordingConnection()))
    request = types.SimpleNamespace(
        args={'count_only': '1', 'format': 'all', 'state': 'NE', 'has_insurance': '1'},
        app=types.SimpleNamespace(),
    )
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload == {'rows': {'Pharmacy': 12, 'Pharmacist': 33}}
    assert 'GROUP BY q.classification' in str(calls['sql'])
    assert calls['params']['state'] == 'NE'


@pytest.mark.asyncio
async def test_get_all_deduplicates_rows(monkeypatch):
    connections = [
        FakeConnection([[(1,)]]),
        FakeConnection([[ _build_result_row(999), _build_result_row(999) ]]),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, 'db', FakeDB())
    request = types.SimpleNamespace(args={'limit': '1'}, app=types.SimpleNamespace())
    response = await npi_module.get_all(request)
    payload = json.loads(response.body)
    assert payload['total'] == 1
    assert len(payload['rows']) == 1


@pytest.mark.asyncio
async def test_build_npi_details_empty(monkeypatch):
    class FakeQuery:
        def __init__(self, rows):
            self._rows = rows

        def select_from(self, *_args, **_kwargs):
            return self

        def where(self, *_args, **_kwargs):
            return self

        def outerjoin(self, *_args, **_kwargs):
            return self

        def group_by(self, *_args, **_kwargs):
            return self

        def alias(self, *_args, **_kwargs):
            alias_table = table('address_alias', column('npi'), column('type'))
            return alias_table.alias('address_list')

        def order_by(self, *_args, **_kwargs):
            return self

        async def all(self):
            return self._rows

    class DummyAcquire:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeDB:
        def __init__(self):
            self.query = FakeQuery([])

        def select(self, *_args, **_kwargs):
            return self.query

        def acquire(self):
            return DummyAcquire()

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])
    # Keep SQLAlchemy select objects for the NPI-first optimization barrier.

    result = await npi_module._build_npi_details(123)
    assert result == {}


@pytest.mark.asyncio
async def test_get_npi_address_geocode_paths(monkeypatch):
    details = {
        'npi': 123,
        'do_business_as': ['Existing DBA'],
        'taxonomy_list': [],
        'taxonomy_group_list': [],
        'address_list': [
            {
                'npi': 123,
                'type': 'primary',
                'checksum': 1,
                'first_line': '10 Main St',
                'second_line': '',
                'city_name': 'Town',
                'state_name': 'IL',
                'postal_code': '12345',
                'lat': None,
                'long': None,
                'formatted_address': None,
                'plans_network_array': [],
                'taxonomy_array': []
            },
            {
                'npi': 123,
                'type': 'secondary',
                'checksum': 2,
                'first_line': '20 Oak St',
                'second_line': '',
                'city_name': 'Town',
                'state_name': 'IL',
                'postal_code': '123456789',
                'lat': None,
                'long': None,
                'formatted_address': None,
                'plans_network_array': [],
                'taxonomy_array': []
            },
        ],
    }

    mapbox_response = json.dumps({
        'features': [
            {
                'geometry': {'coordinates': [10.0, 20.0]},
                'matching_place_name': 'Match Address'
            }
        ]
    })

    responses = [mapbox_response, Exception('mapbox failure'), Exception('google failure')]

    async def fake_download(url, local_timeout=None):
        value = responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    async def fake_fetch_other_names(_):
        return [{'other_provider_identifier': 'DBA FROM OTHER', 'other_provider_identifier_type_code': '3'}]

    async def fake_scalar(*_args, **_kwargs):
        return None

    class FakeDB:
        async def update(self, *args, **kwargs):
            class _Stmt:
                async def values(self, *a, **kw):
                    return self

                async def status(self):
                    pass
            return _Stmt()

        async def insert(self, *args, **kwargs):
            class _Stmt:
                def values(self, *_a, **_kw):
                    return self

                def on_conflict_do_update(self, *a, **kw):
                    return self

                async def status(self):
                    pass
            return _Stmt()

        async def scalar(self, *_args, **_kwargs):
            return await fake_scalar()

    monkeypatch.setattr(npi_module, '_build_npi_details', AsyncMock(return_value=details))
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(side_effect=fake_fetch_other_names))
    monkeypatch.setattr(npi_module, 'download_it', AsyncMock(side_effect=fake_download))

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])

    class App:
        config = {
            'GEOCODE_MAPBOX_STYLE_URL': 'https://mapbox/',
            'GEOCODE_MAPBOX_STYLE_ADDRESS_PARAM': 'address',
            'GEOCODE_MAPBOX_STYLE_KEY_PARAM': 'key',
            'GEOCODE_MAPBOX_STYLE_KEY': '["key123"]',
            'GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM': 'address',
            'GEOCODE_GOOGLE_STYLE_KEY_PARAM': 'key',
            'GEOCODE_GOOGLE_STYLE_KEY': 'k',
            'GEOCODE_GOOGLE_STYLE_URL': 'https://maps.googleapis.com',
            'NPI_API_UPDATE_GEOCODE': True,
        }

        def add_task(self, coro):
            if asyncio.iscoroutine(coro):
                coro.close()
            self.last_task = coro

    request = types.SimpleNamespace(args={'force_address_update': '1'}, app=App())
    response = await npi_module.get_npi(request, '123')
    payload = json.loads(response.body)
    assert payload['do_business_as'] == ['Existing DBA']
    assert payload['address_list'][0]['formatted_address'] == 'Match Address'


@pytest.mark.asyncio
async def test_get_npi_not_found(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {}

    monkeypatch.setattr(npi_module, '_build_npi_details', AsyncMock(side_effect=fake_build))

    request = types.SimpleNamespace(args={}, app=types.SimpleNamespace())
    with pytest.raises(sanic.exceptions.NotFound):
        await npi_module.get_npi(request, '123')


@pytest.mark.asyncio
async def test_get_npi_update_addr_coordinates_row_missing(monkeypatch):
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 1,
        'first_line': '10 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        payload = {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }
        return payload

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    monkeypatch.setattr(
        npi_module,
        '_fetch_provider_enrichment_summary_detail',
        AsyncMock(return_value={
            'summary': None,
            'enrollments': {
                'ffs_public': [
                    {
                        'address_key': '00000000-0000-0000-0000-000000000002',
                        'address_line_1': '10 Main',
                    }
                ]
            },
        }),
    )
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row(), None]

        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            raise AssertionError('address archive insert should not run when row missing')

        async def scalar(self, *_args, **_kwargs):
            if self.scalar_values:
                return self.scalar_values.pop(0)
            return None

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not execute when archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {'NPI_API_UPDATE_GEOCODE': True}

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 41.0
    assert tasks, 'expected geocode update task'
    await tasks[0]


@pytest.mark.asyncio
async def test_get_npi_update_addr_coordinates_handles_exception(monkeypatch):
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 2,
        'first_line': '11 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))
    monkeypatch.setattr(npi_module.random, 'choice', lambda seq: seq[0])

    class ErrorInsert:
        def __init__(self):
            self.values_payload = None

        def values(self, obj):
            self.values_payload = obj
            return self

        def on_conflict_do_update(self, **_kwargs):
            return self

        async def status(self):
            raise RuntimeError('boom')

    class FakeDB:
        def __init__(self):
            self.scalar_values = [_make_address_row(), _make_address_row()]

        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            return ErrorInsert()

        async def scalar(self, *_args, **_kwargs):
            if self.scalar_values:
                return self.scalar_values.pop(0)
            return None

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not execute when archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {'NPI_API_UPDATE_GEOCODE': True}

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 41.0
    assert tasks
    # The task should swallow the insert exception
    await tasks[0]


@pytest.mark.asyncio
async def test_get_npi_skip_update_when_lat_present(monkeypatch):
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 3,
        'first_line': '12 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': 40.0,
        'long': -80.0,
        'formatted_address': 'Town, IL',
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def scalar(self, *_args, **_kwargs):
            raise AssertionError('archive lookup should be skipped when lat present')

        def update(self, *_args, **_kwargs):
            raise AssertionError('update should not be invoked')

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when coordinates exist')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    tasks = []

    class FakeApp:
        def __init__(self):
            self.config = {'NPI_API_UPDATE_GEOCODE': True}

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 40.0
    assert tasks == []


@pytest.mark.asyncio
async def test_get_npi_v2_archive_is_disabled_without_cutover_flag(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", raising=False)
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 4,
        'first_line': '13 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def first(self, *_args, **_kwargs):
            raise AssertionError('v2 archive lookup should require explicit cutover flag')

        async def scalar(self, *_args, **_kwargs):
            return _make_address_row()

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when legacy archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no geocode update task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 41.0


@pytest.mark.asyncio
async def test_get_npi_v2_archive_cutover_reads_geocodes_for_concurrent_addresses(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1")
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
    addresses = [
        {
            'npi': 1518379601,
            'type': 'primary',
            'checksum': 10,
            'first_line': '10 Main St',
            'second_line': '',
            'city_name': 'Town',
            'state_name': 'IL',
            'postal_code': '123450000',
            'country_code': 'US',
            'lat': None,
            'long': None,
            'formatted_address': None,
            'plans_network_array': [],
            'taxonomy_array': [],
        },
        {
            'npi': 1518379601,
            'type': 'secondary',
            'checksum': 11,
            'first_line': '11 Main St',
            'second_line': '',
            'city_name': 'Town',
            'state_name': 'IL',
            'postal_code': '123450000',
            'country_code': 'US',
            'lat': None,
            'long': None,
            'formatted_address': None,
            'plans_network_array': [],
            'taxonomy_array': [],
        },
    ]

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address) for address in addresses],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        def __init__(self):
            self.catalog_calls = 0
            self.first_calls = 0

        async def scalar(self, query, **_kwargs):
            if not isinstance(query, str):
                raise AssertionError('legacy checksum archive should not be used when v2 geocode exists')
            self.catalog_calls += 1
            if 'to_regclass' in query:
                await asyncio.sleep(0)
                return 'mrf.address_archive_v2'
            if 'information_schema.columns' in query:
                return True
            if 'to_regprocedure' in query:
                return 'mrf.addr_key_v1(text,text,text,text,text,text)'
            raise AssertionError(query)

        async def first(self, *_args, **_kwargs):
            self.first_calls += 1
            return _make_v2_archive_row(lat=42.0 + self.first_calls)

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when v2 archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no geocode update task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)

    assert [address['lat'] for address in payload['address_list']] == [43.0, 44.0]
    assert fake_db.first_calls == 2
    assert fake_db.catalog_calls == 4


@pytest.mark.asyncio
async def test_get_npi_v2_archive_geocodeless_row_falls_back_to_legacy(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "true")
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 12,
        'first_line': '12 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'country_code': 'US',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        async def scalar(self, query, **_kwargs):
            if isinstance(query, str):
                if 'to_regclass' in query:
                    return 'mrf.address_archive_v2'
                if 'information_schema.columns' in query:
                    return True
                if 'to_regprocedure' in query:
                    return 'mrf.addr_key_v1(text,text,text,text,text,text)'
                raise AssertionError(query)
            return _make_address_row()

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(npi_module, 'db', FakeDB())

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when legacy archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no geocode update task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 41.0


@pytest.mark.asyncio
async def test_get_npi_v2_archive_geocode_write_uses_deduped_address_key_upsert(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "1")
    address_entry = {
        'npi': 1518379601,
        'type': 'primary',
        'checksum': 13,
        'first_line': '13 Main St',
        'second_line': '',
        'city_name': 'Town',
        'state_name': 'IL',
        'postal_code': '123450000',
        'country_code': 'US',
        'lat': None,
        'long': None,
        'formatted_address': None,
        'plans_network_array': [],
        'taxonomy_array': [],
    }

    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [dict(address_entry)],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeDB:
        def __init__(self):
            self.status_sql = []
            self.status_kwargs = []

        async def scalar(self, query, **_kwargs):
            if isinstance(query, str):
                if 'to_regclass' in query:
                    return 'mrf.address_archive_v2'
                if 'information_schema.columns' in query:
                    return True
                if 'to_regprocedure' in query:
                    return 'mrf.addr_key_v1(text,text,text,text,text,text)'
                raise AssertionError(query)
            return types.SimpleNamespace()

        async def first(self, *_args, **_kwargs):
            return _make_v2_archive_row(lat=45.0)

        def update(self, *_args, **_kwargs):
            return FakeUpdate()

        def insert(self, *_args, **_kwargs):
            raise AssertionError('legacy AddressArchive insert should not run during v2 cutover')

        async def status(self, sql, **_kwargs):
            self.status_sql.append(sql)
            self.status_kwargs.append(_kwargs)
            return 1

    fake_db = FakeDB()
    monkeypatch.setattr(npi_module, 'db', fake_db)

    async def fail_download(*_args, **_kwargs):  # pragma: no cover - guard
        raise AssertionError('download should not run when v2 archive hit')

    monkeypatch.setattr(npi_module, 'download_it', fail_download)

    tasks = []

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': True}

        def add_task(self, coro):
            tasks.append(coro)

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['lat'] == 45.0
    assert tasks
    await tasks[0]

    upsert_sql = "\n".join(fake_db.status_sql)
    assert "INSERT INTO mrf.address_archive_v2" in upsert_sql
    assert "SELECT DISTINCT ON" in upsert_sql
    assert "ON CONFLICT (address_key) DO UPDATE" in upsert_sql
    assert "WHERE checksum = :checksum" in upsert_sql
    assert "LEFT(mrf.addr_state_code_v1(state_name), 32)" in upsert_sql
    assert "geo_source, geocode_source, geocode_quality" in upsert_sql
    assert "CAST(:geo_source AS mrf.address_archive_geo_source)" in upsert_sql
    assert "geo_source = COALESCE(mrf.address_archive_v2.geo_source, EXCLUDED.geo_source)" in upsert_sql
    assert fake_db.status_kwargs[-1]["geo_source"] == "google"


@pytest.mark.asyncio
async def test_get_npi_exposes_address_key_and_hides_premise_key(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'primary',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'premise_key': '00000000-0000-0000-0000-000000000002',
                    'lat': 40.0,
                    'long': -80.0,
                    'formatted_address': 'Town, IL',
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'][0]['address_key'] == '00000000-0000-0000-0000-000000000001'
    assert payload['address_list'][0]['address_site_key'] == '00000000-0000-0000-0000-000000000002'
    assert 'premise_key' not in payload['address_list'][0]
    assert 'premise_key' not in json.dumps(payload)


@pytest.mark.asyncio
async def test_get_npi_debug_flags_include_sources_and_evidence(monkeypatch):
    captured_kwargs = {}

    async def fake_build(_npi, **kwargs):
        captured_kwargs.update(kwargs)
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'primary',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'premise_key': '00000000-0000-0000-0000-000000000002',
                    'lat': 40.0,
                    'long': -80.0,
                    'location_key': 'loc-debug',
                    'address_sources': ['npi', 'mrf'],
                    'source_record_ids': ['npi:1518379601', 'mrf:row-1'],
                    'source_count': 2,
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(
        args={'include_sources': 'true', 'include_evidence': 'true'},
        app=FakeApp(),
    )
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    address = payload['address_list'][0]

    assert captured_kwargs['include_sources'] is True
    assert captured_kwargs['include_evidence'] is True
    assert address['address_sources'] == ['npi', 'mrf']
    assert address['source_record_ids'] == ['npi:1518379601', 'mrf:row-1']
    assert address['source_count'] == 2
    assert address['address_key'] == '00000000-0000-0000-0000-000000000001'
    assert address['address_site_key'] == '00000000-0000-0000-0000-000000000002'
    assert 'premise_key' not in address


@pytest.mark.asyncio
async def test_get_npi_hides_provider_directory_source_details_by_default(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'practice',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'address_precision': 'street',
                    'first_line': '1 Main St',
                    'city_name': 'Bloomfield',
                    'state_name': 'CT',
                    'postal_code': '06002',
                    'lat': 41.0,
                    'long': -72.0,
                    'formatted_address': None,
                    'place_id': None,
                    'address_sources': ['provider_directory_fhir'],
                    'source_record_ids': [
                        'provider_directory_fhir:practitioner_role:pdfhir_cigna:role-1:loc-1'
                    ],
                    'source_count': 1,
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    fetch_details = AsyncMock(return_value={
        'pdfhir_cigna': {
            'source': 'provider_directory_fhir',
            'source_id': 'pdfhir_cigna',
            'org_name': 'Cigna',
            'plan_name': 'Commercial',
            'canonical_api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
            'api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
            'auth_type': 'none',
            'last_validated_status': 'valid',
        }
    })

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_provider_directory_source_detail_map', fetch_details)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379602')
    payload = json.loads(response.body)
    address = payload['address_list'][0]

    fetch_details.assert_not_awaited()
    assert 'provider_directory_sources' not in address
    assert address['address_sources'] == ['provider_directory_fhir']
    assert 'source_record_ids' not in address


@pytest.mark.asyncio
async def test_get_npi_include_sources_enriches_provider_directory_source_summary(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [
                {
                    'npi': _npi,
                    'type': 'practice',
                    'checksum': 5,
                    'address_key': '00000000-0000-0000-0000-000000000001',
                    'address_precision': 'street',
                    'first_line': '1 Main St',
                    'city_name': 'Bloomfield',
                    'state_name': 'CT',
                    'postal_code': '06002',
                    'lat': 41.0,
                    'long': -72.0,
                    'formatted_address': None,
                    'place_id': None,
                    'address_sources': ['provider_directory_fhir'],
                    'source_record_ids': [
                        'provider_directory_fhir:practitioner_role:pdfhir_cigna:role-1:loc-1'
                    ],
                    'source_count': 1,
                    'plans_network_array': [],
                    'taxonomy_array': [],
                }
            ],
        }

    source_detail = {
        'source': 'provider_directory_fhir',
        'source_id': 'pdfhir_cigna',
        'endpoint_id': 'pd_endpoint_cigna',
        'org_name': 'Cigna',
        'plan_name': 'Commercial',
        'canonical_api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
        'api_base': 'https://fhir.cigna.com/ProviderDirectory/v1',
        'auth_type': 'none',
        'auth_required': False,
        'requires_api_key': False,
        'credential_name': 'PAYER_DIRECTORY_KEY',
        'headers': {'X-API-Key': 'secret'},
        'token': 'secret-token',
        'last_validated_status': 'valid',
    }
    fetch_details = AsyncMock(return_value={'pdfhir_cigna': source_detail})

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_provider_directory_source_detail_map', fetch_details)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        config = {'NPI_API_UPDATE_GEOCODE': False}

        def add_task(self, coro):  # pragma: no cover - guard
            raise AssertionError('no task expected')

    request = types.SimpleNamespace(args={'include_sources': 'true'}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379602')
    payload = json.loads(response.body)
    address = payload['address_list'][0]

    fetch_details.assert_awaited_once_with(['pdfhir_cigna'], session=None)
    assert address['provider_directory_sources'] == [
        {
            'source': 'provider_directory_fhir',
            'source_ids': ['pdfhir_cigna'],
            'endpoint_id': 'pd_endpoint_cigna',
            'catalog_aliases_verified': False,
            'catalog_aliases': [
                {
                    'source_id': 'pdfhir_cigna',
                    'org_name': 'Cigna',
                    'plan_name': 'Commercial',
                }
            ],
        }
    ]
    source_json = json.dumps(address['provider_directory_sources'])
    endpoint_source = address['provider_directory_sources'][0]
    for sensitive_key in (
        'api_base',
        'canonical_api_base',
        'auth_type',
        'auth_required',
        'requires_api_key',
        'credential_name',
        'headers',
    ):
        assert sensitive_key not in endpoint_source
    assert 'secret-token' not in source_json
    assert address['address_sources'] == ['provider_directory_fhir']
    assert 'source_record_ids' not in address


@pytest.mark.asyncio
async def test_get_npi_address_list_clears_empty_entries(monkeypatch):
    async def fake_build(_npi, **_kwargs):
        return {
            'npi': _npi,
            'do_business_as': [],
            'taxonomy_list': [],
            'taxonomy_group_list': [],
            'address_list': [None],
        }

    monkeypatch.setattr(npi_module, '_build_npi_details', fake_build)
    monkeypatch.setattr(npi_module, '_fetch_other_names', AsyncMock(return_value=[]))

    class FakeApp:
        def __init__(self):
            self.config = {}
            self.tasks = []

        def add_task(self, coro):
            raise AssertionError('no geocode tasks expected when address list empty')

    request = types.SimpleNamespace(args={}, app=FakeApp())
    response = await npi_module.get_npi(request, '1518379601')
    payload = json.loads(response.body)
    assert payload['address_list'] == []


def test_dedupe_addresses_merges_nondiscriminating_site_key_with_fhir_overlay():
    address_key = "00000000-0000-0000-0000-000000000001"
    addresses = [
        {
            "address_key": address_key,
            "address_site_key": address_key,
            "first_line": "1600 11th St",
            "type": "practice",
            "phone_number": "9407647000",
            "lat": 33.90559,
            "long": -98.47978,
            "address_sources": ["nppes"],
            "source_record_ids": ["npi:1588616783"],
        },
        {
            "address_key": address_key,
            "first_line": "1600 11th St",
            "type": "practice",
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": [
                "provider_directory_fhir:practitioner_role:pdfhir_source:role-1:location-1"
            ],
        },
    ]

    deduped = npi_module._dedupe_addresses_by_key(addresses)

    assert len(deduped) == 1
    merged_address = deduped[0]
    assert merged_address["address_site_key"] == address_key
    assert merged_address["phone_number"] == "9407647000"
    assert (merged_address["lat"], merged_address["long"]) == (33.90559, -98.47978)
    assert merged_address["address_sources"] == ["nppes", "provider_directory_fhir"]
    assert len(merged_address["source_record_ids"]) == 2
