# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
import types
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


@pytest.mark.asyncio
async def test_get_all_returns_rows(monkeypatch):
    connections = [
        FakeConnection([[(2,)]]),
        FakeConnection([[ _build_result_row(1234567890) ]]),
    ]

    class FakeDB:
        def acquire(self):
            return FakeAcquire(connections.pop(0))

    monkeypatch.setattr(npi_module, "db", FakeDB())
    request = types.SimpleNamespace(
        args={
            "limit": "1",
            "classification": "Test",
            "name_like": "Doc",
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
    responses = [
        [{"intptlat": "41.0", "intptlon": "-87.0"}],
        [_build_near_row(1112223334), _build_near_row(1112223334)],
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


@pytest.mark.asyncio
async def test_build_npi_details(monkeypatch):
    row_values = []
    for column in NPIData.__table__.columns:
        if column.key == "npi":
            row_values.append(1234567890)
        else:
            row_values.append(f"data_{column.key}")
    row_values.append([{"taxonomy": 1}])
    row_values.append([{"group": 2}])
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
    assert payload["taxonomy_group_list"]
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


@pytest.mark.asyncio
async def test_get_npi_geocode_mapbox(monkeypatch):
    async def fake_build(_npi):
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
    await tasks[0]
    assert hasattr(insert, "payload")


@pytest.mark.asyncio
async def test_get_npi_geocode_google(monkeypatch):
    async def fake_build(_npi):
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
    await tasks[0]
    assert hasattr(insert, "payload")


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
    assert payload["rows"] == 1234


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
    assert payload["rows"] == "Pharmacist"


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
            "name_like": "Clinic",
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
async def test_get_npi_uses_cached_address(monkeypatch):
    async def fake_build(_npi):
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
            "name_like": "doc",
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
    monkeypatch.setattr(npi_module, 'select', lambda *_args, **_kwargs: FakeQuery([]))

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
    async def fake_build(_npi):
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

    async def fake_build(_npi):
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

    async def fake_build(_npi):
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

    async def fake_build(_npi):
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
async def test_get_npi_address_list_clears_empty_entries(monkeypatch):
    async def fake_build(_npi):
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