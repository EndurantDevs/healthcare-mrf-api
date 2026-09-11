# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic coverage for source-bound hospital discovery."""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import replace

import pytest
from sanic import Blueprint, Sanic

from api import hospital_price_payer_plans as discovery
from api import hospital_price_request as requests
from api.endpoint import hospital_prices as endpoint
from api.hospital_price_serving_sql import PAYER_PLAN_CATALOG_SQL, PAYER_PLAN_LEGACY_CATALOG_SQL, VERSION_SQL
from support.hospital_price_native_validation import HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256
from tests.test_hospital_price_serving import VERSION_ID, _Begin, _Result, _version


def _dictionary():
    return {
        "page_index": 0,
        "page_count": 1,
        "items": [
            {"payer_name": "Synthetic payer", "plan_name": None, "key_sha256": b"a" * 32},
            {"payer_name": "Synthetic payer", "plan_name": "Synthetic plan", "key_sha256": b"b" * 32},
        ],
    }


def _record():
    return {
        "logical_first": 3,
        "logical_count": 2,
        "page_index": 0,
        "page_count": 1,
        "key_sha256": b"a" * 32,
        "parent_sha256": b"b" * 32,
        "payload": b"dictionary",
    }


class _Session:
    def __init__(self):
        self.version = _version(code_selector_key_count=3, payer_plan_selector_key_count=2)
        self.records = [_record()]
        self.calls = []

    def begin(self):
        return _Begin()

    async def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))
        if statement is VERSION_SQL:
            return _Result([self.version])
        if statement is PAYER_PLAN_CATALOG_SQL or statement is PAYER_PLAN_LEGACY_CATALOG_SQL:
            return _Result(self.records)
        assert statement is discovery._READ_TRANSACTION_SQL
        return _Result()


@pytest.fixture
def native_reader(monkeypatch):
    async def decode(name, payload):
        assert name == "hospital_price_decode_payer_plan_keys"
        assert payload == b"dictionary"
        return _dictionary()

    monkeypatch.setattr(discovery, "_native_call", decode)
    monkeypatch.setattr(discovery, "hospital_hpt_group_ids", lambda identity: (identity,))


@pytest.mark.asyncio
async def test_payer_plan_pages_keep_exact_pairs_and_version(native_reader):
    session = _Session()
    query = requests.validate_hospital_payer_plan_query("hospital-000001", limit="1")
    first = await discovery.read_hospital_payer_plan_page(session, query)
    assert first["items"] == [{"payer_name": "Synthetic payer", "plan_name": None, "plan_missing": True}]
    assert first["pagination"]["scanned"] == 1
    cursor = first["pagination"]["next_cursor"]
    assert len(cursor) == 96
    second = await discovery.read_hospital_payer_plan_page(session, replace(query, cursor=cursor))
    assert second["items"] == [{"payer_name": "Synthetic payer", "plan_name": "Synthetic plan", "plan_missing": False}]
    assert second["pagination"]["next_cursor"] is None
    assert len([statement for statement, _params in session.calls if statement is PAYER_PLAN_CATALOG_SQL]) == 2
    assert first["version"]["version_id"] == VERSION_ID


@pytest.mark.asyncio
async def test_payer_plan_empty_dictionary_needs_no_native_block(native_reader):
    session = _Session()
    session.version.update(payer_plan_selector_key_count=0, fact_count=0, version_fact_count=0, current_fact_count=0)
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    page = await discovery.read_hospital_payer_plan_page(session, query)
    assert page["items"] == []
    assert page["pagination"]["next_cursor"] is None
    assert len(session.calls) == 2


def test_discovery_cursor_cannot_cross_resource_or_version():
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    cursor = requests.encode_hospital_price_cursor(query, VERSION_ID, 3)
    assert requests.decode_hospital_price_cursor(replace(query, cursor=cursor), VERSION_ID) == 3
    with pytest.raises(requests.HospitalPriceCursorStaleError):
        requests.decode_hospital_price_cursor(replace(query, cursor=cursor), "b" * 64)
    with pytest.raises(requests.HospitalPriceInvalidRequestError):
        requests.decode_hospital_price_cursor(replace(query, cursor=cursor, version_id="b" * 64), "b" * 64)
    with pytest.raises(requests.HospitalPriceInvalidRequestError):
        requests.decode_hospital_price_cursor(replace(query, cursor=cursor, hospital_id="hospital-000002"), VERSION_ID)
    price_query = requests.validate_hospital_price_query(
        "hospital-000001", code_type="LOCAL", code="payer-plans", cursor=cursor
    )
    with pytest.raises(requests.HospitalPriceInvalidRequestError):
        requests.decode_hospital_price_cursor(price_query, VERSION_ID)


@pytest.mark.parametrize(
    "updates",
    [
        {"code_selector_key_count": None},
        {"code_selector_key_count": 0},
        {"payer_plan_selector_key_count": -1},
        {"payer_plan_selector_key_count": 0},
        {"payer_plan_selector_key_count": 1 << 32},
    ],
)
def test_dictionary_root_fails_closed(updates):
    version = _version(code_selector_key_count=3, payer_plan_selector_key_count=2) | updates
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        discovery._selector_key_bounds(version)


@pytest.mark.parametrize(
    "updates",
    [
        {"logical_count": 1},
        {"page_index": 1},
        {"page_count": 0},
        {"key_sha256": b"c" * 32},
        {"parent_sha256": b"c" * 32},
    ],
)
def test_dictionary_metadata_fails_closed(updates):
    record = _record() | updates
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        discovery._validated_dictionary_keys(record, _dictionary(), 2)


@pytest.mark.parametrize("decoded", [None, {}, {"items": []}, {"items": [None]}])
def test_dictionary_decoder_shape_fails_closed(decoded):
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        discovery._validated_dictionary_keys(_record(), decoded, 2)


@pytest.mark.parametrize("field", ["payer_name", "plan_name"])
def test_dictionary_missing_field_fails_closed(field):
    decoded = _dictionary()
    del decoded["items"][0][field]
    with pytest.raises(discovery.HospitalPriceServingUnavailableError, match="hospital payer-plan key is invalid"):
        discovery._validated_dictionary_keys(_record(), decoded, 2)


@pytest.mark.parametrize(
    "field,value",
    [
        ("payer_name", ""),
        ("plan_name", " bad"),
        ("key_sha256", "invalid"),
        ("payer_name", "x" * 4097),
        ("plan_name", "é" * 2049),
    ],
)
def test_dictionary_invalid_key_fails_closed(field, value):
    decoded = deepcopy(_dictionary())
    decoded["items"][0][field] = value
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        discovery._validated_dictionary_keys(_record(), decoded, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "records", [[], [_record(), _record()], [_record() | {"logical_first": 4}], [_record() | {"logical_count": 3}]]
)
async def test_dictionary_gaps_fail_closed(native_reader, records):
    session = _Session()
    session.records = records
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        await discovery.read_hospital_payer_plan_page(session, query)


@pytest.mark.asyncio
async def test_dictionary_cursor_outside_bounds_is_invalid(native_reader):
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    for after_key in (0, 4, 100):
        cursor = requests.encode_hospital_price_cursor(query, VERSION_ID, after_key)
        with pytest.raises(requests.HospitalPriceInvalidRequestError):
            await discovery.read_hospital_payer_plan_page(_Session(), replace(query, cursor=cursor))


@pytest.mark.asyncio
async def test_dictionary_byte_budget_is_enforced(native_reader, monkeypatch):
    monkeypatch.setattr(discovery, "HOSPITAL_PRICE_PUBLIC_DATA_BYTES", 1)
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        await discovery.read_hospital_payer_plan_page(_Session(), query)


def test_dictionary_sql_uses_one_indexed_first_page():
    sql = str(PAYER_PLAN_CATALOG_SQL)
    assert "block_kind=4 AND page_index=0" in sql
    assert "logical_first <= :after_key + 1" in sql
    assert "ORDER BY logical_first DESC LIMIT 1" in sql


@pytest.mark.asyncio
async def test_payer_plan_pages_span_blocks_without_duplicate_continuation_keys(monkeypatch):
    session = _Session()
    monkeypatch.setattr(discovery, "hospital_hpt_group_ids", lambda identity: (identity,))

    async def decode(_name, payload):
        selected_key = 0 if payload == b"first" else 1
        return {
            "page_index": 0,
            "page_count": 7 if selected_key == 0 else 1,
            "items": [_dictionary()["items"][selected_key]],
        }

    monkeypatch.setattr(discovery, "_native_call", decode)
    session.records = [
        _record() | {"logical_count": 1, "page_count": 7, "parent_sha256": b"a" * 32, "payload": b"first"}
    ]
    query = requests.validate_hospital_payer_plan_query("hospital-000001", limit="100")
    first = await discovery.read_hospital_payer_plan_page(session, query)
    assert len(first["items"]) == 1
    assert first["pagination"]["next_cursor"]
    session.records = [
        _record() | {"logical_first": 4, "logical_count": 1, "key_sha256": b"b" * 32, "payload": b"second"}
    ]
    second = await discovery.read_hospital_payer_plan_page(
        session, replace(query, cursor=first["pagination"]["next_cursor"])
    )
    assert len(second["items"]) == 1
    assert second["pagination"]["next_cursor"] is None
    assert first["items"] != second["items"]


@pytest.mark.asyncio
async def test_legacy_interleaved_selector_ordinals_are_complete(monkeypatch, native_reader):
    session = _Session()
    session.version.update(format_version=1, parser_contract_sha256=HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256)
    # Legacy keys can be code 0, payer 1, code 2, payer 3, code 4.
    session.records = [_record() | {"logical_first": 1, "logical_count": 1, "has_more": True, "payload": b"first"}]

    async def decode(_name, payload):
        return _dictionary() | {"items": [_dictionary()["items"][0 if payload == b"first" else 1]]}

    monkeypatch.setattr(discovery, "_native_call", decode)
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    first = await discovery.read_hospital_payer_plan_page(session, query)
    cursor = first["pagination"]["next_cursor"]
    assert requests.decode_hospital_price_cursor(replace(query, cursor=cursor), VERSION_ID) == 1
    assert len(first["items"]) == 1
    session.records = [
        _record()
        | {"logical_first": 3, "logical_count": 1, "has_more": False, "key_sha256": b"b" * 32, "payload": b"second"}
    ]
    second = await discovery.read_hospital_payer_plan_page(session, replace(query, cursor=cursor))
    assert len(second["items"]) == 1
    assert second["pagination"]["next_cursor"] is None
    assert first["items"] != second["items"]
    seeks = [
        parameters["after_key"] for statement, parameters in session.calls if statement is PAYER_PLAN_LEGACY_CATALOG_SQL
    ]
    assert seeks == [-1, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    [
        {"logical_first": -1},
        {"logical_first": 5},
        {"logical_count": 2},
        {"has_more": "true"},
        {"logical_first": 4, "has_more": True},
    ],
)
async def test_legacy_dictionary_metadata_fails_closed(updates, native_reader):
    session = _Session()
    session.version.update(format_version=1, parser_contract_sha256=HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256)
    session.records = [_record() | {"logical_first": 1, "logical_count": 1, "has_more": True} | updates]
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        await discovery.read_hospital_payer_plan_page(session, query)


@pytest.mark.asyncio
async def test_legacy_empty_dictionary_needs_no_block(native_reader):
    session = _Session()
    session.version.update(
        format_version=1,
        parser_contract_sha256=HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256,
        payer_plan_selector_key_count=0,
        fact_count=0,
        version_fact_count=0,
        current_fact_count=0,
    )
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    page = await discovery.read_hospital_payer_plan_page(session, query)
    assert page["items"] == []
    assert page["pagination"]["next_cursor"] is None
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_legacy_missing_dictionary_fails_closed(native_reader):
    session = _Session()
    session.version.update(format_version=1, parser_contract_sha256=HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256)
    session.records = []
    query = requests.validate_hospital_payer_plan_query("hospital-000001")
    with pytest.raises(discovery.HospitalPriceServingUnavailableError):
        await discovery.read_hospital_payer_plan_page(session, query)


def test_legacy_dictionary_sql_uses_bounded_successor_seeks():
    sql = str(PAYER_PLAN_LEGACY_CATALOG_SQL)
    assert "block_kind=4 AND page_index=0" in sql
    assert "logical_first > :after_key" in sql
    assert "ORDER BY logical_first LIMIT 1" in sql
    assert "SELECT selected.*, EXISTS" in sql
    assert "successor.logical_first > selected.logical_first" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix,status",
    [
        ("?limit=101", 400),
        ("?limit=1&limit=2", 400),
        ("?payer_name=Example", 400),
        ("?cursor=bad", 400),
        ("?version_id=bad", 400),
    ],
)
async def test_payer_plan_route_rejects_invalid_query(suffix, status):
    app = Sanic(f"hospital-payer-query-{uuid.uuid4().hex}")
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    _request, result = await app.asgi_client.get(
        f"/api/v1/hospital-prices/facilities/hospital-000001/payer-plans{suffix}"
    )
    assert result.status == status


@pytest.mark.asyncio
async def test_payer_plan_route_returns_source_hidden_page(monkeypatch):
    app = Sanic(f"hospital-payer-page-{uuid.uuid4().hex}")
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def install_session(request):
        request.ctx.sa_session = object()

    async def read_page(_session, query):
        assert query.limit == 25
        return {"items": [{"payer_name": "Synthetic payer", "plan_name": None, "plan_missing": True}]}

    monkeypatch.setattr(endpoint, "read_hospital_payer_plan_page", read_page)
    _request, result = await app.asgi_client.get("/api/v1/hospital-prices/facilities/hospital-000001/payer-plans")
    assert result.status == 200
    assert result.json["items"][0]["plan_missing"] is True
    assert result.headers["cache-control"] == "private, no-store"
