# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from types import SimpleNamespace
import uuid

import pytest
from sanic import Blueprint, Sanic

from api import hospital_price_serving as serving
from api.endpoint import hospital_prices as endpoint
from api.hospital_price_serving_sql import CODE_SELECTOR_SQL
from api.hospital_price_serving_sql import FACT_BLOCK_SQL
from api.hospital_price_serving_sql import PAYER_SELECTOR_SQL
from api.hospital_price_serving_sql import SERVICE_BLOCK_SQL
from api.hospital_price_serving_sql import VERSION_SQL
from support.hospital_price_native_validation import (
    HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
)


VERSION_ID = "a" * 64
LEGACY_PAYER_PARENT_SHA256 = hashlib.sha256(
    b"payer\0" + len(b"Payer").to_bytes(8, "little") + b"Payer"
).digest()


class _Result:
    def __init__(self, rows=()):
        self.rows = tuple(rows)

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Begin:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


def _version(version_id=VERSION_ID, **overrides):
    return {
        "version_id": version_id,
        "parser_contract_sha256": HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        "source_format": "csv-tall",
        "template_version": "3.0.0",
        "version_service_count": 3,
        "version_charge_count": 3,
        "version_fact_count": 3,
        "current_service_count": 3,
        "current_charge_count": 3,
        "current_fact_count": 3,
        "format_version": 2,
        "service_count": 3,
        "charge_count": 3,
        "fact_count": 3,
        **overrides,
    }


def _service(service_ordinal, charge_key):
    return {
        "service_ordinal": service_ordinal,
        "description": f"Synthetic service {service_ordinal}",
        "drug_unit": None,
        "drug_type": None,
        "codes": [{"code_type": "CPT", "code": "12345"}],
        "charges": [
            {
                "charge_key": charge_key,
                "charge_ordinal": charge_key + 10,
                "setting": "outpatient",
                "billing_class": "facility",
                "modifier_codes": [],
                "gross_charge": "100.00",
                "discounted_cash": "80.00",
                "minimum": "50.00",
                "maximum": "120.00",
                "additional_generic_notes": None,
                "first_fact_ordinal": charge_key,
                "fact_count": 1,
            }
        ],
    }


def _fact(charge_key, payer="Payer", plan="Plan"):
    return {
        "charge_key": charge_key,
        "payer_name": payer,
        "plan_name": plan,
        "negotiated_dollar": f"{70 + charge_key}.00",
        "negotiated_percentage": None,
        "negotiated_algorithm": None,
        "methodology": "fee schedule",
        "median_amount": None,
        "percentile_10": None,
        "percentile_90": None,
        "allowed_count": None,
        "additional_payer_notes": None,
        "comparison_amount": f"{70 + charge_key}.00",
    }


class _Native:
    def __init__(self):
        self.service_rows = [_service(index, index) for index in range(3)]
        self.fact_rows = [_fact(0), _fact(1, "Other", "Other"), _fact(2)]
        self.payer_refs = [0, 2]

    @staticmethod
    def hospital_price_selector_sha256(_kind, _first, _second):
        return b"s" * 32

    def hospital_price_decode_selector_page(
        self, payload, kind, _first, _second, ranges, max_refs
    ):
        refs = [0, 1, 2] if kind == "code" else self.payer_refs
        assert payload == (b"code" if kind == "code" else b"payer")
        selected_refs = [
            reference for reference in refs
            if any(start <= reference < end for start, end in ranges)
        ]
        return {
            "page_index": 0,
            "page_count": 1,
            "row_count": 1,
            "page_ref_count": len(refs),
            "found": True,
            "ref_count": len(refs),
            "first_ref": refs[0],
            "refs": selected_refs[:max_refs],
            "truncated": len(selected_refs) > max_refs,
        }

    def hospital_price_decode_service_block(self, payload):
        assert payload == b"service"
        return deepcopy(self.service_rows)

    def hospital_price_decode_fact_block(self, payload):
        assert payload == b"fact"
        return deepcopy(self.fact_rows)


@pytest.mark.asyncio
async def test_v2_selector_page_allows_absent_key_inside_digest_range(monkeypatch):
    async def absent_key(*_args):
        return {
            "page_index": 0,
            "page_count": 1,
            "row_count": 2,
            "page_ref_count": 2,
            "found": False,
            "ref_count": 0,
            "first_ref": None,
            "refs": [],
            "truncated": False,
        }

    monkeypatch.setattr(serving, "_native_call", absent_key)
    selector_record_by_field = {
        "format_version": 2,
        "logical_first": 0,
        "logical_count": 2,
        "page_index": 0,
        "page_count": 1,
        "secondary_first": 0,
        "secondary_count": 2,
        "key_sha256": b"a" * 32,
        "parent_sha256": b"z" * 32,
        "payload": b"selector",
    }

    assert await serving._selector_refs(
        (selector_record_by_field,), "code", "CPT", "missing", [(0, 2)], 2,
        b"m" * 32,
    ) == ([], False, [0], 1)


class _Session:
    def __init__(self, native=None, format_version=2):
        self.native = native or _Native()
        self.version = _version(
            format_version=format_version,
            parser_contract_sha256=(
                HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256
                if format_version == 1
                else HOSPITAL_MRF_PARSER_CONTRACT_SHA256
            ),
        )
        self.statements = []

    def begin(self):
        return _Begin()

    async def execute(self, statement, params=None):
        return self._statement_result(statement, params)

    def _statement_result(self, statement, params=None):
        self.statements.append((statement, params or {}))
        if statement is VERSION_SQL:
            return _Result([self.version])
        if statement is CODE_SELECTOR_SQL:
            return _Result(
                [{
                    "block_ordinal": 0, "logical_first": 0,
                    "logical_count": 1,
                    "secondary_first": 0, "secondary_count": 3,
                    "page_index": 0, "page_count": 1, "payload": b"code",
                    "key_sha256": b"s" * 32,
                    "parent_sha256": (
                        None if self.version["format_version"] == 1 else b"s" * 32
                    ),
                    "format_version": self.version["format_version"],
                }]
            )
        if statement is SERVICE_BLOCK_SQL:
            return _Result(
                [{
                    "block_ordinal": 0, "logical_first": 0, "logical_count": 3,
                    "secondary_first": 0, "secondary_count": 3,
                    "payload": b"service",
                }]
            )
        if statement is PAYER_SELECTOR_SQL:
            return _Result(
                [{
                    "block_ordinal": 2, "logical_first": 1,
                    "logical_count": 1,
                    "secondary_first": self.native.payer_refs[0],
                    "secondary_count": len(self.native.payer_refs),
                    "page_index": 0, "page_count": 1, "payload": b"payer",
                    "key_sha256": b"s" * 32,
                    "parent_sha256": (
                        LEGACY_PAYER_PARENT_SHA256
                        if self.version["format_version"] == 1
                        else b"s" * 32
                    ),
                    "format_version": self.version["format_version"],
                    "range_indexes": [0], "key_page_count": 1,
                }]
            )
        if statement is FACT_BLOCK_SQL:
            return _Result(
                [{
                    "block_ordinal": 1, "logical_first": 0,
                    "logical_count": 3, "payload": b"fact",
                }]
            )
        return _Result()


def _query(**overrides):
    values_by_field = {
        "hospital_id": "hospital-000001",
        "code_type": "CPT",
        "code": "12345",
        "payer_name": "Payer",
        "plan_name": "Plan",
        "limit": "2",
    }
    values_by_field.update(overrides)
    return serving.validate_hospital_price_query(**values_by_field)


@pytest.mark.asyncio
async def test_populated_payer_page_is_charge_bounded_and_version_bound(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    page = await serving.read_hospital_price_page(session, _query())

    assert page["version"] == {
        "version_id": VERSION_ID,
        "source_format": "csv-tall",
        "schema_version": "3.0.0",
    }
    assert page["pagination"]["unit"] == "charges"
    assert page["pagination"]["scanned"] == 2
    assert page["pagination"]["next_cursor"]
    assert page["query"]["negotiated_prices_requested"] is True
    assert [item["charge"]["charge_ordinal"] for item in page["items"]] == [10]
    assert page["items"][0]["negotiated_prices"][0]["payer_name"] == "Payer"
    assert "charge_key" not in page["items"][0]["negotiated_prices"][0]

    next_page = await serving.read_hospital_price_page(
        session,
        _query(cursor=page["pagination"]["next_cursor"]),
    )
    assert next_page["pagination"]["scanned"] == 1
    assert next_page["pagination"]["next_cursor"] is None
    assert [item["charge"]["charge_ordinal"] for item in next_page["items"]] == [12]


@pytest.mark.asyncio
async def test_v1_selectors_preserve_legacy_layout_and_pagination(monkeypatch):
    session = _Session(format_version=1)
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    page = await serving.read_hospital_price_page(session, _query())
    next_page = await serving.read_hospital_price_page(
        session,
        _query(cursor=page["pagination"]["next_cursor"]),
    )

    assert page["pagination"]["scanned"] == 2
    assert page["pagination"]["next_cursor"]
    assert [item["charge"]["charge_ordinal"] for item in page["items"]] == [10]
    assert next_page["pagination"] == {
        "unit": "charges", "limit": 2, "scanned": 1, "next_cursor": None,
    }
    assert [item["charge"]["charge_ordinal"] for item in next_page["items"]] == [12]
    assert sum(statement is CODE_SELECTOR_SQL for statement, _ in session.statements) == 2
    assert sum(statement is PAYER_SELECTOR_SQL for statement, _ in session.statements) == 2


@pytest.mark.asyncio
async def test_unfiltered_page_reads_no_fact_blocks(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    page = await serving.read_hospital_price_page(
        session, _query(payer_name=None, plan_name=None)
    )

    assert len(page["items"]) == 2
    assert page["query"]["negotiated_prices_requested"] is False
    assert all(item["negotiated_prices"] == [] for item in page["items"])
    assert all(statement is not FACT_BLOCK_SQL for statement, _params in session.statements)


@pytest.mark.asyncio
async def test_missing_or_corrupt_native_data_fails_closed(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", None)
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        await serving.read_hospital_price_page(session, _query())

    monkeypatch.setattr(serving, "_NATIVE", session.native)
    session.native.service_rows = [_service(0, 0)]
    with pytest.raises(
        serving.HospitalPriceServingUnavailableError,
        match="service block",
    ):
        await serving.read_hospital_price_page(
            session, _query(payer_name=None, plan_name=None)
        )


@pytest.mark.asyncio
async def test_version_contract_and_cursor_generation_fail_closed(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)
    page = await serving.read_hospital_price_page(session, _query())

    session.version = _version("b" * 64)
    with pytest.raises(serving.HospitalPriceCursorStaleError):
        await serving.read_hospital_price_page(
            session, _query(cursor=page["pagination"]["next_cursor"])
        )

    session.version = _version(parser_contract_sha256="c" * 64)
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        await serving.read_hospital_price_page(session, _query())

    session.version = _version(version_id="invalid")
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        await serving.read_hospital_price_page(session, _query())

    assert serving._validated_version((
        _version(
            format_version=1,
            parser_contract_sha256=HOSPITAL_MRF_LEGACY_PARSER_CONTRACT_SHA256,
        ),
    ))["format_version"] == 1
    assert serving._validated_version((
        _version(
            format_version=2,
            parser_contract_sha256=HOSPITAL_MRF_PACKED_V2_PARSER_CONTRACT_SHA256,
        ),
    ))["format_version"] == 2
    assert serving._validated_version((
        _version(template_version="2.0.0"),
    ))["template_version"] == "2.0.0"
    for template_version in ("", " 2.0.0"):
        with pytest.raises(serving.HospitalPriceServingUnavailableError):
            serving._validated_version((_version(template_version=template_version),))


@pytest.mark.asyncio
async def test_matching_fact_fanout_is_bounded_before_fact_blocks(monkeypatch):
    session = _Session()
    session.native.service_rows[0]["charges"][0]["fact_count"] = 10_002
    session.native.payer_refs = list(range(10_002))
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    with pytest.raises(
        serving.HospitalPriceServingUnavailableError,
        match="fanout",
    ):
        await serving.read_hospital_price_page(session, _query(limit="1"))
    assert all(statement is not FACT_BLOCK_SQL for statement, _params in session.statements)


@pytest.mark.parametrize(
    "overrides",
    [
        {"hospital_id": "unknown"},
        {"code_type": None},
        {"code_type": "cpt"},
        {"code": " 12345"},
        {"payer_name": "Payer", "plan_name": None},
        {"limit": "101"},
        {"version_id": "bad"},
        {"cursor": "bad"},
    ],
)
def test_query_validation_is_exact_and_bounded(overrides):
    error = (
        serving.HospitalPriceNotFoundError
        if "hospital_id" in overrides else serving.HospitalPriceInvalidRequestError
    )
    with pytest.raises(error):
        _query(**overrides)


@pytest.mark.asyncio
async def test_public_route_shape_and_errors(monkeypatch):
    app = Sanic(f"hospital-price-serving-{uuid.uuid4().hex}")
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))

    @app.middleware("request")
    async def install_session(request):
        request.ctx.sa_session = object()

    route_payload_by_field = {
        "hospital_id": "hospital-000001",
        "version": {"version_id": VERSION_ID},
        "query": {"code_type": "CPT", "code": "12345"},
        "pagination": {"unit": "charges", "limit": 25, "scanned": 0, "next_cursor": None},
        "items": [],
    }

    async def read_page(_session, _query_value):
        return route_payload_by_field

    monkeypatch.setattr(endpoint, "read_hospital_price_page", read_page)
    _request, ok = await app.asgi_client.get(
        "/api/v1/hospital-prices/facilities/hospital-000001/prices"
        "?code_type=CPT&code=12345"
    )
    assert ok.status == 200
    assert json.loads(ok.body) == route_payload_by_field
    assert ok.headers["cache-control"] == "private, no-store"

    _request, bad = await app.asgi_client.get(
        "/api/v1/hospital-prices/facilities/hospital-000001/prices"
        "?code_type=CPT&code=12345&unknown=x"
    )
    assert bad.status == 400
    assert json.loads(bad.body)["error"]["code"] == "hospital_price_invalid_request"

    async def oversized_page(_session, _query_value):
        return {"items": [{"notes": "x" * (2 << 20)}]}

    monkeypatch.setattr(endpoint, "read_hospital_price_page", oversized_page)
    _request, unavailable = await app.asgi_client.get(
        "/api/v1/hospital-prices/facilities/hospital-000001/prices"
        "?code_type=CPT&code=12345"
    )
    assert unavailable.status == 503
    assert json.loads(unavailable.body)["error"]["code"] == (
        "hospital_price_serving_unavailable"
    )
