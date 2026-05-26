# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from datetime import datetime
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "api" / "endpoint" / "codes.py"
MODULE_SPEC = spec_from_file_location("codes_endpoint_unit", MODULE_PATH)
codes_module = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(codes_module)

get_code = codes_module.get_code
get_related_codes = codes_module.get_related_codes
list_codes = codes_module.list_codes


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results=None):
        self._results = list(results or [])
        self.calls = []

    async def execute(self, *_args, **_kwargs):
        self.calls.append((_args, _kwargs))
        if self._results:
            return self._results.pop(0)
        return FakeResult([], 0)


def make_request(results, args=None):
    session = FakeSession(results)
    return types.SimpleNamespace(
        args=args or {},
        ctx=types.SimpleNamespace(sa_session=session),
    )


@pytest.mark.asyncio
async def test_list_codes_success():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[{"code_system": "HCPCS", "code": "99213", "display_name": "Office visit"}]),
        ],
        args={"code_system": "hcpcs", "limit": "10"},
    )
    response = await list_codes(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["code"] == "99213"


@pytest.mark.asyncio
async def test_list_codes_normalizes_code_system_alias():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[{"code_system": "POS", "code": "23", "display_name": "Emergency Room - Hospital"}]),
        ],
        args={"code_system": "place_of_service", "limit": "10"},
    )

    response = await list_codes(request)
    payload = json.loads(response.body)

    assert payload["query"]["code_system"] == "POS"
    assert payload["items"][0]["code_system"] == "POS"


@pytest.mark.asyncio
async def test_list_codes_searches_code_synonyms_without_changing_response_shape():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[{"code_system": "CDT", "code": "D0120", "display_name": "Periodic oral evaluation"}]),
        ],
        args={"q": "oral evaluation", "limit": "10"},
    )

    response = await list_codes(request)
    payload = json.loads(response.body)
    query_sql = "\n".join(str(call[0][0]) for call in request.ctx.sa_session.calls)

    assert payload["items"][0]["code_system"] == "CDT"
    assert "code_synonym" in query_sql
    assert "synonym" not in payload["items"][0]


@pytest.mark.asyncio
async def test_list_codes_serializes_datetime_fields():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "code_system": "CPT",
                        "code": "99213",
                        "display_name": "Office visit",
                        "updated_at": datetime(2026, 2, 15, 23, 52, 40, 779030),
                    }
                ]
            ),
        ],
        args={"code_system": "cpt", "limit": "10"},
    )
    response = await list_codes(request)
    payload = json.loads(response.body)
    assert payload["items"][0]["updated_at"] == "2026-02-15T23:52:40.779030"


@pytest.mark.asyncio
async def test_get_code_success():
    request = make_request(
        [FakeResult(rows=[{"code_system": "HCPCS", "code": "99213", "display_name": "Office visit"}])]
    )
    response = await get_code(request, "hcpcs", "99213")
    payload = json.loads(response.body)
    assert payload["code_system"] == "HCPCS"
    assert payload["code"] == "99213"


@pytest.mark.asyncio
async def test_get_code_canonicalizes_revenue_code():
    request = make_request(
        [FakeResult(rows=[{"code_system": "RC", "code": "0450", "display_name": "Emergency Room"}])]
    )

    response = await get_code(request, "revenue_code", "450")
    payload = json.loads(response.body)

    assert payload["code_system"] == "RC"
    assert payload["code"] == "0450"


@pytest.mark.asyncio
async def test_get_code_canonicalizes_place_of_service_alias():
    request = make_request(
        [FakeResult(rows=[{"code_system": "POS", "code": "23", "display_name": "Emergency Room - Hospital"}])]
    )

    response = await get_code(request, "place_of_service", "23")
    payload = json.loads(response.body)

    assert payload["code_system"] == "POS"
    assert payload["code"] == "23"


@pytest.mark.asyncio
async def test_get_related_codes_success():
    request = make_request(
        [
            FakeResult(rows=[{"from_system": "CPT", "from_code": "99213", "to_system": "HCPCS", "to_code": "99213"}]),
            FakeResult(rows=[]),
        ]
    )
    response = await get_related_codes(request, "cpt", "99213")
    payload = json.loads(response.body)
    assert payload["input_code"]["code_system"] == "CPT"
    assert len(payload["related"]["forward"]) == 1
