# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from datetime import datetime
from decimal import Decimal
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
import sanic.exceptions

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
async def test_get_code_hides_restricted_terminology_by_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES", raising=False)
    request = make_request([FakeResult(rows=[{"code_system": "SNOMEDCT_US", "code": "123"}])])

    with pytest.raises(sanic.exceptions.NotFound):
        await get_code(request, "snomed", "123")

    assert request.ctx.sa_session.calls == []


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


def test_code_helpers_reject_missing_context_and_serialize_scalar_edges(
    monkeypatch,
):
    with pytest.raises(RuntimeError, match="session not available"):
        codes_module._get_session(
            types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=None))
        )

    mapping_row = types.SimpleNamespace(_mapping={"amount": Decimal("1.25")})
    assert codes_module._row_to_dict(mapping_row) == {"amount": Decimal("1.25")}
    assert codes_module._json_safe_value(Decimal("1.25")) == 1.25
    with pytest.raises(sanic.exceptions.InvalidUsage, match="either 'asc' or 'desc'"):
        codes_module._normalize_order("sideways")

    monkeypatch.setenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES", "true")
    assert codes_module._restricted_public_filter(
        codes_module.code_catalog_table.c.code_system
    ) is None


@pytest.mark.asyncio
async def test_list_codes_supports_unfiltered_descending_and_source_scopes(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES", "true")
    unfiltered_request = make_request(
        [FakeResult(scalar=0), FakeResult(rows=[])],
        args={"order": "desc"},
    )

    unfiltered_response = await list_codes(unfiltered_request)

    assert json.loads(unfiltered_response.body)["query"] == {
        "code_system": None,
        "q": None,
        "source": None,
        "order_by": "code",
        "order": "desc",
    }
    unfiltered_sql = "\n".join(
        str(call_args[0][0])
        for call_args in unfiltered_request.ctx.sa_session.calls
    )
    assert " WHERE " not in unfiltered_sql

    source_request = make_request(
        [FakeResult(scalar=0), FakeResult(rows=[])],
        args={"source": "  CMS  "},
    )
    source_response = await list_codes(source_request)
    assert json.loads(source_response.body)["query"]["source"] == "cms"
    assert "lower(mrf.code_catalog.source)" in str(
        source_request.ctx.sa_session.calls[0][0][0]
    )


@pytest.mark.asyncio
async def test_list_codes_rejects_unknown_order_column():
    request = make_request([], args={"order_by": "unknown"})

    with pytest.raises(sanic.exceptions.InvalidUsage, match="Unsupported order_by"):
        await list_codes(request)


@pytest.mark.asyncio
async def test_code_detail_rejects_empty_identity_and_missing_record():
    with pytest.raises(sanic.exceptions.InvalidUsage, match="Path parameters"):
        await get_code(make_request([]), "", "")

    with pytest.raises(sanic.exceptions.NotFound, match="Code not found"):
        await get_code(make_request([FakeResult(rows=[])]), "cpt", "99213")


@pytest.mark.asyncio
async def test_related_codes_reject_invalid_or_restricted_identity(monkeypatch):
    with pytest.raises(sanic.exceptions.InvalidUsage, match="Path parameters"):
        await get_related_codes(make_request([]), "", "")

    monkeypatch.delenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES", raising=False)
    with pytest.raises(sanic.exceptions.NotFound, match="Code not found"):
        await get_related_codes(make_request([]), "snomed", "123")


@pytest.mark.asyncio
async def test_related_codes_allows_restricted_system_when_explicitly_enabled(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES", "true")
    request = make_request([FakeResult(rows=[]), FakeResult(rows=[])])

    related_response = await get_related_codes(request, "snomed", "123")

    related_payload = json.loads(related_response.body)
    assert related_payload["input_code"] == {
        "code_system": "SNOMEDCT_US",
        "code": "123",
    }
