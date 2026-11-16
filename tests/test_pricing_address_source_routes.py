# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public pricing routes keep count, page and hydrated offices under one threshold."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from tests import test_pricing_api as support
from tests.test_ptg2_serving import FakePagination, _strict_v3_tables

pricing = support.pricing_module


@pytest.mark.asyncio
@pytest.mark.parametrize("unified", [False, True])
async def test_group_plan_count_and_hydration_use_the_same_qualifying_offices(monkeypatch, unified):
    support._patch_group_plan_snapshot_tables(monkeypatch)
    monkeypatch.setattr(
        pricing,
        "_group_plan_provider_address_source",
        AsyncMock(
            return_value=("mrf.entity_address_unified" if unified else "mrf.npi_address", unified, unified, unified)
        ),
    )
    provider_rows = [SimpleNamespace(npi=1234567890)] if unified else []
    office_by_field = {
        "npi": 1234567890,
        "type": "practice",
        "first_line": "100 Example Road",
        "city_name": "Example City",
        "state_name": "IL",
        "postal_code": "60001",
        "zip5": "60001",
        "source_count": 3,
    }
    request = support.make_request(
        [support.FakeResult(rows=provider_rows), support.FakeResult(rows=[office_by_field])],
        args={"plan_id": "SYNTHETIC-PLAN", "min_address_sources": "3", "enrich": "0", "limit": "10", "count": "true"},
    )
    document_by_field = orjson.loads((await pricing.group_plan_providers(request)).body)
    executions = request.ctx.sa_session.executions
    if unified:
        assert document_by_field["providers"]["items"][0]["address"]["first_line"] == "100 Example Road"
        assert len(executions) == 2
        for arguments, _ in executions:
            assert "addr.source_count >= 3" in str(arguments[0])
            assert arguments[1]["min_address_sources"] == 3
    else:
        assert document_by_field["providers"]["items"] == []
        assert len(executions) == 2
        assert all("FALSE" in str(arguments[0]) for arguments, _ in executions)


@pytest.mark.asyncio
async def test_exact_claims_provider_keeps_utilization_without_unknown_postal_fields():
    request = support.make_request(
        [
            support.FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "total_services": 7,
                        "city": "Example City",
                        "state": "IL",
                        "zip5": "60001",
                        "country": "US",
                    }
                ]
            ),
            support.FakeResult(scalar=2),
            support.FakeResult(scalar=0),
        ],
        args={"year": "2023", "min_address_sources": "3"},
    )
    document_by_field = orjson.loads((await pricing.get_pricing_provider(request, "1234567890")).body)
    assert document_by_field["npi"] == 1234567890 and document_by_field["total_services"] == 7
    assert document_by_field["locations"] == []
    assert document_by_field["summary"] == {"service_count": 2, "location_count": 0}
    assert not {"city", "state", "zip5", "country"}.intersection(document_by_field)
    location_statement = request.ctx.sa_session.executions[-1][0][0]
    assert "false" in str(location_statement.compile(compile_kwargs={"literal_binds": True})).lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["providers", "procedure", "procedure_search", "prescription", "prescription_search"])
async def test_unknown_claims_offices_are_excluded_from_both_count_and_page(monkeypatch, route):
    monkeypatch.setattr(pricing, "_is_table_available", AsyncMock(return_value=True))
    args_by_name = {"year": "2023", "min_address_sources": "3", "limit": "10"}
    if route == "prescription_search":
        args_by_name.update(code="DEMO", rx_code_system="HP_RX_CODE")
    elif route == "procedure_search":
        args_by_name.update(code="123", code_system="HP_PROCEDURE_CODE")
    request = support.make_request([support.FakeResult(scalar=0), support.FakeResult(rows=[])], args=args_by_name)
    if route == "providers":
        reply = await pricing.list_pricing_providers(request)
    elif route == "procedure":
        reply = await pricing.list_procedure_providers(request, "HP_PROCEDURE_CODE", "123")
    elif route == "procedure_search":
        reply = await pricing.list_providers_by_procedure(request)
    elif route == "prescription":
        reply = await pricing.list_prescription_providers(request, "HP_RX_CODE", "DEMO")
    else:
        reply = await pricing.list_providers_by_prescription(request)
    document_by_field = orjson.loads(reply.body)
    assert document_by_field["items"] == [] and document_by_field["pagination"]["total"] == 0
    statements = [
        str(arguments[0].compile(compile_kwargs={"literal_binds": True})).lower()
        for arguments, _ in request.ctx.sa_session.executions
    ]
    assert sum("false" in statement and "where" in statement for statement in statements) >= 2, (
        document_by_field,
        statements,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_count_only_search_preserves_cost_order_without_a_geographic_locator(monkeypatch, minimum):
    loader = AsyncMock(return_value=[])
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", loader)
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(return_value=({"01" * 16}, {"01" * 16: [{"npi": 101}]})),
    )
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={"01" * 16: 1}))
    result = await serving._search_manifest_serving_table(
        object(),
        "snapshot",
        {"plan_id": "SYNTHETIC-PLAN", "code_system": "CPT", "code": "99213", "min_address_sources": str(minimum)},
        FakePagination(),
        _strict_v3_tables(),
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )
    assert result is None
    actual_arguments = loader.await_args.args[2]
    assert actual_arguments.get("order_by") == ("total_allowed_amount" if minimum else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["service_locations", "prescription"])
async def test_physician_aliases_preserve_the_threshold_when_delegating(monkeypatch, route):
    delegate = AsyncMock(return_value=object())
    request = support.make_request([], args={"min_address_sources": "3"})
    if route == "service_locations":
        monkeypatch.setattr(pricing, "_provider_procedure_locations", delegate)
        await pricing.list_physician_service_locations(request, "1234567890", "CPT", "99213")
    else:
        monkeypatch.setattr(pricing, "_provider_prescription_detail", delegate)
        await pricing.get_physician_prescription(request, "1234567890", "HP_RX_CODE", "DEMO")
    assert delegate.await_args.args[0] is request
    assert request.args["min_address_sources"] == "3"


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0"])
async def test_default_claims_search_keeps_the_precomputed_count_shortcut(monkeypatch, minimum):
    precomputed = AsyncMock(return_value=7)
    monkeypatch.setattr(pricing, "_precomputed_procedure_provider_count", precomputed)
    args_by_name = {"year": "2023", "code": "123", "code_system": "HP_PROCEDURE_CODE", "limit": "10"}
    if minimum is not None:
        args_by_name["min_address_sources"] = minimum
    request = support.make_request([support.FakeResult(rows=[])], args=args_by_name)
    document_by_field = orjson.loads((await pricing.list_providers_by_procedure(request)).body)
    assert document_by_field["pagination"]["total"] == 7
    precomputed.assert_awaited_once()
    assert len(request.ctx.sa_session.executions) == 1
