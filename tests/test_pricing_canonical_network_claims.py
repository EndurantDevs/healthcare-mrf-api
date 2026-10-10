# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical claims query custody; pure fixtures do not establish native authority."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, SanicException, ServiceUnavailable
from sqlalchemy.dialects import postgresql

from api import network_address_scope as scope_module
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_pricing_api import FakeResult, make_request, pricing_module
from tests.test_pricing_procedure_fast_path import _resolve_single_internal_code

MANIFEST = PinnedNetworkServingManifest(
    7,
    "00000000-0000-0000-0000-000000000001",
    "network_candidate_00000000000000000000000000000001",
    1,
    {},
    4,
    "c" * 64,
    81,
)


@pytest.fixture
def canonical_pin(monkeypatch):
    pin = AsyncMock(return_value=(MANIFEST, (42, 88), "a" * 64))
    monkeypatch.setattr(scope_module, "_pin_network_scope", pin)
    monkeypatch.setattr(
        pricing_module,
        "_resolve_internal_codes_for_request",
        _resolve_single_internal_code,
    )
    return pin


def request_for(**selectors):
    return make_request(
        [FakeResult(scalar=0), FakeResult(rows=[])],
        args={
            "code": "99213",
            "code_system": "CPT",
            "year": "2023",
            "limit": "2",
            "offset": "4",
            "network_ids": "42,88",
            "network_generation": "7",
            **selectors,
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("order_by", ["total_allowed_amount", "total_services"])
async def test_canonical_count_and_page_scope_precedes_limit(canonical_pin, monkeypatch, order_by):
    fast_count = AsyncMock(side_effect=AssertionError("global count must not be used"))
    monkeypatch.setattr(pricing_module, "_precomputed_procedure_provider_count", fast_count)
    request = request_for(order_by=order_by)
    reply = await pricing_module.list_providers_by_procedure(request)
    assert json.loads(reply.body)["pagination"] == {
        "total": 0,
        "limit": 2,
        "offset": 4,
        "page": 3,
    }
    assert reply.headers["X-Network-Generation"] == "7"
    assert reply.headers["Cache-Control"] == "private, no-store"
    canonical_pin.assert_awaited_once_with(request, request.args)
    fast_count.assert_not_awaited()
    assert len(request.ctx.sa_session.executions) == 2
    for (statement, *_arguments), _options in request.ctx.sa_session.executions:
        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        assert "EXISTS (SELECT" in sql and MANIFEST.schema_name in sql
        assert "canonical_network_ids &&" in sql
        assert compiled.params["_canonical_network_ids"] == [42, 88]
        assert "coalesce(entity_address_unified.npi, entity_address_unified.inferred_npi)" in sql
        assert sql.index("EXISTS (SELECT") < sql.index("GROUP BY")
        if "LIMIT" in sql:
            assert sql.index("GROUP BY") < sql.index("LIMIT")
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["source_key", "snapshot_id", "plan_external_id", "billing_entity_ref"])
async def test_unscoped_pricing_lanes_refuse_before_body_sql(canonical_pin, selector):
    request = request_for(**{selector: "unavailable"})
    with pytest.raises(InvalidUsage, match="claims lane only"):
        await pricing_module.list_providers_by_procedure(request)
    assert not request.ctx.sa_session.executions
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
async def test_billing_selector_presence_cannot_escape_claims_guard(canonical_pin):
    request = request_for(billing_entity_ref="")
    with pytest.raises(InvalidUsage, match="claims lane only"):
        await pricing_module.list_providers_by_procedure(request)
    assert not request.ctx.sa_session.executions


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["plan_id", "plan_release_id", "plan_network_checksum"])
async def test_existing_selector_namespaces_remain_disjoint(canonical_pin, selector):
    request = request_for(**{selector: "unavailable"})
    with pytest.raises(InvalidUsage, match="one explicit network selector namespace") as caught:
        await pricing_module.list_providers_by_procedure(request)
    assert caught.value.headers["Cache-Control"] == "private, no-store"
    assert "X-Network-Generation" not in caught.value.headers
    canonical_pin.assert_not_awaited()
    assert not request.ctx.sa_session.executions


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 403])
async def test_pre_pin_refusal_keeps_private_headers_without_generation(canonical_pin, status):
    failure = SanicException("Canonical selection unavailable.", status_code=status, headers={"X-Test": "retained"})
    canonical_pin.side_effect = failure
    request = request_for()
    with pytest.raises(SanicException) as caught:
        await pricing_module.list_providers_by_procedure(request)
    assert caught.value is failure
    assert caught.value.headers == {"X-Test": "retained", "Cache-Control": "private, no-store"}
    assert not request.ctx.sa_session.executions
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
async def test_failed_manifest_never_runs_unpinned_pricing(canonical_pin):
    from process.network_serving_read import NetworkServingReadUnavailable

    canonical_pin.side_effect = NetworkServingReadUnavailable("unavailable")
    request = request_for()
    with pytest.raises(ServiceUnavailable):
        await pricing_module.list_providers_by_procedure(request)
    assert not request.ctx.sa_session.executions
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
async def test_original_cancellation_restores_scope(canonical_pin):
    marker = asyncio.CancelledError()
    request = request_for()
    request.ctx.sa_session.execute = AsyncMock(side_effect=marker)
    with pytest.raises(asyncio.CancelledError) as caught:
        await pricing_module.list_providers_by_procedure(request)
    assert caught.value is marker
    assert scope_module.current_network_address_scope() is None


def test_custom_import_is_explicitly_unsupported_inside_canonical_scope():
    with (
        scope_module.network_address_read_scope(MANIFEST, (42,)),
        pytest.raises(InvalidUsage, match="claims lane only"),
    ):
        pricing_module._canonical_network_procedure_clause({}, object(), pricing_module.provider_procedure_table.c.npi)


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["source_key", "snapshot_id", "plan_external_id", "billing_entity_ref"])
@pytest.mark.parametrize("value", [None, "", "null"])
async def test_unsupported_selector_presence_never_enters_pricing(canonical_pin, selector, value):
    request = request_for(**{selector: value})
    with pytest.raises(InvalidUsage, match="claims lane only"):
        await pricing_module.list_providers_by_procedure(request)
    assert not request.ctx.sa_session.executions
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["source_key", "snapshot_id", "plan_external_id", "billing_entity_ref"])
async def test_blank_raw_selector_cannot_disappear_from_request_args(canonical_pin, selector):
    request = request_for()
    request.query_string = f"network_ids=42,88&network_generation=7&{selector}="
    with pytest.raises(InvalidUsage, match="claims lane only"):
        await pricing_module.list_providers_by_procedure(request)
    assert not request.ctx.sa_session.executions
    assert scope_module.current_network_address_scope() is None
