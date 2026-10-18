# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Default visibility preserves public claims identities and reported geography."""

import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from api import network_address_scope as scope_module
from api.provider_service_scope import _canonical_network_procedure_clause
from tests.test_pricing_api import FakeResult, make_request, pricing_module
from tests.test_pricing_canonical_network_claims import MANIFEST, request_for
from tests.test_pricing_procedure_fast_path import _resolve_single_internal_code


def _remove_selected_network(request):
    request.args.pop("network_ids", None)
    request.args.pop("network_generation", None)
    return request


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded", [(), (42,)])
@pytest.mark.parametrize("order_by", ["total_allowed_amount", "total_services"])
async def test_default_claims_count_and_page_keep_public_native_query(monkeypatch, excluded, order_by):
    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", _resolve_single_internal_code)
    monkeypatch.setattr(pricing_module, "_precomputed_procedure_provider_count", AsyncMock(return_value=None))
    baseline = _remove_selected_network(request_for(order_by=order_by))
    expected = await pricing_module.list_providers_by_procedure(baseline)
    request = _remove_selected_network(request_for(order_by=order_by))
    with scope_module.network_address_read_scope(
        MANIFEST, None, access_scope_sha256="b" * 64, excluded_network_ids=excluded
    ):
        reply = await pricing_module.list_providers_by_procedure(request)
        assert _canonical_network_procedure_clause({}, None, pricing_module.provider_procedure_table.c.npi) is None
    assert json.loads(reply.body) == json.loads(expected.body)
    assert len(request.ctx.sa_session.executions) == len(baseline.ctx.sa_session.executions) == 2
    for observed, original in zip(request.ctx.sa_session.executions, baseline.ctx.sa_session.executions, strict=True):
        statement = observed[0][0].compile(dialect=postgresql.dialect())
        expected_statement = original[0][0].compile(dialect=postgresql.dialect())
        assert str(statement) == str(expected_statement) and statement.params == expected_statement.params
        assert "entity_address_unified" not in str(statement)
    assert scope_module.current_network_address_scope() is None


def _public_claim_request():
    return make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1000000012,
                        "city": "Example City",
                        "state": "NJ",
                        "zip5": "07001",
                        "total_services": 10,
                        "total_submitted_charges": 1200,
                        "total_allowed_amount": 900,
                    }
                ]
            ),
        ],
        args={"code": "99213", "code_system": "CPT", "year": "2023", "include_evidence": "true"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("excluded", [(), (42,)])
async def test_default_claims_preserve_addressless_public_provider_and_claims_geography(monkeypatch, excluded):
    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", _resolve_single_internal_code)
    monkeypatch.setattr(pricing_module, "_precomputed_procedure_provider_count", AsyncMock(return_value=None))
    monkeypatch.setattr(pricing_module, "_enrich_provider_service_cost_indices", AsyncMock())
    office_read = AsyncMock(side_effect=AssertionError("default claims must not require directory offices"))
    monkeypatch.setattr(pricing_module, "_canonical_procedure_locations", office_read)
    baseline = await pricing_module.list_providers_by_procedure(_public_claim_request())
    request = _public_claim_request()
    with scope_module.network_address_read_scope(
        MANIFEST, None, access_scope_sha256="b" * 64, excluded_network_ids=excluded
    ):
        reply = await pricing_module.list_providers_by_procedure(request)
    document = json.loads(reply.body)
    assert document == json.loads(baseline.body)
    assert document["pagination"]["total"] == 1
    assert {name: document["items"][0][name] for name in ("npi", "city", "state", "zip5")} == {
        "npi": 1000000012,
        "city": "Example City",
        "state": "NJ",
        "zip5": "07001",
    }
    assert "locations" not in document["items"][0] and "claims_geography" not in document["items"][0]
    assert len(request.ctx.sa_session.executions) == 2
    office_read.assert_not_awaited()
