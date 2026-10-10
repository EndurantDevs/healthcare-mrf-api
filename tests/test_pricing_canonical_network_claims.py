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
            assert str(statement._order_by_clauses[-1]).endswith(".npi ASC")
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


def _selected_office_claim_request():
    """Keep selected office identity distinct from the historical claims address."""
    office_by_field = {
        "location_id": "00000000-0000-4000-8000-000000000001",
        "location_key": "a" * 64,
        "first_line": "10 Example Street",
        "second_line": None,
        "city_name": "Selected City",
        "state_name": "NY",
        "postal_code": "10001",
        "country_code": "US",
        "lat": 40.75,
        "long": -73.99,
    }
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1000000012,
                        "city": "Excluded City",
                        "state": "NJ",
                        "zip5": "07001",
                        "total_services": 10,
                        "total_submitted_charges": 1200,
                        "total_allowed_amount": 900,
                    }
                ]
            ),
            FakeResult(scalar=[{"npi": 1000000012, "location": office_by_field}]),
        ],
        args={
            "code": "99213",
            "code_system": "CPT",
            "year": "2023",
            "network_ids": "42,88",
            "state": "NY",
            "city": "Selected City",
            "zip5": "10001",
            "zip_radius_miles": "0",
            "include_evidence": "true",
        },
    )
    return office_by_field, request


@pytest.mark.asyncio
async def test_selected_offices_are_separate_from_historical_claim_geography(canonical_pin, monkeypatch):
    async def no_historical_fallback(_session, _provider_items, **options):
        assert all(options[name] is None for name in ("fallback_state", "fallback_city", "fallback_zip5"))

    monkeypatch.setattr(pricing_module, "_enrich_provider_service_cost_indices", no_historical_fallback)
    office_by_field, request = _selected_office_claim_request()
    reply = await pricing_module.list_providers_by_procedure(request)
    response_body = json.loads(reply.body)
    assert response_body["pagination"]["total"] == 1
    provider_item = response_body["items"][0]
    assert provider_item["locations"] == [office_by_field]
    assert provider_item["claims_geography"] == {"city": "Excluded City", "state": "NJ", "zip5": "07001"}
    assert provider_item["claims_evidence_grain"] == "provider_npi/year/procedure aggregate"
    assert provider_item["total_services"] == 10 and provider_item["avg_submitted_charge"] == 120
    assert not {"city", "state", "zip5", "distance_miles", "priceable", "in_network"}.intersection(provider_item)
    assert response_body["evidence"]["matched_provider_count"] == 1
    assert "matched_provider_location_count" not in response_body["evidence"]
    assert len(request.ctx.sa_session.executions) == 3
    for (query, *_), _options in request.ctx.sa_session.executions[:2]:
        compiled = query.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        office_predicate = sql.split("EXISTS (SELECT", 1)[1].split("GROUP BY", 1)[0]
        assert all(name in office_predicate for name in ("state_code", "city_norm", "postal_code"))
        assert "pricing_provider.state =" not in office_predicate
        assert "pricing_provider.city LIKE" not in office_predicate
        assert compiled.params["_canonical_network_ids"] == [42, 88]
    (hydration, *_), _options = request.ctx.sa_session.executions[-1]
    compiled = hydration.compile(dialect=postgresql.dialect())
    assert "provider_location_binding" in str(compiled)
    assert "count(distinct(selected_procedure_offices.location_key))" in str(compiled)
    assert "octet_length" in str(compiled) and 101 in compiled.params.values()


def test_geo_conjunction_uses_one_actual_office_and_excludes_zip_centroids():
    from api.provider_service_scope import _canonical_procedure_location_filter

    with scope_module.network_address_read_scope(MANIFEST, (42,)):
        predicate = _canonical_procedure_location_filter(
            state="NY",
            city="selected",
            zip5="10001",
            zip_radius=5,
            zip_anchor=(40.75, -73.99),
            coordinates=(40.76, -73.98, 2),
        )
        clause = pricing_module._canonical_network_procedure_clause(
            {}, None, pricing_module.provider_procedure_table.c.npi, location_filter=predicate
        )
        compiled = clause.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        assert sql.count("ST_DWithin(") == 2 and sql.count("EXISTS") == 1
        assert "geo_zip" not in sql and "city_zip" in compiled.params.values()
        assert "entity_address_unified.lat" in sql and "entity_address_unified.long" in sql
        assert "pricing_provider_procedure.npi" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [None, [], [{"npi": 1000000004, "location": {}}]])
async def test_missing_or_over_budget_office_evidence_refuses(retained):
    from api.provider_service_scope import _canonical_procedure_locations
    from tests.test_pricing_api import FakeSession

    with scope_module.network_address_read_scope(MANIFEST, (42,)):
        with pytest.raises(ServiceUnavailable, match="evidence"):
            await _canonical_procedure_locations(
                FakeSession([FakeResult(scalar=retained)]), [{"npi": 1000000012}], None
            )


def test_zip_radius_without_genuine_anchor_refuses():
    from api.provider_service_scope import _canonical_procedure_location_filter

    with scope_module.network_address_read_scope(MANIFEST, (42,)):
        with pytest.raises(ServiceUnavailable, match="ZIP coordinates"):
            _canonical_procedure_location_filter(
                state="",
                city="",
                zip5="10001",
                zip_radius=10,
                coordinates=(None, None, 10),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("alternate", [{"latitude": 41.75, "longitude": -74.99}, None])
async def test_canonical_zip_uses_nearby_zcta_anchor_before_count_and_page(canonical_pin, monkeypatch, alternate):
    historical = AsyncMock(return_value=alternate)
    monkeypatch.setattr(pricing_module, "_lookup_zip_context", historical)
    request = request_for(zip5="10001", zip_radius_miles="10")
    request.ctx.sa_session._results.insert(0, FakeResult(rows=[{"intptlat": "40.75", "intptlon": "-73.99"}]))
    reply = await pricing_module.list_providers_by_procedure(request)
    assert json.loads(reply.body)["pagination"]["total"] == 0
    canonical_pin.assert_awaited_once_with(request, request.args)
    historical.assert_not_awaited()
    assert len(request.ctx.sa_session.executions) == 3
    (lookup, parameters), _ = request.ctx.sa_session.executions[0]
    assert str(lookup) == "select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"
    assert parameters == {"zip_code": "10001"}
    for (statement, *_), _ in request.ctx.sa_session.executions[1:]:
        compiled = statement.compile(dialect=postgresql.dialect())
        assert "ST_DWithin(" in str(compiled) and "EXISTS (SELECT" in str(compiled)
        assert 40.75 in compiled.params.values() and -73.99 in compiled.params.values()
        assert 41.75 not in compiled.params.values()
        assert compiled.params["_canonical_network_ids"] == [42, 88]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [],
        [{"intptlat": None, "intptlon": "-73.99"}],
        [{"intptlat": "invalid", "intptlon": "-73.99"}],
        [{"intptlat": "nan", "intptlon": "-73.99"}],
        [{"intptlat": "40.75", "intptlon": "inf"}],
        [{"intptlat": "91", "intptlon": "-73.99"}],
        [{"intptlat": "40.75", "intptlon": "-181"}],
    ],
)
async def test_canonical_zip_missing_or_invalid_zcta_refuses_before_count(canonical_pin, monkeypatch, rows):
    historical = AsyncMock(return_value={"latitude": 40.75, "longitude": -73.99})
    monkeypatch.setattr(pricing_module, "_lookup_zip_context", historical)
    request = request_for(zip5="10001", zip_radius_miles="10")
    request.ctx.sa_session._results.insert(0, FakeResult(rows=rows))
    with pytest.raises(ServiceUnavailable, match="ZIP coordinates"):
        await pricing_module.list_providers_by_procedure(request)
    historical.assert_not_awaited()
    assert len(request.ctx.sa_session.executions) == 1
    assert scope_module.current_network_address_scope() is None


@pytest.mark.asyncio
async def test_canonical_exact_zip_does_not_resolve_an_anchor(canonical_pin, monkeypatch):
    anchor = AsyncMock(side_effect=AssertionError("exact ZIP must not read coordinates"))
    monkeypatch.setattr(pricing_module, "_canonical_procedure_zip_anchor", anchor)
    reply = await pricing_module.list_providers_by_procedure(request_for(zip5="10001", zip_radius_miles="0"))
    assert json.loads(reply.body)["pagination"]["total"] == 0
    anchor.assert_not_awaited()
