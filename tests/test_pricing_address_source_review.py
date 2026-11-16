# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pricing source thresholds retain coherent offices, ordering and V3 serving."""

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import orjson
import pytest

from api import plan_pricing_projection_read as projections
from api import pricing_address_source_filter as source_filter
from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.plan_pricing_projection_materialize import CardStats, card_fragment
from tests.ptg2_factorized_card_support import MappingResult, completion_row
from tests.test_ptg2_serving import _strict_v3_tables


def _priced_provider(npi, price, network_bound):
    return {
        "npi": npi,
        "provider_name": f"Provider {npi}",
        "prices": [{"negotiated_rate": price}],
        "address_verification": {"network_bound_address": network_bound, "displayed_address_present": True},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
@pytest.mark.parametrize("offset", [0, 1])
async def test_network_prefixes_and_global_tail_use_the_same_effective_order(monkeypatch, minimum, offset):
    providers_by_snapshot = {
        "first": [_priced_provider(101, 10, False), _priced_provider(102, 30, True)],
        "second": [_priced_provider(103, 20, True), _priced_provider(104, 40, False)],
    }
    monkeypatch.setattr(
        serving, "current_network_snapshots_for_plan", AsyncMock(return_value=[("one", "first"), ("two", "second")])
    )
    monkeypatch.setattr(
        serving, "_network_tables_by_snapshot_id", AsyncMock(return_value={"first": object(), "second": object()})
    )
    seen_orders = []

    async def read_network(_session, snapshot, args, pagination, **_options):
        seen_orders.append((args.get("order_by"), pagination.limit, pagination.offset))
        ordered = serving._sort_ptg2_manifest_provider_items(
            providers_by_snapshot[snapshot], args, location_filter_requested=False
        )
        return {"items": ordered[: pagination.limit], "pagination": {"total": 2, "total_is_exact": True}, "query": {}}

    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", read_network)
    response_by_field = await serving.search_current_ptg2_index(
        object(),
        {"plan_id": "SYNTHETIC-PLAN", "min_address_sources": str(minimum)},
        PaginationParams(page=offset + 1, limit=1, offset=offset, source="offset"),
    )
    assert [provider_item["npi"] for provider_item in response_by_field["items"]] == (
        [101, 103] if minimum else [103, 102]
    )[offset : offset + 1]
    assert seen_orders == [("total_allowed_amount" if minimum else None, offset + 1, 0)] * 2
    assert response_by_field["pagination"]["total"] == 4


@pytest.mark.parametrize(
    "args",
    [
        {},
        {"min_address_sources": "0"},
        {"min_address_sources": "3", "state": "IL"},
        {"min_address_sources": "3", "order_by": "distance"},
    ],
)
def test_effective_order_preserves_default_zero_geographic_and_explicit_requests(args):
    assert source_filter.normalize_address_source_order(args) is args


@pytest.mark.asyncio
@pytest.mark.parametrize("cards,minimum", [([], 3), ([{"npi": 101, "zip5": "10001"}], 0)])
async def test_compact_postal_hydration_skips_unchanged_or_empty_requests(cards, minimum):
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("no office query expected")))
    assert await source_filter.qualifying_compact_offices(session, cards, minimum) is cards
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_compact_postal_hydration_refuses_a_missing_qualified_office():
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: iter(()))))
    with pytest.raises(PlanPricingProjectionUnavailable, match="qualifying compact-card office"):
        await source_filter.qualifying_compact_offices(session, [{"npi": 101, "zip5": "10001"}], 3)


def _v3_rate_row(set_id, price_id):
    return {
        "provider_set_global_id_128": set_id,
        "price_set_global_id_128": price_id,
        "serving_content_hash_128": "33" * 16,
        "price_key": 1,
        "source_key": 0,
        "provider_count": 1,
        "reported_code_system": "CPT",
        "reported_code": "27447",
        "plan_id": "SYNTHETIC-PLAN",
        "plan_market_type": "group",
    }


@pytest.mark.asyncio
async def test_nonempty_v3_count_only_search_uses_existing_address_scoped_reads(monkeypatch):
    set_id, price_id = "11" * 16, "22" * 16
    qualified_by_field = {
        "npi": 1234567890,
        "provider_name": "Qualified Provider",
        "city": "Example City",
        "state": "IL",
        "zip5": "10001",
        "location_hash": "entity_address_unified:qualified",
        "location_source": "entity_address_unified",
        "address_payload": {
            "first_line": "100 Qualified Road",
            "source_count": 3,
            "city_name": "Example City",
            "postal_code": "10001",
        },
    }
    locations = AsyncMock(return_value=({set_id}, {set_id: [qualified_by_field]}))
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", locations)
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={set_id: 1}))
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 7, "rate_count": 1}]))
    merge = AsyncMock(return_value=[_v3_rate_row(set_id, price_id)])
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge)
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    monkeypatch.setattr(
        serving, "_prices_for_price_sets", AsyncMock(return_value={price_id: [{"negotiated_rate": 10}]})
    )
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))
    v4_caps = Mock(side_effect=AssertionError("V3 must not read V4 limits"))
    monkeypatch.setattr(serving, "_v4_provider_expansion_request_caps", v4_caps)
    response_by_field = await serving._search_manifest_serving_table(
        object(),
        "snapshot",
        {"plan_id": "SYNTHETIC-PLAN", "code_system": "CPT", "code": "27447", "min_address_sources": "3"},
        PaginationParams(page=1, limit=1, offset=0, source="page"),
        _strict_v3_tables(),
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )
    assert response_by_field["items"][0]["npi"] == 1234567890
    assert response_by_field["items"][0]["prices"][0]["negotiated_rate"] == 10
    assert response_by_field["items"][0]["address"]["first_line"] == "100 Qualified Road"
    assert response_by_field["pagination"]["total"] == 1 and response_by_field["pagination"]["total_is_exact"]
    assert locations.await_args.args[2]["min_address_sources"] == "3"
    assert locations.await_args.kwargs["require_exhaustive"] is True
    assert merge.await_args.kwargs["provider_set_keys"] == [1]
    v4_caps.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["legacy", "factorized"])
async def test_nonempty_compact_read_preserves_prices_and_qualifies_current_postal_fields(kind):
    row_by_field = completion_row(101, "10", "20", 2)
    office_result = SimpleNamespace(
        mappings=lambda: iter([{"npi": 101, "geo_cell": "60601", "city": "Qualified City", "state": "IN"}])
    )
    if kind == "legacy":
        frozen_by_field = orjson.loads(row_by_field["fragment"]) | {
            "minimum_negotiated_rate": 10,
            "maximum_negotiated_rate": 20,
            "rate_count": 2,
        }
        first_result = SimpleNamespace(all=lambda: [(orjson.dumps(frozen_by_field), 1)])
    else:
        first_result = MappingResult([row_by_field])
    session = SimpleNamespace(execute=AsyncMock(side_effect=[first_result, office_result]))
    if kind == "legacy":
        fragments, total = await projections._read_page(
            session,
            SimpleNamespace(
                result_type="provider_cards",
                contract="plan_pricing_card_v2",
                projection_id="projection",
                code_system="CPT",
                code="27447",
            ),
            ["60601"],
            {"min_address_sources": "3"},
            SimpleNamespace(limit=1, offset=0),
        )
        assert total == 1
        card_by_field = orjson.loads(orjson.dumps(fragments[0]))
    else:
        cards = await serving._factorized_card_complete_selected_npis(
            session,
            "projection",
            ("CPT", "27447"),
            ["60601"],
            {"min_address_sources": "3"},
            (101,),
            {101: Decimal(10)},
        )
        card_by_field = cards[0]
    assert card_by_field["provider_name"] == "Frozen provider 101"
    assert (card_by_field["city"], card_by_field["state"], card_by_field["zip5"]) == ("Qualified City", "IN", "60601")
    assert (
        card_by_field["minimum_negotiated_rate"],
        card_by_field["maximum_negotiated_rate"],
        card_by_field["rate_count"],
    ) == (10, 20, 2)
    assert session.execute.await_count == 2
    assert session.execute.await_args.args[1] == {
        "office_npis": [101],
        "office_cells": ["60601"],
        "min_address_sources": 3,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "minimum,maximum",
    [
        ("9007199254740993.12345", "9007199254740993.12346"),
        ("0.0000000000000000001234567890123456789", "0.0000000000000000001234567890123456790"),
        ("184467440737095516160000000000000000000", "184467440737095516160000000000000000001"),
    ],
)
async def test_legacy_postal_rewrite_preserves_exact_decimal_wire_prices(minimum, maximum):
    frozen_provider_by_field = {
        "npi": 101,
        "provider_name": "Frozen Provider",
        "zip5": "10001",
        "city": "Frozen City",
        "state": "IL",
    }
    original_fragment = card_fragment(CardStats(frozen_provider_by_field, Decimal(minimum), Decimal(maximum), 2))
    page = SimpleNamespace(all=lambda: [(original_fragment, 1)])
    office = SimpleNamespace(
        mappings=lambda: iter([{"npi": 101, "geo_cell": "10001", "city": "Qualified City", "state": "IN"}])
    )
    session = SimpleNamespace(execute=AsyncMock(side_effect=[page, page, office]))
    request = SimpleNamespace(
        result_type="provider_cards",
        contract="plan_pricing_card_v2",
        projection_id="projection",
        code_system="CPT",
        code="27447",
    )
    pagination = SimpleNamespace(limit=1, offset=0)
    unfiltered, _ = await projections._read_page(session, request, ["10001"], {}, pagination)
    filtered, total = await projections._read_page(
        session, request, ["10001"], {"min_address_sources": "3"}, pagination
    )
    assert total == 1 and orjson.dumps(unfiltered[0]) == original_fragment
    original_by_field = json.loads(original_fragment, parse_float=Decimal, parse_int=Decimal)
    current_by_field = json.loads(orjson.dumps(filtered[0]), parse_float=Decimal, parse_int=Decimal)
    assert current_by_field["minimum_negotiated_rate"] == Decimal(minimum)
    assert current_by_field["maximum_negotiated_rate"] == Decimal(maximum)
    assert current_by_field == original_by_field | {"city": "Qualified City", "state": "IN"}


def _install_snapshot_npi_reads(monkeypatch):
    set_id, price_id = "11" * 16, "22" * 16
    qualified_by_field = {
        "npi": 1234567890,
        "provider_name": "Provider",
        "city": "Qualified City",
        "state": "IL",
        "zip5": "10001",
        "location_hash": "entity_address_unified:qualified",
        "address_payload": {"first_line": "100 Qualified Road", "source_count": 3},
    }
    unfiltered_by_field = qualified_by_field | {
        "city": "Original City",
        "address_payload": {"first_line": "200 Original Road", "source_count": 1},
    }
    graph = AsyncMock(side_effect=[{1234567890: (7,)}, {7: (1,)}])
    monkeypatch.setattr(serving, "lookup_shared_graph_members_from_db", graph)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", AsyncMock(return_value={1: set_id}))
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={set_id: 1}))
    forward = AsyncMock(return_value=[SimpleNamespace(provider_set_key=1)])
    monkeypatch.setattr(serving, "_shared_forward_entries_for_code_rows", forward)
    loader = AsyncMock(return_value=[{"code_key": 7, "rate_count": 1}])
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", loader)
    location = AsyncMock(
        side_effect=lambda _session, _tables, args, **_kwargs: (
            [qualified_by_field] if int(args.get("min_address_sources") or 0) <= 3 else []
        )
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location)
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", AsyncMock(return_value=[unfiltered_by_field]))
    monkeypatch.setattr(serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.entity_address_unified"))
    merge = AsyncMock(return_value=[_v3_rate_row(set_id, price_id)])
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge)
    prices = AsyncMock(return_value={price_id: [{"negotiated_rate": 10}]})
    monkeypatch.setattr(serving, "_prices_for_price_sets", prices)
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    return SimpleNamespace(graph=graph, forward=forward, loader=loader, location=location, merge=merge, prices=prices)


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0", "3", "4"])
@pytest.mark.parametrize("selector", ["snapshot_id", "source_key"])
async def test_snapshot_scoped_exact_npi_count_filter_does_not_require_a_plan(monkeypatch, minimum, selector):
    reads = _install_snapshot_npi_reads(monkeypatch)
    tables = _strict_v3_tables(snapshot_id="pinned-snapshot")
    args_by_name = {
        selector: "pinned-snapshot" if selector == "snapshot_id" else tables.source_key,
        "npi": "1234567890",
        "code_system": "CPT",
        "code": "27447",
    }
    if minimum is not None:
        args_by_name["min_address_sources"] = minimum
    response_by_field = await serving._search_manifest_serving_table(
        object(),
        tables.snapshot_id,
        args_by_name,
        PaginationParams(page=1, limit=1, offset=0, source="page"),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )
    assert response_by_field is not None
    if minimum == "4":
        assert response_by_field["items"] == [] and response_by_field["pagination"]["total"] == 0
        reads.prices.assert_not_awaited()
    else:
        provider_by_field = response_by_field["items"][0]
        assert provider_by_field["npi"] == 1234567890 and provider_by_field["prices"][0]["negotiated_rate"] == 10
        assert provider_by_field["address"]["source_count"] == (3 if minimum == "3" else 1)
        assert provider_by_field["address"]["first_line"] == (
            "100 Qualified Road" if minimum == "3" else "200 Original Road"
        )
        assert reads.merge.await_args.kwargs["provider_set_keys"] == [1]
        assert reads.merge.await_args.args[1].snapshot_id == tables.snapshot_id
    if minimum in {"3", "4"}:
        assert reads.forward.await_args.kwargs["provider_set_keys"] == (1,)
        assert reads.location.await_args.kwargs["candidate_npis"] == (1234567890,)
        assert reads.location.await_args.kwargs["limit"] == 1
        assert all(not call.args[2].get("plan_id") for call in reads.loader.await_args_list)
    else:
        reads.location.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "args",
    [
        {"snapshot_id": "snapshot", "min_address_sources": "3"},
        {"npi": "1234567890", "min_address_sources": "3"},
        {"snapshot_id": "snapshot", "npi": "1234567890", "min_address_sources": "0"},
    ],
)
async def test_planless_graph_requires_positive_npi_and_explicit_source(monkeypatch, args):
    reader = AsyncMock(side_effect=AssertionError("unsupported scope must not read code or offices"))
    monkeypatch.setattr(serving, "_graph_candidates_for_request", reader)
    result = await serving._graph_location_matches(
        object(),
        _strict_v3_tables(),
        {"code_system": "CPT", "code": "27447", **args},
        candidate_limit=1,
        plan_id="",
    )
    assert result is None
    reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_planless_threshold_does_not_expand_an_unproven_provider_set_scope(monkeypatch):
    loader = AsyncMock(side_effect=AssertionError("unproven scope cannot load snapshot-wide code rows"))
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", loader)
    monkeypatch.setattr(serving, "_shared_rate_provider_set_keys", AsyncMock(return_value=()))
    keys = await serving._request_rate_provider_set_keys(
        object(),
        _strict_v3_tables(),
        {"npi": "1234567890", "snapshot_id": "snapshot", "min_address_sources": "3"},
        {"plan_id": "", "requested_code": "27447", "requested_system": "CPT"},
        None,
    )
    assert keys == frozenset()
    loader.assert_not_awaited()
