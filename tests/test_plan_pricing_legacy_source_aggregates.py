# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy filtered aggregates keep their full ZIP shape and exact medians."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_legacy_source_aggregates as legacy
from api import plan_pricing_projection_read as cards
from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api.plan_pricing_projection_contract import LEGACY_PROJECTION_CONTRACT, PlanPricingProjectionUnsupported


def _install_retained_rates(monkeypatch):
    set_ids = ("11" * 16, "22" * 16)
    price_ids = ("33" * 16, "44" * 16)
    binding = SimpleNamespace(snapshot_id="retained", source_key="source", plan_id="plan", plan_market_type="group")
    tables = SimpleNamespace(
        storage_generation="generation", shared_snapshot_key=7, coverage_scope_id=1, network_names=[]
    )
    selection = SimpleNamespace(in_network_bindings=(binding,), serving_tables_for_snapshot=lambda _snapshot: tables)
    monkeypatch.setattr(legacy, "prepare_plan_release_bindings", AsyncMock(return_value=selection))
    monkeypatch.setattr(serving, "_payload_schema", lambda _tables: "retained_rates")
    monkeypatch.setattr(serving, "_required_shared_snapshot_key", lambda _tables: 7)
    memberships = AsyncMock(return_value={101: (set_ids[0],), 102: set_ids})
    monkeypatch.setattr(legacy, "bounded_source_provider_sets", memberships)
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value=dict(zip(set_ids, (1, 2)))))
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 1}]))
    rate_rows = [
        {"provider_set_global_id_128": set_id, "price_set_global_id_128": price_id, "price_key": index}
        for index, (set_id, price_id) in enumerate(zip(set_ids, price_ids), 1)
    ]
    merge = AsyncMock(return_value=rate_rows)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merge)
    prices_by_set = {
        price_ids[0]: [{"negotiated_rate": "10"}, {"negotiated_rate": "20"}],
        price_ids[1]: [{"negotiated_rate": "100"}],
    }
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(side_effect=AssertionError("unbounded price hydration must not run")),
    )
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(return_value={index: prices_by_set[price_id] for index, price_id in enumerate(price_ids, 1)}),
    )
    return selection, memberships, merge


def _request_and_session():
    request = SimpleNamespace(
        result_type="rate_aggregates",
        contract=LEGACY_PROJECTION_CONTRACT,
        projection_id="a" * 64,
        code_system="CPT",
        code="27447",
    )
    rows = [{"npi": 101, "geo_cell": "10001"}, {"npi": 102, "geo_cell": "10002"}]
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: iter(rows))))
    return request, session


@pytest.mark.asyncio
async def test_explicit_legacy_aggregate_keeps_shape_and_complete_filtered_rate_distribution(monkeypatch):
    selection, memberships, merge = _install_retained_rates(monkeypatch)
    request, session = _request_and_session()
    monkeypatch.setattr(cards, "geo_cells", AsyncMock(return_value=["10001", "10002"]))
    response_by_field = await cards._projection_page_response(
        session,
        request,
        {"view": "card", "include_providers": False, "min_address_sources": "3"},
        SimpleNamespace(limit=10, offset=0, page=1),
        None,
        selection,
    )
    assert response_by_field["result_type"] == "rate_aggregates"
    assert response_by_field["query"]["view"] == "card"
    assert response_by_field["query"]["projection_contract"] == LEGACY_PROJECTION_CONTRACT
    assert response_by_field["pagination"]["total"] == 2 and response_by_field["pagination"]["total_is_exact"]
    entries = [orjson.loads(orjson.dumps(entry)) for entry in response_by_field["items"]]
    assert [
        (entry["geo_cell"], entry["provider_count"], entry["rate_count"], entry["median_negotiated_rate"])
        for entry in entries
    ] == [("10001", 1, 2, 15), ("10002", 1, 3, 20)]
    sql = str(session.execute.await_args.args[0])
    assert sql.index("source_address.source_count") < sql.index("LIMIT :office_probe_limit")
    memberships.assert_awaited_once_with(
        session, selection.serving_tables_for_snapshot("retained"), (101, 102), legacy._MAX_PARTIAL_MEMBERSHIPS
    )
    assert (
        serving._version_three_bounded_prices_by_key.await_args.kwargs["maximum_atom_count"]
        == legacy._MAX_PARTIAL_RATE_VALUES
    )
    assert merge.await_args.kwargs["provider_set_keys"] == (1, 2)


@pytest.mark.asyncio
async def test_legacy_aggregate_deduplicates_same_named_snapshot_and_rejects_incomplete_work(monkeypatch):
    selection, memberships, _ = _install_retained_rates(monkeypatch)
    selection.in_network_bindings = selection.in_network_bindings * 2
    request, session = _request_and_session()
    args_by_name = {"min_address_sources": "3"}
    fragments, total = await legacy.read_legacy_source_aggregate_page(
        session,
        request,
        ["10001", "10002"],
        args_by_name,
        SimpleNamespace(limit=1, offset=1),
        selection,
        None,
    )
    assert total == 2 and len(fragments) == 1
    assert orjson.loads(orjson.dumps(fragments[0]))["geo_cell"] == "10002"
    assert memberships.await_count == 1
    monkeypatch.setattr(legacy, "_MAX_PARTIAL_RATE_VALUES", 3)
    request, session = _request_and_session()
    with pytest.raises(PlanPricingProjectionUnsupported, match="rate-value bound"):
        await legacy.read_legacy_source_aggregate_page(
            session,
            request,
            ["10001", "10002"],
            args_by_name,
            SimpleNamespace(limit=1, offset=0),
            selection,
            None,
        )


@pytest.mark.asyncio
async def test_legacy_card_binding_subset_preserves_existing_admission_fallback(monkeypatch):
    selection, memberships, _ = _install_retained_rates(monkeypatch)
    request, session = _request_and_session()
    request.result_type = "provider_cards"
    binding = selection.in_network_bindings[0]
    binding.binding_ordinal = 0
    selection.in_network_bindings = (binding, SimpleNamespace(binding_ordinal=1))
    monkeypatch.setattr(cards, "geo_cells", AsyncMock(return_value=["10001", "10002"]))
    result = await cards._projection_page_response(
        session,
        request,
        {"view": "card", "min_address_sources": "3"},
        SimpleNamespace(limit=10, offset=0, page=1),
        (binding,),
        selection,
    )
    assert result is None
    memberships.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_aggregate_rejects_wide_prices_before_atom_hydration(monkeypatch):
    from process.ptg_parts.ptg2_manifest_artifacts import ManifestReadLimitError

    selection, _, _ = _install_retained_rates(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_version_three_bounded_prices_by_key",
        AsyncMock(side_effect=ManifestReadLimitError("PTG2 v3 price hydration exceeds its atom limit")),
    )
    request, session = _request_and_session()
    with pytest.raises(PlanPricingProjectionUnsupported, match="rate-value bound"):
        await legacy.read_legacy_source_aggregate_page(
            session,
            request,
            ["10001", "10002"],
            {"min_address_sources": "3"},
            SimpleNamespace(limit=1, offset=0),
            selection,
            None,
        )
    serving._prices_for_price_sets.assert_not_awaited()
