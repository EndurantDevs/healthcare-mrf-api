# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Filtered pricing refuses incomplete evidence and bounded partial results."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest

from api import plan_pricing_legacy_source_aggregates as legacy
from api import plan_pricing_legacy_source_memberships as memberships
from api import plan_pricing_projection_read as projections
from api import plan_pricing_projection_source as provider_source
from api import plan_pricing_state_scan as state_scan
from api import plan_pricing_state_scan_sources as state_sources
from api import pricing_address_source_filter as predicates
from api import ptg2_serving as serving
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, PlanPricingProjectionUnsupported
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_legacy_source_aggregates import _install_retained_rates, _request_and_session


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary,error_type,message",
    [
        ("offices", PlanPricingProjectionUnsupported, "membership bound"),
        ("memberships", PlanPricingProjectionUnsupported, "membership bound"),
        ("provider_sets", PlanPricingProjectionUnsupported, "provider-set bound"),
        ("rate_rows", PlanPricingProjectionUnsupported, "rate-row bound"),
        ("stored_bytes", PlanPricingProjectionUnsupported, "byte bound"),
        ("dictionary", PlanPricingProjectionUnavailable, "membership is incomplete"),
        ("rates", PlanPricingProjectionUnavailable, "retained rate rows are unavailable"),
        ("source", PlanPricingProjectionUnavailable, "retained source is unavailable"),
    ],
)
async def test_legacy_aggregate_refuses_incomplete_cells_instead_of_paging_them(
    monkeypatch, boundary, error_type, message
):
    selection, _, merge = _install_retained_rates(monkeypatch)
    request, session = _request_and_session()
    if boundary in {"offices", "memberships"}:
        monkeypatch.setattr(legacy, "_MAX_PARTIAL_MEMBERSHIPS", 1 if boundary == "offices" else 2)
    if boundary == "provider_sets":
        monkeypatch.setattr(legacy, "_MAX_PARTIAL_PROFILES", 1)
    if boundary == "rate_rows":
        monkeypatch.setattr(legacy, "_MAX_PARTIAL_PROFILES", 2)
        merge.return_value = merge.return_value + merge.return_value[:1]
    if boundary == "stored_bytes":
        monkeypatch.setattr(legacy, "MAX_AGGREGATE_REQUEST_STORED_BYTES", 1)
    if boundary == "dictionary":
        monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={}))
    if boundary == "rates":
        merge.return_value = None
    if boundary == "source":
        selection.serving_tables_for_snapshot = lambda _snapshot: None
    with pytest.raises(error_type, match=message):
        await legacy.read_legacy_source_aggregate_page(
            session,
            request,
            ["10001", "10002"],
            {"min_address_sources": "3"},
            SimpleNamespace(limit=1, offset=0),
            selection,
            None,
        )
    if boundary != "stored_bytes":
        serving._version_three_bounded_prices_by_key.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["offices", "admission", "memberships", "prices"])
async def test_empty_qualified_population_never_fabricates_provider_or_rate_totals(monkeypatch, reason):
    selection, membership_reader, merge = _install_retained_rates(monkeypatch)
    request, session = _request_and_session()
    if reason == "offices":
        session.execute.return_value = SimpleNamespace(mappings=lambda: iter(()))
    elif reason == "memberships":
        membership_reader.return_value = {101: (), 102: ()}
    elif reason == "prices":
        serving._version_three_bounded_prices_by_key.return_value = {}
    items, total = await legacy.read_legacy_source_aggregate_page(
        session,
        request,
        ["10001", "10002"],
        {"min_address_sources": "3"},
        SimpleNamespace(limit=1, offset=0),
        selection,
        () if reason == "admission" else None,
    )
    assert items == [] and total == 0
    if reason != "prices":
        merge.assert_not_awaited()
    if reason in {"offices", "admission"}:
        membership_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_retained_rate_binding_fails_before_reverse_membership_reads(monkeypatch):
    selection, membership_reader, _ = _install_retained_rates(monkeypatch)
    selection.serving_tables_for_snapshot = lambda _snapshot: None
    with pytest.raises(PlanPricingProjectionUnavailable, match="retained rates are unavailable"):
        await legacy._binding_rate_rows(
            object(), selection, selection.in_network_bindings[0], {}, {101: ()}, {"memberships": 0}
        )
    membership_reader.assert_not_awaited()


def test_shared_groups_count_each_provider_set_once_per_provider():
    assert memberships._bounded_sets_by_npi(
        (101,), {101: ("group-a", "group-b")}, {"group-a": ("set",), "group-b": ("set",)}, 1
    ) == {101: ("set",)}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["corrupt", "projection_bound", "dictionary"])
async def test_reverse_membership_errors_keep_corruption_distinct_from_capacity(monkeypatch, failure):
    reverse = AsyncMock(return_value={101: (1,)})
    monkeypatch.setattr(serving, "_v4_sets_by_npi", reverse)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", AsyncMock(return_value={}))
    if failure == "corrupt":
        reverse.side_effect = PTG2ManifestArtifactError("invalid membership payload")
        expected, message = PTG2ManifestArtifactError, "invalid membership payload"
    elif failure == "projection_bound":
        reverse.side_effect = PTG2ManifestArtifactError("PTG2 shared graph selection exceeds max_projection_members")
        expected, message = PlanPricingProjectionUnsupported, "membership bound"
    else:
        expected, message = PlanPricingProjectionUnavailable, "membership is incomplete"
    with pytest.raises(expected, match=message):
        await memberships.bounded_source_provider_sets(object(), SimpleNamespace(uses_v4_graph=True), (101,), 7)


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted", [None, (SimpleNamespace(binding_ordinal=1),)])
async def test_filtered_factorized_aggregate_reconstructs_only_admitted_bindings(monkeypatch, admitted):
    reader = AsyncMock(return_value=([], 0))
    monkeypatch.setattr(projections, "_read_page", reader)
    selection = SimpleNamespace(
        in_network_bindings=(SimpleNamespace(binding_ordinal=0), SimpleNamespace(binding_ordinal=1))
    )
    result = await projections._read_source_filtered_page(
        object(),
        SimpleNamespace(result_type="rate_aggregates", contract="plan_pricing_factorized_v4"),
        ["10001"],
        {"min_address_sources": "3"},
        SimpleNamespace(limit=1, offset=0),
        selection,
        admitted,
        None,
    )
    assert result == ([], 0)
    assert reader.await_args.kwargs["binding_ordinals"] == ((0, 1) if admitted is None else (1,))


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum,rows", [(0, []), (3, []), (3, [101, 102])])
async def test_state_identity_page_qualifies_offices_before_hydrating_only_the_selected_page(
    monkeypatch, minimum, rows
):
    state_rows = [{"npi": npi, "provider_fragment": b"frozen"} for npi in rows]
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: state_rows))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    hydrate = AsyncMock(return_value={101: b"qualified"})
    monkeypatch.setattr(state_sources, "qualifying_state_fragments", hydrate)
    npis, fragments_by_npi, has_more = await state_scan._read_state_npis(
        session,
        "projection",
        "IL",
        0,
        1,
        binding_ordinals=(1,),
        min_address_sources=minimum,
    )
    assert npis == ((101,) if rows else ()) and has_more == bool(rows)
    if rows:
        assert fragments_by_npi == {101: b"qualified"}
        hydrate.assert_awaited_once_with(session, state_rows[:1], "IL", 3)
    else:
        assert fragments_by_npi == {}
        hydrate.assert_not_awaited()
    if minimum:
        assert session.execute.await_args.args[1]["min_address_sources"] == minimum


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "choices", [(), ({"state": "IL", "state_address_rank": 2},), ({"state": "IL", "state_address_rank": 1},) * 2]
)
async def test_state_hydration_refuses_missing_or_ambiguous_qualified_offices(monkeypatch, choices):
    monkeypatch.setattr(state_sources, "projection_provider_rows_for_npis", AsyncMock(return_value={101: choices}))
    with pytest.raises(PlanPricingProjectionUnavailable, match="qualifying state-scan office"):
        await state_sources.qualifying_state_fragments(
            object(), [{"npi": 101, "provider_fragment": b"frozen"}], "IL", 3
        )


def test_zero_threshold_preserves_unqualified_card_and_exact_location_predicates():
    assert predicates.qualifying_cell_sql("provider", 0) == "TRUE"
    assert predicates.qualifying_location_sql("location", 0) == "TRUE"


@pytest.mark.asyncio
async def test_filtered_office_hydration_uses_the_shared_current_relation_and_bound_count(monkeypatch):
    relation = "current_offices"
    monkeypatch.setattr(provider_source, "scoped_address_relation_sql", lambda *_args, **_kwargs: relation)
    monkeypatch.setattr(provider_source, "_hydrate_state_address_provenance", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(return_value=[{"npi": 101, "zip5": "10001", "state": "IL"}]))
    rows_by_npi = await provider_source.projection_provider_rows_for_npis(session, (101,), min_address_sources=3)
    statement, parameters_by_name = session.execute.await_args.args
    assert "FROM current_offices addr" in str(statement)
    assert str(statement).index("addr.source_count >= 3") < str(statement).index("zip_address_rank")
    assert parameters_by_name["min_address_sources"] == 3 and parameters_by_name["npis"] == [101]
    assert tuple(rows_by_npi) == (101,)
