# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_serving as serving
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)

_PROVIDER_SET_ID = "11" * 16
_OTHER_PROVIDER_SET_ID = "12" * 16
_PRICE_SET_ID = "21" * 16
_NPI = 1234567890
_CODE_ROW = {
    "code_key": 7,
    "plan_id": "synthetic-plan",
    "plan_market_type": "group",
    "reported_code_system": "CPT",
    "reported_code": "99213",
    "negotiation_arrangement": "FFS",
    "rate_count": 1,
}
_RATE_ROW = {
    "serving_content_hash_128": "31" * 16,
    "plan_id": "synthetic-plan",
    "plan_market_type": "group",
    "reported_code_system": "CPT",
    "reported_code": "99213",
    "negotiation_arrangement": "FFS",
    "provider_set_global_id_128": _PROVIDER_SET_ID,
    "provider_count": 1,
    "price_set_global_id_128": _PRICE_SET_ID,
    "price_key": 9,
    "source_key": 0,
    "network_names": [],
    "_ptg_provider_set_key": 3,
}
_PROVIDER_ROW = {
    "npi": _NPI,
    "provider_name": "Synthetic Provider",
    "address_payload": {"city": "Example City", "state": "IL"},
}


def _query_args(**overrides):
    args_by_name = {
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "code_system": "CPT",
        "code": "99213",
    }
    args_by_name.update(overrides)
    return args_by_name


def _install_base_dependencies(monkeypatch, *, merge_result=None):
    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=[dict(_RATE_ROW)] if merge_result is None else merge_result),
    )
    monkeypatch.setattr(
        serving,
        "_hydrate_provider_set_network_names",
        AsyncMock(),
    )
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={_PRICE_SET_ID: []}),
    )
    monkeypatch.setattr(
        serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_rows_for_sets",
        AsyncMock(return_value={_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]}),
    )
    monkeypatch.setattr(
        serving,
        "_exact_npi_provider_rows_by_set",
        AsyncMock(return_value={_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={_PROVIDER_SET_ID: 3}),
    )


async def _search(*, args=None, code_rows=None, pagination=None, serving_tables=None):
    session = FakeSession(
        [FakeResult([dict(_CODE_ROW) for _ in range(1)])] if code_rows is None else [FakeResult(code_rows)]
    )
    response = await serving._search_manifest_serving_table(
        session,
        "synthetic-snapshot",
        args or _query_args(),
        pagination or SimpleNamespace(limit=10, offset=0),
        serving_tables or strict_v3_tables(snapshot_id="synthetic-snapshot"),
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )
    return response, session


@pytest.mark.asyncio
async def test_manifest_search_rejects_unscoped_text_query_before_io():
    """Leave free-text and incomplete requests to another serving path."""

    response, session = await _search(args=_query_args(q="provider"))
    assert response is None
    assert session.calls == []


@pytest.mark.asyncio
async def test_manifest_search_returns_exact_empty_for_empty_npi_scope(monkeypatch):
    """Return a trustworthy empty response for a proven empty NPI scope."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=serving._ExplicitNpiGraphScope(_NPI, ())),
    )

    response, session = await _search(args=_query_args(npi=str(_NPI)))
    assert response["items"] == []
    assert response["pagination"]["total_is_exact"] is True
    assert session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("location_result", [None, (set(), {})])
async def test_manifest_location_preselection_distinguishes_unavailable_and_empty(
    monkeypatch,
    location_result,
):
    """Distinguish an unavailable location projection from a proven empty scope."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(return_value=location_result),
    )

    response, session = await _search(args=_query_args(state="IL"))
    assert response is None if location_result is None else response["items"] == []
    assert session.calls == []


@pytest.mark.asyncio
async def test_manifest_location_rejects_unknown_provider_set(monkeypatch):
    """Reject location evidence that the sealed provider dictionary cannot map."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(return_value=({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [_PROVIDER_ROW]})),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={}),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="unknown provider set"):
        await _search(args=_query_args(state="IL"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code_rows", "error_match"),
    [
        ([{**_CODE_ROW, "code_key": None}], "invalid key"),
        ([{**_CODE_ROW, "rate_count": 0}], None),
    ],
)
async def test_manifest_code_metadata_fails_closed(monkeypatch, code_rows, error_match):
    """Reject invalid keys and treat zero sealed rate cardinality as unavailable."""

    _install_base_dependencies(monkeypatch)
    if error_match:
        with pytest.raises(PTG2ManifestArtifactError, match=error_match):
            await _search(code_rows=code_rows)
        return
    response, _session = await _search(code_rows=code_rows)
    assert response is None


@pytest.mark.asyncio
async def test_manifest_strict_cost_selection_propagates_unavailability(monkeypatch):
    """Do not fall back to unordered serving when exact cost expansion is unavailable."""

    _install_base_dependencies(monkeypatch)
    selector = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_strict_cost_provider_expansion_selection", selector)

    response, _session = await _search(args=_query_args(include_providers=True))
    assert response is None
    selector.assert_awaited_once()


@pytest.mark.asyncio
async def test_manifest_general_merge_propagates_unavailability(monkeypatch):
    """Do not shape a response when the bounded forward reader is unavailable."""

    _install_base_dependencies(monkeypatch, merge_result=None)
    monkeypatch.setattr(
        serving,
        "_merge_manifest_code_variant_rows",
        AsyncMock(return_value=None),
    )
    response, _session = await _search()
    assert response is None


@pytest.mark.asyncio
async def test_manifest_price_filter_returns_proven_empty_response(monkeypatch):
    """Return exact empty when decoded prices do not match the requested rate."""

    _install_base_dependencies(monkeypatch)
    response, _session = await _search(args=_query_args(negotiated_rate="999.00"))
    assert response["items"] == []
    assert response["pagination"]["total"] == 0


def _install_deferred_location(monkeypatch, *, location_result, rate_row=None):
    _install_base_dependencies(
        monkeypatch,
        merge_result=[dict(rate_row or _RATE_ROW)],
    )
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={_PRICE_SET_ID: [{"negotiated_rate": "125.00"}]}),
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(return_value=location_result),
    )


@pytest.mark.asyncio
async def test_deferred_location_requires_provider_set_keys(monkeypatch):
    """Reject price-filtered location rows without a sealed provider-set key."""

    _install_deferred_location(
        monkeypatch,
        location_result=({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [_PROVIDER_ROW]}),
        rate_row={**_RATE_ROW, "_ptg_provider_set_key": None},
    )
    with pytest.raises(PTG2ManifestArtifactError, match="missing provider-set keys"):
        await _search(args=_query_args(state="IL", negotiated_rate="125.00"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("location_result", "expected"),
    [
        (None, "unavailable"),
        ((set(), {}), "empty"),
        (({_OTHER_PROVIDER_SET_ID}, {_OTHER_PROVIDER_SET_ID: [_PROVIDER_ROW]}), "mismatch"),
    ],
)
async def test_deferred_location_projection_fail_closed(
    monkeypatch,
    location_result,
    expected,
):
    """Classify unavailable, empty, and inconsistent deferred location evidence."""

    _install_deferred_location(monkeypatch, location_result=location_result)
    if expected == "mismatch":
        with pytest.raises(PTG2ManifestArtifactError, match="did not retain"):
            await _search(args=_query_args(state="IL", negotiated_rate="125.00"))
        return
    response, _session = await _search(args=_query_args(state="IL", negotiated_rate="125.00"))
    assert response is None if expected == "unavailable" else response["items"] == []


@pytest.mark.asyncio
async def test_exact_npi_enrichment_propagates_unavailability(monkeypatch):
    """Do not broaden an exact-NPI request when its scoped enrichment is unavailable."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=serving._ExplicitNpiGraphScope(_NPI, (3,))),
    )
    monkeypatch.setattr(
        serving,
        "_exact_npi_provider_rows_by_set",
        AsyncMock(return_value=None),
    )

    response, _session = await _search(args=_query_args(npi=str(_NPI), include_providers=True))
    assert response is None


@pytest.mark.asyncio
async def test_broad_provider_enrichment_propagates_unavailability(monkeypatch):
    """Do not emit partial provider rows when broad enrichment is unavailable."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_provider_rows_for_sets",
        AsyncMock(return_value=None),
    )
    response, _session = await _search(args=_query_args(include_providers=True, order_by="provider_name"))
    assert response is None


@pytest.mark.asyncio
async def test_providerless_set_retains_an_explicit_status(monkeypatch):
    """Retain a rate occurrence when its provider set has no NPI members."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_provider_rows_for_sets",
        AsyncMock(return_value={_PROVIDER_SET_ID: []}),
    )
    response, _session = await _search(args=_query_args(include_providers=True, order_by="provider_name"))

    assert response["items"][0]["npi"] is None
    assert response["items"][0]["provider_expansion_status"] == "no_npi_members"


@pytest.mark.asyncio
async def test_filtered_providerless_set_returns_no_result(monkeypatch):
    """Do not substitute an NPI-free row for an explicit provider filter."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_provider_rows_for_sets",
        AsyncMock(return_value={_PROVIDER_SET_ID: []}),
    )
    response, _session = await _search(
        args=_query_args(
            include_providers=True,
            order_by="provider_name",
            provider_sex_code="F",
        )
    )
    assert response is None


@pytest.mark.asyncio
async def test_unexpanded_rate_page_stops_at_public_limit(monkeypatch):
    """Stop shaping unexpanded rows once the requested public page is full."""

    _install_base_dependencies(
        monkeypatch,
        merge_result=[dict(_RATE_ROW), {**_RATE_ROW, "source_key": 1}],
    )
    response, _session = await _search(pagination=SimpleNamespace(limit=1, offset=0))
    assert len(response["items"]) == 1


def _exact_selection(*, rank_by_key):
    return serving._ProviderExpansionSelection(
        row_data=[dict(_RATE_ROW)],
        providers_by_set={
            _PROVIDER_SET_ID: [
                dict(_PROVIDER_ROW),
                {**_PROVIDER_ROW, "npi": 1234567891},
            ]
        },
        rank_by_key=rank_by_key,
        exhausted=False,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("materializes_selected_key", [True, False])
async def test_exact_cost_page_filters_unranked_rows_and_requires_witness(
    monkeypatch,
    materializes_selected_key,
):
    """Filter unrelated providers and require every ranked provider witness."""

    _install_base_dependencies(monkeypatch)
    selected_key = ("npi", str(_NPI), "CPT", "99213", "FFS", "0")
    rank_key = (
        selected_key
        if materializes_selected_key
        else (
            "npi",
            "9999999999",
            "CPT",
            "99213",
            "FFS",
            "0",
        )
    )
    selector = AsyncMock(return_value=_exact_selection(rank_by_key={rank_key: 0}))
    monkeypatch.setattr(serving, "_strict_cost_provider_expansion_selection", selector)

    if not materializes_selected_key:
        with pytest.raises(PTG2ManifestArtifactError, match="failed to materialize"):
            await _search(args=_query_args(include_providers=True))
        return
    response, _session = await _search(args=_query_args(include_providers=True))
    assert [item["npi"] for item in response["items"]] == [_NPI]


@pytest.mark.asyncio
async def test_location_page_requires_exhaustion_proof(monkeypatch):
    """Reject a short public page unless both membership and rate reads are exhausted."""

    _install_base_dependencies(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_rate_candidate_limit",
        lambda *_args, **_kwargs: 2,
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_serving_row_limit",
        lambda *_args, **_kwargs: 1,
    )
    location_providers = [{**_PROVIDER_ROW, "npi": _NPI + offset} for offset in range(2)]
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_provider_matches",
        AsyncMock(
            return_value=(
                {_PROVIDER_SET_ID},
                {_PROVIDER_SET_ID: location_providers},
            )
        ),
    )
    monkeypatch.setattr(
        serving,
        "_merge_ptg2_provider_rate_items",
        lambda response_items: response_items[:1],
    )

    with pytest.raises(PTG2ManifestArtifactError, match="prove the requested page"):
        await _search(
            args=_query_args(
                state="IL",
                include_providers=True,
                order_by="distance",
            )
        )


def _install_ranked_geo_rate_batch(monkeypatch, split_group):
    from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables

    second_price_set = "22" * 16
    second_rate_by_field = {
        **_RATE_ROW,
        "serving_content_hash_128": "32" * 16,
        "price_set_global_id_128": second_price_set,
        "price_key": 10,
        "negotiation_arrangement": "capitation" if split_group else "FFS",
    }
    _install_base_dependencies(monkeypatch, merge_result=[dict(_RATE_ROW), second_rate_by_field])
    monkeypatch.setattr(serving, "_version_three_provider_pages_for_keys", AsyncMock(return_value=None))
    monkeypatch.setattr(
        serving,
        "_shared_rows_for_scope",
        AsyncMock(return_value=[dict(_RATE_ROW), second_rate_by_field]),
    )
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(
            return_value={
                _PRICE_SET_ID: [{"negotiated_rate": "125.00"}],
                second_price_set: [{"negotiated_rate": "135.00"}],
            }
        ),
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_source_provenance_for_rows",
        AsyncMock(return_value={0: {"source_key": 0, "source_artifact_name": "synthetic rates"}}),
    )
    provider_by_field = {**_PROVIDER_ROW, "distance_miles": 1.25}
    matches = ({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [provider_by_field]})
    old_location_reader = AsyncMock(return_value=matches)
    graph_projection = AsyncMock(return_value=matches)
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", old_location_reader)
    monkeypatch.setattr(serving, "_project_graph_candidates", graph_projection)
    args = _query_args(
        lat="41.0",
        long="-87.0",
        radius_miles="10",
        order_by="distance",
        include_providers=True,
        include_sources=True,
        include_evidence=True,
    )
    tables = strict_v3_tables(snapshot_id="synthetic-snapshot")
    limits = serving._v4_geo_rate_forward_limits(_local_distance_tables())
    scope = serving._RankedDistanceHydration(
        serving._GraphLocationCandidates([provider_by_field], {_NPI: {3}}, taxonomy_filtered=True),
        limits,
    )
    return args, tables, limits, scope, old_location_reader, graph_projection


@pytest.mark.asyncio
@pytest.mark.parametrize("split_group", [False, True])
async def test_ranked_geographic_batch_preserves_complete_native_rate_options(monkeypatch, split_group):
    """Preserve complete rate options and reject a split rate group."""
    args, tables, limits, scope, old_location_reader, graph_projection = _install_ranked_geo_rate_batch(
        monkeypatch, split_group
    )
    legacy, _ = await _search(args=args, serving_tables=tables, code_rows=[{**_CODE_ROW, "rate_count": 2}])
    assert legacy is not None and len(legacy["items"]) == (2 if split_group else 1)
    session = FakeSession([FakeResult([{**_CODE_ROW, "rate_count": 2}])])

    async def ranked():
        return await serving._search_manifest_serving_table(
            session,
            "synthetic-snapshot",
            args,
            SimpleNamespace(limit=1 if split_group else 10, offset=0),
            tables,
            serving.PTG2_MODE_PRODUCT_SEARCH,
            ranked_distance=scope,
        )

    if split_group:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_occurrences"):
            await ranked()
    else:
        response = await ranked()
        assert response is not None
        assert serving._shape_ptg2_response(response, args) == legacy
        assert response["items"][0]["rate_option_count"] == 2
    assert old_location_reader.await_count == 1
    assert graph_projection.await_args.args[2] is scope.candidates
    assert serving._shared_rows_for_scope.await_args.args[3].scan_budget is limits.scan_budget


@pytest.mark.asyncio
async def test_admitted_ranked_snapshot_uses_one_budget_and_keeps_source_identity(monkeypatch):
    from tests.test_plan_release_serving import _network_binding

    args, tables, limits, scope, _, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    binding = _network_binding(1, tables.snapshot_id, tables.source_key, plan_id=_CODE_ROW["plan_id"])
    anchor = (1.0, _NPI)
    ranked = serving._RankedDistanceCandidates(scope.candidates, 1, (1.25, _NPI))
    seek = AsyncMock(return_value=ranked)
    monkeypatch.setattr(serving, "_uses_local_distance_rate_scope", lambda *_: True)
    monkeypatch.setattr(
        serving, "_shared_rate_code_scope_rows", AsyncMock(return_value=[{**_CODE_ROW, "rate_count": 2}])
    )
    monkeypatch.setattr(serving, "_v4_geo_rate_forward_limits", lambda _: limits)
    monkeypatch.setattr(serving, "load_v4_graph_root", AsyncMock(return_value=SimpleNamespace(representation="paged")))
    monkeypatch.setattr(serving, "_seek_local_distance_graph", seek)
    session = FakeSession([FakeResult([{**_CODE_ROW, "rate_count": 2}])])

    batch, metadata = await serving._read_ranked_distance_snapshot(session, binding, tables, args, 2, anchor)

    assert seek.await_args.kwargs == {"after": anchor, "include_anchor": True}
    assert seek.await_args.args[3].forward_limits is limits
    assert serving._shared_rows_for_scope.await_args.args[3].scan_budget is limits.scan_budget
    assert (batch.source_key, batch.snapshot_id, batch.exhausted) == (tables.source_key, tables.snapshot_id, False)
    assert len(batch.items) == 1
    assert batch.items[0]["source_key"] == tables.source_key
    assert batch.items[0]["snapshot_id"] == tables.snapshot_id
    assert batch.items[0]["rate_option_count"] == 2
    assert metadata["provenance"]["snapshot_id"] == tables.snapshot_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changed", "offset"),
    [
        ({"lat": None, "long": None, "radius_miles": None}, 0),
        ({"include_providers": False}, 0),
        ({"npi": str(_NPI)}, 0),
        ({"negotiated_rate": "125.00"}, 0),
        ({"order": "desc"}, 0),
        ({}, 1),
    ],
)
async def test_ranked_hydration_rejects_incomplete_geographic_requests_before_reads(monkeypatch, changed, offset):
    args, tables, _, scope, _, graph_projection = _install_ranked_geo_rate_batch(monkeypatch, False)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    with pytest.raises(PTG2ManifestArtifactError, match="requires a complete geographic batch"):
        await serving._search_manifest_serving_table(
            session,
            tables.snapshot_id,
            args | changed,
            SimpleNamespace(limit=10, offset=offset),
            tables,
            serving.PTG2_MODE_PRODUCT_SEARCH,
            ranked_distance=scope,
        )
    graph_projection.assert_not_awaited()
    serving._shared_rows_for_scope.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_ranked_empty_geographic_batch_is_exact_and_needs_no_rate_reads(monkeypatch):
    args, tables, limits, _, _, graph_projection = _install_ranked_geo_rate_batch(monkeypatch, False)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    scope = serving._RankedDistanceHydration(serving._GraphLocationCandidates([], {}, taxonomy_filtered=True), limits)

    result = await serving._search_manifest_serving_table(
        session,
        tables.snapshot_id,
        args,
        SimpleNamespace(limit=10, offset=0),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
        ranked_distance=scope,
    )

    assert result["items"] == []
    assert result["pagination"]["total"] == 0 and result["pagination"]["total_is_exact"]
    graph_projection.assert_not_awaited()
    serving._shared_rows_for_scope.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("unavailable", ["layout", "procedure", "code-rows"])
async def test_ranked_snapshot_admission_stops_before_graph_reads(monkeypatch, unavailable):
    from tests.test_plan_release_serving import _network_binding

    args, tables, _, _, _, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    binding = _network_binding(1, tables.snapshot_id, tables.source_key, plan_id=_CODE_ROW["plan_id"])
    monkeypatch.setattr(serving, "read_serving_tables", AsyncMock(return_value=tables))
    monkeypatch.setattr(serving, "_uses_local_distance_rate_scope", lambda *_: unavailable != "layout")
    code_rows = AsyncMock(return_value=[])
    graph = AsyncMock(side_effect=AssertionError("unadmitted code must not reach graph reads"))
    monkeypatch.setattr(serving, "_shared_rate_code_scope_rows", code_rows)
    monkeypatch.setattr(serving, "_hydrate_ranked_snapshot_rates", graph)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    if unavailable == "procedure":
        args.pop("code")
    request = serving._read_ranked_distance_snapshot(session, binding, tables, args, 2, None)
    if unavailable == "code-rows":
        batch, metadata = await request
        assert (batch.source_key, batch.snapshot_id, batch.items, batch.exhausted) == (
            binding.source_key,
            binding.snapshot_id,
            [],
            True,
        )
        assert metadata == {}
        code_rows.assert_awaited_once()
    else:
        error = (
            serving.PlanPricingProjectionUnavailable
            if unavailable == "layout"
            else serving.PlanPricingProjectionUnsupported
        )
        with pytest.raises(error):
            await request
        code_rows.assert_not_awaited()
    graph.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["unavailable-addresses", "empty-addresses", "unavailable-rates", "changed-npi", "changed-distance"]
)
async def test_ranked_snapshot_hydration_requires_the_selected_provider_ranks(monkeypatch, failure):
    from tests.test_plan_release_serving import _network_binding

    args, tables, limits, scope, _, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    binding = _network_binding(1, tables.snapshot_id, tables.source_key, plan_id=_CODE_ROW["plan_id"])
    ranked = serving._RankedDistanceCandidates(scope.candidates, 1, None)
    if failure == "unavailable-addresses":
        ranked = None
    if failure == "empty-addresses":
        ranked = serving._RankedDistanceCandidates(serving._GraphLocationCandidates([], {}), 0, None)
    monkeypatch.setattr(serving, "_seek_local_distance_graph", AsyncMock(return_value=ranked))
    monkeypatch.setattr(serving, "load_v4_graph_root", AsyncMock(return_value=SimpleNamespace(representation="paged")))
    provider_by_field = {"npi": _NPI, "distance_miles": 1.25}
    if failure == "changed-npi":
        provider_by_field["npi"] += 1
    if failure == "changed-distance":
        provider_by_field["distance_miles"] += 1
    hydrate = AsyncMock(return_value=None if failure == "unavailable-rates" else {"items": [provider_by_field]})
    monkeypatch.setattr(serving, "_search_manifest_serving_table", hydrate)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unmocked database read")))
    graph_request = serving._LocalDistanceGraphRequest(2, [_CODE_ROW], limits)
    request = serving._hydrate_ranked_snapshot_rates(session, binding, tables, args, graph_request, None)
    if failure == "empty-addresses":
        batch, metadata = await request
        assert batch.items == [] and batch.exhausted and metadata == {}
    else:
        error = (
            serving.PlanPricingProjectionUnavailable
            if failure == "unavailable-addresses"
            else PTG2ManifestArtifactError
        )
        with pytest.raises(error, match="unavailable|changed the selected provider ranks"):
            await request
    if failure in {"unavailable-addresses", "empty-addresses"}:
        hydrate.assert_not_awaited()
    else:
        assert hydrate.await_args.args[0] is session
        assert hydrate.await_args.kwargs["ranked_distance"].candidates is scope.candidates
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("scoped_count", [2, 9])
async def test_ranked_hydration_uses_real_complete_rate_reservation(monkeypatch, scoped_count):
    from dataclasses import replace

    args, tables, limits, scope, _, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    budget = serving.ForwardReadBudget(10, 100_000, maximum_row_capacity=11)
    budget.reserve_row_capacity(read_rows=2, result_rows=2)
    limits = replace(limits, maximum_code_occurrences=10, scan_budget=budget)
    scope = serving._RankedDistanceHydration(scope.candidates, limits)
    read_rows = AsyncMock(
        return_value=[
            {**_RATE_ROW, "price_key": 9 + ordinal, "serving_content_hash_128": f"{31 + ordinal:032x}"}
            for ordinal in range(scoped_count)
        ]
    )
    monkeypatch.setattr(serving, "_shared_rows_for_scope", read_rows)
    session = FakeSession([FakeResult([{**_CODE_ROW, "rate_count": 100_000}])])
    request = serving._search_manifest_serving_table(
        session,
        tables.snapshot_id,
        args,
        SimpleNamespace(limit=10, offset=0),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
        ranked_distance=scope,
    )
    if scoped_count == 9:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_occurrences"):
            await request
        serving._prices_for_price_sets.assert_not_awaited()
    else:
        response = await request
        assert response["items"][0]["rate_option_count"] == 2
    assert read_rows.await_args.kwargs["limit"] == 9
    assert budget.peak_read_row_capacity == budget.peak_result_row_capacity == 11
    assert budget.active_read_row_capacity == budget.active_result_row_capacity == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(("rate_count", "reserved_rows"), [(2, 0), (4, 0), (2, 1)])
async def test_ranked_provider_rate_fanout_stays_within_shared_allowance(monkeypatch, rate_count, reserved_rows):
    from dataclasses import replace

    args, tables, limits, scope, _, graph_projection = _install_ranked_geo_rate_batch(monkeypatch, False)
    providers = [{**_PROVIDER_ROW, "npi": _NPI + ordinal, "distance_miles": 1.25} for ordinal in range(3)]
    graph_projection.return_value = ({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: providers})
    budget = serving.ForwardReadBudget(10, 100_000, maximum_row_capacity=7)
    budget.reserve_row_capacity(read_rows=reserved_rows, result_rows=reserved_rows)
    limits = replace(limits, maximum_code_occurrences=6, scan_budget=budget)
    scope = serving._RankedDistanceHydration(scope.candidates, limits)
    monkeypatch.setattr(
        serving,
        "_shared_rows_for_scope",
        AsyncMock(
            return_value=[
                {**_RATE_ROW, "serving_content_hash_128": f"{31 + ordinal:032x}"} for ordinal in range(rate_count)
            ]
        ),
    )
    provider_fields = Mock(wraps=serving._request_local_provider_fields)
    monkeypatch.setattr(serving, "_request_local_provider_fields", provider_fields)
    request = serving._search_manifest_serving_table(
        FakeSession([FakeResult([{**_CODE_ROW, "rate_count": rate_count}])]),
        tables.snapshot_id,
        args,
        SimpleNamespace(limit=6, offset=0),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
        ranked_distance=scope,
    )
    if rate_count * len(providers) > 6 - reserved_rows:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_occurrences"):
            await request
        assert provider_fields.call_count <= 6 - reserved_rows
    else:
        response = await request
        assert [provider_item["npi"] for provider_item in response["items"]] == [
            provider["npi"] for provider in providers
        ]
        assert [provider_item["rate_option_count"] for provider_item in response["items"]] == [rate_count] * len(
            providers
        )
        assert provider_fields.call_count == rate_count * len(providers)
    assert budget.active_read_row_capacity == budget.active_result_row_capacity == reserved_rows
