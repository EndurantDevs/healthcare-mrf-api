# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

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
        [FakeResult([dict(_CODE_ROW) for _ in range(1)])]
        if code_rows is None
        else [FakeResult(code_rows)]
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
    response, _session = await _search(
        args=_query_args(state="IL", negotiated_rate="125.00")
    )
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

    response, _session = await _search(
        args=_query_args(npi=str(_NPI), include_providers=True)
    )
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
    response, _session = await _search(
        args=_query_args(include_providers=True, order_by="provider_name")
    )
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
    response, _session = await _search(
        args=_query_args(include_providers=True, order_by="provider_name")
    )

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
    rank_key = selected_key if materializes_selected_key else (
        "npi",
        "9999999999",
        "CPT",
        "99213",
        "FFS",
        "0",
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
    location_providers = [
        {**_PROVIDER_ROW, "npi": _NPI + offset}
        for offset in range(2)
    ]
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
