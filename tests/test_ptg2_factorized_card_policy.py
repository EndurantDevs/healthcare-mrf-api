# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission precedes bounded factorized card reads and metadata."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import ARRAY

from api import ptg2_serving as serving
from api.plan_pricing_projection_read import _factorized_card_binding_parameter
from tests.ptg2_factorized_card_support import (
    MappingResult,
    candidate_row,
    card_pagination,
    completion_row,
    factorized_selection,
)
from tests.test_ptg2_factorized_card_serving import _card_args


def _managed(monkeypatch, positions):
    selection = factorized_selection(binding_count=3)
    bindings = tuple(selection.bindings[index] for index in positions)
    admission = AsyncMock(return_value=bindings)
    monkeypatch.setattr(serving, "admitted_plan_release_bindings", admission)
    monkeypatch.setattr(serving, "current_network_address_scope", lambda: object())
    return SimpleNamespace(selection=selection, admitted=bindings, admission=admission)


@pytest.mark.parametrize(
    ("builder", "window", "predicate", "probe"),
    [
        (serving._factorized_card_candidate_query, "profile_window", "binding_ordinal", "provider_set_probe_limit"),
        (
            serving._factorized_card_completion_query,
            "selected_memberships",
            "member.binding_ordinal",
            "membership_probe_limit",
        ),
    ],
)
def test_allowed_ordinals_are_typed_and_filtered_before_first_window_limit(builder, window, predicate, probe):
    query = builder("", binding_ordinals=(4, 1))
    sql = str(query)
    start = sql.index(f"{window} AS MATERIALIZED")
    restriction = sql.index(f"AND {predicate} = ANY(CAST(:allowed_binding_ordinals AS integer[]))", start)
    assert start < restriction < sql.index("ORDER BY", start) < sql.index(f"LIMIT :{probe}", start)
    parameter = query.compile().binds["allowed_binding_ordinals"]
    assert parameter.value == [4, 1]
    assert isinstance(parameter.type, ARRAY) and isinstance(parameter.type.item_type, Integer)
    assert "allowed_binding_ordinals" not in str(builder(""))
    assert "provider_set_probe_limit" in sql and "membership_limit" in sql


@pytest.mark.parametrize("value", [[1], (True,), (-1,), (2**31,), (1, 1), tuple(range(5001))])
def test_ordinal_binding_rejects_malformed_or_unbounded_values(value):
    with pytest.raises(serving.PlanPricingProjectionUnavailable):
        _factorized_card_binding_parameter(value)


@pytest.mark.asyncio
@pytest.mark.parametrize("positions", [(), (1,), (0, 2), (0, 1, 2)])
@pytest.mark.parametrize("offset", [0, 5])
async def test_one_admission_precedes_both_actual_queries_and_metadata_ignores_page(monkeypatch, positions, offset):
    prepared = _managed(monkeypatch, positions)
    candidate_rows = [candidate_row(profile_exhausted=True, total_lower_bound=1)]
    session = AsyncMock()
    reads = []

    async def execute(query, parameters):
        prepared.admission.assert_awaited_once_with(session, prepared.selection)
        ordinal_values = query.compile().params["allowed_binding_ordinals"]
        assert ordinal_values == list(positions)
        assert parameters["projection_id"] == prepared.selection.pricing_projection_id
        reads.append(str(query))
        return MappingResult(candidate_rows if len(reads) == 1 else [completion_row()])

    session.execute.side_effect = execute
    monkeypatch.setattr(serving, "_plan_pricing_projection_geo_cells", AsyncMock(return_value=["60601"]))
    response = await serving._search_factorized_plan_release_cards(
        session,
        {**_card_args(), "include_sources": "true"},
        card_pagination(limit=1, offset=offset),
        prepared.selection,
    )
    prepared.admission.assert_awaited_once_with(session, prepared.selection)
    assert len(reads) == (2 if positions else 0)
    expected_snapshot_ids = [binding.snapshot_id for binding in prepared.admitted]
    assert response["in_network_snapshot_ids"] == sorted(expected_snapshot_ids)
    assert response["query"]["resolved_snapshot_ids"] == sorted(expected_snapshot_ids)
    assert [
        snapshot_by_field["snapshot_id"] for snapshot_by_field in response["query"]["snapshots"]
    ] == expected_snapshot_ids
    assert response["binding_set_digest"] == prepared.selection.binding_set_digest
    assert len(prepared.selection.bindings) == 3
    assert response["pagination"]["total"] == bool(positions)
    assert response["pagination"]["total_is_exact"] is True
    assert bool(response["items"]) == bool(positions and offset == 0)
    if response["items"]:
        assert response["items"][0]["minimum_negotiated_rate"] == 10
        assert response["items"][0]["maximum_negotiated_rate"] == 30
        assert response["items"][0]["rate_count"] == 4
        assert len(response["items"][0]) == 13
    for binding in prepared.selection.bindings:
        if binding not in prepared.admitted:
            assert binding.source_key not in json.dumps(response)
            assert binding.plan_id not in json.dumps(response)
            assert binding.snapshot_id not in json.dumps(response)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [serving.PlanPricingProjectionUnavailable("unavailable"), asyncio.CancelledError()])
async def test_admission_failure_or_cancellation_never_reaches_geo_or_rate_reads(monkeypatch, failure):
    prepared = _managed(monkeypatch, (1,))
    prepared.admission.side_effect = failure
    session = AsyncMock()
    geo = AsyncMock()
    monkeypatch.setattr(serving, "_plan_pricing_projection_geo_cells", geo)
    with pytest.raises(type(failure)) as caught:
        await serving._search_factorized_plan_release_cards(
            session, _card_args(), card_pagination(), prepared.selection
        )
    assert caught.value is failure
    geo.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"order": "desc"}, {"mode": "exact_source"}, {"code": ""}])
async def test_empty_admission_still_rejects_invalid_requests(monkeypatch, changes):
    prepared = _managed(monkeypatch, ())
    session = AsyncMock()
    with pytest.raises(serving.PlanPricingProjectionUnsupported):
        await serving._search_factorized_plan_release_cards(
            session, {**_card_args(), **changes}, card_pagination(), prepared.selection
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_admission_preserves_original_window_and_projection_bounds(monkeypatch):
    prepared = _managed(monkeypatch, ())
    session = AsyncMock()
    with pytest.raises(serving.PlanPricingProjectionUnsupported):
        await serving._search_factorized_plan_release_cards(
            session, _card_args(), card_pagination(limit=201), prepared.selection
        )
    with pytest.raises(serving.PlanPricingProjectionUnavailable):
        await serving._search_factorized_plan_release_cards(
            session, _card_args(), card_pagination(), replace(prepared.selection, pricing_projection_id="invalid")
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["candidate", "completion"])
async def test_managed_reads_keep_native_budget_refusal(monkeypatch, stage):
    prepared = _managed(monkeypatch, (1,))
    session = AsyncMock()
    candidate = candidate_row(profile_exhausted=True, total_lower_bound=1)
    completion = completion_row()
    if stage == "candidate":
        candidate["provider_set_count"] = serving._FACTORIZED_CARD_PROVIDER_SET_LIMIT + 1
    else:
        completion["rate_value_count"] = serving._FACTORIZED_CARD_RATE_VALUE_LIMIT + 1
    session.execute.side_effect = [MappingResult([candidate]), MappingResult([completion])]
    monkeypatch.setattr(serving, "_plan_pricing_projection_geo_cells", AsyncMock(return_value=["60601"]))
    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded):
        await serving._search_factorized_plan_release_cards(
            session, _card_args(), card_pagination(), prepared.selection
        )
    prepared.admission.assert_awaited_once_with(session, prepared.selection)
    assert session.execute.await_count == (1 if stage == "candidate" else 2)


@pytest.mark.asyncio
async def test_unscoped_reads_keep_original_positional_call_shapes(monkeypatch):
    monkeypatch.setattr(serving, "current_network_address_scope", lambda: None)
    selection = factorized_selection(binding_count=3)
    candidate = AsyncMock(return_value=serving._FactorizedCardCandidateSelection({}, 0, True))
    completion = AsyncMock(return_value=[])
    monkeypatch.setattr(serving, "_factorized_card_candidates", candidate)
    monkeypatch.setattr(serving, "_factorized_card_complete_selected_npis", completion)
    monkeypatch.setattr(serving, "_plan_pricing_projection_geo_cells", AsyncMock(return_value=["60601"]))
    response = await serving._search_factorized_plan_release_cards(object(), _card_args(), card_pagination(), selection)
    assert candidate.await_args.kwargs == completion.await_args.kwargs == {}
    assert len(candidate.await_args.args) == 6 and len(completion.await_args.args) == 7
    assert response["in_network_snapshot_ids"] == selection.response_metadata()["in_network_snapshot_ids"]
