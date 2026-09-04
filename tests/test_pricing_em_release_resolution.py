# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded E&M cards reuse exact release metadata without widening guards."""

import json
from unittest.mock import AsyncMock

import pytest

from tests.test_plan_release_serving import PLAN_RELEASE_ID, _Session, _binding_row
from tests.test_pricing_api import make_request, pricing_module


def _distance_args(**updates):
    return {
        "plan_release_id": PLAN_RELEASE_ID,
        "code": "99203",
        "view": "card",
        "order_by": "distance",
        "zip5": "60601",
        "zip_radius_miles": "0",
        **updates,
    }


def _install_search(monkeypatch):
    search = AsyncMock(return_value={
        "items": [],
        "pagination": {"total": 0, "limit": 25, "offset": 0, "page": 1},
        "query": {"source": "ptg2"},
    })
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", search)
    return search


@pytest.mark.asyncio
@pytest.mark.parametrize("projection_relation", [True, False])
@pytest.mark.parametrize("binding_state", ["ready", "absent", "unpinned"])
async def test_distance_card_resolves_release_metadata_once(
    monkeypatch, projection_relation, binding_state
):
    rows = [] if binding_state == "absent" else [
        _binding_row(is_pinned=binding_state == "ready")
    ]
    session = _Session(rows, pricing_projection_relation=projection_relation)
    search = _install_search(monkeypatch)
    request = make_request([], args=_distance_args())
    request.ctx.sa_session = session

    response = await pricing_module.list_providers_by_procedure(request)

    assert response.status == 200
    assert len(session.calls) == 2  # Optional relation probe and one frozen release read.
    assert "to_regclass" in session.calls[0][0]
    assert "plan_release_serving_revision" in session.calls[1][0]
    assert ("plan_pricing_projection_candidate" in session.calls[1][0]) is projection_relation
    selection = search.await_args.kwargs["release_selection"]
    if binding_state == "ready":
        assert selection.plan_release_id == PLAN_RELEASE_ID
        assert selection.in_network_bindings[0].plan_market_type == "group"
        assert selection._validated_serving_tables == ()
    else:
        assert selection is None
    assert search.await_count == 1
    assert search.await_args.args[1]["code"] == "99203"
    assert json.loads(response.body)["query"]["source"] == "ptg2"


@pytest.mark.asyncio
@pytest.mark.parametrize("updates", [
    {"view": "full"},
    {"code": "99202"},
    {"offset": "200"},
    {"start": "200"},
    {"page": "9"},
    {"include_providers": "false"},
    {"zip_radius_miles": "26"},
])
async def test_ineligible_distance_request_keeps_projection_free_guard(
    monkeypatch, updates
):
    session = _Session([_binding_row()])
    resolver = AsyncMock(return_value=None)
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", resolver)
    _install_search(monkeypatch)
    request = make_request([], args=_distance_args(**updates))
    request.ctx.sa_session = session
    monkeypatch.setattr(pricing_module, "_lookup_zip_context", AsyncMock(return_value=None))

    response = await pricing_module.list_providers_by_procedure(request)

    assert response.status == 200
    assert len(session.calls) == 1
    assert "plan_pricing_projection_candidate" not in session.calls[0][0]
    assert "to_regclass" not in session.calls[0][0]
    resolver.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["card", "full"])
async def test_cost_ordered_office_visit_refuses_before_projection_resolution(
    monkeypatch, view
):
    session = _Session([_binding_row()])
    resolver = AsyncMock()
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", resolver)
    monkeypatch.setattr(pricing_module, "is_em_distance_projection_ready", AsyncMock(return_value=True))
    search = _install_search(monkeypatch)
    request = make_request([], args=_distance_args(view=view, order_by="total_allowed_amount"))
    request.ctx.sa_session = session

    response = await pricing_module.list_providers_by_procedure(request)

    assert response.status == 422
    assert json.loads(response.body)["code"] == "ptg2_provider_scope_refused"
    assert len(session.calls) == 1
    assert "plan_pricing_projection_candidate" not in session.calls[0][0]
    resolver.assert_not_awaited()
    search.assert_not_awaited()
