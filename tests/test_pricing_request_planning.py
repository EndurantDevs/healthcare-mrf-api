# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Interactive planning is limited to the HTTP-owned Reader transaction."""

from unittest.mock import AsyncMock

import pytest

from api import network_address_scope
from tests.test_plan_release_serving import PLAN_RELEASE_ID
from tests.test_pricing_api import make_request
from tests.test_pricing_api import pricing_module as pricing
from tests.test_pricing_canonical_network_claims import MANIFEST


def _request(**updates):
    request = make_request(
        [],
        args={
            "plan_release_id": PLAN_RELEASE_ID,
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "include_allowed_amounts": "false",
            **updates,
        },
    )
    request.ctx._sa_reader_scope = object()
    return request


def _install_search(monkeypatch):
    resolver = AsyncMock(return_value=None)
    search = AsyncMock(return_value={"items": [], "query": {"source": "ptg2"}})
    monkeypatch.setattr(pricing, "resolve_plan_release_serving", resolver)
    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    return resolver, search


@pytest.mark.parametrize("geo", [False, True])
async def test_owned_interactive_reader_disables_jit_before_resolution(monkeypatch, geo):
    request = _request(
        **(
            {
                "npi": None,
                "lat": "42",
                "long": "-83",
                "radius_miles": "10",
                "order_by": "distance",
                "classification": "Family Medicine",
            }
            if geo
            else {}
        )
    )
    monkeypatch.setattr(pricing, "is_owned_reader", lambda session: session is request.ctx.sa_session)
    tuner = AsyncMock()
    monkeypatch.setattr(pricing, "execute_network_read", tuner)
    resolver, search = _install_search(monkeypatch)
    original_specialty = pricing._resolve_ptg_specialty_or_raise

    async def specialty(session, args):
        tuner.assert_awaited_once()
        assert tuner.await_args.args[0] is session
        assert str(tuner.await_args.args[1]) == (
            "SELECT set_config('jit', 'off', true), set_config('plan_cache_mode', 'force_custom_plan', true)"
        )
        return await original_specialty(session, args)

    monkeypatch.setattr(pricing, "_resolve_ptg_specialty_or_raise", specialty)
    guard = AsyncMock(side_effect=AssertionError("bounded full search does not need a guard-only release read"))
    monkeypatch.setattr(pricing, "resolve_plan_release_guard_selection", guard)
    response = await pricing.list_providers_by_procedure(request)
    assert response.status == 200
    resolver.assert_awaited_once()
    search.assert_awaited_once()
    guard.assert_not_awaited()


@pytest.mark.parametrize("excluded", ["borrowed", "unverified", "card", "no_providers"])
async def test_other_callers_receive_no_early_setting(monkeypatch, excluded):
    request = _request(
        **(
            {"view": "card"}
            if excluded == "card"
            else {"include_providers": "false"}
            if excluded == "no_providers"
            else {}
        )
    )
    if excluded == "borrowed":
        request.ctx._sa_reader_scope = None
    monkeypatch.setattr(pricing, "is_owned_reader", lambda _session: excluded != "unverified")
    tuner = AsyncMock(side_effect=AssertionError("caller settings must be preserved"))
    monkeypatch.setattr(pricing, "execute_network_read", tuner)
    _install_search(monkeypatch)
    assert (await pricing.list_providers_by_procedure(request)).status == 200
    tuner.assert_not_awaited()


async def test_valid_managed_native_request_retains_caller_settings(monkeypatch):
    request = _request()
    native_args, request.args = request.args, {}
    monkeypatch.setattr(pricing, "is_owned_reader", lambda _session: True)
    tuner = AsyncMock(side_effect=AssertionError("native caller settings must be preserved"))
    monkeypatch.setattr(pricing, "execute_network_read", tuner)
    resolver, search = _install_search(monkeypatch)
    with network_address_scope.network_address_read_scope(
        MANIFEST, None, access_scope_sha256="b" * 64, excluded_network_ids=()
    ):
        assert (await pricing.list_providers_by_procedure(request, native_args=native_args)).status == 200
    resolver.assert_awaited_once()
    search.assert_awaited_once()
    tuner.assert_not_awaited()


async def test_state_scan_receives_no_early_setting(monkeypatch):
    request = _request()
    monkeypatch.setattr(pricing, "is_plan_pricing_state_scan", lambda _args: True)
    monkeypatch.setattr(pricing, "is_owned_reader", lambda _session: True)
    tuner = AsyncMock(side_effect=AssertionError("bulk planning is unchanged"))
    monkeypatch.setattr(pricing, "execute_network_read", tuner)
    resolver, _ = _install_search(monkeypatch)
    assert (await pricing.list_providers_by_procedure(request)).status == 503
    resolver.assert_awaited_once()
    tuner.assert_not_awaited()
