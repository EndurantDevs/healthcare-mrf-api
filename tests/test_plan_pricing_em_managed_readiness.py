# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Readiness uses the same managed authorities as the bounded reader."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_address_scope, plan_pricing_em_attribution, provider_service_source_policy
from api.plan_pricing_em_distance import is_em_distance_projection_ready
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs
from tests.test_plan_pricing_em_distance_pagination import _release_selection


@pytest.fixture
def held(monkeypatch):
    scope = SimpleNamespace(manifest=object())
    selection = _release_selection()
    driver = object()
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=AssertionError("legacy readiness must not run")),
        connection=AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        ),
    )
    calls = []

    async def admit(actual_session, actual_selection):
        assert actual_session is session and actual_selection is selection
        calls.append("admission")
        return ()

    async def attributed(actual_session, actual_selection):
        assert actual_session is session and actual_selection is selection
        calls.append("attribution")
        return object()

    async def providers(actual_driver, manifest):
        assert actual_driver is driver and manifest is scope.manifest
        calls.append("provider inputs")
        return object()

    monkeypatch.setattr(network_address_scope, "current_network_address_scope", lambda: scope)
    admission = AsyncMock(side_effect=admit)
    attribution = AsyncMock(side_effect=attributed)
    inputs = AsyncMock(side_effect=providers)
    monkeypatch.setattr(provider_service_source_policy, "admitted_plan_release_bindings", admission)
    monkeypatch.setattr(plan_pricing_em_attribution, "resolve_em_attribution", attribution)
    monkeypatch.setattr(network_em_provider_inputs, "resolve_em_provider_inputs", inputs)
    return SimpleNamespace(
        session=session, selection=selection, calls=calls, admission=admission, attribution=attribution, inputs=inputs
    )


async def test_managed_ready_checks_all_authorities_even_with_zero_contributors(held):
    assert await is_em_distance_projection_ready(held.session, held.selection) is True
    assert held.calls == ["admission", "attribution", "provider inputs"]
    held.session.execute.assert_not_awaited()


@pytest.mark.parametrize("stage", ["admission", "attribution", "inputs"])
async def test_managed_unavailable_never_uses_legacy_attachment(held, stage):
    getattr(held, stage).side_effect = PlanPricingProjectionUnavailable("unavailable")
    assert await is_em_distance_projection_ready(held.session, held.selection) is False
    held.session.execute.assert_not_awaited()
    if stage == "admission":
        held.attribution.assert_not_awaited()
        held.session.connection.assert_not_awaited()
    elif stage == "attribution":
        held.inputs.assert_not_awaited()
        held.session.connection.assert_not_awaited()


async def test_managed_readiness_does_not_turn_cancellation_into_unready(held):
    held.admission.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await is_em_distance_projection_ready(held.session, held.selection)
    held.attribution.assert_not_awaited()


@pytest.mark.parametrize("ready", [False, True])
async def test_unscoped_legacy_readiness_retains_its_exact_query(held, monkeypatch, ready):
    monkeypatch.setattr(network_address_scope, "current_network_address_scope", lambda: None)
    held.session.execute.side_effect = None
    held.session.execute.return_value = SimpleNamespace(
        mappings=lambda: SimpleNamespace(first=lambda: {"projection_ready": ready})
    )
    assert await is_em_distance_projection_ready(held.session, held.selection) is ready
    held.admission.assert_not_awaited()
    held.attribution.assert_not_awaited()
    held.inputs.assert_not_awaited()
    parameters = held.session.execute.await_args.args[1]
    assert parameters["serving_revision_id"] == held.selection.serving_revision_id
    assert parameters["binding_set_digest"] == held.selection.binding_set_digest
