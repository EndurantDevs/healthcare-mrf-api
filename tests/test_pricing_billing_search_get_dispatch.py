# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Presence-first dispatch guards for the shared procedure-search handler."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.endpoint import pricing


@pytest.mark.asyncio
@pytest.mark.parametrize("selector_value", (None, "", "be1_invalid"))
async def test_selector_presence_dispatches_before_every_legacy_parser(
    monkeypatch,
    selector_value,
) -> None:
    session = object()
    expected_response = object()
    request = SimpleNamespace(
        args={"billing_entity_ref": selector_value},
        ctx=SimpleNamespace(sa_session=session),
    )

    async def serve(call_request, call_session):
        assert call_request is request
        assert call_session is session
        return expected_response

    def legacy_reached(*_args, **_kwargs):
        raise AssertionError("billing selector reached legacy request parsing")

    monkeypatch.setattr(pricing, "serve_billing_search_get", serve)
    monkeypatch.setattr(
        pricing,
        "_reject_resolver_only_procedure_search_params",
        legacy_reached,
    )
    monkeypatch.setattr(pricing, "begin_capacity_evidence", legacy_reached)

    assert await pricing.list_providers_by_procedure(request) is expected_response


@pytest.mark.asyncio
async def test_selector_absence_preserves_the_legacy_entry_path(monkeypatch) -> None:
    class _LegacyReached(RuntimeError):
        pass

    request = SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=object()))

    async def billing_reached(*_args, **_kwargs):
        raise AssertionError("selector-free request entered billing search")

    def legacy_entry(_args):
        raise _LegacyReached

    monkeypatch.setattr(pricing, "serve_billing_search_get", billing_reached)
    monkeypatch.setattr(
        pricing,
        "_reject_resolver_only_procedure_search_params",
        legacy_entry,
    )

    with pytest.raises(_LegacyReached):
        await pricing.list_providers_by_procedure(request)
