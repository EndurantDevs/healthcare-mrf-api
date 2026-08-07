# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_post_operation as operation
from api.billing_search_pagination import BillingSearchGenerationPin
from api.billing_search_post_request import parse_billing_search_post_request
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
)
from tests.billing_search_post_support import PLAN_RELEASE_ID, selection

TRUSTED_NOW = "2026-08-07T10:00:10Z"


def _request(*, cursor: str | None = None):
    page_by_field = {"limit": 25, "cursor": cursor}
    return parse_billing_search_post_request(
        {
            "healthporta_plan_id": selection().healthporta_plan_id,
            "billing_identity": {"billing_entity_ref": "be1_" + "a" * 48},
            "procedure": {
                "code_system": "CPT",
                "code": "00000",
                "modifiers": [],
                "place_of_service": [],
            },
            "geo": {"zip5": "00000", "radius_miles": 0},
            "page": page_by_field,
        }
    )


def _access(*, cursor: str | None = None):
    request = _request(cursor=cursor)
    return SimpleNamespace(
        plan_release_id=PLAN_RELEASE_ID,
        request=request,
        authorization_context=SimpleNamespace(plan_entitlement_sha256="1" * 64),
    )


def _selector(state: str, *, digest: str | None = "2" * 64):
    binding = SimpleNamespace(state=state)
    return SimpleNamespace(
        selector_scope=SimpleNamespace(bindings=(binding,)),
        selector_scope_sha256=digest,
    )


class _Session:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def execute(self, statement):
        self.events.append(str(statement))
        return None


def _generation_pin() -> BillingSearchGenerationPin:
    return BillingSearchGenerationPin(
        snapshot_set_sha256="3" * 64,
        generation_bundle_sha256="4" * 64,
        address_relation_oid=1001,
        address_evidence_relation_oid=1002,
    )


def _patch_common(monkeypatch, *, access, selector, events):
    selected = selection()
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )
    monkeypatch.setattr(
        operation,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=SimpleNamespace(
                state=PLAN_RELEASE_RESOLUTION_READY,
                selection=selected,
            )
        ),
    )

    async def _pin(_session, candidate):
        events.append("pin_selection")
        return candidate

    async def _resolve_selector(*_args, **_kwargs):
        events.append("resolve_selector")
        return selector

    monkeypatch.setattr(operation, "pin_billing_search_selection", _pin)
    monkeypatch.setattr(
        operation,
        "resolve_billing_search_selector",
        _resolve_selector,
    )
    monkeypatch.setattr(
        operation,
        "_audit_record",
        lambda *_args, **_kwargs: {"event": "billing_search_access"},
    )
    monkeypatch.setattr(
        operation,
        "shape_billing_search_response",
        lambda result, *, next_cursor=None: {
            "result_state": result.state,
            "next_cursor": next_cursor,
        },
    )
    return selected


@pytest.mark.asyncio
async def test_no_match_avoids_generation_and_address_locks(monkeypatch) -> None:
    events: list[str] = []
    access = _access()
    selected = _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(BILLING_SELECTOR_NO_MATCH),
        events=events,
    )

    async def _search(*_args, **_kwargs):
        events.append("service")
        return SimpleNamespace(
            state="no_matching_tax_identity",
            has_more=False,
            next_sort_key=None,
        )

    monkeypatch.setattr(operation, "search_exact_billing_provider_page", _search)
    capture = AsyncMock()
    monkeypatch.setattr(operation, "capture_billing_search_generation_pin", capture)

    execution = await operation.execute_billing_search_post(
        _Session(events),
        access,
        trusted_now=TRUSTED_NOW,
        radius_zip_context_resolver=AsyncMock(),
        environment_map={},
    )

    assert events[0].startswith("SET TRANSACTION ISOLATION LEVEL")
    assert events[-3:] == ["pin_selection", "resolve_selector", "service"]
    assert execution.payload["result_state"] == "no_matching_tax_identity"
    assert execution.audit_record["event"] == "billing_search_access"
    capture.assert_not_awaited()
    assert selected.plan_release_id == PLAN_RELEASE_ID


@pytest.mark.asyncio
async def test_matched_page_pins_and_reauthenticates_cursor(monkeypatch) -> None:
    events: list[str] = []
    access = _access()
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(BILLING_SELECTOR_MATCHED),
        events=events,
    )
    generation_pin = _generation_pin()

    async def _capture(*_args, **_kwargs):
        events.append("capture_generation")
        return generation_pin

    next_key = (0, 1.0, 0, "snapshot", 1000000004, "address", "location")

    async def _search(*_args, **_kwargs):
        events.append("service")
        return SimpleNamespace(
            state="matched",
            has_more=True,
            next_sort_key=next_key,
        )

    monkeypatch.setattr(operation, "capture_billing_search_generation_pin", _capture)
    monkeypatch.setattr(
        operation,
        "build_billing_search_cursor_binding",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(operation, "search_exact_billing_provider_page", _search)
    monkeypatch.setattr(
        operation,
        "seal_billing_search_page_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(token="sealed-next"),
    )
    opened_tokens: list[object] = []

    def _open(token, **_kwargs):
        opened_tokens.append(token)
        return next_key

    monkeypatch.setattr(operation, "open_billing_search_page_cursor", _open)
    keyring = SimpleNamespace()
    monkeypatch.setattr(operation, "_cursor_keyring", lambda *_args: keyring)

    execution = await operation.execute_billing_search_post(
        _Session(events),
        access,
        trusted_now=TRUSTED_NOW,
        radius_zip_context_resolver=AsyncMock(),
        environment_map={},
    )

    assert events.index("capture_generation") < events.index("service")
    assert opened_tokens == ["sealed-next"]
    assert execution.payload["next_cursor"] == "sealed-next"


@pytest.mark.asyncio
async def test_unavailable_projection_with_cursor_returns_503_seam(
    monkeypatch,
) -> None:
    events: list[str] = []
    access = _access(cursor="opaque-cursor")
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(
            BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            digest=None,
        ),
        events=events,
    )

    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
        )
