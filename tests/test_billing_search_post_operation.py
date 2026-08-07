# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_post_operation as operation
from api import billing_search_post_cursor_preflight as cursor_preflight
from api.billing_search_access_contract import (
    build_billing_search_authorization_context,
)
from api.billing_search_cursor import BillingSearchCursorKeyring
from api.billing_search_cursor_scope import (
    billing_search_stable_request_fingerprint,
    select_billing_search_cursor_chain_keyring,
)
from api.billing_search_pagination import (
    BillingSearchGenerationPin,
    build_billing_search_cursor_binding,
    seal_billing_search_page_cursor,
)
from api.billing_search_post_request import parse_billing_search_post_request
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
)
from tests.billing_search_post_support import PLAN_RELEASE_ID, selection

TRUSTED_NOW = "2026-08-07T10:00:10Z"
KEYRING = BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)
SORT_KEY = (
    0,
    1.0,
    0,
    "ptg2:synthetic-billing-search",
    1000000004,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)


def _request(
    *,
    cursor: str | None = None,
    radius_miles: float = 0.0,
    billing_entity_ref: str = "be1_" + "a" * 64,
):
    page_by_field = {"limit": 25, "cursor": cursor}
    return parse_billing_search_post_request(
        {
            "healthporta_plan_id": selection().healthporta_plan_id,
            "billing_identity": {"billing_entity_ref": billing_entity_ref},
            "procedure": {
                "code_system": "CPT",
                "code": "00000",
                "modifiers": [],
                "place_of_service": [],
            },
            "geo": {"zip5": "00000", "radius_miles": radius_miles},
            "page": page_by_field,
        }
    )


def _authorization_context(**overrides):
    claims_by_name = {
        "principal_scope_sha256": "1" * 64,
        "tenant_scope_sha256": "2" * 64,
        "plan_entitlement_sha256": "3" * 64,
        "audit_scope_sha256": "4" * 64,
        "quota_scope_sha256": "5" * 64,
        "capabilities": ("pricing:billing-search",),
        "issued_at": "2026-08-07T10:00:00Z",
        "expires_at": "2026-08-07T10:01:00Z",
    }
    claims_by_name.update(overrides)
    return build_billing_search_authorization_context(
        claims_by_name,
        trusted_now=TRUSTED_NOW,
    )


def _access(
    *,
    cursor: str | None = None,
    radius_miles: float = 0.0,
    billing_entity_ref: str = "be1_" + "a" * 64,
    authorization_context=None,
):
    request = _request(
        cursor=cursor,
        radius_miles=radius_miles,
        billing_entity_ref=billing_entity_ref,
    )
    return SimpleNamespace(
        plan_release_id=PLAN_RELEASE_ID,
        request=request,
        authorization_context=(
            authorization_context or SimpleNamespace(plan_entitlement_sha256="1" * 64)
        ),
    )


def _continuation_access(
    *,
    pin: BillingSearchGenerationPin | None = None,
    radius_miles: float = 0.0,
    billing_entity_ref: str = "be1_" + "a" * 64,
    authorization_context=None,
):
    context = authorization_context or _authorization_context()
    first_request = _request(
        radius_miles=radius_miles,
        billing_entity_ref=billing_entity_ref,
    )
    chain_keyring = select_billing_search_cursor_chain_keyring(
        None,
        keyring=KEYRING,
    )
    fingerprint = billing_search_stable_request_fingerprint(
        first_request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=chain_keyring,
    )
    binding = build_billing_search_cursor_binding(
        fingerprint,
        context,
        pin or _generation_pin(),
        trusted_now=TRUSTED_NOW,
    )
    sealed = seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=chain_keyring,
        binding=binding,
    )
    return _access(
        cursor=sealed.token,
        radius_miles=radius_miles,
        billing_entity_ref=billing_entity_ref,
        authorization_context=context,
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


def _generation_pin(
    *,
    snapshot_digest: str = "3" * 64,
    generation_digest: str = "4" * 64,
) -> BillingSearchGenerationPin:
    return BillingSearchGenerationPin(
        snapshot_set_sha256=snapshot_digest,
        generation_bundle_sha256=generation_digest,
        address_relation_oid=1001,
        address_evidence_relation_oid=1002,
    )


def _patch_common(monkeypatch, *, access, selector, events, selected=None):
    selected = selected or selection()
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )

    async def _resolve_release(*_args, **_kwargs):
        events.append("resolve_release")
        return SimpleNamespace(
            state=PLAN_RELEASE_RESOLUTION_READY,
            selection=selected,
        )

    monkeypatch.setattr(
        operation,
        "resolve_plan_release_serving_resolution",
        _resolve_release,
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
        "billing_search_post_success_journal",
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
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        capture,
    )

    execution = await operation.execute_billing_search_post(
        _Session(events),
        access,
        trusted_now=TRUSTED_NOW,
        radius_zip_context_resolver=AsyncMock(),
        environment_map={},
    )

    assert events[:4] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "SET LOCAL lock_timeout = '250ms'",
        "SET LOCAL statement_timeout = '2000ms'",
        "resolve_release",
    ]
    assert events[-3:] == ["pin_selection", "resolve_selector", "service"]
    assert execution.payload["result_state"] == "no_matching_tax_identity"
    assert execution.audit_record["event"] == "billing_search_access"
    capture.assert_not_awaited()
    assert selected.plan_release_id == PLAN_RELEASE_ID


def _install_matched_cursor_mocks(
    monkeypatch,
    events: list[str],
    generation_pin,
    next_key: tuple[int | float | str, ...],
) -> list[object]:
    async def _capture(*_args, **_kwargs):
        events.append("capture_generation")
        return generation_pin

    async def _search(*_args, **_kwargs):
        events.append("service")
        return SimpleNamespace(
            state="matched",
            has_more=True,
            next_sort_key=next_key,
        )

    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        _capture,
    )
    monkeypatch.setattr(
        cursor_preflight,
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
    return opened_tokens


@pytest.mark.asyncio
async def test_matched_page_pins_and_reauthenticates_cursor(monkeypatch) -> None:
    """Matched first pages pin before reading and reauthenticate next cursors."""

    events: list[str] = []
    access = _access()
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(BILLING_SELECTOR_MATCHED),
        events=events,
    )
    next_key = (0, 1.0, 0, "snapshot", 1000000004, "address", "location")
    opened_tokens = _install_matched_cursor_mocks(
        monkeypatch,
        events,
        _generation_pin(),
        next_key,
    )
    execution = await operation.execute_billing_search_post(
        _Session(events),
        access,
        trusted_now=TRUSTED_NOW,
        radius_zip_context_resolver=AsyncMock(),
        environment_map={},
        cursor_keyring=KEYRING,
    )

    assert events.index("capture_generation") < events.index("service")
    assert opened_tokens == ["sealed-next"]
    assert execution.payload["next_cursor"] == "sealed-next"


@pytest.mark.parametrize(
    ("selector_state", "result_state"),
    [
        (
            BILLING_SELECTOR_NO_MATCH,
            BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
        ),
        (
            BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
        ),
    ],
)
@pytest.mark.asyncio
async def test_terminal_selector_skips_missing_radius_centroid(
    monkeypatch,
    selector_state,
    result_state,
) -> None:
    events: list[str] = []
    access = _access(radius_miles=25.0)
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(
            selector_state,
            digest=(
                None
                if selector_state == BILLING_SELECTOR_PROJECTION_UNAVAILABLE
                else "2" * 64
            ),
        ),
        events=events,
    )
    radius_resolver = AsyncMock(return_value=None)

    async def _search(*_args, **kwargs):
        events.append("service")
        query = kwargs["query"]
        assert query.zip5 == "00000"
        assert query.radius_miles is None
        return SimpleNamespace(
            state=result_state,
            has_more=False,
            next_sort_key=None,
        )

    monkeypatch.setattr(operation, "search_exact_billing_provider_page", _search)
    capture = AsyncMock()
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        capture,
    )

    execution = await operation.execute_billing_search_post(
        _Session(events),
        access,
        trusted_now=TRUSTED_NOW,
        radius_zip_context_resolver=radius_resolver,
        environment_map={},
    )

    assert execution.payload["result_state"] == result_state
    radius_resolver.assert_not_awaited()
    capture.assert_not_awaited()


@pytest.mark.parametrize(
    "successor_selector_state",
    [BILLING_SELECTOR_NO_MATCH, BILLING_SELECTOR_PROJECTION_UNAVAILABLE],
)
@pytest.mark.asyncio
async def test_successor_generation_expires_cursor_before_selector_transition(
    monkeypatch,
    successor_selector_state,
) -> None:
    events: list[str] = []
    prior_pin = _generation_pin()
    successor_pin = _generation_pin(
        snapshot_digest="6" * 64,
        generation_digest="7" * 64,
    )
    successor_selection = replace(
        selection(),
        serving_revision_id="hpserve_" + "3" * 26,
        binding_set_digest="8" * 64,
    )
    access = _continuation_access(pin=prior_pin)
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(successor_selector_state, digest=None),
        events=events,
        selected=successor_selection,
    )

    async def _capture(_session, selected):
        events.append("capture_generation")
        assert selected is successor_selection
        return successor_pin

    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        _capture,
    )
    radius_resolver = AsyncMock()

    with pytest.raises(
        operation.BillingSearchCursorGenerationExpiredError,
        match="billing_search_cursor_generation_expired",
    ):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=radius_resolver,
            environment_map={},
            cursor_keyring=KEYRING,
        )

    assert "capture_generation" in events
    assert "resolve_selector" not in events
    radius_resolver.assert_not_awaited()
