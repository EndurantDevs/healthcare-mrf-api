# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure-ordering and value-safe timeout tests for billing-search POST."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_post_cursor_preflight as cursor_preflight
from api import billing_search_post_operation as operation
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_UNAVAILABLE
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
)
from tests.test_billing_search_post_operation import (
    KEYRING,
    TRUSTED_NOW,
    _Session,
    _access,
    _authorization_context,
    _continuation_access,
    _generation_pin,
    _patch_common,
    _selector,
)


@pytest.mark.asyncio
async def test_unavailable_projection_with_cursor_returns_503_seam(
    monkeypatch,
) -> None:
    generation_pin = _generation_pin()
    access = _continuation_access(pin=generation_pin)
    events: list[str] = []
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(
            BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            digest=None,
        ),
        events=events,
    )
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        AsyncMock(return_value=generation_pin),
    )

    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
            cursor_keyring=KEYRING,
        )


@pytest.mark.asyncio
async def test_no_validated_ready_generation_remains_503(monkeypatch) -> None:
    access = _continuation_access()
    events: list[str] = []
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )

    async def _resolve_release(*_args, **_kwargs):
        events.append("resolve_release")
        return SimpleNamespace(
            state=PLAN_RELEASE_RESOLUTION_UNAVAILABLE,
            selection=None,
        )

    monkeypatch.setattr(
        operation,
        "resolve_plan_release_serving_resolution",
        _resolve_release,
    )
    selector_resolver = AsyncMock()
    capture = AsyncMock()
    monkeypatch.setattr(operation, "resolve_billing_search_selector", selector_resolver)
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        capture,
    )

    with pytest.raises(
        operation.BillingSearchPostServingUnavailableError,
        match="billing_search_serving_unavailable",
    ):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
            cursor_keyring=KEYRING,
        )

    selector_resolver.assert_not_awaited()
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_malformed_cursor_is_rejected_before_selector_resolution(
    monkeypatch,
) -> None:
    access = _access(cursor="opaque-cursor")
    events: list[str] = []
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(BILLING_SELECTOR_MATCHED),
        events=events,
    )
    capture = AsyncMock()
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        capture,
    )

    with pytest.raises(
        operation.BillingSearchPostCursorInvalidError,
        match="billing_search_cursor_invalid",
    ):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
            cursor_keyring=KEYRING,
        )

    assert "resolve_selector" not in events
    capture.assert_not_awaited()


@pytest.mark.parametrize("scope_change", ["selector", "authority"])
@pytest.mark.asyncio
async def test_cursor_scope_change_remains_invalid_before_selector(
    monkeypatch,
    scope_change,
) -> None:
    generation_pin = _generation_pin()
    prior_access = _continuation_access(pin=generation_pin)
    changed_reference = (
        "be1_" + "b" * 64
        if scope_change == "selector"
        else prior_access.request.billing_entity_ref
    )
    changed_context = (
        _authorization_context(principal_scope_sha256="9" * 64)
        if scope_change == "authority"
        else prior_access.authorization_context
    )
    access = _access(
        cursor=prior_access.request.cursor,
        billing_entity_ref=changed_reference,
        authorization_context=changed_context,
    )
    events: list[str] = []
    _patch_common(
        monkeypatch,
        access=access,
        selector=_selector(BILLING_SELECTOR_MATCHED),
        events=events,
    )
    monkeypatch.setattr(
        cursor_preflight,
        "capture_billing_search_generation_pin",
        AsyncMock(return_value=generation_pin),
    )

    with pytest.raises(
        operation.BillingSearchPostCursorInvalidError,
        match="billing_search_cursor_invalid",
    ):
        await operation.execute_billing_search_post(
            _Session(events),
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
            cursor_keyring=KEYRING,
        )

    assert "resolve_selector" not in events


@pytest.mark.asyncio
async def test_timeout_failure_maps_without_database_error_value(monkeypatch) -> None:
    sensitive_marker = "sensitive-timeout-marker"
    access = _access()
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda candidate: candidate,
    )

    class _TimeoutSession:
        def __init__(self) -> None:
            self.statements: list[str] = []

        async def execute(self, statement):
            self.statements.append(str(statement))
            if len(self.statements) == 2:
                raise TimeoutError(sensitive_marker)
            return None

    session = _TimeoutSession()
    with pytest.raises(
        operation.BillingSearchPostServingUnavailableError,
        match="billing_search_serving_unavailable",
    ) as caught:
        await operation.execute_billing_search_post(
            session,
            access,
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=AsyncMock(),
            environment_map={},
        )

    assert session.statements == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "SET LOCAL lock_timeout = '250ms'",
    ]
    assert sensitive_marker not in str(caught.value)
