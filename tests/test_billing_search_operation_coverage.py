# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure-order and selector coverage for the billing-search operation."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from api import billing_search_post_operation as operation
from api import billing_search_selector_resolution as selector_resolution
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
)
from api.billing_search_cursor_keys import BillingSearchCursorKeyringError
from api.billing_search_post_request import BillingSearchPostRequestError
from api.ptg2_billing_entity_refs import PTG2BillingAssociationProjectionUnavailable
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
    serving_unavailable,
)
from tests.billing_search_post_support import (
    PLAN_RELEASE_ID,
    selector_scope,
    selection,
)
from tests.test_billing_search_post_operation import (
    KEYRING,
    TRUSTED_NOW,
    _Session,
    _access,
)
from tests.test_billing_search_selector_resolution import (
    _ein_access,
    _opaque_access,
    _valid_publication,
)


@pytest.mark.asyncio
async def test_radius_context_requires_a_callable_mapping_resolver() -> None:
    access = _access(radius_miles=25.0)

    with pytest.raises(BillingSearchServingUnavailableError):
        await operation._radius_context(object(), access, None)

    async def missing_context(_session: object, _zip5: str) -> None:
        return None

    with pytest.raises(BillingSearchServingUnavailableError):
        await operation._radius_context(object(), access, missing_context)

    expected_context_by_name = {
        "zip5": "00000",
        "latitude": 0.0,
        "longitude": 0.0,
    }

    async def valid_context(_session: object, _zip5: str):
        return expected_context_by_name

    assert (
        await operation._radius_context(object(), access, valid_context)
        is expected_context_by_name
    )


def test_selector_scope_helpers_reject_unpageable_cursor_states() -> None:
    assert operation._selector_states(SimpleNamespace(bindings=[])) == frozenset()
    cursor_access = _access(cursor="synthetic-cursor")

    for state, expected in (
        (
            BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            operation.BillingSearchPostServingUnavailableError,
        ),
        (BILLING_SELECTOR_NO_MATCH, operation.BillingSearchPostCursorInvalidError),
    ):
        resolution = SimpleNamespace(
            selector_scope=SimpleNamespace(bindings=(SimpleNamespace(state=state),))
        )
        with pytest.raises(expected):
            operation._is_pageable_selector_scope(cursor_access, resolution)


def test_cursor_keyring_rejects_wrong_injection_and_loads_configured_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(BillingSearchCursorKeyringError):
        operation._cursor_keyring(object(), {})

    monkeypatch.setattr(
        operation,
        "load_billing_search_cursor_keyring",
        lambda environment: (environment, "loaded"),
    )
    assert operation._cursor_keyring(None, {"synthetic": "value"}) == (
        {"synthetic": "value"},
        "loaded",
    )


@pytest.mark.asyncio
async def test_release_resolution_maps_missing_plan_to_generic_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_op(_session: object) -> None:
        return None

    async def not_found(_session: object, _plan_release_id: str):
        return SimpleNamespace(
            state=operation.PLAN_RELEASE_RESOLUTION_NOT_FOUND,
            selection=None,
        )

    monkeypatch.setattr(operation, "configure_billing_search_read_snapshot", no_op)
    monkeypatch.setattr(
        operation,
        "resolve_plan_release_serving_resolution",
        not_found,
    )

    with pytest.raises(operation.BillingSearchResourceNotFoundError):
        await operation._resolve_ready_selection(
            object(),
            _access(),
            stage_timings=[],
        )


def test_next_cursor_requires_complete_state_and_exact_reauthentication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_result = SimpleNamespace(has_more=True, next_sort_key=(1,))
    incomplete = SimpleNamespace(
        cursor_binding=None,
        chain_keyring=KEYRING,
    )
    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        operation._sealed_next_cursor(service_result, incomplete)

    complete = SimpleNamespace(cursor_binding=object(), chain_keyring=KEYRING)
    monkeypatch.setattr(
        operation,
        "seal_billing_search_page_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(**{"token": "synthetic-token"}),
    )
    monkeypatch.setattr(
        operation,
        "open_billing_search_page_cursor",
        lambda *_args, **_kwargs: (2,),
    )
    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        operation._sealed_next_cursor(service_result, complete)


@pytest.mark.asyncio
async def test_terminal_scope_rejects_an_inherited_page_position() -> None:
    scope = SimpleNamespace(
        is_pageable=False,
        selection=SimpleNamespace(plan_release_id=PLAN_RELEASE_ID),
    )
    page_context = SimpleNamespace(after_sort_key=(1,))

    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        await operation._serve_post_page(
            object(),
            _access(),
            scope,
            page_context,
            stage_timings=[],
        )


@pytest.mark.asyncio
async def test_public_operation_rejects_nonmapping_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda access: access,
    )

    with pytest.raises(operation.BillingSearchPostServingUnavailableError):
        await operation.execute_billing_search_post(
            object(),
            _access(),
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=None,
            environment_map=object(),
        )


@pytest.mark.parametrize(
    ("source_error", "public_error"),
    [
        (
            selector_resolution.BillingSearchSelectorNotFoundError("synthetic"),
            operation.BillingSearchResourceNotFoundError,
        ),
        (
            BillingSearchCursorGenerationExpired("synthetic"),
            operation.BillingSearchCursorGenerationExpiredError,
        ),
        (
            BillingSearchCursorError("synthetic"),
            operation.BillingSearchPostCursorInvalidError,
        ),
        (
            BillingSearchServingUnavailableError("synthetic"),
            operation.BillingSearchPostServingUnavailableError,
        ),
        (RuntimeError("synthetic"), operation.BillingSearchPostServingUnavailableError),
    ],
)
@pytest.mark.asyncio
async def test_public_operation_translates_internal_failures(
    monkeypatch: pytest.MonkeyPatch,
    source_error: Exception,
    public_error: type[Exception],
) -> None:
    monkeypatch.setattr(
        operation,
        "validate_billing_search_post_endpoint_access",
        lambda access: access,
    )

    async def fail(*_args: object, **_kwargs: object):
        raise source_error

    monkeypatch.setattr(operation, "_execute_billing_search_post", fail)

    with pytest.raises(public_error):
        await operation.execute_billing_search_post(
            _Session([]),
            _access(),
            trusted_now=TRUSTED_NOW,
            radius_zip_context_resolver=None,
            environment_map={},
        )


def test_operation_execution_repr_is_value_free() -> None:
    execution = operation.BillingSearchPostExecution({}, {}, ())

    assert repr(execution) == "<billing-search-post-execution>"


def test_selector_resolution_validates_scope_and_digest() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution.BillingSearchSelectorResolution(object(), None)
    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution.BillingSearchSelectorResolution(selector_scope(), "0" * 64)


def test_selector_source_pins_require_validated_network_tables() -> None:
    candidate = SimpleNamespace(
        network_tables_by_snapshot=lambda: None,
        in_network_bindings=(),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution._source_pinned_binding_pins(candidate)


def test_selector_access_selection_rejects_wrong_type_or_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    access = _opaque_access()
    monkeypatch.setattr(
        selector_resolution,
        "validate_billing_search_post_endpoint_access",
        lambda _candidate: access,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution._validate_access_selection(access, object())

    changed = replace(selection(), healthporta_plan_id="hpplan_" + "9" * 26)
    with pytest.raises(selector_resolution.BillingSearchSelectorNotFoundError):
        selector_resolution._validate_access_selection(access, changed)


@pytest.mark.parametrize("reference", [None, "be1_invalid"])
def test_opaque_selector_preparation_rejects_missing_or_malformed_reference(
    reference: object,
) -> None:
    with pytest.raises(selector_resolution.BillingSearchSelectorNotFoundError):
        selector_resolution._prepared_opaque_bindings(
            SimpleNamespace(billing_entity_ref=reference),
            (),
        )


@pytest.mark.parametrize("failure", ["projector-policy", "token-type"])
def test_ein_policy_verification_rejects_mismatched_projector_results(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    publication = _valid_publication()
    projector = SimpleNamespace(
        token_policy_id=(
            "ptg-tin-hmac-sha256-v1:other"
            if failure == "projector-policy"
            else publication.token_policy_id
        ),
        tokenize_ein=lambda _value: object(),
    )
    monkeypatch.setattr(
        selector_resolution,
        "load_billing_search_tin_policy",
        lambda *_args, **_kwargs: projector,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution._verified_policy_token(
            publication,
            "123456789",
            environment_map={},
        )


def test_ein_preparation_rejects_wrong_type_missing_source_and_snapshot_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        selector_resolution,
        "apply_entitled_billing_search_tax_identity",
        lambda _request, transform: transform("npi", "1000000004"),
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution._prepared_ein_bindings(object(), (), environment_map={})

    monkeypatch.undo()
    request = _ein_access().request
    missing_source_pin = SimpleNamespace(source_publication=None)
    prepared = selector_resolution._prepared_ein_bindings(
        request,
        (missing_source_pin,),
        environment_map={},
    )
    assert prepared[0].billing_entity_ref is None

    invalid_snapshot_pin = SimpleNamespace(
        source_publication=_valid_publication(),
        serving_tables=SimpleNamespace(shared_snapshot_key="not-an-integer"),
    )
    with pytest.raises(BillingSearchPostRequestError):
        selector_resolution._prepared_ein_bindings(
            request,
            (invalid_snapshot_pin,),
            environment_map={},
        )


def test_selector_preparation_rejects_unsupported_identity_type() -> None:
    request = SimpleNamespace(selector_kind="tax_identity", tax_identity_type="other")

    with pytest.raises(BillingSearchServingUnavailableError):
        selector_resolution._prepared_bindings(request, (), environment_map={})


@pytest.mark.asyncio
async def test_resolved_binding_rejects_scope_from_another_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pin = selector_resolution._source_pinned_binding_pins(selection())[0]
    prepared = selector_resolution._PreparedBindingSelector(
        pin,
        _opaque_access().request.billing_entity_ref,
        "1" * 64,
    )

    async def wrong_scope(*_args: object, **_kwargs: object):
        return object()

    monkeypatch.setattr(
        selector_resolution,
        "resolve_billing_entity_ref_source_scope",
        wrong_scope,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await selector_resolution._resolved_binding(
            object(),
            prepared,
            schema_name="ptg2",
        )


@pytest.mark.parametrize(
    "source_error",
    [
        serving_unavailable(),
        PTG2BillingAssociationProjectionUnavailable("synthetic"),
        RuntimeError("synthetic"),
    ],
)
@pytest.mark.asyncio
async def test_selector_boundary_preserves_only_public_failure_semantics(
    monkeypatch: pytest.MonkeyPatch,
    source_error: Exception,
) -> None:
    def fail(*_args: object, **_kwargs: object):
        raise source_error

    monkeypatch.setattr(selector_resolution, "_validate_access_selection", fail)

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await selector_resolution.resolve_billing_search_selector(
            object(),
            access=_opaque_access(),
            source_pinned_selection=selection(),
        )

    assert captured.value.__cause__ is None
