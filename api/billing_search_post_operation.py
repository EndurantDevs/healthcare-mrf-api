# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Orchestrate one exact billing-search page inside a read-only snapshot."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import os
import time
from typing import Any

from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
)
from api.billing_search_cursor_keys import (
    BillingSearchCursorKeyringError,
    load_billing_search_cursor_keyring,
)
from api.billing_search_pagination import (
    open_billing_search_page_cursor,
    seal_billing_search_page_cursor,
)
from api.billing_search_post_cursor_preflight import (
    BillingSearchPostPageContext,
    capture_billing_search_post_page_context,
    empty_billing_search_post_page_context,
)
from api.billing_search_post_endpoint_access import (
    BillingSearchPostEndpointAccess,
    validate_billing_search_post_endpoint_access,
)
from api.billing_search_post_endpoint_journal import (
    billing_search_post_success_journal,
)
from api.billing_search_post_query import (
    build_billing_search_resolved_query,
    build_billing_search_terminal_query,
)
from api.billing_search_response import shape_billing_search_response
from api.billing_search_selector_resolution import (
    BillingSearchSelectorResourceNotFoundError,
    resolve_billing_search_selector,
)
from api.billing_search_post_transaction import configure_billing_search_read_snapshot
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_NOT_FOUND,
    PLAN_RELEASE_RESOLUTION_READY,
    resolve_plan_release_serving_resolution,
)
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_service import (
    pin_billing_search_selection,
    search_exact_billing_provider_page,
)

_RESOURCE_NOT_FOUND = "billing_search_resource_not_found"
_CURSOR_INVALID = "billing_search_cursor_invalid"
_CURSOR_EXPIRED = "billing_search_cursor_generation_expired"
_UNAVAILABLE = "billing_search_serving_unavailable"
_RadiusResolver = Callable[[Any, str], Awaitable[Mapping[str, object] | None]]


class BillingSearchPostOperationError(RuntimeError):
    """Base value-free public operation failure."""


class BillingSearchResourceNotFoundError(BillingSearchPostOperationError):
    """Unknown or inaccessible plan/reference resource."""


class BillingSearchPostCursorInvalidError(BillingSearchPostOperationError):
    """Malformed, expired, or scope-mismatched cursor."""


class BillingSearchCursorGenerationExpiredError(BillingSearchPostOperationError):
    """Cursor references a no-longer-current immutable generation."""


class BillingSearchPostServingUnavailableError(BillingSearchPostOperationError):
    """No validated serving capability can complete the request."""


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchPostExecution:
    """Safe response plus pseudonymous operational evidence."""

    payload: dict[str, Any]
    audit_record: dict[str, object]
    stage_timings_ms: tuple[tuple[str, float], ...]

    def __repr__(self) -> str:
        return "<billing-search-post-execution>"


@dataclass(frozen=True, slots=True)
class _ResolvedPostScope:
    selection: Any
    selector_resolution: Any
    radius_context: Mapping[str, object] | None
    is_pageable: bool


def _record_stage(
    timings: list[tuple[str, float]],
    stage_name: str,
    started_at: float,
) -> float:
    now = time.perf_counter()
    timings.append((stage_name, round((now - started_at) * 1_000, 3)))
    return now


async def _radius_context(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    resolver: _RadiusResolver,
) -> Mapping[str, object] | None:
    request = access.request
    if request.radius_miles == 0.0:
        return None
    if not callable(resolver):
        raise BillingSearchServingUnavailableError(
            "billing_search_serving_generation_unavailable"
        )
    context = await resolver(session, request.zip5)
    if not isinstance(context, Mapping):
        raise BillingSearchServingUnavailableError(
            "billing_search_serving_generation_unavailable"
        )
    return context


def _selector_states(selector_scope: object) -> frozenset[str]:
    bindings = getattr(selector_scope, "bindings", None)
    if type(bindings) is not tuple:
        return frozenset()
    return frozenset(binding.state for binding in bindings)


def _is_pageable_selector_scope(
    access: BillingSearchPostEndpointAccess,
    selector_resolution: Any,
) -> bool:
    selector_states = _selector_states(selector_resolution.selector_scope)
    has_selector_match = BILLING_SELECTOR_MATCHED in selector_states
    has_unavailable_projection = (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE in selector_states
    )
    is_pageable_scope = has_selector_match and not has_unavailable_projection
    if access.request.cursor is not None and not is_pageable_scope:
        if has_unavailable_projection:
            raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
        raise BillingSearchPostCursorInvalidError(_CURSOR_INVALID)
    return is_pageable_scope


def _cursor_keyring(
    injected: BillingSearchCursorKeyring | None,
    environment: Mapping[str, str],
) -> BillingSearchCursorKeyring:
    if injected is not None:
        if type(injected) is not BillingSearchCursorKeyring:
            raise BillingSearchCursorKeyringError(
                "billing_search_cursor_keyring_invalid"
            )
        return injected
    return load_billing_search_cursor_keyring(environment)


async def _resolve_ready_selection(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    *,
    stage_timings: list[tuple[str, float]],
) -> Any:
    """Start the bounded snapshot and source-pin one validated ready release."""

    stage_started_at = time.perf_counter()
    await configure_billing_search_read_snapshot(session)
    resolution = await resolve_plan_release_serving_resolution(
        session,
        access.plan_release_id,
    )
    stage_started_at = _record_stage(
        stage_timings,
        "release_resolution",
        stage_started_at,
    )
    if resolution.state == PLAN_RELEASE_RESOLUTION_NOT_FOUND:
        raise BillingSearchResourceNotFoundError(_RESOURCE_NOT_FOUND)
    if resolution.state != PLAN_RELEASE_RESOLUTION_READY:
        raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
    return await pin_billing_search_selection(session, resolution.selection)


async def _resolve_post_scope(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    selection: Any,
    *,
    radius_zip_context_resolver: _RadiusResolver,
    environment: Mapping[str, str],
    stage_timings: list[tuple[str, float]],
) -> _ResolvedPostScope:
    """Resolve selector first and geography only for a pageable traversal."""

    stage_started_at = time.perf_counter()
    selector_resolution = await resolve_billing_search_selector(
        session,
        access=access,
        source_pinned_selection=selection,
        environment_map=environment,
    )
    _record_stage(
        stage_timings,
        "selector_resolution",
        stage_started_at,
    )
    is_pageable = _is_pageable_selector_scope(access, selector_resolution)
    radius_context = (
        await _radius_context(
            session,
            access,
            radius_zip_context_resolver,
        )
        if is_pageable
        else None
    )
    return _ResolvedPostScope(
        selection=selection,
        selector_resolution=selector_resolution,
        radius_context=radius_context,
        is_pageable=is_pageable,
    )


async def _open_post_cursor(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    selection: Any,
    *,
    trusted_now: str,
    environment: Mapping[str, str],
    cursor_keyring: BillingSearchCursorKeyring | None,
    stage_timings: list[tuple[str, float]],
) -> BillingSearchPostPageContext:
    """Capture one generation and authenticate its stable request cursor."""

    stage_started_at = time.perf_counter()
    page_context = await capture_billing_search_post_page_context(
        session,
        access,
        selection,
        trusted_now=trusted_now,
        configured_keyring=_cursor_keyring(cursor_keyring, environment),
    )
    _record_stage(stage_timings, "cursor_open", stage_started_at)
    return page_context


def _sealed_next_cursor(
    service_result: Any,
    page_context: BillingSearchPostPageContext,
) -> str | None:
    if not service_result.has_more:
        return None
    if (
        page_context.cursor_binding is None
        or service_result.next_sort_key is None
        or type(page_context.chain_keyring) is not BillingSearchCursorKeyring
    ):
        raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
    sealed_cursor = seal_billing_search_page_cursor(
        service_result.next_sort_key,
        keyring=page_context.chain_keyring,
        binding=page_context.cursor_binding,
    )
    reauthenticated_key = open_billing_search_page_cursor(
        sealed_cursor.token,
        keyring=page_context.chain_keyring,
        binding=page_context.cursor_binding,
    )
    if reauthenticated_key != service_result.next_sort_key:
        raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
    return sealed_cursor.token


async def _serve_post_page(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    scope: _ResolvedPostScope,
    page_context: BillingSearchPostPageContext,
    *,
    stage_timings: list[tuple[str, float]],
) -> dict[str, Any]:
    """Run the exact reader and authenticate any outgoing cursor."""

    stage_started_at = time.perf_counter()
    service_query = access.request.service_query
    if scope.is_pageable:
        query = build_billing_search_resolved_query(
            service_query,
            plan_release_id=scope.selection.plan_release_id,
            radius_zip_context=scope.radius_context,
            after_sort_key=page_context.after_sort_key,
        )
    else:
        if page_context.after_sort_key is not None:
            raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
        query = build_billing_search_terminal_query(
            service_query,
            plan_release_id=scope.selection.plan_release_id,
        )
    service_result = await search_exact_billing_provider_page(
        session,
        query=query,
        selection=scope.selection,
        selector_scope=scope.selector_resolution.selector_scope,
    )
    stage_started_at = _record_stage(
        stage_timings,
        "exact_reader",
        stage_started_at,
    )
    response_payload = shape_billing_search_response(
        service_result,
        next_cursor=_sealed_next_cursor(service_result, page_context),
    )
    _record_stage(stage_timings, "response_shape", stage_started_at)
    return response_payload


def _completed_execution(
    access: BillingSearchPostEndpointAccess,
    page_context: BillingSearchPostPageContext,
    response_payload: dict[str, Any],
    stage_timings: list[tuple[str, float]],
    *,
    trusted_now: str,
    started_at: float,
) -> BillingSearchPostExecution:
    generation_digest = (
        page_context.generation_pin.generation_bundle_sha256
        if page_context.generation_pin is not None
        else None
    )
    return BillingSearchPostExecution(
        payload=response_payload,
        audit_record=billing_search_post_success_journal(
            access,
            generation_bundle_sha256=generation_digest,
            trusted_observed_at=trusted_now,
            started_at=started_at,
        ),
        stage_timings_ms=tuple(stage_timings),
    )


async def _execute_billing_search_post(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    *,
    trusted_now: str,
    radius_zip_context_resolver: _RadiusResolver,
    environment: Mapping[str, str],
    cursor_keyring: BillingSearchCursorKeyring | None,
) -> BillingSearchPostExecution:
    """Execute one authorized request inside a single read-only snapshot."""

    started_at = time.perf_counter()
    stage_timings: list[tuple[str, float]] = []
    selection = await _resolve_ready_selection(
        session, access, stage_timings=stage_timings
    )
    page_context = empty_billing_search_post_page_context()
    if access.request.cursor is not None:
        page_context = await _open_post_cursor(
            session,
            access,
            selection,
            trusted_now=trusted_now,
            environment=environment,
            cursor_keyring=cursor_keyring,
            stage_timings=stage_timings,
        )
    scope = await _resolve_post_scope(
        session,
        access,
        selection,
        radius_zip_context_resolver=radius_zip_context_resolver,
        environment=environment,
        stage_timings=stage_timings,
    )
    if access.request.cursor is None and scope.is_pageable:
        page_context = await _open_post_cursor(
            session,
            access,
            selection,
            trusted_now=trusted_now,
            environment=environment,
            cursor_keyring=cursor_keyring,
            stage_timings=stage_timings,
        )
    response_payload = await _serve_post_page(
        session,
        access,
        scope,
        page_context,
        stage_timings=stage_timings,
    )
    return _completed_execution(
        access,
        page_context,
        response_payload,
        stage_timings,
        trusted_now=trusted_now,
        started_at=started_at,
    )


async def execute_billing_search_post(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    *,
    trusted_now: str,
    radius_zip_context_resolver: _RadiusResolver,
    environment_map: Mapping[str, str] | None = None,
    cursor_keyring: BillingSearchCursorKeyring | None = None,
) -> BillingSearchPostExecution:
    """Execute one request while translating failures to value-free seams."""

    try:
        validated_access = validate_billing_search_post_endpoint_access(access)
        environment = os.environ if environment_map is None else environment_map
        if not isinstance(environment, Mapping):
            raise BillingSearchPostServingUnavailableError(_UNAVAILABLE)
        return await _execute_billing_search_post(
            session,
            validated_access,
            trusted_now=trusted_now,
            radius_zip_context_resolver=radius_zip_context_resolver,
            environment=environment,
            cursor_keyring=cursor_keyring,
        )
    except BillingSearchPostOperationError:
        raise
    except BillingSearchSelectorResourceNotFoundError:
        raise BillingSearchResourceNotFoundError(_RESOURCE_NOT_FOUND) from None
    except BillingSearchCursorGenerationExpired:
        raise BillingSearchCursorGenerationExpiredError(_CURSOR_EXPIRED) from None
    except BillingSearchCursorError:
        raise BillingSearchPostCursorInvalidError(_CURSOR_INVALID) from None
    except BillingSearchServingUnavailableError:
        raise BillingSearchPostServingUnavailableError(_UNAVAILABLE) from None
    except Exception:
        raise BillingSearchPostServingUnavailableError(_UNAVAILABLE) from None
