# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation preflight for one billing-search cursor chain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from api.billing_search_cursor import BillingSearchCursorKeyring
from api.billing_search_cursor_scope import (
    billing_search_stable_request_fingerprint,
    select_billing_search_cursor_chain_keyring,
)
from api.billing_search_pagination import (
    BillingSearchCursorBinding,
    BillingSearchGenerationPin,
    build_billing_search_cursor_binding,
    capture_billing_search_generation_pin,
    open_billing_search_page_cursor,
)
from api.billing_search_post_endpoint_access import BillingSearchPostEndpointAccess
from api.plan_release_serving import PlanReleaseServingSelection


@dataclass(frozen=True, slots=True, repr=False)
class BillingSearchPostPageContext:
    """One generation pin, authenticated page position, and retained chain key."""

    generation_pin: BillingSearchGenerationPin | None
    cursor_binding: BillingSearchCursorBinding | None
    after_sort_key: tuple[int | float | str, ...] | None
    chain_keyring: BillingSearchCursorKeyring | None

    def __repr__(self) -> str:
        return "<billing-search-post-page-context>"


def empty_billing_search_post_page_context() -> BillingSearchPostPageContext:
    """Return the generation-free context used by terminal first pages."""

    return BillingSearchPostPageContext(None, None, None, None)


async def capture_billing_search_post_page_context(
    session: Any,
    access: BillingSearchPostEndpointAccess,
    selection: PlanReleaseServingSelection,
    *,
    trusted_now: str,
    configured_keyring: BillingSearchCursorKeyring,
) -> BillingSearchPostPageContext:
    """Capture current generation and authenticate an optional continuation."""

    chain_keyring = select_billing_search_cursor_chain_keyring(
        access.request.cursor,
        keyring=configured_keyring,
    )
    request_fingerprint = billing_search_stable_request_fingerprint(
        access.request,
        plan_release_id=access.plan_release_id,
        chain_keyring=chain_keyring,
    )
    generation_pin = await capture_billing_search_generation_pin(
        session,
        selection,
    )
    cursor_binding = build_billing_search_cursor_binding(
        request_fingerprint,
        access.authorization_context,
        generation_pin,
        trusted_now=trusted_now,
    )
    after_sort_key = None
    if access.request.cursor is not None:
        after_sort_key = open_billing_search_page_cursor(
            access.request.cursor,
            keyring=chain_keyring,
            binding=cursor_binding,
        )
    return BillingSearchPostPageContext(
        generation_pin,
        cursor_binding,
        after_sort_key,
        chain_keyring,
    )


__all__ = [
    "BillingSearchPostPageContext",
    "capture_billing_search_post_page_context",
    "empty_billing_search_post_page_context",
]
