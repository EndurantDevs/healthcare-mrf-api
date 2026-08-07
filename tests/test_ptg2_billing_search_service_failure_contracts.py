# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed structural boundaries for exact billing search service."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_service as service
from api.billing_search_selector_contract import BILLING_SELECTOR_NO_MATCH
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_READY,
    PlanReleaseServingResolution,
)
from api.ptg2_billing_search_contract import (
    BillingSearchResourceNotFoundError,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_service_support import (
    CURSOR_KEYRING,
    TRUSTED_NOW,
    access,
    install_access,
    install_ready_release,
    selection,
    selector_resolution,
)


def test_candidate_count_rejects_untyped_candidate_scope():
    with pytest.raises(BillingSearchServingUnavailableError):
        service._candidate_geo_witness_count((object(),))


def test_selector_bindings_reject_untyped_corrupt_and_mismatched_scope():
    release_selection = selection()
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validated_selector_bindings(release_selection, object())

    corrupt_resolution = selector_resolution(release_selection)
    object.__setattr__(corrupt_resolution, "selector_scope_sha256", "0" * 64)
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validated_selector_bindings(
            release_selection,
            corrupt_resolution,
        )

    with pytest.raises(BillingSearchServingUnavailableError):
        service._validated_selector_bindings(
            selection(binding_count=2),
            selector_resolution(release_selection),
        )


@pytest.mark.asyncio
async def test_traversal_rejects_matched_scope_without_source_witness():
    release_selection = selection()
    selected_resolution = selector_resolution(release_selection)
    object.__setattr__(
        selected_resolution.selector_scope.bindings[0],
        "source_scope",
        None,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_release(
            object(),
            selection=release_selection,
            selector_resolution=selected_resolution,
            request=access().request,
        )


@pytest.mark.asyncio
async def test_traversal_rejects_binding_without_serving_tables():
    release_selection = selection()
    selected_resolution = selector_resolution(release_selection)
    release_without_tables = replace(
        release_selection,
        _validated_serving_tables=(),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_release(
            object(),
            selection=release_without_tables,
            selector_resolution=selected_resolution,
            request=access().request,
        )


@pytest.mark.asyncio
async def test_no_match_selector_scope_is_generic_not_found(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    install_ready_release(monkeypatch, release_selection)
    monkeypatch.setattr(
        service.billing_search_entity_ref_resolution,
        "resolve_billing_search_entity_ref_selector",
        AsyncMock(
            return_value=selector_resolution(
                release_selection,
                states=(BILLING_SELECTOR_NO_MATCH,),
            )
        ),
    )

    with pytest.raises(BillingSearchResourceNotFoundError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(),
            cursor_keyring=CURSOR_KEYRING,
            trusted_now=TRUSTED_NOW,
        )


@pytest.mark.asyncio
async def test_service_rejects_untyped_cursor_keyring(monkeypatch):
    install_access(monkeypatch)
    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(),
            cursor_keyring=object(),
            trusted_now=TRUSTED_NOW,
        )


@pytest.mark.asyncio
async def test_service_rejects_incomplete_ready_release_tuple(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    monkeypatch.setattr(
        service,
        "_ready_release_and_cursor",
        AsyncMock(
            return_value=(
                PlanReleaseServingResolution(
                    PLAN_RELEASE_RESOLUTION_READY,
                    release_selection,
                ),
                None,
                None,
                None,
            )
        ),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(),
            cursor_keyring=CURSOR_KEYRING,
            trusted_now=TRUSTED_NOW,
        )
