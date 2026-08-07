# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from api import billing_search_selector_resolution as resolution
from api.billing_search_post_request import BillingSearchPostRequestError
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_selector_support import (
    NOW,
    ein_access,
    npi_access,
    opaque_access,
    resolved_source_scope,
    serving_tables,
    source_pinned_selection,
)


def _binding_pins():
    return resolution._source_pinned_binding_pins(source_pinned_selection())


@pytest.mark.asyncio
async def test_incomplete_source_aware_descriptor_cut_fails_closed() -> None:
    incomplete_selection = replace(
        source_pinned_selection(),
        _validated_serving_tables=(),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=incomplete_selection,
            trusted_now=NOW,
        )


@pytest.mark.parametrize("reference", [None, "be1_invalid"])
def test_opaque_preparation_rejects_missing_or_noncanonical_reference(
    reference,
) -> None:
    request = SimpleNamespace(billing_entity_ref=reference)

    with pytest.raises(resolution.BillingSearchSelectorNotFoundError):
        resolution._prepared_opaque_bindings(request, _binding_pins())


def test_ein_preparation_rejects_a_non_ein_request() -> None:
    with pytest.raises(BillingSearchPostRequestError):
        resolution._prepared_ein_bindings(
            npi_access().request,
            _binding_pins(),
            environment_map=None,
        )


@pytest.mark.asyncio
async def test_ein_without_source_publication_is_explicitly_unavailable(
    monkeypatch,
) -> None:
    def unexpected(*_args, **_kwargs):
        raise AssertionError("missing publication must stop before token or DB work")

    monkeypatch.setattr(resolution, "load_billing_search_tin_policy", unexpected)
    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    resolved = await resolution.resolve_billing_search_selector(
        object(),
        access=ein_access(),
        source_pinned_selection=source_pinned_selection(
            tables=serving_tables(include_publication=False)
        ),
        trusted_now=NOW,
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert resolved.selector_scope_sha256 is None


@pytest.mark.asyncio
async def test_ein_with_invalid_snapshot_locator_fails_closed() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await resolution.resolve_billing_search_selector(
            object(),
            access=ein_access(),
            source_pinned_selection=source_pinned_selection(
                tables=serving_tables(shared_snapshot_key=None)
            ),
            trusted_now=NOW,
        )


def test_unsupported_internal_selector_type_fails_closed() -> None:
    request = SimpleNamespace(
        selector_kind="tax_identity",
        tax_identity_type="unsupported",
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        resolution._prepared_bindings(
            request,
            _binding_pins(),
            environment_map=None,
        )


@pytest.mark.asyncio
async def test_preparation_not_found_is_returned_without_exception_context(
    monkeypatch,
) -> None:
    def not_found(*_args, **_kwargs):
        raise resolution.BillingSearchSelectorNotFoundError(
            "billing_search_resource_not_found"
        )

    monkeypatch.setattr(resolution, "_prepared_bindings", not_found)

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError
    ) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )

    assert captured.value.__context__ is None


@pytest.mark.asyncio
async def test_completed_resolution_contract_failure_is_generic(
    monkeypatch,
) -> None:
    async def source_scope(_session, **options_by_name):
        return resolved_source_scope(
            publication=options_by_name["source_publication"]
        )

    def invalid_scope(*_args, **_kwargs):
        raise RuntimeError("synthetic-internal-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        source_scope,
    )
    monkeypatch.setattr(resolution, "BillingSearchSelectorScope", invalid_scope)

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await resolution.resolve_billing_search_selector(
            object(),
            access=opaque_access(),
            source_pinned_selection=source_pinned_selection(),
            trusted_now=NOW,
        )

    assert captured.value.__context__ is None
