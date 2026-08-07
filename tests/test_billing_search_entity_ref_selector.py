# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transport-neutral opaque billing-reference selector tests."""

from __future__ import annotations

import pytest

from api import billing_search_entity_ref_resolution as entity_ref_resolution
from api import billing_search_selector_resolution as resolution
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_selector_support import (
    NOW,
    billing_entity_reference,
    opaque_access,
    resolved_source_scope,
    serving_tables,
    source_pinned_selection,
    source_publication,
)


async def _resolve_reference(
    reference: object,
    *,
    selection=None,
    authorized_plan_release_id: object | None = None,
):
    selected_release = selection or source_pinned_selection()
    return await entity_ref_resolution.resolve_billing_search_entity_ref_selector(
        object(),
        billing_entity_ref=reference,
        authorized_plan_release_id=(
            selected_release.plan_release_id
            if authorized_plan_release_id is None
            else authorized_plan_release_id
        ),
        source_pinned_selection=selected_release,
    )


@pytest.mark.asyncio
async def test_neutral_reference_seam_matches_existing_post_resolution(
    monkeypatch,
) -> None:
    reference = billing_entity_reference()
    publication = source_publication()
    selection = source_pinned_selection(
        tables=serving_tables(publication=publication)
    )
    calls: list[dict[str, object]] = []

    async def exact_scope(_session, **options_by_name):
        calls.append(options_by_name)
        return resolved_source_scope(publication=publication)

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        exact_scope,
    )

    neutral = await _resolve_reference(reference, selection=selection)
    post = await resolution.resolve_billing_search_selector(
        object(),
        access=opaque_access(reference),
        source_pinned_selection=selection,
        trusted_now=NOW,
    )

    assert neutral == post
    assert neutral.selector_scope.bindings[0].state == BILLING_SELECTOR_MATCHED
    assert neutral.selector_scope.bindings[0].billing_entity_ref == reference
    assert len(neutral.selector_scope_sha256 or "") == 64
    assert len(calls) == 2
    assert all(call["billing_entity_ref"] == reference for call in calls)


@pytest.mark.asyncio
async def test_neutral_reference_release_mismatch_is_generic_not_found(
    monkeypatch,
) -> None:
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("resolver must not run")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError,
        match="^billing_search_resource_not_found$",
    ) as captured:
        await _resolve_reference(
            billing_entity_reference(),
            authorized_plan_release_id="hprelease_" + "9" * 26,
        )

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize("reference", [None, "", "be1_invalid", object()])
async def test_neutral_reference_rejects_malformed_values_without_lookup(
    monkeypatch,
    reference,
) -> None:
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("resolver must not run")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError,
        match="^billing_search_resource_not_found$",
    ):
        await _resolve_reference(reference)


@pytest.mark.asyncio
async def test_neutral_reference_all_definitive_misses_are_not_found(
    monkeypatch,
) -> None:
    async def no_match(*_args, **_kwargs):
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        no_match,
    )

    with pytest.raises(
        resolution.BillingSearchSelectorNotFoundError,
        match="^billing_search_resource_not_found$",
    ):
        await _resolve_reference(billing_entity_reference())


@pytest.mark.asyncio
async def test_neutral_reference_preserves_missing_projection_state(
    monkeypatch,
) -> None:
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("resolver must not run")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unexpected,
    )
    selection = source_pinned_selection(
        tables=serving_tables(include_publication=False)
    )

    resolved = await _resolve_reference(
        billing_entity_reference(),
        selection=selection,
    )

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    [object(), source_pinned_selection(include_source_proof=False)],
)
async def test_neutral_reference_invalid_or_unproven_selection_fails_closed(
    selection,
) -> None:
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ):
        await entity_ref_resolution.resolve_billing_search_entity_ref_selector(
            object(),
            billing_entity_ref=billing_entity_reference(),
            authorized_plan_release_id=(
                selection.plan_release_id
                if hasattr(selection, "plan_release_id")
                else "hprelease_" + "1" * 26
            ),
            source_pinned_selection=selection,
        )
