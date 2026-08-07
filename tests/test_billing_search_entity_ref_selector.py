# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transport-neutral opaque billing-reference selector tests."""

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_entity_ref_resolution as resolution
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchServingUnavailableError,
)
from api.plan_release_serving import PTG2_SCHEMA
from api.ptg2_billing_entity_refs import (
    PTG2BillingAssociationProjectionUnavailable,
)
from tests.billing_search_entity_ref_support import (
    SNAPSHOT_ID,
    billing_entity_reference,
    release_binding,
    resolved_source_scope,
    serving_tables,
    source_pinned_selection,
    source_publication,
)

SECOND_SNAPSHOT_ID = "ptg2:synthetic-billing-selector-b"
MATCHED_SCOPE_SHA256 = (
    "131351188b683a5aee1b45fc5b1dab523f47304b2206419bc705f8ffc01eac37"
)


async def _resolve_reference(
    reference: object,
    *,
    selection=None,
    authorized_plan_release_id: object | None = None,
    schema_name: str = PTG2_SCHEMA,
):
    selected_release = selection or source_pinned_selection()
    return await resolution.resolve_billing_search_entity_ref_selector(
        object(),
        billing_entity_ref=reference,
        authorized_plan_release_id=(
            getattr(
                selected_release,
                "plan_release_id",
                "hprelease_" + "1" * 26,
            )
            if authorized_plan_release_id is None
            else authorized_plan_release_id
        ),
        source_pinned_selection=selected_release,
        schema_name=schema_name,
    )


def _two_binding_selection():
    first_publication = source_publication()
    second_publication = source_publication(content_digest="6" * 64)
    first_tables = serving_tables(publication=first_publication)
    second_tables = serving_tables(
        publication=second_publication,
        snapshot_id=SECOND_SNAPSHOT_ID,
        shared_snapshot_key=18,
    )
    return replace(
        source_pinned_selection(tables=first_tables),
        bindings=(
            release_binding(),
            release_binding(
                binding_ordinal=1,
                snapshot_id=SECOND_SNAPSHOT_ID,
            ),
        ),
        _validated_serving_tables=(
            (SECOND_SNAPSHOT_ID, second_tables),
            (SNAPSHOT_ID, first_tables),
        ),
    )


@pytest.mark.asyncio
async def test_reference_resolution_preserves_source_binding_and_digest(
    monkeypatch,
) -> None:
    reference = billing_entity_reference()
    publication = source_publication()
    selection = source_pinned_selection(tables=serving_tables(publication=publication))
    calls: list[dict[str, object]] = []

    async def exact_scope(_session, **options_by_name):
        calls.append(options_by_name)
        return resolved_source_scope(publication=publication)

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        exact_scope,
    )

    resolved = await _resolve_reference(reference, selection=selection)

    assert calls == [
        {
            "schema_name": PTG2_SCHEMA,
            "snapshot_key": 17,
            "billing_entity_ref": reference,
            "source_publication": publication,
        }
    ]
    assert resolved.selector_scope.selector_kind == "billing_entity_ref"
    assert resolved.selector_scope.bindings[0].state == BILLING_SELECTOR_MATCHED
    assert resolved.selector_scope.bindings[0].billing_entity_ref == reference
    assert resolved.selector_scope.bindings[0].source_scope == (
        resolved_source_scope(publication=publication)
    )
    assert resolved.selector_scope_sha256 == MATCHED_SCOPE_SHA256


@pytest.mark.asyncio
async def test_reference_release_mismatch_is_generic_not_found(
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
async def test_reference_rejects_malformed_values_without_lookup(
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
async def test_reference_all_definitive_misses_are_not_found(monkeypatch) -> None:
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
async def test_reference_preserves_missing_projection_state(monkeypatch) -> None:
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
async def test_reference_preserves_runtime_projection_unavailable(
    monkeypatch,
) -> None:
    async def unavailable(*_args, **_kwargs):
        raise PTG2BillingAssociationProjectionUnavailable("synthetic")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        unavailable,
    )

    resolved = await _resolve_reference(billing_entity_reference())

    assert resolved.selector_scope.bindings[0].state == (
        BILLING_SELECTOR_PROJECTION_UNAVAILABLE
    )
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second_outcome,expected_states",
    [
        (
            "no_match",
            [BILLING_SELECTOR_MATCHED, BILLING_SELECTOR_NO_MATCH],
        ),
        (
            "unavailable",
            [
                BILLING_SELECTOR_NO_MATCH,
                BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            ],
        ),
    ],
)
async def test_reference_preserves_binding_order_and_mixed_outcomes(
    monkeypatch,
    second_outcome,
    expected_states,
) -> None:
    calls: list[int] = []

    async def resolve_scope(_session, **options_by_name):
        snapshot_key = options_by_name["snapshot_key"]
        calls.append(snapshot_key)
        if second_outcome == "no_match":
            if snapshot_key == 18:
                return None
            return resolved_source_scope(
                publication=options_by_name["source_publication"],
                snapshot_key=snapshot_key,
            )
        if snapshot_key == 18:
            raise PTG2BillingAssociationProjectionUnavailable("synthetic")
        return None

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        resolve_scope,
    )

    resolved = await _resolve_reference(
        billing_entity_reference(),
        selection=_two_binding_selection(),
    )

    assert calls == [17, 18]
    assert [
        (binding.binding_ordinal, binding.snapshot_id)
        for binding in resolved.selector_scope.bindings
    ] == [(0, SNAPSHOT_ID), (1, SECOND_SNAPSHOT_ID)]
    assert [binding.state for binding in resolved.selector_scope.bindings] == (
        expected_states
    )
    assert len(resolved.selector_scope_sha256 or "") == 64


@pytest.mark.asyncio
async def test_reference_checks_every_binding_before_generic_not_found(
    monkeypatch,
) -> None:
    calls: list[int] = []

    async def no_match(_session, **options_by_name):
        calls.append(options_by_name["snapshot_key"])
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
        await _resolve_reference(
            billing_entity_reference(),
            selection=_two_binding_selection(),
        )

    assert calls == [17, 18]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "selection",
    [
        object(),
        source_pinned_selection(include_source_proof=False),
        replace(source_pinned_selection(), _validated_serving_tables=()),
        source_pinned_selection(
            tables=serving_tables(source_key="synthetic-other-source")
        ),
    ],
)
async def test_reference_invalid_or_unproven_source_pin_fails_closed(
    selection,
) -> None:
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ):
        await _resolve_reference(
            billing_entity_reference(),
            selection=selection,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("forgery", ["snapshot", "publication", "type"])
async def test_reference_forged_source_scope_fails_closed(
    monkeypatch,
    forgery,
) -> None:
    reference = billing_entity_reference()

    async def forged_scope(_session, **options_by_name):
        if forgery == "type":
            return object()
        scope = resolved_source_scope(
            publication=options_by_name["source_publication"],
            snapshot_key=options_by_name["snapshot_key"],
        )
        return type(scope)(
            snapshot_key=(99 if forgery == "snapshot" else scope.snapshot_key),
            publication=(
                source_publication(content_digest="7" * 64)
                if forgery == "publication"
                else scope.publication
            ),
            witnesses=scope.witnesses,
        )

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        forged_scope,
    )

    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="^billing_search_serving_generation_unavailable$",
    ) as captured:
        await _resolve_reference(reference)

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert reference not in repr(captured.value)


@pytest.mark.asyncio
async def test_binding_contract_failure_is_generic(monkeypatch) -> None:
    async def no_match(*_args, **_kwargs):
        return None

    def invalid_binding_scope(*_args, **_kwargs):
        raise RuntimeError("synthetic-internal-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        no_match,
    )
    monkeypatch.setattr(
        resolution,
        "BillingSearchSelectorBindingScope",
        invalid_binding_scope,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await _resolve_reference(billing_entity_reference())

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


@pytest.mark.asyncio
async def test_completed_scope_contract_failure_is_generic(monkeypatch) -> None:
    async def exact_scope(_session, **options_by_name):
        return resolved_source_scope(
            publication=options_by_name["source_publication"],
            snapshot_key=options_by_name["snapshot_key"],
        )

    def invalid_selector_scope(*_args, **_kwargs):
        raise RuntimeError("synthetic-internal-detail")

    monkeypatch.setattr(
        resolution,
        "resolve_billing_entity_ref_source_scope",
        exact_scope,
    )
    monkeypatch.setattr(
        resolution,
        "BillingSearchSelectorScope",
        invalid_selector_scope,
    )

    with pytest.raises(BillingSearchServingUnavailableError) as captured:
        await _resolve_reference(billing_entity_reference())

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
