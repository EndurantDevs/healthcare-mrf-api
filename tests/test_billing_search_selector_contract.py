# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from api import billing_search_selector_contract as contract
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorResolution,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.billing_search_entity_ref_support import (
    GROUP_REF,
    SNAPSHOT_ID,
    billing_entity_reference,
    release_binding,
    resolved_source_scope,
    serving_tables,
    source_publication,
)


def _matched_binding() -> BillingSearchSelectorBindingScope:
    return BillingSearchSelectorBindingScope(
        binding_ordinal=0,
        snapshot_id=SNAPSHOT_ID,
        state=BILLING_SELECTOR_MATCHED,
        source_scope=resolved_source_scope(),
        billing_entity_ref=billing_entity_reference(),
    )


def test_source_pinned_contracts_are_immutable_and_redacted() -> None:
    reference = billing_entity_reference()
    pin = BillingSearchBindingPin(release_binding(), serving_tables())
    binding_scope = _matched_binding()
    scope = BillingSearchSelectorScope("billing_entity_ref", (binding_scope,))
    resolution = BillingSearchSelectorResolution(scope, "1" * 64)

    assert pin.source_publication == source_publication()
    assert "source=<redacted>" in repr(pin)
    assert reference not in repr(binding_scope)
    assert GROUP_REF not in repr(binding_scope)
    assert "binding_count=1" in repr(scope)
    assert reference not in repr(resolution)
    with pytest.raises(Exception):
        pin.binding = release_binding(binding_ordinal=1)


@pytest.mark.parametrize(
    "state",
    [BILLING_SELECTOR_NO_MATCH, BILLING_SELECTOR_PROJECTION_UNAVAILABLE],
)
def test_nonmatched_binding_states_are_explicit_and_evidence_free(state: str) -> None:
    binding_scope = BillingSearchSelectorBindingScope(
        binding_ordinal=0,
        snapshot_id=SNAPSHOT_ID,
        state=state,
    )

    assert binding_scope.source_scope is None
    assert binding_scope.billing_entity_ref is None


@pytest.mark.parametrize(
    "binding,tables",
    [
        (release_binding(role="allowed_amounts"), serving_tables()),
        (
            release_binding(snapshot_id="ptg2:other"),
            serving_tables(),
        ),
        (
            release_binding(),
            serving_tables(storage_generation="shared_blocks_v3"),
        ),
        (
            release_binding(),
            serving_tables(source_key="other-source"),
        ),
        (
            release_binding(),
            serving_tables(
                publication=source_publication(source_count=1),
            ),
        ),
        (
            release_binding(),
            serving_tables(
                provider_tax_identity_source_publication=object(),
            ),
        ),
    ],
)
def test_binding_pin_rejects_nonexact_or_uncanonical_descriptors(
    binding,
    tables,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchBindingPin(binding, tables)


@pytest.mark.parametrize("canonical_result", ["error", "mismatch"])
def test_binding_pin_rejects_unreproducible_source_publication(
    monkeypatch,
    canonical_result,
) -> None:
    if canonical_result == "error":

        def canonicalize(_metadata):
            raise TaxIdentitySourceProjectionError("synthetic")

    else:

        def canonicalize(_metadata):
            return source_publication(content_digest="6" * 64)

    monkeypatch.setattr(
        contract,
        "tax_identity_source_publication_from_metadata",
        canonicalize,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchBindingPin(release_binding(), serving_tables())


@pytest.mark.parametrize(
    "updates",
    [
        {"binding_ordinal": True},
        {"binding_ordinal": -1},
        {"snapshot_id": ""},
        {"snapshot_id": object()},
        {"state": "unexpected"},
        {"state": []},
    ],
)
def test_binding_scope_rejects_invalid_coordinates_and_states(updates) -> None:
    fields_by_name = {
        "binding_ordinal": 0,
        "snapshot_id": SNAPSHOT_ID,
        "state": BILLING_SELECTOR_NO_MATCH,
    }
    fields_by_name.update(updates)

    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorBindingScope(**fields_by_name)


@pytest.mark.parametrize(
    "updates",
    [
        {"source_scope": resolved_source_scope()},
        {"billing_entity_ref": billing_entity_reference()},
    ],
)
def test_nonmatched_scope_rejects_fabricated_evidence(updates) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorBindingScope(
            binding_ordinal=0,
            snapshot_id=SNAPSHOT_ID,
            state=BILLING_SELECTOR_NO_MATCH,
            **updates,
        )


@pytest.mark.parametrize(
    "source_scope,reference",
    [
        (None, billing_entity_reference()),
        (resolved_source_scope(), None),
        (resolved_source_scope(), "be1_invalid"),
        (object(), billing_entity_reference()),
    ],
)
def test_matched_scope_requires_typed_scope_and_canonical_reference(
    source_scope,
    reference,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorBindingScope(
            binding_ordinal=0,
            snapshot_id=SNAPSHOT_ID,
            state=BILLING_SELECTOR_MATCHED,
            source_scope=source_scope,
            billing_entity_ref=reference,
        )


@pytest.mark.parametrize(
    "selector_kind,bindings",
    [
        ("unexpected", (_matched_binding(),)),
        (object(), (_matched_binding(),)),
        ("billing_entity_ref", ()),
        ("billing_entity_ref", [_matched_binding()]),
        ("billing_entity_ref", (object(),)),
        (
            "billing_entity_ref",
            (
                replace(_matched_binding(), binding_ordinal=1),
                _matched_binding(),
            ),
        ),
        (
            "billing_entity_ref",
            (_matched_binding(), _matched_binding()),
        ),
    ],
)
def test_selector_scope_requires_nonempty_unique_sorted_binding_cut(
    selector_kind,
    bindings,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorScope(selector_kind, bindings)


@pytest.mark.parametrize(
    "selector_scope,digest",
    [
        (object(), "1" * 64),
        (
            BillingSearchSelectorScope(
                "billing_entity_ref",
                (_matched_binding(),),
            ),
            "",
        ),
        (
            BillingSearchSelectorScope(
                "billing_entity_ref",
                (_matched_binding(),),
            ),
            "0" * 64,
        ),
        (
            BillingSearchSelectorScope(
                "billing_entity_ref",
                (_matched_binding(),),
            ),
            "g" * 64,
        ),
        (
            BillingSearchSelectorScope(
                "billing_entity_ref",
                (_matched_binding(),),
            ),
            b"1" * 64,
        ),
    ],
)
def test_resolution_rejects_invalid_scope_or_pseudonymous_digest(
    selector_scope,
    digest,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        BillingSearchSelectorResolution(selector_scope, digest)


def test_resolution_allows_unavailable_tax_identity_scope_without_a_digest() -> None:
    scope = BillingSearchSelectorScope(
        "tax_identity",
        (
            BillingSearchSelectorBindingScope(
                0,
                SNAPSHOT_ID,
                BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
            ),
        ),
    )

    assert BillingSearchSelectorResolution(scope, None).selector_scope is scope
