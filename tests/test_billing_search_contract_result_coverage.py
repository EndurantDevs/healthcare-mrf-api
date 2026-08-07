# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundary coverage for billing-search immutable contracts."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import plan_release_serving_resolution as release_resolution
from api import ptg2_billing_search_contract as contract
from api.ptg2_billing_geo_contract import (
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
)
from api.ptg2_billing_search_result import (
    BillingSearchMatchedProvider,
    BillingSearchProviderCandidate,
    BillingSearchProviderPage,
    BillingSearchServiceResult,
    validate_service_result,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
)
from tests.billing_search_post_support import (
    SNAPSHOT_ID,
    address,
    billing_entity_ref,
    binding,
    code_witness,
    matched_result,
    provider_rate,
    publication,
    query,
    selection,
    serving_tables,
    source_scope,
)


def _raise_projection_error(_metadata):
    raise TaxIdentitySourceProjectionError("synthetic projection failure")


def _valid_candidate() -> BillingSearchProviderCandidate:
    return matched_result().providers[0].candidate


def _valid_provider() -> BillingSearchMatchedProvider:
    return matched_result().providers[0]


@pytest.mark.parametrize(
    ("state", "selected"),
    (
        ("invalid", None),
        (release_resolution.PLAN_RELEASE_RESOLUTION_READY, None),
        (release_resolution.PLAN_RELEASE_RESOLUTION_NOT_FOUND, selection()),
    ),
)
def test_release_resolution_rejects_incoherent_state(state, selected) -> None:
    with pytest.raises(ValueError, match="invalid plan release serving resolution"):
        release_resolution.PlanReleaseServingResolution(state, selected)


@pytest.mark.asyncio
@pytest.mark.parametrize("readiness_outcome", (False, PTG2ManifestArtifactError("x")))
async def test_release_binding_validation_fails_closed(
    monkeypatch, readiness_outcome
) -> None:
    readiness = (
        AsyncMock(side_effect=readiness_outcome)
        if isinstance(readiness_outcome, Exception)
        else AsyncMock(return_value=readiness_outcome)
    )
    monkeypatch.setattr(
        release_resolution.plan_release_serving,
        "is_release_binding_serving_ready",
        readiness,
    )

    validated = await release_resolution._validate_release_bindings(
        object(), selection()
    )

    assert validated is None


@pytest.mark.asyncio
async def test_release_binding_validation_rejects_missing_network_projection(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        release_resolution.plan_release_serving,
        "is_release_binding_serving_ready",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        type(selection()),
        "network_tables_by_snapshot",
        lambda _selection: None,
    )

    assert (
        await release_resolution._validate_release_bindings(object(), selection())
        is None
    )


@pytest.mark.asyncio
async def test_release_resolution_propagates_failed_binding_validation(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        release_resolution,
        "_load_release_rows",
        AsyncMock(return_value=[{"synthetic": True}]),
    )
    monkeypatch.setattr(
        release_resolution, "_selection_from_rows", lambda *_: selection()
    )
    monkeypatch.setattr(
        release_resolution,
        "_validate_release_bindings",
        AsyncMock(return_value=None),
    )

    resolved = await release_resolution.resolve_plan_release_serving_resolution(
        object(), selection().plan_release_id
    )

    assert resolved.state == release_resolution.PLAN_RELEASE_RESOLUTION_UNAVAILABLE


@pytest.mark.parametrize(
    "filter_values",
    ([], ("AA", "AA"), ("BB", "AA"), ("lower",), tuple("A" for _ in range(9))),
)
def test_filter_values_must_be_closed_canonical_tuples(filter_values) -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract._canonical_filter_values(
            filter_values,
            pattern=contract._MODIFIER_PATTERN,
            maximum_count=8,
        )


def test_resolved_query_rejects_noncanonical_geo_projection(monkeypatch) -> None:
    monkeypatch.setattr(contract, "validated_geo_args", lambda _args: {})

    with pytest.raises(contract.BillingSearchServingUnavailableError):
        query()


@pytest.mark.parametrize(
    "fields_by_name",
    (
        {"binding_ordinal": -1, "snapshot_id": SNAPSHOT_ID, "state": "no_match"},
        {"binding_ordinal": 0, "snapshot_id": "", "state": "no_match"},
        {"binding_ordinal": 0, "snapshot_id": SNAPSHOT_ID, "state": "invalid"},
    ),
)
def test_selector_binding_rejects_invalid_coordinates(fields_by_name) -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchSelectorBindingScope(**fields_by_name)


def test_matched_selector_binding_requires_valid_source_and_reference() -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchSelectorBindingScope(
            0,
            SNAPSHOT_ID,
            contract.BILLING_SELECTOR_MATCHED,
        )
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchSelectorBindingScope(
            0,
            SNAPSHOT_ID,
            contract.BILLING_SELECTOR_MATCHED,
            source_scope(),
            "invalid-reference",
        )


def test_selector_scope_rejects_invalid_shape_and_duplicate_coordinates() -> None:
    valid_binding = contract.BillingSearchSelectorBindingScope(
        0,
        SNAPSHOT_ID,
        contract.BILLING_SELECTOR_NO_MATCH,
    )
    assert "state=no_match binding_ordinal=0" in repr(valid_binding)
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchSelectorScope("invalid", (valid_binding,))
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchSelectorScope(
            "tax_identity", (valid_binding, valid_binding)
        )
    assert "binding_count=1" in repr(
        contract.BillingSearchSelectorScope("tax_identity", (valid_binding,))
    )


def test_source_publication_must_be_typed_replayable_and_canonical(monkeypatch) -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract._canonical_source_publication(object())

    original = publication()
    monkeypatch.setattr(
        contract,
        "tax_identity_source_publication_from_metadata",
        _raise_projection_error,
    )
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract._canonical_source_publication(original)

    changed = publication(content_digest="6" * 64)
    monkeypatch.setattr(
        contract,
        "tax_identity_source_publication_from_metadata",
        lambda _metadata: changed,
    )
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract._canonical_source_publication(original)


def test_binding_pin_rejects_invalid_scope_and_source_count() -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchBindingPin(object(), serving_tables())
    mismatched_tables = replace(serving_tables(), source_count=3)
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        contract.BillingSearchBindingPin(binding(), mismatched_tables)
    assert "source=<redacted>" in repr(
        contract.BillingSearchBindingPin(binding(), serving_tables())
    )


def test_provider_candidate_rejects_reference_structure_and_witness_mismatch() -> None:
    candidate = _valid_candidate()
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(candidate, billing_entity_ref="invalid-reference")
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(candidate, geo_witnesses=())

    changed_address = replace(address(), location_key="7" * 64)
    changed_geo = BillingProviderGeoWitness(provider_rate(), changed_address)
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(candidate, geo_witnesses=(changed_geo,))
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(candidate, code_witnesses_by_key=())
    assert "scope=<redacted>" in repr(candidate)


def test_matched_provider_rejects_empty_and_foreign_price_witnesses() -> None:
    provider = _valid_provider()
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(provider, price_witnesses=())

    foreign_geo = BillingProviderGeoWitness(
        provider_rate(), replace(address(), location_key="7" * 64)
    )
    foreign_price = BillingProviderGeoPriceWitness(
        foreign_geo,
        ({"negotiated_rate": 10},),
    )
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(provider, price_witnesses=(foreign_price,))
    assert "price_witness_count=1" in repr(provider)


def test_provider_page_rejects_shape_and_pagination_mismatch() -> None:
    provider = _valid_provider()
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        BillingSearchProviderPage([provider], False, None)
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        BillingSearchProviderPage((), True, None)
    assert "provider_count=1" in repr(
        BillingSearchProviderPage((provider,), False, None)
    )


def test_service_result_rejects_structure_and_binding_coordinate_mismatch() -> None:
    valid = matched_result()
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(valid, state="invalid")
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        replace(valid, binding_pins=())
    assert "provider_count=1" in repr(valid)


def test_service_result_validator_rejects_foreign_type() -> None:
    with pytest.raises(contract.BillingSearchServingUnavailableError):
        validate_service_result(BillingSearchServiceResult)
