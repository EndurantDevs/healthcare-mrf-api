# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed reader and service coverage for exact billing search."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_code_reader as code_reader
from api import ptg2_billing_price_reader as price_reader
from api import ptg2_billing_search_service as service
from api.ptg2_billing_geo_contract import BillingGeoSelection
from api.ptg2_billing_search_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BillingSearchBindingPin,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_result import BillingSearchProviderPage
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_post_support import (
    SNAPSHOT_ID,
    binding,
    billing_entity_ref,
    code_witness,
    matched_result,
    provider_rate,
    publication,
    query,
    selection,
    selector_scope,
    serving_tables,
    source_scope,
)


def _code_row(*, code_key: int = 5) -> dict[str, object]:
    return {
        "code_key": code_key,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "plan_id": "synthetic-plan-token",
        "plan_market_type": "group",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_name": None,
        "source_description": None,
    }


def _pin(*, include_publication: bool = True) -> BillingSearchBindingPin:
    return BillingSearchBindingPin(
        binding(), serving_tables(include_publication=include_publication)
    )


def _matched_selector_binding():
    return selector_scope().bindings[0]


def test_code_witness_rejects_invalid_key_and_redacts_repr() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="key is malformed"):
        replace(code_witness(), code_key=-1)
    assert "code_key=<internal>" in repr(code_witness())


def test_code_witness_rejects_noncanonical_normalizer_replay(monkeypatch) -> None:
    monkeypatch.setattr(
        code_reader,
        "_exact_code",
        lambda _system, code: ("HCPCS", code),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="witness is malformed"):
        code_reader.BillingCodeWitness(
            code_key=5,
            code_system="CPT",
            code="99213",
            negotiation_arrangement=None,
            billing_code_type_version=None,
            source_name=None,
            source_description=None,
        )


def test_exact_code_rejects_foreign_types_and_normalizer_failure(monkeypatch) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="code is invalid"):
        code_reader._exact_code(object(), "99213")

    monkeypatch.setattr(
        code_reader,
        "normalize_capacity_code_system",
        lambda _system: (_ for _ in ()).throw(
            code_reader.CapacityEvidenceError("synthetic", "code_system")
        ),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="code is invalid"):
        code_reader._exact_code("CPT", "99213")


def test_exact_code_rejects_noncanonical_normalizer_output(monkeypatch) -> None:
    monkeypatch.setattr(
        code_reader, "normalize_capacity_code_system", lambda _system: "CPT"
    )
    monkeypatch.setattr(
        code_reader, "normalize_capacity_code", lambda _system, _code: "99213"
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not canonical"):
        code_reader._exact_code("cpt", "99213")


def test_code_metadata_validators_reject_malformed_values() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="source name is malformed"):
        code_reader._optional_text("line\nbreak", category="source name")
    with pytest.raises(PTG2ManifestArtifactError, match="key is malformed"):
        code_reader._code_key(True)
    with pytest.raises(PTG2ManifestArtifactError, match="row is malformed"):
        code_reader._billing_code_witness(
            object(), binding=binding(), code_system="CPT", code="99213"
        )


def test_code_witness_collection_enforces_cap_and_unique_keys(monkeypatch) -> None:
    monkeypatch.setattr(code_reader, "MAX_EXACT_BILLING_CODE_WITNESSES", 0)
    with pytest.raises(PTG2ManifestArtifactError, match="witness limit"):
        code_reader._validated_code_witnesses(
            (_code_row(),),
            binding=binding(),
            code_system="CPT",
            code="99213",
        )

    monkeypatch.setattr(code_reader, "MAX_EXACT_BILLING_CODE_WITNESSES", 256)
    with pytest.raises(PTG2ManifestArtifactError, match="duplicate keys"):
        code_reader._validated_code_witnesses(
            (_code_row(), _code_row()),
            binding=binding(),
            code_system="CPT",
            code="99213",
        )


def test_code_reader_rejects_unsealed_binding() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="sealed network binding"):
        code_reader._validate_binding(object(), binding())


def test_price_filter_rejects_foreign_args_and_snapshot() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="filter is malformed"):
        price_reader._normalized_price_filter_args([])

    crossed_rate = replace(provider_rate().rate_occurrence, snapshot_key=999)
    with pytest.raises(PTG2ManifestArtifactError, match="crossed its snapshot"):
        price_reader._normalized_rate_witnesses(serving_tables(), (crossed_rate,))


@pytest.mark.asyncio
async def test_price_filter_rejects_invalid_runtime_limits(monkeypatch) -> None:
    monkeypatch.setattr(price_reader, "MAX_RATE_FILTER_PRICE_KEYS", 0)

    with pytest.raises(PTG2ManifestArtifactError, match="invalid limits"):
        await price_reader.filter_exact_billing_rate_occurrences(
            object(),
            serving_tables(),
            rate_witnesses=(provider_rate().rate_occurrence,),
            price_filter_args={"modifiers": ("AA",)},
        )


@pytest.mark.asyncio
async def test_source_pin_rejects_foreign_missing_and_incomplete_selection(
    monkeypatch,
) -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._source_pinned_selection(object(), object())

    monkeypatch.setattr(
        type(selection()), "network_tables_by_snapshot", lambda _selection: None
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._source_pinned_selection(object(), selection())


@pytest.mark.asyncio
async def test_source_pin_reuses_one_snapshot_read_for_duplicate_bindings(
    monkeypatch,
) -> None:
    duplicate_bindings = (binding(), binding())
    monkeypatch.setattr(
        type(selection()),
        "in_network_bindings",
        property(lambda _selection: duplicate_bindings),
    )
    snapshot_read = AsyncMock(return_value=serving_tables())
    monkeypatch.setattr(
        service.ptg2_tables,
        "snapshot_serving_tables",
        snapshot_read,
    )

    pinned = await service._source_pinned_selection(object(), selection())

    assert pinned.network_tables_by_snapshot() == {SNAPSHOT_ID: serving_tables()}
    snapshot_read.assert_awaited_once()

    monkeypatch.setattr(
        type(selection()), "network_tables_by_snapshot", lambda _selection: {}
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._source_pinned_selection(object(), selection())


@pytest.mark.asyncio
async def test_public_source_pin_translates_manifest_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_source_pinned_selection",
        AsyncMock(side_effect=PTG2ManifestArtifactError("synthetic")),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.pin_billing_search_selection(object(), selection())


def test_binding_pin_projection_rejects_missing_table_map(monkeypatch) -> None:
    monkeypatch.setattr(
        type(selection()), "network_tables_by_snapshot", lambda _selection: None
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        service._binding_pins(selection())


def test_selector_binding_validation_rejects_missing_publication_and_state() -> None:
    no_match = BillingSearchSelectorBindingScope(
        0,
        SNAPSHOT_ID,
        BILLING_SELECTOR_NO_MATCH,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validate_selector_binding(
            query(), no_match, _pin(include_publication=False)
        )

    object.__setattr__(no_match, "state", "invalid")
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validate_selector_binding(query(), no_match, _pin())


def test_selector_binding_validation_rejects_crossed_source_publication() -> None:
    crossed_scope = source_scope(
        source_publication=publication(content_digest="6" * 64)
    )
    crossed_binding = BillingSearchSelectorBindingScope(
        0,
        SNAPSHOT_ID,
        BILLING_SELECTOR_MATCHED,
        crossed_scope,
        billing_entity_ref(),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        service._validate_selector_binding(query(), crossed_binding, _pin())


def test_scope_validation_rejects_foreign_query_and_coordinates() -> None:
    pin = _pin()
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validated_scope_bindings(object(), selector_scope(), (pin,))

    empty_scope = BillingSearchSelectorScope("tax_identity", ())
    with pytest.raises(BillingSearchServingUnavailableError):
        service._validated_scope_bindings(query(), empty_scope, (pin,))


@pytest.mark.asyncio
async def test_binding_candidates_require_matched_source_material() -> None:
    no_match = BillingSearchSelectorBindingScope(
        0,
        SNAPSHOT_ID,
        BILLING_SELECTOR_NO_MATCH,
    )
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._binding_candidates(
            object(), query=query(), selector_binding=no_match, binding_pin=_pin()
        )


def _install_candidate_chain(
    monkeypatch,
    *,
    codes=(code_witness(),),
    rates=(provider_rate().rate_occurrence,),
    providers=(provider_rate(),),
    geo_selection=BillingGeoSelection(True, ()),
) -> None:
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(return_value=codes),
    )
    monkeypatch.setattr(
        service,
        "_load_price_filtered_rate_witnesses",
        AsyncMock(return_value=rates),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "expand_billing_rate_witnesses_to_npis",
        AsyncMock(return_value=providers),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "load_exact_billing_geo_witnesses",
        AsyncMock(return_value=geo_selection),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_stage", ("codes", "rates", "providers"))
async def test_binding_candidates_stop_at_empty_exact_stage(
    monkeypatch, empty_stage
) -> None:
    chain_by_name = {
        "codes": {"codes": ()},
        "rates": {"rates": ()},
        "providers": {"providers": ()},
    }
    _install_candidate_chain(monkeypatch, **chain_by_name[empty_stage])

    candidates, has_rates = await service._binding_candidates(
        object(),
        query=query(),
        selector_binding=_matched_selector_binding(),
        binding_pin=_pin(),
    )

    assert candidates == ()
    assert has_rates is False


@pytest.mark.asyncio
async def test_binding_candidates_require_address_projection(monkeypatch) -> None:
    _install_candidate_chain(
        monkeypatch,
        geo_selection=BillingGeoSelection(False, ()),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service._binding_candidates(
            object(),
            query=query(),
            selector_binding=_matched_selector_binding(),
            binding_pin=_pin(),
        )


@pytest.mark.asyncio
async def test_traversal_skips_nonmatch_and_enforces_candidate_invariants(
    monkeypatch,
) -> None:
    no_match = BillingSearchSelectorBindingScope(
        0,
        SNAPSHOT_ID,
        BILLING_SELECTOR_NO_MATCH,
    )
    skipped = await service._traverse_matched_bindings(
        object(),
        query=query(),
        selector_bindings=(no_match,),
        binding_pins=(_pin(),),
    )
    assert skipped.candidates == ()

    candidate = matched_result().providers[0].candidate
    monkeypatch.setattr(
        service,
        "_binding_candidates",
        AsyncMock(return_value=((candidate,), True)),
    )
    monkeypatch.setattr(service, "MAX_PROVIDER_RATE_WITNESSES", 0)
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_matched_bindings(
            object(),
            query=query(),
            selector_bindings=(_matched_selector_binding(),),
            binding_pins=(_pin(),),
        )

    monkeypatch.setattr(service, "MAX_PROVIDER_RATE_WITNESSES", 32768)
    service._binding_candidates.return_value = ((candidate, candidate), True)
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_matched_bindings(
            object(),
            query=query(),
            selector_bindings=(_matched_selector_binding(),),
            binding_pins=(_pin(),),
        )


@pytest.mark.asyncio
async def test_source_pinned_search_rejects_release_mismatch() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._search_source_pinned_selection(
            object(),
            query=query(plan_release_id="hprelease_" + "9" * 26),
            selection=selection(),
            selector_scope=selector_scope(),
        )


@pytest.mark.asyncio
async def test_source_pinned_search_maps_empty_hydrated_page(monkeypatch) -> None:
    candidate = matched_result().providers[0].candidate
    monkeypatch.setattr(
        service,
        "_traverse_matched_bindings",
        AsyncMock(return_value=service._BillingSearchTraversal((candidate,), True)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(return_value=BillingSearchProviderPage((), False, None)),
    )

    result = await service._search_source_pinned_selection(
        object(),
        query=query(),
        selection=selection(),
        selector_scope=selector_scope(),
    )

    assert result.state == service.BILLING_SEARCH_RESULT_NO_MATCHING_RATES


@pytest.mark.asyncio
async def test_public_search_translates_manifest_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "_search_source_pinned_selection",
        AsyncMock(side_effect=PTG2ManifestArtifactError("synthetic")),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            query=query(),
            selection=selection(),
            selector_scope=selector_scope(),
        )
