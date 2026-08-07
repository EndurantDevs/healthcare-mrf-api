# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact service orchestration tests for billing-search POST."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_service as service
from api.ptg2_billing_geo_contract import (
    BillingGeoSelection,
    BillingProviderGeoPriceWitness,
    BillingProviderGeoWitness,
)
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorScope,
    BillingSearchServingUnavailableError,
)
from api.ptg2_billing_search_result import (
    BillingSearchMatchedProvider,
    BillingSearchProviderPage,
)
from tests.billing_search_post_support import (
    SNAPSHOT_ID,
    address,
    binding,
    code_witness,
    provider_rate,
    publication,
    query,
    selection,
    selector_scope,
    serving_tables,
)


def _nonmatched_scope(state: str) -> BillingSearchSelectorScope:
    return BillingSearchSelectorScope(
        selector_kind="tax_identity",
        bindings=(
            BillingSearchSelectorBindingScope(
                binding_ordinal=0,
                snapshot_id=SNAPSHOT_ID,
                state=state,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_source_pin_rereads_exact_publication_descriptor(monkeypatch) -> None:
    unpinned_tables = serving_tables(include_publication=False)
    pinned_tables = serving_tables()
    table_reader = AsyncMock(return_value=pinned_tables)
    monkeypatch.setattr(service.ptg2_tables, "snapshot_serving_tables", table_reader)

    pinned_selection = await service.pin_billing_search_selection(
        object(),
        selection(tables=unpinned_tables),
    )

    assert pinned_selection.serving_tables_for_snapshot(SNAPSHOT_ID) == pinned_tables
    assert table_reader.await_args.args[1] == SNAPSHOT_ID
    assert table_reader.await_args.kwargs == {
        "include_billing_tax_identity_source": True
    }


@pytest.mark.asyncio
async def test_source_pin_rejects_a_changed_serving_descriptor(monkeypatch) -> None:
    monkeypatch.setattr(
        service.ptg2_tables,
        "snapshot_serving_tables",
        AsyncMock(return_value=serving_tables(include_publication=False)),
    )
    changed_prior = replace(
        serving_tables(include_publication=False),
        price_dictionary_item_count=127,
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.pin_billing_search_selection(
            object(),
            selection(tables=changed_prior),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tables", "scope", "expected_state"),
    (
        (
            serving_tables(),
            _nonmatched_scope(BILLING_SELECTOR_NO_MATCH),
            BILLING_SEARCH_RESULT_NO_MATCHING_TAX_IDENTITY,
        ),
        (
            serving_tables(include_publication=False),
            _nonmatched_scope(BILLING_SELECTOR_PROJECTION_UNAVAILABLE),
            BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
        ),
    ),
)
async def test_selector_no_match_states_are_successful_and_reader_free(
    monkeypatch,
    tables,
    scope,
    expected_state,
) -> None:
    code_reader = AsyncMock()
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        code_reader,
    )

    result = await service.search_exact_billing_provider_page(
        object(),
        query=query(),
        selection=selection(tables=tables),
        selector_scope=scope,
    )

    assert result.state == expected_state
    assert result.providers == ()
    code_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_npi_selector_reports_projection_unavailable_on_ein_publication() -> None:
    tables = serving_tables()

    result = await service.search_exact_billing_provider_page(
        object(),
        query=query(tax_identity_type="npi"),
        selection=selection(tables=tables),
        selector_scope=_nonmatched_scope(BILLING_SELECTOR_PROJECTION_UNAVAILABLE),
    )

    assert result.state == BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE


@pytest.mark.asyncio
async def test_ein_projection_unavailable_contradicts_existing_publication() -> None:
    tables = serving_tables()

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            query=query(),
            selection=selection(tables=tables),
            selector_scope=_nonmatched_scope(BILLING_SELECTOR_PROJECTION_UNAVAILABLE),
        )


@pytest.mark.asyncio
async def test_release_without_in_network_snapshot_has_explicit_state() -> None:
    result = await service.search_exact_billing_provider_page(
        object(),
        query=query(),
        selection=selection(bindings=()),
        selector_scope=BillingSearchSelectorScope("tax_identity", ()),
    )

    assert result.state == BILLING_SEARCH_RESULT_NO_SNAPSHOT


def _install_matched_reader_chain(
    monkeypatch,
    *,
    exact_code,
    exact_rate,
    exact_provider_rate,
    exact_geo,
):
    code_lookup = AsyncMock(return_value=(exact_code,))
    rate_lookup = AsyncMock(return_value=(exact_rate,))
    npi_lookup = AsyncMock(return_value=(exact_provider_rate,))
    geo_lookup = AsyncMock(return_value=BillingGeoSelection(True, (exact_geo,)))

    async def hydrate_page(_session, *, candidates, **_kwargs):
        candidate = tuple(candidates)[0]
        price_witness = BillingProviderGeoPriceWitness(
            candidate.geo_witnesses[0],
            ({"negotiated_rate": 20},),
        )
        matched_provider = BillingSearchMatchedProvider(
            candidate,
            (price_witness,),
        )
        return BillingSearchProviderPage((matched_provider,), False, None)

    patches = (
        (
            service.ptg2_billing_code_reader,
            "load_exact_billing_code_witnesses",
            code_lookup,
        ),
        (
            service.ptg2_billing_exact_reader,
            "load_exact_billing_rate_occurrence_witnesses",
            rate_lookup,
        ),
        (
            service.ptg2_billing_geo_reader,
            "expand_billing_rate_witnesses_to_npis",
            npi_lookup,
        ),
        (
            service.ptg2_billing_geo_reader,
            "load_exact_billing_geo_witnesses",
            geo_lookup,
        ),
        (service.ptg2_billing_search_page, "hydrate_billing_search_page", hydrate_page),
    )
    for module, attribute_name, replacement in patches:
        monkeypatch.setattr(module, attribute_name, replacement)
    return code_lookup, rate_lookup, npi_lookup, geo_lookup


@pytest.mark.asyncio
async def test_matched_service_preserves_every_exact_reader_stage(
    monkeypatch,
) -> None:
    tables = serving_tables()
    exact_code = code_witness()
    exact_provider_rate = provider_rate()
    exact_rate = exact_provider_rate.rate_occurrence
    exact_geo = BillingProviderGeoWitness(exact_provider_rate, address())
    code_lookup, rate_lookup, npi_lookup, geo_lookup = _install_matched_reader_chain(
        monkeypatch,
        exact_code=exact_code,
        exact_rate=exact_rate,
        exact_provider_rate=exact_provider_rate,
        exact_geo=exact_geo,
    )
    scope = selector_scope(
        source_publication=tables.provider_tax_identity_source_publication
    )

    service_result = await service.search_exact_billing_provider_page(
        object(),
        query=query(),
        selection=selection(tables=tables),
        selector_scope=scope,
    )

    assert service_result.state == BILLING_SEARCH_RESULT_MATCHED
    assert service_result.providers[0].candidate.geo_witnesses == (exact_geo,)
    assert service_result.providers[0].candidate.billing_entity_ref == (
        scope.bindings[0].billing_entity_ref
    )
    assert code_lookup.await_args.args[1:] == (tables, binding())
    assert rate_lookup.await_args.kwargs == {
        "source_scope": scope.bindings[0].source_scope,
        "code_keys": (5,),
    }
    assert npi_lookup.await_args.kwargs == {
        "rate_witnesses": (exact_rate,),
        "provider_npi": None,
    }
    assert geo_lookup.await_args.kwargs == {
        "provider_rate_witnesses": (exact_provider_rate,),
        "geo_args": {"zip5": "25000"},
    }


@pytest.mark.asyncio
async def test_price_filter_prunes_cross_group_rate_before_provider_expansion(
    monkeypatch,
) -> None:
    tables = serving_tables()
    exact_provider_rate = provider_rate()
    matching_rate = exact_provider_rate.rate_occurrence
    nonmatching_rate = replace(
        matching_rate,
        source_record_ordinal=1,
        provider_group_ref="bb" * 16,
        provider_set_key=4,
        price_key=11,
        occurrence_ordinal=1,
    )
    exact_geo = BillingProviderGeoWitness(exact_provider_rate, address())
    _code_lookup, rate_lookup, npi_lookup, _geo_lookup = _install_matched_reader_chain(
        monkeypatch,
        exact_code=code_witness(),
        exact_rate=matching_rate,
        exact_provider_rate=exact_provider_rate,
        exact_geo=exact_geo,
    )
    rate_lookup.return_value = (matching_rate, nonmatching_rate)
    price_filter = AsyncMock(return_value=(matching_rate,))
    monkeypatch.setattr(
        service.ptg2_billing_price_reader,
        "filter_exact_billing_rate_occurrences",
        price_filter,
    )

    service_result = await service.search_exact_billing_provider_page(
        object(),
        query=query(modifiers=("AA",), place_of_service=("11",)),
        selection=selection(tables=tables),
        selector_scope=selector_scope(
            source_publication=tables.provider_tax_identity_source_publication
        ),
    )

    assert service_result.state == BILLING_SEARCH_RESULT_MATCHED
    assert price_filter.await_args.kwargs == {
        "rate_witnesses": (matching_rate, nonmatching_rate),
        "price_filter_args": {
            "modifiers": ("AA",),
            "place_of_service": ("11",),
        },
    }
    assert npi_lookup.await_args.kwargs["rate_witnesses"] == (matching_rate,)


@pytest.mark.asyncio
async def test_empty_price_filter_result_is_not_misclassified_as_geo_miss(
    monkeypatch,
) -> None:
    tables = serving_tables()
    exact_rate = provider_rate().rate_occurrence
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(return_value=(code_witness(),)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_exact_reader,
        "load_exact_billing_rate_occurrence_witnesses",
        AsyncMock(return_value=(exact_rate,)),
    )
    price_filter = AsyncMock(return_value=())
    npi_lookup = AsyncMock()
    geo_lookup = AsyncMock()
    monkeypatch.setattr(
        service.ptg2_billing_price_reader,
        "filter_exact_billing_rate_occurrences",
        price_filter,
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "expand_billing_rate_witnesses_to_npis",
        npi_lookup,
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "load_exact_billing_geo_witnesses",
        geo_lookup,
    )

    service_result = await service.search_exact_billing_provider_page(
        object(),
        query=query(modifiers=("AA",)),
        selection=selection(tables=tables),
        selector_scope=selector_scope(
            source_publication=tables.provider_tax_identity_source_publication
        ),
    )

    assert service_result.state == BILLING_SEARCH_RESULT_NO_MATCHING_RATES
    price_filter.assert_awaited_once()
    npi_lookup.assert_not_awaited()
    geo_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_rates_without_provider_address_mean_no_geo_match(
    monkeypatch,
) -> None:
    tables = serving_tables()
    exact_provider_rate = provider_rate()
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(return_value=(code_witness(),)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_exact_reader,
        "load_exact_billing_rate_occurrence_witnesses",
        AsyncMock(return_value=(exact_provider_rate.rate_occurrence,)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "expand_billing_rate_witnesses_to_npis",
        AsyncMock(return_value=(exact_provider_rate,)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "load_exact_billing_geo_witnesses",
        AsyncMock(return_value=BillingGeoSelection(True, ())),
    )

    service_result = await service.search_exact_billing_provider_page(
        object(),
        query=query(),
        selection=selection(tables=tables),
        selector_scope=selector_scope(
            source_publication=tables.provider_tax_identity_source_publication
        ),
    )

    assert service_result.state == BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS


@pytest.mark.asyncio
async def test_empty_rate_intersection_means_no_matching_rates(monkeypatch) -> None:
    tables = serving_tables()
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(return_value=(code_witness(),)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_exact_reader,
        "load_exact_billing_rate_occurrence_witnesses",
        AsyncMock(return_value=()),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "expand_billing_rate_witnesses_to_npis",
        AsyncMock(return_value=()),
    )

    result = await service.search_exact_billing_provider_page(
        object(),
        query=query(),
        selection=selection(tables=tables),
        selector_scope=selector_scope(
            source_publication=tables.provider_tax_identity_source_publication
        ),
    )

    assert result.state == BILLING_SEARCH_RESULT_NO_MATCHING_RATES


@pytest.mark.asyncio
async def test_no_result_cursor_state_fails_closed() -> None:
    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            query=query(after_sort_key=(0, 0.0, 0, SNAPSHOT_ID, 1000000004, "x", "y")),
            selection=selection(),
            selector_scope=_nonmatched_scope(BILLING_SELECTOR_NO_MATCH),
        )
