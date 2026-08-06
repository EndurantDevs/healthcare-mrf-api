# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure and multi-binding edges for exact billing-search service."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_search_service as service
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
)
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_NOT_FOUND,
    PlanReleaseServingResolution,
)
from api.ptg2_billing_geo_contract import BillingGeoSelection
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BillingSearchMatchedProvider,
    BillingSearchProviderPage,
    BillingSearchServingUnavailableError,
)
from tests.billing_search_page_support import (
    NPI_VALUES,
    code_witness,
    geo_witness,
    hydrated_price,
)
from tests.billing_search_service_support import (
    CURSOR_KEYRING,
    access,
    install_access,
    install_binding_readers,
    install_ready_release,
    selection,
)


def _matched_provider(release_selection, binding_index=0):
    release_binding = release_selection.in_network_bindings[binding_index]
    serving_tables = release_selection.serving_tables_for_snapshot(
        release_binding.snapshot_id
    )
    selected_geo_witness = geo_witness(snapshot_key=17 + binding_index)
    candidate = service.ptg2_billing_search_page.group_billing_geo_candidates(
        binding=release_binding,
        serving_tables=serving_tables,
        code_witnesses=(code_witness(),),
        geo_witnesses=(selected_geo_witness,),
    )[0]
    return BillingSearchMatchedProvider(
        candidate,
        (hydrated_price(selected_geo_witness),),
    )


@pytest.mark.asyncio
async def test_address_projection_unavailable_fails_closed(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    install_ready_release(monkeypatch, release_selection)
    install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(False, ()),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_generation_expired_cursor_remains_distinct(monkeypatch):
    install_access(monkeypatch)
    install_ready_release(monkeypatch, selection())
    monkeypatch.setattr(
        service.billing_search_pagination,
        "open_billing_search_page_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BillingSearchCursorGenerationExpired(
                "billing_search_cursor_generation_expired"
            )
        ),
    )

    with pytest.raises(BillingSearchCursorGenerationExpired):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(cursor="bsc1_cursor-v1_synthetic"),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_missing_release_expires_a_supplied_generation_cursor(monkeypatch):
    install_access(monkeypatch)
    monkeypatch.setattr(
        service.plan_release_serving,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=PlanReleaseServingResolution(
                PLAN_RELEASE_RESOLUTION_NOT_FOUND,
                None,
            )
        ),
    )

    with pytest.raises(BillingSearchCursorGenerationExpired):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(cursor="bsc1_cursor-v1_unavailable-generation"),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_cursor_sealing_failure_maps_to_generic_unavailable(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    install_ready_release(monkeypatch, release_selection)
    matched_provider = _matched_provider(release_selection)
    install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(
            True,
            (matched_provider.candidate.geo_witnesses[0],),
        ),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(
            return_value=BillingSearchProviderPage(
                (matched_provider,),
                True,
                matched_provider.candidate.sort_key,
            )
        ),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "seal_billing_search_page_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BillingSearchCursorError("billing_search_cursor_invalid")
        ),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_issued_cursor_with_empty_traversal_fails_closed(monkeypatch):
    install_access(monkeypatch)
    install_ready_release(monkeypatch, selection(), after_sort_key=(1,))
    install_binding_readers(monkeypatch, source_scope=None)

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(cursor="bsc1_cursor-v1_synthetic"),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_issued_cursor_with_empty_hydrated_page_fails_closed(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    matched_provider = _matched_provider(release_selection)
    install_ready_release(
        monkeypatch,
        release_selection,
        after_sort_key=matched_provider.candidate.sort_key,
    )
    install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(
            True,
            (matched_provider.candidate.geo_witnesses[0],),
        ),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(return_value=BillingSearchProviderPage((), False, None)),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=access(cursor="bsc1_cursor-v1_synthetic"),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_first_binding_miss_does_not_hide_later_binding_match(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection(binding_count=2)
    install_ready_release(monkeypatch, release_selection)
    matched_provider = _matched_provider(release_selection, binding_index=1)
    source_resolver = AsyncMock(side_effect=(None, object()))
    monkeypatch.setattr(
        service.ptg2_billing_entity_source_resolution,
        "resolve_billing_entity_ref_source_scope",
        source_resolver,
    )
    monkeypatch.setattr(
        service,
        "_binding_candidates",
        AsyncMock(return_value=((matched_provider.candidate,), True)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(
            return_value=BillingSearchProviderPage(
                (matched_provider,),
                False,
                None,
            )
        ),
    )

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=access(),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_MATCHED
    assert search_result.providers == (matched_provider,)
    assert source_resolver.await_count == 2


@pytest.mark.asyncio
async def test_first_page_price_filter_miss_is_explicit_no_rates(monkeypatch):
    install_access(monkeypatch)
    release_selection = selection()
    install_ready_release(monkeypatch, release_selection)
    matched_provider = _matched_provider(release_selection)
    install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(
            True,
            (matched_provider.candidate.geo_witnesses[0],),
        ),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(return_value=BillingSearchProviderPage((), False, None)),
    )

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=access(),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_NO_MATCHING_RATES


def _assert_source_and_code_calls(
    session,
    release_selection,
    endpoint_access,
    source_scope,
):
    """Verify identity and code readers share the exact release binding."""

    release_binding = release_selection.in_network_bindings[0]
    serving_tables = release_selection.serving_tables_for_snapshot(
        release_binding.snapshot_id
    )
    source_reader = (
        service.ptg2_billing_entity_source_resolution.resolve_billing_entity_ref_source_scope
    )
    code_reader = service.ptg2_billing_code_reader.load_exact_billing_code_witnesses
    exact_reader = (
        service.ptg2_billing_exact_reader.load_exact_billing_rate_occurrence_witnesses
    )
    assert source_reader.await_args.args == (session,)
    assert source_reader.await_args.kwargs == {
        "schema_name": service.ptg2_serving.PTG2_SCHEMA,
        "snapshot_key": 17,
        "billing_entity_ref": endpoint_access.request.billing_entity_ref,
    }
    assert code_reader.await_args.args == (session, serving_tables, release_binding)
    assert code_reader.await_args.kwargs == {
        "code_system": "CPT",
        "code": "99213",
    }
    assert exact_reader.await_args.kwargs["source_scope"] is source_scope
    return serving_tables


def _assert_rate_and_geo_calls(serving_tables, rate_witnesses, provider_rates):
    """Verify rate, NPI, and GEO readers retain one physical scope."""

    exact_reader = (
        service.ptg2_billing_exact_reader.load_exact_billing_rate_occurrence_witnesses
    )
    npi_reader = service.ptg2_billing_geo_reader.expand_billing_rate_witnesses_to_npis
    geo_reader = service.ptg2_billing_geo_reader.load_exact_billing_geo_witnesses
    assert exact_reader.await_args.args[1] is serving_tables
    assert exact_reader.await_args.kwargs["code_keys"] == (5,)
    assert npi_reader.await_args.args[1] is serving_tables
    assert npi_reader.await_args.kwargs == {
        "rate_witnesses": rate_witnesses,
        "provider_npi": NPI_VALUES[1],
    }
    assert geo_reader.await_args.args[1] is serving_tables
    assert geo_reader.await_args.kwargs == {
        "provider_rate_witnesses": provider_rates,
        "geo_args": {"zip5": "25000"},
    }


@pytest.mark.asyncio
async def test_reader_chain_receives_one_exact_binding_scope(monkeypatch):
    """Every reader must receive one unchanged binding and witness chain."""

    install_access(monkeypatch)
    release_selection = selection()
    install_ready_release(monkeypatch, release_selection)
    source_scope = object()
    rate_witnesses = (object(),)
    provider_rates = (object(),)
    install_binding_readers(
        monkeypatch,
        source_scope=source_scope,
        provider_rates=provider_rates,
    )
    service.ptg2_billing_exact_reader.load_exact_billing_rate_occurrence_witnesses.return_value = (
        rate_witnesses
    )

    endpoint_access = access(provider_npi=NPI_VALUES[1])
    session = object()
    await service.search_exact_billing_provider_page(
        session,
        access=endpoint_access,
        cursor_keyring=CURSOR_KEYRING,
    )

    serving_tables = _assert_source_and_code_calls(
        session,
        release_selection,
        endpoint_access,
        source_scope,
    )
    _assert_rate_and_geo_calls(serving_tables, rate_witnesses, provider_rates)
