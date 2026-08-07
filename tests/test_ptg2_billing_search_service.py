# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact billing traversal and state-mapping tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_billing_search_service as service
from api.billing_search_cursor import (
    BillingSearchCursorError,
    _new_sealed_page_cursor,
)
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_NOT_FOUND,
    PLAN_RELEASE_RESOLUTION_UNAVAILABLE,
    PlanReleaseServingResolution,
)
from api.ptg2_billing_geo_contract import BillingGeoSelection
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_MATCHED,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SEARCH_RESULT_NO_SNAPSHOT,
    BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE,
    BillingSearchMatchedProvider,
    BillingSearchProviderPage,
    BillingSearchResourceNotFoundError,
    BillingSearchServingUnavailableError,
)
from api.ptg2_shared_blocks import PTG2SharedBlockError
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_page_support import (
    NPI_VALUES,
    code_witness,
    geo_witness,
    hydrated_price,
)
from tests.billing_search_service_support import (
    CURSOR_KEYRING,
    access as _access,
    install_access as _install_access,
    install_binding_readers as _install_binding_readers,
    install_ready_release as _install_ready_release,
    selection as _selection,
)


@pytest.mark.asyncio
async def test_missing_release_returns_explicit_no_snapshot(monkeypatch):
    _install_access(monkeypatch)
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

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=_access(),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_NO_SNAPSHOT
    assert search_result.selection is None


@pytest.mark.asyncio
async def test_unavailable_release_fails_closed(monkeypatch):
    _install_access(monkeypatch)
    monkeypatch.setattr(
        service.plan_release_serving,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=PlanReleaseServingResolution(
                PLAN_RELEASE_RESOLUTION_UNAVAILABLE,
                None,
            )
        ),
    )

    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_shared_block_failure_is_translated_to_serving_unavailable(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection()
    _install_ready_release(monkeypatch, release_selection)
    monkeypatch.setattr(
        service,
        "_search_ready_release",
        AsyncMock(side_effect=PTG2SharedBlockError("private graph coordinate")),
    )

    with pytest.raises(BillingSearchServingUnavailableError) as failure:
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(),
            cursor_keyring=CURSOR_KEYRING,
        )

    assert "private graph coordinate" not in str(failure.value)


@pytest.mark.asyncio
async def test_unknown_opaque_ref_and_missing_rate_states_are_distinct(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection()
    _install_ready_release(monkeypatch, release_selection)
    _install_binding_readers(monkeypatch, source_scope=None)

    with pytest.raises(BillingSearchResourceNotFoundError):
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(),
            cursor_keyring=CURSOR_KEYRING,
        )

    _install_binding_readers(monkeypatch, code_witnesses=())
    missing_rate = await service.search_exact_billing_provider_page(
        object(),
        access=_access(),
        cursor_keyring=CURSOR_KEYRING,
    )
    assert missing_rate.state == BILLING_SEARCH_RESULT_NO_MATCHING_RATES


@pytest.mark.asyncio
async def test_unknown_opaque_ref_checks_every_entitled_binding(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection(binding_count=2)
    _install_ready_release(monkeypatch, release_selection)
    _install_binding_readers(monkeypatch, source_scope=None)

    with pytest.raises(BillingSearchResourceNotFoundError):
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(),
            cursor_keyring=CURSOR_KEYRING,
        )

    source_resolver = (
        service.ptg2_billing_entity_source_resolution.resolve_billing_entity_ref_source_scope
    )
    assert source_resolver.await_count == 2


@pytest.mark.asyncio
async def test_source_projection_failure_discards_partial_binding_results(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection(binding_count=2)
    _install_ready_release(monkeypatch, release_selection)
    source_resolver = AsyncMock(
        side_effect=(
            object(),
            service.PTG2BillingAssociationDataError("synthetic failure"),
        )
    )
    monkeypatch.setattr(
        service.ptg2_billing_entity_source_resolution,
        "resolve_billing_entity_ref_source_scope",
        source_resolver,
    )
    _install_binding_readers(monkeypatch)
    monkeypatch.setattr(
        service.ptg2_billing_entity_source_resolution,
        "resolve_billing_entity_ref_source_scope",
        source_resolver,
    )

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=_access(),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_TAX_IDENTITY_UNAVAILABLE
    assert search_result.providers == ()
    assert source_resolver.await_count == 2


@pytest.mark.asyncio
async def test_release_traversal_caps_candidates_before_aggregate_sort(monkeypatch):
    release_selection = _selection(binding_count=2)
    monkeypatch.setattr(service, "MAX_PROVIDER_RATE_WITNESSES", 1)
    monkeypatch.setattr(
        service,
        "_binding_source_scope",
        AsyncMock(return_value=object()),
    )
    candidate_reader = AsyncMock(
        side_effect=(
            ((object(),), True),
            ((object(),), True),
        )
    )
    monkeypatch.setattr(service, "_binding_candidates", candidate_reader)

    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_release(
            object(),
            selection=release_selection,
            request=_access().request,
        )

    assert candidate_reader.await_count == 2


@pytest.mark.asyncio
async def test_provider_rates_without_geo_match_return_radius_state(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection()
    _install_ready_release(monkeypatch, release_selection)
    _install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(True, ()),
    )

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=_access(),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS


@pytest.mark.asyncio
async def test_optional_npi_is_passed_to_exact_group_expansion(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection()
    _install_ready_release(monkeypatch, release_selection)
    _install_binding_readers(monkeypatch, provider_rates=())

    await service.search_exact_billing_provider_page(
        object(),
        access=_access(provider_npi=NPI_VALUES[1]),
        cursor_keyring=CURSOR_KEYRING,
    )

    expansion = service.ptg2_billing_geo_reader.expand_billing_rate_witnesses_to_npis
    assert expansion.await_args.kwargs["provider_npi"] == NPI_VALUES[1]


@pytest.mark.asyncio
async def test_matched_page_seals_last_returned_provider_cursor(monkeypatch):
    _install_access(monkeypatch)
    release_selection = _selection()
    _install_ready_release(monkeypatch, release_selection)
    selected_geo_witness = geo_witness()
    _install_binding_readers(
        monkeypatch,
        geo_selection=BillingGeoSelection(True, (selected_geo_witness,)),
    )
    candidate = service.ptg2_billing_search_page.group_billing_geo_candidates(
        binding=release_selection.in_network_bindings[0],
        serving_tables=release_selection._validated_serving_tables[0][1],
        code_witnesses=(code_witness(),),
        geo_witnesses=(selected_geo_witness,),
    )[0]
    matched_provider = BillingSearchMatchedProvider(
        candidate,
        (hydrated_price(selected_geo_witness),),
    )
    monkeypatch.setattr(
        service.ptg2_billing_search_page,
        "hydrate_billing_search_page",
        AsyncMock(
            return_value=BillingSearchProviderPage(
                (matched_provider,),
                True,
                candidate.sort_key,
            )
        ),
    )
    sealed_cursor = _new_sealed_page_cursor(
        "bsc1_k1_" + "A" * 40,
        candidate.sort_key,
    )
    sealer = Mock(return_value=sealed_cursor)

    def seal_cursor(*_args, **_kwargs):
        return sealer(*_args, **_kwargs)

    monkeypatch.setattr(
        service.billing_search_pagination,
        "seal_billing_search_page_cursor",
        seal_cursor,
    )

    search_result = await service.search_exact_billing_provider_page(
        object(),
        access=_access(limit=1),
        cursor_keyring=CURSOR_KEYRING,
    )

    assert search_result.state == BILLING_SEARCH_RESULT_MATCHED
    assert search_result.next_cursor is sealed_cursor
    assert sealer.call_args.args[0] == candidate.sort_key


@pytest.mark.asyncio
async def test_graph_failure_maps_to_generic_serving_failure(monkeypatch):
    _install_access(monkeypatch)
    _install_ready_release(monkeypatch, _selection())
    _install_binding_readers(monkeypatch)
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(side_effect=PTG2ManifestArtifactError("internal detail")),
    )

    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="billing_search_serving_generation_unavailable",
    ):
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(),
            cursor_keyring=CURSOR_KEYRING,
        )


@pytest.mark.asyncio
async def test_invalid_cursor_error_remains_distinct(monkeypatch):
    _install_access(monkeypatch)
    _install_ready_release(monkeypatch, _selection())
    monkeypatch.setattr(
        service.billing_search_pagination,
        "open_billing_search_page_cursor",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BillingSearchCursorError("billing_search_cursor_invalid")
        ),
    )

    with pytest.raises(BillingSearchCursorError):
        await service.search_exact_billing_provider_page(
            object(),
            access=_access(cursor="bsc1_cursor-v1_synthetic"),
            cursor_keyring=CURSOR_KEYRING,
        )
