from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from api import mrf_discovery_catalog_paging as catalog_paging
from api import provider_specialty_filters as specialty_filters
from api.endpoint import reports


@pytest.mark.asyncio
async def test_specialty_cache_and_normalization_edges(monkeypatch) -> None:
    assert specialty_filters._specialty_key_variants("") == ()
    cache = specialty_filters.SpecialtyResolutionCache()
    assert cache.suggestions("") == ()
    assert specialty_filters._static_specialty_suggestions("") == ()
    assert specialty_filters._is_normalized_boolean(True) is True
    assert specialty_filters._normalize_taxonomy_codes(123) == ("123",)
    assert specialty_filters._normalize_taxonomy_codes(
        [" 207Q00000X ", "", "207q00000x"]
    ) == ("207Q00000X",)

    ensure = AsyncMock()
    process_cache = SimpleNamespace(ensure=ensure)
    monkeypatch.setattr(
        specialty_filters,
        "_SPECIALTY_RESOLUTION_CACHE",
        process_cache,
    )
    await specialty_filters.ensure_specialty_resolution_cache(object())
    ensure.assert_awaited_once()
    assert specialty_filters.specialty_resolution_cache() is process_cache

    cache = specialty_filters.SpecialtyResolutionCache()

    class _RefreshCompletedBeforeLock:
        async def __aenter__(self):
            cache._loaded_at = specialty_filters.time.monotonic()

        async def __aexit__(self, *_args):
            return None

    cache._lock = _RefreshCompletedBeforeLock()
    cache._load_dynamic_entries = AsyncMock(
        side_effect=AssertionError("fresh cache must not reload")
    )
    await cache.ensure(object())


@pytest.mark.asyncio
async def test_dynamic_specialty_none_and_sql_contract_edges(monkeypatch) -> None:
    dynamic_module = ModuleType("api.provider_specialty_cache_entries")
    dynamic_module.load_dynamic_specialty_entry = AsyncMock(return_value=None)
    monkeypatch.setitem(
        sys.modules,
        "api.provider_specialty_cache_entries",
        dynamic_module,
    )

    unresolved = await specialty_filters.resolve_ptg_provider_specialty_filter(
        object(),
        {"specialty": "not a known specialty"},
    )
    assert unresolved.unresolved_specialty == "not a known specialty"

    inactive = specialty_filters.ProviderSpecialtyFilter()
    assert inactive.response_payload() is None
    assert specialty_filters.provider_specialty_taxonomy_exists_sql(
        "p.npi",
        {},
        "inactive",
        inactive,
    ) == ""

    classification = specialty_filters.ProviderSpecialtyFilter(
        classification="Dentist",
        include_subspecialties=False,
        primary_only=False,
    )
    semijoin_parameter_map: dict[str, object] = {}
    semijoin_sql = specialty_filters.provider_specialty_taxonomy_semijoin_sql(
        semijoin_parameter_map,
        "semi",
        classification,
    )
    exists_parameter_map: dict[str, object] = {}
    exists_sql = specialty_filters.provider_specialty_taxonomy_exists_sql(
        "p.npi",
        exists_parameter_map,
        "exists",
        classification,
    )
    assert "specialization" in semijoin_sql
    assert "specialization" in exists_sql
    assert semijoin_parameter_map == {"semi_classification": "Dentist"}
    assert exists_parameter_map == {"exists_classification": "Dentist"}


@pytest.mark.asyncio
async def test_report_market_handlers_select_zip_and_county_defaults(
    monkeypatch,
) -> None:
    market_query = AsyncMock(return_value=(0, []))
    monkeypatch.setattr(reports, "_query_market_summaries", market_query)
    monkeypatch.setattr(
        reports,
        "parse_pagination",
        lambda *_args, **_kwargs: SimpleNamespace(limit=25, offset=0, page=1),
    )

    for handler in (
        reports.list_pharmacy_markets,
        reports.list_pharmacy_access_rankings,
    ):
        for arguments, expected_scope in (
            ({"zip": "02139"}, "zip"),
            ({"county": "Middlesex"}, "county"),
        ):
            request = SimpleNamespace(
                ctx=SimpleNamespace(sa_session=object()),
                args=arguments,
            )
            market_response = await handler(request)
            assert market_response.status == 200
            assert market_query.await_args.kwargs["scope"] == expected_scope


@pytest.mark.asyncio
async def test_report_endpoint_error_and_context_scope_edges(monkeypatch) -> None:
    request = SimpleNamespace(
        ctx=SimpleNamespace(sa_session=object()),
        args={},
    )
    with pytest.raises(InvalidUsage, match="market_id"):
        await reports.get_pharmacy_market_by_id(request, "invalid")

    monkeypatch.setattr(reports, "_ALLOWED_SCOPES", {"city"})
    with pytest.raises(InvalidUsage, match="Unsupported"):
        await reports.get_pharmacy_market_by_id(request, "state:MA")

    monkeypatch.setattr(reports, "_ALLOWED_SCOPES", {"state", "city", "county", "zip"})
    market_query = AsyncMock(return_value=(0, []))
    monkeypatch.setattr(reports, "_query_market_summaries", market_query)
    with pytest.raises(NotFound, match="Unknown market_id"):
        await reports.get_pharmacy_market_by_id(request, "state:MA")

    with pytest.raises(InvalidUsage, match="name_like"):
        await reports.get_pharmacy_chain_summary(request)

    fetch_pharmacy_context = AsyncMock(return_value=None)
    monkeypatch.setattr(
        reports,
        "_fetch_pharmacy_context",
        fetch_pharmacy_context,
    )
    with pytest.raises(NotFound, match="Pharmacy not found"):
        await reports.get_pharmacy_market_context(request, "1234567890")

    for pharmacy_context, expected_scope in (
        ({"county": "Middlesex", "state": "MA"}, "county"),
        ({"state": "MA"}, "state"),
    ):
        fetch_pharmacy_context.return_value = pharmacy_context
        context_response = await reports.get_pharmacy_market_context(
            request,
            "1234567890",
        )
        assert context_response.status == 200
        assert market_query.await_args.kwargs["scope"] == expected_scope


def test_catalog_paging_mapping_and_identity_edges() -> None:
    windows, cursor = catalog_paging.bounded_file_windows(
        [{"mrf_file_id": "file-a", "plan_ids": []}],
        limit=1,
        cursor_plan_offset=0,
        plan_reference_limit=10,
    )
    assert windows[0].plan_limit == 0
    assert cursor is None

    assert catalog_paging.value_count(" ") == 0
    assert catalog_paging.value_count(42) == 0
    driver_row = SimpleNamespace(_mapping={"mrf_file_id": "file-b"})
    assert catalog_paging.row_mapping(driver_row) == {"mrf_file_id": "file-b"}

    ambiguous_plan_keys = catalog_paging.ambiguous_plan_identity_keys(
        [
            "not-a-row",
            {
                "plan_id": "ignored",
                "plan_market_type": "group",
                "plan_hash": "present",
            },
            {"plan_id": "missing-market", "plan_name": "ignored"},
            {
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "plan_name": "First",
            },
            {
                "plan_id": "plan-a",
                "plan_market_type": "group",
                "plan_name": "Second",
            },
        ]
    )
    assert ambiguous_plan_keys == {("plan-a", "", "GROUP")}
    assert catalog_paging.plan_identity_key({"plan_id": "missing-market"}) is None
