# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.test_ptg2_geo_rate_prefix import _production_tables
from tests.test_ptg2_manifest_search_transitions import (
    _CODE_ROW,
    _NPI,
    _PROVIDER_ROW,
    _PROVIDER_SET_ID,
    _RATE_ROW,
    _install_base_dependencies,
    _query_args,
    _search,
)


async def _radiology_search(monkeypatch, include_providers=None):
    radiology_rate_by_field = {**_RATE_ROW, "reported_code": "73721"}
    _install_base_dependencies(monkeypatch, merge_result=[radiology_rate_by_field])
    location_matches = AsyncMock(
        return_value=({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]})
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", location_matches)
    args_by_name = _query_args(code="73721", state="IL")
    if include_providers is not None:
        args_by_name["include_providers"] = include_providers
    response_by_field, _session = await _search(
        args=args_by_name,
        code_rows=[{**_CODE_ROW, "reported_code": "73721"}],
    )
    return response_by_field, location_matches


@pytest.mark.asyncio
async def test_omitted_expansion_matches_true_with_inferred_taxonomy(monkeypatch):
    omitted_response_by_field, omitted_location_matches = await _radiology_search(
        monkeypatch
    )
    true_response_by_field, true_location_matches = await _radiology_search(
        monkeypatch,
        "true",
    )

    for response_by_field, location_matches in (
        (omitted_response_by_field, omitted_location_matches),
        (true_response_by_field, true_location_matches),
    ):
        assert response_by_field["query"]["include_providers"] is True
        assert response_by_field["items"][0]["npi"] == _NPI
        filter_args_by_name = location_matches.await_args.args[2]
        assert serving._inferred_provider_taxonomy_rule(filter_args_by_name) is not None
        assert any(
            "npi_taxonomy" in clause
            for clause in serving._membership_taxonomy_filters(filter_args_by_name, {})
        )
    assert omitted_response_by_field == true_response_by_field


@pytest.mark.asyncio
async def test_explicit_true_expands_with_inferred_taxonomy(monkeypatch):
    response_by_field, location_matches = await _radiology_search(monkeypatch, "true")

    assert response_by_field["query"]["include_providers"] is True
    assert response_by_field["items"][0]["npi"] == _NPI
    assert serving._inferred_provider_taxonomy_rule(
        location_matches.await_args.args[2]
    ) is not None


@pytest.mark.asyncio
async def test_omitted_expansion_applies_explicit_provider_filter(monkeypatch):
    _install_base_dependencies(monkeypatch)
    location_matches = AsyncMock(
        return_value=({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]})
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", location_matches)

    response_by_field, _session = await _search(
        args=_query_args(state="IL", classification="Family Medicine")
    )

    assert response_by_field["query"]["include_providers"] is True
    assert response_by_field["items"][0]["npi"] == _NPI
    assert location_matches.await_args.args[2]["classification"] == "Family Medicine"
    assert location_matches.await_args.kwargs["require_provider_set_coverage"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_filter",
    (
        {"classification": "Family Medicine"},
        {"taxonomy_code": "207Q00000X"},
        {"provider_sex_code": "F"},
    ),
)
async def test_explicit_false_rejects_unscoped_provider_filter(provider_filter):
    """Never return an aggregate row after silently dropping a provider filter."""

    with pytest.raises(
        serving.PTG2ProviderFilterScopeError,
        match="require an NPI or supported cost-ordered geographic scope",
    ):
        await _search(
            args=_query_args(
                include_providers="false",
                **provider_filter,
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_filter",
    (
        "provider_type",
        "taxonomy_classification",
        "taxonomy_specialization",
        "taxonomy_section",
    ),
)
async def test_ptg2_rejects_unsupported_provider_filter(provider_filter):
    with pytest.raises(
        serving.PTG2ProviderFilterUnsupportedError,
        match=provider_filter,
    ):
        await _search(args=_query_args(**{provider_filter: "Synthetic filter"}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("serving_tables", "query_overrides"),
    (
        (None, {"zip5": "60601"}),
        (_production_tables(), {"zip5": "60601", "negotiated_rate": "100"}),
        (_production_tables(), {"zip5": "60601", "order_by": "distance"}),
    ),
)
async def test_explicit_false_rejects_unsupported_geographic_provider_filter(
    serving_tables,
    query_overrides,
):
    """Return a client error before unsupported filtered coverage can fail."""

    with pytest.raises(
        serving.PTG2ProviderFilterScopeError,
        match="require an NPI or supported cost-ordered geographic scope",
    ):
        await _search(
            args=_query_args(
                include_providers="false",
                classification="Family Medicine",
                **query_overrides,
            ),
            serving_tables=serving_tables,
        )
