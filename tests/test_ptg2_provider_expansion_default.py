# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
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
async def test_omitted_expansion_matches_false_with_inferred_taxonomy(monkeypatch):
    omitted_response_by_field, omitted_location_matches = await _radiology_search(
        monkeypatch
    )
    false_response_by_field, false_location_matches = await _radiology_search(
        monkeypatch,
        "false",
    )

    for response_by_field, location_matches in (
        (omitted_response_by_field, omitted_location_matches),
        (false_response_by_field, false_location_matches),
    ):
        assert response_by_field["query"]["include_providers"] is False
        assert "npi" not in response_by_field["items"][0]
        filter_args_by_name = location_matches.await_args.args[2]
        assert serving._inferred_provider_taxonomy_rule(filter_args_by_name) is not None
        assert any(
            "npi_taxonomy" in clause
            for clause in serving._membership_taxonomy_filters(filter_args_by_name, {})
        )
    assert omitted_response_by_field == false_response_by_field


@pytest.mark.asyncio
async def test_explicit_true_expands_with_inferred_taxonomy(monkeypatch):
    response_by_field, location_matches = await _radiology_search(monkeypatch, "true")

    assert response_by_field["query"]["include_providers"] is True
    assert response_by_field["items"][0]["npi"] == _NPI
    assert serving._inferred_provider_taxonomy_rule(
        location_matches.await_args.args[2]
    ) is not None


@pytest.mark.asyncio
async def test_omitted_expansion_keeps_explicit_provider_filter(monkeypatch):
    _install_base_dependencies(monkeypatch)
    location_matches = AsyncMock(
        return_value=({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: [dict(_PROVIDER_ROW)]})
    )
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", location_matches)

    response_by_field, _session = await _search(
        args=_query_args(state="IL", classification="Family Medicine")
    )

    assert response_by_field["query"]["include_providers"] is False
    assert "npi" not in response_by_field["items"][0]
    assert location_matches.await_args.args[2]["classification"] == "Family Medicine"
    assert location_matches.await_args.kwargs["require_provider_set_coverage"] is True
