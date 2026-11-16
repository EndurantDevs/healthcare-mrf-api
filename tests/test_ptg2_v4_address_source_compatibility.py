# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stored-count filtering supports V4 snapshots without the optional projection."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api.endpoint.pagination import PaginationParams
from tests.test_pricing_address_source_review import _v3_rate_row
from tests.test_ptg2_serving import _strict_v4_tables

_NPIS = (1234567890, 1234567891, 1234567892)
_SET_IDS = {key: f"{key:032x}" for key in range(1, 5)}
_KEYS_BY_NPI = {_NPIS[0]: (1,), _NPIS[1]: (2, 3), _NPIS[2]: (4,)}


def _source_filter_rows():
    """Build offices and all prices for the same provider population."""
    offices = [
        {
            "npi": npi,
            "provider_name": f"Provider {npi}",
            "location_hash": f"entity_address_unified:{index}",
            "address_payload": {"first_line": f"{index} Example Road", "source_count": index},
        }
        for index, npi in enumerate(_NPIS, start=2)
    ]
    rates = [
        _v3_rate_row(_SET_IDS[key], f"{key + 16:032x}")
        | {"price_key": key, "_ptg_provider_set_key": key, "serving_content_hash_128": f"{key + 32:032x}"}
        for key in _SET_IDS
    ]
    return offices, rates


def _install_source_filter_reads(monkeypatch):
    """Exercise full result completion with deterministic office and rate reads."""
    offices, rates = _source_filter_rows()

    async def location_rows(_session, _tables, args, **options):
        assert options["candidate_npis"] is None
        minimum = int(args["min_address_sources"])
        return [office for office in offices if office["address_payload"]["source_count"] >= minimum][
            options.get("offset", 0) : options["limit"]
        ]

    locations = AsyncMock(side_effect=location_rows)
    monkeypatch.setattr(serving, "_membership_location_rows", locations)
    monkeypatch.setattr(serving, "_shared_rate_provider_set_keys", AsyncMock(return_value=tuple(_SET_IDS)))
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(side_effect=lambda _session, _tables, npis, **_options: {npi: _KEYS_BY_NPI[npi] for npi in npis}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(side_effect=lambda _session, _tables, keys: {key: _SET_IDS[key] for key in keys}),
    )
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(
            side_effect=lambda _session, _tables, ids: {value: key for key, value in _SET_IDS.items() if value in ids}
        ),
    )
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", AsyncMock(return_value=offices))
    monkeypatch.setattr(serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 7, "rate_count": 4}]))

    async def merge_rows(*_args, **options):
        assert options["limit"] is None and options["offset"] == 0
        return [row for row in rates if row["_ptg_provider_set_key"] in options["provider_set_keys"]]

    merged = AsyncMock(side_effect=merge_rows)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", merged)
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(
        serving,
        "_prices_for_price_sets",
        AsyncMock(return_value={f"{key + 16:032x}": [{"negotiated_rate": key * 10}] for key in _SET_IDS}),
    )
    return locations, merged


async def _search(tables, minimum, offset=0):
    query_parameters_by_name = {"plan_id": "SYNTHETIC-PLAN", "code_system": "CPT", "code": "27447"}
    if minimum is not None:
        query_parameters_by_name["min_address_sources"] = str(minimum)

    async def taxonomy_rows(statement, parameters):
        assert "SELECT source_npis.npi" in str(statement)
        assert "npi_taxonomy" in str(statement)
        assert set(parameters["npis"]) <= set(_NPIS)
        assert 0 < parameters["limit"] <= len(_NPIS)
        return [{"npi": npi} for npi in sorted(parameters["npis"])[: parameters["limit"]]]

    session = SimpleNamespace(execute=AsyncMock(side_effect=taxonomy_rows))
    return await serving._search_manifest_serving_table(
        session,
        tables.snapshot_id,
        query_parameters_by_name,
        PaginationParams(page=offset + 1, limit=1, offset=offset, source="offset"),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "minimum,offset,expected", [(3, 0, _NPIS[1]), (3, 1, _NPIS[2]), (4, 0, _NPIS[2]), (5, 0, None)]
)
async def test_missing_optional_projection_keeps_filtered_totals_pages_and_all_prices(
    monkeypatch, minimum, offset, expected
):
    locations, merged = _install_source_filter_reads(monkeypatch)
    tables = _strict_v4_tables()
    assert tables.provider_graph_v4_inferred_taxonomy_candidates is None
    response = await _search(tables, minimum, offset)
    assert response is not None
    assert response["pagination"]["total"] == max(5 - minimum, 0)
    assert response["pagination"]["total_is_exact"] is True
    assert [item["npi"] for item in response["items"]] == ([] if expected is None else [expected])
    assert locations.await_args.args[2]["min_address_sources"] == str(minimum)
    if expected is None:
        merged.assert_not_awaited()
    else:
        expected_keys = [2, 3, 4] if minimum == 3 else [4]
        assert sorted(merged.await_args.kwargs["provider_set_keys"]) == expected_keys
        item = response["items"][0]
        assert item["address"]["source_count"] >= minimum
        assert sorted(price["negotiated_rate"] for price in item["prices"]) == (
            [20, 30] if expected == _NPIS[1] else [40]
        )


@pytest.mark.asyncio
async def test_missing_optional_projection_preserves_exhaustive_scope_bound(monkeypatch):
    _install_source_filter_reads(monkeypatch)
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 1)
    with pytest.raises(serving.PTG2ManifestArtifactError, match="configured exactness bound"):
        await _search(_strict_v4_tables(), 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum,projection", [(None, None), (0, None), (3, {})])
async def test_default_and_projection_present_keep_strict_cost_routing(monkeypatch, minimum, projection):
    tables = replace(_strict_v4_tables(), provider_graph_v4_inferred_taxonomy_candidates=projection)
    monkeypatch.setattr(code_scope, "load_sealed_code_rows", AsyncMock(return_value=[{"code_key": 7, "rate_count": 4}]))
    selection = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_strict_cost_provider_expansion_selection", selection)
    locations = AsyncMock(side_effect=AssertionError("unchanged strict-cost routing"))
    monkeypatch.setattr(serving, "_ptg2_manifest_location_provider_matches", locations)
    assert await _search(tables, minimum) is None
    selection.assert_awaited_once()
    locations.assert_not_awaited()
