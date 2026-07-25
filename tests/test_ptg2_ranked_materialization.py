# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public parity contracts for bounded ranked-provider materialization."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import orjson
import pytest

from api import ptg2_response
from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


_PROVIDER_SET_IDS = ("11" * 16, "12" * 16)
_NPIS = (1234567890, 1234567891)
_PRICE_ROWS_BY_KEY = {
    1: [{"negotiated_rate": "100.00", "billing_class": "professional"}],
    2: [{"negotiated_rate": "300.00", "billing_class": "professional"}],
    3: [{"negotiated_rate": "50.00", "billing_class": "professional"}],
    4: [{"negotiated_rate": "200.00", "billing_class": "professional"}],
}


def _rate_row(
    rate_number: int,
    provider_set_index: int,
    source_key: int,
) -> dict[str, object]:
    return {
        "serving_content_hash_128": f"{30 + rate_number:02x}" * 16,
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "provider_set_global_id_128": _PROVIDER_SET_IDS[provider_set_index],
        "provider_count": 101,
        "price_set_global_id_128": f"{20 + rate_number:02x}" * 16,
        "price_key": rate_number,
        "source_key": source_key,
        "network_names": ["Stored network"],
        "_ptg_provider_set_key": provider_set_index + 3,
    }


def _selected_provider(selected_npi: int, provider_number: int):
    return {
        "npi": selected_npi,
        "provider_name": f"Synthetic provider {provider_number}",
        "address_payload": {
            "first_line": f"{provider_number} Test Way",
            "city": "Example City",
            "state": "IL",
            "address_sources": ["provider_directory_fhir"],
            "provider_directory_org_name": "Synthetic Health",
            "provider_directory_network_names": ["Hydrated network"],
            "provider_directory_network_matches": [
                {
                    "name": "Directory network",
                    "aliases": ["Hydrated Network", "Other alias"],
                    "resource_id": f"network-{provider_number}",
                }
            ],
            "address_verification_evidence": {
                "matched_on": "npi_address_key_plan"
            },
        },
        "taxonomy_codes": ["000000000X"],
        "specialties": ["Synthetic specialty"],
        "classifications": ["Synthetic classification"],
        "specializations": ["Synthetic specialization"],
    }


def _provider_rows(selected_npi: int, provider_number: int):
    return [
        _selected_provider(selected_npi, provider_number),
        *(
            {
                "npi": selected_npi + provider_number * 1_000 + offset,
                "provider_name": f"Unranked provider {offset}",
            }
            for offset in range(1, 101)
        ),
    ]


def _selection(descending: bool) -> serving._ProviderExpansionSelection:
    rate_coordinates = (
        ((2, 0, 7), (4, 1, 8), (1, 0, 7), (3, 1, 8))
        if descending
        else ((3, 1, 8), (1, 0, 7), (4, 1, 8), (2, 0, 7))
    )
    ranked_providers = (
        ((_NPIS[0], 7), (_NPIS[1], 8))
        if descending
        else ((_NPIS[1], 8), (_NPIS[0], 7))
    )
    return serving._ProviderExpansionSelection(
        row_data=[_rate_row(*coordinate) for coordinate in rate_coordinates],
        providers_by_set={
            provider_set_id: _provider_rows(_NPIS[index], index + 1)
            for index, provider_set_id in enumerate(_PROVIDER_SET_IDS)
        },
        rank_by_key={
            ("npi", str(npi), "CPT", "00001", "FFS", str(source_key)): rank
            for rank, (npi, source_key) in enumerate(ranked_providers)
        },
        exhausted=True,
    )


def _query_args(order: str) -> dict[str, object]:
    return {
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "code_system": "CPT",
        "code": "00001",
        "include_providers": True,
        "include_unverified_addresses": True,
        "order_by": "negotiated_rate",
        "order": order,
    }


async def _search(order: str, tables) -> dict[str, object]:
    code_row_by_field = {
        "code_key": 7,
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "rate_count": 4,
    }
    session = FakeSession([FakeResult([code_row_by_field])])
    return await serving._search_manifest_serving_table(
        session,
        "synthetic-snapshot",
        _query_args(order),
        SimpleNamespace(limit=2, offset=0),
        tables,
        serving.PTG2_MODE_PRODUCT_SEARCH,
    )


def _install_public_dependencies(monkeypatch, selector):
    async def hydrate_network_names(_session, _tables, rate_rows):
        for rate_row in rate_rows:
            rate_row["network_names"] = ["Hydrated network"]

    async def prices_for_sets(
        _session,
        _tables,
        price_set_ids,
        *,
        price_key_by_set_id,
    ):
        return {
            price_set_id: deepcopy(
                _PRICE_ROWS_BY_KEY[price_key_by_set_id[price_set_id]]
            )
            for price_set_id in price_set_ids
        }

    monkeypatch.setattr(serving, "_select_v4_provider_expansion", selector)
    monkeypatch.setattr(
        serving,
        "_v4_inferred_taxonomy_projection_rule",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        serving,
        "_version_three_explicit_npi_graph_scope",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        serving,
        "_hydrate_provider_set_network_names",
        hydrate_network_names,
    )
    monkeypatch.setattr(serving, "_prices_for_price_sets", prices_for_sets)
    monkeypatch.setattr(
        serving,
        "_procedure_details_for_rows",
        AsyncMock(return_value={}),
    )


def _assert_nested_directory_evidence(item):
    assert item["taxonomy_codes"] == ["000000000X"]
    verification = item["address_verification"]
    assert verification["address_evidence_level"] == (
        "payer_directory_network_location"
    )
    assert verification["address_sources"] == ["provider_directory_fhir"]
    assert verification["provider_directory_network_names"] == [
        "Hydrated network"
    ]
    assert verification["provider_directory_network_matches"][0][
        "provider_directory_network_name"
    ] == "Hydrated Network"
    assert verification["address_verification_evidence"]["matched_on"] == (
        "npi_address_key_plan"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("order", ["asc", "desc"])
async def test_ranked_public_output_is_identical_on_cache_miss_and_hit(
    monkeypatch,
    order,
):
    """Preserve nested evidence and exact price order without rebuilding fanout."""

    source_selection = _selection(order == "desc")
    untouched_selection = deepcopy(source_selection)
    selector = AsyncMock(return_value=source_selection)
    _install_public_dependencies(monkeypatch, selector)
    tables = strict_v3_tables(
        snapshot_id="synthetic-snapshot",
        storage_generation="shared_blocks_v4",
        shared_block_layout="packed_snapshot_maps_v4",
    )
    serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        miss_response = await _search(order, tables)
        project_provider_fields = serving._request_local_provider_fields
        counted_provider_fields = Mock(wraps=project_provider_fields)

        monkeypatch.setattr(
            serving,
            "_request_local_provider_fields",
            counted_provider_fields,
        )
        hit_response = await _search(order, tables)
    finally:
        serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert hit_response == miss_response
    assert selector.await_count == 1
    assert counted_provider_fields.call_count == 4
    assert source_selection == untouched_selection
    expected_npis = list(_NPIS) if order == "desc" else list(reversed(_NPIS))
    assert [
        provider_item["npi"]
        for provider_item in hit_response["items"]
    ] == expected_npis
    expected_prices_by_npi = {
        _NPIS[0]: [300.0, 100.0] if order == "desc" else [100.0, 300.0],
        _NPIS[1]: [200.0, 50.0] if order == "desc" else [50.0, 200.0],
    }
    for provider_item in hit_response["items"]:
        assert [
            price["negotiated_rate"]
            for price in provider_item["prices"]
        ] == (
            expected_prices_by_npi[provider_item["npi"]]
        )
        _assert_nested_directory_evidence(provider_item)
    wire_payload = ptg2_response._fragment_exact_numbers(hit_response)
    assert orjson.loads(orjson.dumps(wire_payload))["items"]
