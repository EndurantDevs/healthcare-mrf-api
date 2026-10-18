# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the completeness of network totals visible after a plan union."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving


@pytest.mark.asyncio
@pytest.mark.parametrize("exactness", [(True, True, True, True), (True, False, True, True), (True, None, True, True)])
async def test_combined_page_preserves_conservative_total_exactness(monkeypatch, exactness):
    responses = [
        (
            f"source-{index}",
            f"snapshot-{index}",
            {
                "items": [{"npi": 1234567890 + index, "distance_miles": 1.0}],
                "pagination": {"total": index + 1, "total_is_exact": exact},
                "query": {"code": "99213"},
            },
        )
        for index, exact in enumerate(exactness)
    ]
    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", AsyncMock(return_value=responses))
    response = await serving._search_multi_ptg2_snapshots(
        object(),
        [(source, snapshot) for source, snapshot, _ in responses],
        {"lat": 40, "long": -80, "order_by": "distance"},
        SimpleNamespace(offset=0, limit=2),
    )
    assert response["pagination"] == {
        "total": 10,
        "total_lower_bound": 10,
        "total_is_exact": all(exact is True for exact in exactness),
        "offset": 0,
        "limit": 2,
        "page": 1,
        "has_more": True,
    }


def _network_rows():
    network_rows = [
        [
            {
                "npi": 1234567800 + index,
                "provider_name": f"Provider {index}",
                "distance_miles": index // 3,
                "source_key": f"source-{network}",
                "address": {"formatted_address": f"{index} Example Street"},
                "prices": [{"negotiated_rate": str(100 + network)}, {"negotiated_rate": str(200 + network)}],
                "extensions": {"npi_scores": {"episodes": [{"episode_quality": index % 5}]}},
            }
            for index in range(61)
        ]
        for network in range(4)
    ]

    return network_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("page_sizes", [(12,), (25,), (50,), (50, 12, 25)])
async def test_combined_distance_pages_preserve_ties_sources_rates_and_scores(monkeypatch, page_sizes):
    network_rows = _network_rows()

    async def read_networks(_session, networks, _args, pagination, _selection):
        return [
            (
                source,
                snapshot,
                {
                    "items": provider_rows[: pagination.limit],
                    "pagination": {"total": len(provider_rows), "total_is_exact": True},
                    "query": {"code": "99213"},
                },
            )
            for (source, snapshot), provider_rows in zip(networks, network_rows)
        ]

    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", read_networks)
    networks = [(f"source-{index}", f"snapshot-{index}") for index in range(4)]
    args_by_name = {"lat": 40, "long": -80, "order_by": "distance", "order": "asc"}
    collected_rows = []
    page_index = 0
    while True:
        page_size = page_sizes[page_index % len(page_sizes)]
        page_index += 1
        response = await serving._search_multi_ptg2_snapshots(
            object(), networks, args_by_name, SimpleNamespace(offset=len(collected_rows), limit=page_size)
        )
        collected_rows.extend(response["items"])
        assert response["pagination"]["total_is_exact"] is True
        if not response["pagination"]["has_more"]:
            break
    # Independent ordering includes every attributable source row and both prices.
    expected = sorted(
        (
            dict(provider_row, network=provider_row["source_key"])
            for provider_rows in network_rows
            for provider_row in provider_rows
        ),
        key=lambda provider_row: (
            provider_row["distance_miles"],
            provider_row["npi"],
            int(provider_row["prices"][0]["negotiated_rate"]),
        ),
    )
    assert collected_rows == expected
    assert len({(provider_row["npi"], provider_row["source_key"]) for provider_row in collected_rows}) == 244


def _tied_network_rows():
    network_rows = []
    for network in range(4):
        provider_rows = []
        for provider in range(3):
            for address in range(5):
                for variant, arrangement in enumerate(("ffs", "capitation")):
                    provider_rows.append(
                        {
                            "npi": 1000000000 + provider,
                            "provider_name": f"Provider {provider}",
                            "distance_miles": 1.0,
                            "source_key": f"source-{network}",
                            "address": {
                                "address_key": f"address-{address:02}",
                                "formatted_address": f"{address} Example Street",
                            },
                            "reported_code_system": "CPT",
                            "reported_code": "99213",
                            "negotiation_arrangement": arrangement,
                            "prices": [{"negotiated_rate": "100"}, {"negotiated_rate": "200"}],
                            "extensions": {"npi_scores": {"episodes": [{"episode_quality": 4}]}},
                            "_ptg_price_key": network * 1000 + provider * 10 + address * 2 + variant,
                        }
                    )
        network_rows.append(provider_rows)
    return network_rows


def _tied_network_responses(network_rows, args_by_name, selected, pagination):
    responses = []
    for (source, snapshot), provider_rows in zip(selected, network_rows):
        local = serving._sort_ptg2_manifest_provider_items(
            list(reversed(provider_rows)), args_by_name, location_filter_requested=True
        )
        # The strict source response strips its private price/rank coordinates before union.
        public_rows = [
            {key: field_value for key, field_value in provider_row.items() if key != "_ptg_price_key"}
            for provider_row in local[: pagination.limit]
        ]
        responses.append(
            (
                source,
                snapshot,
                {
                    "items": public_rows,
                    "pagination": {"total": len(provider_rows), "total_is_exact": True},
                    "query": {"code": "99213"},
                },
            )
        )
    return responses


def _assert_tied_rows(network_rows, collected_rows):
    expected = sorted(
        (
            {
                **{key: field_value for key, field_value in provider_row.items() if key != "_ptg_price_key"},
                "network": provider_row["source_key"],
            }
            for provider_rows in network_rows
            for provider_row in provider_rows
        ),
        key=lambda provider_row: (
            provider_row["npi"],
            provider_row["address"]["address_key"],
            provider_row["negotiation_arrangement"],
            provider_row["source_key"],
        ),
    )
    assert collected_rows == expected
    assert (
        len(
            {
                (
                    provider_row["npi"],
                    provider_row["address"]["address_key"],
                    provider_row["source_key"],
                    provider_row["negotiation_arrangement"],
                )
                for provider_row in collected_rows
            }
        )
        == 120
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("page_sizes", [(50,), (25,), (12,), (50, 12, 25)])
async def test_distance_pages_preserve_address_source_and_code_ties(monkeypatch, page_sizes):
    network_rows = _tied_network_rows()
    args_by_name = {"lat": 40, "long": -80, "order_by": "distance", "order": "asc"}
    networks = [(f"source-{network}", f"snapshot-{network}") for network in range(4)]

    async def read_networks(_session, selected, _args, pagination, _selection):
        return _tied_network_responses(network_rows, args_by_name, selected, pagination)

    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", read_networks)
    collected_rows = []
    page = 0
    while True:
        response = await serving._search_multi_ptg2_snapshots(
            object(),
            networks,
            args_by_name,
            SimpleNamespace(offset=len(collected_rows), limit=page_sizes[page % len(page_sizes)]),
        )
        page += 1
        collected_rows.extend(response["items"])
        if not response["pagination"]["has_more"]:
            break
    _assert_tied_rows(network_rows, collected_rows)
