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


@pytest.mark.asyncio
@pytest.mark.parametrize("page_sizes", [(12,), (25,), (50,), (50, 12, 25)])
async def test_combined_distance_pages_preserve_ties_sources_rates_and_scores(monkeypatch, page_sizes):
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

    async def read_networks(_session, networks, _args, pagination, _selection):
        return [
            (
                source,
                snapshot,
                {
                    "items": rows[: pagination.limit],
                    "pagination": {"total": len(rows), "total_is_exact": True},
                    "query": {"code": "99213"},
                },
            )
            for (source, snapshot), rows in zip(networks, network_rows)
        ]

    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", read_networks)
    networks = [(f"source-{index}", f"snapshot-{index}") for index in range(4)]
    args = {"lat": 40, "long": -80, "order_by": "distance", "order": "asc"}
    collected = []
    page_index = 0
    while True:
        page_size = page_sizes[page_index % len(page_sizes)]
        page_index += 1
        response = await serving._search_multi_ptg2_snapshots(
            object(), networks, args, SimpleNamespace(offset=len(collected), limit=page_size)
        )
        collected.extend(response["items"])
        assert response["pagination"]["total_is_exact"] is True
        if not response["pagination"]["has_more"]:
            break
    # Independent ordering includes every attributable source row and both prices.
    expected = sorted(
        (dict(row, network=row["source_key"]) for rows in network_rows for row in rows),
        key=lambda row: (row["distance_miles"], row["npi"], int(row["prices"][0]["negotiated_rate"])),
    )
    assert collected == expected
    assert len({(row["npi"], row["source_key"]) for row in collected}) == 244


@pytest.mark.asyncio
@pytest.mark.parametrize("page_sizes", [(50,), (25,), (12,), (50, 12, 25)])
async def test_complete_network_pages_keep_equal_distance_address_source_and_code_variant_ties(monkeypatch, page_sizes):
    network_rows = []
    for network in range(4):
        rows = []
        for provider in range(3):
            for address in range(5):
                for variant, arrangement in enumerate(("ffs", "capitation")):
                    rows.append(
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
        network_rows.append(rows)
    args = {"lat": 40, "long": -80, "order_by": "distance", "order": "asc"}
    networks = [(f"source-{network}", f"snapshot-{network}") for network in range(4)]

    async def read_networks(_session, selected, _args, pagination, _selection):
        responses = []
        for (source, snapshot), rows in zip(selected, network_rows):
            local = serving._sort_ptg2_manifest_provider_items(
                list(reversed(rows)), args, location_filter_requested=True
            )
            # The strict source response strips its private price/rank coordinates before union.
            public = [
                {key: value for key, value in row.items() if key != "_ptg_price_key"}
                for row in local[: pagination.limit]
            ]
            responses.append(
                (
                    source,
                    snapshot,
                    {
                        "items": public,
                        "pagination": {"total": len(rows), "total_is_exact": True},
                        "query": {"code": "99213"},
                    },
                )
            )
        return responses

    monkeypatch.setattr(serving, "_read_multi_ptg2_snapshots", read_networks)
    collected = []
    page = 0
    while True:
        result = await serving._search_multi_ptg2_snapshots(
            object(), networks, args, SimpleNamespace(offset=len(collected), limit=page_sizes[page % len(page_sizes)])
        )
        page += 1
        collected.extend(result["items"])
        if not result["pagination"]["has_more"]:
            break
    expected = sorted(
        (
            {**{key: value for key, value in row.items() if key != "_ptg_price_key"}, "network": row["source_key"]}
            for rows in network_rows
            for row in rows
        ),
        key=lambda row: (row["npi"], row["address"]["address_key"], row["negotiation_arrangement"], row["source_key"]),
    )
    assert collected == expected
    assert (
        len(
            {
                (row["npi"], row["address"]["address_key"], row["source_key"], row["negotiation_arrangement"])
                for row in collected
            }
        )
        == 120
    )
