from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_code_details


def _catalog_detail_rows():
    return (
        {
            "code_system": "CPT",
            "code": "00100",
            "display_name": "Procedure",
            "short_description": "Procedure detail",
        },
        {
            "code_system": "POS",
            "code": "11",
            "display_name": "Office",
            "short_description": "Office setting",
        },
        {
            "code_system": "MODIFIER",
            "code": "25",
            "display_name": "Separate service",
            "short_description": "Modifier detail",
        },
    )


def _response_with_code_detail_candidates():
    return {
        "items": [
            {
                "reported_code_system": "CPT",
                "reported_code": "00100",
                "prices": [
                    {
                        "negotiated_rate": "10.00",
                        "service_code": ["11", "99", ""],
                        "billing_code_modifier": ["25", "XX", ""],
                    },
                    {"service_code": [], "billing_code_modifier": []},
                ],
            },
            {
                "billing_code_type": "HCPCS",
                "billing_code": "A0001",
                "prices": None,
            },
            {"prices": []},
        ],
        "query": {"code": "00100"},
    }


@pytest.mark.asyncio
async def test_code_details_skip_unrequested_and_empty_catalog_scopes():
    session = SimpleNamespace(execute=AsyncMock())
    response_by_field = {"items": [{"prices": []}]}

    assert await ptg2_code_details._enrich_ptg2_code_details(
        session,
        response_by_field,
        {},
    ) is response_by_field
    assert await ptg2_code_details._enrich_ptg2_code_details(
        session,
        response_by_field,
        {"include_code_details": "true"},
    ) is response_by_field
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_code_details_enrich_catalog_hits_and_explicit_misses():
    session = SimpleNamespace(
        execute=AsyncMock(return_value=_catalog_detail_rows())
    )
    response_by_field = _response_with_code_detail_candidates()

    enriched = await ptg2_code_details._enrich_ptg2_code_details(
        session,
        response_by_field,
        {"include_code_details": "yes"},
    )

    first_item = enriched["items"][0]
    assert first_item["billing_code_detail"]["display_name"] == "Procedure"
    assert first_item["prices"] is first_item["tic_prices"]
    assert first_item["prices"][0]["service_code_details"] == [
        {
            "code_system": "POS",
            "code": "11",
            "display_name": "Office",
            "short_description": "Office setting",
        }
    ]
    assert [
        detail["code"]
        for detail in first_item["prices"][0][
            "billing_code_modifier_details"
        ]
    ] == ["25", "XX"]
    assert first_item["prices"][0]["billing_code_modifier_details"][1][
        "catalog_status"
    ] == "missing"
    assert "service_code_details" not in first_item["prices"][1]
    assert "billing_code_modifier_details" not in first_item["prices"][1]
    assert "billing_code_detail" not in enriched["items"][1]
    assert enriched["items"][1]["prices"] == []
    assert enriched["items"][2]["price_summary"] == []
    assert "billing_code_detail" not in response_by_field["items"][0]
    assert session.execute.await_count == 1
    query_params = session.execute.await_args.args[1]
    assert set(query_params.values()) == {
        "00100",
        "11",
        "25",
        "99",
        "A0001",
        "CPT",
        "HCPCS",
        "MODIFIER",
        "POS",
        "XX",
    }
