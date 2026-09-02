# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAPI contract for compact plan-pricing projection responses."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def _openapi_spec():
    return yaml.safe_load(OPENAPI_PATH.read_text())


def test_openapi_documents_projection_view_routing():
    spec = _openapi_spec()
    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        view_parameter = next(
            parameter for parameter in parameters if parameter.get("name") == "view"
        )
        assert view_parameter["schema"] == {
            "type": "string",
            "enum": ["full", "card"],
        }
        assert "default" not in view_parameter["schema"]
        description = " ".join(view_parameter["description"].split())
        assert "release-frozen factorized provider cards" in description
        assert "exact packed ZIP-cell rate aggregates" in description
        assert "With `view=card`, `include_providers=false`" in description
        assert "retain the existing aggregate reader" in description
        assert "Omitting `view` with explicit" in description
        assert "no ready projection" in description
        assert "Only explicit `view=card` requires" in description
        assert "`view=full` always preserves the existing PTG/TiC" in description


def test_openapi_documents_projection_response_shapes():
    spec = _openapi_spec()
    schemas = spec["components"]["schemas"]
    response = schemas["PricingProcedureProviderListResponse"]
    assert response["properties"]["plan_version_id"] == {
        "type": "string",
        "nullable": True,
        "pattern": "^hpversion_[0-9A-HJKMNP-TV-Z]{26}$",
    }
    assert response["properties"]["serving_revision_id"] == {
        "type": "string",
        "nullable": True,
        "pattern": "^hpserve_[0-9A-HJKMNP-TV-Z]{26}$",
    }
    assert response["properties"]["serving_revision_published_at"] == {
        "type": "string",
        "format": "date-time",
        "nullable": True,
        "pattern": (
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
            "[0-9]{2}\\.[0-9]{6}Z$"
        ),
    }
    assert response["properties"]["result_type"]["enum"] == [
        "provider_cards",
        "rate_aggregates",
    ]
    assert schemas["PricingProcedureProviderListResponse"]["properties"][
        "query"
    ]["properties"]["projection_contract"] == {
        "type": "string",
        "nullable": True,
        "enum": [
            "plan_pricing_card_v2",
            "plan_pricing_factorized_v3",
            "plan_pricing_em_distance_v1",
        ],
    }
    item_refs = {
        variant["$ref"]
        for variant in response["properties"]["items"]["items"]["anyOf"]
    }
    assert item_refs == {
        "#/components/schemas/PricingProcedureProviderRecord",
        "#/components/schemas/PricingProcedureProviderCardRecord",
        "#/components/schemas/PricingProcedureRateAggregateRecord",
    }
    assert "minimum_negotiated_rate" in schemas[
        "PricingProcedureProviderCardRecord"
    ]["properties"]
    assert schemas["PricingProcedureProviderCardRecord"]["properties"][
        "distance_miles"
    ] == {"type": "number", "minimum": 0}
    assert "distance_miles" in schemas["PricingProcedureProviderCardRecord"][
        "required"
    ]
    assert "median_negotiated_rate" in schemas[
        "PricingProcedureRateAggregateRecord"
    ]["properties"]
