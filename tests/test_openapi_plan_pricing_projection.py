# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAPI contract for compact plan-pricing projection responses."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def test_openapi_documents_card_and_aggregate_projection_shapes():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
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
            "default": "full",
        }
        description = " ".join(view_parameter["description"].split())
        assert "pre-rendered provider cards" in description
        assert "With `view=card`, `include_providers=false`" in description
        assert "retain the existing aggregate reader" in description
        assert "`view=full` preserves the existing PTG/TiC response" in description

    schemas = spec["components"]["schemas"]
    response = schemas["PricingProcedureProviderListResponse"]
    assert response["properties"]["result_type"]["enum"] == [
        "provider_cards",
        "rate_aggregates",
    ]
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
    assert "median_negotiated_rate" in schemas[
        "PricingProcedureRateAggregateRecord"
    ]["properties"]
