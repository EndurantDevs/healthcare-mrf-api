# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused OpenAPI contract for packed hospital-price responses."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def test_hospital_price_response_publishes_nested_contract():
    document = yaml.safe_load(OPENAPI_PATH.read_text())
    response_schema = document["paths"][
        "/hospital-prices/facilities/{hospital_id}/prices"
    ]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    properties = response_schema["properties"]

    assert set(properties["version"]["properties"]) >= {"version_id"}
    assert set(properties["query"]["properties"]) >= {
        "code_type",
        "code",
        "payer_name",
        "plan_name",
        "negotiated_prices_requested",
    }
    assert set(properties["pagination"]["properties"]) >= {
        "unit",
        "limit",
        "scanned",
    }
    item_properties = properties["items"]["items"]["properties"]
    assert set(item_properties) >= {"service", "charge", "negotiated_prices"}
    assert "codes" in item_properties["service"]["properties"]
