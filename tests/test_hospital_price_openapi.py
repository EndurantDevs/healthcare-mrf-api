# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused OpenAPI contract for packed hospital-price responses."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def test_hospital_facility_search_publishes_source_hidden_contract():
    document = yaml.safe_load(OPENAPI_PATH.read_text())
    operation = document["paths"]["/hospital-prices/facilities"]["get"]
    assert operation["operationId"] == "searchHospitalPriceFacilities"
    assert {parameter["name"] for parameter in operation["parameters"]} == {
        "q",
        "published",
        "cursor",
        "limit",
    }
    response_schema = operation["responses"]["200"]["content"][
        "application/json"
    ]["schema"]
    item = response_schema["properties"]["items"]["items"]
    assert set(item["required"]) == {
        "hospital_id",
        "alias_hospital_ids",
        "name",
        "publication",
    }
    assert set(item["properties"]) == set(item["required"])
    assert item["properties"]["publication"]["nullable"] is True
    assert "cms_hpt_url" not in str(item)
    assert "attempt" not in str(item)


def test_hospital_price_response_publishes_nested_contract():
    document = yaml.safe_load(OPENAPI_PATH.read_text())
    response_schema = document["paths"][
        "/hospital-prices/facilities/{hospital_id}/prices"
    ]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    properties = response_schema["properties"]

    assert set(response_schema["required"]) >= {
        "hospital_id",
        "version",
        "query",
        "pagination",
        "items",
    }
    assert set(properties["version"]["properties"]) >= {"version_id"}
    assert set(properties["version"]["required"]) >= {"version_id"}
    assert set(properties["query"]["properties"]) >= {
        "code_type",
        "code",
        "payer_name",
        "plan_name",
        "negotiated_prices_requested",
    }
    assert set(properties["query"]["required"]) >= {
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
    assert set(properties["pagination"]["required"]) >= {"unit", "limit", "scanned"}
    item_schema = properties["items"]["items"]
    assert set(item_schema["required"]) >= {
        "service",
        "charge",
        "negotiated_prices",
    }
    item_properties = item_schema["properties"]
    assert set(item_properties) >= {"service", "charge", "negotiated_prices"}
    assert "negotiated_rate_term" in item_properties["negotiated_prices"][
        "items"
    ]["properties"]
    service_schema = item_properties["service"]
    assert set(service_schema["required"]) >= {"codes"}
    assert "codes" in service_schema["properties"]
    assert set(service_schema["properties"]["codes"]["items"]["required"]) >= {
        "code_type",
        "code",
    }
