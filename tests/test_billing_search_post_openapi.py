# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""OpenAPI parity and privacy contracts for exact billing-identity search."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from api.billing_search_access_contract import BILLING_SEARCH_CACHE_CONTROL
from api.billing_search_post_request import (
    BILLING_SEARCH_POST_DEFAULT_LIMIT,
    BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS,
    BILLING_SEARCH_POST_MAX_LIMIT,
    BILLING_SEARCH_POST_MAX_RADIUS_MILES,
)
from api.billing_search_response import (
    BILLING_SEARCH_ASSOCIATION_SCOPE,
    BILLING_SEARCH_EXACT_WITNESS_SCOPE,
    BILLING_SEARCH_GEO_SCOPE,
    BILLING_SEARCH_PRICING_SCOPE,
)
from api.ptg2_billing_search_contract import BILLING_SEARCH_RESULT_STATES

OPENAPI_PATH = Path("doc/openapi.yaml")
SEARCH_PATH = "/pricing/providers/search-by-procedure"
GET_OPERATION_SHA256 = (
    "19e2dc58284f912b7a818d554455b51025454645515207d0f62ac11cdb7d1922"
)


def _specification() -> dict[str, Any]:
    return yaml.safe_load(OPENAPI_PATH.read_text())


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _referenced_schema_names(value: object) -> set[str]:
    references: set[str] = set()
    pending_values = [value]
    while pending_values:
        current = pending_values.pop()
        if isinstance(current, dict):
            reference = current.get("$ref")
            if isinstance(reference, str) and reference.startswith(
                "#/components/schemas/"
            ):
                references.add(reference.rsplit("/", 1)[-1])
            pending_values.extend(current.values())
        elif isinstance(current, list):
            pending_values.extend(current)
    return references


def _response_schema_graph(specification: dict[str, Any]) -> dict[str, Any]:
    schemas = specification["components"]["schemas"]
    pending_names = ["BillingIdentityPricingSearchResponse"]
    retained_by_name: dict[str, Any] = {}
    while pending_names:
        schema_name = pending_names.pop()
        if schema_name in retained_by_name:
            continue
        retained_by_name[schema_name] = schemas[schema_name]
        pending_names.extend(_referenced_schema_names(schemas[schema_name]))
    return retained_by_name


def test_post_is_distinct_and_preserves_the_existing_get_contract() -> None:
    specification = _specification()
    path_item = specification["paths"][SEARCH_PATH]

    assert _canonical_sha256(path_item["get"]) == GET_OPERATION_SHA256
    assert set(path_item) == {"get", "post"}
    operation = path_item["post"]
    assert operation["operationId"] == (
        "postPricingProvidersSearchByProcedureByBillingIdentity"
    )
    assert "parameters" not in operation
    assert operation["requestBody"] == {
        "required": True,
        "content": {
            "application/json": {
                "schema": {
                    "$ref": (
                        "#/components/schemas/" "BillingIdentityPricingSearchRequest"
                    )
                }
            }
        },
    }


def test_post_request_is_closed_and_accepts_exactly_one_selector() -> None:
    schemas = _specification()["components"]["schemas"]
    request_schema = schemas["BillingIdentityPricingSearchRequest"]

    assert request_schema["additionalProperties"] is False
    assert set(request_schema["required"]) == {
        "healthporta_plan_id",
        "billing_identity",
        "procedure",
        "geo",
    }
    assert set(request_schema["properties"]) == {
        "healthporta_plan_id",
        "billing_identity",
        "procedure",
        "geo",
        "provider_npi",
        "include_evidence",
        "page",
    }
    selector = schemas["BillingIdentityPricingSelector"]
    assert selector["oneOf"] == [
        {"$ref": "#/components/schemas/BillingIdentityPricingTaxSelector"},
        {"$ref": ("#/components/schemas/" "BillingIdentityPricingReferenceSelector")},
    ]
    for selector_name, required_property in (
        ("BillingIdentityPricingTaxSelector", "tax_identity"),
        ("BillingIdentityPricingReferenceSelector", "billing_entity_ref"),
    ):
        selector_arm = schemas[selector_name]
        assert selector_arm["additionalProperties"] is False
        assert selector_arm["required"] == [required_property]
        assert set(selector_arm["properties"]) == {required_property}

    tax_identity = schemas["BillingIdentityPricingTaxIdentity"]
    assert tax_identity["additionalProperties"] is False
    assert set(tax_identity["required"]) == {"type", "value"}
    assert tax_identity["properties"]["type"]["enum"] == ["ein", "npi"]
    sensitive_value = tax_identity["properties"]["value"]
    assert sensitive_value["writeOnly"] is True
    assert {"example", "default"}.isdisjoint(sensitive_value)
    assert sensitive_value["minLength"] == 9
    assert sensitive_value["maxLength"] == 10


def test_post_request_bounds_match_the_python_parser_contract() -> None:
    schemas = _specification()["components"]["schemas"]
    request = schemas["BillingIdentityPricingSearchRequest"]["properties"]
    procedure = schemas["BillingIdentityPricingProcedureRequest"]["properties"]
    geo = schemas["BillingIdentityPricingGeoRequest"]["properties"]
    page = schemas["BillingIdentityPricingPageRequest"]["properties"]

    assert "checksum-valid NPI" in request["provider_npi"]["description"]
    assert procedure["modifiers"]["maxItems"] == 8
    assert procedure["place_of_service"]["maxItems"] == 16
    assert procedure["modifiers"]["uniqueItems"] is True
    assert procedure["place_of_service"]["uniqueItems"] is True
    assert geo["radius_miles"]["minimum"] == 0
    assert geo["radius_miles"]["maximum"] == (BILLING_SEARCH_POST_MAX_RADIUS_MILES)
    assert page["limit"] == {
        "type": "integer",
        "minimum": 1,
        "maximum": BILLING_SEARCH_POST_MAX_LIMIT,
        "default": BILLING_SEARCH_POST_DEFAULT_LIMIT,
    }
    assert page["cursor"]["maxLength"] == (BILLING_SEARCH_POST_MAX_CURSOR_CHARACTERS)
    assert request["include_evidence"]["default"] is False
    assert "provenance capability" in request["include_evidence"]["description"]


def test_post_response_matches_the_closed_response_shaper() -> None:
    schemas = _specification()["components"]["schemas"]
    response_schema = schemas["BillingIdentityPricingSearchResponse"]
    properties = response_schema["properties"]

    assert response_schema["additionalProperties"] is False
    assert set(response_schema["required"]) == set(properties)
    assert set(properties) == {
        "result_state",
        "pricing_scope",
        "billing_association_scope",
        "geo_match_scope",
        "resolved_release",
        "billing_identity",
        "procedure",
        "items",
        "pagination",
    }
    assert set(properties["result_state"]["enum"]) == BILLING_SEARCH_RESULT_STATES
    assert properties["pricing_scope"]["enum"] == [BILLING_SEARCH_PRICING_SCOPE]
    assert properties["billing_association_scope"]["enum"] == [
        BILLING_SEARCH_ASSOCIATION_SCOPE
    ]
    assert properties["geo_match_scope"]["enum"] == [BILLING_SEARCH_GEO_SCOPE]

    provider = schemas["BillingIdentityPricingProviderResult"]
    assert provider["additionalProperties"] is False
    assert set(provider["required"]) == set(provider["properties"]) - {
        "address_evidence"
    }
    assert provider["properties"]["billing_witness_scope"]["enum"] == [
        BILLING_SEARCH_EXACT_WITNESS_SCOPE
    ]
    site_match = schemas["BillingIdentityPricingSiteMatch"]["properties"]
    assert site_match["classification"]["enum"] == ["not_comparable"]
    assert site_match["confidence"]["enum"] == ["unknown"]

    rate_occurrence = schemas["BillingIdentityPricingRateOccurrence"]
    assert set(rate_occurrence["required"]) == set(rate_occurrence["properties"])
    assert rate_occurrence["properties"]["procedure"] == {
        "$ref": "#/components/schemas/BillingIdentityPricingRateProcedure"
    }
    assert properties["procedure"] == {
        "$ref": "#/components/schemas/BillingIdentityPricingProcedureResult"
    }


def test_post_statuses_are_explicit_generic_and_never_cacheable() -> None:
    specification = _specification()
    responses = specification["paths"][SEARCH_PATH]["post"]["responses"]

    assert set(responses) == {"200", "400", "404", "409", "503"}
    for status, response in responses.items():
        assert response["headers"]["Cache-Control"]["schema"]["enum"] == [
            BILLING_SEARCH_CACHE_CONTROL
        ]
        expected_schema = (
            "BillingIdentityPricingSearchResponse"
            if status == "200"
            else "BillingIdentityPricingSearchError"
        )
        assert response["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{expected_schema}"
        }
    assert "indistinguishable" in responses["404"]["description"]
    assert "pinned serving generation" in responses["409"]["description"]
    assert "unavailable" in responses["503"]["description"]

    error_schema = specification["components"]["schemas"][
        "BillingIdentityPricingSearchError"
    ]
    assert error_schema["additionalProperties"] is False
    assert error_schema["required"] == ["error"]
    assert set(error_schema["properties"]) == {"error"}
    assert error_schema["properties"]["error"] == {
        "$ref": "#/components/schemas/BillingIdentityPricingSearchErrorDetail"
    }
    error_detail = specification["components"]["schemas"][
        "BillingIdentityPricingSearchErrorDetail"
    ]
    assert error_detail["additionalProperties"] is False
    assert set(error_detail["required"]) == {"code", "message"}
    assert set(error_detail["properties"]["code"]["enum"]) == {
        "invalid_request",
        "resource_not_found",
        "cursor_generation_expired",
        "billing_search_unavailable",
    }


def test_response_graph_has_no_tax_identity_value_or_internal_witness_keys() -> None:
    specification = _specification()
    response_graph = _response_schema_graph(specification)
    forbidden_property_names = {
        "value",
        "tax_identity",
        "tax_identity_value",
        "tin",
        "tin_value",
        "masked_tin",
        "provider_group_ref",
        "provider_set_key",
        "source_record_id",
        "tin_hmac_sha256",
    }

    for schema in response_graph.values():
        assert forbidden_property_names.isdisjoint(schema.get("properties", {}))
        encoded_schema = json.dumps(schema, sort_keys=True)
        assert '"writeOnly"' not in encoded_schema
        assert '"example"' not in encoded_schema
        assert '"examples"' not in encoded_schema

    tax_value = specification["components"]["schemas"][
        "BillingIdentityPricingTaxIdentity"
    ]["properties"]["value"]
    assert tax_value["writeOnly"] is True
    billing_schema_by_name = {
        schema_name: schema
        for schema_name, schema in specification["components"]["schemas"].items()
        if schema_name.startswith("BillingIdentityPricing")
    }
    encoded_billing_schemas = json.dumps(billing_schema_by_name, sort_keys=True)
    assert '"example"' not in encoded_billing_schemas
    assert '"examples"' not in encoded_billing_schemas
