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
            "plan_pricing_factorized_v4",
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
    assert "median_negotiated_rate" in schemas[
        "PricingProcedureRateAggregateRecord"
    ]["properties"]


def test_openapi_card_allows_projection_specific_optional_fields():
    """Distance is lane-specific and coordinate cards may lack a ZIP."""

    card_schema = _openapi_spec()["components"]["schemas"][
        "PricingProcedureProviderCardRecord"
    ]
    assert card_schema["properties"]["distance_miles"] == {
        "type": "number",
        "minimum": 0,
    }
    assert "distance_miles" not in card_schema["required"]
    assert card_schema["properties"]["zip5"] == {
        "type": "string",
        "nullable": True,
        "pattern": "^[0-9]{5}$",
    }


def test_openapi_documents_bounded_v4_state_scan_contract():
    spec = _openapi_spec()
    operation = spec["paths"]["/pricing/providers/search-by-procedure"]["get"]
    description = " ".join(operation["description"].split())
    parameters_by_name = {
        parameter["name"]: parameter for parameter in operation["parameters"]
    }

    assert "release-bound state scan" in description
    assert "include_allowed_amounts=false" in description
    assert "immutable projection" in description
    assert "shorter complete NPI prefix" in description
    assert "single indivisible NPI" in description
    assert "may legitimately return no items" in description
    assert "256 rate occurrences" in description
    assert "256 emitted price atoms" in description
    assert "State alone is not a geographic distance anchor" in description
    assert parameters_by_name["cursor"]["schema"] == {
        "type": "string",
        "maxLength": 2048,
        "pattern": "^bsc1_[a-z0-9][a-z0-9-]{0,31}_[A-Za-z0-9_-]+$",
    }
    pagination = spec["components"]["schemas"]["PaginationMeta"]
    assert pagination["properties"]["scanned_npi_count"]["minimum"] == 0
    assert pagination["properties"]["next_cursor"]["maxLength"] == 2048
    assert operation["responses"]["422"]["content"]["application/json"][
        "schema"
    ] == {
        "$ref": "#/components/schemas/PlanPricingStateScanBudgetRefusal"
    }
    assert operation["responses"]["422"]["headers"]["Cache-Control"] == {
        "$ref": "#/components/headers/BillingSearchCacheControl"
    }
    refusal = spec["components"]["schemas"][
        "PlanPricingStateScanBudgetRefusal"
    ]
    assert refusal["required"] == ["status", "code", "message", "fix_it"]
    assert refusal["properties"]["code"]["enum"] == [
        "ptg2_online_work_budget_exceeded"
    ]
    assert refusal["properties"]["fix_it"]["properties"]["retry_options"][
        "maxItems"
    ] == 0
