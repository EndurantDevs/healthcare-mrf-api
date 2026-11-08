# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen public contract for the exact billing-identity GET branch."""

from pathlib import Path

import yaml

OPENAPI_PATH = Path(__file__).parents[1] / "doc" / "openapi.yaml"
OPERATION_PATH = "/pricing/providers/search-by-procedure"
BILLING_OBJECT_SCHEMAS = {
    "BillingSearchResponse": {
        "result_state",
        "pricing_scope",
        "billing_association_scope",
        "geo_match_scope",
        "plan_release_id",
        "billing_entity_ref",
        "procedure",
        "items",
        "pagination",
    },
    "BillingSearchProcedure": {
        "code_system",
        "code",
        "modifiers",
        "place_of_service",
    },
    "BillingSearchProvider": {
        "npi",
        "billing_entity_ref",
        "address",
        "distance_miles",
        "rate_occurrences",
    },
    "BillingSearchAddress": {
        "address_kind",
        "first_line",
        "second_line",
        "city",
        "state",
        "postal_code",
        "country_code",
    },
    "BillingSearchAddressEvidence": {
        "evidence_level",
        "selection_contract",
        "sources",
    },
    "BillingSearchAddressEvidenceSource": {"dataset", "retrieved_at"},
    "BillingSearchRateOccurrence": {
        "occurrence_ordinal",
        "billing_entity_ref",
        "procedure",
        "prices",
    },
    "BillingSearchRateProcedure": {
        "code_system",
        "code",
        "negotiation_arrangement",
        "billing_code_type_version",
    },
    "BillingSearchNegotiatedPrice": {
        "negotiated_rate",
        "negotiated_type",
        "expiration_date",
        "service_code",
        "billing_class",
        "setting",
        "billing_code_modifier",
        "additional_information",
    },
    "BillingSearchPagination": {"limit", "has_more", "next_cursor"},
    "BillingSearchErrorResponse": {"error"},
    "BillingSearchError": {"code", "message"},
    "PricingProcedureProviderBudgetErrorResponse": {"error"},
    "PricingProcedureProviderBudgetError": {"code", "message", "dimension"},
}


def _specification():
    return yaml.safe_load(OPENAPI_PATH.read_text())


def _operation():
    return _specification()["paths"][OPERATION_PATH]["get"]


def test_exact_billing_selector_and_cursor_are_canonical_path_only():
    specification = _specification()
    operation = specification["paths"][OPERATION_PATH]["get"]
    parameters_by_name = {parameter["name"]: parameter for parameter in operation["parameters"]}

    assert parameters_by_name["billing_entity_ref"]["schema"] == {
        "type": "string",
        "pattern": "^be1_[A-Za-z0-9_-]{64}$",
    }
    assert parameters_by_name["cursor"]["schema"] == {
        "type": "string",
        "maxLength": 2048,
        "pattern": "^bsc1_[a-z0-9][a-z0-9-]{0,31}_[A-Za-z0-9_-]+$",
    }
    assert parameters_by_name["healthporta_plan_id"]["schema"] == {
        "type": "string",
        "pattern": "^hpplan_[0-9A-HJKMNP-TV-Z]{26}$",
    }
    for alias_path in (
        "/pricing/providers/by-procedure",
        "/pricing/providers/by-service",
        "/pricing/physicians/by-service",
    ):
        alias_operation = specification["paths"].get(alias_path, {}).get("get")
        if alias_operation is None:
            continue
        assert {"billing_entity_ref", "cursor", "healthporta_plan_id"}.isdisjoint(
            parameter["name"] for parameter in alias_operation["parameters"]
        )


def test_exact_billing_operation_documents_closed_conditional_contract():
    operation = _operation()
    description = operation["description"]
    normalized_description = " ".join(description.split())
    parameter_names = {parameter["name"] for parameter in operation["parameters"]}

    for required_name in (
        "billing_entity_ref",
        "healthporta_plan_id",
        "code_system",
        "code",
        "limit",
    ):
        assert f"`{required_name}`" in description
    assert "requires" in description
    assert "`limit` from 1 through 100" in normalized_description
    assert "exactly one GEO form" in normalized_description
    assert "raw tax identifiers" in normalized_description
    assert "client-supplied `plan_release_id`" in normalized_description
    assert "resolves the plan" in normalized_description
    assert "allowed-amount/CMS fallback is disabled" in normalized_description
    assert {"tin", "tax_identity", "search_by"}.isdisjoint(parameter_names)


def test_exact_billing_documents_evidence_and_cursor_privacy_boundaries():
    operation = _operation()
    parameters_by_name = {parameter["name"]: parameter for parameter in operation["parameters"]}
    evidence_description = " ".join(parameters_by_name["include_evidence"]["description"].split())
    cursor_description = " ".join(parameters_by_name["cursor"]["description"].split())
    bad_request_description = " ".join(operation["responses"]["400"]["description"].split())
    not_found_description = " ".join(operation["responses"]["404"]["description"].split())

    assert "only the dataset identifier and retrieval timestamp" in (evidence_description)
    assert "source record/version identifiers and URLs remain internal" in (evidence_description)
    assert "syntactically valid sealed cursor" in cursor_description
    assert "syntactically valid sealed billing cursor" in bad_request_description
    assert "malformed cursor syntax" in not_found_description


def test_exact_billing_success_and_error_responses_are_explicit():
    responses = _operation()["responses"]

    assert responses["200"]["content"]["application/json"]["schema"] == {
        "oneOf": [
            {
                "allOf": [
                    {"$ref": ("#/components/schemas/PricingProcedureProviderListResponse")},
                    {"not": {"required": ["billing_association_scope"]}},
                ]
            },
            {"$ref": "#/components/schemas/BillingSearchResponse"},
        ]
    }
    assert responses["400"]["content"]["application/json"]["schema"] == {
        "oneOf": [
            {"$ref": "#/components/schemas/Error"},
            {"$ref": "#/components/schemas/BillingSearchErrorResponse"},
        ]
    }
    expected_error_code_by_status = {
        "400": "billing_search_cursor_invalid",
        "404": "resource_not_found",
        "409": "billing_search_cursor_generation_expired",
        "503": "billing_search_serving_unavailable",
    }
    for status, expected_code in expected_error_code_by_status.items():
        response = responses[status]
        assert response["headers"]["Cache-Control"] == {"$ref": "#/components/headers/BillingSearchCacheControl"}
        media_type = response["content"]["application/json"]
        example = (
            media_type["examples"]["billingCursorInvalid"]["value"]
            if status == "400"
            else (
                media_type["examples"]["billingServingUnavailable"]["value"]
                if status == "503"
                else media_type["example"]
            )
        )
        assert example["error"]["code"] == expected_code
    assert responses["200"]["headers"]["Cache-Control"] == {"$ref": "#/components/headers/BillingSearchCacheControl"}


def test_exact_billing_object_schemas_are_closed_and_required_keys_are_frozen():
    schemas = _specification()["components"]["schemas"]

    for schema_name, required_fields in BILLING_OBJECT_SCHEMAS.items():
        schema = schemas[schema_name]
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == required_fields
        assert required_fields.issubset(schema["properties"])

    assert "address_evidence" in schemas["BillingSearchProvider"]["properties"]
    assert "address_evidence" not in schemas["BillingSearchProvider"]["required"]
    assert schemas["BillingSearchAddressEvidenceSource"]["properties"]["retrieved_at"]["format"] == "date-time"
    assert schemas["BillingSearchAddressEvidenceSource"]["properties"]["retrieved_at"]["maxLength"] == 64
    assert set(schemas["BillingSearchAddressEvidenceSource"]["properties"]["dataset"]["enum"]) == {
        "cms_nppes_registry",
        "marketplace_provider_directory",
        "cms_doctors_and_clinicians",
        "cms_provider_enrollment_ffs",
        "cms_provider_enrollment_facility",
        "facility_reference",
        "payer_transparency_in_coverage",
        "payer_provider_directory_fhir",
    }
    negotiated_price_properties = schemas["BillingSearchNegotiatedPrice"]["properties"]
    assert negotiated_price_properties["service_code"]["uniqueItems"] is True
    assert negotiated_price_properties["billing_code_modifier"]["uniqueItems"] is True
    assert schemas["BillingSearchProvider"]["properties"]["npi"] == {
        "type": "integer",
        "minimum": 1000000000,
        "maximum": 2999999999,
        "description": ("Checksum-valid NPI retained through the exact billing-group witness."),
    }


def test_exact_billing_result_states_and_error_codes_are_frozen():
    specification = _specification()
    schemas = specification["components"]["schemas"]

    assert set(schemas["BillingSearchResponse"]["properties"]["result_state"]["enum"]) == {
        "matched",
        "no_matching_tax_identity",
        "tax_identity_unavailable_for_snapshot",
        "no_matching_rates",
        "no_match_in_radius",
        "no_snapshot_for_plan",
    }
    assert set(schemas["BillingSearchError"]["properties"]["code"]["enum"]) == {
        "billing_search_cursor_invalid",
        "resource_not_found",
        "billing_search_cursor_generation_expired",
        "billing_search_serving_unavailable",
        "pricing_projection_unavailable",
    }
    assert specification["components"]["headers"]["BillingSearchCacheControl"]["schema"]["enum"] == [
        "private, no-store"
    ]


def test_exact_billing_success_is_disjoint_from_the_open_legacy_schema():
    success_schema = _operation()["responses"]["200"]["content"]["application/json"]["schema"]
    legacy_exclusion = success_schema["oneOf"][0]["allOf"][1]
    billing_required_fields = set(_specification()["components"]["schemas"]["BillingSearchResponse"]["required"])

    assert legacy_exclusion == {"not": {"required": ["billing_association_scope"]}}
    assert "billing_association_scope" in billing_required_fields


def test_shared_503_preserves_a_disjoint_legacy_budget_error():
    specification = _specification()
    unavailable_media_type = _operation()["responses"]["503"]["content"]["application/json"]

    assert unavailable_media_type["schema"] == {
        "oneOf": [
            {"$ref": "#/components/schemas/BillingSearchErrorResponse"},
            {"$ref": ("#/components/schemas/PricingProcedureProviderBudgetErrorResponse")},
        ]
    }
    legacy_example = unavailable_media_type["examples"]["legacyOnlineWorkBudgetExceeded"]["value"]
    assert legacy_example["error"] == {
        "code": "ptg2_online_work_budget_exceeded",
        "message": ("The exact query exceeds this snapshot's sealed online work budget."),
        "dimension": "candidate_members",
    }
    billing_codes = set(specification["components"]["schemas"]["BillingSearchError"]["properties"]["code"]["enum"])
    legacy_codes = set(
        specification["components"]["schemas"]["PricingProcedureProviderBudgetError"]["properties"]["code"]["enum"]
    )
    assert billing_codes.isdisjoint(legacy_codes)


def test_imported_billing_route_preserves_both_proofs_and_native_shape():
    specification = _specification()
    operation = specification["paths"]["/extensions/custom-import/providers/billing-search"]["post"]
    parameters = [
        specification["components"]["parameters"][parameter["$ref"].rsplit("/", 1)[-1]]
        if "$ref" in parameter
        else parameter
        for parameter in operation["parameters"]
    ]
    assert {parameter["name"] for parameter in parameters} == {
        f"X-HealthPorta-{family}-{suffix}"
        for family in ("Extension-Read", "Billing-Search")
        for suffix in ("Context", "Key-Id", "Signature")
    }
    assert all(parameter["in"] == "header" and parameter["required"] for parameter in parameters)
    schemas = specification["components"]["schemas"]
    request = schemas["CustomImportBillingSearchRequest"]
    assert request["additionalProperties"] is False
    assert set(request["required"]) == set(schemas["CustomImportProviderRequest"]["required"]) | {
        "include_filter",
        "billing_transport_context_sha256",
    }
    assert request["properties"]["native_query"]["additionalProperties"]["type"] == "string"
    assert request["properties"]["billing_transport_context_sha256"]["pattern"] == "^[0-9a-f]{64}$"
    assert schemas["CustomImportBillingSearchResponse"]["required"] == schemas["BillingSearchResponse"]["required"]
    provider_schema = schemas["CustomImportBillingSearchProvider"]
    assert provider_schema["additionalProperties"] is False
    assert provider_schema["required"] == schemas["BillingSearchProvider"]["required"]
    assert provider_schema["properties"]["custom_import"] == {
        "$ref": "#/components/schemas/CustomImportProviderResult/properties/custom_import"
    }
    assert "custom_import" not in provider_schema["required"]
    assert provider_schema["properties"]["rate_occurrences"] == {
        "$ref": "#/components/schemas/BillingSearchProvider/properties/rate_occurrences"
    }


def test_grouped_billing_query_projection_matches_shared_import_schema():
    from api import custom_import_billing_http as billing
    from api import custom_import_provider_http as provider
    from api import custom_import_read_http as transport
    from process.custom_import import grouped_read, read_core
    from tests import custom_import_grouped_support as grouped
    from tests.custom_import_billing_support import signed_request

    parsed = billing._parse_billing_request(
        signed_request(body_changes={"grouped_entity_selection": grouped.selection_document()}).body
    )
    assert parsed.provider.include_filter and parsed.provider.family_entitlement is None
    plan = grouped_read.normalize_plan(
        grouped.context(),
        provider._provider_relation_query(parsed.provider),
        read_core.ExtensionReadScope("synthetic:grouped"),
    )
    family = read_core.SearchItem(
        None,
        (read_core.ReadFieldValue("period", "integer", "value", 2024),),
        None,
        (read_core.ReadFieldValue("metric", "integer", "value", 4),),
    )
    imported = read_core.EntityFamilySet(
        None, plan.projection, "period", 2024, (("segment_a", family),), ("segment_b",), plan.fingerprint, "a" * 64
    )
    family_set_payload = transport._family_set_payload(imported, parsed.target)
    schemas = _specification()["components"]["schemas"]
    alternatives = schemas["CustomImportProviderResult"]["properties"]["custom_import"]["oneOf"]
    assert {"$ref": "#/components/schemas/CustomImportGroupedQueryProjectionSet"} in alternatives
    query_schema = schemas["CustomImportGroupedQueryProjectionSet"]
    assert query_schema["additionalProperties"] is False
    assert set(family_set_payload) == set(query_schema["required"]) == set(query_schema["properties"])
    assert family_set_payload["projection"] == "query_projection"
    assert query_schema["properties"]["projection"]["enum"] == [family_set_payload["projection"]]
    family_schema = query_schema["properties"]["families"]["items"]
    assert family_schema["additionalProperties"] is False
    assert set(family_set_payload["families"][0]) == set(family_schema["required"]) == set(family_schema["properties"])
    assert family_schema["properties"]["root_fields"] == {
        "$ref": "#/components/schemas/CustomImportReadItem/properties/root_fields"
    }
    assert family_schema["properties"]["context_fields"] == {
        "$ref": "#/components/schemas/CustomImportReadItem/properties/context_fields"
    }
    complete = schemas["CustomImportGroupedFullFamilySet"]["properties"]
    assert complete["projection"]["enum"] == ["full_family"]
    assert "children" in complete["families"]["items"]["required"]
    assert "context_fields" not in complete["families"]["items"]["properties"]
