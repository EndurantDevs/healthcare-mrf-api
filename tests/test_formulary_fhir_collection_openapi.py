# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Frozen OpenAPI contract for source-hidden FHIR formulary collections."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path(__file__).parents[1] / "doc" / "openapi.yaml"
COLLECTION_PATH = "/formulary/fhir/"
ALIAS_PATH = "/formulary/fhir/{formulary_id}/aliases"
DRUG_PATH = ALIAS_PATH + "/{alias_id}/drugs"
DRUG_DETAIL_PATH = DRUG_PATH + "/{drug_id}"
FORMULARY_PATTERN = "^fhir_[a-z2-7]{26}$"
ALIAS_PATTERN = "^ffa_[0-9a-f]{48}$"
DRUG_PATTERN = "^ffm_[0-9a-f]{48}$"
FORBIDDEN_FIELDS = {
    "source_id",
    "dataset_id",
    "run_id",
    "generation",
    "alias_version_id",
    "source_plan_identifier",
    "upstream_medication_id",
    "upstream_list_id",
    "canonical_base",
    "coverage_hash",
    "membership_hash",
    "raw_reference",
    "corrected_reference",
    "rule_version",
    "evidence_json",
    "metadata_json",
    "codings_json",
}


def _specification():
    return yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))


def _parameter_names(operation, location):
    return {
        parameter["name"]
        for parameter in operation["parameters"]
        if parameter.get("in") == location
    }


def test_collection_operations_are_unique_and_have_exact_response_statuses():
    specification = _specification()
    expected_by_path = {
        COLLECTION_PATH: (
            "listFHIRFormularies",
            {"200", "400", "409", "503"},
        ),
        ALIAS_PATH: (
            "listFHIRFormularyAliases",
            {"200", "400", "404", "409", "503"},
        ),
        DRUG_PATH: (
            "listFHIRFormularyDrugs",
            {"200", "400", "404", "409", "503"},
        ),
        DRUG_DETAIL_PATH: (
            "getFHIRFormularyDrug",
            {"200", "400", "404", "503"},
        ),
    }
    all_operation_ids = [
        operation["operationId"]
        for path_item in specification["paths"].values()
        for operation in path_item.values()
        if isinstance(operation, dict) and "operationId" in operation
    ]

    for path, (operation_id, response_statuses) in expected_by_path.items():
        operation = specification["paths"][path]["get"]
        assert operation["operationId"] == operation_id
        assert all_operation_ids.count(operation_id) == 1
        assert set(operation["responses"]) == response_statuses


def test_collection_selectors_are_closed_and_public_only():
    specification = _specification()
    operation_by_path = {
        path: specification["paths"][path]["get"]
        for path in (COLLECTION_PATH, ALIAS_PATH, DRUG_PATH, DRUG_DETAIL_PATH)
    }

    assert _parameter_names(operation_by_path[COLLECTION_PATH], "path") == set()
    assert _parameter_names(operation_by_path[COLLECTION_PATH], "query") == {
        "cursor",
        "limit",
    }
    assert _parameter_names(operation_by_path[ALIAS_PATH], "path") == {
        "formulary_id"
    }
    assert _parameter_names(operation_by_path[ALIAS_PATH], "query") == {
        "cursor",
        "limit",
    }
    assert _parameter_names(operation_by_path[DRUG_PATH], "path") == {
        "alias_id",
        "formulary_id",
    }
    assert _parameter_names(operation_by_path[DRUG_PATH], "query") == {
        "cursor",
        "limit",
        "ndc11",
        "prior_authorization",
        "quantity_limit",
        "rxnorm_id",
        "step_therapy",
        "tier",
    }
    assert _parameter_names(operation_by_path[DRUG_DETAIL_PATH], "path") == {
        "alias_id",
        "drug_id",
        "formulary_id",
    }
    assert _parameter_names(operation_by_path[DRUG_DETAIL_PATH], "query") == set()
    all_parameter_names = {
        parameter["name"]
        for operation in operation_by_path.values()
        for parameter in operation["parameters"]
    }
    assert FORBIDDEN_FIELDS.isdisjoint(all_parameter_names)


def test_public_identifier_patterns_and_cursor_bounds_are_exact():
    specification = _specification()
    operations = [
        specification["paths"][path]["get"]
        for path in (COLLECTION_PATH, ALIAS_PATH, DRUG_PATH, DRUG_DETAIL_PATH)
    ]
    schemas_by_name = {
        parameter["name"]: parameter["schema"]
        for operation in operations
        for parameter in operation["parameters"]
        if parameter["name"] in {"formulary_id", "alias_id", "drug_id"}
    }

    assert schemas_by_name["formulary_id"] == {
        "type": "string",
        "minLength": 31,
        "maxLength": 31,
        "pattern": FORMULARY_PATTERN,
    }
    assert schemas_by_name["alias_id"] == {
        "type": "string",
        "minLength": 52,
        "maxLength": 52,
        "pattern": ALIAS_PATTERN,
    }
    assert schemas_by_name["drug_id"] == {
        "type": "string",
        "minLength": 52,
        "maxLength": 52,
        "pattern": DRUG_PATTERN,
    }
    for operation in operations[:3]:
        cursor_parameters = [
            parameter
            for parameter in operation["parameters"]
            if parameter["name"] == "cursor"
        ]
        if cursor_parameters:
            assert cursor_parameters == [
                {
                    "name": "cursor",
                    "in": "query",
                    "description": (
                        "Opaque continuation returned by this exact operation."
                    ),
                    "schema": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 512,
                    },
                }
            ]


def test_collection_payload_schemas_are_closed_bounded_and_source_hidden():
    schemas = _specification()["components"]["schemas"]
    expected_fields_by_schema = {
        "FHIRFormularyPage": {"items", "next_cursor"},
        "FHIRFormularyAlias": {
            "formulary_id",
            "alias_id",
            "drug_count",
            "coverage",
        },
        "FHIRFormularyAliasPage": {"items", "next_cursor"},
        "FHIRFormularyAlternatives": {
            "resolved_drug_ids",
            "unresolved_count",
        },
        "FHIRFormularyDrug": {
            "formulary_id",
            "alias_id",
            "drug_id",
            "status",
            "name",
            "rxnorm_id",
            "ndc11",
            "last_updated",
            "tier",
            "prior_authorization",
            "step_therapy",
            "quantity_limit",
            "alternatives",
            "coverage",
        },
        "FHIRFormularyDrugPage": {"items", "next_cursor"},
    }

    for schema_name, expected_fields in expected_fields_by_schema.items():
        schema = schemas[schema_name]
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == expected_fields
        assert set(schema["properties"]) == expected_fields
        assert FORBIDDEN_FIELDS.isdisjoint(expected_fields)
    for page_name in (
        "FHIRFormularyPage",
        "FHIRFormularyAliasPage",
        "FHIRFormularyDrugPage",
    ):
        assert schemas[page_name]["properties"]["items"]["maxItems"] == 100
    alternatives = schemas["FHIRFormularyAlternatives"]["properties"]
    assert alternatives["resolved_drug_ids"]["maxItems"] == 100
    assert alternatives["resolved_drug_ids"]["uniqueItems"] is True
    assert alternatives["unresolved_count"]["maximum"] == 100
    assert "version-scoped" in schemas["FHIRFormularyDrug"]["description"]


def test_collection_success_responses_reference_only_closed_public_schemas():
    specification = _specification()
    expected_schema_by_path = {
        COLLECTION_PATH: "FHIRFormularyPage",
        ALIAS_PATH: "FHIRFormularyAliasPage",
        DRUG_PATH: "FHIRFormularyDrugPage",
        DRUG_DETAIL_PATH: "FHIRFormularyDrug",
    }

    for path, schema_name in expected_schema_by_path.items():
        response = specification["paths"][path]["get"]["responses"]["200"]
        assert response["headers"]["Cache-Control"] == {
            "$ref": "#/components/headers/FHIRFormularyCacheControl"
        }
        assert response["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{schema_name}"
        }
