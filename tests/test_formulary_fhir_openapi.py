# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen public contract for source-hidden FHIR formulary detail."""

from pathlib import Path

import yaml


OPENAPI_PATH = Path(__file__).parents[1] / "doc" / "openapi.yaml"
OPERATION_PATH = "/formulary/fhir/{formulary_id}"
PUBLIC_ID_SCHEMA = {
    "type": "string",
    "minLength": 31,
    "maxLength": 31,
    "pattern": "^fhir_[a-z2-7]{26}$",
}
DETAIL_FIELDS = {
    "formulary_id",
    "status",
    "title",
    "name",
    "period",
    "last_updated",
    "as_of",
    "published_at",
    "coverage",
}
FORBIDDEN_PUBLIC_FIELDS = {
    "source_id",
    "canonical_base",
    "dataset_id",
    "generation",
    "run_id",
    "coverage_hash",
    "membership_hash",
    "alias_id",
    "source_plan_identifier",
    "upstream_list_id",
    "upstream_version_id",
}


def _specification():
    return yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))


def _operation():
    return _specification()["paths"][OPERATION_PATH]["get"]


def test_fhir_formulary_detail_route_has_one_public_path_selector():
    specification = _specification()
    operation = specification["paths"][OPERATION_PATH]["get"]
    operation_ids = [
        candidate["operationId"]
        for path_item in specification["paths"].values()
        for candidate in path_item.values()
        if isinstance(candidate, dict) and "operationId" in candidate
    ]

    assert operation["operationId"] == "getFHIRFormularyDetail"
    assert operation_ids.count("getFHIRFormularyDetail") == 1
    assert operation["parameters"] == [
        {
            "name": "formulary_id",
            "in": "path",
            "required": True,
            "description": "Opaque public identifier for one FHIR formulary.",
            "schema": PUBLIC_ID_SCHEMA,
        }
    ]
    assert set(operation["responses"]) == {"200", "400", "404", "503"}


def test_fhir_formulary_detail_and_period_are_closed_and_fully_required():
    schemas = _specification()["components"]["schemas"]
    detail = schemas["FHIRFormularyDetail"]
    period = schemas["FHIRFormularyPeriod"]

    assert detail["type"] == "object"
    assert detail["additionalProperties"] is False
    assert set(detail["required"]) == DETAIL_FIELDS
    assert set(detail["properties"]) == DETAIL_FIELDS
    assert detail["properties"]["formulary_id"] == PUBLIC_ID_SCHEMA
    assert detail["properties"]["period"] == {
        "$ref": "#/components/schemas/FHIRFormularyPeriod"
    }
    assert period["type"] == "object"
    assert period["nullable"] is True
    assert period["additionalProperties"] is False
    assert set(period["required"]) == {"start", "end"}
    assert set(period["properties"]) == {"start", "end"}
    for field_name in ("start", "end"):
        assert period["properties"][field_name] == {
            "type": "string",
            "format": "date-time",
            "maxLength": 40,
            "nullable": True,
        }


def test_fhir_formulary_coverage_is_closed_nullable_and_arithmetic_bounded():
    schemas = _specification()["components"]["schemas"]
    coverage = schemas["FHIRFormularyCoverage"]

    assert coverage["type"] == "object"
    assert coverage["nullable"] is True
    assert coverage["additionalProperties"] is False
    assert set(coverage["required"]) == {
        "status",
        "expected_artifact_count",
        "included_artifact_count",
        "missing_artifact_count",
    }
    assert coverage["properties"]["status"]["enum"] == [
        "complete",
        "partial",
    ]
    for field_name in (
        "expected_artifact_count",
        "included_artifact_count",
    ):
        assert coverage["properties"][field_name]["minimum"] == 1
    assert coverage["properties"]["missing_artifact_count"]["minimum"] == 0
    assert schemas["FHIRFormularyDetail"]["properties"]["coverage"] == {
        "$ref": "#/components/schemas/FHIRFormularyCoverage"
    }


def test_fhir_formulary_detail_exposes_only_allowlisted_public_fields():
    operation = _operation()
    schemas = _specification()["components"]["schemas"]
    parameter_names = {parameter["name"] for parameter in operation["parameters"]}
    detail_fields = set(schemas["FHIRFormularyDetail"]["properties"])

    assert parameter_names == {"formulary_id"}
    assert detail_fields == DETAIL_FIELDS
    assert FORBIDDEN_PUBLIC_FIELDS.isdisjoint(parameter_names | detail_fields)
    assert "query" not in {parameter["in"] for parameter in operation["parameters"]}


def test_fhir_formulary_responses_are_private_and_exactly_typed():
    specification = _specification()
    operation = specification["paths"][OPERATION_PATH]["get"]
    expected_schema_by_status = {
        "200": "FHIRFormularyDetail",
        "400": "FHIRFormularyInvalidRequestResponse",
        "404": "FHIRFormularyNotFoundResponse",
        "503": "FHIRFormularyServingUnavailableResponse",
    }
    expected_header_by_field = {
        "$ref": "#/components/headers/FHIRFormularyCacheControl"
    }

    assert specification["components"]["headers"]["FHIRFormularyCacheControl"][
        "schema"
    ] == {"type": "string", "enum": ["private, no-store"]}
    for status, schema_name in expected_schema_by_status.items():
        documented_response = operation["responses"][status]
        if "$ref" in documented_response:
            response_name = documented_response["$ref"].rsplit("/", 1)[-1]
            documented_response = specification["components"]["responses"][
                response_name
            ]
        assert (
            documented_response["headers"]["Cache-Control"]
            == expected_header_by_field
        )
        assert documented_response["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{schema_name}"
        }


def test_fhir_formulary_errors_are_closed_and_status_specific():
    schemas = _specification()["components"]["schemas"]
    expected_error_by_response = {
        "FHIRFormularyNotFoundResponse": (
            "FHIRFormularyNotFoundError",
            "formulary_fhir_not_found",
            "FHIR formulary not found.",
        ),
        "FHIRFormularyServingUnavailableResponse": (
            "FHIRFormularyServingUnavailableError",
            "formulary_fhir_serving_unavailable",
            "FHIR formulary serving is temporarily unavailable.",
        ),
    }

    for response_name, (error_name, code, message) in expected_error_by_response.items():
        response_schema = schemas[response_name]
        assert response_schema == {
            "type": "object",
            "additionalProperties": False,
            "required": ["error"],
            "properties": {
                "error": {"$ref": f"#/components/schemas/{error_name}"}
            },
        }
        assert schemas[error_name] == {
            "type": "object",
            "additionalProperties": False,
            "required": ["code", "message"],
            "properties": {
                "code": {"type": "string", "enum": [code]},
                "message": {"type": "string", "enum": [message]},
            },
        }
