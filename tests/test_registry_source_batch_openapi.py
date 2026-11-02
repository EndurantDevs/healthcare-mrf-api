# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source batch documentation preserves compact commands and existing transport caps."""

from tests.openapi_route_contract_support import load_openapi_document
from tests.test_openapi_spec import OPENAPI_PATH, _contract_literal

CONTRACT = "process/registry_source_binding_publication.py"


def test_source_batch_route_preserves_control_auth_and_closed_operation_modes():
    operation = load_openapi_document(OPENAPI_PATH)["paths"]["/registry/publication/source-batch/{operation}"]["post"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert operation["parameters"] == [
        {
            "name": "operation",
            "in": "path",
            "required": True,
            "schema": {"type": "string", "enum": ["selection", "preview", "queue"]},
        }
    ]
    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"]["oneOf"] == [
        {"$ref": "#/components/schemas/RegistrySourceBatchPublicationCommand"},
        {"$ref": "#/components/schemas/RegistrySourceBatchSelectionRequest"},
    ]
    assert set(operation["responses"]) == {"200", "400", "403", "409", "503"}
    assert operation["responses"]["200"]["headers"]["Cache-Control"] == {
        "$ref": "#/components/headers/DirectoryCacheControl"
    }


def test_source_batch_schema_matches_compact_command_and_existing_page_limits():
    schemas = load_openapi_document(OPENAPI_PATH)["components"]["schemas"]
    command = schemas["RegistrySourceBatchPublicationCommand"]
    required = _contract_literal(CONTRACT, "_FIELDS") | {"actor", "session_token_sha256"}
    assert command["additionalProperties"] is False
    assert set(command["required"]) == required
    assert set(command["properties"]) == required | {"offset"}
    fields = command["properties"]
    assert fields["operation"]["enum"] == [_contract_literal(CONTRACT, "OPERATION")]
    assert fields["batch_references"]["maxItems"] == 100
    reference = fields["batch_references"]["items"]
    assert reference["additionalProperties"] is False
    assert (
        set(reference["required"])
        == set(reference["properties"])
        == {"idempotency_key", "request_sha256", "custom_revision"}
    )
    coordinates = fields["source_coordinates"]
    assert coordinates["additionalProperties"] is False
    assert (
        set(coordinates["required"])
        == set(coordinates["properties"])
        == {"source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id"}
    )
    assert coordinates["properties"]["source_system"]["enum"] == ["fhir"]
    maximum = _contract_literal(CONTRACT, "MAX_SELECTIONS")
    page_size = _contract_literal(CONTRACT, "PAGE_SIZE")
    page = schemas["RegistrySourceBatchSelectionPage"]
    assert page["additionalProperties"] is False
    assert set(page["required"]) == set(page["properties"])
    assert fields["selection_count"]["maximum"] == page["properties"]["selection_count"]["maximum"] == maximum
    assert fields["selection_count"]["nullable"] is fields["selection_sha256"]["nullable"] is True
    assert fields["offset"]["multipleOf"] == page["properties"]["selection"]["maxItems"] == page_size
    assert schemas["RegistryPublicationCommand"]["properties"]["selection"]["maxItems"] == page_size
    retained = schemas["RegistrySourceBatchSelectionRequest"]
    assert retained["additionalProperties"] is False
    assert set(retained["required"]) == {"request_id", "actor", "session_token_sha256"}
    assert set(retained["properties"]) == set(retained["required"]) | {"offset"}
    assert retained["properties"]["offset"]["maximum"] == maximum - page_size
    assert schemas["RegistrySourceBatchPreview"]["properties"]["records"]["maxItems"] == 0
