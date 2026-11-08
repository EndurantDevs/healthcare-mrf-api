# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The explicit provider evidence contract cannot describe participating offices."""

from pathlib import Path

from tests.openapi_route_contract_support import load_openapi_document


def test_provider_only_mode_is_list_only_and_keeps_office_response_branch():
    document = load_openapi_document(Path("doc/openapi.yaml"))
    operation = document["paths"]["/registry/serving/providers"]["get"]
    assert operation["operationId"] == "listRegistryNetworkProviders"
    mode = next(parameter for parameter in operation["parameters"] if parameter.get("name") == "membership_level")
    assert mode["schema"] == {"type": "string", "enum": ["office", "provider"], "default": "office"}
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "oneOf": [
            {"$ref": "#/components/schemas/RegistryNetworkProviderPage"},
            {"$ref": "#/components/schemas/RegistryProviderOnlyPage"},
        ]
    }
    detail = document["paths"]["/registry/serving/providers/{provider_system}/{provider_id}"]["get"]
    assert not any(parameter.get("name") == "membership_level" for parameter in detail["parameters"])


def test_provider_evidence_has_no_location_and_bounds_complete_native_evidence():
    schemas = load_openapi_document(Path("doc/openapi.yaml"))["components"]["schemas"]
    page = schemas["RegistryProviderOnlyPage"]
    identity = schemas["RegistryProviderOnlyIdentity"]
    membership = schemas["RegistryProviderOnlyMembership"]
    assert page["properties"]["membership_level"]["enum"] == ["provider"]
    assert set(identity["required"]) == {"provider_system", "provider_id", "display_name", "npi", "network_memberships"}
    assert set(identity["properties"]) == set(identity["required"])
    assert all(schema["additionalProperties"] is False for schema in (page, identity, membership))
    assert identity["properties"]["network_memberships"]["maxItems"] == 50
    evidence = membership["properties"]["evidence_ids"]
    assert (evidence["minItems"], evidence["maxItems"], evidence["uniqueItems"]) == (1, 1000, True)
    assert evidence["items"] == {"type": "string", "pattern": "^[0-9a-f]{64}$"}
    assert schemas["RegistryNetworkProvider"]["properties"]["locations"]["minItems"] == 1
