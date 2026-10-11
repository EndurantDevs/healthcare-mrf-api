# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Company search retains a bounded, explicitly scoped OpenAPI contract."""

import re

from tests.openapi_route_contract_support import load_openapi_document
from tests.test_openapi_spec import OPENAPI_PATH


def test_company_search_openapi_preserves_complete_scope_and_bounds():
    """Keep the dedicated trusted search contract separate from legacy writes."""
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"]["/registry/manage/company/search"]["post"]
    schemas = spec["components"]["schemas"]
    request_schema = schemas["RegistryCompanySearchRequest"]
    properties = request_schema["properties"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert not operation.get("parameters")
    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/RegistryCompanySearchRequest"
    }
    assert request_schema["additionalProperties"] is False
    assert request_schema["required"] == ["q", "limit", "offset", "record_ids"]
    assert set(properties) == set(request_schema["required"])
    assert properties["q"]["minLength"] == 1
    assert "maxLength" not in properties["q"]
    assert "128 UTF-8 bytes" in " ".join(properties["q"]["description"].split())
    assert properties["limit"] == {"type": "integer", "minimum": 1, "maximum": 100}
    assert properties["offset"] == {"type": "integer", "minimum": 0, "maximum": 1000000}
    selector = properties["record_ids"]
    assert selector["type"] == "array" and selector["nullable"] is True
    assert selector["maxItems"] == 5000 and selector["uniqueItems"] is True
    assert "[] remains empty" in " ".join(selector["description"].split())
    assert re.fullmatch(selector["items"]["pattern"], "12345678-1234-1234-1234-123456789abc")
    for invalid in ("00000000-0000-0000-0000-000000000000", "12345678-1234-1234-1234-123456789ABC"):
        assert re.fullmatch(selector["items"]["pattern"], invalid) is None
    assert "262144 UTF-8 bytes" in " ".join(operation["description"].split())
    assert set(operation["responses"]) == {"200", "400", "403", "503"}
    success = operation["responses"]["200"]
    assert success["headers"]["Cache-Control"] == {"$ref": "#/components/headers/DirectoryCacheControl"}
    assert success["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/RegistryCompanySearchPage"
    }
    page = schemas["RegistryCompanySearchPage"]
    assert page["additionalProperties"] is False and page["required"] == ["records"]
    assert page["properties"]["records"]["maxItems"] == 100
