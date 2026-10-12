# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Company search retains a bounded, explicitly scoped OpenAPI contract."""

import re

import pytest

from tests.openapi_route_contract_support import load_openapi_document
from tests.test_openapi_spec import OPENAPI_PATH


@pytest.mark.parametrize("kind, schema_name", [("company", "RegistryCompanySearch"), ("group", "RegistryGroupSearch")])
def test_company_search_openapi_preserves_complete_scope_and_bounds(kind, schema_name):
    """Keep the dedicated trusted search contract separate from legacy writes."""
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"][f"/registry/manage/{kind}/search"]["post"]
    schemas = spec["components"]["schemas"]
    request_schema = schemas[f"{schema_name}Request"]
    properties = request_schema["properties"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert not operation.get("parameters")
    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": f"#/components/schemas/{schema_name}Request"
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
    assert success["content"]["application/json"]["schema"] == {"$ref": f"#/components/schemas/{schema_name}Page"}
    page = schemas[f"{schema_name}Page"]
    assert page["additionalProperties"] is False and page["required"] == ["records"]
    assert page["properties"]["records"]["maxItems"] == 100


def test_group_company_links_optional_effective_date_is_explicit_and_canonical():
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"]["/registry/manage/company_links/group_snapshot"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    assert request_schema["additionalProperties"] is False
    assert request_schema["required"] == ["group_id", "company_ids", "limit", "offset"]
    selector = request_schema["properties"]["effective_on"]
    assert selector["type"] == "string" and selector["format"] == "date"
    assert selector.get("nullable", False) is False
    assert re.fullmatch(selector["pattern"], "2026-06-01")
    for invalid in ("20260601", "2026-W01-1", "2026-6-1", "2026-06-01T00:00:00"):
        assert re.fullmatch(selector["pattern"], invalid) is None
    description = " ".join(operation["description"].split())
    assert "valid_from <= effective_on < valid_to" in description
    assert "unqualified network_ids" in description and "management history" in description
    page = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert page["required"] == ["group", "records", "draft_revision", "limit", "offset"]
    assert page["properties"]["effective_on"]["format"] == "date"


@pytest.mark.parametrize("path", ["/registry/manage/{kind}", "/registry/manage/{kind}/{record_id}"])
def test_membership_reads_document_the_optional_global_custom_revision(path):
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"][path]["get"]
    parameter = next(item for item in operation["parameters"] if item["name"] == "custom_revision")
    assert parameter["in"] == "query" and parameter["required"] is False
    assert parameter["schema"] == {"type": "integer", "format": "int64", "minimum": 0, "maximum": 9223372036854775807}
    description = " ".join(parameter["description"].split())
    assert "Membership reads only" in description
    assert "global custom revision" in description
    assert "canonical nonnegative decimal text" in description
    assert "other kinds reject" in description
    assert "does not approve or publish drafts" in description
