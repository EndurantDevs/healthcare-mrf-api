# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected read documentation stays aligned with the closed wire contracts."""

import ast
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.openapi_route_contract_support import load_openapi_document


def _constant(path, name):
    for node in ast.parse(Path(path).read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name for target in node.targets
        ):
            value = node.value
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Name) and value.func.id == "frozenset":
                value = value.args[0]
            if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute) and value.func.attr == "split":
                return frozenset(ast.literal_eval(value.func.value).split())
            return ast.literal_eval(value)
    raise AssertionError(name)


@pytest.fixture
def schemas():
    return load_openapi_document(Path("doc/openapi.yaml"))["components"]["schemas"]


@pytest.mark.parametrize("suffix", ["providers/detail", "providers/collection", "directory/entities"])
def test_managed_operations_require_full_policy_and_private_manifest(suffix):
    spec = load_openapi_document(Path("doc/openapi.yaml"))
    operation = spec["paths"]["/registry/serving/" + suffix]["post"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert all(parameter["in"] == "header" for parameter in operation.get("parameters", []))
    envelope = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"]
    assert envelope[0] == {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"}
    assert {"200", "400", "403", "404", "409", "503"} <= set(operation["responses"])
    assert set(operation["responses"]["200"]["headers"]) >= {
        "Cache-Control",
        "X-Network-Generation",
        "X-Network-Access-Scope",
    }
    assert "1 MiB" in operation["description"]
    assert ("4 MiB" if suffix == "directory/entities" else "32 MiB") in operation["description"]


def test_detail_query_matches_closed_parser_fields(schemas):
    contract = "api/provider_detail_network_contract.py"
    query = schemas["RegistryProviderDetailQuery"]
    native = set(_constant(contract, "_BOOLEAN_FIELDS")) | set(_constant(contract, "_UUID_FIELDS"))
    assert query["additionalProperties"] is False and query["required"] == ["npi"]
    assert set(query["properties"]) == native | {
        "npi",
        "include",
        "selector",
        "network_generation",
        "address_grouping",
        "address_limit",
        "address_offset",
    }
    assert query["properties"]["include"]["uniqueItems"] is True
    assert query["properties"]["address_limit"]["default"] == "5"
    assert "from 1 through 5" in query["description"]


@pytest.mark.parametrize("native_absence", [False, True])
def test_detail_404_documents_actual_absence_and_denied_selector(native_absence):
    from api.endpoint import network_catalog

    spec = load_openapi_document(Path("doc/openapi.yaml"))
    schemas = spec["components"]["schemas"]
    operation = spec["paths"]["/registry/serving/providers/detail"]["post"]
    assert operation["responses"]["404"] == {"$ref": "#/components/responses/RegistryProviderDetailNotFound"}
    contract = spec["components"]["responses"]["RegistryProviderDetailNotFound"]
    branches = contract["content"]["application/json"]["schema"]["oneOf"]
    assert branches == [
        {"$ref": "#/components/schemas/RegistryProviderDetailAbsence"},
        {"$ref": "#/components/schemas/RegistryNetworkCatalogError"},
    ]
    proof_by_field = dict(
        generation="9",
        approved_custom_revision="4",
        manifest_sha256="b" * 64,
        npi="1000000491",
        access_scope_sha256="a" * 64,
    )
    reply = (
        network_catalog._detail_absence_response(
            SimpleNamespace(proof=proof_by_field),
            "provider-detail",
            object(),
            dict(client_id="synthetic-client", policy_revision="3"),
            "a" * 64,
        )
        if native_absence
        else network_catalog._error(404, "a" * 64)
    )
    response_by_field = json.loads(reply.body)
    schema = schemas[branches[0 if native_absence else 1]["$ref"].rsplit("/", 1)[1]]
    assert reply.status == 404 and schema["additionalProperties"] is False
    assert set(response_by_field) == set(schema["required"]) == set(schema["properties"])
    for field, field_value in response_by_field.items():
        field_schema = schema["properties"][field]
        if "$ref" in field_schema:
            field_schema = schemas[field_schema["$ref"].rsplit("/", 1)[1]]
        if field == "error":
            assert field_schema["additionalProperties"] is False and set(field_value) == {"code"}
            assert field_value["code"] in field_schema["properties"]["code"]["enum"]
        else:
            assert field_schema["type"] == "string" and isinstance(field_value, str)
            assert re.fullmatch(field_schema.get("pattern", ".*"), field_value)
            assert (
                field_schema.get("minLength", 0) <= len(field_value) <= field_schema.get("maxLength", len(field_value))
            )
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert reply.headers["X-Network-Access-Scope"] == proof_by_field["access_scope_sha256"]
    assert ("X-Network-Generation" in reply.headers) is native_absence
    assert ("X-Network-Manifest-SHA256" in reply.headers) is native_absence
    assert "only" in contract["headers"]["X-Network-Manifest-SHA256"]["description"]
    assert "without generation or manifest headers" in contract["description"]


def test_collection_native_allowlists_match_each_real_kind(schemas):
    contract = "api/provider_collection_network_contract.py"
    network = _constant(contract, "_NETWORK_FIELDS")
    provider = _constant("api/custom_import_provider_http.py", "_PROVIDER_QUERY_FIELDS") - network
    expected_by_kind = {
        "list": provider
        | set(
            "count_only format response_format address_limit address_offset include_address_total extra_info address_grouping".split()
        ),
        "geo": (_constant("api/custom_import_provider_geo.py", "_GEO_QUERY_FIELDS") - network) | {"count_only"},
        "batch": provider | {"extra_info", "address_grouping"},
        "by-service": _constant(contract, "_SERVICE_NATIVE_FIELDS") - network,
        "group-plan": _constant("api/provider_group_plan_network_read.py", "GROUP_PLAN_NATIVE_FIELDS") - network,
        "billing-search": _constant("api/provider_billing_network_read.py", "BILLING_SEARCH_NATIVE_FIELDS") - network,
    }
    from api.endpoint.reports import MANAGED_REPORT_FIELDS

    expected_by_kind.update(MANAGED_REPORT_FIELDS)
    branches = schemas["RegistryProviderCollectionQuery"]["oneOf"]
    assert {branch["properties"]["kind"]["enum"][0] for branch in branches} == set(expected_by_kind)
    for branch in branches:
        props = branch["properties"]
        kind = props["kind"]["enum"][0]
        native_name = props["native_query"]["$ref"].rsplit("/", 1)[1]
        native = schemas[native_name]
        assert branch["additionalProperties"] is False and native["additionalProperties"] is False
        assert set(native["properties"]) == expected_by_kind[kind]
        assert ("native_batch" in props) == (kind == "batch")
        assert ("native_batch" in branch["required"]) == (kind == "batch")
        assert "16 KiB" in native["description"]
    billing = schemas["RegistryManagedBillingSearchNativeQuery"]
    assert set(billing["required"]) == {
        "plan_release_id",
        "billing_entity_ref",
        "code",
        "code_system",
        "limit",
        "billing_transport_context_sha256",
    }
    assert len(billing["oneOf"]) == 2  # ZIP and coordinate modes remain exclusive.
    assert schemas["RegistryManagedListNativeQuery"]["properties"]["name_like"]["oneOf"][1]["maxItems"] == 16


def test_selector_batch_and_directory_bounds(schemas):
    canonical, legacy = schemas["RegistryManagedNetworkSelector"]["oneOf"]
    assert canonical["properties"]["values"]["maxItems"] == 100
    assert canonical["properties"]["values"]["items"]["maximum"] == 2147483647
    assert legacy["properties"]["values"]["maxItems"] == 50
    batch = schemas["RegistryProviderCollectionNativeBatch"]
    assert batch["additionalProperties"] is False
    assert batch["properties"]["npis"]["maxItems"] == 100
    assert batch["properties"]["address_offset"]["maximum"] == 1000000
    directory = schemas["RegistryManagedDirectoryQuery"]
    assert directory["additionalProperties"] is False
    assert directory["properties"]["limit"]["maximum"] == 100
    assert directory["properties"]["cursor"]["maxLength"] == 512
    assert directory["properties"]["generation_id"]["maxLength"] == 128
    assert {row["properties"]["source_id"]["enum"][0] for row in directory["oneOf"]} == {"cms-npd", "cms-doctors"}
    assert len(directory["allOf"]) == 2  # Shape/identity and cursor/generation dependencies.
    assert len(schemas["RegistryManagedDirectoryResponse"]["properties"]["data"]["anyOf"]) == 3


@pytest.mark.parametrize(
    "name", ["RegistryProviderDetailResponse", "RegistryProviderCollectionResponse", "RegistryManagedDirectoryResponse"]
)
def test_managed_results_close_complete_policy_and_manifest_proof(schemas, name):
    result = schemas[name]
    assert result["additionalProperties"] is False
    assert (
        set(result["required"])
        == set(result["properties"])
        == {"client_id", "policy_revision", "generation", "approved_custom_revision", "manifest_sha256", "data"}
    )
    assert result["properties"]["manifest_sha256"]["pattern"] == "^[0-9a-f]{64}$"
    error_codes = schemas["RegistryNetworkCatalogError"]["properties"]["error"]["properties"]["code"]["enum"]
    assert "network_catalog_source_changed" in error_codes


@pytest.mark.parametrize(
    ("field", "value", "accepted"),
    [
        ("limit", "1000", True),
        ("limit", "01000", True),
        ("limit", "1001", False),
        ("cursor", "9999999999", True),
        ("cursor", "10000000000", False),
        ("cursor", "0", False),
        ("cursor", "١", False),
    ],
)
def test_group_native_bounds_preserve_internal_decimal_contract(schemas, field, value, accepted):
    pattern = schemas["RegistryManagedGroupPlanNativeQuery"]["properties"][field]["pattern"]
    assert bool(re.fullmatch(pattern, value)) is accepted


@pytest.mark.parametrize(
    "kind,required",
    [
        ("pharmacy-markets", set()),
        ("pharmacy-market", {"market_id"}),
        ("pharmacy-rankings", set()),
        ("pharmacy-chain-summary", {"name_like"}),
        ("pharmacy-state-stats", set()),
        ("pharmacy-context", {"npi"}),
    ],
)
def test_pharmacy_report_schemas_preserve_envelope_and_native_identity(schemas, kind, required):
    from api.endpoint.reports import MANAGED_REPORT_FIELDS

    branch = next(
        row
        for row in schemas["RegistryProviderCollectionQuery"]["oneOf"]
        if row["properties"]["kind"]["enum"] == [kind]
    )
    assert set(branch["properties"]) == {"kind", "native_query", "selector", "network_generation"}
    assert branch["required"] == ["kind", "native_query"]
    assert branch["properties"]["selector"] == {"$ref": "#/components/schemas/RegistryManagedNetworkSelector"}
    assert branch["properties"]["network_generation"] == {
        "$ref": "#/components/schemas/RegistryNetworkCatalogGeneration"
    }
    native = schemas[branch["properties"]["native_query"]["$ref"].rsplit("/", 1)[1]]
    assert set(native["properties"]) == MANAGED_REPORT_FIELDS[kind]
    assert set(native.get("required", [])) == required
    assert native["additionalProperties"] is False
    if kind == "pharmacy-chain-summary":
        assert native["properties"]["name_like"]["oneOf"][1]["maxItems"] == 16


def test_legacy_source_restart_contract_is_conditional_and_private(schemas):
    from api.endpoint import network_catalog
    from process.registry_network_catalog_read import RegistryNetworkCatalogError

    operation = load_openapi_document(Path("doc/openapi.yaml"))["paths"]["/registry/serving/catalog/legacy"]["post"]
    policies = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"][0]["oneOf"]
    assert policies == [
        {"$ref": "#/components/schemas/RegistryNetworkCatalogPolicyEnvelope"},
        {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"},
    ]
    refusal = operation["responses"]["409"]
    assert set(refusal["headers"]) == {"Cache-Control", "X-Network-Access-Scope"}
    assert refusal["headers"]["Cache-Control"] == {"$ref": "#/components/headers/DirectoryCacheControl"}
    assert refusal["headers"]["X-Network-Access-Scope"] == {"$ref": "#/components/headers/RegistryNetworkAccessScope"}
    assert refusal["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/RegistryNetworkCatalogError"
    }
    assert operation["responses"]["503"] == {"$ref": "#/components/responses/RegistryNetworkCatalogUnavailable"}
    description = operation["description"]
    assert all(
        field_value in description
        for field_value in ("complete v2", "V1", "malformed source-change reader", "native failures", "503", "restart")
    )
    headers = load_openapi_document(Path("doc/openapi.yaml"))["components"]["headers"]
    assert headers["DirectoryCacheControl"]["schema"]["enum"] == ["private, no-store"]
    assert (
        "network_catalog_source_changed"
        in schemas["RegistryNetworkCatalogError"]["properties"]["error"]["properties"]["code"]["enum"]
    )
    for full_policy in (False, True):
        for signal in (
            "registry_network_catalog_source_changed",
            "registry_network_catalog_source_changed:invalid",
            "registry_network_catalog_unavailable",
        ):
            reply = network_catalog._catalog_reader_error(
                RegistryNetworkCatalogError(signal), "legacy", object() if full_policy else None, "a" * 64
            )
            expected = 409 if full_policy and signal == "registry_network_catalog_source_changed" else 503
            assert reply.status == expected
            assert "X-Network-Generation" not in reply.headers
            assert reply.headers["Cache-Control"] == "private, no-store"
            if full_policy:
                assert reply.headers["X-Network-Access-Scope"] == "a" * 64
