# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected read documentation stays aligned with the closed wire contracts."""

import ast
import re
from pathlib import Path

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
