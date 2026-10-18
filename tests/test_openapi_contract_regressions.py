# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import ast
import re

from tests import openapi_route_contract_support as openapi_support
from tests.openapi_route_contract_support import HIDDEN_RUNTIME_ALIASES, load_openapi_document
from tests.test_openapi_spec import (
    ENDPOINT_DIR,
    OPENAPI_PATH,
    _collect_query_params,
    _collect_spec_routes,
    _QueryParamCollector,
)


def test_openapi_parse_reuse_keeps_mutations_and_file_changes_independent(tmp_path, monkeypatch):
    document_path = tmp_path / "openapi.yaml"
    original_text = "paths:\n  /example:\n    get:\n      summary: original\n"
    document_path.write_text(original_text, encoding="utf-8")
    parsed_texts = []
    parse = openapi_support.yaml.safe_load

    def track_parse(text):
        parsed_texts.append(text)
        return parse(text)

    monkeypatch.setattr(openapi_support.yaml, "safe_load", track_parse)
    openapi_support._parse_openapi_document.cache_clear()
    try:
        first = load_openapi_document(document_path)
        first["paths"]["/example"]["get"]["summary"] = "mutated"
        second = load_openapi_document(document_path)
        assert second["paths"]["/example"]["get"]["summary"] == "original"
        assert parsed_texts == [original_text]

        changed_text = original_text.replace("original", "changed")
        document_path.write_text(changed_text, encoding="utf-8")
        current = load_openapi_document(document_path)
        assert current["paths"]["/example"]["get"]["summary"] == "changed"
        assert parsed_texts == [original_text, changed_text]
    finally:
        openapi_support._parse_openapi_document.cache_clear()


def test_query_param_collector_includes_list_and_pagination_helpers():
    function_node = ast.parse(
        """
async def endpoint(request):
    args = request.args
    args.getlist("name_like")
    parse_pagination(
        args,
        default_limit=25,
        max_limit=200,
        allow_offset=False,
    )
    detail_args = request.args if native_args is None else native_args
    detail_args.get("address_limit")
    group_args = group_plan_arguments(request, native_args)
    group_args.get("plan_release_id")
    report_args = _report_args(request, native_args)
    report_args.get("include_states")
    other_args = _report_args(other_request, native_args)
    other_args.get("unrelated")
"""
    ).body[0]

    assert _collect_query_params(function_node) == {
        "address_limit",
        "include_states",
        "limit",
        "name_like",
        "page",
        "page_size",
        "plan_release_id",
        "start",
    }


def test_openapi_operation_ids_are_present_and_unique():
    text = OPENAPI_PATH.read_text()
    operation_ids = re.findall(
        r"^\s+operationId:\s+([A-Za-z0-9_]+)\s*$",
        text,
        flags=re.MULTILINE,
    )
    spec_routes = {(method, path) for (method, path), _route_info in _collect_spec_routes().items() if method}

    assert len(operation_ids) == len(spec_routes)
    assert len(operation_ids) == len(set(operation_ids))
    assert not (HIDDEN_RUNTIME_ALIASES & spec_routes)


def test_delegated_provider_detail_queries_follow_native_parser():
    tree = ast.parse((ENDPOINT_DIR / "npi.py").read_text())
    functions_by_name = {node.name: node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)}
    delegation = functions_by_name["get_npi"].body[-1]
    assert isinstance(delegation, ast.Return) and isinstance(delegation.value, ast.Await)
    assert delegation.value.value.func.id == "read_npi_detail"
    native_parameters = _collect_query_params(functions_by_name["read_npi_detail"])
    assert native_parameters == openapi_support.ROUTE_QUERY_PARAM_ADDITIONS[("get", "/npi/id/{npi}")]


def test_doctor_search_contract_documents_cards_and_filter_defaults():
    """Keep doctor-search cards and filters aligned with the runtime contract."""

    spec = load_openapi_document(OPENAPI_PATH)
    schemas = spec["components"]["schemas"]
    npi_all = spec["paths"]["/npi/all"]["get"]
    npi_near = spec["paths"]["/npi/near/"]["get"]

    all_parameters_by_name = {
        parameter_by_field["name"]: parameter_by_field for parameter_by_field in npi_all["parameters"]
    }
    assert {"name_like", "page", "offset", "page_size"} <= set(all_parameters_by_name)
    assert all_parameters_by_name["name_like"]["schema"]["type"] == "array"
    assert all_parameters_by_name["npi"]["schema"] == {
        "type": "string",
        "pattern": "^[1-9][0-9]{9}$",
    }
    assert all_parameters_by_name["show"]["schema"]["enum"] == ["chain"]
    assert all_parameters_by_name["view"]["schema"]["enum"] == [
        "sitemap",
        "card",
    ]
    near_parameters_by_name = {
        parameter_by_field["name"]: parameter_by_field for parameter_by_field in npi_near["parameters"]
    }
    assert near_parameters_by_name["view"]["schema"]["enum"] == ["card"]
    assert near_parameters_by_name["radius"]["schema"] == {
        "type": "number",
        "minimum": 0,
        "maximum": 100,
    }

    assert schemas["NpiSearchResponse"]["properties"]["total_source"]["type"] == "string"
    assert schemas["NpiCard"]["additionalProperties"] is False

    detail_parameters_by_name = {
        parameter_by_field["name"]: parameter_by_field
        for parameter_by_field in spec["paths"]["/npi/id/{npi}"]["get"]["parameters"]
    }
    assert detail_parameters_by_name["show"]["schema"]["enum"] == ["chain"]
    assert detail_parameters_by_name["view"]["schema"]["enum"] == [
        "full",
        "summary",
    ]
    assert "enrichment" in detail_parameters_by_name["view"]["description"].lower()


def test_provider_specialty_filter_primary_only_defaults_to_true():
    spec = load_openapi_document(OPENAPI_PATH)
    group_plan_parameters = spec["paths"]["/pricing/group-plan-providers"]["get"]["parameters"]
    assert {
        "classification",
        "include_subspecialties",
        "primary_only",
        "specialty",
        "taxonomy_codes",
    }.issubset({parameter.get("name") for parameter in group_plan_parameters})
    for path in (
        "/pricing/group-plan-providers",
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        primary_only = next(
            parameter_by_field for parameter_by_field in parameters if parameter_by_field.get("name") == "primary_only"
        )
        assert primary_only["schema"] == {
            "type": "boolean",
            "default": True,
        }


def test_npi_endpoint_has_no_shadow_table_probe_or_dead_count_stub():
    tree = ast.parse((ENDPOINT_DIR / "npi.py").read_text())
    list_provider_node = next(
        node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "list_providers"
    )
    detail_node = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_npi")

    assert (
        sum(
            isinstance(node, ast.AsyncFunctionDef) and node.name == "get_formatted_count"
            for node in ast.walk(list_provider_node)
        )
        == 1
    )
    assert not any(
        isinstance(node, ast.AsyncFunctionDef) and node.name == "_is_table_available" for node in ast.walk(detail_node)
    )


def test_source_rollback_contracts_keep_preparation_and_publication_distinct():
    document = load_openapi_document(OPENAPI_PATH)
    schemas = document["components"]["schemas"]
    for suffix, name, operation, identity_fields in (
        ("preview", "RegistrySourceRollbackPreparationCommand", "prepare_source_rollback", {"target_generation"}),
        (
            "publish",
            "RegistrySourceRollbackPublicationCommand",
            "publish_source_rollback",
            {"preview_request_id", "candidate_sha256"},
        ),
    ):
        endpoint = document["paths"][f"/registry/publication/source-rollback/{suffix}"]["post"]
        assert endpoint["requestBody"]["content"]["application/json"]["schema"] == {
            "$ref": f"#/components/schemas/{name}"
        }
        command = schemas[name]
        assert command["additionalProperties"] is False
        assert (
            set(command["required"])
            == set(command["properties"])
            == identity_fields
            | {
                "operation",
                "expected_serving_generation",
                "expected_approved_revision",
                "reason",
                "idempotency_key",
                "actor",
                "session_token_sha256",
            }
        )
        assert command["properties"]["operation"]["enum"] == [operation]
        assert command["properties"]["expected_serving_generation"]["minimum"] == 1
        assert command["properties"]["actor"]["properties"]["kind"]["enum"] == ["platform_admin"]

    description = document["paths"]["/registry/publication/requests/{request_id}"]["get"]["description"]
    for contract in (
        "diff_version 1",
        "distinct evidence-ID",
        "20 per category",
        "64 KiB",
        "legacy preparation receipts",
        "verified READY",
    ):
        assert contract in description


def test_approved_catalog_declared_domains_are_closed_and_list_only():
    spec = load_openapi_document(OPENAPI_PATH)
    schemas = spec["components"]["schemas"]
    query = schemas["RegistryNetworkCatalogListQuery"]
    assert query["additionalProperties"] is False
    assert "benefit_code" not in query.get("required", [])
    assert query["properties"]["benefit_code"]["enum"] == ["medical", "dental", "vision"]
    for name in ("RegistryNetworkCatalogDetailQuery", "RegistryNetworkCatalogLegacyQuery"):
        assert schemas[name]["additionalProperties"] is False
        assert "benefit_code" not in schemas[name]["properties"]
    declarations = schemas["RegistryNetworkCatalogItem"]["properties"]["benefit_codes"]
    assert declarations["type"] == "array" and declarations["nullable"] is True
    assert (declarations["minItems"], declarations["maxItems"], declarations["uniqueItems"]) == (1, 3, True)
    assert declarations["items"] == {"type": "string", "enum": ["medical", "dental", "vision"]}
    assert declarations["enum"] == [
        None,
        ["medical"],
        ["dental"],
        ["vision"],
        ["medical", "dental"],
        ["medical", "vision"],
        ["dental", "vision"],
        ["medical", "dental", "vision"],
    ]
    assert "explicit approved company-network assertions" in declarations["description"]
    assert "No current eligibility" in declarations["description"]
    assert schemas["RegistryNetworkCatalogItem"]["properties"]["priceable"]["enum"] == [None]


def test_legacy_catalog_batch_keeps_scalar_scope_and_bounded_values():
    document = load_openapi_document(OPENAPI_PATH)
    schemas = document["components"]["schemas"]
    scalar = schemas["RegistryNetworkCatalogLegacyQuery"]
    assert scalar["required"] == ["namespace", "value", "source_scope", "source"]
    batch = schemas["RegistryNetworkCatalogLegacyBatchQuery"]
    assert batch["additionalProperties"] is False and batch["required"] == ["namespace", "values"]
    assert set(batch["properties"]) == {"namespace", "values", "network_generation"}
    assert batch["properties"]["namespace"]["enum"] == ["checksum_network", "legacy_fhir_uuid"]
    values = batch["properties"]["values"]
    assert (values["minItems"], values["maxItems"], values["uniqueItems"]) == (1, 50, True)
    assert values["items"]["type"] == "string"
    operation = document["paths"]["/registry/serving/catalog/legacy"]["post"]
    query = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"][1]
    assert query["properties"]["query"]["oneOf"] == [
        {"$ref": "#/components/schemas/RegistryNetworkCatalogLegacyQuery"},
        {"$ref": "#/components/schemas/RegistryNetworkCatalogLegacyBatchQuery"},
    ]


def test_catalog_v2_preserves_v1_and_integer_digest():
    document = load_openapi_document(OPENAPI_PATH)
    schemas = document["components"]["schemas"]
    legacy = schemas["RegistryNetworkCatalogPolicyEnvelope"]
    assert legacy["additionalProperties"] is False
    assert legacy["required"] == ["client_id", "policy_revision", "excluded_network_ids", "query"]
    assert "contract_version" not in legacy["properties"]
    policy = schemas["RegistryCatalogPolicyEnvelopeV2"]
    assert policy["additionalProperties"] is False and len(policy["required"]) == 9
    assert policy["properties"]["contract_version"]["enum"] == ["registry-catalog-policy/v2"]
    assert policy["properties"]["organization_visibility_default"]["enum"] == ["system_wide"]
    for name in ("excluded_company_ids", "excluded_group_ids"):
        assert policy["properties"][name]["uniqueItems"] is True
        assert policy["properties"][name]["maxItems"] == 50000
        assert policy["properties"][name]["items"]["format"] == "uuid"
    assert "JSON int64" in policy["description"] and "other seven policy fields" in policy["description"]
    for suffix in ("", "/detail", "/legacy"):
        request = document["paths"]["/registry/serving/catalog" + suffix]["post"]["requestBody"]
        choices = request["content"]["application/json"]["schema"]["allOf"][0]["oneOf"]
        assert choices == [
            {"$ref": "#/components/schemas/RegistryNetworkCatalogPolicyEnvelope"},
            {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"},
        ]


def test_organization_routes_pages_and_nested_projection_are_closed():
    document = load_openapi_document(OPENAPI_PATH)
    schemas = document["components"]["schemas"]
    for suffix in ("", "/detail"):
        operation = document["paths"]["/registry/serving/organizations" + suffix]["post"]
        policy = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"][0]
        assert policy == {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"}
        assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
        assert set(operation["responses"]) == (
            {"200", "400", "403", "503", "404"} if suffix else {"200", "400", "403", "503"}
        )
    page = schemas["RegistryOrganizationCatalogPage"]
    assert page["additionalProperties"] is False and len(page["required"]) == 9
    assert page["properties"]["record_kind"]["enum"] == ["company", "group"]
    for name in (
        "RegistryServingCompany",
        "RegistryServingGroup",
        "RegistryOrganizationRoleAssertion",
        "RegistryOrganizationIdentifierAssertion",
        "RegistryOrganizationGroupAssertion",
    ):
        assert schemas[name]["additionalProperties"] is False
        assert set(schemas[name]["required"]) == set(schemas[name]["properties"])
    assert "identifier_scope" in schemas["RegistryOrganizationIdentifierAssertion"]["required"]
    query = schemas["RegistryOrganizationCatalogListQuery"]
    assert query["additionalProperties"] is False
    assert set(query["properties"]) == {"record_kind", "network_generation", "limit", "offset"}
    assert query["properties"]["limit"]["default"] == 50


def test_directory_unknown_source_time_requires_committed_retained_cms_scope():
    schema = load_openapi_document(OPENAPI_PATH)["components"]["schemas"]["DirectoryEvidence"]
    assert "observed_at" in schema["required"]
    observed = schema["properties"]["observed_at"]
    assert observed["nullable"] is True
    assert observed["type"] == "string" and observed["format"] == "date-time"
    for term in (
        "current or unscoped",
        "network-scoped CMS",
        "committed retained",
        "X-CMS-Retained-Publication",
        "committed/v1",
    ):
        assert term in observed["description"]


def test_approved_autocomplete_is_additive_control_read_with_closed_v2_query():
    spec = load_openapi_document(OPENAPI_PATH)
    path = spec["paths"]["/plan/network/autocomplete"]
    assert path["get"]["operationId"] == "getPlanNetworkAutocomplete"
    assert {parameter["name"] for parameter in path["get"]["parameters"]} == {"query", "state", "zip_code"}
    operation = path["post"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert "parameters" not in operation
    envelope = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"]
    assert envelope[0] == {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"}
    schemas = spec["components"]["schemas"]
    query = schemas["RegistryNetworkAutocompleteQuery"]
    assert query["additionalProperties"] is False and query["required"] == ["query", "limit"]
    assert set(query["properties"]) == {"query", "limit", "zip_code", "network_generation"}
    assert query["properties"]["limit"] == {"type": "integer", "minimum": 1, "maximum": 50}
    response_schema = schemas["RegistryNetworkAutocompleteResponse"]
    assert response_schema["additionalProperties"] is False
    assert set(response_schema["required"]) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "plans",
    }
    plan = response_schema["properties"]["plans"]["items"]
    assert plan["additionalProperties"] is False and plan["properties"]["year"]["type"] == "integer"
    assert set(operation["responses"]) == {"200", "400", "403", "503"}
    assert set(operation["responses"]["200"]["headers"]) == {
        "Cache-Control",
        "X-Network-Generation",
        "X-Network-Access-Scope",
    }


def test_protected_provider_plans_preserves_closed_legacy_models_and_v2_policy():
    from db.models import Issuer, PlanNPIRaw

    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"]["/registry/serving/providers/plans"]["post"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert "parameters" not in operation
    envelope = operation["requestBody"]["content"]["application/json"]["schema"]["allOf"]
    assert envelope[0] == {"$ref": "#/components/schemas/RegistryCatalogPolicyEnvelopeV2"}
    schemas = spec["components"]["schemas"]
    query = schemas["RegistryProviderPlansQuery"]
    assert query["additionalProperties"] is False and query["required"] == ["npi"]
    assert set(query["properties"]) == {"npi", "year", "network_generation"}
    assert query["properties"]["year"] == {"type": "integer", "minimum": 2000, "maximum": 2100}
    for name, model in (("RegistryProviderPlansRaw", PlanNPIRaw), ("RegistryProviderPlansIssuer", Issuer)):
        schema = schemas[name]
        assert schema["additionalProperties"] is False
        assert (
            set(schema["required"]) == set(schema["properties"]) == {column.name for column in model.__table__.columns}
        )
    body = schemas["RegistryProviderPlansResponse"]
    assert body["additionalProperties"] is False and set(body["required"]) == set(body["properties"])
    assert set(body["properties"]) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "npi_data",
        "plan_data",
        "issuer_data",
    }
    assert body["properties"]["plan_data"]["maxItems"] == body["properties"]["issuer_data"]["maxItems"] == 0
    assert set(operation["responses"]) == {"200", "400", "403", "404", "503"}
    assert set(operation["responses"]["200"]["headers"]) == {
        "Cache-Control",
        "X-Network-Generation",
        "X-Network-Access-Scope",
    }


def test_signed_service_import_documents_conditional_network_pin():
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"]["/extensions/custom-import/providers/by-service"]["post"]
    assert "network_binding" in operation["description"]
    header = operation["responses"]["200"]["headers"]["X-Network-Generation"]
    assert "only when" in header["description"]
    assert header["schema"] == {"$ref": "#/components/schemas/RegistryNetworkCatalogGeneration"}


def test_protected_profile_uses_closed_normalized_strings_and_separate_generation():
    spec = load_openapi_document(OPENAPI_PATH)
    operation = spec["paths"]["/registry/serving/providers/profile"]["post"]
    assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
    assert "parameters" not in operation
    schemas = spec["components"]["schemas"]
    query = schemas["RegistryProviderProfileQuery"]
    assert query["additionalProperties"] is False and query["required"] == ["npi"]
    assert set(query["properties"]) == {
        "npi",
        "include_evidence",
        "include_sensitive",
        "categories",
        "category",
        "limit",
        "offset",
        "generation_id",
    }
    assert all(field["type"] == "string" for field in query["properties"].values())
    assert query["properties"]["generation_id"]["pattern"] == "^[0-9a-f]{64}$"
    body = schemas["RegistryProviderProfileResponse"]
    assert body["additionalProperties"] is False
    assert set(body["properties"]) == set(body["required"]) | {"provider_profile_evidence"}
    assert body["properties"]["provider_profile"] == {"$ref": "#/components/schemas/ProviderProfileDocument"}
    assert body["properties"]["generation"] == {"$ref": "#/components/schemas/RegistryNetworkCatalogGeneration"}
    assert set(operation["responses"]) == {"200", "400", "403", "404", "409", "503"}


def test_delegated_market_queries_follow_the_actual_parser():
    tree = ast.parse((ENDPOINT_DIR / "reports.py").read_text())
    parser = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_market_list_request"
    )
    collector = _QueryParamCollector()
    collector.aliases.add("args")
    collector.visit(parser)
    assert collector.params == openapi_support.ROUTE_QUERY_PARAM_ADDITIONS[("get", "/reports/pharmacies/markets")]
