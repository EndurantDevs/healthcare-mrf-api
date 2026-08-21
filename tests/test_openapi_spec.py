# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import ast
import re
from pathlib import Path

import yaml

from tests.openapi_route_contract_support import (
    HIDDEN_RUNTIME_ALIASES,
    ROUTE_QUERY_PARAM_ADDITIONS,
    ROUTE_QUERY_PARAM_REMOVALS,
)

HTTP_METHODS = {"get", "post", "put", "delete", "patch", "options", "head"}
ENDPOINT_DIR = Path("api/endpoint")
OPENAPI_PATH = Path("doc/openapi.yaml")


def _combine_paths(prefix: str, route_path: str) -> str:
    base = prefix or ""
    if base and not base.startswith("/"):
        base = f"/{base}"
    if not route_path.startswith("/"):
        route_path = f"/{route_path}"
    if route_path == "/" and base:
        return base.rstrip("/") + "/"
    if base:
        return base.rstrip("/") + route_path
    return route_path


class _QueryParamCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.aliases = {"request.args"}
        self.params: set[str] = set()

    def visit_Assign(self, node: ast.Assign) -> None:
        if isinstance(node.value, ast.Attribute) and self._is_request_args(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.aliases.add(target.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in {"get", "getlist"}:
            if self._is_request_args_resolution(func.value):
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    self.params.add(node.args[0].value)
        if isinstance(func, ast.Name) and func.id == "_get_list_param":
            if node.args and self._is_request_args_resolution(node.args[0]):
                if len(node.args) > 1 and isinstance(node.args[1], ast.Constant) and isinstance(node.args[1].value, str):
                    self.params.add(node.args[1].value)
        if isinstance(func, ast.Name) and func.id == "parse_pagination":
            if node.args and self._is_request_args_resolution(node.args[0]):
                self.params.update({"limit", "page"})
                enabled_aliases = {
                    "allow_offset": "offset",
                    "allow_start": "start",
                    "allow_page_size": "page_size",
                }
                keyword_values = {
                    keyword.arg: keyword.value
                    for keyword in node.keywords
                    if keyword.arg is not None
                }
                for keyword_name, parameter_name in enabled_aliases.items():
                    keyword_value = keyword_values.get(keyword_name)
                    if not (
                        isinstance(keyword_value, ast.Constant)
                        and keyword_value.value is False
                    ):
                        self.params.add(parameter_name)
        self.generic_visit(node)

    @staticmethod
    def _is_request_args(node: ast.AST) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "request"
            and node.attr == "args"
        )

    def _is_request_args_resolution(self, node: ast.AST) -> bool:
        if self._is_request_args(node):
            return True
        if isinstance(node, ast.Name) and node.id in self.aliases:
            return True
        return False


def _collect_code_routes() -> dict[tuple[str, str], dict[str, set[str]]]:
    routes_by_key: dict[tuple[str, str], dict[str, set[str]]] = {}
    for path in ENDPOINT_DIR.glob("*.py"):
        tree = ast.parse(path.read_text())
        prefix = _extract_blueprint_prefix(tree)
        collector = _EndpointCollector(prefix)
        collector.visit(tree)
        for route in collector.routes:
            key = (route["method"], route["spec_path"])
            routes_by_key[key] = {
                "query_params": route["query_params"],
                "path_params": route["path_params"],
                "source": {"file": path.name, "function": route["function"]},
            }
    return routes_by_key


def _extract_blueprint_prefix(tree: ast.AST) -> str:
    for node in getattr(tree, "body", []):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "blueprint" for t in node.targets):
            continue
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == "Blueprint":
            for kw in node.value.keywords:
                if kw.arg == "url_prefix" and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    return kw.value.value
    return ""


class _EndpointCollector(ast.NodeVisitor):
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix or ""
        self.routes: list[dict[str, object]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._collect_route(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._collect_route(node)
        self.generic_visit(node)

    def _collect_route(self, node: ast.AST) -> None:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return
        query_params = _collect_query_params(node)
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            func = decorator.func
            if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name) and func.value.id == "blueprint"):
                continue
            method = func.attr.lower()
            if method not in HTTP_METHODS:
                continue
            if not decorator.args:
                continue
            first_arg = decorator.args[0]
            if not (isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str)):
                continue
            route_fragment = first_arg.value
            full_path = _combine_paths(self.prefix, route_fragment)
            spec_path = full_path.replace("<", "{").replace(">", "}")
            path_params = set(re.findall(r"<([^>]+)>", route_fragment))
            self.routes.append(
                {
                    "method": method,
                    "raw_path": full_path,
                    "spec_path": spec_path,
                    "query_params": set(query_params),
                    "path_params": path_params,
                    "function": node.name,
                }
            )


def _collect_query_params(node: ast.AST) -> set[str]:
    visitor = _QueryParamCollector()
    visitor.visit(node)
    return visitor.params


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
"""
    ).body[0]

    assert _collect_query_params(function_node) == {
        "limit",
        "name_like",
        "page",
        "page_size",
        "start",
    }


def _collect_spec_routes() -> dict[tuple[str, str], dict[str, set[str]]]:
    """Collect route parameters from the checked-in OpenAPI document."""
    document = yaml.safe_load(OPENAPI_PATH.read_text())

    def resolve_parameter(parameter_by_field: dict) -> dict:
        resolved = parameter_by_field
        while "$ref" in resolved:
            target = document
            for token in resolved["$ref"].removeprefix("#/").split("/"):
                target = target[token]
            resolved = target
        return resolved

    routes_by_operation: dict[tuple[str, str], dict[str, set[str]]] = {}
    for path, path_by_field in document["paths"].items():
        shared_parameters = path_by_field.get("parameters", [])
        for method, operation_by_field in path_by_field.items():
            if method not in HTTP_METHODS:
                continue
            parameters = [
                resolve_parameter(parameter_by_field)
                for parameter_by_field in (
                    *shared_parameters,
                    *operation_by_field.get("parameters", []),
                )
            ]
            routes_by_operation[(method, path)] = {
                "query_params": {
                    parameter_by_field["name"]
                    for parameter_by_field in parameters
                    if parameter_by_field.get("in") == "query"
                },
                "path_params": {
                    parameter_by_field["name"]
                    for parameter_by_field in parameters
                    if parameter_by_field.get("in") == "path"
                },
            }
    return routes_by_operation


def test_openapi_routes_match_code():
    code_routes = _collect_code_routes()
    spec_routes_raw = _collect_spec_routes()
    spec_routes_by_key = {(method, path): info for (method, path), info in spec_routes_raw.items() if method}

    # Normalise spec keys to align with code keys
    spec_keys = set(spec_routes_by_key.keys())
    code_keys = set(code_routes.keys()) - HIDDEN_RUNTIME_ALIASES

    missing_in_spec = sorted(code_keys - spec_keys)
    extra_in_spec = sorted(spec_keys - code_keys)

    assert not missing_in_spec, f"Routes missing from OpenAPI: {missing_in_spec}"
    assert not extra_in_spec, f"Extra routes in OpenAPI not in code: {extra_in_spec}"

    for key in sorted(code_keys):
        code_info = code_routes[key]
        spec_info = spec_routes_by_key[key]
        assert code_info["path_params"] == spec_info["path_params"], (
            f"Path parameters mismatch for {key}: code={sorted(code_info['path_params'])}, "
            f"spec={sorted(spec_info['path_params'])}"
        )
        code_query_params = (
            code_info["query_params"] | ROUTE_QUERY_PARAM_ADDITIONS.get(key, set())
        ) - ROUTE_QUERY_PARAM_REMOVALS.get(key, set())
        assert code_query_params == spec_info["query_params"], (
            f"Query parameters mismatch for {key}: code={sorted(code_query_params)}, "
            f"spec={sorted(spec_info['query_params'])}"
        )


def test_openapi_operation_ids_are_present_and_unique():
    text = OPENAPI_PATH.read_text()
    operation_ids = re.findall(r"^\s+operationId:\s+([A-Za-z0-9_]+)\s*$", text, flags=re.MULTILINE)
    spec_routes = {(method, path) for (method, path), _info in _collect_spec_routes().items() if method}

    assert len(operation_ids) == len(spec_routes)
    assert len(operation_ids) == len(set(operation_ids))
    assert not (HIDDEN_RUNTIME_ALIASES & spec_routes)


def test_openapi_strict_ptg_pagination_exposes_exact_page_continuation():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    schemas = spec["components"]["schemas"]
    pagination_properties = schemas["PaginationMeta"]["properties"]

    assert {"has_more", "total_is_exact", "total_lower_bound"} <= set(
        pagination_properties
    )
    assert pagination_properties["has_more"]["type"] == "boolean"
    assert pagination_properties["total_is_exact"]["type"] == "boolean"
    assert pagination_properties["total_lower_bound"] == {
        "type": "integer",
        "minimum": 0,
        "description": (
            "Optional proven lower bound when an exact total would require "
            "exhaustive expansion."
        ),
    }

    strict_pagination = schemas["PtgPricingPaginationMeta"]["allOf"]
    assert strict_pagination[0] == {
        "$ref": "#/components/schemas/PaginationMeta"
    }
    required_parameters = set(strict_pagination[1]["required"])
    assert required_parameters == {"total", "limit", "offset", "has_more"}
    assert {"total_is_exact", "total_lower_bound"}.isdisjoint(required_parameters)
    assert (
        schemas["PricingProcedureProviderListResponse"]["properties"][
            "pagination"
        ]["$ref"]
        == "#/components/schemas/PtgPricingPaginationMeta"
    )


def test_openapi_exposes_strict_v3_allowed_amount_fallback():
    """Document allowed fallback routing and response states."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        allowed_parameter = next(
            parameter_by_field
            for parameter_by_field in parameters
            if parameter_by_field.get("name") == "include_allowed_amounts"
        )
        assert allowed_parameter["schema"] == {
            "type": "boolean",
            "default": True,
        }
        description = allowed_parameter["description"]
        assert "strict-V3" in description
        assert "every isolated current allowed-evidence source" in description
        assert "covering the" in description
        assert "requested plan" in description
        assert "request predicates can be" in description
        assert "preserved" in description
        assert "rate-tolerance predicates do not fall back" in description
        assert "not negotiated rates" in description

    response_schema = spec["components"]["schemas"][
        "PricingProcedureProviderListResponse"
    ]
    response_properties = response_schema["properties"]
    assert set(response_properties["result_state"]["enum"]) == {
        "matched",
        "allowed_amounts_found",
        "no_match_in_radius",
        "no_matching_rates",
        "no_snapshot_for_plan",
    }
    assert set(response_properties["pricing_scope"]["enum"]) == {
        "plan_scoped_ptg",
        "plan_scoped_allowed_amounts",
    }
    assert set(
        response_properties["query"]["properties"]["status"]["enum"]
    ) == {
        "matched",
        "allowed_amounts_found",
        "no_match",
        "no_route",
    }


def test_doctor_search_contract_documents_cards_and_filter_defaults():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    schemas = spec["components"]["schemas"]
    npi_all = spec["paths"]["/npi/all"]["get"]
    npi_near = spec["paths"]["/npi/near/"]["get"]

    all_params = {item["name"]: item for item in npi_all["parameters"]}
    assert {"name_like", "page", "offset", "page_size"} <= set(all_params)
    assert all_params["name_like"]["schema"]["type"] == "array"
    assert all_params["npi"]["schema"] == {
        "type": "string",
        "pattern": "^[1-9][0-9]{9}$",
    }
    assert all_params["show"]["schema"]["enum"] == ["chain"]
    assert all_params["view"]["schema"]["enum"] == ["sitemap", "card"]
    near_params = {item["name"]: item for item in npi_near["parameters"]}
    assert near_params["view"]["schema"]["enum"] == ["card"]

    assert schemas["NpiSearchResponse"]["properties"]["total_source"][
        "type"
    ] == "string"
    assert schemas["NpiCard"]["additionalProperties"] is False

    detail_params = {
        item["name"]: item
        for item in spec["paths"]["/npi/id/{npi}"]["get"]["parameters"]
    }
    assert detail_params["show"]["schema"]["enum"] == ["chain"]
    assert detail_params["view"]["schema"]["enum"] == ["full", "summary"]
    assert "enrichment" in detail_params["view"]["description"].lower()

    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        primary_only = next(
            item for item in parameters if item.get("name") == "primary_only"
        )
        assert primary_only["schema"] == {
            "type": "boolean",
            "default": True,
        }


def test_npi_endpoint_has_no_shadow_table_probe_or_dead_count_stub():
    tree = ast.parse((ENDPOINT_DIR / "npi.py").read_text())
    list_provider_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "list_providers"
    )
    detail_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_npi"
    )

    assert sum(
        isinstance(node, ast.AsyncFunctionDef)
        and node.name == "get_formatted_count"
        for node in ast.walk(list_provider_node)
    ) == 1
    assert not any(
        isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_is_table_available"
        for node in ast.walk(detail_node)
    )


def test_provider_routes_share_canonical_provider_sex_parameter():
    """Keep one provider-sex parameter name and value contract across APIs."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    for path in (
        "/npi/all",
        "/npi/near/",
        "/pricing/group-plan-providers",
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        provider_sex_parameter = next(
            parameter_by_field
            for parameter_by_field in parameters
            if parameter_by_field.get("name") == "provider_sex_code"
        )
        assert provider_sex_parameter["in"] == "query"
        assert provider_sex_parameter["schema"]["enum"] == ["M", "F", "U", "X"]
        assert provider_sex_parameter["schema"]["type"] == "string"


def test_openapi_documents_allowed_unverified_location_suppression():
    """Document allowed fallback output suppression without changing filtering."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        unverified_address_parameter = next(
            parameter_by_field
            for parameter_by_field in parameters
            if parameter_by_field.get("name") == "include_unverified_addresses"
        )
        description = " ".join(
            unverified_address_parameter["description"].split()
        )
        required_phrases = (
            "Allowed-amount fallback never returns", "provider address", "distance",
            "location filters", "apply internally",
            "no-address-binding metadata remains",
        )
        assert all(
            required_phrase in description for required_phrase in required_phrases
        )


def test_npi_profile_contract_is_typed_and_address_refresh_is_boolean():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    npi_parameters = spec["paths"]["/npi/id/{npi}"]["get"]["parameters"]
    parameters_by_name = {parameter["name"]: parameter for parameter in npi_parameters}
    schemas = spec["components"]["schemas"]

    assert list(parameter["name"] for parameter in npi_parameters).count("address_key") == 1
    assert parameters_by_name["force_address_update"]["schema"] == {
        "type": "boolean",
        "default": False,
    }
    assert schemas["ProviderDirectoryProfile"]["additionalProperties"] is False
    assert schemas["ProviderDirectoryProfileFact"]["additionalProperties"] is False
    assert schemas["ProviderDirectoryProfileFactEvidence"]["additionalProperties"] is False
    profile_evidence = schemas["NpiRecord"]["properties"][
        "provider_directory_profile_evidence"
    ]
    assert profile_evidence["$ref"] == (
        "#/components/schemas/ProviderDirectoryProfileEvidence"
    )


def test_provider_profile_endpoint_documents_compact_and_paged_contracts():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    operation = spec["paths"]["/npi/id/{npi}/profile"]["get"]
    parameters_by_name = {
        parameter["name"]: parameter
        for parameter in operation["parameters"]
    }
    schemas = spec["components"]["schemas"]

    assert {
        "categories",
        "category",
        "limit",
        "offset",
        "generation_id",
        "include_evidence",
        "include_sensitive",
    } <= parameters_by_name.keys()
    assert parameters_by_name["category"]["schema"] == {
        "$ref": "#/components/schemas/ProviderProfileCategory"
    }
    assert parameters_by_name["limit"]["schema"]["maximum"] == 50
    assert parameters_by_name["generation_id"]["schema"]["pattern"] == (
        "^[0-9a-f]{64}$"
    )
    assert len(schemas["ProviderProfileCategory"]["enum"]) == 30
    assert operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/ProviderProfileResponse"}
    assert operation["responses"]["409"]["content"]["application/json"][
        "schema"
    ] == {
        "$ref": "#/components/schemas/ProviderProfileGenerationConflict"
    }
    display_description = schemas["ProviderProfileFact"]["properties"]["display"][
        "description"
    ]
    assert display_description.startswith("Concise human-readable")
    assert "never contains serialized JSON" in display_description
    assert schemas["ProviderProfileDocument"]["properties"]["composer_version"][
        "example"
    ] == "provider-profile-composer/v5"
    summary = schemas["ProviderProfessionalSummary"]
    assert summary["additionalProperties"] is False
    assert summary["required"] == ["label", "text", "authorship", "basis"]
    assert summary["properties"]["label"]["enum"] == [
        "Generated professional summary"
    ]
    assert summary["properties"]["authorship"]["enum"] == [
        "generated_from_structured_source_data"
    ]
    basis_item = summary["properties"]["basis"]["items"]
    assert basis_item["additionalProperties"] is False
    assert basis_item["required"] == ["category", "item_id"]
    document = schemas["ProviderProfileDocument"]
    assert "professional_summary" not in document["required"]
    assert document["properties"]["professional_summary"] == {
        "$ref": "#/components/schemas/ProviderProfessionalSummary"
    }


def test_npi_near_documents_exact_cursor_page_identity():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    operation = spec["paths"]["/npi/near/"]["get"]
    parameter_names = {parameter["name"] for parameter in operation["parameters"]}

    assert {"cursor", "include_total", "provider_sex_code", "specialization"} <= parameter_names
    response_schema = operation["responses"]["200"]["content"]["application/json"][
        "schema"
    ]
    assert {"$ref": "#/components/schemas/NpiNearPage"} in response_schema["oneOf"]
    page_schema = spec["components"]["schemas"]["NpiNearPage"]
    assert page_schema["required"] == [
        "items",
        "total_count",
        "next_cursor",
        "has_more",
        "result_identity",
    ]
    assert page_schema["properties"]["result_identity"]["example"] == [
        "npi",
        "address_key",
    ]
