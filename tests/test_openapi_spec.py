# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import ast
import re
from pathlib import Path

import yaml

HTTP_METHODS = {"get", "post", "put", "delete", "patch", "options", "head"}
ENDPOINT_DIR = Path("api/endpoint")
OPENAPI_PATH = Path("doc/openapi.yaml")
HIDDEN_RUNTIME_ALIASES = {
    # Control-authenticated candidate validation is intentionally excluded
    # from the public OpenAPI contract.
    ("get", "/pricing/providers/audit-search-by-procedure"),
    ("post", "/pricing/providers/audit-source-witness-batch"),
    ("get", "/pricing/physicians"),
    ("get", "/pricing/physicians/{npi}"),
    ("get", "/pricing/physicians/{npi}/score"),
    ("get", "/pricing/physicians/{npi}/services"),
    ("get", "/pricing/physicians/{npi}/services/{code_system}/{code}"),
    ("get", "/pricing/physicians/{npi}/services/{code_system}/{code}/estimated-cost-level"),
    ("get", "/pricing/physicians/{npi}/services/{code_system}/{code}/locations"),
    ("get", "/pricing/services/autocomplete"),
    ("get", "/pricing/services/resolve"),
    ("get", "/pricing/drugs/autocomplete"),
    ("get", "/pricing/drugs/resolve"),
    ("get", "/pricing/prescriptions/resolve"),
    ("get", "/pricing/medications/autocomplete"),
    ("get", "/pricing/providers/by-service"),
    ("get", "/pricing/physicians/by-service"),
    ("get", "/pricing/providers/by-drug"),
    ("get", "/pricing/physicians/by-prescription"),
    ("get", "/pricing/physicians/by-drug"),
    ("get", "/pricing/physicians/{npi}/prescriptions"),
    ("get", "/pricing/physicians/{npi}/prescriptions/{rx_code_system}/{rx_code}"),
}


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
        self._process(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._process(node)
        self.generic_visit(node)

    def _process(self, node: ast.AST) -> None:
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


def _collect_spec_routes() -> dict[tuple[str, str], dict[str, set[str]]]:
    """Support the collect spec routes test fixture."""
    routes_by_path: dict[tuple[str, str], dict[str, set[str]]] = {}
    lines = OPENAPI_PATH.read_text().splitlines()
    is_in_paths = False
    current_path: str | None = None
    current_method: str | None = None
    is_in_parameters = False
    current_parameter_by_field: dict[str, str] | None = None

    for raw_line in lines:
        if not is_in_paths:
            if raw_line.strip() == "paths:":
                is_in_paths = True
            continue
        if raw_line.strip().startswith("components:"):
            break
        if not raw_line.strip():
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()
        if indent == 2 and stripped.endswith(":") and stripped.startswith("/"):
            current_path = stripped[:-1]
            current_method = None
            routes_by_path.setdefault(("", current_path), {})
            continue
        if indent == 4 and stripped.endswith(":") and current_path:
            token = stripped[:-1].lower()
            if token in HTTP_METHODS:
                current_method = token
                routes_by_path[("", current_path)][current_method] = []
                is_in_parameters = False
                continue
            current_method = None
        if indent == 6 and current_method:
            if stripped == "parameters:":
                is_in_parameters = True
                routes_by_path[("", current_path)][current_method] = []
                continue
            else:
                is_in_parameters = False
        if indent == 8 and is_in_parameters and current_method:
            if stripped.startswith("- name:"):
                name = stripped.split(":", 1)[1].strip().strip("'\"")
                current_parameter_by_field = {"name": name}
                routes_by_path[("", current_path)][current_method].append(current_parameter_by_field)
                continue
        if indent >= 10 and is_in_parameters and current_method and current_parameter_by_field is not None:
            if stripped.startswith("in:"):
                current_parameter_by_field["in"] = stripped.split(":", 1)[1].strip()
            continue
        if indent <= 6:
            current_parameter_by_field = None
            is_in_parameters = is_in_parameters and stripped == "parameters:"

    spec_routes_by_operation: dict[tuple[str, str], dict[str, set[str]]] = {}
    for (_, path), methods in routes_by_path.items():
        for method, params in methods.items():
            query_params = {parameter_by_field["name"] for parameter_by_field in params if parameter_by_field.get("in") == "query"}
            path_params = {parameter_by_field["name"] for parameter_by_field in params if parameter_by_field.get("in") == "path"}
            spec_routes_by_operation[(method, path)] = {
                "query_params": query_params,
                "path_params": path_params,
            }
    return spec_routes_by_operation


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
        assert code_info["query_params"] == spec_info["query_params"], (
            f"Query parameters mismatch for {key}: code={sorted(code_info['query_params'])}, "
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
            if parameter_by_field["name"] == "include_allowed_amounts"
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
            if parameter_by_field["name"] == "provider_sex_code"
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
            if parameter_by_field["name"] == "include_unverified_addresses"
        )
        description = unverified_address_parameter["description"]
        assert "allowed-amount fallback results" in description
        assert "suppresses distance" in description
        assert "location-verification metadata" in description
        assert "location filters" in description
        assert "apply internally" in description


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


def test_npi_near_documents_exact_cursor_page_identity():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    operation = spec["paths"]["/npi/near/"]["get"]
    parameter_names = {parameter["name"] for parameter in operation["parameters"]}

    assert {"cursor", "include_total", "provider_sex_code"} <= parameter_names
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
