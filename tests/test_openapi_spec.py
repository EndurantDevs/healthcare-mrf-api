# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import ast
import re
from pathlib import Path

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

    def visit_Assign(self, node: ast.Assign) -> None:  # type: ignore[override]
        if isinstance(node.value, ast.Attribute) and self._is_request_args(node.value):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.aliases.add(target.id)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # type: ignore[override]
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in {"get", "getlist"}:
            if self._resolves_to_request_args(func.value):
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    self.params.add(node.args[0].value)
        if isinstance(func, ast.Name) and func.id == "_get_list_param":
            if node.args and self._resolves_to_request_args(node.args[0]):
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

    def _resolves_to_request_args(self, node: ast.AST) -> bool:
        if self._is_request_args(node):
            return True
        if isinstance(node, ast.Name) and node.id in self.aliases:
            return True
        return False


def _collect_code_routes() -> dict[tuple[str, str], dict[str, set[str]]]:
    routes: dict[tuple[str, str], dict[str, set[str]]] = {}
    for path in ENDPOINT_DIR.glob("*.py"):
        tree = ast.parse(path.read_text())
        prefix = _extract_blueprint_prefix(tree)
        collector = _EndpointCollector(prefix)
        collector.visit(tree)
        for route in collector.routes:
            key = (route["method"], route["spec_path"])
            routes[key] = {
                "query_params": route["query_params"],
                "path_params": route["path_params"],
                "source": {"file": path.name, "function": route["function"]},
            }
    return routes


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

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # type: ignore[override]
        self._process(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # type: ignore[override]
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
    spec_routes: dict[tuple[str, str], dict[str, set[str]]] = {}
    lines = OPENAPI_PATH.read_text().splitlines()
    in_paths = False
    current_path: str | None = None
    current_method: str | None = None
    in_parameters = False
    current_param: dict[str, str] | None = None

    for raw_line in lines:
        if not in_paths:
            if raw_line.strip() == "paths:":
                in_paths = True
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
            spec_routes.setdefault(("", current_path), {})
            continue
        if indent == 4 and stripped.endswith(":") and current_path:
            token = stripped[:-1].lower()
            if token in HTTP_METHODS:
                current_method = token
                spec_routes[("", current_path)][current_method] = []
                in_parameters = False
                continue
            current_method = None
        if indent == 6 and current_method:
            if stripped == "parameters:":
                in_parameters = True
                spec_routes[("", current_path)][current_method] = []
                continue
            else:
                in_parameters = False
        if indent == 8 and in_parameters and current_method:
            if stripped.startswith("- name:"):
                name = stripped.split(":", 1)[1].strip().strip("'\"")
                current_param = {"name": name}
                spec_routes[("", current_path)][current_method].append(current_param)
                continue
        if indent >= 10 and in_parameters and current_method and current_param is not None:
            if stripped.startswith("in:"):
                current_param["in"] = stripped.split(":", 1)[1].strip()
            continue
        if indent <= 6:
            current_param = None
            in_parameters = in_parameters and stripped == "parameters:"

    result: dict[tuple[str, str], dict[str, set[str]]] = {}
    for (_, path), methods in spec_routes.items():
        for method, params in methods.items():
            query_params = {p["name"] for p in params if p.get("in") == "query"}
            path_params = {p["name"] for p in params if p.get("in") == "path"}
            result[(method, path)] = {
                "query_params": query_params,
                "path_params": path_params,
            }
    return result


def test_openapi_routes_match_code():
    code_routes = _collect_code_routes()
    spec_routes_raw = _collect_spec_routes()
    spec_routes = {(method, path): info for (method, path), info in spec_routes_raw.items() if method}

    # Normalise spec keys to align with code keys
    spec_keys = set(spec_routes.keys())
    code_keys = set(code_routes.keys())

    missing_in_spec = sorted(code_keys - spec_keys)
    extra_in_spec = sorted(spec_keys - code_keys)

    assert not missing_in_spec, f"Routes missing from OpenAPI: {missing_in_spec}"
    assert not extra_in_spec, f"Extra routes in OpenAPI not in code: {extra_in_spec}"

    for key in sorted(code_keys):
        code_info = code_routes[key]
        spec_info = spec_routes[key]
        assert code_info["path_params"] == spec_info["path_params"], (
            f"Path parameters mismatch for {key}: code={sorted(code_info['path_params'])}, "
            f"spec={sorted(spec_info['path_params'])}"
        )
        assert code_info["query_params"] == spec_info["query_params"], (
            f"Query parameters mismatch for {key}: code={sorted(code_info['query_params'])}, "
            f"spec={sorted(spec_info['query_params'])}"
        )
