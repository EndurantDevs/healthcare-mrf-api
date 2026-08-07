# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Negative route and OpenAPI guards for the inactive GET-only search mode."""

from __future__ import annotations

import ast
from pathlib import Path

import yaml

HTTP_METHODS = frozenset({"get", "post", "put", "delete", "patch", "options", "head"})
PRICING_ENDPOINT_PATH = Path("api/endpoint/pricing.py")
OPENAPI_PATH = Path("doc/openapi.yaml")
CANONICAL_ROUTE_FRAGMENT = "/providers/search-by-procedure"
CANONICAL_OPENAPI_PATH = f"/pricing{CANONICAL_ROUTE_FRAGMENT}"
BILLING_SEARCH_PARAMETERS = frozenset(
    {"billing_entity_ref", "cursor", "healthporta_plan_id"}
)
REMOVED_RAW_SELECTOR_PARAMETERS = frozenset(
    {
        "billing_identity",
        "billing_npi",
        "ein",
        "tax_id",
        "tax_identity",
        "tax_identity_type",
        "tax_identity_value",
        "tin",
    }
)


def _canonical_route_methods() -> set[str]:
    tree = ast.parse(PRICING_ENDPOINT_PATH.read_text())
    methods: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not decorator.args:
                continue
            function = decorator.func
            route = decorator.args[0]
            if (
                isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Name)
                and function.value.id == "blueprint"
                and function.attr in HTTP_METHODS
                and isinstance(route, ast.Constant)
                and route.value == CANONICAL_ROUTE_FRAGMENT
            ):
                methods.add(function.attr)
    return methods


def test_canonical_billing_search_route_and_openapi_remain_get_only() -> None:
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    path_item = spec["paths"][CANONICAL_OPENAPI_PATH]
    operation = path_item["get"]
    documented_methods = set(path_item).intersection(HTTP_METHODS)
    query_parameter_names = {
        parameter["name"]
        for parameter in operation["parameters"]
        if parameter["in"] == "query"
    }

    assert _canonical_route_methods() == {"get"}
    assert documented_methods == {"get"}
    assert "requestBody" not in operation
    assert BILLING_SEARCH_PARAMETERS.issubset(query_parameter_names)
    assert REMOVED_RAW_SELECTOR_PARAMETERS.isdisjoint(query_parameter_names)

    for alias_path in (
        "/pricing/providers/by-procedure",
        "/pricing/providers/by-service",
        "/pricing/physicians/by-service",
    ):
        alias_operation = spec["paths"].get(alias_path, {}).get("get")
        if alias_operation is None:
            continue
        alias_parameter_names = {
            parameter["name"]
            for parameter in alias_operation["parameters"]
            if parameter["in"] == "query"
        }
        assert BILLING_SEARCH_PARAMETERS.isdisjoint(alias_parameter_names)
