# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import ast
import re

import yaml

from tests.openapi_route_contract_support import HIDDEN_RUNTIME_ALIASES
from tests.test_openapi_spec import (
    ENDPOINT_DIR,
    OPENAPI_PATH,
    _collect_query_params,
    _collect_spec_routes,
)


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


def test_openapi_operation_ids_are_present_and_unique():
    text = OPENAPI_PATH.read_text()
    operation_ids = re.findall(
        r"^\s+operationId:\s+([A-Za-z0-9_]+)\s*$",
        text,
        flags=re.MULTILINE,
    )
    spec_routes = {
        (method, path)
        for (method, path), _route_info in _collect_spec_routes().items()
        if method
    }

    assert len(operation_ids) == len(spec_routes)
    assert len(operation_ids) == len(set(operation_ids))
    assert not (HIDDEN_RUNTIME_ALIASES & spec_routes)


def test_doctor_search_contract_documents_cards_and_filter_defaults():
    """Keep doctor-search cards and filters aligned with the runtime contract."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    schemas = spec["components"]["schemas"]
    npi_all = spec["paths"]["/npi/all"]["get"]
    npi_near = spec["paths"]["/npi/near/"]["get"]

    all_parameters_by_name = {
        parameter_by_field["name"]: parameter_by_field
        for parameter_by_field in npi_all["parameters"]
    }
    assert {"name_like", "page", "offset", "page_size"} <= set(
        all_parameters_by_name
    )
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
        parameter_by_field["name"]: parameter_by_field
        for parameter_by_field in npi_near["parameters"]
    }
    assert near_parameters_by_name["view"]["schema"]["enum"] == ["card"]
    assert near_parameters_by_name["radius"]["schema"] == {
        "type": "number",
        "minimum": 0,
        "maximum": 100,
    }

    assert schemas["NpiSearchResponse"]["properties"]["total_source"][
        "type"
    ] == "string"
    assert schemas["NpiCard"]["additionalProperties"] is False

    detail_parameters_by_name = {
        parameter_by_field["name"]: parameter_by_field
        for parameter_by_field in spec["paths"]["/npi/id/{npi}"]["get"][
            "parameters"
        ]
    }
    assert detail_parameters_by_name["show"]["schema"]["enum"] == ["chain"]
    assert detail_parameters_by_name["view"]["schema"]["enum"] == [
        "full",
        "summary",
    ]
    assert "enrichment" in detail_parameters_by_name["view"][
        "description"
    ].lower()



def test_procedure_provider_primary_taxonomy_filter_defaults_to_true():
    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec["paths"][path]["get"]["parameters"]
        primary_only = next(
            parameter_by_field
            for parameter_by_field in parameters
            if parameter_by_field.get("name") == "primary_only"
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
