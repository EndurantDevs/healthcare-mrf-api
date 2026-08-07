# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolver-first clinical-intent contract for raw pricing search."""

from pathlib import Path

import yaml

OPENAPI_PATH = Path("doc/openapi.yaml")
RAW_SEARCH_PATHS = (
    "/pricing/providers/search-by-procedure",
    "/pricing/providers/by-procedure",
)


def test_procedure_search_documents_resolver_only_clinical_intent():
    """Keep resolver inputs out of raw pricing-search parameter contracts."""

    spec = yaml.safe_load(OPENAPI_PATH.read_text())
    resolver_parameters = _parameter_names(
        spec,
        "/pricing/procedure-taxonomy/resolve",
    )
    assert {"clinical_intent", "intent"}.issubset(resolver_parameters)

    for path in RAW_SEARCH_PATHS:
        operation = spec["paths"][path]["get"]
        assert not {"clinical_intent", "intent"}.intersection(
            _parameter_names(spec, path)
        )
        description = operation["description"]
        assert "resolver-only" in description
        assert "/pricing/procedure-taxonomy/resolve" in description
        assert "recommended_mode=hard_filter" in description
        assert "provider_filter.taxonomy_codes" in description
        assert "provider_filter.primary_only" in description
        assert "provider_filter.include_subspecialties" in description
        response_schema = operation["responses"]["400"]["content"]["application/json"][
            "schema"
        ]
        if path == "/pricing/providers/search-by-procedure":
            assert response_schema == {
                "oneOf": [
                    {"$ref": "#/components/schemas/Error"},
                    {"$ref": "#/components/schemas/BillingSearchErrorResponse"},
                ]
            }
        else:
            assert response_schema == {"$ref": "#/components/schemas/Error"}


def _parameter_names(spec: dict, path: str) -> set[str]:
    return {parameter["name"] for parameter in spec["paths"][path]["get"]["parameters"]}
