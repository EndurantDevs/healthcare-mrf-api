# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Route visibility and query-parser exceptions for OpenAPI parity tests."""

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
    (
        "get",
        "/pricing/physicians/{npi}/services/{code_system}/{code}/estimated-cost-level",
    ),
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
ROUTE_QUERY_PARAM_ADDITIONS = {
    # The gateway resolves the public plan ID, while the isolated helper parses
    # the internal selector and cursor outside the legacy handler's AST.
    ("get", "/pricing/providers/search-by-procedure"): {
        "billing_entity_ref",
        "cursor",
        "healthporta_plan_id",
    },
}
ROUTE_QUERY_PARAM_REMOVALS = {
    # Billing search is transport-bound to the canonical path, not aliases.
    ("get", "/pricing/providers/by-procedure"): {"billing_entity_ref"},
}
