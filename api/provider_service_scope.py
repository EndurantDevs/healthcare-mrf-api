# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-service selector boundaries and canonical membership predicates."""

from __future__ import annotations

from urllib.parse import parse_qs

from sanic.exceptions import InvalidUsage
from sqlalchemy import func, literal, select

from api.network_address_scope import current_network_address_scope


def _canonical_network_procedure_clause(args, import_context, provider_npi, *, query_string=None):
    """Restrict claims providers to the pinned network before count and pagination."""
    network_scope = current_network_address_scope()
    if network_scope is None:
        return None
    raw_args = parse_qs(query_string or "", keep_blank_values=True)
    unsupported_fields = (
        "billing_entity_ref",
        "plan_id",
        "plan_external_id",
        "plan_release_id",
        "source_key",
        "snapshot_id",
    )
    if import_context is not None or any(name in args or name in raw_args for name in unsupported_fields):
        raise InvalidUsage("Canonical network procedure queries support the claims lane only.")
    addresses = network_scope.address_relation
    return (
        select(literal(1))
        .select_from(addresses)
        .where(func.coalesce(addresses.c.npi, addresses.c.inferred_npi) == provider_npi)
        .exists()
        .params(_canonical_network_ids=list(network_scope.network_ids))
    )


def _reject_resolver_only_procedure_search_params(args) -> None:
    """Direct callers must resolve clinical intent before pricing search."""

    resolver_only_param = next(
        (param_name for param_name in ("clinical_intent", "intent") if args.get(param_name) is not None),
        None,
    )
    if resolver_only_param is None:
        return
    raise InvalidUsage(
        f"Parameter '{resolver_only_param}' is resolver-only. Call "
        "/pricing/procedure-taxonomy/resolve first. When "
        "recommended_mode=hard_filter, copy provider_filter.taxonomy_codes, "
        "provider_filter.primary_only, and "
        "provider_filter.include_subspecialties into the pricing search."
    )
