# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed group directories use admitted sources and the pinned office heap."""

from sanic.exceptions import InvalidUsage, ServiceUnavailable

from api.network_address_scope import (
    _validate_network_scope_inputs,
    current_network_address_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.provider_service_source_policy import admitted_plan_release_bindings

GROUP_PLAN_NATIVE_FIELDS = frozenset(
    "plan_release_id plan_id market_type source_key cursor limit enrich count specialty classification "
    "taxonomy_codes provider_sex_code include_subspecialties primary_only city state zip5 "
    "zip_radius_miles include_mail_addresses".split()
)


def group_plan_arguments(request, native_args):
    """Only the managed dispatcher may replace the real request's native options."""
    scope = current_network_address_scope()
    if native_args is not None and scope is None:
        raise ServiceUnavailable("Managed group directory scope is unavailable.")
    args = request.args if native_args is None else native_args
    if scope is not None:
        if not args.get("plan_release_id"):
            raise ServiceUnavailable("Managed group directory release is unavailable.")
        for name, maximum in (("limit", 1000), ("cursor", 9999999999)):
            raw = args.get(name)
            if raw not in (None, "") and (
                type(raw) is not str or not raw.isascii() or not raw.isdecimal() or not 1 <= int(raw) <= maximum
            ):
                raise InvalidUsage(f"Parameter '{name}' is invalid")
    return args


async def group_plan_release_bindings(session, selection):
    """Keep the complete recipe; remove denied contributors before any native read."""
    if current_network_address_scope() is not None and selection is None:
        raise ServiceUnavailable("Managed group directory release is unavailable.")
    if selection is None:
        return ()
    try:
        return await admitted_plan_release_bindings(session, selection)
    except PlanPricingProjectionUnavailable as error:
        raise ServiceUnavailable("Managed group directory sources are unavailable.") from error


def group_plan_address_source(schema):
    """Use the exact retained address predicate without consulting current legacy heads."""
    scope = current_network_address_scope()
    if scope is None:
        return None
    try:
        _validate_network_scope_inputs(
            scope.manifest,
            scope.network_ids,
            scope.access_scope_sha256,
            scope.excluded_network_ids,
            scope.organization_policy,
        )
        relation = scoped_address_relation_sql(f"{schema}.entity_address_unified")
        parameters = scoped_address_parameters({})
    except (ValueError, TypeError) as error:
        raise ServiceUnavailable("Managed group directory offices are unavailable.") from error
    # Coverage arrays belong to this heap; a current plan bridge is a different snapshot.
    return (relation, True, True, False), parameters
