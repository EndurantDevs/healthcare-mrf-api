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
    "zip_radius_miles include_mail_addresses min_address_sources".split()
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


def group_plan_member_queries(
    tables_by_snapshot, params, candidate_cte, *, exact_zip_count, local_candidates, filter_sql
):
    """Build a member page and the optional ZIP-local count from validated read inputs."""
    page_join_sql = "JOIN local_provider_npis local ON local.npi = gm.npi" if local_candidates else ""
    page_filter_sql = "" if local_candidates else filter_sql
    bounded_member_streams = []
    for stream_index, (snapshot_key, table) in enumerate(sorted(tables_by_snapshot.items())):
        stream_key_parameter = f"page_snapshot_key_{stream_index}"
        params[stream_key_parameter] = snapshot_key
        if exact_zip_count:
            bounded_member_streams.append(
                f"SELECT 1 FROM {table} gm WHERE gm.snapshot_key = :{stream_key_parameter} AND gm.npi = local.npi"
            )
            continue
        bounded_member_streams.append(
            f"""(
                SELECT DISTINCT gm.npi
                  FROM {table} gm
                  {page_join_sql}
                 WHERE gm.snapshot_key = :{stream_key_parameter}
                   AND gm.npi BETWEEN :npi_min AND :npi_max
                   AND gm.npi > :cursor_npi{page_filter_sql}
                 ORDER BY gm.npi
                 LIMIT :limit
            )"""
        )
    member_union = " UNION ALL ".join(bounded_member_streams)
    if exact_zip_count:
        # Probe only the selected table and key pairs, stopping at the first match for each local NPI.
        zip_members_sql = f"""
          FROM local_provider_npis local
          CROSS JOIN LATERAL (
              {member_union}
               LIMIT 1
          ) member
         WHERE local.npi BETWEEN :npi_min AND :npi_max
        """
        page_sql = f"""
            {candidate_cte}SELECT local.npi {zip_members_sql}
               AND local.npi > :cursor_npi
             ORDER BY local.npi
             LIMIT :limit
            """
        return page_sql, f"{candidate_cte}SELECT COUNT(*) {zip_members_sql}"

    page_sql = f"""
        {candidate_cte}SELECT DISTINCT bounded_group_member.npi
          FROM ({member_union}) AS bounded_group_member
         ORDER BY bounded_group_member.npi
         LIMIT :limit
        """
    return page_sql, None
