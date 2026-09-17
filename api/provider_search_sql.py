# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from collections import defaultdict
from textwrap import dedent
from typing import Any, Iterable, Sequence

import sanic.exceptions

from api.plan_release_serving import (
    normalize_plan_release_id,
    resolve_plan_release_serving,
)
from api.ptg2_serving import _ptg2_npi_scope_table


def build_provider_name_where(
    *,
    prefix: str,
    name_clause: str,
    name_parameters: dict[str, object],
    first_name: str | None,
    last_name: str | None,
    organization_name: str | None,
    organization_expression: str,
    entity_type_code: int | None,
) -> tuple[str, dict[str, object]]:
    """Combine normalized provider-name predicates and parameters."""

    clauses = [name_clause] if name_clause else []
    parameters_by_name = dict(name_parameters)
    if first_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_first_name, '')) LIKE :first_name")
        parameters_by_name["first_name"] = f"%{first_name.lower()}%"
    if last_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_last_name, '')) LIKE :last_name")
        parameters_by_name["last_name"] = f"%{last_name.lower()}%"
    if organization_name:
        clauses.append(f"({organization_expression} LIKE :organization_name)")
        parameters_by_name["organization_name"] = f"%{organization_name.lower()}%"
    if entity_type_code is not None:
        clauses.append(f"{prefix}entity_type_code = :entity_type_code")
        parameters_by_name["entity_type_code"] = entity_type_code
    return " AND ".join(clauses), parameters_by_name


def broad_name_page_sql(
    npi_where: str,
    address_table_sql: str,
    provider_npi_sql: str,
    address_clauses: Sequence[str],
) -> str:
    """Build a bounded NPI-ordered broad-name page with eligible addresses."""

    return f"""
    SELECT b.npi
      FROM mrf.npi AS b
     WHERE {npi_where}
       AND EXISTS (
           SELECT 1
             FROM {address_table_sql} AS c
            WHERE {provider_npi_sql} = b.npi
              AND {' and '.join(address_clauses)}
       )
    """


def taxonomy_codes_subquery(conditions: str) -> str:
    """Build the NUCC code-array subquery used by provider searches."""

    return (
        dedent(
            """
            (
                SELECT ARRAY_AGG(code) AS codes,
                       ARRAY_AGG(int_code) AS int_codes
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def primary_taxonomy_predicate(indent: str) -> str:
    """Build an indented primary-taxonomy SQL predicate."""

    return f"\n{indent}AND UPPER(COALESCE(provider_taxonomy.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'"


def provider_taxonomy_lateral_join(
    address_alias: str = "c",
    taxonomy_alias: str = "q",
    code_placeholders: Sequence[str] = (),
    provider_npi_sql: str | None = None,
    primary_only: bool = False,
) -> str:
    """Return an NPI-first taxonomy probe for selective provider searches."""

    taxonomy_code_predicate = (
        f"provider_taxonomy.healthcare_provider_taxonomy_code IN ({', '.join(code_placeholders)})"
        if code_placeholders
        else f"provider_taxonomy.healthcare_provider_taxonomy_code = ANY({taxonomy_alias}.codes)"
    )
    provider_npi = provider_npi_sql or f"{address_alias}.npi"
    primary_predicate = primary_taxonomy_predicate("           ") if primary_only else ""
    return f"""
    JOIN LATERAL (
        SELECT 1
          FROM mrf.npi_taxonomy AS provider_taxonomy
         WHERE provider_taxonomy.npi = {provider_npi}
           AND {taxonomy_code_predicate}{primary_predicate}
         LIMIT 1
    ) AS provider_taxonomy_match ON TRUE
    """


def provider_taxonomy_code_parameters(
    taxonomy_codes: Sequence[str],
    parameter_prefix: str,
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return scalar taxonomy parameters that keep composite indexes usable."""

    parameters_by_name = {
        f"{parameter_prefix}_{index}": str(taxonomy_code).upper() for index, taxonomy_code in enumerate(taxonomy_codes)
    }
    placeholders = tuple(f":{parameter_name}" for parameter_name in parameters_by_name)
    return parameters_by_name, placeholders


def direct_name_taxonomy_cte(
    code_placeholders: Sequence[str],
    npi_where: str,
    npi_projection: str,
    primary_only: bool,
) -> str:
    """Build the normalized-taxonomy CTE for a name and code search."""

    primary_predicate = primary_taxonomy_predicate("           ") if primary_only else ""
    return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT DISTINCT {npi_projection}
          FROM mrf.npi_taxonomy AS provider_taxonomy
          JOIN mrf.npi AS b
            ON b.npi = provider_taxonomy.npi
         WHERE provider_taxonomy.healthcare_provider_taxonomy_code IN ({", ".join(code_placeholders)})
           AND ({npi_where}){primary_predicate}
    )
    """


def projected_name_taxonomy_cte(
    code_placeholders: Sequence[str],
    npi_where: str,
    npi_projection: str,
) -> str:
    """Build the projected-taxonomy CTE for a name and code search."""

    return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT {npi_projection}
          FROM mrf.npi AS b
         WHERE b.search_taxonomy_codes && ARRAY[{", ".join(code_placeholders)}]::varchar[]
           AND ({npi_where})
    )
    """


def taxonomy_match_sql(
    taxonomy_conditions: str,
    code_placeholders: Sequence[str],
    primary_only: bool,
) -> str:
    """Build the taxonomy portion of a provider membership join."""

    primary_predicate = primary_taxonomy_predicate("           ") if primary_only else ""
    if code_placeholders:
        return (
            "WHERE provider_taxonomy.healthcare_provider_taxonomy_code "
            f"IN ({', '.join(code_placeholders)}){primary_predicate}"
        )
    primary_where = primary_taxonomy_predicate("         ").replace("AND ", "WHERE ", 1) if primary_only else ""
    return f"""
          JOIN (
                SELECT code
                  FROM mrf.nucc_taxonomy
                 WHERE {taxonomy_conditions}
          ) AS matched_taxonomy
            ON matched_taxonomy.code =
               provider_taxonomy.healthcare_provider_taxonomy_code
          {primary_where}
        """


def provider_taxonomy_matched_npi_cte(
    taxonomy_conditions: str,
    *,
    code_placeholders: Sequence[str] = (),
    npi_where: str = "",
    npi_projection: str = "b.npi",
    primary_only: bool = False,
    is_projection_enabled: bool = True,
) -> str:
    """Materialize the smaller side of a provider/taxonomy intersection."""

    if code_placeholders and npi_where:
        if primary_only or not is_projection_enabled:
            return direct_name_taxonomy_cte(code_placeholders, npi_where, npi_projection, primary_only)
        return projected_name_taxonomy_cte(code_placeholders, npi_where, npi_projection)
    match_sql = taxonomy_match_sql(
        taxonomy_conditions,
        code_placeholders,
        primary_only,
    )
    return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT DISTINCT fn.*
          FROM filtered_npi AS fn
          JOIN mrf.npi_taxonomy AS provider_taxonomy
            ON provider_taxonomy.npi = fn.npi
          {match_sql}
    )
    """


def is_location_first_taxonomy_filter(
    use_taxonomy_filter: bool,
    candidate_filters: Iterable[Any],
) -> bool:
    """Return whether selective candidates should drive taxonomy probes."""

    return use_taxonomy_filter and any(candidate not in (None, "", (), [], {}) for candidate in candidate_filters)


def taxonomy_classification_subquery(conditions: str) -> str:
    """Build a NUCC classification subquery for grouped counts."""

    return (
        dedent(
            """
            (
                SELECT int_code,
                       classification
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def normalized_plan_release_id(raw_plan_release_id: Any) -> str | None:
    """Validate and normalize an optional immutable plan release identifier."""

    requested_plan_release_id = str(raw_plan_release_id or "").strip()
    if not requested_plan_release_id:
        return None
    plan_release_id = normalize_plan_release_id(requested_plan_release_id)
    if plan_release_id is None:
        raise sanic.exceptions.InvalidUsage(
            "plan_release_id must be hprelease_ followed by 26 uppercase Crockford base32 characters"
        )
    return plan_release_id


async def plan_release_npi_scope(
    session: Any,
    raw_plan_release_id: Any,
) -> tuple[str | None, dict[str, list[int]]]:
    """Return an exact immutable provider-membership subquery for a plan release."""

    plan_release_id = normalized_plan_release_id(raw_plan_release_id)
    if plan_release_id is None:
        return None, {}
    if session is None:
        raise RuntimeError("SQLAlchemy session not available on request context")

    selection = await resolve_plan_release_serving(session, plan_release_id)
    if selection is None:
        return "SELECT NULL::BIGINT AS npi WHERE FALSE", {}
    tables_by_snapshot = selection.network_tables_by_snapshot()
    if tables_by_snapshot is None:
        raise RuntimeError("published plan release has incomplete network bindings")

    snapshot_keys_by_table: dict[str, set[int]] = defaultdict(set)
    for binding in selection.in_network_bindings:
        serving_tables = tables_by_snapshot[binding.snapshot_id]
        if serving_tables.shared_snapshot_key is None:
            raise RuntimeError("published plan snapshot is not bound to strict shared-block storage")
        snapshot_keys_by_table[_ptg2_npi_scope_table(serving_tables)].add(int(serving_tables.shared_snapshot_key))
    if not snapshot_keys_by_table:
        return "SELECT NULL::BIGINT AS npi WHERE FALSE", {}
    return plan_scope_query(snapshot_keys_by_table)


def plan_scope_query(
    snapshot_keys_by_table: dict[str, set[int]],
) -> tuple[str, dict[str, list[int]]]:
    """Build a parameterized union across plan-scope storage versions."""

    query_parameters_by_name: dict[str, list[int]] = {}
    scope_queries = []
    for index, (table_name, snapshot_keys) in enumerate(sorted(snapshot_keys_by_table.items())):
        parameter_name = f"plan_scope_snapshot_keys_{index}"
        query_parameters_by_name[parameter_name] = sorted(snapshot_keys)
        scope_queries.append(f"SELECT npi FROM {table_name} WHERE snapshot_key = ANY(:{parameter_name})")
    return " UNION ALL ".join(scope_queries), query_parameters_by_name
