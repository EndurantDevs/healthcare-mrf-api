# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""GIN-indexed network operating-state filter resolution for ACA plan search."""

from __future__ import annotations

import re

import asyncpg
from sanic.exceptions import BadRequest
from sqlalchemy import String, bindparam, func, literal, select
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER

from api.network_address_scope import current_network_address_scope
from db.plan_release_storage import storage_identifier
from db.registry_schema import registry_schema
from process.network_published_api_pin import read_published_api_pin
from process.network_serving_read import NetworkServingReadUnavailable
from process.network_stats_contract import SUMMARY_TABLE
from process.registry_effective_network_catalog import CATALOG_GENERATION, effective_catalog_sql

_STATE_CODE_RE = re.compile(r"[A-Z0-9]{2,8}\Z")
_VALID_MODES = frozenset({"any", "all"})
_VALID_SCOPES = frozenset({"any", "core"})


class PlanNetworkStatsUnavailable(RuntimeError):
    """Current network serving generation is unavailable or predates precomputed statistics."""


def _raw_param_values(args, name: str) -> list[str]:
    getlist = getattr(args, "getlist", None)
    if callable(getlist):
        raw_values = getlist(name)
    else:
        single_entry = args.get(name)
        if single_entry is None:
            raw_values = []
        elif isinstance(single_entry, (list, tuple)):
            raw_values = list(single_entry)
        else:
            raw_values = [single_entry]
    return [str(entry) for entry in raw_values if entry is not None]


def _parse_state_tokens(raw_states: list[str]) -> tuple[str, ...]:
    if not raw_states:
        return ()
    if sum(len(chunk) for chunk in raw_states) > 512:
        raise BadRequest("Parameter 'network_state' exceeds maximum length")
    seen_states: set[str] = set()
    for chunk in raw_states:
        for part in chunk.split(","):
            code = part.strip().upper()
            if not code or _STATE_CODE_RE.fullmatch(code) is None:
                raise BadRequest("Parameter 'network_state' must contain 2-8 character state codes")
            seen_states.add(code)
            if len(seen_states) > 60:
                raise BadRequest("Parameter 'network_state' supports at most 60 state codes")
    return tuple(sorted(seen_states))


def _parse_optional_choice(
    raw_entries: list[str], param_name: str, allowed_choices: frozenset[str], *, has_states: bool
) -> str:
    if len(raw_entries) > 1:
        raise BadRequest(f"Parameter '{param_name}' must not be repeated")
    if not raw_entries:
        return "any"
    cleaned_choice = raw_entries[0].strip().lower()
    if cleaned_choice not in allowed_choices:
        options_label = "' or '".join(sorted(allowed_choices))
        raise BadRequest(f"Parameter '{param_name}' must be '{options_label}'")
    if not has_states:
        raise BadRequest(f"Parameter 'network_state' is required when '{param_name}' is provided")
    return cleaned_choice


def parse_plan_network_state_params(args) -> tuple[tuple[str, ...], str, str]:
    """Parse and validate network_state plus optional network_state_mode and network_state_scope."""
    raw_states = _raw_param_values(args, "network_state")
    raw_modes = _raw_param_values(args, "network_state_mode")
    raw_scopes = _raw_param_values(args, "network_state_scope")
    if len(raw_modes) > 1:
        raise BadRequest("Parameter 'network_state_mode' must not be repeated")
    if len(raw_scopes) > 1:
        raise BadRequest("Parameter 'network_state_scope' must not be repeated")
    states = _parse_state_tokens(raw_states)
    mode = _parse_optional_choice(raw_modes, "network_state_mode", _VALID_MODES, has_states=bool(states))
    scope_kind = _parse_optional_choice(raw_scopes, "network_state_scope", _VALID_SCOPES, has_states=bool(states))
    return states, mode, scope_kind


def _matching_networks_sql(stats_schema: str, *, state_mode: str, state_scope: str, has_selected: bool) -> str:
    stats_rel = f"{storage_identifier(stats_schema)}.{storage_identifier(SUMMARY_TABLE)}"
    col = "core_states" if state_scope == "core" else "states"
    op = "@>" if state_mode == "all" else "&&"
    scope_clause = (
        "AND st.network_id = ANY($2::integer[])" if has_selected else "AND NOT (st.network_id = ANY($2::integer[]))"
    )
    return (
        f"SELECT st.network_id FROM ONLY {stats_rel} st "
        f'WHERE st.{col} {op} ($1::text[] COLLATE "C") {scope_clause} ORDER BY st.network_id'
    )


def _aca_bindings_sql(records_sql: str, *, pnt_relation: str | None) -> str:
    tier_guard = (
        f"""AND (
            b.record_json->>'producer_id' = 'registry-native-network-source-v1'
            OR EXISTS (
                SELECT 1 FROM {pnt_relation} pnt
                WHERE pnt.plan_id = left(b.record_json->'source_scope_json'->>'plan_id', 14)
                  AND pnt.year = (b.record_json->'source_scope_json'->>'plan_year')::integer
                  AND pnt.issuer_id = (b.record_json->'source_scope_json'->>'issuer_id')::integer
                  AND pnt.checksum_network = (b.record_json->'source_scope_json'->>'checksum_network')::bigint
            )
        )"""
        if pnt_relation is not None
        else ""
    )
    return f"""WITH records AS MATERIALIZED (
        {records_sql}
    ), active_networks AS MATERIALIZED (
        SELECT (r.record_json->>'network_id')::integer AS network_id
        FROM records r
        WHERE r.record_kind = 'network'
          AND jsonb_typeof(r.record_json->'network_id') = 'number'
          AND r.record_json->>'network_id' = r.record_key
          AND r.record_json->'archived' = 'false'::jsonb
          AND (r.record_json->>'network_id')::integer = ANY($1::integer[])
    )
    SELECT DISTINCT
        left(b.record_json->'source_scope_json'->>'plan_id', 14) AS plan_id,
        (b.record_json->'source_scope_json'->>'plan_year')::integer AS plan_year,
        (b.record_json->'source_scope_json'->>'issuer_id')::integer AS issuer_id,
        b.record_json->'source_scope_json'->>'state' AS state
    FROM records b
    JOIN active_networks an
      ON an.network_id = CASE
          WHEN jsonb_typeof(b.record_json->'network_id') = 'number'
           AND pg_input_is_valid(b.record_json->>'network_id', 'integer')
          THEN (b.record_json->>'network_id')::integer
      END
    WHERE b.record_kind = 'network_binding'
      AND b.record_json->'archived' = 'false'::jsonb
      AND b.record_json->>'source_system' = 'aca'
      AND jsonb_typeof(b.record_json->'source_scope_json'->'plan_id') = 'string'
      AND b.record_json->'source_scope_json'->>'plan_id' <> ''
      AND jsonb_typeof(b.record_json->'source_scope_json'->'plan_year') = 'number'
      AND pg_input_is_valid(b.record_json->'source_scope_json'->>'plan_year', 'integer')
      AND jsonb_typeof(b.record_json->'source_scope_json'->'issuer_id') = 'string'
      AND pg_input_is_valid(b.record_json->'source_scope_json'->>'issuer_id', 'integer')
      AND jsonb_typeof(b.record_json->'source_scope_json'->'state') = 'string'
      AND b.record_json->'source_scope_json'->>'state' <> ''
      AND jsonb_typeof(b.record_json->'source_scope_json'->'checksum_network') = 'number'
      AND pg_input_is_valid(b.record_json->'source_scope_json'->>'checksum_network', 'bigint')
      {tier_guard}"""


async def _resolve_driver_and_manifest(session, control_schema: str):
    sa_conn = await session.connection()
    raw_conn = await sa_conn.get_raw_connection()
    driver = raw_conn.driver_connection
    bound_scope = current_network_address_scope()
    if bound_scope is not None:
        return driver, bound_scope.manifest, bound_scope
    try:
        manifest, _, _ = await read_published_api_pin(driver, None, control_schema, approved=False)
    except (NetworkServingReadUnavailable, asyncpg.PostgresError, ValueError, KeyError, TypeError) as err:
        raise PlanNetworkStatsUnavailable("Published network generation is unavailable") from err
    return driver, manifest, None


async def _fetch_matching_network_ids(
    driver, stats_schema: str, *, states: tuple[str, ...], state_mode: str, state_scope: str, bound_scope
) -> list[int]:
    has_selected = bound_scope is not None and bound_scope.network_ids is not None
    scope_ids = (
        list(bound_scope.network_ids)
        if has_selected
        else (list(bound_scope.excluded_network_ids) if bound_scope and bound_scope.excluded_network_ids else [])
    )
    try:
        network_rows = await driver.fetch(
            _matching_networks_sql(
                stats_schema, state_mode=state_mode, state_scope=state_scope, has_selected=has_selected
            ),
            list(states),
            scope_ids,
        )
    except asyncpg.PostgresError as err:
        raise PlanNetworkStatsUnavailable("Precomputed network statistics table is unavailable") from err
    return [int(row["network_id"]) for row in network_rows]


async def _fetch_aca_binding_rows(
    driver, manifest, control_schema: str, matching_network_ids: list[int], plan_network_tier_table
) -> list:
    control_ns = storage_identifier(control_schema)
    try:
        if getattr(manifest, "source_generations", {}).get(CATALOG_GENERATION) is not None:
            effective_sql = await effective_catalog_sql(
                driver, manifest, control_schema, selected_networks="$1::integer[]"
            )
            records_sql = f"SELECT approved_revision, record_kind, record_key, record_json FROM {effective_sql} eff"
        else:
            revision_id = int(manifest.approved_custom_revision)
            records_sql = (
                f"SELECT approved_revision, record_kind, record_key, record_json "
                f"FROM ONLY {control_ns}.registry_approved_record WHERE approved_revision = {revision_id}"
            )
        tier_schema = str(plan_network_tier_table.schema or "public")
        tier_qualified = f"{storage_identifier(tier_schema)}.{storage_identifier(str(plan_network_tier_table.name))}"
        has_tier_table = await driver.fetchval("SELECT to_regclass($1) IS NOT NULL", tier_qualified)
        return await driver.fetch(
            _aca_bindings_sql(records_sql, pnt_relation=tier_qualified if has_tier_table else None),
            matching_network_ids,
        )
    except (asyncpg.PostgresError, ValueError, KeyError, TypeError) as err:
        raise PlanNetworkStatsUnavailable("Canonical ACA network bindings are unavailable") from err


def _unnested_plan_exists_clause(binding_rows: list, plan_table):
    plan_ids = [row["plan_id"] for row in binding_rows]
    plan_years = [int(row["plan_year"]) for row in binding_rows]
    issuer_ids = [int(row["issuer_id"]) for row in binding_rows]
    plan_states = [row["state"] for row in binding_rows]
    matched_plans = select(
        func.unnest(bindparam("_ns_plan_ids", plan_ids, type_=ARRAY(String))).label("plan_id"),
        func.unnest(bindparam("_ns_plan_years", plan_years, type_=ARRAY(INTEGER))).label("plan_year"),
        func.unnest(bindparam("_ns_issuer_ids", issuer_ids, type_=ARRAY(INTEGER))).label("issuer_id"),
        func.unnest(bindparam("_ns_states", plan_states, type_=ARRAY(String))).label("state"),
    ).subquery("network_state_plans")
    return (
        select(1)
        .select_from(matched_plans)
        .where(
            matched_plans.c.plan_id == plan_table.c.plan_id,
            matched_plans.c.plan_year == plan_table.c.year,
            matched_plans.c.issuer_id == plan_table.c.issuer_id,
            matched_plans.c.state == plan_table.c.state,
        )
        .exists()
    )


async def resolve_plan_network_state_clause(
    session,
    *,
    states: tuple[str, ...],
    state_mode: str,
    state_scope: str,
    plan_table,
    plan_network_tier_table,
):
    """Build an indexed SQLAlchemy predicate restricting ACA plans by network operating states."""
    control_schema = registry_schema()
    driver, manifest, bound_scope = await _resolve_driver_and_manifest(session, control_schema)
    layout = getattr(manifest, "compact_layout", None)
    stats_schema = getattr(layout, "network_stats_state_search", None)
    if layout is None or stats_schema is None:
        raise PlanNetworkStatsUnavailable("Current serving generation predates precomputed network statistics")

    matching_network_ids = await _fetch_matching_network_ids(
        driver, stats_schema, states=states, state_mode=state_mode, state_scope=state_scope, bound_scope=bound_scope
    )
    if not matching_network_ids:
        return literal(False)

    binding_rows = await _fetch_aca_binding_rows(
        driver, manifest, control_schema, matching_network_ids, plan_network_tier_table
    )
    if not binding_rows:
        return literal(False)
    return _unnested_plan_exists_clause(binding_rows, plan_table)
