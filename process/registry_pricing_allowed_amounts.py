# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native allowed-amount sibling coverage; full release readiness is unproven."""

from __future__ import annotations

import asyncpg

from api.plan_release_readiness import _ALLOWED_AMOUNT_BINDING_READINESS_SQL
from api.plan_release_serving import PlanReleaseSnapshotBinding
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.domain import PTG2_DOMAIN_ALLOWED_AMOUNT
from process.registry_pricing_snapshot_descriptors import _available_root_tables
from process.registry_pricing_snapshot_selectors import (
    _selector_arguments,
    _selector_key,
    _validated_selector_page,
)

_REQUIRED_ALLOWED_TABLES = (
    "ptg2_snapshot",
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)
_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)


def _allowed_amount_set_sql():
    expression = _ALLOWED_AMOUNT_BINDING_READINESS_SQL.strip().removeprefix("SELECT ")
    replacements_by_parameter = {
        "CAST(:plan_ids AS text[])": "request.plan_ids",
        ":snapshot_id": "request.snapshot_id",
        ":source_key": "request.source_key",
        ":market_type": "request.plan_market_type",
        ":allowed_contract": "$7::text",
        ":allowed_data_domain": "$8::text",
    }
    for parameter, column in replacements_by_parameter.items():
        expression = expression.replace(parameter, column)
    return f"""WITH requested AS (
  SELECT input.request_no,snapshot_id,source_key,plan_market_type,
    ARRAY(SELECT variant.plan_id FROM unnest($4::bigint[],$5::text[])
      AS variant(request_no,plan_id) WHERE variant.request_no=input.request_no) AS plan_ids
  FROM unnest($1::text[],$2::text[],$3::text[]) WITH ORDINALITY
    AS input(snapshot_id,source_key,plan_market_type,request_no)
), selected AS MATERIALIZED (
  SELECT request.request_no,request.snapshot_id,{expression} AS matches FROM requested request
), bounds AS (
  SELECT coalesce(sum(octet_length(to_jsonb(selected)::text)+2),0)+2 AS byte_count FROM selected
)
SELECT current_setting('transaction_isolation') AS isolation,
  current_setting('transaction_read_only') AS read_only,byte_count<=$6::bigint AS bounded,
  CASE WHEN byte_count<=$6::bigint THEN (
    SELECT coalesce(jsonb_agg(to_jsonb(selected) ORDER BY request_no),'[]'::jsonb)::text FROM selected
  ) END AS rows_json FROM bounds"""


def _allowed_bindings(metadata_by_release_id):
    bindings = []
    for metadata in metadata_by_release_id.values():
        if metadata is None:
            continue
        for binding_fields in metadata["bindings"]:
            if binding_fields["role"] not in {"in_network", "allowed_amounts"}:
                raise ValueError("registry_pricing_allowed_binding_invalid")
            if binding_fields["role"] != "allowed_amounts":
                continue
            try:
                binding = PlanReleaseSnapshotBinding(**binding_fields)
            except TypeError as error:
                raise ValueError("registry_pricing_allowed_binding_invalid") from error
            if (
                any(
                    type(value) is not str or not value.strip()
                    for value in (binding.snapshot_id, binding.source_key, binding.plan_id)
                )
                or type(binding.plan_market_type) is not str
            ):
                raise ValueError("registry_pricing_allowed_binding_invalid")
            bindings.append(binding)
    return bindings


async def _fetch_allowed_matches(connection, request_keys, arguments):
    if await _available_root_tables(connection, _REQUIRED_ALLOWED_TABLES) != len(_REQUIRED_ALLOWED_TABLES):
        return None
    page = await connection.fetchrow(
        _allowed_amount_set_sql(), *arguments, PTG2_ALLOWED_AMOUNT_CONTRACT, PTG2_DOMAIN_ALLOWED_AMOUNT
    )
    return _validated_selector_page(page, request_keys, arguments[-1])


async def _optional_allowed_matches(connection, request_keys, arguments):
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        matches_by_key = await _fetch_allowed_matches(connection, request_keys, arguments)
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional allowed-amount lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(primary_error, _STORAGE_ERRORS):
            return None
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_allowed_transaction_invalid")
    return matches_by_key


async def read_pricing_allowed_amount_checks(connection, metadata_by_release_id, *, max_report_bytes):
    """Check all original role siblings with one exact bound request relation."""
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_allowed_transaction_invalid")
    bindings = _allowed_bindings(metadata_by_release_id)
    if not bindings:
        return {}
    request_keys = tuple(dict.fromkeys(_selector_key(binding) for binding in bindings))
    arguments = _selector_arguments(request_keys, max_report_bytes)
    matches_by_key = (
        await _optional_allowed_matches(connection, request_keys, arguments) if arguments is not None else None
    )
    coverage_by_snapshot = dict.fromkeys((binding.snapshot_id for binding in bindings), True)
    for binding in bindings:
        coverage_by_snapshot[binding.snapshot_id] &= bool(
            matches_by_key is not None and matches_by_key[_selector_key(binding)]
        )
    return coverage_by_snapshot
