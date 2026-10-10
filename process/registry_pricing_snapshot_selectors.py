# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native set selectors; later release readiness remains unassessed."""

from __future__ import annotations

import json

import asyncpg

from api.plan_release_readiness import is_release_binding_serving_scope_exact
from api.plan_release_serving import PlanReleaseSnapshotBinding
from api.ptg2_serving_utils import ein_plan_id_variants
from api.ptg2_snapshot import (
    _explicit_snapshot_plan_sql,
    _explicit_snapshot_source_sql,
    _explicit_snapshot_status_sql,
)
from api.ptg2_tables import PTG2_SCHEMA
from process.registry_pricing_snapshot_descriptors import _available_root_tables

_REQUIRED_SELECTOR_TABLES = (
    "ptg2_snapshot",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v4_snapshot_map_root",
)
_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)


def _selector_set_sql():
    query_parameters_by_name = {}
    status_sql, relation_sql = _explicit_snapshot_status_sql(
        requested_snapshot_id="selected",
        requested_source_key="source",
        requested_plan_id="plan",
        requested_plan_market_type="market",
        candidate_audit_access=None,
        query_params_by_name=query_parameters_by_name,
    )
    source_sql = _explicit_snapshot_source_sql("source", None, query_parameters_by_name)
    plan_sql = _explicit_snapshot_plan_sql("plan", "market", query_parameters_by_name)
    source_sql = source_sql.replace(":source_key", "request.source_key")
    plan_sql = plan_sql.replace("CAST(:plan_ids AS text[])", "request.plan_ids")
    plan_sql = plan_sql.replace(
        "AND snapshot_scope.plan_market_type = :plan_market_type",
        "AND (request.plan_market_type = '' OR snapshot_scope.plan_market_type = request.plan_market_type)",
    )
    return f"""WITH requested AS (
  SELECT input.request_no,snapshot_id,source_key,plan_market_type,
    ARRAY(SELECT variant.plan_id FROM unnest($4::bigint[],$5::text[])
      AS variant(request_no,plan_id) WHERE variant.request_no=input.request_no) AS plan_ids
  FROM unnest($1::text[],$2::text[],$3::text[]) WITH ORDINALITY
    AS input(snapshot_id,source_key,plan_market_type,request_no)
), selected AS MATERIALIZED (
  SELECT request.request_no,request.snapshot_id,EXISTS (
    SELECT 1 FROM {PTG2_SCHEMA}.ptg2_snapshot
    WHERE snapshot_id=request.snapshot_id AND {status_sql} AND {relation_sql}
      {source_sql} {plan_sql}
  ) AS matches FROM requested request
), bounds AS (
  SELECT coalesce(sum(octet_length(to_jsonb(selected)::text)+2),0)+2 AS byte_count FROM selected
)
SELECT current_setting('transaction_isolation') AS isolation,
  current_setting('transaction_read_only') AS read_only,byte_count<=$6::bigint AS bounded,
  CASE WHEN byte_count<=$6::bigint THEN (
    SELECT coalesce(jsonb_agg(to_jsonb(selected) ORDER BY request_no),'[]'::jsonb)::text FROM selected
  ) END AS rows_json FROM bounds"""


def _selector_key(binding):
    return (
        binding.snapshot_id,
        binding.source_key.strip().lower(),
        binding.plan_id.strip(),
        binding.plan_market_type.strip().lower(),
    )


def _selected_bindings(metadata_by_release_id):
    bindings = []
    for metadata in metadata_by_release_id.values():
        if metadata is None:
            continue
        for binding_fields in metadata["bindings"]:
            if binding_fields["role"] not in {"in_network", "allowed_amounts"}:
                raise ValueError("registry_pricing_selector_binding_invalid")
            if binding_fields["role"] != "in_network":
                continue
            try:
                binding = PlanReleaseSnapshotBinding(**binding_fields)
            except TypeError as error:
                raise ValueError("registry_pricing_selector_binding_invalid") from error
            if (
                any(
                    type(value) is not str or not value.strip()
                    for value in (binding.snapshot_id, binding.source_key, binding.plan_id)
                )
                or type(binding.plan_market_type) is not str
            ):
                raise ValueError("registry_pricing_selector_binding_invalid")
            bindings.append(binding)
    return bindings


def _selector_arguments(request_keys, max_report_bytes):
    if type(max_report_bytes) is not int or max_report_bytes < 1:
        raise ValueError("registry_pricing_selector_bound_invalid")
    variant_owners, plan_variants = [], []
    for request_no, request_key in enumerate(request_keys, 1):
        variants = ein_plan_id_variants(request_key[2])
        variant_owners.extend([request_no] * len(variants))
        plan_variants.extend(variants)
    arguments = (
        [key[0] for key in request_keys],
        [key[1] for key in request_keys],
        [key[3] for key in request_keys],
        variant_owners,
        plan_variants,
        max_report_bytes,
    )
    encoded = json.dumps(arguments[:-1], ensure_ascii=False, separators=(",", ":")).encode()
    return arguments if len(encoded) <= max_report_bytes else None


def _validated_selector_page(page, request_keys, max_report_bytes):
    if page["isolation"] not in {"repeatable read", "serializable"} or page["read_only"] != "on":
        raise ValueError("registry_pricing_selector_transaction_invalid")
    if page["bounded"] is not True:
        return None
    encoded = page["rows_json"]
    if type(encoded) is not str or len(encoded.encode()) > max_report_bytes:
        raise ValueError("registry_pricing_selector_rows_invalid")
    selected_rows = json.loads(encoded)
    if type(selected_rows) is not list:
        raise ValueError("registry_pricing_selector_rows_invalid")
    matches_by_key = {}
    for selected in selected_rows:
        request_no = selected.get("request_no") if type(selected) is dict else None
        if type(request_no) is not int or not 1 <= request_no <= len(request_keys):
            raise ValueError("registry_pricing_selector_identity_invalid")
        key = request_keys[request_no - 1]
        if key in matches_by_key or selected.get("snapshot_id") != key[0] or type(selected.get("matches")) is not bool:
            raise ValueError("registry_pricing_selector_identity_invalid")
        matches_by_key[key] = selected["matches"]
    if set(matches_by_key) != set(request_keys):
        raise ValueError("registry_pricing_selector_identity_invalid")
    return matches_by_key


async def _fetch_selector_matches(connection, request_keys, arguments):
    if await _available_root_tables(connection, _REQUIRED_SELECTOR_TABLES) != len(_REQUIRED_SELECTOR_TABLES):
        return None
    page = await connection.fetchrow(_selector_set_sql(), *arguments)
    return _validated_selector_page(page, request_keys, arguments[-1])


async def _optional_selector_matches(connection, request_keys, arguments):
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        matches_by_key = await _fetch_selector_matches(connection, request_keys, arguments)
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional selector lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(primary_error, _STORAGE_ERRORS):
            return None
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_selector_transaction_invalid")
    return matches_by_key


async def read_pricing_binding_selectors(
    connection, metadata_by_release_id, descriptor_by_snapshot, *, max_report_bytes
):
    """Require every in-network sibling's selectors and exact descriptor scope."""
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_selector_transaction_invalid")
    bindings = _selected_bindings(metadata_by_release_id)
    if not bindings:
        return {}
    request_keys = tuple(dict.fromkeys(_selector_key(binding) for binding in bindings))
    arguments = _selector_arguments(request_keys, max_report_bytes)
    matches_by_key = (
        await _optional_selector_matches(connection, request_keys, arguments) if arguments is not None else None
    )
    exact_by_snapshot = dict.fromkeys((binding.snapshot_id for binding in bindings), True)
    for binding in bindings:
        descriptor = descriptor_by_snapshot.get(binding.snapshot_id)
        is_exact = bool(
            matches_by_key is not None
            and matches_by_key[_selector_key(binding)]
            and descriptor is not None
            and descriptor.snapshot_id == binding.snapshot_id
            and descriptor.uses_shared_blocks
            and is_release_binding_serving_scope_exact(descriptor, binding)
        )
        exact_by_snapshot[binding.snapshot_id] &= is_exact
    return exact_by_snapshot
