# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed bulk binding metadata; it grants no pricing or serving readiness."""

from __future__ import annotations

import json
from dataclasses import asdict

import asyncpg

from api.plan_release_pricing_projection import plan_release_binding_set_sql, pricing_projection_relation
from api.plan_release_serving import (
    PLAN_RELEASE_PIN_OWNER_TYPE,
    PTG2_SCHEMA,
    _selection_from_rows,
    normalize_plan_release_id,
)
from process.network_approved_catalog_evidence import (
    MAX_NETWORKS,
    ApprovedNetworkCatalogEvidenceResourceLimit,
    read_approved_network_catalog_evidence,
)
from process.registry_pricing_snapshot_rows import read_pricing_snapshot_row_checks

_REQUIRED_RELATIONS = (
    "plan_release_serving_revision",
    "plan_release_snapshot_binding",
    "ptg2_snapshot",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_snapshot_pin",
)
_OPTIONAL_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)
_AVAILABILITY_SQL = """SELECT relation_name,to_regclass(relation_name) IS NOT NULL AS available
FROM unnest($1::text[]) AS names(relation_name)"""

_BOUNDED_METADATA_SQL = """WITH bindings AS MATERIALIZED ({binding_sql}), bounds AS (
  SELECT coalesce(sum(octet_length(to_jsonb(binding)::text)+2),0)+2 AS byte_count FROM bindings binding
)
SELECT byte_count <= $3::bigint AS bounded,
  CASE WHEN byte_count <= $3::bigint THEN (
    SELECT coalesce(jsonb_agg(to_jsonb(binding) ORDER BY binding.plan_release_id,
      binding.role,binding.binding_ordinal),'[]'::jsonb)::text FROM bindings binding
  ) END AS rows_json
FROM bounds"""


def _resolved_network_ids(report):
    selected_ids = []
    for target_by_field in report["targets"]:
        if target_by_field["mapping_status"] == "resolved":
            network_id = target_by_field["network_id"]
            if type(network_id) is not int or not 1 <= network_id <= 2147483647:
                raise ValueError("registry_pricing_binding_identity_invalid")
            selected_ids.append(network_id)
    return tuple(sorted(set(selected_ids)))


async def _fetch_release_binding_rows(connection, release_ids, max_report_bytes):
    required_names = {f"{PTG2_SCHEMA}.{name}" for name in _REQUIRED_RELATIONS}
    projection_name = pricing_projection_relation(PTG2_SCHEMA)
    availability_rows = await connection.fetch(_AVAILABILITY_SQL, sorted(required_names | {projection_name}))
    available_names = {row["relation_name"] for row in availability_rows if row["available"] is True}
    if not required_names <= available_names:
        return None
    binding_sql = plan_release_binding_set_sql(
        PTG2_SCHEMA, include_pricing_projection=projection_name in available_names
    )
    page = await connection.fetchrow(
        _BOUNDED_METADATA_SQL.format(binding_sql=binding_sql),
        list(release_ids),
        PLAN_RELEASE_PIN_OWNER_TYPE,
        max_report_bytes,
    )
    if page["bounded"] is not True:
        return None
    encoded_rows = page["rows_json"]
    if type(encoded_rows) is not str or len(encoded_rows.encode()) > max_report_bytes:
        raise ValueError("registry_pricing_binding_metadata_invalid")
    binding_rows = json.loads(encoded_rows)
    if type(binding_rows) is not list:
        raise ValueError("registry_pricing_binding_metadata_invalid")
    return binding_rows


async def _optional_release_binding_rows(connection, release_ids, max_report_bytes):
    """Recover only physical-query errors; caller and savepoint errors remain primary."""
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        binding_rows = await _fetch_release_binding_rows(connection, release_ids, max_report_bytes)
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional binding lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(primary_error, _OPTIONAL_STORAGE_ERRORS):
            return None
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_binding_transaction_invalid")
    return binding_rows


def _metadata_by_release_id(release_ids, binding_rows):
    rows_by_release_id = {identity: [] for identity in release_ids}
    for row in binding_rows or ():
        identity = row["plan_release_id"]
        if identity not in rows_by_release_id:
            raise ValueError("registry_pricing_binding_identity_invalid")
        rows_by_release_id[identity].append(row)
    selections_by_release_id = {
        identity: _selection_from_rows(identity, release_rows) for identity, release_rows in rows_by_release_id.items()
    }
    return {
        identity: None
        if selection is None
        else {
            **selection.response_metadata(),
            "bindings": [asdict(binding) for binding in selection.bindings],
        }
        for identity, selection in selections_by_release_id.items()
    }


def _reference_metadata(reference_by_field, metadata_by_release_id):
    metadata = metadata_by_release_id.get(reference_by_field["plan_release_id"])
    has_exact_tuple = metadata is not None and (
        metadata["healthporta_plan_id"] == reference_by_field["healthporta_plan_id"]
        and metadata["serving_revision_id"] == reference_by_field["serving_revision_id"]
        and any(
            (binding["role"], binding["binding_ordinal"], binding["snapshot_id"])
            == (
                reference_by_field["role"],
                reference_by_field["ordinal"],
                reference_by_field["snapshot_id"],
            )
            for binding in metadata["bindings"]
        )
    )
    return {
        "reference": reference_by_field,
        "resolved": False,
        "binding_metadata_match": has_exact_tuple,
        "unresolved_reason": "full_readiness_not_proven"
        if has_exact_tuple
        else "binding_metadata_unavailable_or_moved",
    }


def _batch_provenance(batch, network_requests, metadata_by_release_id):
    records_by_network_id = {record.network_id: record for record in batch.records}
    return {
        "status": "binding_metadata_only",
        "full_readiness": "not_assessed",
        "approved_revision": batch.approved_revision,
        "approved_map_sha256": batch.approved_map_sha256,
        "request_sha256": batch.request_sha256,
        "release_metadata": metadata_by_release_id,
        "networks": [
            {
                **asdict(records_by_network_id[request["network_id"]]),
                "pricing_refs": None
                if request["pricing_refs"] is None
                else [_reference_metadata(reference, metadata_by_release_id) for reference in request["pricing_refs"]],
            }
            for request in network_requests
        ],
    }


async def _pricing_binding_metadata(connection, report, approved, control_schema, max_report_bytes):
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_binding_transaction_invalid")
    network_ids = _resolved_network_ids(report)
    if not network_ids or len(network_ids) > MAX_NETWORKS:
        return {"status": "not_assessed" if not network_ids else "unavailable", "full_readiness": "not_assessed"}
    try:
        batch = await read_approved_network_catalog_evidence(
            connection,
            approved,
            network_ids=network_ids,
            control_schema=control_schema,
        )
    except ApprovedNetworkCatalogEvidenceResourceLimit:
        return {"status": "unavailable", "full_readiness": "not_assessed"}
    network_requests = json.loads(batch.request_bytes)["networks"]
    release_ids = tuple(
        sorted(
            {
                reference["plan_release_id"]
                for request in network_requests
                for reference in request["pricing_refs"] or ()
            }
        )
    )
    if any(normalize_plan_release_id(identity) != identity for identity in release_ids):
        raise ValueError("registry_pricing_binding_identity_invalid")
    binding_rows = (
        await _optional_release_binding_rows(connection, release_ids, max_report_bytes) if release_ids else []
    )
    metadata_by_release_id = _metadata_by_release_id(release_ids, binding_rows)
    provenance = _batch_provenance(batch, network_requests, metadata_by_release_id)
    provenance["snapshot_row_checks"] = await read_pricing_snapshot_row_checks(
        connection,
        metadata_by_release_id,
        max_report_bytes=max_report_bytes,
    )
    return provenance


async def append_pricing_binding_metadata(connection, report, approved, *, control_schema, max_report_bytes):
    """Retain optional metadata without changing directory results or public NULLs."""
    provenance = await _pricing_binding_metadata(connection, report, approved, control_schema, max_report_bytes)
    report_with_metadata_by_field = {
        **report,
        "provenance": {**report["provenance"], "pricing_binding_metadata": provenance},
    }
    encoded = json.dumps(
        report_with_metadata_by_field, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()
    return report_with_metadata_by_field if len(encoded) <= max_report_bytes else report
