# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed cold published-row checks; full snapshot readiness is unproven."""

from __future__ import annotations

import json

import asyncpg

from api.ptg2_tables import (
    _PUBLISHED_SNAPSHOT_SQL,
    PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS,
    PTG2_SCHEMA,
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
    _database_execution_evidence,
    _optional_integer,
    _strict_coverage_scope_id,
    _strict_v3_manifest_fields,
    _validated_published_snapshot_fields,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

_REQUIRED_TABLES = (
    "ptg2_snapshot",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_snapshot_source",
)
_AVAILABILITY_SQL = """SELECT relation_name,to_regclass(relation_name) IS NOT NULL AS available
FROM unnest($1::text[]) AS names(relation_name)"""
_SET_SQL = (
    _PUBLISHED_SNAPSHOT_SQL.replace("__PTG2_SCHEMA__", PTG2_SCHEMA)
    .replace(
        "SELECT layout.layout_manifest",
        "SELECT snapshot.snapshot_id,layout.layout_manifest",
        1,
    )
    .replace(
        "snapshot.snapshot_id = :snapshot_id",
        "snapshot.snapshot_id = ANY($1::text[])",
    )
    .replace(
        "CAST(:storage_generations AS text[])",
        "$2::text[]",
    )
    .replace(
        "CAST(:attestation_contracts AS text[])",
        "$3::text[]",
    )
    .replace("LIMIT 1", "")
)
_BOUNDED_SQL = """WITH snapshots AS MATERIALIZED ({set_sql}), bounds AS (
  SELECT coalesce(sum(octet_length(to_jsonb(snapshot)::text)+2),0)+2 AS byte_count FROM snapshots snapshot
)
SELECT current_setting('transaction_isolation') AS isolation,
  current_setting('transaction_read_only') AS read_only,byte_count<=$4::bigint AS bounded,CASE WHEN byte_count<=$4::bigint THEN (
  SELECT coalesce(jsonb_agg(to_jsonb(snapshot) ORDER BY snapshot.snapshot_id),'[]'::jsonb)::text FROM snapshots snapshot
) END AS rows_json FROM bounds"""
_OPTIONAL_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)


def _published_row_proof(row_fields, *, physical_binding=None):
    if row_fields.get("has_local_physical_binding") and physical_binding is None:
        return {"status": "local_custody_not_assessed", "full_readiness": "not_assessed"}
    serving_index = row_fields.get("layout_serving_index")
    if isinstance(serving_index, str):
        try:
            serving_index = json.loads(serving_index)
        except json.JSONDecodeError as exc:
            raise PTG2ManifestArtifactError("PTG2 serving_index manifest is malformed") from exc
    if not isinstance(serving_index, dict):
        raise PTG2ManifestArtifactError("PTG2 snapshot is missing a strict serving_index; reimport the snapshot")
    shared_key, storage_generation, _, audit_sample = _strict_v3_manifest_fields(serving_index)
    source_count = _optional_integer(serving_index.get("source_count"))
    code_count = _optional_integer(serving_index.get("code_count"))
    source_set_by_field, source_key = _validated_published_snapshot_fields(
        row_fields,
        shared_snapshot_key=shared_key,
        physical_binding=physical_binding,
        coverage_scope_id=_strict_coverage_scope_id(serving_index),
        audit_sample=audit_sample,
        source_count=source_count,
        code_count=code_count,
    )
    serving_rate_count = _optional_integer(serving_index.get("serving_rates"))
    if code_count == 0 and serving_rate_count is not None and serving_rate_count > 0:
        raise PTG2ManifestArtifactError("PTG2 shared layout is missing code metadata for a non-empty snapshot")
    _database_execution_evidence(row_fields)
    return {
        "status": "published_row_validated",
        "full_readiness": "not_assessed",
        "shared_snapshot_key": shared_key,
        "storage_generation": storage_generation,
        "source_key": source_key,
        "coverage_scope_id": _strict_coverage_scope_id(serving_index),
        "source_count": source_count,
        "source_set_sha256": source_set_by_field["raw_container_sha256_digest"],
        "audit_sample_sha256": audit_sample["sample_digest"],
        "unresolved_reason": "full_readiness_not_proven",
        "plan_id": str(row_fields.get("snapshot_plan_id") or "").strip() or None,
        "plan_market_type": str(row_fields.get("snapshot_plan_market_type") or "").strip() or None,
    }


async def _fetch_snapshot_rows(connection, snapshot_ids, max_report_bytes):
    names = [f"{PTG2_SCHEMA}.{table}" for table in _REQUIRED_TABLES]
    available = await connection.fetch(_AVAILABILITY_SQL, names)
    if {row["relation_name"] for row in available if row["available"] is True} != set(names):
        return None
    page = await connection.fetchrow(
        _BOUNDED_SQL.format(set_sql=_SET_SQL),
        list(snapshot_ids),
        [PTG2_V3_SHARED_GENERATION, PTG2_V4_SHARED_GENERATION],
        list(PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS),
        max_report_bytes,
    )
    if page["isolation"] not in {"repeatable read", "serializable"} or page["read_only"] != "on":
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    if page["bounded"] is not True:
        return None
    encoded = page["rows_json"]
    if type(encoded) is not str or len(encoded.encode()) > max_report_bytes:
        raise ValueError("registry_pricing_snapshot_rows_invalid")
    snapshot_rows = json.loads(encoded)
    if type(snapshot_rows) is not list:
        raise ValueError("registry_pricing_snapshot_rows_invalid")
    return snapshot_rows


async def _optional_snapshot_rows(connection, snapshot_ids, max_report_bytes):
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        snapshot_rows = await _fetch_snapshot_rows(connection, snapshot_ids, max_report_bytes)
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional snapshot lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(primary_error, _OPTIONAL_STORAGE_ERRORS):
            return None
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    return snapshot_rows


async def _append_allowed_amount_checks(connection, metadata_by_release_id, checks_by_snapshot_id, max_report_bytes):
    from process.registry_pricing_allowed_amounts import read_pricing_allowed_amount_checks

    coverage_by_snapshot = await read_pricing_allowed_amount_checks(
        connection, metadata_by_release_id, max_report_bytes=max_report_bytes
    )
    for identity, has_coverage in coverage_by_snapshot.items():
        snapshot_check_by_field = checks_by_snapshot_id.setdefault(
            identity,
            {
                "status": "allowed_amounts_coverage_validated" if has_coverage else "unavailable",
                "full_readiness": "not_assessed",
            },
        )
        snapshot_check_by_field["allowed_amounts_status"] = "validated" if has_coverage else "unavailable"
    return checks_by_snapshot_id


def _resolved_snapshot_row_checks(rows_by_snapshot_id, local_by_snapshot):
    checks_by_snapshot_id = {}
    validated_rows = []
    for identity, selected_rows in rows_by_snapshot_id.items():
        snapshot_check_by_field = {"status": "unavailable", "full_readiness": "not_assessed"}
        if len(selected_rows) != 1:
            checks_by_snapshot_id[identity] = snapshot_check_by_field
            continue
        try:
            local = local_by_snapshot.get(identity)
            if local is not None:
                snapshot_check_by_field = _published_row_proof(local[0], physical_binding=local[1])
                snapshot_check_by_field.update(
                    status="local_published_row_validated",
                    local_custody_status="validated",
                    local_descriptor_status="not_assessed",
                    binding_selector_status="not_assessed",
                )
            else:
                snapshot_check_by_field = _published_row_proof(selected_rows[0])
                if snapshot_check_by_field["status"] == "published_row_validated":
                    validated_rows.append(selected_rows[0])
        except PTG2ManifestArtifactError:
            snapshot_check_by_field = {"status": "unavailable", "full_readiness": "not_assessed"}
        checks_by_snapshot_id[identity] = snapshot_check_by_field
    return checks_by_snapshot_id, validated_rows


async def read_pricing_snapshot_row_checks(connection, metadata_by_release_id, *, max_report_bytes):
    """Check all pricing-role siblings on the existing native transaction."""
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    snapshot_ids = tuple(
        sorted(
            {
                binding["snapshot_id"]
                for metadata in metadata_by_release_id.values()
                if metadata is not None
                for binding in metadata["bindings"]
                if binding["role"] == "in_network"
            }
        )
    )
    if not snapshot_ids:
        return await _append_allowed_amount_checks(connection, metadata_by_release_id, {}, max_report_bytes)
    snapshot_rows = await _optional_snapshot_rows(connection, snapshot_ids, max_report_bytes)
    rows_by_snapshot_id = {identity: [] for identity in snapshot_ids}
    for row_fields in snapshot_rows or ():
        identity = row_fields["snapshot_id"]
        if identity not in rows_by_snapshot_id:
            raise ValueError("registry_pricing_snapshot_identity_invalid")
        rows_by_snapshot_id[identity].append(row_fields)
    from process.registry_pricing_local_rows import read_pricing_local_rows

    local_rows = [
        selected[0]
        for selected in rows_by_snapshot_id.values()
        if len(selected) == 1 and selected[0].get("has_local_physical_binding")
    ]
    local_by_snapshot = await read_pricing_local_rows(connection, local_rows, max_report_bytes=max_report_bytes)
    checks_by_snapshot_id, validated_rows = _resolved_snapshot_row_checks(rows_by_snapshot_id, local_by_snapshot)
    from process.registry_pricing_snapshot_descriptors import read_pricing_shared_descriptors

    descriptor_by_snapshot = await read_pricing_shared_descriptors(
        connection, validated_rows, max_report_bytes=max_report_bytes
    )
    for identity, descriptor in descriptor_by_snapshot.items():
        checks_by_snapshot_id[identity]["shared_descriptor_status"] = (
            "validated" if descriptor is not None else "unavailable"
        )
    from process.registry_pricing_snapshot_selectors import read_pricing_binding_selectors

    exact_by_snapshot = await read_pricing_binding_selectors(
        connection, metadata_by_release_id, descriptor_by_snapshot, max_report_bytes=max_report_bytes
    )
    for identity, is_exact in exact_by_snapshot.items():
        if checks_by_snapshot_id[identity]["status"] == "published_row_validated":
            checks_by_snapshot_id[identity]["binding_selector_status"] = "validated" if is_exact else "unavailable"
    return await _append_allowed_amount_checks(
        connection, metadata_by_release_id, checks_by_snapshot_id, max_report_bytes
    )
