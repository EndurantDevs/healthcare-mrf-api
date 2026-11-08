# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate a published PTG result without claiming frozen input-file provenance."""

from __future__ import annotations

import datetime
import hashlib
import json
from typing import Any, Mapping

from sqlalchemy import text

from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from process.ptg_parts.ptg2_candidate_attestation import _candidate_evidence_identity
from process.ptg_parts.ptg2_candidate_layout_identity import validate_candidate_layout_identity
from process.ptg_parts.result_archive_closure import ResultArchiveClosureError, _validate_layout
from process.ptg_parts.result_archive_source_authority import result_archive_manifest_sha256


def _validate_published_ownership(snapshot_by_field: Mapping[str, Any]) -> tuple:
    """Validate publication ownership and reject protected or payload-bearing inputs."""
    snapshot_id = str(snapshot_by_field.get("snapshot_id") or "").strip()
    import_run_id = str(snapshot_by_field.get("import_run_id") or "").strip()
    import_month = str(snapshot_by_field.get("import_month") or "").strip()
    run_import_month = str(snapshot_by_field.get("run_import_month") or "").strip()
    run_source_key = str(snapshot_by_field.get("run_source_key") or "").strip().lower()
    manifest = snapshot_by_field.get("manifest")
    layout_manifest = snapshot_by_field.get("layout_manifest")
    try:
        month_date = datetime.date.fromisoformat(import_month)
    except ValueError as exc:
        raise ValueError("published result import month is invalid") from exc
    if month_date.day != 1 or import_month != run_import_month or import_month != month_date.isoformat():
        raise ValueError("published result import month disagrees with its run")
    if not snapshot_id or not import_run_id or not run_source_key:
        raise ValueError("published result is missing its source ownership")
    if not isinstance(manifest, Mapping) or not isinstance(layout_manifest, Mapping):
        raise ValueError("published result is missing its sealed manifest")
    activation = manifest.get("activation")
    serving = manifest.get("serving_index")
    layout_serving = layout_manifest.get("serving_index")
    if (
        snapshot_by_field.get("status") != "published"
        or not isinstance(activation, Mapping)
        or activation.get("contract") != "ptg2_candidate_activation_v1"
        or activation.get("state") != "activated"
        or str(activation.get("source_key") or "").strip().lower() != run_source_key
        or not isinstance(serving, Mapping)
        or not isinstance(layout_serving, Mapping)
    ):
        raise ValueError("published result is not an activated source-owned snapshot")
    if snapshot_by_field.get("frozen_binding_payload") is not None:
        raise ValueError("published result has a protected frozen-file binding")
    if type(snapshot_by_field.get("artifact_count")) is not int or snapshot_by_field["artifact_count"] != 0:
        raise ValueError("published result contains unreviewed artifact payloads")
    return snapshot_id, import_run_id, import_month, run_source_key, manifest, activation, serving, layout_serving


def _validate_published_source_scope(snapshot_by_field: Mapping[str, Any], identity: Mapping[str, Any]) -> tuple:
    """Require complete primary-plan and ordered source assignment evidence."""
    plan_scopes = snapshot_by_field.get("plan_scopes")
    source_assignments = snapshot_by_field.get("source_assignments")
    if (
        not isinstance(plan_scopes, list)
        or not plan_scopes
        or any(not isinstance(scope, list) or len(scope) != 2 or not all(scope) for scope in plan_scopes)
        or len({tuple(scope) for scope in plan_scopes}) != len(plan_scopes)
        or [identity["plan_id"], identity["plan_market_type"]] not in plan_scopes
        or not isinstance(source_assignments, list)
        or len(source_assignments) != len(snapshot_by_field["raw_container_sha256_values"])
        or any(not isinstance(source_assignment_by_field, Mapping) for source_assignment_by_field in source_assignments)
    ):
        raise ValueError("published result has an incomplete plan or source scope")
    source_hashes = [
        str(source_assignment_by_field.get("raw_container_sha256") or "").lower()
        for source_assignment_by_field in source_assignments
    ]
    if source_hashes != [
        raw_digest.hex() if isinstance(raw_digest, (bytes, bytearray, memoryview)) else str(raw_digest).lower()
        for raw_digest in snapshot_by_field["raw_container_sha256_values"]
    ]:
        raise ValueError("published result source assignments changed")
    return plan_scopes, source_assignments


def validate_published_result_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the exact published result using the existing public row interface."""
    return _validated_published_result_identity(row)


def _validated_published_result_identity(snapshot_by_field: Mapping[str, Any]) -> dict[str, Any]:
    """Bind one legacy published result to its sealed V4 physical evidence.

    This is not export authorization: the caller must hold the real retention
    pin and validate the full archive closure in its repeatable-read clone.
    """

    snapshot_id, import_run_id, import_month, run_source_key, manifest, activation, serving, layout_serving = (
        _validate_published_ownership(snapshot_by_field)
    )
    generation = validate_candidate_layout_identity(snapshot_by_field, serving, layout_serving)
    if generation != "shared_blocks_v4":
        raise ValueError("published result archive requires a sealed V4 layout")
    try:
        snapshot_key, layout_digest, map_digest, finalizer_digest = _validate_layout(
            {
                **snapshot_by_field,
                "state": snapshot_by_field.get("layout_state"),
                "generation": generation,
                "mapping_digest": snapshot_by_field.get("layout_mapping_digest"),
            }
        )
        identity = _candidate_evidence_identity(
            snapshot_by_field,
            activation_by_field=activation,
            serving_index_by_field=serving,
            layout_serving_index_by_field=layout_serving,
            storage_generation=generation,
        )
    except (ResultArchiveClosureError, FrozenRateFileMismatchError) as exc:
        raise ValueError("published result evidence is inconsistent") from exc
    if not snapshot_by_field.get("raw_container_sha256_values") or identity["source_key"] != run_source_key:
        raise ValueError("published result has no source-set ownership")
    plan_scopes, source_assignments = _validate_published_source_scope(snapshot_by_field, identity)
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "import_month": import_month,
        "source_key": run_source_key,
        "snapshot_manifest_sha256": result_archive_manifest_sha256(manifest),
        "snapshot_key": snapshot_key,
        "plan_id": identity["plan_id"],
        "plan_market_type": identity["plan_market_type"],
        "coverage_scope_id": identity["coverage_scope_id"],
        "source_set_digest": identity["source_set_digest"],
        "source_count": len(snapshot_by_field["raw_container_sha256_values"]),
        "plan_scopes_sha256": hashlib.sha256(canonical_json_dumps(plan_scopes).encode()).hexdigest(),
        "source_assignments_sha256": hashlib.sha256(canonical_json_dumps(source_assignments).encode()).hexdigest(),
        "layout_mapping_digest": layout_digest,
        "map_digest": map_digest,
        "finalizer_map_digest": finalizer_digest,
    }


_PUBLISHED_RESULT_IDENTITY_SQL = """
            SELECT snapshot.snapshot_id, snapshot.status, snapshot.import_run_id,
                   snapshot.import_month,
                   snapshot.manifest,
                   internal_run.import_month AS run_import_month,
                   internal_run.options ->> 'source_key' AS run_source_key,
                   frozen.binding_payload AS frozen_binding_payload,
                   binding.snapshot_key,
                   scope.plan_id, scope.plan_market_type, scope.coverage_scope_id,
                   layout.state AS layout_state,
                   layout.generation AS layout_generation,
                   layout.mapping_digest AS layout_mapping_digest,
                   layout.layout_manifest,
                   map_root.state AS v4_root_state,
                   map_root.map_digest AS v4_root_map_digest,
                   map_root.state AS map_root_state,
                   map_root.map_format,
                   map_root.map_digest,
                   finalizer.state AS finalizer_root_state,
                   finalizer.contract AS finalizer_contract,
                   finalizer.map_format AS finalizer_map_format,
                   finalizer.map_digest AS finalizer_map_digest,
                   internal_run.options -> 'invalid_price_exclusion_policy'
                       AS invalid_price_exclusion_policy,
                   ARRAY(
                       SELECT source.raw_container_sha256
                         FROM {schema}.ptg2_v3_snapshot_source AS source
                        WHERE source.snapshot_id = snapshot.snapshot_id
                        ORDER BY source.source_key
                   ) AS raw_container_sha256_values
                   ,(
                       SELECT jsonb_agg(jsonb_build_array(plan_id, lower(plan_market_type))
                                        ORDER BY plan_id, lower(plan_market_type))
                         FROM {schema}.ptg2_v3_snapshot_plan_scope
                        WHERE snapshot_id = snapshot.snapshot_id
                   ) AS plan_scopes
                   ,(
                       SELECT jsonb_agg(
                                  jsonb_build_object(
                                      'source_key', source_key,
                                      'source_type', source_type,
                                      'identity_kind', identity_kind,
                                      'identity_sha256', identity_sha256,
                                      'raw_container_sha256', raw_container_sha256,
                                      'logical_json_sha256', logical_json_sha256,
                                      'logical_hash_deferred', logical_hash_deferred,
                                      'source_trace_set_hash', source_trace_set_hash
                                  ) ORDER BY source_key)
                         FROM {schema}.ptg2_v3_snapshot_source
                        WHERE snapshot_id = snapshot.snapshot_id
                   ) AS source_assignments
                   ,(
                       SELECT count(*)
                         FROM {schema}.ptg2_artifact_manifest
                        WHERE snapshot_id = snapshot.snapshot_id
                   ) AS artifact_count
              FROM {schema}.ptg2_snapshot AS snapshot
              JOIN {schema}.ptg2_import_run AS internal_run
                ON internal_run.import_run_id = snapshot.import_run_id
              JOIN {schema}.ptg2_v3_snapshot_binding AS binding
                ON binding.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_scope AS scope
                ON scope.snapshot_id = snapshot.snapshot_id
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = binding.snapshot_key
              LEFT JOIN {schema}.ptg2_v4_snapshot_map_root AS map_root
                ON map_root.snapshot_key = layout.snapshot_key
              LEFT JOIN {schema}.ptg2_v4_finalizer_map_root AS finalizer
                ON finalizer.snapshot_key = layout.snapshot_key
              LEFT JOIN {schema}.ptg2_frozen_source_file_binding AS frozen
                ON frozen.internal_run_id = snapshot.import_run_id
             WHERE snapshot.snapshot_id = :snapshot_id
            {locking_clause}
"""


def _published_result_identity_sql(schema_name: str, *, lock: bool, snapshot_parameter: str) -> str:
    return _PUBLISHED_RESULT_IDENTITY_SQL.format(
        schema=_quote_ident(schema_name),
        locking_clause="FOR KEY SHARE OF snapshot, internal_run, binding, scope, layout" if lock else "",
    ).replace(":snapshot_id", snapshot_parameter)


async def load_published_result_identity(
    session: Any, *, schema_name: str, snapshot_id: str, lock: bool = False
) -> dict[str, Any]:
    """Read one exact published result; the caller owns transaction and pinning."""

    snapshot_id = str(snapshot_id or "").strip()
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    identity_query_result = await session.execute(
        text(_published_result_identity_sql(schema_name, lock=lock, snapshot_parameter=":snapshot_id")),
        {"snapshot_id": snapshot_id},
    )
    identity_rows = identity_query_result.all()
    if len(identity_rows) != 1:
        raise ValueError("published result is missing or ambiguous")
    return validate_published_result_identity(dict(identity_rows[0]._mapping))


async def load_published_result_identity_asyncpg(
    connection: Any, *, schema_name: str, snapshot_id: str, lock: bool = False
) -> dict[str, Any]:
    """Use the same evidence query inside a caller-owned asyncpg transaction."""

    snapshot_id = str(snapshot_id or "").strip()
    if not snapshot_id:
        raise ValueError("snapshot_id is required")
    rows = await connection.fetch(
        _published_result_identity_sql(schema_name, lock=lock, snapshot_parameter="$1"), snapshot_id
    )
    if len(rows) != 1:
        raise ValueError("published result is missing or ambiguous")
    identity_map = dict(rows[0])
    for field_name in (
        "manifest",
        "layout_manifest",
        "plan_scopes",
        "source_assignments",
        "invalid_price_exclusion_policy",
    ):
        if isinstance(identity_map.get(field_name), str):
            identity_map[field_name] = json.loads(identity_map[field_name])
    return validate_published_result_identity(identity_map)


__all__ = [
    "load_published_result_identity",
    "load_published_result_identity_asyncpg",
    "validate_published_result_identity",
]
