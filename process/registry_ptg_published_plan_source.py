# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual published plan membership over the complete immutable snapshot source set."""

import hashlib

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.result_archive_source_authority import (
    prepare_ptg_result_archive_source_authority,
)
from process.registry_ptg_cohort_authority import _SOURCE_FIELDS
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeError,
    _digest,
    _engine_identity,
    _required_text,
    _sha256,
)
from process.registry_ptg_published_plan_contract import (
    AUTHORITY_CONTRACT,
    SELECTION_MODE,
)

_MAX_ROWS = 128


async def _published_plan_rows(session, schema, snapshot_id, identity):
    plan_rows = (
        (
            await session.execute(
                text(f"""
        SELECT plan_id,lower(plan_market_type) AS plan_market_type
          FROM {schema}.ptg2_v3_snapshot_plan_scope WHERE snapshot_id=:snapshot_id
         ORDER BY plan_id,lower(plan_market_type) LIMIT 129
    """),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    )
    plans = [[row["plan_id"], row["plan_market_type"]] for row in plan_rows]
    if not 1 <= len(plans) <= _MAX_ROWS or len({tuple(plan) for plan in plans}) != len(plans):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    if hashlib.sha256(canonical_json_dumps(plans).encode()).hexdigest() != identity["plan_scopes_sha256"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return [{"plan_id": plan[0], "plan_market_type": plan[1]} for plan in plans]


async def _require_complete_traces(session, schema, snapshot_id):
    """Refuse missing raw trace references before resolving any file identities."""
    has_missing_trace = (
        await session.execute(
            text(f"""
        SELECT EXISTS (
          SELECT FROM {schema}.ptg2_v3_snapshot_source source
          LEFT JOIN {schema}.ptg2_source_trace_set trace_set
            ON trace_set.source_trace_set_hash=source.source_trace_set_hash
          WHERE source.snapshot_id=:snapshot_id AND (
            trace_set.source_trace_set_hash IS NULL OR cardinality(trace_set.source_trace_hashes)=0 OR EXISTS (
              SELECT FROM unnest(trace_set.source_trace_hashes) trace_hash(value)
              LEFT JOIN {schema}.ptg2_source_trace trace ON trace.source_trace_hash=trace_hash.value
              WHERE trace.source_trace_hash IS NULL)))
    """),
            {"snapshot_id": snapshot_id},
        )
    ).scalar_one()
    if has_missing_trace is not False:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")


async def _published_file_rows(session, schema, snapshot_id, identity):
    """Resolve every assignment to one exact complete file identity or refuse."""
    assignments = [
        dict(assignment_row)
        for assignment_row in (
            await session.execute(
                text(
                    f"SELECT {','.join(_SOURCE_FIELDS)} FROM {schema}.ptg2_v3_snapshot_source "
                    "WHERE snapshot_id=:snapshot_id ORDER BY source_key LIMIT 129"
                ),
                {"snapshot_id": snapshot_id},
            )
        )
        .mappings()
        .all()
    ]
    if not 1 <= len(assignments) <= _MAX_ROWS or len(assignments) != identity["source_count"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    if _digest(assignments) != identity["source_assignments_sha256"]:
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    await _require_complete_traces(session, schema, snapshot_id)
    file_rows = (
        (
            await session.execute(
                text(CANDIDATE_SOURCE_RECORDS_SQL.format(schema=schema) + " LIMIT 129"), {"snapshot_id": snapshot_id}
            )
        )
        .mappings()
        .all()
    )
    if len(file_rows) != len(assignments):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    file_versions = []
    source_keys = []
    for assignment_by_field, file_row in zip(assignments, file_rows):
        if (
            type(assignment_by_field["source_key"]) is not int
            or assignment_by_field["source_key"] < 0
            or file_row["source_key"] != assignment_by_field["source_key"]
            or file_row["source_file_version_count"] != 1
            or file_row["version_raw_sha256"] != assignment_by_field["raw_container_sha256"]
            or file_row["raw_container_sha256"] != assignment_by_field["raw_container_sha256"]
        ):
            raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
        file_versions.append(
            {
                "source_file_version_id": _required_text(file_row["source_file_version_id"], 128),
                "source_identity_sha256": _engine_identity(file_row["version_source_identity_hash"]),
                "raw_sha256": _sha256(file_row["version_raw_sha256"]),
            }
        )
        source_keys.append(assignment_by_field["source_key"])
    if len(set(source_keys)) != len(source_keys) or len(
        {file["source_file_version_id"] for file in file_versions}
    ) != len(file_versions):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return sorted(file_versions, key=lambda file: file["source_file_version_id"]), source_keys


async def published_plan_inventory(session, schema_name, ownership, *, operation_id):
    """Verify original authority and exact file joins; never infer plan-specific subsets."""
    authority = (
        await prepare_ptg_result_archive_source_authority(
            session, schema_name=schema_name, snapshot_id=ownership["snapshot_id"], operation_id=operation_id
        )
    ).as_dict()
    if authority["contract"] != AUTHORITY_CONTRACT:
        raise RegistryPTGProducerScopeError("registry_ptg_published_source_required")
    identity = authority["identity"]
    if any(
        identity[name] != ownership[other]
        for name, other in (
            ("snapshot_id", "snapshot_id"),
            ("source_key", "source_key"),
            ("import_run_id", "engine_run_id"),
        )
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    schema = _identifier(schema_name)
    plan_scopes = await _published_plan_rows(session, schema, ownership["snapshot_id"], identity)
    file_versions, source_keys = await _published_file_rows(session, schema, ownership["snapshot_id"], identity)
    if (
        sum(
            version["source_file_version_id"] == ownership["engine_source_file_version_id"]
            and version["source_identity_sha256"] == ownership["engine_source_identity_hash"]
            for version in file_versions
        )
        != 1
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_scope_source_changed")
    return {
        "authority": authority,
        "identity": identity,
        "plan_scopes": plan_scopes,
        "file_versions": file_versions,
        "source_keys": source_keys,
        "selection_mode": SELECTION_MODE,
    }
