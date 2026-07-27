# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cleanup helpers for source-scoped PTG2 snapshot tables."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from db.connection import db
from process.ptg_parts.artifacts import resolve_ptg2_artifact_dir
from process.ptg_parts.db_tables import _quote_ident, _is_table_available
from process.ptg_parts.domain import (
    PTG2_STATUS_BUILDING,
    PTG2_STATUS_PENDING,
    PTG2_STATUS_RUNNING,
    PTG2_STATUS_VALIDATED,
)
from process.ptg_parts.ptg2_artifact_blobs import ptg2_artifact_id_from_db_uri
from process.ptg_parts.ptg2_shared_audit import (
    PTG2_V3_AUDIT_CONTRACT,
    PTG2_V3_AUDIT_MAX_SAMPLE_ROWS,
    PTG2_V3_AUDIT_METHOD,
    persisted_audit_sample_digest,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
)
from process.ptg_parts.ptg2_shared_gc import (
    release_unbound_ptg2_shared_layouts,
)
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_v4_stale_metadata_fence import (
    drop_attempt_stage_tables,
)
from process.ptg_parts.source_pointers import PTG2_SOURCE_POINTER_GC_LOCK_KEY


PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV = "HLTHPRT_PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE"
PTG2_V3_ARCH_VERSION = "postgres_binary_v3"
_STRICT_V3_SHARED_TABLE_NAMES = (
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_block",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_graph_owner",
    "ptg2_v3_code",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_price_attr",
    "ptg2_v3_npi_scope",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_source_audit_witness_part",
    "ptg2_v3_source_audit_witness",
)
_COVERAGE_SCOPE_ID_RE = re.compile(r"^[0-9a-f]{64}$")
PTG2_IN_FLIGHT_SNAPSHOT_STATUSES = frozenset(
    {
        PTG2_STATUS_PENDING,
        PTG2_STATUS_RUNNING,
        PTG2_STATUS_BUILDING,
        PTG2_STATUS_VALIDATED,
    }
)

_CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL = """
SELECT DISTINCT snapshot_id
  FROM (
        SELECT snapshot_id
          FROM __SCHEMA__.ptg2_current_source_snapshot
         WHERE source_key = :source_key
           AND snapshot_id IS NOT NULL
        UNION ALL
        SELECT previous_snapshot_id AS snapshot_id
          FROM __SCHEMA__.ptg2_current_source_snapshot
         WHERE source_key = :source_key
           AND previous_snapshot_id IS NOT NULL
        UNION ALL
        SELECT snapshot_id
          FROM __SCHEMA__.ptg2_current_plan_source
         WHERE source_key = :source_key
           AND snapshot_id IS NOT NULL
        UNION ALL
        SELECT previous_snapshot_id AS snapshot_id
          FROM __SCHEMA__.ptg2_current_plan_source
         WHERE source_key = :source_key
           AND previous_snapshot_id IS NOT NULL
        UNION ALL
        SELECT current_pointer.snapshot_id
          FROM __SCHEMA__.ptg2_current_snapshot AS current_pointer
          JOIN __SCHEMA__.ptg2_snapshot AS snapshot
            ON snapshot.snapshot_id = current_pointer.snapshot_id
         WHERE current_pointer.slot = 'current'
           AND snapshot.manifest->'serving_index'->>'source_key' = :source_key
        UNION ALL
        SELECT current_pointer.previous_snapshot_id AS snapshot_id
          FROM __SCHEMA__.ptg2_current_snapshot AS current_pointer
          JOIN __SCHEMA__.ptg2_snapshot AS snapshot
            ON snapshot.snapshot_id = current_pointer.previous_snapshot_id
         WHERE current_pointer.slot = 'current'
           AND current_pointer.previous_snapshot_id IS NOT NULL
           AND snapshot.manifest->'serving_index'->>'source_key' = :source_key
        UNION ALL
        SELECT pin.snapshot_id
          FROM __SCHEMA__.ptg2_snapshot_pin AS pin
          JOIN __SCHEMA__.ptg2_snapshot AS snapshot
            ON snapshot.snapshot_id = pin.snapshot_id
         WHERE snapshot.manifest->'serving_index'->>'source_key' = :source_key
  ) current_refs
"""

_SNAPSHOT_SERVING_RESOURCE_SQL = """
SELECT layout.state,
       layout.generation,
       layout.layout_manifest,
       layout.mapping_digest,
       layout.support_digest,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_snapshot_block AS mapping
         WHERE mapping.snapshot_key = layout.snapshot_key) AS mapping_count,
       (SELECT COUNT(*)
          FROM __SCHEMA__.ptg2_v3_snapshot_block AS mapping
          JOIN __SCHEMA__.ptg2_v3_block AS block
            ON block.block_hash = mapping.block_hash
         WHERE mapping.snapshot_key = layout.snapshot_key) AS resolved_mapping_count,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_graph_owner AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_graph_owner,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_code AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_code,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_code AS dense
         WHERE dense.snapshot_key = layout.snapshot_key) AS code_count,
       (SELECT COUNT(DISTINCT dense.coverage_scope_id)
          FROM __SCHEMA__.ptg2_v3_code AS dense
         WHERE dense.snapshot_key = layout.snapshot_key) AS code_scope_count,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_code AS dense
         WHERE dense.snapshot_key = layout.snapshot_key
           AND dense.coverage_scope_id = :coverage_scope_id) AS matching_code_scope_count,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_provider_group AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_provider_group,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_provider_set AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_provider_set,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_price_attr AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_price_attr,
       EXISTS (SELECT 1 FROM __SCHEMA__.ptg2_v3_npi_scope AS dense
                WHERE dense.snapshot_key = layout.snapshot_key LIMIT 1) AS has_npi_scope,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_snapshot_scope AS scope
         WHERE scope.snapshot_id = binding.snapshot_id) AS scope_count,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_snapshot_scope AS scope
         WHERE scope.snapshot_id = binding.snapshot_id
           AND scope.coverage_scope_id = :coverage_scope_id) AS matching_scope_count,
       (SELECT COUNT(*) FROM __SCHEMA__.ptg2_v3_snapshot_plan_scope AS plan_scope
         WHERE plan_scope.snapshot_id = binding.snapshot_id) AS logical_plan_scope_count
  FROM __SCHEMA__.ptg2_v3_snapshot_binding AS binding
  JOIN __SCHEMA__.ptg2_v3_snapshot_layout AS layout
    ON layout.snapshot_key = binding.snapshot_key
 WHERE binding.snapshot_id = :snapshot_id
   AND binding.snapshot_key = :shared_snapshot_key
"""


def _is_ptg2_snapshot_in_flight(snapshot_status: Any) -> bool:
    return (
        str(snapshot_status or "").strip().lower() in PTG2_IN_FLIGHT_SNAPSHOT_STATUSES
    )


def _is_strict_shared_manifest(
    serving_index: dict[str, Any] | None,
) -> bool:
    """Return whether a manifest declares the only cleanup-safe layout."""

    if not isinstance(serving_index, dict):
        return False
    return (
        str(serving_index.get("arch_version") or "").strip().lower()
        == PTG2_V3_ARCH_VERSION
        and str(serving_index.get("storage_generation") or "").strip().lower()
        == PTG2_V3_SHARED_GENERATION
    )


is_strict_ptg2_v3_shared_blocks_manifest = _is_strict_shared_manifest


def is_shared_snapshot_control_manifest(
    serving_index: dict[str, Any] | None,
) -> bool:
    """Admit only shared V3/V4 layouts to generation-aware snapshot control."""

    if not isinstance(serving_index, dict):
        return False
    return (
        str(serving_index.get("arch_version") or "").strip().lower()
        == PTG2_V3_ARCH_VERSION
        and str(serving_index.get("storage_generation") or "").strip().lower()
        in {PTG2_V3_SHARED_GENERATION, PTG2_V4_SHARED_GENERATION}
    )


def _dedupe_preserve_table_names(seq: list[str]) -> list[str]:
    seen_table_names: set[str] = set()
    unique_table_names: list[str] = []
    for table_name in seq:
        if table_name not in seen_table_names:
            seen_table_names.add(table_name)
            unique_table_names.append(table_name)
    return unique_table_names


def _snapshot_manifest_table_names(serving_index: dict[str, Any] | None) -> list[str]:
    if not serving_index:
        return []
    materialized_tables = serving_index.get("materialized_tables")
    materialized_table_values = (
        list(materialized_tables.values())
        if isinstance(materialized_tables, dict)
        else []
    )
    table_values = [
        serving_index.get("table"),
        serving_index.get("serving_binary_table"),
        serving_index.get("price_code_set_table"),
        serving_index.get("price_atom_table"),
        serving_index.get("price_atom_dictionary_table"),
        serving_index.get("price_table"),
        serving_index.get("price_set_entry_table"),
        serving_index.get("procedure_table"),
        serving_index.get("provider_set_table"),
        serving_index.get("provider_set_dictionary_table"),
        serving_index.get("provider_set_component_table"),
        serving_index.get("provider_set_entry_table"),
        serving_index.get("provider_entry_component_table"),
        serving_index.get("provider_group_member_table"),
        serving_index.get("provider_npi_scope_table"),
        serving_index.get("provider_group_location_table"),
        serving_index.get("provider_group_rate_scope_table"),
        serving_index.get("code_count_table"),
        *materialized_table_values,
    ]
    allowed_prefixes = (
        "ptg2_serving_",
        "ptg2_serving_binary_",
        "ptg2_serving_rate_compact_",
        "ptg2_code_count_",
        "ptg2_price_code_set_",
        "ptg2_price_atom_",
        "ptg2_price_set_",
        "ptg2_price_set_entry_",
        "ptg2_procedure_",
        "ptg2_provider_set_",
        "ptg2_provider_set_entry_",
        "ptg2_provider_entry_component_",
        "ptg2_provider_group_member_",
        "ptg2_provider_npi_scope_",
        "ptg2_provider_group_location_",
        "ptg2_provider_group_rate_scope_",
    )
    manifest_table_names: list[str] = []
    for table_value in table_values:
        if not table_value:
            continue
        table_text = str(table_value)
        table_name = table_text.split(".", 1)[1] if "." in table_text else table_text
        if table_name.startswith(allowed_prefixes) and re.fullmatch(
            r"[a-z0-9_]{1,63}", table_name
        ):
            manifest_table_names.append(table_name)
    return _dedupe_preserve_table_names(manifest_table_names)


def _required_snapshot_table_names(serving_index: dict[str, Any]) -> list[str]:
    materialized_tables = serving_index.get("materialized_tables")
    if isinstance(materialized_tables, dict) and materialized_tables:
        return _snapshot_manifest_table_names(
            {"materialized_tables": materialized_tables}
        )
    table_names = _snapshot_manifest_table_names(serving_index)
    if serving_index.get("serving_table_retained") is not False:
        return table_names
    transient_table_names = set(
        _snapshot_manifest_table_names({"table": serving_index.get("table")})
    )
    return [
        table_name
        for table_name in table_names
        if table_name not in transient_table_names
    ]


def _snapshot_artifact_entries(serving_index: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = serving_index.get("artifacts")
    if not isinstance(artifacts, dict):
        return []
    artifact_entries: list[dict[str, Any]] = []
    for artifact_name, artifact_value in artifacts.items():
        if artifact_name == "sidecars" and isinstance(artifact_value, list):
            artifact_entries.extend(
                entry for entry in artifact_value if isinstance(entry, dict)
            )
        elif isinstance(artifact_value, dict):
            artifact_entries.append(artifact_value)
    return artifact_entries


def _record_snapshot_artifact_reference(
    raw_reference: Any,
    db_artifact_ids: set[str],
    local_artifact_references: set[str],
    *,
    include_local_artifacts: bool,
) -> None:
    artifact_reference = str(raw_reference or "").strip()
    if not artifact_reference:
        return
    artifact_id = ptg2_artifact_id_from_db_uri(artifact_reference)
    if artifact_id:
        db_artifact_ids.add(artifact_id)
    elif include_local_artifacts:
        local_artifact_references.add(artifact_reference)


def _uses_postgres_only_artifacts(serving_index: dict[str, Any]) -> bool:
    return is_strict_ptg2_v3_shared_blocks_manifest(serving_index)


def _snapshot_artifact_references(
    serving_index: dict[str, Any],
) -> tuple[set[str], set[str]]:
    db_artifact_ids: set[str] = set()
    local_artifact_references: set[str] = set()
    include_local_artifacts = not _uses_postgres_only_artifacts(serving_index)
    for artifact_reference_key in ("artifact_uri", "storage_uri"):
        _record_snapshot_artifact_reference(
            serving_index.get(artifact_reference_key),
            db_artifact_ids,
            local_artifact_references,
            include_local_artifacts=include_local_artifacts,
        )
    for artifact_entry in _snapshot_artifact_entries(serving_index):
        storage_uri = artifact_entry.get("storage_uri")
        _record_snapshot_artifact_reference(
            storage_uri if storage_uri else artifact_entry.get("path"),
            db_artifact_ids,
            local_artifact_references,
            include_local_artifacts=include_local_artifacts,
        )
    return db_artifact_ids, local_artifact_references


def _is_local_artifact_available(artifact_reference: str) -> bool:
    if artifact_reference.startswith("file://"):
        artifact_path = Path(unquote(urlsplit(artifact_reference).path))
    else:
        artifact_path = Path(artifact_reference)
    candidate_paths = [artifact_path]
    if not artifact_path.is_absolute():
        candidate_paths.append(resolve_ptg2_artifact_dir() / artifact_path)
    return any(candidate_path.is_file() for candidate_path in candidate_paths)


async def _available_snapshot_db_artifact_ids(
    schema_name: str,
    snapshot_id: str,
    artifact_ids: set[str],
) -> set[str]:
    if not artifact_ids:
        return set()
    artifact_tables_are_available = await _is_table_available(
        schema_name, "ptg2_artifact_manifest"
    ) and await _is_table_available(schema_name, "ptg2_artifact_blob_chunk")
    if not artifact_tables_are_available:
        return set()
    artifact_rows = await db.all(
        f"""
        SELECT artifact.artifact_id
          FROM {_quote_ident(schema_name)}.ptg2_artifact_manifest AS artifact
          JOIN {_quote_ident(schema_name)}.ptg2_artifact_blob_chunk AS chunk
            ON chunk.artifact_id = artifact.artifact_id
         WHERE artifact.snapshot_id = :snapshot_id
           AND artifact.artifact_id = ANY(:artifact_ids)
         GROUP BY artifact.artifact_id, artifact.byte_count
        HAVING COUNT(*) > 0
           AND (artifact.byte_count IS NULL OR SUM(chunk.raw_byte_count) = artifact.byte_count)
        """,
        snapshot_id=snapshot_id,
        artifact_ids=sorted(artifact_ids),
    )
    return {
        str(
            (
                artifact_row
                if isinstance(artifact_row, dict)
                else artifact_row._mapping
            ).get("artifact_id")
        )
        for artifact_row in artifact_rows
    }


def _audit_sample_context(
    audit_sample: Any,
) -> tuple[int, str, bool]:
    if not isinstance(audit_sample, dict):
        return -1, "", False
    try:
        sample_count = int(audit_sample.get("sample_count"))
        format_version = int(audit_sample.get("format_version"))
        maximum_rows = int(audit_sample.get("maximum_rows"))
    except (TypeError, ValueError):
        return -1, "", False
    sample_digest = str(audit_sample.get("sample_digest") or "").strip().lower()
    valid = (
        audit_sample.get("contract") == PTG2_V3_AUDIT_CONTRACT
        and audit_sample.get("method") == PTG2_V3_AUDIT_METHOD
        and audit_sample.get("serving_multiplicity_semantics")
        == PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS
        and audit_sample.get("complete_population") is False
        and format_version == 2
        and maximum_rows == PTG2_V3_AUDIT_MAX_SAMPLE_ROWS
        and 0 <= sample_count <= PTG2_V3_AUDIT_MAX_SAMPLE_ROWS
        and bool(_COVERAGE_SCOPE_ID_RE.fullmatch(sample_digest))
    )
    return sample_count, sample_digest, valid


def _serving_contract_context(
    serving_index: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    errors: list[str] = []
    expected_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
        "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
        "serving_multiplicity_semantics": PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    }
    for field_name, expected_value in expected_by_field.items():
        observed = str(serving_index.get(field_name) or "").strip().lower()
        if observed != expected_value:
            errors.append(field_name)
    try:
        snapshot_key = int(serving_index.get("shared_snapshot_key"))
    except (TypeError, ValueError):
        snapshot_key = 0
    if snapshot_key <= 0:
        errors.append("shared_snapshot_key")
    raw_coverage_scope_id = serving_index.get("coverage_scope_id")
    if isinstance(raw_coverage_scope_id, str) and _COVERAGE_SCOPE_ID_RE.fullmatch(
        raw_coverage_scope_id
    ):
        coverage_scope_id = bytes.fromhex(raw_coverage_scope_id)
    else:
        coverage_scope_id = b""
    if len(coverage_scope_id) != 32:
        errors.append("coverage_scope_id")
    sample_count, sample_digest, audit_valid = _audit_sample_context(
        serving_index.get("audit_sample")
    )
    if not audit_valid:
        errors.append("audit_sample")
    return {
        "snapshot_key": snapshot_key,
        "coverage_scope_id": coverage_scope_id,
        "audit_sample_count": sample_count,
        "audit_sample_digest": sample_digest,
    }, errors


async def _load_serving_resource_record(
    schema_name: str,
    snapshot_id: str,
    context: dict[str, Any],
) -> dict[str, Any] | None:
    rows = await db.all(
        _SNAPSHOT_SERVING_RESOURCE_SQL.replace(
            "__SCHEMA__",
            _quote_ident(schema_name),
        ),
        snapshot_id=snapshot_id,
        shared_snapshot_key=context["snapshot_key"],
        coverage_scope_id=context["coverage_scope_id"],
    )
    if len(rows) != 1:
        return None
    return rows[0] if isinstance(rows[0], dict) else dict(rows[0]._mapping)


def _validate_resource_identity(resource_record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if str(resource_record.get("state") or "").strip().lower() != "sealed":
        errors.append("layout_state")
    if (
        str(resource_record.get("generation") or "").strip().lower()
        != PTG2_V3_SHARED_GENERATION
    ):
        errors.append("layout_generation")
    if len(bytes(resource_record.get("mapping_digest") or b"")) != 32:
        errors.append("mapping_digest")
    if len(bytes(resource_record.get("support_digest") or b"")) != 32:
        errors.append("support_digest")
    mapping_count = int(resource_record.get("mapping_count") or 0)
    if mapping_count <= 0:
        errors.append("snapshot_blocks")
    if int(resource_record.get("resolved_mapping_count") or 0) != mapping_count:
        errors.append("unresolved_snapshot_blocks")
    if int(resource_record.get("scope_count") or 0) != 1:
        errors.append("snapshot_scope")
    if int(resource_record.get("matching_scope_count") or 0) != 1:
        errors.append("coverage_scope_binding")
    if int(resource_record.get("logical_plan_scope_count") or 0) <= 0:
        errors.append("logical_plan_scope")
    return errors


def _validate_code_scope(resource_record: dict[str, Any]) -> tuple[int, list[str]]:
    code_count = int(resource_record.get("code_count") or 0)
    code_scope_count = int(resource_record.get("code_scope_count") or 0)
    matching_code_scope_count = int(
        resource_record.get("matching_code_scope_count") or 0
    )
    if code_count == 0:
        if code_scope_count != 0 or matching_code_scope_count != 0:
            return code_count, ["coverage_scope_code"]
    elif code_scope_count != 1 or matching_code_scope_count != code_count:
        return code_count, ["coverage_scope_code"]
    return code_count, []


def _validate_layout_manifest(
    resource_record: dict[str, Any],
    serving_index: dict[str, Any],
) -> list[str]:
    layout_manifest = resource_record.get("layout_manifest")
    if isinstance(layout_manifest, str):
        try:
            layout_manifest = json.loads(layout_manifest)
        except json.JSONDecodeError:
            layout_manifest = None
    layout_serving_index = (
        layout_manifest.get("serving_index")
        if isinstance(layout_manifest, dict)
        else None
    )
    if not isinstance(layout_serving_index, dict):
        return ["layout_manifest"]
    fields = (
        "arch_version",
        "storage_generation",
        "cold_lookup_contract",
        "price_membership_semantics",
        "serving_multiplicity_semantics",
        "shared_snapshot_key",
        "coverage_scope_id",
        "serving_rates",
        "atom_key_bits",
        "audit_sample",
    )
    return [
        f"layout_manifest:{field_name}"
        for field_name in fields
        if layout_serving_index.get(field_name) != serving_index.get(field_name)
    ]


async def _validate_persisted_audit(
    schema_name: str,
    context: dict[str, Any],
) -> list[str]:
    schema = _quote_ident(schema_name)
    audit_rows = await db.all(
        f"""
        SELECT occurrence_id, code_key, provider_set_key, price_key,
               source_key, npi, atom_ordinal, atom_key
          FROM {schema}.ptg2_v3_audit_occurrence
         WHERE snapshot_key = :shared_snapshot_key
         ORDER BY occurrence_id
         LIMIT :row_limit
        """,
        shared_snapshot_key=context["snapshot_key"],
        row_limit=PTG2_V3_AUDIT_MAX_SAMPLE_ROWS + 1,
    )
    audit_row_mappings = [
        audit_row if isinstance(audit_row, dict) else dict(audit_row._mapping)
        for audit_row in audit_rows
    ]
    if len(audit_row_mappings) != context["audit_sample_count"]:
        return ["audit_sample:row_count"]
    if (
        persisted_audit_sample_digest(audit_row_mappings)
        != context["audit_sample_digest"]
    ):
        return ["audit_sample:digest"]
    return []


def _validate_dense_resources(
    resource_record: dict[str, Any],
    serving_index: dict[str, Any],
    code_count: int,
) -> list[str]:
    errors: list[str] = []
    serving_rate_count = int(serving_index.get("serving_rates") or 0)
    provider_graph = serving_index.get("provider_graph")
    provider_graph = provider_graph if isinstance(provider_graph, dict) else {}
    required_dense_count_by_name = {
        "graph_owner": int(provider_graph.get("owner_count") or 0),
        "provider_group": int(provider_graph.get("provider_group_count") or 0),
        "provider_set": serving_rate_count,
        "price_attr": serving_rate_count,
        "npi_scope": int(provider_graph.get("npi_count") or 0),
    }
    for dense_name, expected_count in required_dense_count_by_name.items():
        if expected_count > 0 and not bool(
            resource_record.get(f"has_{dense_name}")
        ):
            errors.append(f"dense:{dense_name}")
    if serving_rate_count > 0 and (
        code_count <= 0 or not bool(resource_record.get("has_code"))
    ):
        errors.append("dense:code")
    return errors


async def _missing_snapshot_serving_resources(
    schema_name: str,
    snapshot_id: str,
    serving_index: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Return missing serving resources and contract errors for a snapshot."""

    context, contract_errors = _serving_contract_context(serving_index)
    missing_table_names = [
        table_name
        for table_name in _STRICT_V3_SHARED_TABLE_NAMES
        if not await _is_table_available(schema_name, table_name)
    ]
    if missing_table_names or contract_errors:
        return missing_table_names, contract_errors
    resource_record = await _load_serving_resource_record(
        schema_name,
        snapshot_id,
        context,
    )
    if resource_record is None:
        return missing_table_names, ["snapshot_binding"]
    contract_errors.extend(_validate_resource_identity(resource_record))
    code_count, code_errors = _validate_code_scope(resource_record)
    contract_errors.extend(code_errors)
    contract_errors.extend(
        _validate_layout_manifest(resource_record, serving_index)
    )
    contract_errors.extend(
        await _validate_persisted_audit(schema_name, context)
    )
    contract_errors.extend(
        _validate_dense_resources(resource_record, serving_index, code_count)
    )
    return missing_table_names, contract_errors


async def _drop_ptg2_snapshot_table_names(
    table_names: list[str],
    *,
    executor: Any | None = None,
    snapshot_id: str | None = None,
    internal_run_id: str | None = None,
    retain_attempt_registration: bool = False,
) -> None:
    if not table_names:
        return
    schema_name = resolve_ptg2_schema()
    executor = executor or db
    if snapshot_id or internal_run_id:
        if not snapshot_id or not internal_run_id:
            raise ValueError(
                "attempt stage cleanup requires snapshot and run identifiers"
            )
        await drop_attempt_stage_tables(
            executor,
            schema_name=schema_name,
            snapshot_id=snapshot_id,
            internal_run_id=internal_run_id,
            table_names=table_names,
            retain_registration=retain_attempt_registration,
        )
        return
    for table_name in table_names:
        await executor.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)};"
        )


async def _drop_snapshot_tables_for_manifest(
    serving_index: dict[str, Any] | None,
) -> None:
    if not is_strict_ptg2_v3_shared_blocks_manifest(serving_index):
        return
    # Strict V3 stores serving data in shared PostgreSQL blocks, never in
    # manifest-declared snapshot tables.
    await _drop_ptg2_snapshot_table_names([])


_drop_ptg2_snapshot_tables_for_manifest = _drop_snapshot_tables_for_manifest


def _source_snapshot_lineage_limit() -> int:
    raw_value = os.getenv(PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "4")
    try:
        return max(1, min(int(raw_value), 50))
    except (TypeError, ValueError):
        return 4


def _source_snapshot_keep_ids(
    rows: list[Any], current_snapshot_ids: set[str]
) -> set[str]:
    previous_snapshot_by_id = {}
    for row in rows:
        data = row if isinstance(row, dict) else row._mapping
        snapshot_id = str(data.get("snapshot_id") or "")
        previous_snapshot_by_id[snapshot_id] = str(
            data.get("previous_snapshot_id") or ""
        )
    keep_snapshot_ids = {
        str(snapshot_id) for snapshot_id in current_snapshot_ids if snapshot_id
    }
    for current_snapshot_id in tuple(keep_snapshot_ids):
        lineage_snapshot_id = current_snapshot_id
        for _lineage_depth in range(1, _source_snapshot_lineage_limit()):
            lineage_snapshot_id = previous_snapshot_by_id.get(lineage_snapshot_id, "")
            if not lineage_snapshot_id or lineage_snapshot_id in keep_snapshot_ids:
                break
            keep_snapshot_ids.add(lineage_snapshot_id)
    return keep_snapshot_ids


async def _current_source_pointer_snapshot_ids(
    executor: Any,
    *,
    schema_name: str,
    source_key: str,
) -> set[str]:
    pointer_rows = await executor.all(
        _CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL.replace(
            "__SCHEMA__",
            _quote_ident(schema_name),
        ),
        source_key=source_key,
    )
    return {
        str(
            (
                pointer_row if isinstance(pointer_row, dict) else pointer_row._mapping
            ).get("snapshot_id")
        )
        for pointer_row in pointer_rows
        if (pointer_row if isinstance(pointer_row, dict) else pointer_row._mapping).get(
            "snapshot_id"
        )
    }


def _snapshot_serving_index_dict(snapshot_row: Any) -> dict[str, Any]:
    snapshot_fields = (
        snapshot_row if isinstance(snapshot_row, dict) else snapshot_row._mapping
    )
    manifest_dict = snapshot_fields.get("manifest") or {}
    if isinstance(manifest_dict, str):
        try:
            manifest_dict = json.loads(manifest_dict)
        except json.JSONDecodeError:
            manifest_dict = {}
    if not isinstance(manifest_dict, dict):
        return {}
    return manifest_dict.get("serving_index") or {}


def _snapshot_table_owners(snapshot_rows: list[Any]) -> dict[str, set[str]]:
    snapshot_ids_by_table_name: dict[str, set[str]] = {}
    for snapshot_row in snapshot_rows:
        snapshot_fields = (
            snapshot_row if isinstance(snapshot_row, dict) else snapshot_row._mapping
        )
        snapshot_id = str(snapshot_fields.get("snapshot_id") or "")
        if not snapshot_id:
            continue
        for table_name in _snapshot_manifest_table_names(
            _snapshot_serving_index_dict(snapshot_row)
        ):
            snapshot_ids_by_table_name.setdefault(table_name, set()).add(snapshot_id)
    return snapshot_ids_by_table_name


def _exclusively_owned_snapshot_table_names(
    snapshot_id: str,
    table_names: list[str],
    snapshot_rows: list[Any],
) -> list[str]:
    table_owners = _snapshot_table_owners(snapshot_rows)
    for table_name in table_names:
        table_owners.setdefault(table_name, set()).add(snapshot_id)
    return [
        table_name
        for table_name in _dedupe_preserve_table_names(table_names)
        if table_owners.get(table_name) == {snapshot_id}
    ]


async def _all_snapshot_manifest_rows(executor: Any, *, schema_name: str) -> list[Any]:
    return await executor.all(
        f"""
        SELECT snapshot_id, previous_snapshot_id, status, manifest
          FROM {_quote_ident(schema_name)}.ptg2_snapshot
        """
    )


async def _cleanup_source_tables(
    executor: Any,
    *,
    source_key: str,
    keep_snapshot_ids: set[str],
) -> None:
    schema_name = resolve_ptg2_schema()
    all_snapshot_rows = await _all_snapshot_manifest_rows(
        executor,
        schema_name=schema_name,
    )
    snapshot_rows = [
        snapshot_row
        for snapshot_row in all_snapshot_rows
        if str(_snapshot_serving_index_dict(snapshot_row).get("source_key") or "")
        == source_key
        and is_strict_ptg2_v3_shared_blocks_manifest(
            _snapshot_serving_index_dict(snapshot_row)
        )
    ]
    if snapshot_rows:
        await release_unbound_ptg2_shared_layouts(
            schema_name=schema_name,
            executor=executor,
            require_shared=True,
        )


async def _cleanup_old_ptg2_source_tables(
    source_key: str,
    keep_snapshot_ids: set[str],
    *,
    lock_pointer_state: bool = False,
) -> None:
    if not lock_pointer_state:
        await _cleanup_source_tables(
            db,
            source_key=source_key,
            keep_snapshot_ids=keep_snapshot_ids,
        )
        return

    schema_name = resolve_ptg2_schema()
    async with db.acquire() as connection:
        await connection.status(
            "SELECT pg_advisory_xact_lock(hashtext(:publish_lock_key))",
            publish_lock_key=PTG2_SOURCE_POINTER_GC_LOCK_KEY,
        )
        live_snapshot_ids = await _current_source_pointer_snapshot_ids(
            connection,
            schema_name=schema_name,
            source_key=source_key,
        )
        authoritative_keep_ids = live_snapshot_ids or set(keep_snapshot_ids)
        await _cleanup_source_tables(
            connection,
            source_key=source_key,
            keep_snapshot_ids=authoritative_keep_ids,
        )
