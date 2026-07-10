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
from process.ptg_parts.db_tables import _quote_ident, _table_exists
from process.ptg_parts.domain import (
    PTG2_STATUS_BUILDING,
    PTG2_STATUS_PENDING,
    PTG2_STATUS_RUNNING,
    PTG2_STATUS_VALIDATED,
)
from process.ptg_parts.ptg2_artifact_blobs import ptg2_artifact_id_from_db_uri
from process.ptg_parts.source_pointers import PTG2_SOURCE_POINTER_GC_LOCK_KEY


PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV = "HLTHPRT_PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE"
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
  ) current_refs
"""


def _is_ptg2_snapshot_in_flight(snapshot_status: Any) -> bool:
    return str(snapshot_status or "").strip().lower() in PTG2_IN_FLIGHT_SNAPSHOT_STATUSES


def _dedupe_preserve_table_names(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def _snapshot_manifest_table_names(serving_index: dict[str, Any] | None) -> list[str]:
    if not serving_index:
        return []
    materialized_tables = serving_index.get("materialized_tables")
    materialized_table_values = (
        list(materialized_tables.values()) if isinstance(materialized_tables, dict) else []
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
    result: list[str] = []
    for value in table_values:
        if not value:
            continue
        table_name = str(value).split(".", 1)[1] if "." in str(value) else str(value)
        if table_name.startswith(allowed_prefixes) and re.fullmatch(r"[a-z0-9_]{1,63}", table_name):
            result.append(table_name)
    return _dedupe_preserve_table_names(result)


def _required_snapshot_table_names(serving_index: dict[str, Any]) -> list[str]:
    materialized_tables = serving_index.get("materialized_tables")
    if isinstance(materialized_tables, dict) and materialized_tables:
        return _snapshot_manifest_table_names({"materialized_tables": materialized_tables})
    table_names = _snapshot_manifest_table_names(serving_index)
    if serving_index.get("serving_table_retained") is not False:
        return table_names
    transient_table_names = set(
        _snapshot_manifest_table_names({"table": serving_index.get("table")})
    )
    return [table_name for table_name in table_names if table_name not in transient_table_names]


def _snapshot_artifact_entries(serving_index: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = serving_index.get("artifacts")
    if not isinstance(artifacts, dict):
        return []
    artifact_entries: list[dict[str, Any]] = []
    for artifact_name, artifact_value in artifacts.items():
        if artifact_name == "sidecars" and isinstance(artifact_value, list):
            artifact_entries.extend(entry for entry in artifact_value if isinstance(entry, dict))
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
    arch_version = str(
        serving_index.get("arch_version")
        or serving_index.get("serving_binary_arch_version")
        or ""
    ).strip().lower()
    return arch_version == "postgres_binary_v3"


def _snapshot_artifact_references(serving_index: dict[str, Any]) -> tuple[set[str], set[str]]:
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
    artifact_tables_are_available = await _table_exists(
        schema_name, "ptg2_artifact_manifest"
    ) and await _table_exists(schema_name, "ptg2_artifact_blob_chunk")
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
        str((artifact_row if isinstance(artifact_row, dict) else artifact_row._mapping).get("artifact_id"))
        for artifact_row in artifact_rows
    }


async def _missing_snapshot_serving_resources(
    schema_name: str,
    snapshot_id: str,
    serving_index: dict[str, Any],
) -> tuple[list[str], list[str]]:
    required_table_names = _required_snapshot_table_names(serving_index)
    missing_table_names = [
        table_name
        for table_name in required_table_names
        if not await _table_exists(schema_name, table_name)
    ]
    db_artifact_ids, local_artifact_references = _snapshot_artifact_references(serving_index)
    available_db_artifact_ids = await _available_snapshot_db_artifact_ids(
        schema_name,
        snapshot_id,
        db_artifact_ids,
    )
    missing_artifacts = sorted(db_artifact_ids - available_db_artifact_ids)
    missing_artifacts.extend(
        sorted(
            reference
            for reference in local_artifact_references
            if not _is_local_artifact_available(reference)
        )
    )
    return missing_table_names, missing_artifacts


async def _drop_ptg2_snapshot_table_names(
    table_names: list[str],
    *,
    executor: Any | None = None,
) -> None:
    if not table_names:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    executor = executor or db
    for table_name in table_names:
        await executor.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(table_name)};"
        )


async def _drop_ptg2_snapshot_tables_for_manifest(serving_index: dict[str, Any] | None) -> None:
    await _drop_ptg2_snapshot_table_names(_snapshot_manifest_table_names(serving_index))


def _source_snapshot_lineage_limit() -> int:
    raw_value = os.getenv(PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "4")
    try:
        return max(1, min(int(raw_value), 50))
    except (TypeError, ValueError):
        return 4


def _source_snapshot_keep_ids(rows: list[Any], current_snapshot_ids: set[str]) -> set[str]:
    previous_snapshot_by_id = {}
    for row in rows:
        data = row if isinstance(row, dict) else row._mapping
        snapshot_id = str(data.get("snapshot_id") or "")
        previous_snapshot_by_id[snapshot_id] = str(data.get("previous_snapshot_id") or "")
    keep_snapshot_ids = {str(snapshot_id) for snapshot_id in current_snapshot_ids if snapshot_id}
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
            (pointer_row if isinstance(pointer_row, dict) else pointer_row._mapping).get(
                "snapshot_id"
            )
        )
        for pointer_row in pointer_rows
        if (pointer_row if isinstance(pointer_row, dict) else pointer_row._mapping).get(
            "snapshot_id"
        )
    }


def _snapshot_serving_index_dict(snapshot_row: Any) -> dict[str, Any]:
    snapshot_fields = snapshot_row if isinstance(snapshot_row, dict) else snapshot_row._mapping
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
        snapshot_fields = snapshot_row if isinstance(snapshot_row, dict) else snapshot_row._mapping
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
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    all_snapshot_rows = await _all_snapshot_manifest_rows(
        executor,
        schema_name=schema_name,
    )
    snapshot_rows = [
        snapshot_row
        for snapshot_row in all_snapshot_rows
        if str(_snapshot_serving_index_dict(snapshot_row).get("source_key") or "")
        == source_key
    ]
    keep_snapshot_ids = _source_snapshot_keep_ids(snapshot_rows, keep_snapshot_ids)
    table_owners = _snapshot_table_owners(all_snapshot_rows)
    drop_table_names: list[str] = []
    for snapshot_row in snapshot_rows:
        snapshot_fields = snapshot_row if isinstance(snapshot_row, dict) else snapshot_row._mapping
        snapshot_id = str(snapshot_fields.get("snapshot_id") or "")
        snapshot_table_names = _snapshot_manifest_table_names(
            _snapshot_serving_index_dict(snapshot_row)
        )
        if snapshot_id in keep_snapshot_ids or _is_ptg2_snapshot_in_flight(
            snapshot_fields.get("status")
        ):
            continue
        drop_table_names.extend(
            table_name
            for table_name in snapshot_table_names
            if table_owners.get(table_name) == {snapshot_id}
        )
    await _drop_ptg2_snapshot_table_names(
        _dedupe_preserve_table_names(drop_table_names),
        executor=executor,
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

    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
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
