"""Attributable physical PostgreSQL storage accounting for PTG V4."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

import asyncpg

from scripts.ptg_v4_dev_canary_publication import (
    REQUIRED_PHYSICAL_RELATIONS,
    STORAGE_EVIDENCE_CONTRACT,
    WHOLE_SNAPSHOT_PHYSICAL_RELATIONS,
)
from scripts.ptg_v4_dev_canary_cas import collect_cas_evidence
from scripts.ptg_v4_dev_canary_storage_sql import owned_row_bytes


STORAGE_BASELINE_CONTRACT = "ptg_v4_physical_storage_baseline_v1"
MAP_OBJECT_KIND = "snapshot_coordinate_map_v1"


@dataclass(frozen=True)
class PhysicalStorageRequest:
    """Immutable inputs for one whole-snapshot physical measurement."""

    schema_name: str
    snapshot_id: str
    snapshot_key: int
    import_run_id: str
    relation_manifests: Sequence[Mapping[str, Any]]
    baseline: Mapping[str, Any]
    import_started_at: datetime
    import_finished_at: datetime
    retained_raw_artifacts: Mapping[str, Any]


@dataclass(frozen=True)
class _StorageAttributionScope:
    """Snapshot ownership and timing inputs shared across relation attribution."""

    schema_name: str
    snapshot_id: str
    snapshot_key: int
    import_run_id: str
    baseline_bytes: Mapping[str, int]
    import_started_at: datetime
    import_finished_at: datetime
    cas_evidence: Mapping[str, Any]


@dataclass(frozen=True)
class _StorageReportContext:
    """Precomputed sections used to shape the final storage report."""

    storage_records: Sequence[Mapping[str, Any]]
    totals_by_field: Mapping[str, int]
    object_kind_evidence: Mapping[str, Any]
    cas_record: Mapping[str, Any]
    cas_reference_source: str
    cas_reference_population: str
    retained_raw_artifacts: Mapping[str, Any]


async def relation_size_rows(
    connection: asyncpg.Connection,
    schema_name: str,
    relation_names: Sequence[str],
) -> list[dict[str, Any]]:
    """Measure relations plus every partition, including indexes."""

    relation_records = []
    for relation_name in relation_names:
        total_bytes = await connection.fetchval(
            """
            WITH RECURSIVE relation_tree(oid) AS (
                SELECT class.oid
                  FROM pg_class AS class
                  JOIN pg_namespace AS namespace
                    ON namespace.oid = class.relnamespace
                 WHERE namespace.nspname = $1 AND class.relname = $2
                UNION ALL
                SELECT inherits.inhrelid
                  FROM pg_inherits AS inherits
                  JOIN relation_tree ON relation_tree.oid = inherits.inhparent
            )
            SELECT COALESCE(SUM(pg_total_relation_size(oid)), 0)::bigint
              FROM relation_tree
            """,
            schema_name,
            relation_name,
        )
        exists = await connection.fetchval(
            "SELECT to_regclass(format('%I.%I', $1::text, $2::text)) IS NOT NULL",
            schema_name,
            relation_name,
        )
        relation_records.append(
            {
                "relation": relation_name,
                "exists": bool(exists),
                "total_bytes": int(total_bytes or 0),
            }
        )
    return relation_records


async def collect_physical_storage(
    connection: asyncpg.Connection,
    request: PhysicalStorageRequest,
) -> dict[str, Any]:
    """Allocate physical bytes to a snapshot and reconcile shared CAS blocks."""

    relation_names = sorted(WHOLE_SNAPSHOT_PHYSICAL_RELATIONS)
    size_rows = await relation_size_rows(
        connection,
        request.schema_name,
        relation_names,
    )
    attribution_scope = await _storage_attribution_scope(
        connection,
        request,
        relation_names=relation_names,
    )
    storage_records = await _build_storage_records(
        connection,
        attribution_scope,
        size_rows=size_rows,
    )
    object_kind_evidence = _object_kind_evidence(
        attribution_scope.cas_evidence,
        relation_manifests=request.relation_manifests,
    )
    totals_by_field = _storage_totals(
        storage_records,
        attribution_scope.baseline_bytes,
        retained_raw_artifacts=request.retained_raw_artifacts,
    )
    cas_record = next(
        storage_record
        for storage_record in storage_records
        if storage_record["relation"] == "ptg2_v3_block"
    )
    return _storage_report(
        _StorageReportContext(
            storage_records=storage_records,
            totals_by_field=totals_by_field,
            object_kind_evidence=object_kind_evidence,
            cas_record=cas_record,
            cas_reference_source=str(
                attribution_scope.cas_evidence.get("reference_source") or ""
            ),
            cas_reference_population=str(
                attribution_scope.cas_evidence.get("reference_population")
                or ""
            ),
            retained_raw_artifacts=request.retained_raw_artifacts,
        ),
    )


async def _storage_attribution_scope(
    connection: asyncpg.Connection,
    request: PhysicalStorageRequest,
    *,
    relation_names: Sequence[str],
) -> _StorageAttributionScope:
    """Load the baseline and CAS population shared by relation attribution."""

    baseline_bytes = _baseline_bytes(
        request.baseline,
        request.schema_name,
        relation_names,
    )
    cas_evidence = await collect_cas_evidence(
        connection,
        schema_name=request.schema_name,
        snapshot_key=request.snapshot_key,
        import_started_at=request.import_started_at,
        import_finished_at=request.import_finished_at,
    )
    return _StorageAttributionScope(
        schema_name=request.schema_name,
        snapshot_id=request.snapshot_id,
        snapshot_key=request.snapshot_key,
        import_run_id=request.import_run_id,
        baseline_bytes=baseline_bytes,
        import_started_at=request.import_started_at,
        import_finished_at=request.import_finished_at,
        cas_evidence=cas_evidence,
    )


async def _build_storage_records(
    connection: asyncpg.Connection,
    attribution_scope: _StorageAttributionScope,
    *,
    size_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Attribute every global relation record to the selected snapshot."""

    storage_records: list[dict[str, Any]] = []
    for size_row in size_rows:
        storage_records.append(
            await _measure_relation_attribution(
                connection,
                attribution_scope,
                size_row=size_row,
            )
        )
    return storage_records


async def _measure_relation_attribution(
    connection: asyncpg.Connection,
    attribution_scope: _StorageAttributionScope,
    *,
    size_row: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one physical relation record with reconciled row attribution."""

    relation_name = str(size_row["relation"])
    if relation_name == "ptg2_v3_block":
        row_bytes_by_field = dict(
            attribution_scope.cas_evidence.get("bytes") or {}
        )
    else:
        row_bytes_by_field = await owned_row_bytes(
            connection,
            attribution_scope.schema_name,
            relation_name,
            attribution_scope.snapshot_id,
            attribution_scope.snapshot_key,
            attribution_scope.import_run_id,
        )
    total_bytes = int(size_row["total_bytes"])
    allocated_by_field = {
        "attributed_bytes": _allocated_bytes(
            total_bytes,
            row_bytes_by_field["snapshot_weighted_row_bytes"],
            row_bytes_by_field["global_row_bytes"],
        ),
        "allocated_all_snapshot_physical_bytes": _allocated_bytes(
            total_bytes,
            row_bytes_by_field["allocated_all_snapshot_row_bytes"],
            row_bytes_by_field["global_row_bytes"],
        ),
        "unallocated_physical_bytes": _allocated_bytes(
            total_bytes,
            row_bytes_by_field["unallocated_row_bytes"],
            row_bytes_by_field["global_row_bytes"],
        ),
    }
    return {
        **size_row,
        **row_bytes_by_field,
        **allocated_by_field,
        "baseline_total_bytes": attribution_scope.baseline_bytes[relation_name],
        "allocation_reconciliation_delta_bytes": (
            total_bytes
            - allocated_by_field["allocated_all_snapshot_physical_bytes"]
            - allocated_by_field["unallocated_physical_bytes"]
        ),
    }


def _object_kind_evidence(
    cas_evidence: Mapping[str, Any],
    *,
    relation_manifests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return required CAS object-kind counts and any missing kinds."""

    counts_by_object_kind = dict(cas_evidence.get("object_kind_counts") or {})
    required_kinds = {
        str(relation_manifest[field_name])
        for relation_manifest in relation_manifests
        for field_name in ("member_object_kind", "locator_object_kind")
    }
    map_block_count = int(cas_evidence.get("map_cas_block_count") or 0)
    missing_kinds = sorted(required_kinds - set(counts_by_object_kind))
    if map_block_count < 1:
        missing_kinds.append(MAP_OBJECT_KIND)
    return {
        "counts": counts_by_object_kind,
        "missing": missing_kinds,
        "map_cas_block_count": map_block_count,
    }


def _storage_totals(
    storage_records: Sequence[Mapping[str, Any]],
    baseline_bytes: Mapping[str, int],
    *,
    retained_raw_artifacts: Mapping[str, Any],
) -> dict[str, int]:
    """Calculate attributed bytes and positive global growth for both scopes."""

    graph_records = [
        storage_record
        for storage_record in storage_records
        if storage_record["relation"] in REQUIRED_PHYSICAL_RELATIONS
    ]
    graph_attributed_total = sum(
        int(storage_record["attributed_bytes"])
        for storage_record in graph_records
    )
    retained_raw_physical_bytes = int(
        retained_raw_artifacts.get("referenced_physical_bytes") or 0
    )
    snapshot_attributed_total = retained_raw_physical_bytes + sum(
        int(storage_record["attributed_bytes"])
        for storage_record in storage_records
    )
    graph_before_total = sum(
        baseline_bytes[str(storage_record["relation"])]
        for storage_record in graph_records
    )
    graph_after_total = sum(
        int(storage_record["total_bytes"])
        for storage_record in graph_records
    )
    snapshot_before_total = sum(baseline_bytes.values())
    snapshot_after_total = sum(
        int(storage_record["total_bytes"])
        for storage_record in storage_records
    )
    return {
        "graph_attributed": graph_attributed_total,
        "graph_growth": max(graph_after_total - graph_before_total, 0),
        "snapshot_attributed": snapshot_attributed_total,
        "snapshot_growth": max(snapshot_after_total - snapshot_before_total, 0),
        "retained_raw_physical": retained_raw_physical_bytes,
    }


def _storage_report(
    context: _StorageReportContext,
) -> dict[str, Any]:
    """Shape the final physical-storage evidence contract."""

    totals_by_field = context.totals_by_field
    return {
        "contract": STORAGE_EVIDENCE_CONTRACT,
        "baseline_captured": True,
        "relations": list(context.storage_records),
        "storage_claim_scope": (
            "whole_snapshot_v4_graph_and_retained_raw"
        ),
        "graph_attributed_physical_bytes": totals_by_field["graph_attributed"],
        "graph_positive_import_delta_bytes": totals_by_field["graph_growth"],
        "graph_gate_bytes": max(
            totals_by_field["graph_attributed"],
            totals_by_field["graph_growth"],
        ),
        "snapshot_attributed_physical_bytes": totals_by_field[
            "snapshot_attributed"
        ],
        "snapshot_positive_import_delta_bytes": totals_by_field["snapshot_growth"],
        "retained_raw_artifact_physical_bytes": totals_by_field[
            "retained_raw_physical"
        ],
        "retained_raw_artifacts": dict(context.retained_raw_artifacts),
        "snapshot_gate_bytes": max(
            totals_by_field["snapshot_attributed"],
            totals_by_field["snapshot_growth"],
        ),
        "allocation_reconciled": _is_allocation_reconciled(
            context.storage_records
        ),
        "object_kind_counts": context.object_kind_evidence["counts"],
        "missing_required_object_kinds": context.object_kind_evidence["missing"],
        "map_cas_block_count": context.object_kind_evidence[
            "map_cas_block_count"
        ],
        "cas": _cas_storage_summary(context),
        "shared_cas_accounting": "weighted_by_distinct_snapshot_reference_count",
    }


def _is_allocation_reconciled(
    storage_records: Sequence[Mapping[str, Any]],
) -> bool:
    """Return whether row and allocated-byte attribution fully reconcile."""

    return all(
        int(storage_record["row_reconciliation_delta_bytes"]) == 0
        and abs(int(storage_record["allocation_reconciliation_delta_bytes"]))
        <= 1
        for storage_record in storage_records
    )


def _cas_storage_summary(context: _StorageReportContext) -> dict[str, Any]:
    """Shape authenticated shared-CAS reference accounting."""

    return {
        "reference_source": context.cas_reference_source,
        "reference_population": context.cas_reference_population,
        **{
            field_name: context.cas_record[field_name]
            for field_name in (
                "distinct_referenced_block_count",
                "new_during_import_block_count",
                "preexisting_reused_block_count",
                "shared_block_count",
            )
        },
    }


def _baseline_bytes(
    baseline: Mapping[str, Any],
    schema_name: str,
    relation_names: Sequence[str],
) -> dict[str, int]:
    if (
        baseline.get("contract") != STORAGE_BASELINE_CONTRACT
        or baseline.get("schema") != schema_name
    ):
        raise ValueError("physical storage baseline is missing or incompatible")
    relation_records = baseline.get("relations")
    if not isinstance(relation_records, list):
        raise ValueError("physical storage baseline relations are missing")
    bytes_by_relation = {
        str(relation_record.get("relation")): int(
            relation_record.get("total_bytes")
        )
        for relation_record in relation_records
        if isinstance(relation_record, Mapping)
    }
    if set(relation_names) - set(bytes_by_relation):
        raise ValueError("physical storage baseline lacks required relations")
    return {name: bytes_by_relation[name] for name in relation_names}


def _allocated_bytes(total_bytes: int, snapshot_bytes: int, global_bytes: int) -> int:
    if global_bytes <= 0:
        return 0
    return round(total_bytes * snapshot_bytes / global_bytes)
