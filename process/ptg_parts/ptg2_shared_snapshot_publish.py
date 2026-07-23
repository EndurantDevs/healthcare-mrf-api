# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""End-to-end physical publication for strict shared-block PTG V3."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_COLD_LOOKUP_CONTRACT,
    PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
    PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS,
    PTG2_V3_SHARED_BLOCK_LAYOUT,
    PTG2_V3_SHARED_GENERATION,
    SharedBlockReference,
    SharedLayoutBuildOwnership,
    SharedMappingDigestSummary,
    seal_shared_layout,
    shared_support_digest,
    summarize_shared_snapshot_mappings,
    touch_shared_layout_build,
)
from process.ptg_parts.ptg2_shared_audit import (
    publish_shared_audit_sample,
    sealed_audit_sample_metadata,
)
from process.ptg_parts.ptg2_shared_finalize import (
    PTG2_V3_DURABLE_SCRATCH_DURABILITY,
    PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY,
    run_v3_direct_finalizer,
)
from process.ptg_parts.ptg2_shared_graph import SharedGraphConversionResult
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.rust_scanner import (
    convert_membership_shards_to_shared_graph_rust,
)
from process.ptg_parts.ptg2_shared_price import (
    PTG2_V3_PRICE_KEY_ORDER,
    PreparedSharedPriceArtifacts,
    PreparedSharedPriceKeyMap,
    _await_cleanup_task,
    cleanup_prepared_shared_price_artifacts,
    export_shared_price_key_map,
    prepare_shared_price_artifacts,
    publish_shared_price_artifacts,
)
from process.ptg_parts.ptg2_shared_publish import (
    SharedBlockCopyMetrics,
    _validated_coverage_scope_id,
    copy_shared_block_binary_file,
    create_shared_block_stage,
    _copy_binary_file_to_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    publish_shared_graph,
    publish_v4_cas_block_stage,
    shared_block_stage_name,
    shared_graph_bundles_from_artifacts,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    deterministic_source_key_assignments,
    normalized_full_rebuild_scope_digest,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_source_witness_store import publish_shared_source_witness
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphCompilationResult,
    compile_provider_graph_v4_rust,
)
from process.ptg_parts.ptg2_v4_audit import publish_v4_audit_sample
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
    PTG2_V4_NPI_TABLE,
    PTG2_V4_SHARED_GENERATION,
    V4SnapshotMapSummary,
    lock_v4_shared_layout_for_map_write,
    publish_v4_heavy_owners,
    publish_v4_relation_manifests,
    publish_v4_snapshot_maps,
    seal_v4_shared_layout,
    touch_v4_shared_layout_build,
)


_REQUIRED_OBJECT_KINDS = frozenset(
    {
        "by_code_provider_shard_v1",
        "by_code_price_page_v4",
        "by_code_price_dictionary",
        "provider_set_count_dictionary",
        "provider_set_codes_v3",
        "provider_set_page_v3_s2",
        "price_set_atom_memberships_v3",
        "price_atoms_v3",
        "graph_npi_groups_v1",
        "graph_group_npis_v1",
        "graph_group_provider_sets_v1",
        "graph_provider_set_groups_v1",
    }
)
_REQUIRED_PRICE_OBJECT_KINDS = _REQUIRED_OBJECT_KINDS - {
    "graph_npi_groups_v1",
    "graph_group_npis_v1",
    "graph_group_provider_sets_v1",
    "graph_provider_set_groups_v1",
}


@dataclass(frozen=True)
class SharedSnapshotPublication:
    snapshot_key: int
    serving_index: Mapping[str, Any]
    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    mapping_digest: bytes
    finalizer_summary: Mapping[str, Any]
    layout_reused_at_seal: bool
    stored_byte_count: int


@dataclass(frozen=True)
class _PreparedFinalizer:
    """Finalizer output produced while independent price stages are still running."""

    summary: Mapping[str, Any]
    price_key_map_export_seconds: float
    finalizer_seconds: float
    overlap_wall_seconds: float


@dataclass(frozen=True)
class _PreparedPricePublication:
    """Price blocks published while the Rust finalizer is still running."""

    publication: Any
    publish_seconds: float


@dataclass(frozen=True)
class _FinalizerBlockPublicationResult:
    """Keep durable publication and immutable COPY proof together."""

    publication: Any
    serving_copy: SharedBlockCopyMetrics
    price_dictionary_copy: SharedBlockCopyMetrics

    def copy_manifest(self) -> dict[str, Any]:
        """Return per-lane and total selective-staging proof."""

        total = SharedBlockCopyMetrics.combine(
            self.serving_copy,
            self.price_dictionary_copy,
        )
        return {
            "contract": "selective_shared_block_copy_v1",
            "serving": self.serving_copy.as_dict(),
            "price_dictionary": self.price_dictionary_copy.as_dict(),
            "total": total.as_dict(),
        }


@dataclass(frozen=True)
class _V4GraphPublication:
    """Published V4 graph CAS, packed maps, and relational dictionaries."""

    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    block_count: int
    owner_count: int
    provider_group_count: int
    npi_count: int
    support_digest: bytes
    logical_byte_count: int
    stored_byte_count: int
    map_summary: V4SnapshotMapSummary
    representation: str
    compiler_summary: Mapping[str, Any]
    audit_witness_path: Path


def _completed_prepared_price(
    prepare_task: asyncio.Task[tuple[PreparedSharedPriceArtifacts, float]],
) -> PreparedSharedPriceArtifacts | None:
    """Return a successful completed preparation without masking its caller's error."""

    if not prepare_task.done() or prepare_task.cancelled():
        return None
    try:
        return prepare_task.result()[0]
    except BaseException:
        return None


def _validate_authoritative_mapping_summary(
    summary: SharedMappingDigestSummary,
    *lane_publications: Any,
) -> None:
    """Cross-check bounded lane aggregates against the authoritative mapping set."""

    lane_kinds: list[str] = []
    expected_mapping_count = 0
    expected_unique_block_count = 0
    expected_logical_byte_count = 0
    for publication in lane_publications:
        publication_kinds = tuple(publication.object_kinds)
        if publication_kinds != tuple(sorted(set(publication_kinds))):
            raise RuntimeError("strict V3 publication lane returned invalid object kinds")
        duplicate_kinds = set(lane_kinds).intersection(publication_kinds)
        if duplicate_kinds:
            raise RuntimeError(
                "strict V3 publication lanes overlap object kinds: "
                f"{sorted(duplicate_kinds)}"
            )
        lane_kinds.extend(publication_kinds)
        expected_mapping_count += int(publication.mapping_count)
        expected_unique_block_count += int(publication.unique_block_count)
        expected_logical_byte_count += int(publication.logical_byte_count)

    expected_kinds = tuple(sorted(lane_kinds))
    expected_by_field = {
        "object_kinds": expected_kinds,
        "mapping_count": expected_mapping_count,
        "unique_block_count": expected_unique_block_count,
        "logical_byte_count": expected_logical_byte_count,
    }
    for field_name, expected_value in expected_by_field.items():
        observed_value = getattr(summary, field_name)
        if observed_value != expected_value:
            raise RuntimeError(
                "strict V3 authoritative mapping summary disagrees with publication "
                f"lanes for {field_name}: expected {expected_value!r}, "
                f"observed {observed_value!r}"
            )


async def _run_independent_publication_lanes(
    *,
    finalizer_blocks: Callable[[], Awaitable[Any]],
    provider_graph: Callable[[], Awaitable[Any]],
    price: Callable[[], Awaitable[Any]],
    source_witness: Callable[[], Awaitable[Any]],
) -> tuple[Any, Any, Any, Any]:
    """Run independent durable outputs concurrently and fail as one unit."""

    async with asyncio.TaskGroup() as task_group:
        finalizer_block_task = task_group.create_task(finalizer_blocks())
        provider_graph_task = task_group.create_task(provider_graph())
        price_task = task_group.create_task(price())
        source_witness_task = task_group.create_task(source_witness())
    return (
        finalizer_block_task.result(),
        provider_graph_task.result(),
        price_task.result(),
        source_witness_task.result(),
    )


async def _export_price_map_and_run_finalizer(
    *,
    prepared_price_key: PreparedSharedPriceKeyMap,
    raw_work_directory: str | Path,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
) -> _PreparedFinalizer:
    """Export and finalize as soon as the independent price-key map is ready."""

    overlap_started_at = time.monotonic()
    stage_started_at = time.monotonic()
    price_key_map_path = await export_shared_price_key_map(
        prepared_price_key,
        Path(raw_work_directory) / "price-key-map.copy",
    )
    price_key_map_export_seconds = time.monotonic() - stage_started_at
    stage_started_at = time.monotonic()
    finalizer_summary = await run_v3_direct_finalizer(
        work_directory=raw_work_directory,
        serving_run_entries=serving_run_entries,
        code_dictionary_entries=code_dictionary_entries,
        provider_set_metadata_entries=provider_set_metadata_entries,
        expected_source_identities=expected_source_identities,
        price_key_map_input=price_key_map_path,
        price_key_map_row_count=prepared_price_key.price_set_count,
        scratch_durability=PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY,
    )
    return _PreparedFinalizer(
        summary=dict(finalizer_summary),
        price_key_map_export_seconds=price_key_map_export_seconds,
        finalizer_seconds=time.monotonic() - stage_started_at,
        overlap_wall_seconds=time.monotonic() - overlap_started_at,
    )


async def _prepare_price_with_early_finalizer(
    *,
    schema_name: str,
    manifest_stage_table: str,
    price_set_summary_source_count: int | None,
    raw_work_directory: str | Path,
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    publish_prepared_price: Callable[
        [PreparedSharedPriceArtifacts], Awaitable[Any]
    ]
    | None = None,
) -> tuple[
    PreparedSharedPriceArtifacts,
    float,
    _PreparedFinalizer | None,
    _PreparedPricePublication | None,
]:
    """Prepare price stages and overlap the finalizer when early readiness exists."""

    loop = asyncio.get_running_loop()
    price_key_ready: asyncio.Future[PreparedSharedPriceKeyMap] = loop.create_future()

    def notify_price_key_ready(prepared_key: PreparedSharedPriceKeyMap) -> None:
        """Announce the single validated price-key map to the finalizer lane."""

        if price_key_ready.done():
            raise RuntimeError("strict V3 price-key stage reported readiness twice")
        price_key_ready.set_result(prepared_key)

    price_prepare_started_at = time.monotonic()

    async def prepare_price() -> tuple[PreparedSharedPriceArtifacts, float]:
        """Prepare price artifacts and retain their measured wall time."""

        prepared = await prepare_shared_price_artifacts(
            schema_name=schema_name,
            manifest_stage_table=manifest_stage_table,
            price_set_summary_source_count=price_set_summary_source_count,
            price_key_ready=notify_price_key_ready,
        )
        return prepared, time.monotonic() - price_prepare_started_at

    prepare_task = asyncio.create_task(prepare_price())
    try:
        completed, _pending = await asyncio.wait(
            (prepare_task, price_key_ready),
            return_when=asyncio.FIRST_COMPLETED,
        )
    except BaseException:
        prepare_task.cancel()
        await _await_cleanup_task(
            asyncio.gather(prepare_task, return_exceptions=True)
        )
        prepared_after_cancellation = _completed_prepared_price(prepare_task)
        if prepared_after_cancellation is not None:
            await _await_cleanup_task(
                asyncio.create_task(
                    cleanup_prepared_shared_price_artifacts(
                        prepared_after_cancellation
                    )
                )
            )
        if not price_key_ready.done():
            price_key_ready.cancel()
        raise
    if price_key_ready not in completed:
        prepared, price_prepare_seconds = await prepare_task
        prepared_price_publication = None
        if publish_prepared_price is not None:
            price_publish_started_at = time.monotonic()
            prepared_price_publication = _PreparedPricePublication(
                publication=await publish_prepared_price(prepared),
                publish_seconds=time.monotonic() - price_publish_started_at,
            )
        return prepared, price_prepare_seconds, None, prepared_price_publication

    finalizer_task = asyncio.create_task(
        _export_price_map_and_run_finalizer(
            prepared_price_key=price_key_ready.result(),
            raw_work_directory=raw_work_directory,
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_source_identities=expected_source_identities,
        )
    )
    early_price_task: asyncio.Task[_PreparedPricePublication] | None = None
    if publish_prepared_price is not None:

        async def publish_after_price_preparation() -> _PreparedPricePublication:
            """Publish prepared price blocks while the finalizer remains active."""

            prepared, _price_prepare_seconds = await prepare_task
            price_publish_started_at = time.monotonic()
            return _PreparedPricePublication(
                publication=await publish_prepared_price(prepared),
                publish_seconds=time.monotonic() - price_publish_started_at,
            )

        early_price_task = asyncio.create_task(publish_after_price_preparation())
    try:
        gathered = await asyncio.gather(
            prepare_task,
            finalizer_task,
            *([early_price_task] if early_price_task is not None else []),
        )
    except BaseException:
        active_tasks = tuple(
            task
            for task in (prepare_task, finalizer_task, early_price_task)
            if task is not None
        )
        for task in active_tasks:
            task.cancel()
        await _await_cleanup_task(
            asyncio.gather(*active_tasks, return_exceptions=True)
        )
        prepared_after_failure = _completed_prepared_price(prepare_task)
        if prepared_after_failure is not None:
            await _await_cleanup_task(
                asyncio.create_task(
                    cleanup_prepared_shared_price_artifacts(prepared_after_failure)
                )
            )
        raise
    prepare_result = gathered[0]
    prepared_finalizer = gathered[1]
    prepared_price_publication = gathered[2] if early_price_task is not None else None
    prepared, price_prepare_seconds = prepare_result
    return (
        prepared,
        price_prepare_seconds,
        prepared_finalizer,
        prepared_price_publication,
    )


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row or {})


def _snapshot_source_rows(
    *,
    snapshot_id: str,
    assignments: Iterable[SharedSnapshotSourceAssignment],
) -> list[dict[str, Any]]:
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id or len(normalized_snapshot_id) > 96:
        raise ValueError("strict V3 source publication requires a valid snapshot_id")
    source_records = [
        {
            "snapshot_id": normalized_snapshot_id,
            "source_key": int(assignment.source_key),
            **assignment.identity.as_dict(),
            "raw_container_sha256": assignment.raw_container_sha256,
            "logical_json_sha256": assignment.logical_json_sha256,
            "logical_hash_deferred": assignment.logical_hash_deferred,
            "source_trace_set_hash": str(assignment.source_trace_set_hash),
        }
        for assignment in assignments
    ]
    if not source_records or [
        source_record["source_key"] for source_record in source_records
    ] != list(range(len(source_records))):
        raise ValueError("strict V3 snapshot source keys must be complete and dense")
    expected_dense = deterministic_source_key_assignments(
        {
            field_name: source_record[field_name]
            for field_name in ("source_type", "identity_kind", "identity_sha256")
        }
        for source_record in source_records
    )
    if any(
        source_key != source_record["source_key"] or identity.as_dict() != {
            field_name: source_record[field_name]
            for field_name in ("source_type", "identity_kind", "identity_sha256")
        }
        for source_record, (source_key, identity) in zip(
            source_records, expected_dense
        )
    ):
        raise ValueError(
            "strict V3 snapshot source keys do not match physical artifact ordinals"
        )
    return source_records


def _logical_snapshot_source_set_update_sql(schema: str) -> str:
    """Build the guarded logical source-set update without touching layouts."""

    return f"""
        WITH expected_source_set AS (
            SELECT jsonb_build_object(
                'contract', CAST(:source_set_contract AS text),
                'source_count', CAST(:source_set_count AS integer),
                'raw_container_sha256_digest', CAST(:source_set_digest AS text)
            ) AS value
        )
        UPDATE {schema}.ptg2_snapshot AS snapshot
           SET manifest = CAST(
               jsonb_set(
                   COALESCE(snapshot.manifest::jsonb, '{{}}'::jsonb),
                   '{{serving_index}}',
                   COALESCE(
                       snapshot.manifest::jsonb->'serving_index',
                       '{{}}'::jsonb
                   ) || jsonb_build_object(
                       'source_set', expected_source_set.value
                   ),
                   true
               ) AS json
           )
          FROM expected_source_set
         WHERE snapshot.snapshot_id = :snapshot_id
           AND snapshot.status = 'building'
           AND jsonb_typeof(COALESCE(
               snapshot.manifest::jsonb->'serving_index',
               '{{}}'::jsonb
           )) = 'object'
           AND (
               snapshot.manifest::jsonb #> '{{serving_index,source_set}}' IS NULL
               OR snapshot.manifest::jsonb #> '{{serving_index,source_set}}'
                   = expected_source_set.value
           )
        RETURNING snapshot.manifest::jsonb
            #> '{{serving_index,source_set}}' AS snapshot_source_set
    """


async def _persist_logical_snapshot_source_set(
    session: Any,
    *,
    schema: str,
    snapshot_id: str,
    source_set_by_field: Mapping[str, Any],
) -> None:
    """Seal one logical source set without changing reusable physical metadata."""

    update_result = await session.execute(
        db.text(_logical_snapshot_source_set_update_sql(schema)),
        {
            "snapshot_id": str(snapshot_id),
            "source_set_contract": source_set_by_field["contract"],
            "source_set_count": source_set_by_field["source_count"],
            "source_set_digest": source_set_by_field[
                "raw_container_sha256_digest"
            ],
        },
    )
    updated_row = update_result.first()
    persisted_source_set = (
        _row_mapping(updated_row).get("snapshot_source_set")
        if updated_row is not None
        else None
    )
    if persisted_source_set != dict(source_set_by_field):
        raise RuntimeError(
            f"PTG snapshot {snapshot_id} has a conflicting logical source-set seal"
        )


async def publish_shared_v3_snapshot_sources(
    *,
    schema_name: str,
    snapshot_id: str,
    plan_scopes: Iterable[SharedLogicalPlanScope],
    coverage_scope_id: bytes,
    assignments: Iterable[SharedSnapshotSourceAssignment],
) -> tuple[dict[str, Any], ...]:
    """Publish immutable source and logical-plan mappings for one snapshot."""

    source_records = _snapshot_source_rows(
        snapshot_id=snapshot_id,
        assignments=assignments,
    )
    source_set_by_field = shared_source_set_metadata(
        source_record["raw_container_sha256"] for source_record in source_records
    )
    scope_id = _validated_coverage_scope_id(coverage_scope_id)
    normalized_plan_scopes = tuple(
        sorted(
            {
                SharedLogicalPlanScope(
                    plan_id=str(scope.plan_id or "").strip(),
                    plan_id_type=str(scope.plan_id_type or "").strip().lower(),
                    plan_market_type=str(
                        scope.plan_market_type or ""
                    ).strip().lower(),
                )
                for scope in plan_scopes
                if str(scope.plan_id or "").strip()
            }
        )
    )
    if not normalized_plan_scopes:
        raise ValueError("strict V3 source publication requires logical plans")
    primary_plan = normalized_plan_scopes[0]
    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_scope
                    (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
                VALUES
                    (:snapshot_id, :plan_id, :plan_market_type, :coverage_scope_id)
                ON CONFLICT (snapshot_id) DO NOTHING
                """
            ),
            {
                "snapshot_id": str(snapshot_id),
                "plan_id": primary_plan.plan_id,
                "plan_market_type": primary_plan.plan_market_type,
                "coverage_scope_id": scope_id,
            },
        )
        scope_result = await session.execute(
            db.text(
                f"""
                SELECT plan_id, plan_market_type, coverage_scope_id
                  FROM {schema}.ptg2_v3_snapshot_scope
                 WHERE snapshot_id = :snapshot_id
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        scope_row = scope_result.first()
        scope = _row_mapping(scope_row)
        if (
            str(scope.get("plan_id") or "") != primary_plan.plan_id
            or str(scope.get("plan_market_type") or "")
            != primary_plan.plan_market_type
            or bytes(scope.get("coverage_scope_id") or b"") != scope_id
        ):
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} already has another immutable source scope"
            )
        plan_scope_records = [
            {
                "snapshot_id": str(snapshot_id),
                "plan_id": logical_plan.plan_id,
                "plan_market_type": logical_plan.plan_market_type,
            }
            for logical_plan in normalized_plan_scopes
        ]
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope
                    (snapshot_id, plan_id, plan_market_type)
                VALUES
                    (:snapshot_id, :plan_id, :plan_market_type)
                ON CONFLICT (snapshot_id, plan_id, plan_market_type) DO NOTHING
                """
            ),
            plan_scope_records,
        )
        observed_plan_result = await session.execute(
            db.text(
                f"""
                SELECT plan_id, plan_market_type
                  FROM {schema}.ptg2_v3_snapshot_plan_scope
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY plan_id, plan_market_type
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        observed_plans = {
            (
                str(_row_mapping(plan_scope_row).get("plan_id") or ""),
                str(_row_mapping(plan_scope_row).get("plan_market_type") or ""),
            )
            for plan_scope_row in observed_plan_result
        }
        expected_plans = {
            (logical_plan.plan_id, logical_plan.plan_market_type)
            for logical_plan in normalized_plan_scopes
        }
        if observed_plans != expected_plans:
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} has stale logical plan mappings"
            )
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_snapshot_source
                    (snapshot_id, source_key, source_type, identity_kind,
                     identity_sha256, raw_container_sha256, logical_json_sha256,
                     logical_hash_deferred, source_trace_set_hash)
                VALUES
                    (:snapshot_id, :source_key, :source_type, :identity_kind,
                     :identity_sha256, :raw_container_sha256, :logical_json_sha256,
                     :logical_hash_deferred, :source_trace_set_hash)
                ON CONFLICT (snapshot_id, source_key) DO NOTHING
                """
            ),
            source_records,
        )
        observed_result = await session.execute(
            db.text(
                f"""
                SELECT snapshot_id, source_key, source_type, identity_kind,
                       identity_sha256, raw_container_sha256, logical_json_sha256,
                       logical_hash_deferred, source_trace_set_hash
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY source_key
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        observed_source_records = [
            _row_mapping(source_record) for source_record in observed_result
        ]
        if observed_source_records != source_records:
            raise RuntimeError(
                f"PTG snapshot {snapshot_id} already has a conflicting source-key mapping"
            )
        await _persist_logical_snapshot_source_set(
            session,
            schema=schema,
            snapshot_id=str(snapshot_id),
            source_set_by_field=source_set_by_field,
        )
    return tuple(source_records)


async def delete_unpublished_snapshot_sources(
    *,
    schema_name: str,
    snapshot_id: str,
) -> None:
    """Remove failed logical metadata without releasing or changing shared layouts."""

    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        await acquire_ptg2_lifecycle_lock(session)
        snapshot_result = await session.execute(
            db.text(
                f"""
                SELECT snapshot.status,
                       EXISTS (
                           SELECT 1
                             FROM {schema}.ptg2_v3_snapshot_binding AS binding
                            WHERE binding.snapshot_id = snapshot.snapshot_id
                       ) AS is_bound
                  FROM {schema}.ptg2_snapshot AS snapshot
                 WHERE snapshot.snapshot_id = :snapshot_id
                 FOR UPDATE
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        snapshot_row = snapshot_result.first()
        if not _can_delete_unpublished_sources(snapshot_row):
            return
        await session.execute(
            db.text(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_source AS source
                 WHERE source.snapshot_id = :snapshot_id
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_v3_snapshot_binding AS binding
                         WHERE binding.snapshot_id = source.snapshot_id
                   )
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )
        await session.execute(
            db.text(
                f"""
                DELETE FROM {schema}.ptg2_v3_snapshot_scope AS scope
                 WHERE scope.snapshot_id = :snapshot_id
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_v3_snapshot_binding AS binding
                         WHERE binding.snapshot_id = scope.snapshot_id
                   )
                """
            ),
            {"snapshot_id": str(snapshot_id)},
        )


def _can_delete_unpublished_sources(snapshot_row: Any) -> bool:
    """Allow cleanup only for unbound logical snapshots that never published."""

    snapshot_state = _row_mapping(snapshot_row) if snapshot_row is not None else {}
    return not snapshot_state.get("is_bound") and str(
        snapshot_state.get("status") or ""
    ) in {"building", "failed"}


async def validate_reused_snapshot_sources(
    *,
    schema_name: str,
    snapshot_key: int,
    logical_snapshot_id: str,
    expected_generation: str = PTG2_V3_SHARED_GENERATION,
) -> dict[str, Any]:
    """Validate reused physical audit source keys against this logical dictionary."""

    async with db.transaction() as session:
        return await sealed_audit_sample_metadata(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            logical_snapshot_id=str(logical_snapshot_id),
            expected_generation=expected_generation,
        )


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"strict V3 finalizer summary is missing {name}")
    return dict(value)


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"strict V3 finalizer summary has invalid {name}") from exc
    if normalized < 0:
        raise RuntimeError(f"strict V3 finalizer summary has negative {name}")
    return normalized


def _output_file(summary: Mapping[str, Any], section: Mapping[str, Any]) -> Path:
    root = Path(str(summary.get("output_directory") or "")).resolve()
    path = (root / str(section.get("path") or "")).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(
            "strict V3 finalizer block path escapes its output directory"
        ) from exc
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError(
            f"strict V3 finalizer block output is missing or empty: {path}"
        )
    return path


async def _export_provider_set_key_map(
    *,
    schema_name: str,
    snapshot_key: int,
    output_path: Path,
) -> Path:
    """Stream the authoritative dense map without materializing database rows."""

    schema = _quote_ident(schema_name)
    query = f"""
        SELECT encode(provider_set_global_id_128, 'hex'), provider_set_key
          FROM {schema}.ptg2_v3_provider_set
         WHERE snapshot_key = {int(snapshot_key)}
         ORDER BY provider_set_global_id_128
    """
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_from_query = getattr(driver_conn, "copy_from_query", None)
        if copy_from_query is None:
            raise NotImplementedError("active database driver does not expose COPY TO")
        with output_path.open("wb") as output:
            await copy_from_query(
                query,
                output=output,
                format="text",
                delimiter="\t",
                null="\\N",
            )
    return output_path


async def _convert_shared_graph_natively(
    *,
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_set_key_map_path: Path,
    work_directory: Path,
) -> SharedGraphConversionResult:
    graph_bundles = shared_graph_bundles_from_artifacts(graph_artifact_entries)
    return await convert_membership_shards_to_shared_graph_rust(
        shards=graph_bundles,
        provider_set_key_map_path=Path(provider_set_key_map_path),
        output_directory=Path(work_directory) / "provider-graph-native",
    )


def _v4_compiler_artifact(
    compilation: V4GraphCompilationResult,
    name: str,
) -> Any:
    matches = tuple(
        artifact for artifact in compilation.output_artifacts if artifact.name == name
    )
    if len(matches) != 1:
        raise RuntimeError(f"PTG V4 compiler output is missing {name!r}")
    return matches[0]


def _iter_v4_block_references(path: Path) -> Iterable[SharedBlockReference]:
    """Re-read the authenticated compiler coordinates as a bounded stream."""

    previous_coordinate: tuple[str, int, int] | None = None
    with path.open("rb") as reference_file:
        for line_number, line in enumerate(reference_file, 1):
            if not line or len(line) > 64 * 1024:
                raise RuntimeError("PTG V4 graph reference record is not bounded")
            try:
                raw = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"PTG V4 graph reference {line_number} is invalid JSON"
                ) from exc
            if not isinstance(raw, dict) or raw.get("codec") != "none":
                raise RuntimeError("PTG V4 graph reference has an invalid codec")
            try:
                object_kind = str(raw["object_kind"])
                block_key = int(raw["block_key"])
                fragment_no = int(raw["fragment_no"])
                entry_count = int(raw["entry_count"])
                raw_byte_count = int(raw["raw_byte_count"])
                stored_byte_count = int(raw["stored_byte_count"])
                block_hash = bytes.fromhex(str(raw["hash"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError("PTG V4 graph reference fields are invalid") from exc
            coordinate = (object_kind, block_key, fragment_no)
            if (
                not object_kind.startswith("v4_")
                or min(block_key, fragment_no, entry_count, raw_byte_count) < 0
                or stored_byte_count != raw_byte_count
                or len(block_hash) != 32
                or (previous_coordinate is not None and coordinate <= previous_coordinate)
            ):
                raise RuntimeError("PTG V4 graph reference ordering or metadata changed")
            previous_coordinate = coordinate
            yield SharedBlockReference(
                object_kind=object_kind,
                block_key=block_key,
                fragment_no=fragment_no,
                entry_count=entry_count,
                block_hash=block_hash,
                raw_byte_count=raw_byte_count,
            )


async def _queue_failed_v4_graph_blocks(
    *,
    schema_name: str,
    reference_manifest_path: Path,
) -> None:
    """Queue orphanable compiler CAS hashes; the normal sweep rechecks reachability."""

    schema = _quote_ident(schema_name)
    hashes: list[bytes] = []

    async def flush() -> None:
        """Persist the currently buffered unreachable block hashes."""

        if not hashes:
            return
        async with db.transaction() as session:
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
                        (block_hash, eligible_at, queued_at)
                    SELECT DISTINCT block_hash, transaction_timestamp(),
                           transaction_timestamp()
                      FROM unnest(CAST(:block_hashes AS bytea[]))
                           AS requested(block_hash)
                    ON CONFLICT (block_hash) DO UPDATE
                        SET eligible_at = GREATEST(
                            candidate.eligible_at,
                            EXCLUDED.eligible_at
                        )
                    """
                ),
                {"block_hashes": list(hashes)},
            )
        hashes.clear()

    for reference in _iter_v4_block_references(reference_manifest_path):
        hashes.append(bytes(reference.block_hash))
        if len(hashes) >= 8_192:
            await flush()
    await flush()


async def _publish_v4_dictionaries_and_maps(
    compilation: V4GraphCompilationResult,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    compressed_acquisition_bytes: int,
    empty_npi_tin_only_normalization_count: int,
) -> V4SnapshotMapSummary:
    """Bulk-copy dense dictionaries, then publish exact metadata and packed maps."""

    if int(compressed_acquisition_bytes) <= 0:
        raise RuntimeError(
            "PTG V4 compressed acquisition bytes must be positive"
        )
    if int(empty_npi_tin_only_normalization_count) < 0:
        raise RuntimeError(
            "PTG V4 empty-NPI TIN-only normalization count cannot be negative"
        )
    schema = _quote_ident(schema_name)
    token = uuid.uuid4().hex[:20]
    group_stage = f"ptg2_v4_group_stage_{token}"
    component_stage = f"ptg2_v4_component_stage_{token}"
    npi_stage = f"ptg2_v4_npi_stage_{token}"
    pattern_stage = f"ptg2_v4_pattern_stage_{token}"
    prefix_stage = f"ptg2_v4_npi_prefix_stage_{token}"
    stages = [group_stage, component_stage, npi_stage, prefix_stage]
    if compilation.pattern_copy_path is not None:
        stages.append(pattern_stage)
    stage_create_statements = [
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(group_stage)} "
        "(provider_group_key integer NOT NULL, "
        " provider_group_global_id_128 bytea NOT NULL)",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(component_stage)} "
        "(component_key integer NOT NULL, component_global_id_128 bytea NOT NULL)",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(npi_stage)} "
        "(npi_key integer NOT NULL, npi bigint NOT NULL)",
        f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(prefix_stage)} "
        "(provider_set_key integer NOT NULL, member_count integer NOT NULL, "
        " member_digest bytea NOT NULL)",
    ]
    if compilation.pattern_copy_path is not None:
        stage_create_statements.append(
            f"CREATE UNLOGGED TABLE {schema}.{_quote_ident(pattern_stage)} "
            "(pattern_key integer NOT NULL, pattern_digest bytea NOT NULL, "
            " set_count bigint NOT NULL)"
        )
    for statement in stage_create_statements:
        await db.status(statement)
    try:
        await _copy_binary_file_to_stage(
            compilation.group_copy_path,
            schema_name=schema_name,
            stage_table=group_stage,
            columns=("provider_group_key", "provider_group_global_id_128"),
        )
        await _copy_binary_file_to_stage(
            compilation.component_copy_path,
            schema_name=schema_name,
            stage_table=component_stage,
            columns=("component_key", "component_global_id_128"),
        )
        await _copy_binary_file_to_stage(
            compilation.npi_copy_path,
            schema_name=schema_name,
            stage_table=npi_stage,
            columns=("npi_key", "npi"),
        )
        await _copy_binary_file_to_stage(
            compilation.provider_set_npi_prefix_override_copy_path,
            schema_name=schema_name,
            stage_table=prefix_stage,
            columns=("provider_set_key", "member_count", "member_digest"),
        )
        if compilation.pattern_copy_path is not None:
            await _copy_binary_file_to_stage(
                compilation.pattern_copy_path,
                schema_name=schema_name,
                stage_table=pattern_stage,
                columns=("pattern_key", "pattern_digest", "set_count"),
            )

        expected_group_count = int(compilation.observe.get("group_count") or 0)
        expected_component_count = int(
            compilation.observe.get("component_count") or 0
        )
        expected_npi_count = int(compilation.observe.get("npi_count") or 0)
        expected_pattern_count = int(
            compilation.observe.get("pattern_count") or 0
        )
        expected_prefix_owner_count = int(
            compilation.observe.get("npi_prefix_override_owner_count") or 0
        )
        expected_prefix_member_count = int(
            compilation.observe.get("npi_prefix_override_member_count") or 0
        )
        prefix_target = int(compilation.summary["npi_prefix_target"])
        published_pattern_count = (
            expected_pattern_count
            if compilation.pattern_copy_path is not None
            else 0
        )
        async with db.transaction() as session:
            await lock_v4_shared_layout_for_map_write(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
            )
            stage_result = await session.execute(
                db.text(
                    f"""
                    SELECT
                      (SELECT COUNT(*) FROM {schema}.{_quote_ident(group_stage)}),
                      (SELECT COUNT(DISTINCT provider_group_key)
                         FROM {schema}.{_quote_ident(group_stage)}),
                      (SELECT COUNT(DISTINCT provider_group_global_id_128)
                         FROM {schema}.{_quote_ident(group_stage)}),
                      (SELECT COALESCE(BOOL_AND(
                                  octet_length(provider_group_global_id_128) = 16
                              ), TRUE)
                         FROM {schema}.{_quote_ident(group_stage)}),
                      (SELECT COUNT(*) FROM {schema}.{_quote_ident(component_stage)}),
                      (SELECT COUNT(DISTINCT component_key)
                         FROM {schema}.{_quote_ident(component_stage)}),
                      (SELECT COALESCE(BOOL_AND(
                                  octet_length(component_global_id_128) = 16
                              ), TRUE)
                         FROM {schema}.{_quote_ident(component_stage)}),
                      (SELECT COUNT(*) FROM {schema}.{_quote_ident(npi_stage)}),
                      (SELECT COUNT(DISTINCT npi_key)
                         FROM {schema}.{_quote_ident(npi_stage)}),
                      (SELECT COUNT(DISTINCT npi)
                         FROM {schema}.{_quote_ident(npi_stage)}),
                      (SELECT COALESCE(BOOL_AND(
                                  npi BETWEEN 1000000000 AND 9999999999
                              ), TRUE)
                         FROM {schema}.{_quote_ident(npi_stage)}),
                      (SELECT COUNT(*)
                         FROM {schema}.{_quote_ident(prefix_stage)}),
                      (SELECT COUNT(DISTINCT provider_set_key)
                         FROM {schema}.{_quote_ident(prefix_stage)}),
                      (SELECT COALESCE(SUM(member_count), 0)
                         FROM {schema}.{_quote_ident(prefix_stage)}),
                      (SELECT COALESCE(BOOL_AND(
                                  member_count BETWEEN 0 AND :prefix_target
                                  AND octet_length(member_digest) = 32
                              ), TRUE)
                         FROM {schema}.{_quote_ident(prefix_stage)})
                    """
                ),
                {"prefix_target": prefix_target},
            )
            counts = stage_result.one()
            if (
                tuple(int(counts[index]) for index in (0, 1, 2))
                != (expected_group_count,) * 3
                or not bool(counts[3])
                or tuple(int(counts[index]) for index in (4, 5))
                != (expected_component_count,) * 2
                or not bool(counts[6])
                or tuple(int(counts[index]) for index in (7, 8, 9))
                != (expected_npi_count,) * 3
                or not bool(counts[10])
                or tuple(int(counts[index]) for index in (11, 12))
                != (expected_prefix_owner_count,) * 2
                or int(counts[13]) != expected_prefix_member_count
                or not bool(counts[14])
            ):
                raise RuntimeError("PTG V4 dictionary COPY changed or duplicated keys")
            for stage_name, key_name, expected_count in (
                (group_stage, "provider_group_key", expected_group_count),
                (component_stage, "component_key", expected_component_count),
                (npi_stage, "npi_key", expected_npi_count),
            ):
                dense_result = await session.execute(
                    db.text(
                        f"SELECT MIN({key_name}), MAX({key_name}) "
                        f"FROM {schema}.{_quote_ident(stage_name)}"
                    )
                )
                first_key, last_key = dense_result.one()
                if (
                    first_key != (0 if expected_count else None)
                    or last_key != (expected_count - 1 if expected_count else None)
                ):
                    raise RuntimeError("PTG V4 dictionary keys are not dense from zero")

            if compilation.pattern_copy_path is not None:
                pattern_result = await session.execute(
                    db.text(
                        f"""
                        SELECT COUNT(*), COUNT(DISTINCT pattern_key),
                               MIN(pattern_key), MAX(pattern_key),
                               COALESCE(BOOL_AND(
                                   octet_length(pattern_digest) = 32
                                   AND set_count >= 0
                               ), TRUE)
                          FROM {schema}.{_quote_ident(pattern_stage)}
                        """
                    )
                )
                pattern = pattern_result.one()
                if (
                    int(pattern[0]) != expected_pattern_count
                    or int(pattern[1]) != expected_pattern_count
                    or pattern[2] != (0 if expected_pattern_count else None)
                    or pattern[3]
                    != (expected_pattern_count - 1 if expected_pattern_count else None)
                    or not bool(pattern[4])
                ):
                    raise RuntimeError("PTG V4 pattern dictionary is invalid")

            # Metadata tables are trigger-fenced by the building map root.
            # Publish the authenticated coordinate map first in this same
            # transaction, then attach dictionaries/manifests beneath it.
            map_summary = await publish_v4_snapshot_maps(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                representation=(
                    "pattern_v1"
                    if compilation.selected_layout == "pattern"
                    else "direct_v1"
                ),
                references=_iter_v4_block_references(
                    compilation.reference_manifest_path
                ),
            )

            snapshot_parameter_map = {"snapshot_key": int(snapshot_key)}
            statements = [
                f"""
                INSERT INTO {schema}.ptg2_v3_provider_group
                    (snapshot_key, provider_group_key,
                     provider_group_global_id_128)
                SELECT :snapshot_key, provider_group_key,
                       provider_group_global_id_128
                  FROM {schema}.{_quote_ident(group_stage)}
                 ORDER BY provider_group_key
                ON CONFLICT DO NOTHING
                """,
                f"""
                INSERT INTO {schema}.ptg2_v4_provider_component
                    (snapshot_key, component_key, component_global_id_128)
                SELECT :snapshot_key, component_key, component_global_id_128
                  FROM {schema}.{_quote_ident(component_stage)}
                 ORDER BY component_key
                ON CONFLICT DO NOTHING
                """,
                f"""
                INSERT INTO {schema}.{PTG2_V4_NPI_TABLE}
                    (snapshot_key, npi_key, npi)
                SELECT :snapshot_key, npi_key, npi
                  FROM {schema}.{_quote_ident(npi_stage)}
                 ORDER BY npi_key
                ON CONFLICT DO NOTHING
                """,
                f"""
                INSERT INTO {schema}.ptg2_v4_provider_set_npi_prefix
                    (snapshot_key, provider_set_key, member_count, member_digest)
                SELECT :snapshot_key, provider_set_key, member_count, member_digest
                  FROM {schema}.{_quote_ident(prefix_stage)}
                 ORDER BY provider_set_key
                ON CONFLICT DO NOTHING
                """,
            ]
            if compilation.pattern_copy_path is not None:
                statements.append(
                    f"""
                    INSERT INTO {schema}.ptg2_v4_pattern
                        (snapshot_key, pattern_key, pattern_digest, set_count)
                    SELECT :snapshot_key, pattern_key, pattern_digest, set_count
                      FROM {schema}.{_quote_ident(pattern_stage)}
                     ORDER BY pattern_key
                    ON CONFLICT DO NOTHING
                    """
                )
            for statement in statements:
                await session.execute(db.text(statement), snapshot_parameter_map)

            diagnostic_parameters_by_name = {
                "snapshot_key": int(snapshot_key),
                "compressed_acquisition_bytes": int(
                    compressed_acquisition_bytes
                ),
                "input_factor_bytes": int(
                    compilation.resource_admission["input_factor_bytes"]
                ),
                "factor_edge_count": int(
                    compilation.resource_admission["factor_edge_count"]
                ),
                "empty_npi_tin_only_normalization_count": int(
                    empty_npi_tin_only_normalization_count
                ),
                "npi_prefix_target": prefix_target,
                "max_set_patterns_per_set": int(
                    compilation.summary["max_set_patterns_per_set"]
                ),
                "max_set_components_per_fallback_set": int(
                    compilation.summary[
                        "max_set_components_per_fallback_set"
                    ]
                ),
                "max_online_group_keys_per_set": int(
                    compilation.summary["max_online_group_keys_per_set"]
                ),
                "max_online_source_owners_per_set": int(
                    compilation.summary["max_online_source_owners_per_set"]
                ),
                "max_online_source_members_per_set": int(
                    compilation.summary["max_online_source_members_per_set"]
                ),
                "max_online_source_pages_per_set": int(
                    compilation.summary["max_online_source_pages_per_set"]
                ),
                "max_online_source_bytes_per_set": int(
                    compilation.summary["max_online_source_bytes_per_set"]
                ),
                "online_group_npi_batch_size": int(
                    compilation.summary["online_group_npi_batch_size"]
                ),
                "max_online_group_npi_members_per_set": int(
                    compilation.summary[
                        "max_online_group_npi_members_per_set"
                    ]
                ),
                "max_online_group_npi_locator_pages_per_set": int(
                    compilation.summary[
                        "max_online_group_npi_locator_pages_per_set"
                    ]
                ),
                "max_online_group_npi_member_pages_per_set": int(
                    compilation.summary[
                        "max_online_group_npi_member_pages_per_set"
                    ]
                ),
                "max_online_group_npi_bytes_per_set": int(
                    compilation.summary[
                        "max_online_group_npi_bytes_per_set"
                    ]
                ),
                "max_online_group_npi_batches_per_set": int(
                    compilation.summary[
                        "max_online_group_npi_batches_per_set"
                    ]
                ),
                "provider_expansion_rate_page_rows": int(
                    compilation.summary["provider_expansion_rate_page_rows"]
                ),
                "max_online_provider_expansion_rate_rows": int(
                    compilation.summary[
                        "max_online_provider_expansion_rate_rows"
                    ]
                ),
                "max_online_provider_expansion_provider_sets": int(
                    compilation.summary[
                        "max_online_provider_expansion_provider_sets"
                    ]
                ),
                "max_online_provider_expansion_graph_batches": int(
                    compilation.summary[
                        "max_online_provider_expansion_graph_batches"
                    ]
                ),
                "maximum_group_npi_member_work": int(
                    compilation.observe[
                        "maximum_online_group_npi_member_work"
                    ]
                ),
                "maximum_group_npi_locator_page_work": int(
                    compilation.observe[
                        "maximum_online_group_npi_locator_page_work"
                    ]
                ),
                "maximum_group_npi_member_page_work": int(
                    compilation.observe[
                        "maximum_online_group_npi_member_page_work"
                    ]
                ),
                "maximum_group_npi_byte_work": int(
                    compilation.observe[
                        "maximum_online_group_npi_byte_work"
                    ]
                ),
                "maximum_group_npi_batch_work": int(
                    compilation.observe[
                        "maximum_online_group_npi_batch_work"
                    ]
                ),
                "group_unsafe_set_count": int(
                    compilation.observe["npi_prefix_group_unsafe_set_count"]
                ),
                "physical_unsafe_set_count": int(
                    compilation.observe["npi_prefix_physical_unsafe_set_count"]
                ),
                "simulated_set_count": int(
                    compilation.observe["npi_prefix_simulated_set_count"]
                ),
                "override_owner_count": expected_prefix_owner_count,
                "override_member_count": expected_prefix_member_count,
                "override_raw_bytes": int(
                    compilation.observe["npi_prefix_override_raw_bytes"]
                ),
                "worst_provider_set_key": compilation.observe[
                    "npi_prefix_worst_provider_set_key"
                ],
                "worst_groups_to_target": int(
                    compilation.observe["npi_prefix_worst_groups_to_target"]
                ),
                "worst_uses_override": bool(
                    compilation.observe[
                        "npi_prefix_worst_provider_set_uses_override"
                    ]
                ),
                "worst_uses_component_fallback": bool(
                    compilation.observe[
                        "npi_prefix_worst_uses_component_fallback"
                    ]
                ),
                "worst_member_count": int(
                    compilation.observe["npi_prefix_worst_member_count"]
                ),
                "worst_member_digest": (
                    bytes.fromhex(
                        str(
                            compilation.observe[
                                "npi_prefix_worst_member_digest"
                            ]
                        )
                    )
                    if compilation.observe.get("npi_prefix_worst_member_digest")
                    is not None
                    else None
                ),
                "worst_source_owner_work": int(
                    compilation.observe["npi_prefix_worst_source_owner_work"]
                ),
                "worst_source_member_work": int(
                    compilation.observe["npi_prefix_worst_source_member_work"]
                ),
                "worst_source_page_work": int(
                    compilation.observe["npi_prefix_worst_source_page_work"]
                ),
                "worst_source_byte_work": int(
                    compilation.observe["npi_prefix_worst_source_byte_work"]
                ),
                "worst_group_npi_member_work": int(
                    compilation.observe[
                        "npi_prefix_worst_group_npi_member_work"
                    ]
                ),
                "worst_group_npi_locator_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_group_npi_locator_page_work"
                    ]
                ),
                "worst_group_npi_member_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_group_npi_member_page_work"
                    ]
                ),
                "worst_group_npi_byte_work": int(
                    compilation.observe[
                        "npi_prefix_worst_group_npi_byte_work"
                    ]
                ),
                "worst_group_npi_batch_work": int(
                    compilation.observe[
                        "npi_prefix_worst_group_npi_batch_work"
                    ]
                ),
                "worst_online_provider_set_key": compilation.observe[
                    "npi_prefix_worst_online_provider_set_key"
                ],
                "worst_online_groups_to_target": int(
                    compilation.observe[
                        "npi_prefix_worst_online_groups_to_target"
                    ]
                ),
                "worst_online_groups_to_target_exact": bool(
                    compilation.observe[
                        "npi_prefix_worst_online_groups_to_target_exact"
                    ]
                ),
                "worst_online_uses_component_fallback": bool(
                    compilation.observe[
                        "npi_prefix_worst_online_uses_component_fallback"
                    ]
                ),
                "worst_online_group_work_bound": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_work_bound"
                    ]
                ),
                "worst_online_member_count": int(
                    compilation.observe[
                        "npi_prefix_worst_online_member_count"
                    ]
                ),
                "worst_online_member_digest": (
                    bytes.fromhex(
                        str(
                            compilation.observe[
                                "npi_prefix_worst_online_member_digest"
                            ]
                        )
                    )
                    if compilation.observe.get(
                        "npi_prefix_worst_online_member_digest"
                    )
                    is not None
                    else None
                ),
                "worst_online_source_owner_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_source_owner_work"
                    ]
                ),
                "worst_online_source_member_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_source_member_work"
                    ]
                ),
                "worst_online_source_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_source_page_work"
                    ]
                ),
                "worst_online_source_byte_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_source_byte_work"
                    ]
                ),
                "worst_online_group_npi_member_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_member_work"
                    ]
                ),
                "worst_online_group_npi_locator_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_locator_page_work"
                    ]
                ),
                "worst_online_group_npi_member_page_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_member_page_work"
                    ]
                ),
                "worst_online_group_npi_byte_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_byte_work"
                    ]
                ),
                "worst_online_group_npi_batch_work": int(
                    compilation.observe[
                        "npi_prefix_worst_online_group_npi_batch_work"
                    ]
                ),
            }
            diagnostic_columns = (
                *PTG2_V4_GRAPH_RESOURCE_FIELDS,
                *PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
            )
            diagnostic_column_sql = ", ".join(diagnostic_columns)
            diagnostic_value_sql = ", ".join(
                f":{column}" for column in diagnostic_columns
            )
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v4_provider_graph_diagnostic
                        (snapshot_key, {diagnostic_column_sql})
                    VALUES
                        (:snapshot_key, {diagnostic_value_sql})
                    ON CONFLICT DO NOTHING
                    """
                ),
                diagnostic_parameters_by_name,
            )
            diagnostic_result = await session.execute(
                db.text(
                    f"""
                    SELECT {diagnostic_column_sql}
                      FROM {schema}.ptg2_v4_provider_graph_diagnostic
                     WHERE snapshot_key = :snapshot_key
                    """
                ),
                snapshot_parameter_map,
            )
            if tuple(diagnostic_result.one()) != tuple(
                diagnostic_parameters_by_name[column]
                for column in diagnostic_columns
            ):
                raise RuntimeError("PTG V4 persisted graph diagnostics changed")

            persisted_result = await session.execute(
                db.text(
                    f"""
                    SELECT
                      (SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_group
                        WHERE snapshot_key = :snapshot_key),
                      (SELECT COUNT(*)
                         FROM {schema}.{_quote_ident(group_stage)} AS staged
                         JOIN {schema}.ptg2_v3_provider_group AS stored
                           ON stored.snapshot_key = :snapshot_key
                          AND stored.provider_group_key = staged.provider_group_key
                          AND stored.provider_group_global_id_128 =
                              staged.provider_group_global_id_128),
                      (SELECT COUNT(*) FROM {schema}.ptg2_v4_provider_component
                        WHERE snapshot_key = :snapshot_key),
                      (SELECT COUNT(*)
                         FROM {schema}.{_quote_ident(component_stage)} AS staged
                         JOIN {schema}.ptg2_v4_provider_component AS stored
                           ON stored.snapshot_key = :snapshot_key
                          AND stored.component_key = staged.component_key
                          AND stored.component_global_id_128 =
                              staged.component_global_id_128),
                      (SELECT COUNT(*) FROM {schema}.{PTG2_V4_NPI_TABLE}
                        WHERE snapshot_key = :snapshot_key),
                      (SELECT COUNT(*)
                         FROM {schema}.{_quote_ident(npi_stage)} AS staged
                         JOIN {schema}.{PTG2_V4_NPI_TABLE} AS stored
                           ON stored.snapshot_key = :snapshot_key
                          AND stored.npi_key = staged.npi_key
                          AND stored.npi = staged.npi),
                      (SELECT COUNT(*) FROM {schema}.ptg2_v4_pattern
                        WHERE snapshot_key = :snapshot_key),
                      (SELECT COUNT(*)
                         FROM {schema}.ptg2_v4_pattern AS stored
                         {(
                            f'JOIN {schema}.{_quote_ident(pattern_stage)} AS staged '
                            'ON stored.snapshot_key = :snapshot_key '
                            'AND stored.pattern_key = staged.pattern_key '
                            'AND stored.pattern_digest = staged.pattern_digest '
                            'AND stored.set_count = staged.set_count'
                         ) if compilation.pattern_copy_path is not None else 'WHERE FALSE'})
                      ,
                      (SELECT COUNT(*)
                         FROM {schema}.ptg2_v4_provider_set_npi_prefix
                        WHERE snapshot_key = :snapshot_key),
                      (SELECT COUNT(*)
                         FROM {schema}.{_quote_ident(prefix_stage)} AS staged
                         JOIN {schema}.ptg2_v4_provider_set_npi_prefix AS stored
                           ON stored.snapshot_key = :snapshot_key
                          AND stored.provider_set_key = staged.provider_set_key
                          AND stored.member_count = staged.member_count
                          AND stored.member_digest = staged.member_digest),
                      (SELECT COUNT(*)
                         FROM {schema}.ptg2_v4_provider_graph_diagnostic
                        WHERE snapshot_key = :snapshot_key)
                    """
                ),
                snapshot_parameter_map,
            )
            if tuple(int(persisted_count) for persisted_count in persisted_result.one()) != (
                expected_group_count,
                expected_group_count,
                expected_component_count,
                expected_component_count,
                expected_npi_count,
                expected_npi_count,
                published_pattern_count,
                published_pattern_count,
                expected_prefix_owner_count,
                expected_prefix_owner_count,
                1,
            ):
                raise RuntimeError("PTG V4 persisted dictionary counts changed")

            await publish_v4_relation_manifests(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                entries=tuple(
                    sorted(compilation.relation_summaries, key=lambda row: row["relation"])
                ),
            )
            await publish_v4_heavy_owners(
                session,
                schema_name=schema_name,
                snapshot_key=int(snapshot_key),
                build_token=build_token,
                entries=tuple(
                    {
                        **dict(bitmap_summary),
                        "fragment_count": int(bitmap_summary["block_count"]),
                    }
                    for bitmap_summary in sorted(
                        compilation.heavy_bitmaps,
                        key=lambda summary: (
                            summary["relation"],
                            int(summary["owner_key"]),
                        ),
                    )
                ),
            )
            return map_summary
    finally:
        await db.status(
            "DROP TABLE IF EXISTS "
            + ", ".join(f"{schema}.{_quote_ident(stage)}" for stage in stages)
            + ";"
        )


async def _publish_v4_graph(
    compilation: V4GraphCompilationResult,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    compressed_acquisition_bytes: int,
    empty_npi_tin_only_normalization_count: int,
) -> _V4GraphPublication:
    """Publish graph blocks only to CAS, then make packed maps authoritative."""

    block_artifact = _v4_compiler_artifact(compilation, "graph_blocks")
    block_stage = shared_block_stage_name(f"v4-graph-{snapshot_key}")
    await create_shared_block_stage(schema_name=schema_name, stage_table=block_stage)
    try:
        await copy_shared_block_binary_file(
            compilation.block_copy_path,
            schema_name=schema_name,
            stage_table=block_stage,
            expected_copy_bytes=int(block_artifact.byte_count),
            expected_copy_sha256=str(block_artifact.sha256),
            reuse_existing=True,
        )
        cas_publication = await publish_v4_cas_block_stage(
            schema_name=schema_name,
            stage_table=block_stage,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
        )
        map_summary = await _publish_v4_dictionaries_and_maps(
            compilation,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
            compressed_acquisition_bytes=int(compressed_acquisition_bytes),
            empty_npi_tin_only_normalization_count=int(
                empty_npi_tin_only_normalization_count
            ),
        )
    except BaseException:
        await _queue_failed_v4_graph_blocks(
            schema_name=schema_name,
            reference_manifest_path=compilation.reference_manifest_path,
        )
        raise
    finally:
        await db.status(
            "DROP TABLE IF EXISTS "
            f"{_quote_ident(schema_name)}.{_quote_ident(block_stage)};"
        )

    artifact_contracts = tuple(
        {
            "name": artifact.name,
            "sha256": artifact.sha256,
            "row_count": int(artifact.row_count),
            "byte_count": int(artifact.byte_count),
        }
        for artifact in compilation.output_artifacts
    )
    support_digest = shared_support_digest(
        {
            "contract_version": 1,
            "compiler_format": compilation.summary.get("format"),
            "selected_layout": compilation.selected_layout,
            "map_digest": map_summary.map_digest.hex(),
            "artifacts": artifact_contracts,
            "relation_summaries": tuple(compilation.relation_summaries),
            "heavy_bitmaps": tuple(compilation.heavy_bitmaps),
            "observe": dict(compilation.observe),
            "resource_admission": {
                "compressed_acquisition_bytes": int(
                    compressed_acquisition_bytes
                ),
                "empty_npi_tin_only_normalization_count": int(
                    empty_npi_tin_only_normalization_count
                ),
                **dict(compilation.resource_admission),
            },
        }
    )
    return _V4GraphPublication(
        object_kinds=map_summary.object_kinds,
        mapping_count=map_summary.coordinate_count,
        unique_block_count=int(cas_publication.unique_block_count),
        block_count=int(compilation.block_count),
        owner_count=sum(
            int(relation.get("owner_count") or 0)
            for relation in compilation.relation_summaries
        ),
        provider_group_count=int(compilation.observe.get("group_count") or 0),
        npi_count=int(compilation.observe.get("npi_count") or 0),
        support_digest=support_digest,
        logical_byte_count=int(cas_publication.logical_byte_count),
        stored_byte_count=(
            int(cas_publication.stored_byte_count)
            + int(map_summary.stored_map_byte_count)
        ),
        map_summary=map_summary,
        representation=(
            "pattern_v1" if compilation.selected_layout == "pattern" else "direct_v1"
        ),
        compiler_summary=dict(compilation.summary),
        audit_witness_path=compilation.provider_set_audit_npi_copy_path,
    )


async def _sealed_shared_serving_index(
    *,
    schema_name: str,
    snapshot_key: int,
    expected_generation: str,
) -> dict[str, Any]:
    """Read back the exact serving index committed by the physical seal."""

    schema = _quote_ident(schema_name)
    async with db.transaction() as session:
        manifest_result = await session.execute(
            db.text(
                f"""
                SELECT layout_manifest
                  FROM {schema}.ptg2_v3_snapshot_layout
                 WHERE snapshot_key = :snapshot_key
                   AND state = 'sealed'
                   AND generation = :generation
                """
            ),
            {
                "snapshot_key": int(snapshot_key),
                "generation": str(expected_generation),
            },
        )
        manifest = manifest_result.scalar()
    serving_index = (
        manifest.get("serving_index") if isinstance(manifest, Mapping) else None
    )
    if not isinstance(serving_index, Mapping):
        raise RuntimeError("sealed shared PTG layout is missing its serving index")
    return dict(serving_index)


def _physical_serving_index(
    *,
    snapshot_key: int,
    coverage_scope_id: bytes,
    finalizer_summary: Mapping[str, Any],
    price_publication: Any,
    graph_publication: Any,
    code_count: int,
    audit_sample: Mapping[str, Any],
    source_witness: Mapping[str, Any],
    provider_identifier_quarantine: Mapping[str, Any],
    finalizer_block_copy: Mapping[str, Any],
    stored_byte_count: int,
    full_rebuild_scope_digest: str | None = None,
) -> dict[str, Any]:
    """Build the physical serving index from validated publication summaries."""

    blocks = _mapping(finalizer_summary.get("blocks"), "blocks")
    dense_keys = _mapping(finalizer_summary.get("dense_keys"), "dense_keys")
    price_dense = _mapping(dense_keys.get("price"), "dense price keys")
    price_encoder = _mapping(
        blocks.get("price_dictionary_encoder"),
        "price dictionary encoder",
    )
    assigned_encoder = _mapping(blocks.get("assigned_encoder"), "assigned encoder")
    membership_summary_map = dict(
        price_publication.stream_summaries["price_set_atom_memberships_v3"]
    )
    atom_summary_map = dict(price_publication.stream_summaries["price_atoms_v3"])
    serving_rate_count = _integer(
        _mapping(finalizer_summary.get("preservation"), "preservation").get(
            "encoded_records"
        ),
        "encoded_records",
    )
    source_count = _integer(finalizer_summary.get("source_count"), "source_count")
    if source_count <= 0:
        raise RuntimeError("strict V3 source_count must be positive")
    quarantine = validate_provider_identifier_quarantine(
        provider_identifier_quarantine
    )
    price_dictionary = {
        **price_encoder,
        "price_set_count": _integer(price_dense.get("count"), "price key count"),
    }
    serving_index = {
        "storage": "manifest_snapshot",
        "type": "ptg2_shared_blocks_v3",
        "snapshot_scoped": True,
        "arch_version": "postgres_binary_v3",
        "storage_generation": PTG2_V3_SHARED_GENERATION,
        "cold_lookup_contract": PTG2_V3_COLD_LOOKUP_CONTRACT,
        "price_membership_semantics": PTG2_V3_PRICE_MEMBERSHIP_SEMANTICS,
        "serving_multiplicity_semantics": (PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS),
        "shared_snapshot_key": int(snapshot_key),
        "coverage_scope_id": coverage_scope_id.hex(),
        "serving_binary_table": None,
        "table": None,
        "materialized_tables": {},
        "provider_scope_strategy": "postgres_shared_graph",
        "id_storage": "binary128",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": PTG2_V3_SHARED_BLOCK_LAYOUT,
        "source_count": source_count,
        "code_count": int(code_count),
        "serving_rates": serving_rate_count,
        "rate_count": serving_rate_count,
        "atom_key_bits": int(price_publication.atom_key_bits),
        "price_atom_constant_keys": dict(price_publication.price_atom_constant_keys),
        "price_atom_constant_values": dict(
            price_publication.price_atom_constant_values
        ),
        "price_stage": dict(price_publication.stage_metrics),
        "serving_binary": {
            "format": "postgres_binary_v3",
            "price_dictionary": price_dictionary,
            "price_set_atom_memberships_v3": membership_summary_map,
            "price_atoms_v3": atom_summary_map,
            "assigned_encoder": assigned_encoder,
        },
        "provider_graph": {
            "owner_count": int(graph_publication.owner_count),
            "provider_group_count": int(graph_publication.provider_group_count),
            "npi_count": int(graph_publication.npi_count),
            "block_count": int(graph_publication.block_count),
        },
        "provider_identifier_quarantine": quarantine,
        "finalizer_block_copy": dict(finalizer_block_copy),
        "audit_sample": dict(audit_sample),
        "source_witness": dict(source_witness),
        "storage_bytes": int(stored_byte_count),
        "timings": dict(finalizer_summary.get("timings") or {}),
    }
    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if normalized_rebuild_digest is not None:
        serving_index["full_rebuild_scope_digest"] = normalized_rebuild_digest
    return serving_index


def _shared_layout_support_digest(
    *,
    core_support: Mapping[str, Any],
    audit_sample: Mapping[str, Any],
    source_witness: Mapping[str, Any],
    full_rebuild_scope_digest: str | None = None,
) -> bytes:
    """Seal support metadata, optionally isolating one controlled rebuild."""

    support_by_field = {
        **dict(core_support),
        "audit_sample": dict(audit_sample),
        "source_witness": dict(source_witness),
    }
    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    if normalized_rebuild_digest is not None:
        support_by_field["full_rebuild_scope_digest"] = normalized_rebuild_digest
    return shared_support_digest(support_by_field)


async def _publish_prepared_shared_layout(
    *,
    schema_name: str,
    manifest_stage_table: str,
    reserved_snapshot_key: int,
    build_token: str,
    expected_coverage_scope_id: bytes,
    logical_snapshot_id: str,
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    source_audit_witness_entries: Iterable[Mapping[str, Any]],
    expected_raw_source_sha256: Iterable[str],
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_identifier_quarantine: Mapping[str, Any],
    prepared_price: PreparedSharedPriceArtifacts,
    publication_started_at: float,
    price_prepare_seconds: float,
    scratch_parent: str | Path | None = None,
    prepared_work_directory: str | Path | None = None,
    prepared_finalizer: _PreparedFinalizer | None = None,
    prepared_price_publication: _PreparedPricePublication | None = None,
    full_rebuild_scope_digest: str | None = None,
    provider_graph_v4: bool = False,
    compressed_acquisition_bytes: int | None = None,
    empty_npi_tin_only_normalization_count: int | None = None,
) -> SharedSnapshotPublication:
    """Finalize, validate, publish, and atomically seal one physical layout."""

    publication_timing_map: dict[str, float] = {
        "price_prepare_seconds": float(price_prepare_seconds),
    }

    def record_stage(stage_name: str, started_at: float) -> None:
        """Record elapsed wall time for a named publication stage."""

        publication_timing_map[f"{stage_name}_seconds"] = time.monotonic() - started_at

    shared_generation = (
        PTG2_V4_SHARED_GENERATION
        if provider_graph_v4
        else PTG2_V3_SHARED_GENERATION
    )
    if provider_graph_v4 and (
        compressed_acquisition_bytes is None
        or int(compressed_acquisition_bytes) <= 0
        or empty_npi_tin_only_normalization_count is None
        or int(empty_npi_tin_only_normalization_count) < 0
    ):
        raise RuntimeError(
            "PTG V4 publication requires authenticated resource evidence"
        )
    configured_schema = str(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").strip()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict shared publication must use the configured PostgreSQL schema"
        )
    coverage_scope_id = _validated_coverage_scope_id(expected_coverage_scope_id)
    quarantine = validate_provider_identifier_quarantine(
        provider_identifier_quarantine
    )

    async def touch_build() -> None:
        """Refresh the reserved layout's build heartbeat transactionally."""

        async with db.transaction() as session:
            touch = (
                touch_v4_shared_layout_build
                if provider_graph_v4
                else touch_shared_layout_build
            )
            await touch(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
                build_token=str(build_token),
            )

    await touch_build()

    work_directory_context = (
        tempfile.TemporaryDirectory(
            prefix=(
                "ptg2-v4-shared-publish-"
                if provider_graph_v4
                else "ptg2-v3-shared-publish-"
            ),
            dir=str(scratch_parent) if scratch_parent is not None else None,
        )
        if prepared_work_directory is None
        else nullcontext(str(prepared_work_directory))
    )
    with work_directory_context as raw_work_directory:
        if prepared_finalizer is None:
            stage_started_at = time.monotonic()
            price_key_map_path = await export_shared_price_key_map(
                prepared_price,
                Path(raw_work_directory) / "price-key-map.copy",
            )
            record_stage("price_key_map_export", stage_started_at)
            stage_started_at = time.monotonic()
            finalizer_summary_by_field = await run_v3_direct_finalizer(
                work_directory=raw_work_directory,
                serving_run_entries=serving_run_entries,
                code_dictionary_entries=code_dictionary_entries,
                provider_set_metadata_entries=provider_set_metadata_entries,
                expected_source_identities=expected_source_identities,
                price_key_map_input=price_key_map_path,
                price_key_map_row_count=prepared_price.price_set_count,
                scratch_durability=(
                    PTG2_V3_EPHEMERAL_SCRATCH_DURABILITY
                    if prepared_work_directory is None
                    else PTG2_V3_DURABLE_SCRATCH_DURABILITY
                ),
            )
            record_stage("finalizer", stage_started_at)
        else:
            finalizer_summary_by_field = dict(prepared_finalizer.summary)
            publication_timing_map.update(
                {
                    "price_key_map_export_seconds": float(
                        prepared_finalizer.price_key_map_export_seconds
                    ),
                    "finalizer_seconds": float(prepared_finalizer.finalizer_seconds),
                    "price_key_ready_finalizer_wall_seconds": float(
                        prepared_finalizer.overlap_wall_seconds
                    ),
                }
            )
        await touch_build()
        finalizer_blocks = _mapping(
            finalizer_summary_by_field.get("blocks"),
            "blocks",
        )
        serving_block_summary = _mapping(
            finalizer_blocks.get("serving"), "serving blocks"
        )
        price_block_summary = _mapping(
            finalizer_blocks.get("price_dictionary"),
            "price dictionary blocks",
        )
        stage_started_at = time.monotonic()
        dictionary_publication = await publish_shared_finalizer_dictionaries(
            dict(finalizer_summary_by_field),
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            build_token=build_token,
            expected_coverage_scope_id=coverage_scope_id,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_generation=shared_generation,
        )
        record_stage("dictionary_publish", stage_started_at)
        await touch_build()
        stage_started_at = time.monotonic()
        provider_set_keys = await _export_provider_set_key_map(
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            output_path=Path(raw_work_directory) / "provider-set-authoritative.tsv",
        )
        record_stage("provider_set_key_export", stage_started_at)
        stage_started_at = time.monotonic()
        if provider_graph_v4:
            compile_task = asyncio.create_task(
                compile_provider_graph_v4_rust(
                    graph_artifact_entries=graph_artifact_entries,
                    provider_set_key_map_path=provider_set_keys,
                    output_directory=(
                        Path(raw_work_directory) / "provider-graph-v4-native"
                    ),
                )
            )
            try:
                while True:
                    try:
                        graph_conversion = await asyncio.wait_for(
                            asyncio.shield(compile_task),
                            timeout=30.0,
                        )
                        break
                    except TimeoutError:
                        await touch_build()
            except BaseException:
                compile_task.cancel()
                await asyncio.gather(compile_task, return_exceptions=True)
                raise
        else:
            graph_conversion = await _convert_shared_graph_natively(
                graph_artifact_entries=graph_artifact_entries,
                provider_set_key_map_path=provider_set_keys,
                work_directory=Path(raw_work_directory),
            )
        record_stage("provider_graph_convert", stage_started_at)

        block_stage = shared_block_stage_name(f"final-{reserved_snapshot_key}")

        async def publish_finalizer_blocks() -> Any:
            """Publish finalizer serving and price blocks."""

            stage_started_at = time.monotonic()
            await create_shared_block_stage(
                schema_name=schema_name,
                stage_table=block_stage,
            )
            try:
                serving_copy_metrics = await copy_shared_block_binary_file(
                    _output_file(finalizer_summary_by_field, serving_block_summary),
                    schema_name=schema_name,
                    stage_table=block_stage,
                    expected_copy_bytes=_integer(
                        serving_block_summary.get("copy_bytes"),
                        "serving block COPY bytes",
                    ),
                    expected_copy_sha256=str(
                        serving_block_summary.get("copy_sha256") or ""
                    ),
                    reuse_existing=True,
                )
                price_copy_metrics = await copy_shared_block_binary_file(
                    _output_file(finalizer_summary_by_field, price_block_summary),
                    schema_name=schema_name,
                    stage_table=block_stage,
                    expected_copy_bytes=_integer(
                        price_block_summary.get("copy_bytes"),
                        "price block COPY bytes",
                    ),
                    expected_copy_sha256=str(
                        price_block_summary.get("copy_sha256") or ""
                    ),
                    reuse_existing=True,
                )
                if serving_copy_metrics is None or price_copy_metrics is None:
                    raise RuntimeError(
                        "strict V3 finalizer block COPY did not return selective proof"
                    )
                publication = await publish_shared_block_stage(
                    schema_name=schema_name,
                    stage_table=block_stage,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
                    expected_generation=shared_generation,
                )
                return _FinalizerBlockPublicationResult(
                    publication=publication,
                    serving_copy=serving_copy_metrics,
                    price_dictionary_copy=price_copy_metrics,
                )
            finally:
                await db.status(
                    "DROP TABLE IF EXISTS "
                    f"{_quote_ident(schema_name)}.{_quote_ident(block_stage)};"
                )
                record_stage("serving_block_publish", stage_started_at)

        async def publish_provider_graph() -> Any:
            """Publish provider graph blocks and relational owner metadata."""

            stage_started_at = time.monotonic()
            try:
                if provider_graph_v4:
                    return await _publish_v4_graph(
                        graph_conversion,
                        schema_name=schema_name,
                        snapshot_key=int(reserved_snapshot_key),
                        build_token=build_token,
                        compressed_acquisition_bytes=int(
                            compressed_acquisition_bytes or 0
                        ),
                        empty_npi_tin_only_normalization_count=int(
                            empty_npi_tin_only_normalization_count or 0
                        ),
                    )
                return await publish_shared_graph(
                    graph_conversion,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
                )
            finally:
                record_stage("provider_graph_publish", stage_started_at)

        dense_keys = _mapping(
            finalizer_summary_by_field.get("dense_keys"),
            "dense keys",
        )
        price_dense = _mapping(dense_keys.get("price"), "dense price keys")
        expected_price_set_count = _integer(
            price_dense.get("count"), "price key count"
        )
        expected_price_key_order = str(price_dense.get("ordering") or "")
        if (
            prepared_price.price_set_count != expected_price_set_count
            or expected_price_key_order != PTG2_V3_PRICE_KEY_ORDER
        ):
            raise RuntimeError(
                "strict V3 finalizer price keys disagree with prepared price publication"
            )

        async def publish_price() -> Any:
            """Publish dense price dictionaries and membership blocks."""

            if prepared_price_publication is not None:
                publication_timing_map["price_publish_seconds"] = float(
                    prepared_price_publication.publish_seconds
                )
                return prepared_price_publication.publication
            stage_started_at = time.monotonic()
            try:
                return await publish_shared_price_artifacts(
                    schema_name=schema_name,
                    manifest_stage_table=manifest_stage_table,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
                    expected_price_set_count=expected_price_set_count,
                    expected_price_key_order=expected_price_key_order,
                    prepared=prepared_price,
                    expected_generation=shared_generation,
                )
            finally:
                record_stage("price_publish", stage_started_at)

        async def publish_source_witness() -> Any:
            """Publish the bounded source-fidelity witness."""

            stage_started_at = time.monotonic()
            try:
                return await publish_shared_source_witness(
                    schema_name=schema_name,
                    build_ownership=SharedLayoutBuildOwnership(
                        snapshot_key=int(reserved_snapshot_key),
                        build_token=build_token,
                    ),
                    entries=tuple(source_audit_witness_entries),
                    expected_raw_source_sha256=tuple(expected_raw_source_sha256),
                    expected_generation=shared_generation,
                )
            finally:
                record_stage("source_witness_publish", stage_started_at)

        independent_publish_started_at = time.monotonic()
        try:
            (
                finalizer_block_result,
                graph_publication,
                price_publication,
                source_witness_publication,
            ) = await _run_independent_publication_lanes(
                finalizer_blocks=publish_finalizer_blocks,
                provider_graph=publish_provider_graph,
                price=publish_price,
                source_witness=publish_source_witness,
            )
        finally:
            # The V4 audit re-authenticates the compiler's bounded witness
            # against the packed graph before seal.  Keep that witness alive
            # until the audit has completed; the V3 publisher has no such
            # post-publication dependency.
            if not provider_graph_v4:
                graph_conversion.cleanup()
        record_stage(
            "independent_publish_wall",
            independent_publish_started_at,
        )
        finalizer_block_publication = finalizer_block_result.publication
        finalizer_block_copy_manifest = finalizer_block_result.copy_manifest()
        await touch_build()
        stage_started_at = time.monotonic()
        async with db.transaction() as session:
            mapping_summary = await summarize_shared_snapshot_mappings(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
            )
        record_stage("mapping_summary", stage_started_at)
        _validate_authoritative_mapping_summary(
            mapping_summary,
            finalizer_block_publication,
            *(() if provider_graph_v4 else (graph_publication,)),
            price_publication,
        )
        observed_kinds = set(mapping_summary.object_kinds)
        missing_kinds = (
            _REQUIRED_PRICE_OBJECT_KINDS if provider_graph_v4 else _REQUIRED_OBJECT_KINDS
        ) - observed_kinds
        if missing_kinds:
            raise RuntimeError(
                f"strict V3 physical layout is missing required blocks: {sorted(missing_kinds)}"
            )
        core_support_map = {
            "contract_version": 2 if provider_graph_v4 else 1,
            "serving_multiplicity_semantics": (PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS),
            "finalizer_dictionaries": dictionary_publication.support_digest.hex(),
            "provider_graph": graph_publication.support_digest.hex(),
            "price_attributes": price_publication.support_digest.hex(),
            "source_witness": source_witness_publication.support_digest.hex(),
            "provider_identifier_quarantine": quarantine["sha256"],
        }
        if provider_graph_v4:
            # The V4 root digest owns only the factored provider graph.  Bind
            # the unchanged V3 rate/finalizer mappings into the support digest
            # so a graph-identical but rate-different layout cannot be reused.
            core_support_map["price_finalizer_mapping_digest"] = (
                mapping_summary.mapping_digest.hex()
            )
            core_support_map["price_finalizer_mapping_count"] = int(
                mapping_summary.mapping_count
            )
        core_support_digest = shared_support_digest(core_support_map)
        price_membership_summary = _mapping(
            price_publication.stream_summaries.get("price_set_atom_memberships_v3"),
            "price membership stream summary",
        )
        price_membership_block_span = _integer(
            price_membership_summary.get("block_span"),
            "price membership block span",
        )
        if price_membership_block_span <= 0:
            raise RuntimeError("strict V3 price membership block span must be positive")
        stage_started_at = time.monotonic()
        audit_build_ownership = SharedLayoutBuildOwnership(
            snapshot_key=int(reserved_snapshot_key),
            build_token=build_token,
        )
        if provider_graph_v4:
            try:
                audit_publication = await publish_v4_audit_sample(
                    schema_name=schema_name,
                    build_ownership=audit_build_ownership,
                    logical_snapshot_id=str(logical_snapshot_id),
                    finalizer_summary=finalizer_summary_by_field,
                    mapping_digest=graph_publication.map_summary.map_digest,
                    core_support_digest=core_support_digest,
                    atom_key_bits=int(price_publication.atom_key_bits),
                    price_membership_block_span=price_membership_block_span,
                    graph_compilation=graph_conversion,
                )
            finally:
                graph_conversion.cleanup()
        else:
            audit_publication = await publish_shared_audit_sample(
                schema_name=schema_name,
                build_ownership=audit_build_ownership,
                logical_snapshot_id=str(logical_snapshot_id),
                finalizer_summary=finalizer_summary_by_field,
                mapping_digest=mapping_summary.mapping_digest,
                core_support_digest=core_support_digest,
                atom_key_bits=int(price_publication.atom_key_bits),
                price_membership_block_span=price_membership_block_span,
            )
        record_stage("audit_publish", stage_started_at)
        await touch_build()
        support_digest = _shared_layout_support_digest(
            core_support=core_support_map,
            audit_sample=audit_publication.metadata,
            source_witness=source_witness_publication.metadata,
            full_rebuild_scope_digest=full_rebuild_scope_digest,
        )
        stored_byte_count = (
            int(finalizer_block_publication.stored_byte_count)
            + int(graph_publication.stored_byte_count)
            + int(price_publication.stored_byte_count)
            + int(source_witness_publication.stored_byte_count)
        )
        provisional_serving_index = _physical_serving_index(
            snapshot_key=int(reserved_snapshot_key),
            coverage_scope_id=coverage_scope_id,
            finalizer_summary=finalizer_summary_by_field,
            price_publication=price_publication,
            graph_publication=graph_publication,
            code_count=dictionary_publication.code_count,
            audit_sample=audit_publication.metadata,
            source_witness=source_witness_publication.metadata,
            provider_identifier_quarantine=quarantine,
            finalizer_block_copy=finalizer_block_copy_manifest,
            stored_byte_count=stored_byte_count,
            full_rebuild_scope_digest=full_rebuild_scope_digest,
        )
        provisional_serving_index["timings"] = {
            **dict(provisional_serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        stage_started_at = time.monotonic()
        async with db.transaction() as session:
            if provider_graph_v4:
                sealed = await seal_v4_shared_layout(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=str(build_token),
                    expected_summary=graph_publication.map_summary,
                    support_digest=support_digest,
                    layout_manifest={"serving_index": provisional_serving_index},
                )
                sealed_audit_metadata_map = dict(audit_publication.metadata)
            else:
                sealed = await seal_shared_layout(
                    session,
                    schema_name=schema_name,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=str(build_token),
                    expected_summary=mapping_summary,
                    support_digest=support_digest,
                    layout_manifest={"serving_index": provisional_serving_index},
                )
                sealed_audit_metadata_map = (
                    await sealed_audit_sample_metadata(
                        session,
                        schema_name=schema_name,
                        snapshot_key=int(sealed.snapshot_key),
                        logical_snapshot_id=str(logical_snapshot_id),
                    )
                    if sealed.reused
                    else dict(audit_publication.metadata)
                )
        record_stage("seal", stage_started_at)
        publication_timing_map["shared_publish_total_seconds"] = (
            time.monotonic() - publication_started_at
        )
        serving_index = (
            await _sealed_shared_serving_index(
                schema_name=schema_name,
                snapshot_key=int(sealed.snapshot_key),
                expected_generation=PTG2_V4_SHARED_GENERATION,
            )
            if provider_graph_v4
            else dict(provisional_serving_index)
        )
        serving_index["timings"] = {
            **dict(serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        serving_index["shared_snapshot_key"] = int(sealed.snapshot_key)
        serving_index["audit_sample"] = sealed_audit_metadata_map
        return SharedSnapshotPublication(
            snapshot_key=int(sealed.snapshot_key),
            serving_index=serving_index,
            object_kinds=(
                graph_publication.object_kinds
                if provider_graph_v4
                else mapping_summary.object_kinds
            ),
            mapping_count=(
                graph_publication.mapping_count
                if provider_graph_v4
                else mapping_summary.mapping_count
            ),
            unique_block_count=(
                graph_publication.unique_block_count
                if provider_graph_v4
                else mapping_summary.unique_block_count
            ),
            mapping_digest=(
                graph_publication.map_summary.map_digest
                if provider_graph_v4
                else mapping_summary.mapping_digest
            ),
            finalizer_summary=dict(finalizer_summary_by_field),
            layout_reused_at_seal=bool(sealed.reused),
            stored_byte_count=stored_byte_count,
        )


async def publish_strict_shared_v3_layout(
    *,
    schema_name: str,
    manifest_stage_table: str,
    reserved_snapshot_key: int,
    build_token: str,
    expected_coverage_scope_id: bytes,
    logical_snapshot_id: str,
    expected_source_identities: Iterable[
        Mapping[str, Any] | SharedPhysicalArtifactIdentity
    ],
    serving_run_entries: Iterable[Mapping[str, Any]],
    code_dictionary_entries: Iterable[Mapping[str, Any]],
    provider_set_metadata_entries: Iterable[Mapping[str, Any]],
    source_audit_witness_entries: Iterable[Mapping[str, Any]],
    price_set_summary_source_count: int | None = None,
    expected_raw_source_sha256: Iterable[str],
    graph_artifact_entries: Iterable[dict[str, Any]],
    provider_identifier_quarantine: Mapping[str, Any],
    scratch_parent: str | Path | None = None,
    full_rebuild_scope_digest: str | None = None,
    provider_graph_v4: bool = False,
    compressed_acquisition_entries: Iterable[Mapping[str, Any]] | None = None,
    empty_npi_tin_only_normalization_count: int | None = None,
) -> SharedSnapshotPublication:
    """Prepare exact price ranks once, then publish and clean every temporary map."""

    normalized_rebuild_digest = normalized_full_rebuild_scope_digest(
        full_rebuild_scope_digest
    )
    serving_run_entries = tuple(serving_run_entries)
    code_dictionary_entries = tuple(code_dictionary_entries)
    provider_set_metadata_entries = tuple(provider_set_metadata_entries)
    expected_source_identities = tuple(expected_source_identities)
    expected_raw_source_digests = tuple(
        str(raw_hash or "").strip().lower()
        for raw_hash in expected_raw_source_sha256
    )
    compressed_acquisition_bytes: int | None = None
    if provider_graph_v4:
        if (
            empty_npi_tin_only_normalization_count is None
            or int(empty_npi_tin_only_normalization_count) < 0
        ):
            raise RuntimeError(
                "PTG V4 empty-NPI TIN-only normalization evidence is invalid"
            )
        byte_count_by_hash: dict[str, int] = {}
        for raw_entry in tuple(compressed_acquisition_entries or ()):
            raw_hash = str(raw_entry.get("raw_sha256") or "").strip().lower()
            try:
                byte_count = int(raw_entry.get("byte_count"))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "PTG V4 compressed acquisition entry is invalid"
                ) from exc
            if (
                len(raw_hash) != 64
                or any(character not in "0123456789abcdef" for character in raw_hash)
                or byte_count <= 0
                or (
                    raw_hash in byte_count_by_hash
                    and byte_count_by_hash[raw_hash] != byte_count
                )
            ):
                raise RuntimeError(
                    "PTG V4 compressed acquisition entry is invalid"
                )
            byte_count_by_hash[raw_hash] = byte_count
        if set(byte_count_by_hash) != set(expected_raw_source_digests):
            raise RuntimeError(
                "PTG V4 compressed acquisition inputs do not match source hashes"
            )
        compressed_acquisition_bytes = sum(byte_count_by_hash.values())
        if compressed_acquisition_bytes <= 0:
            raise RuntimeError(
                "PTG V4 compressed acquisition bytes must be positive"
            )
    publication_started_at = time.monotonic()
    configured_schema = str(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").strip()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict V3 publication must use the configured PostgreSQL schema"
        )
    shared_generation = (
        PTG2_V4_SHARED_GENERATION
        if provider_graph_v4
        else PTG2_V3_SHARED_GENERATION
    )
    async with db.transaction() as session:
        touch = (
            touch_v4_shared_layout_build
            if provider_graph_v4
            else touch_shared_layout_build
        )
        await touch(
            session,
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            build_token=str(build_token),
        )
    with tempfile.TemporaryDirectory(
        prefix=(
            "ptg2-v4-shared-publish-"
            if provider_graph_v4
            else "ptg2-v3-shared-publish-"
        ),
        dir=str(scratch_parent) if scratch_parent is not None else None,
    ) as raw_work_directory:
        async def publish_prepared_price_early(
            prepared: PreparedSharedPriceArtifacts,
        ) -> Any:
            """Publish price blocks while the independent finalizer is active."""

            return await publish_shared_price_artifacts(
                schema_name=schema_name,
                manifest_stage_table=manifest_stage_table,
                snapshot_key=int(reserved_snapshot_key),
                build_token=build_token,
                expected_price_set_count=int(prepared.price_set_count),
                expected_price_key_order=PTG2_V3_PRICE_KEY_ORDER,
                prepared=prepared,
                expected_generation=shared_generation,
            )

        (
            prepared_price,
            price_prepare_seconds,
            prepared_finalizer,
            prepared_price_publication,
        ) = await _prepare_price_with_early_finalizer(
            schema_name=schema_name,
            manifest_stage_table=manifest_stage_table,
            price_set_summary_source_count=price_set_summary_source_count,
            raw_work_directory=raw_work_directory,
            serving_run_entries=serving_run_entries,
            code_dictionary_entries=code_dictionary_entries,
            provider_set_metadata_entries=provider_set_metadata_entries,
            expected_source_identities=expected_source_identities,
            publish_prepared_price=publish_prepared_price_early,
        )
        try:
            publication = await _publish_prepared_shared_layout(
                schema_name=schema_name,
                manifest_stage_table=manifest_stage_table,
                reserved_snapshot_key=int(reserved_snapshot_key),
                build_token=build_token,
                expected_coverage_scope_id=expected_coverage_scope_id,
                logical_snapshot_id=logical_snapshot_id,
                expected_source_identities=expected_source_identities,
                serving_run_entries=serving_run_entries,
                code_dictionary_entries=code_dictionary_entries,
                provider_set_metadata_entries=provider_set_metadata_entries,
                source_audit_witness_entries=source_audit_witness_entries,
                expected_raw_source_sha256=expected_raw_source_digests,
                graph_artifact_entries=graph_artifact_entries,
                provider_identifier_quarantine=provider_identifier_quarantine,
                prepared_price=prepared_price,
                publication_started_at=publication_started_at,
                price_prepare_seconds=price_prepare_seconds,
                scratch_parent=scratch_parent,
                prepared_work_directory=raw_work_directory,
                prepared_finalizer=prepared_finalizer,
                prepared_price_publication=prepared_price_publication,
                full_rebuild_scope_digest=normalized_rebuild_digest,
                provider_graph_v4=provider_graph_v4,
                compressed_acquisition_bytes=compressed_acquisition_bytes,
                empty_npi_tin_only_normalization_count=(
                    empty_npi_tin_only_normalization_count
                ),
            )
        except BaseException:
            cleanup_task = asyncio.create_task(
                cleanup_prepared_shared_price_artifacts(prepared_price)
            )
            await _await_cleanup_task(cleanup_task)
            raise
        cleanup_task = asyncio.create_task(
            cleanup_prepared_shared_price_artifacts(prepared_price)
        )
        await _await_cleanup_task(cleanup_task, propagate_cancellation=True)
        return publication


delete_unpublished_shared_v3_snapshot_sources = delete_unpublished_snapshot_sources
validate_reused_shared_v3_snapshot_sources = validate_reused_snapshot_sources


__all__ = [
    "SharedSnapshotPublication",
    "delete_unpublished_shared_v3_snapshot_sources",
    "delete_unpublished_snapshot_sources",
    "publish_shared_v3_snapshot_sources",
    "publish_strict_shared_v3_layout",
    "validate_reused_shared_v3_snapshot_sources",
    "validate_reused_snapshot_sources",
]
