# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""End-to-end physical publication for strict shared-block PTG V3."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time
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
from process.ptg_parts.ptg2_shared_finalize import run_v3_direct_finalizer
from process.ptg_parts.ptg2_shared_graph import SharedGraphConversionResult
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_provider_quarantine import (
    validate_provider_identifier_quarantine,
)
from process.ptg_parts.rust_scanner import (
    convert_v3_provider_membership_shards_to_shared_graph_rust,
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
    _validated_coverage_scope_id,
    copy_shared_block_binary_file,
    create_shared_block_stage,
    publish_shared_block_stage,
    publish_shared_finalizer_dictionaries,
    publish_shared_graph,
    shared_block_stage_name,
    shared_graph_bundles_from_artifacts,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    deterministic_source_key_assignments,
)
from process.ptg_parts.ptg2_source_witness_store import publish_shared_source_witness


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
    return tuple(source_records)


async def delete_unpublished_shared_v3_snapshot_sources(
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
        snapshot_state = _row_mapping(snapshot_row) if snapshot_row is not None else {}
        if snapshot_state.get("is_bound") or str(snapshot_state.get("status") or "") not in {
            "building",
            "failed",
        }:
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


async def validate_reused_shared_v3_snapshot_sources(
    *,
    schema_name: str,
    snapshot_key: int,
    logical_snapshot_id: str,
) -> dict[str, Any]:
    """Validate reused physical audit source keys against this logical dictionary."""

    async with db.transaction() as session:
        return await sealed_audit_sample_metadata(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            logical_snapshot_id=str(logical_snapshot_id),
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
    return await convert_v3_provider_membership_shards_to_shared_graph_rust(
        shards=graph_bundles,
        provider_set_key_map_path=Path(provider_set_key_map_path),
        output_directory=Path(work_directory) / "provider-graph-native",
    )


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
    stored_byte_count: int,
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
    return {
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
        "audit_sample": dict(audit_sample),
        "source_witness": dict(source_witness),
        "storage_bytes": int(stored_byte_count),
        "timings": dict(finalizer_summary.get("timings") or {}),
    }


async def _publish_strict_shared_v3_layout_prepared(
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
) -> SharedSnapshotPublication:
    """Finalize, validate, publish, and atomically seal one physical layout."""

    publication_timing_map: dict[str, float] = {
        "price_prepare_seconds": float(price_prepare_seconds),
    }

    def record_stage(stage_name: str, started_at: float) -> None:
        """Record elapsed wall time for a named publication stage."""

        publication_timing_map[f"{stage_name}_seconds"] = time.monotonic() - started_at

    configured_schema = str(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").strip()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict V3 publication must use the configured PostgreSQL schema"
        )
    coverage_scope_id = _validated_coverage_scope_id(expected_coverage_scope_id)
    quarantine = validate_provider_identifier_quarantine(
        provider_identifier_quarantine
    )

    async def touch_build() -> None:
        """Refresh the reserved layout's build heartbeat transactionally."""

        async with db.transaction() as session:
            await touch_shared_layout_build(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
                build_token=str(build_token),
            )

    await touch_build()

    work_directory_context = (
        tempfile.TemporaryDirectory(
            prefix="ptg2-v3-shared-publish-",
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
                await copy_shared_block_binary_file(
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
                )
                await copy_shared_block_binary_file(
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
                )
                return await publish_shared_block_stage(
                    schema_name=schema_name,
                    stage_table=block_stage,
                    snapshot_key=int(reserved_snapshot_key),
                    build_token=build_token,
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
                )
            finally:
                record_stage("source_witness_publish", stage_started_at)

        independent_publish_started_at = time.monotonic()
        try:
            (
                finalizer_block_publication,
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
            graph_conversion.cleanup()
        record_stage(
            "independent_publish_wall",
            independent_publish_started_at,
        )
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
            graph_publication,
            price_publication,
        )
        observed_kinds = set(mapping_summary.object_kinds)
        missing_kinds = _REQUIRED_OBJECT_KINDS - observed_kinds
        if missing_kinds:
            raise RuntimeError(
                f"strict V3 physical layout is missing required blocks: {sorted(missing_kinds)}"
            )
        core_support_map = {
            "contract_version": 1,
            "serving_multiplicity_semantics": (PTG2_V3_SERVING_MULTIPLICITY_SEMANTICS),
            "finalizer_dictionaries": dictionary_publication.support_digest.hex(),
            "provider_graph": graph_publication.support_digest.hex(),
            "price_attributes": price_publication.support_digest.hex(),
            "source_witness": source_witness_publication.support_digest.hex(),
            "provider_identifier_quarantine": quarantine["sha256"],
        }
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
        audit_publication = await publish_shared_audit_sample(
            schema_name=schema_name,
            build_ownership=SharedLayoutBuildOwnership(
                snapshot_key=int(reserved_snapshot_key),
                build_token=build_token,
            ),
            logical_snapshot_id=str(logical_snapshot_id),
            finalizer_summary=finalizer_summary_by_field,
            mapping_digest=mapping_summary.mapping_digest,
            core_support_digest=core_support_digest,
            atom_key_bits=int(price_publication.atom_key_bits),
            price_membership_block_span=price_membership_block_span,
        )
        record_stage("audit_publish", stage_started_at)
        await touch_build()
        support_digest = shared_support_digest(
            {
                **core_support_map,
                "audit_sample": dict(audit_publication.metadata),
                "source_witness": dict(source_witness_publication.metadata),
            }
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
            stored_byte_count=stored_byte_count,
        )
        provisional_serving_index["timings"] = {
            **dict(provisional_serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        stage_started_at = time.monotonic()
        async with db.transaction() as session:
            sealed = await seal_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_key=int(reserved_snapshot_key),
                build_token=str(build_token),
                expected_summary=mapping_summary,
                support_digest=support_digest,
                layout_manifest={"serving_index": provisional_serving_index},
            )
            sealed_audit_sample = (
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
        serving_index = dict(provisional_serving_index)
        serving_index["timings"] = {
            **dict(serving_index.get("timings") or {}),
            **publication_timing_map,
        }
        serving_index["shared_snapshot_key"] = int(sealed.snapshot_key)
        serving_index["audit_sample"] = sealed_audit_sample
        return SharedSnapshotPublication(
            snapshot_key=int(sealed.snapshot_key),
            serving_index=serving_index,
            object_kinds=mapping_summary.object_kinds,
            mapping_count=mapping_summary.mapping_count,
            unique_block_count=mapping_summary.unique_block_count,
            mapping_digest=mapping_summary.mapping_digest,
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
) -> SharedSnapshotPublication:
    """Prepare exact price ranks once, then publish and clean every temporary map."""

    serving_run_entries = tuple(serving_run_entries)
    code_dictionary_entries = tuple(code_dictionary_entries)
    provider_set_metadata_entries = tuple(provider_set_metadata_entries)
    expected_source_identities = tuple(expected_source_identities)
    publication_started_at = time.monotonic()
    configured_schema = str(os.getenv("HLTHPRT_DB_SCHEMA") or "mrf").strip()
    if str(schema_name).strip() != configured_schema:
        raise RuntimeError(
            "strict V3 publication must use the configured PostgreSQL schema"
        )
    async with db.transaction() as session:
        await touch_shared_layout_build(
            session,
            schema_name=schema_name,
            snapshot_key=int(reserved_snapshot_key),
            build_token=str(build_token),
        )
    with tempfile.TemporaryDirectory(
        prefix="ptg2-v3-shared-publish-",
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
            publication = await _publish_strict_shared_v3_layout_prepared(
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
                expected_raw_source_sha256=expected_raw_source_sha256,
                graph_artifact_entries=graph_artifact_entries,
                provider_identifier_quarantine=provider_identifier_quarantine,
                prepared_price=prepared_price,
                publication_started_at=publication_started_at,
                price_prepare_seconds=price_prepare_seconds,
                scratch_parent=scratch_parent,
                prepared_work_directory=raw_work_directory,
                prepared_finalizer=prepared_finalizer,
                prepared_price_publication=prepared_price_publication,
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


__all__ = [
    "SharedSnapshotPublication",
    "delete_unpublished_shared_v3_snapshot_sources",
    "publish_shared_v3_snapshot_sources",
    "publish_strict_shared_v3_layout",
    "validate_reused_shared_v3_snapshot_sources",
]
