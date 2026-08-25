"""Thin production publication lifecycle used by the packed-finalizer ABBA screen."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V4_SHARED_GENERATION,
    summarize_native_v4_finalizer_mappings,
    summarize_shared_snapshot_mappings,
)
from process.ptg_parts.ptg2_shared_publish import (
    copy_shared_block_binary_file,
    create_shared_block_stage,
    publish_shared_block_stage,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _FinalizerBlockStageRequest,
    _finalizer_block_stage_name,
    _publish_finalizer_block_stage,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
)
from process.ptg_parts.ptg2_v4_finalizer_publish import _pack_stage_name
from process.ptg_parts.ptg2_v4_snapshot_maps import reserve_v4_shared_layout
from scripts.research.ptg2_packed_finalizer_abba_contract import (
    BenchmarkArtifacts,
    CANONICAL_FIELDS,
)
from scripts.research.ptg2_packed_finalizer_abba_receipt import (
    _jsonable,
    _native_summary_receipt,
    _summary_receipt,
)
from tests.ptg2_v4_attempt_migration_postgres_support import (
    migration,
    run_migration_action,
)
from tests.test_ptg2_shared_snapshot_publish_postgres import (
    _SELECTIVE_V4_MAP_PACK_DDL,
    _SELECTIVE_V4_MAP_ROOT_DDL,
    _create_shared_schema,
)


SCHEMA_RE = re.compile(r"^ptg_packed_abba_[a-f0-9]{12}_(?:a1|b1|b2|a2)$")
NATIVE_SUMMARY_FIELDS = (
    "mapping_count",
    "unique_block_count",
    "entry_count",
    "logical_byte_count",
    "object_kinds",
    "packed_mapping_digest",
    "packed_mapping_count",
    "packed_canonical_byte_count",
    "relational_mapping_digest",
    "relational_mapping_count",
)
_NATIVE_COPY_CONTRACT = "native_unique_shared_block_copy_v2"
_FINALIZER_PHASE_METRICS = frozenset({
    "finalizer_sidecars_staged",
    "finalizer_pins_prepared",
    "finalizer_cas_published",
    "finalizer_map_rows_attached",
    "finalizer_map_attached",
})


@dataclass(frozen=True)
class ArmRequest:
    label: str
    packed: bool
    schema_name: str
    snapshot_key: int
    build_token: str
    work_directory: Path
    artifacts: BenchmarkArtifacts


async def prepare_arm_schema(
    dsn: str,
    *,
    schema_name: str,
    build_token: str,
    shape_sha256: str,
) -> int:
    """Install the real schema/migration and reserve one fresh V4 candidate."""

    await install_arm_schema(dsn, schema_name=schema_name)
    return await reserve_arm_layout(
        schema_name=schema_name,
        build_token=build_token,
        shape_sha256=shape_sha256,
    )


async def install_arm_schema(dsn: str, *, schema_name: str) -> None:
    """Install one exact disposable schema outside the production timing."""

    if not SCHEMA_RE.fullmatch(schema_name):
        raise ValueError("ABBA schema name is not task-scoped")
    await _create_shared_schema(schema_name)
    await db.execute_ddl(
        _SELECTIVE_V4_MAP_ROOT_DDL.format(schema=_quote_ident(schema_name))
    )
    await db.execute_ddl(
        _SELECTIVE_V4_MAP_PACK_DDL.format(schema=_quote_ident(schema_name))
    )
    migration_module = migration("20260825120000_ptg_v4_finalizer_map_pack.py")
    with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema_name}):
        await run_migration_action(dsn, migration_module, "upgrade")


async def reserve_arm_layout(
    *,
    schema_name: str,
    build_token: str,
    shape_sha256: str,
) -> int:
    """Reserve the real production layout included in lifecycle timing."""

    fingerprint = hashlib.sha256(
        b"PTG2-PACKED-FINALIZER-ABBA\x00" + bytes.fromhex(shape_sha256)
    ).digest()
    async with db.transaction() as session:
        reservation = await reserve_v4_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=fingerprint,
            build_token=build_token,
        )
    if reservation.reused:
        raise RuntimeError("fresh ABBA schema unexpectedly reused a V4 layout")
    return int(reservation.snapshot_key)


async def _publish_price_artifact(request: ArmRequest) -> Any:
    stage_table = f"ptg2_v3_block_stage_abba_price_{request.label}"
    await create_shared_block_stage(schema_name=request.schema_name, stage_table=stage_table)
    try:
        artifact = request.artifacts.relational_price
        metrics = await copy_shared_block_binary_file(
            artifact.path,
            schema_name=request.schema_name,
            stage_table=stage_table,
            expected_copy_bytes=artifact.byte_count,
            expected_copy_sha256=artifact.sha256,
            reuse_existing=True,
        )
        if metrics is None:
            raise RuntimeError("ABBA price COPY omitted its production receipt")
        return await publish_shared_block_stage(
            schema_name=request.schema_name,
            stage_table=stage_table,
            snapshot_key=request.snapshot_key,
            build_token=request.build_token,
            expected_generation=PTG2_V4_SHARED_GENERATION,
        )
    finally:
        schema = _quote_ident(request.schema_name)
        stage = _quote_ident(stage_table)
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage}")


def _finalizer_stage_request(
    request: ArmRequest,
    progress_callback: Any = None,
) -> _FinalizerBlockStageRequest:
    summary = request.artifacts.finalizer_summary()
    return _FinalizerBlockStageRequest(
        schema_name=request.schema_name,
        stage_table=_finalizer_block_stage_name(
            request.snapshot_key, request.build_token
        ),
        snapshot_key=request.snapshot_key,
        build_token=request.build_token,
        expected_generation=PTG2_V4_SHARED_GENERATION,
        finalizer_summary=summary,
        serving_summary=summary["blocks"]["serving"],
        price_summary=summary["blocks"]["price_dictionary"],
        work_directory=request.work_directory,
        packed=request.packed,
        progress_callback=progress_callback,
    )


async def _publish_finalizer(request: ArmRequest) -> tuple[Any, float, list[dict[str, Any]]]:
    started_at = time.monotonic()
    timeline_rows: list[dict[str, Any]] = []

    def record_phase(metric: str, amount: int) -> None:
        """Record one monotonic packed-finalizer phase boundary."""

        if metric not in _FINALIZER_PHASE_METRICS:
            return
        elapsed = time.monotonic() - started_at
        previous_elapsed = (
            timeline_rows[-1]["elapsed_seconds"] if timeline_rows else 0.0
        )
        timeline_rows.append({
            "metric": metric,
            "amount": int(amount),
            "elapsed_seconds": elapsed,
            "phase_seconds": elapsed - previous_elapsed,
        })

    publication_result = await _publish_finalizer_block_stage(
        _finalizer_stage_request(request, record_phase)
    )
    elapsed = time.monotonic() - started_at
    previous_elapsed = (
        timeline_rows[-1]["elapsed_seconds"] if timeline_rows else 0.0
    )
    timeline_rows.append({
        "metric": "finalizer_complete",
        "amount": request.artifacts.shape.finalizer_mapping_count,
        "elapsed_seconds": elapsed,
        "phase_seconds": elapsed - previous_elapsed,
    })
    return publication_result, elapsed, timeline_rows


async def run_packed_failure_probe(request: ArmRequest, progress_callback: Any) -> None:
    """Run the real packed wrapper with one deliberate failure injection."""

    if not request.packed:
        raise ValueError("packed failure probe requires the packed arm")
    await _publish_finalizer_block_stage(
        _finalizer_stage_request(request, progress_callback)
    )


async def _persisted_receipt(request: ArmRequest) -> dict[str, Any]:
    schema = _quote_ident(request.schema_name)
    count_row = await db.first(
        f"""
        SELECT
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_root
            WHERE snapshot_key = :snapshot_key),
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_pack
            WHERE snapshot_key = :snapshot_key),
          (SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_target
            WHERE snapshot_key = :snapshot_key),
          (SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block
            WHERE snapshot_key = :snapshot_key),
          (SELECT COUNT(*) FROM {schema}.ptg2_block_build_pin
            WHERE snapshot_key = :snapshot_key),
          (SELECT COUNT(*) FROM {schema}.ptg2_v3_gc_candidate),
          (SELECT COUNT(*) FROM {schema}.ptg2_v3_block)
        """,
        snapshot_key=request.snapshot_key,
    )
    names = (
        "root_rows",
        "pack_rows",
        "target_rows",
        "relational_rows",
        "pin_rows",
        "gc_rows",
        "cas_rows",
    )
    persisted_by_field = dict(zip(names, map(int, count_row), strict=True))
    final_stage = _finalizer_block_stage_name(
        request.snapshot_key, request.build_token
    )
    stages = await db.first(
        "SELECT to_regclass(:final_stage), to_regclass(:pack_stage), "
        "to_regclass(:price_stage)",
        final_stage=f"{request.schema_name}.{final_stage}",
        pack_stage=f"{request.schema_name}.{_pack_stage_name(final_stage)}",
        price_stage=(
            f"{request.schema_name}.ptg2_v3_block_stage_abba_price_{request.label}"
        ),
    )
    persisted_by_field["stage_tables_present"] = sum(
        stage_name is not None for stage_name in stages
    )
    return persisted_by_field


async def inspect_arm_state(request: ArmRequest) -> dict[str, Any]:
    """Read persisted arm state before exact schema cleanup."""

    return await _persisted_receipt(request)


def _expected_storage_shape(request: ArmRequest) -> tuple[int, int, int, int, int]:
    shape = request.artifacts.shape
    if not request.packed:
        return (0, 0, 0, shape.mapping_count, shape.unique_block_count)
    price_mapping_count = shape.mapping_count - shape.finalizer_mapping_count
    return (
        1,
        shape.map_pack_count,
        shape.finalizer_unique_block_count,
        price_mapping_count,
        shape.unique_block_count + shape.map_pack_count,
    )


def _validate_arm(
    request: ArmRequest,
    summary: Mapping[str, Any],
    timed_summary: Mapping[str, Any],
    persisted: Mapping[str, int],
) -> None:
    expected = request.artifacts.expected_summary
    if any(summary[field] != expected[field] for field in CANONICAL_FIELDS):
        raise RuntimeError(f"ABBA {request.label} canonical summary parity failed")
    residue_fields = ("pin_rows", "gc_rows", "stage_tables_present")
    if any(persisted[field] for field in residue_fields):
        raise RuntimeError(f"ABBA {request.label} left stage, pin, or GC residue")
    if request.packed:
        component_fields = (
            "packed_mapping_digest",
            "packed_mapping_count",
            "relational_mapping_digest",
            "relational_mapping_count",
        )
        if any(summary[field] != expected[field] for field in component_fields):
            raise RuntimeError(f"ABBA {request.label} component digest parity failed")
        if any(timed_summary[field] != expected[field] for field in NATIVE_SUMMARY_FIELDS):
            raise RuntimeError(f"ABBA {request.label} native summary parity failed")
    storage_fields = (
        "root_rows",
        "pack_rows",
        "target_rows",
        "relational_rows",
        "cas_rows",
    )
    if tuple(persisted[field] for field in storage_fields) != _expected_storage_shape(request):
        raise RuntimeError(f"ABBA {request.label} persisted storage shape changed")
    if any(request.work_directory.iterdir()):
        raise RuntimeError(f"ABBA {request.label} left packed sidecar residue")


async def _measure_arm_summaries(
    request: ArmRequest,
) -> tuple[dict[str, Any], dict[str, Any], float, float]:
    summary_started_at = time.monotonic()
    async with db.transaction() as session:
        timed_summary = await (
            summarize_native_v4_finalizer_mappings
            if request.packed
            else summarize_shared_snapshot_mappings
        )(
            session, schema_name=request.schema_name, snapshot_key=request.snapshot_key
        )
    summary_seconds = time.monotonic() - summary_started_at
    parity_oracle_started_at = time.monotonic()
    if request.packed:
        async with db.transaction() as session:
            summary = await summarize_shared_snapshot_mappings(
                session,
                schema_name=request.schema_name,
                snapshot_key=request.snapshot_key,
            )
        parity_oracle_seconds = time.monotonic() - parity_oracle_started_at
    else:
        summary = timed_summary
        parity_oracle_seconds = 0.0
    summary_by_field = _summary_receipt(summary)
    timed_summary_by_field = (
        _native_summary_receipt(timed_summary)
        if request.packed
        else summary_by_field
    )
    return (
        summary_by_field,
        timed_summary_by_field,
        summary_seconds,
        parity_oracle_seconds,
    )


async def run_production_arm(request: ArmRequest) -> dict[str, Any]:
    """Time production publication, commit, cleanup, and canonical summary."""

    publication_started_at = time.monotonic()
    if request.packed:
        price_result = await _publish_price_artifact(request)
        finalizer_result, finalizer_seconds, phase_timeline = await _publish_finalizer(
            request
        )
    else:
        (finalizer_result, finalizer_seconds, phase_timeline), price_result = (
            await asyncio.gather(
                _publish_finalizer(request),
                _publish_price_artifact(request),
            )
        )
    publication_seconds = time.monotonic() - publication_started_at
    (
        summary_by_field,
        timed_summary_by_field,
        summary_seconds,
        parity_oracle_seconds,
    ) = await _measure_arm_summaries(request)
    copy_manifest = finalizer_result.copy_manifest()
    if request.packed and (
        finalizer_result.publication.contract
        != PTG2_V4_FINALIZER_MAP_CONTRACT
        or copy_manifest.get("contract") != _NATIVE_COPY_CONTRACT
    ):
        raise RuntimeError(f"ABBA {request.label} native publication contract changed")
    persisted = await _persisted_receipt(request)
    _validate_arm(request, summary_by_field, timed_summary_by_field, persisted)
    whole_seconds = publication_seconds + summary_seconds
    return {
        "label": request.label,
        "arm": "packed" if request.packed else "legacy",
        "finalizer_seconds": finalizer_seconds,
        "finalizer_phase_timeline": phase_timeline,
        "publication_seconds": publication_seconds,
        "summary_seconds": summary_seconds,
        "parity_oracle_seconds": parity_oracle_seconds,
        "parity_oracle_reused_timed_summary": not request.packed,
        "publication_plus_summary_seconds": whole_seconds,
        "finalizer_rows_per_second": (
            request.artifacts.shape.finalizer_mapping_count / finalizer_seconds
        ),
        "publication_plus_summary_rows_per_second": (
            request.artifacts.shape.mapping_count / whole_seconds
        ),
        "finalizer_publication": _jsonable(asdict(finalizer_result.publication)),
        "finalizer_copy_manifest": _jsonable(copy_manifest),
        "price_publication": _jsonable(asdict(price_result)),
        "timed_summary": timed_summary_by_field,
        "summary": summary_by_field,
        "persisted": persisted,
    }


async def is_arm_schema_removed(schema_name: str) -> bool:
    """Drop one exact benchmark schema and prove its namespace is absent."""

    if not SCHEMA_RE.fullmatch(schema_name):
        raise ValueError("ABBA cleanup schema name is not task-scoped")
    await db.status(f"DROP SCHEMA IF EXISTS {_quote_ident(schema_name)} CASCADE")
    return await db.scalar("SELECT to_regnamespace(:schema_name)", schema_name=schema_name) is None


__all__ = (
    "ArmRequest",
    "CANONICAL_FIELDS",
    "NATIVE_SUMMARY_FIELDS",
    "install_arm_schema",
    "inspect_arm_state",
    "is_arm_schema_removed",
    "prepare_arm_schema",
    "reserve_arm_layout",
    "run_packed_failure_probe",
    "run_production_arm",
)
