"""Bounded real-PostgreSQL proof through the finalizer wrapper and summary."""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from db.connection import db
from process.ptg_parts import ptg2_v4_finalizer_publish
from process.ptg_parts.ptg2_lifecycle_lock import PTG2_SOURCE_POINTER_GC_LOCK_KEY
from scripts.research.ptg2_packed_finalizer_abba_lifecycle import (
    ArmRequest,
    NATIVE_SUMMARY_FIELDS,
    inspect_arm_state,
    is_arm_schema_removed,
    prepare_arm_schema,
    run_packed_failure_probe,
    run_production_arm,
)
from scripts.research.ptg2_packed_finalizer_abba_contract import (
    ALL_OBJECT_KINDS,
    BenchmarkShape,
    PRICE_OBJECT_KINDS,
    SHAPE_CONTRACT,
    SYNTHETIC_CLASSIFICATION,
)
from scripts.research.ptg2_packed_finalizer_abba_artifacts import generate_artifacts
from tests.ptg2_v4_stale_metadata_postgres_support import postgres_dsn


def _configure_test_database(monkeypatch, dsn: str) -> None:
    url = make_url(dsn)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host or "127.0.0.1"))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username or "postgres"))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database))
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "8")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "1073741824")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "1073741824")


def _tiny_shape() -> BenchmarkShape:
    return BenchmarkShape.from_mapping(
        {
            "contract": SHAPE_CONTRACT,
            "classification": SYNTHETIC_CLASSIFICATION,
            "allocation_by_kind": {
                kind: {
                    "mapping_count": 1 if kind in PRICE_OBJECT_KINDS else 2,
                    "unique_block_count": 1,
                }
                for kind in ALL_OBJECT_KINDS
            },
        }
    )


def _failure_settings(monkeypatch, failure_mode):
    callback = None
    if failure_mode == "cancel":
        def _cancel_after_cas(metric, _amount):
            if metric == "finalizer_cas_published":
                raise asyncio.CancelledError

        callback = _cancel_after_cas
        expected_error = asyncio.CancelledError
    elif failure_mode == "terminal_callback":
        def _fail_before_commit(metric, _amount):
            if metric == "finalizer_map_attached":
                raise RuntimeError("synthetic terminal callback failure")

        callback = _fail_before_commit
        expected_error = RuntimeError
    elif failure_mode == "ownership_fence_loss":
        monkeypatch.setattr(
            ptg2_v4_finalizer_publish,
            "is_pin_lease_renewed",
            AsyncMock(side_effect=(True, False)),
        )
        expected_error = RuntimeError
    else:
        expected_error = RuntimeError
    return callback, expected_error


@asynccontextmanager
async def _prepared_arm(monkeypatch, tmp_path):
    dsn = postgres_dsn()
    _configure_test_database(monkeypatch, dsn)
    token = uuid.uuid4().hex[:12]
    schema_name = f"ptg_packed_abba_{token}_b1"
    work_directory = tmp_path / "work"
    work_directory.mkdir()
    artifacts = generate_artifacts(tmp_path / "artifacts", _tiny_shape())
    await db.disconnect()
    try:
        await db.connect()
        snapshot_key = await prepare_arm_schema(
            dsn,
            schema_name=schema_name,
            build_token=f"packed-abba-{token}-b1",
            shape_sha256=artifacts.shape.sha256(),
        )
        yield dsn, ArmRequest(
            "b1",
            True,
            schema_name,
            snapshot_key,
            f"packed-abba-{token}-b1",
            work_directory,
            artifacts,
        )
    finally:
        is_schema_removed = (
            await is_arm_schema_removed(schema_name) if db.engine is not None else False
        )
        await db.disconnect()
        artifacts.cleanup()
        work_directory.rmdir()
        assert is_schema_removed
        assert not artifacts.directory.exists()


@pytest.mark.asyncio
async def test_packed_wrapper_summary_and_cleanup_on_postgres(monkeypatch, tmp_path):
    """Prove packed publication, canonical summary parity, and exact cleanup."""

    async with _prepared_arm(monkeypatch, tmp_path) as (_dsn, request):
        arm = await run_production_arm(request)
        artifacts = request.artifacts
        expected_summary_by_field = {
            field: summary_value
            for field, summary_value in artifacts.expected_summary.items()
            if field not in {"map_pack_count", "packed_canonical_byte_count"}
        }
        assert arm["summary"] == expected_summary_by_field
        assert arm["timed_summary"] == {
            field: artifacts.expected_summary[field]
            for field in NATIVE_SUMMARY_FIELDS
        }
        assert arm["parity_oracle_seconds"] > 0
        assert arm["parity_oracle_reused_timed_summary"] is False
        assert arm["finalizer_publication"]["contract"] == "packed_finalizer_map_v2"
        assert (
            arm["finalizer_copy_manifest"]["contract"]
            == "native_unique_shared_block_copy_v2"
        )
        assert arm["persisted"] == {
            "root_rows": 1,
            "pack_rows": 6,
            "target_rows": 6,
            "relational_rows": 2,
            "pin_rows": 0,
            "gc_rows": 0,
            "cas_rows": 14,
            "stage_tables_present": 0,
        }
        assert [entry["metric"] for entry in arm["finalizer_phase_timeline"]] == [
            "finalizer_sidecars_staged",
            "finalizer_pins_prepared",
            "finalizer_cas_published",
            "finalizer_map_rows_attached",
            "finalizer_map_attached",
            "finalizer_complete",
        ]
        assert arm["finalizer_phase_timeline"][-1]["elapsed_seconds"] == pytest.approx(
            arm["finalizer_seconds"]
        )
        assert all(
            entry["phase_seconds"] >= 0
            for entry in arm["finalizer_phase_timeline"]
        )
        assert not any(request.work_directory.iterdir())


@pytest.mark.asyncio
async def test_atomic_publish_holds_gc_fence_until_cancel_rollback(
    monkeypatch,
    tmp_path,
):
    """Prove cancellation rolls back CAS while the GC fence is held."""
    reached_cas_fence = asyncio.Event()
    release_fence = asyncio.Event()
    task = None
    real_renew = ptg2_v4_finalizer_publish.is_pin_lease_renewed
    renew_counts = [0]

    async def pause_after_cas(*args, **kwargs):
        renewed = await real_renew(*args, **kwargs)
        renew_counts[0] += 1
        if renew_counts[0] == 1:
            reached_cas_fence.set()
            await release_fence.wait()
        return renewed

    monkeypatch.setattr(
        ptg2_v4_finalizer_publish,
        "is_pin_lease_renewed",
        pause_after_cas,
    )
    async with _prepared_arm(monkeypatch, tmp_path) as (dsn, request):
        try:
            callback, _expected_error = _failure_settings(monkeypatch, "cancel")
            task = asyncio.create_task(run_packed_failure_probe(request, callback))
            await asyncio.wait_for(reached_cas_fence.wait(), timeout=5)
            connection = await asyncpg.connect(dsn)
            try:
                acquired_gc = await connection.fetchval(
                    "SELECT pg_try_advisory_lock(hashtext($1))",
                    PTG2_SOURCE_POINTER_GC_LOCK_KEY,
                )
                if acquired_gc:
                    await connection.execute(
                        "SELECT pg_advisory_unlock(hashtext($1))",
                        PTG2_SOURCE_POINTER_GC_LOCK_KEY,
                    )
                assert acquired_gc is False
                assert await connection.fetchval(
                    f'SELECT COUNT(*) FROM "{request.schema_name}".ptg2_v3_block'
                ) == 0
            finally:
                await connection.close()
            release_fence.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert await inspect_arm_state(request) == _empty_arm_state()
        finally:
            release_fence.set()
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


def _empty_arm_state() -> dict[str, int]:
    return {
        "root_rows": 0,
        "pack_rows": 0,
        "target_rows": 0,
        "relational_rows": 0,
        "pin_rows": 0,
        "gc_rows": 0,
        "cas_rows": 0,
        "stage_tables_present": 0,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode",
    ("cancel", "ownership_fence_loss", "stale_build_token", "terminal_callback"),
)
async def test_packed_wrapper_failure_rolls_back_cas_and_map(
    monkeypatch,
    tmp_path,
    failure_mode,
):
    """Prove every supported failure boundary leaves zero durable residue."""
    async with _prepared_arm(monkeypatch, tmp_path) as (_dsn, prepared_request):
        request = ArmRequest(
            prepared_request.label,
            prepared_request.packed,
            prepared_request.schema_name,
            prepared_request.snapshot_key,
            "stale-build-token"
            if failure_mode == "stale_build_token"
            else prepared_request.build_token,
            prepared_request.work_directory,
            prepared_request.artifacts,
        )
        callback, expected_error = _failure_settings(monkeypatch, failure_mode)
        with pytest.raises(
            expected_error,
            match={
                "cancel": None,
                "ownership_fence_loss": "heartbeat lost ownership",
                "stale_build_token": "lost build ownership",
                "terminal_callback": "terminal callback failure",
            }[failure_mode],
        ):
            await run_packed_failure_probe(request, callback)
        assert await inspect_arm_state(request) == _empty_arm_state()
        assert not any(request.work_directory.iterdir())
