#!/usr/bin/env python3
"""ABBA mechanism screen through the real PTG finalizer publisher and summarizer."""

from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import tempfile
import time
import uuid
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import AsyncMock, patch

from db.connection import db
from process.ptg_parts import ptg2_v4_finalizer_publish
from process.ptg_parts.ptg2_shared_finalize import (
    _load_v3_finalizer_resource_configuration,
)
from scripts.research.ptg2_packed_finalizer_abba_contract import (
    BenchmarkArtifacts,
    _mechanism_gates,
    failure_probe_shape,
    load_shape,
)
from scripts.research.ptg2_packed_finalizer_abba_artifacts import (
    generate_artifacts,
)
from scripts.research.ptg2_packed_finalizer_abba_inputs import (
    load_representative_artifacts,
)
from scripts.research.ptg2_packed_finalizer_abba_environment import (
    DSN_ENV,
    OPT_IN_ENV,
    assert_source_identity_unchanged,
    capture_source_identity,
    configure_database,
    verify_benchmark_environment,
)
from scripts.research.ptg2_packed_finalizer_abba_lifecycle import (
    ArmRequest,
    install_arm_schema,
    inspect_arm_state,
    is_arm_schema_removed,
    reserve_arm_layout,
    run_packed_failure_probe,
    run_production_arm,
)
from scripts.research.ptg2_packed_finalizer_abba_receipt import (
    RECEIPT_CONTRACT,
    _completed_arm_receipt,
    initial_receipt,
    write_receipt,
)


ABBA_ARMS = (("a1", False), ("b1", True), ("b2", True), ("a2", False))


def _record_error(receipt: dict[str, Any], phase: str, exc: BaseException) -> None:
    receipt["status"] = "failed"
    receipt.setdefault("errors", []).append(
        {"phase": phase, "type": type(exc).__name__, "message": str(exc)}
    )


async def _has_arm_cleanup_failure(
    receipt: dict[str, Any],
    *,
    label: str,
    schema_name: str,
    work_directory: Path,
) -> bool:
    has_cleanup_failure = False
    try:
        is_schema_removed = await is_arm_schema_removed(schema_name)
        receipt["cleanup"]["schemas"][schema_name] = is_schema_removed
        has_cleanup_failure = not is_schema_removed
    except BaseException as exc:
        receipt["cleanup"]["schemas"][schema_name] = False
        _record_error(receipt, f"cleanup_schema_{label}", exc)
        has_cleanup_failure = True
    unexpected_residue_names: list[str] = []
    try:
        unexpected_residue_names = sorted(
            path.name for path in work_directory.iterdir()
        )
    except BaseException as exc:
        _record_error(receipt, f"inspect_work_directory_{label}", exc)
        has_cleanup_failure = True
    try:
        shutil.rmtree(work_directory)
    except BaseException as exc:
        _record_error(receipt, f"cleanup_work_directory_{label}", exc)
        has_cleanup_failure = True
    receipt["cleanup"]["work_directories"][label] = {
        "removed": not work_directory.exists(),
        "unexpected_residue": unexpected_residue_names,
    }
    return (
        has_cleanup_failure
        or bool(unexpected_residue_names)
        or work_directory.exists()
    )


async def _run_arm(
    dsn: str, root: Path, run_token: str, label: str, packed: bool,
    artifacts: BenchmarkArtifacts, receipt: dict[str, Any],
) -> None:
    """Run one timed arm and prove its exact cleanup."""

    schema_name = f"ptg_packed_abba_{run_token}_{label}"
    build_token = f"packed-abba-{run_token}-{label}"
    work_directory = root / f"work-{label}"
    work_directory.mkdir()
    arm_error: BaseException | None = None
    request: ArmRequest | None = None
    try:
        fixture_started_at = time.monotonic()
        await install_arm_schema(dsn, schema_name=schema_name)
        fixture_seconds = time.monotonic() - fixture_started_at
        prepare_started_at = time.monotonic()
        snapshot_key = await reserve_arm_layout(
            schema_name=schema_name,
            build_token=build_token,
            shape_sha256=artifacts.shape.sha256(),
        )
        prepare_seconds = time.monotonic() - prepare_started_at
        request = ArmRequest(
            label, packed, schema_name, snapshot_key,
            build_token, work_directory, artifacts,
        )
        arm_receipt = _completed_arm_receipt(
            await run_production_arm(request),
            fixture_seconds=fixture_seconds,
            prepare_seconds=prepare_seconds,
            mapping_count=artifacts.shape.mapping_count,
        )
        receipt["arms"].append(arm_receipt)
    except BaseException as exc:
        arm_error = exc
        if request is not None:
            try:
                receipt.setdefault("failure_residue", {})[label] = (
                    await inspect_arm_state(request)
                )
            except BaseException as inspection_exc:
                _record_error(
                    receipt,
                    f"inspect_failure_residue_{label}",
                    inspection_exc,
                )
    has_cleanup_failure = await _has_arm_cleanup_failure(
        receipt,
        label=label,
        schema_name=schema_name,
        work_directory=work_directory,
    )
    if arm_error is not None:
        raise arm_error
    if has_cleanup_failure:
        raise RuntimeError(f"ABBA {label} cleanup was incomplete")


def _cancel_after_cas(metric: str, _amount: int) -> None:
    if metric == "finalizer_cas_published":
        raise asyncio.CancelledError


def _fail_before_commit(metric: str, _amount: int) -> None:
    if metric == "finalizer_map_attached":
        raise RuntimeError("synthetic terminal callback failure")


def _failure_probe_settings(mode: str) -> tuple[Any, type[BaseException], Any]:
    if mode == "cancel":
        return _cancel_after_cas, asyncio.CancelledError, nullcontext()
    if mode == "ownership_fence_loss":
        lease_patch = patch.object(
            ptg2_v4_finalizer_publish,
            "is_pin_lease_renewed",
            new=AsyncMock(side_effect=(True, False)),
        )
        return None, RuntimeError, lease_patch
    if mode == "terminal_callback":
        return _fail_before_commit, RuntimeError, nullcontext()
    if mode == "stale_build_token":
        return None, RuntimeError, nullcontext()
    raise ValueError("ABBA failure probe mode is invalid")


def _assert_failure_probe_residue(
    state_by_field: Mapping[str, Any],
    work_directory: Path,
    mode: str,
) -> None:
    expected_state_by_field = {
        "root_rows": 0,
        "pack_rows": 0,
        "target_rows": 0,
        "relational_rows": 0,
        "pin_rows": 0,
        "gc_rows": 0,
        "cas_rows": 0,
        "stage_tables_present": 0,
    }
    if state_by_field != expected_state_by_field or any(work_directory.iterdir()):
        raise RuntimeError(f"ABBA {mode} failure residue changed")


async def _execute_failure_case(
    request: ArmRequest,
    mode: str,
    work_directory: Path,
) -> tuple[BaseException, dict[str, Any]]:
    callback, expected_error, lease_patch = _failure_probe_settings(mode)
    probe_error: BaseException | None = None
    with lease_patch:
        try:
            await run_packed_failure_probe(request, callback)
        except BaseException as exc:
            if not isinstance(exc, expected_error):
                raise
            if (
                mode == "ownership_fence_loss"
                and "heartbeat lost ownership" not in str(exc)
            ):
                raise
            if (
                mode == "stale_build_token"
                and "lost build ownership" not in str(exc)
            ):
                raise
            if (
                mode == "terminal_callback"
                and "terminal callback failure" not in str(exc)
            ):
                raise
            probe_error = exc
    if probe_error is None:
        raise RuntimeError(f"ABBA {mode} probe did not fail")
    state_by_field = await inspect_arm_state(request)
    _assert_failure_probe_residue(
        state_by_field,
        work_directory,
        mode,
    )
    return probe_error, state_by_field


async def _run_failure_case(
    dsn: str,
    root: Path,
    mode: str,
    artifacts: BenchmarkArtifacts,
    receipt: dict[str, Any],
) -> dict[str, Any]:
    token = uuid.uuid4().hex[:12]
    schema_name = f"ptg_packed_abba_{token}_b1"
    build_token = f"packed-abba-{token}-b1"
    cleanup_label = f"probe_{mode}"
    work_directory = root / f"work-{cleanup_label}"
    work_directory.mkdir()
    probe_error: BaseException | None = None
    state_by_field: dict[str, Any] | None = None
    try:
        await install_arm_schema(dsn, schema_name=schema_name)
        snapshot_key = await reserve_arm_layout(
            schema_name=schema_name,
            build_token=build_token,
            shape_sha256=artifacts.shape.sha256(),
        )
        request = ArmRequest(
            "b1",
            True,
            schema_name,
            snapshot_key,
            "stale-build-token" if mode == "stale_build_token" else build_token,
            work_directory,
            artifacts,
        )
        probe_error, state_by_field = await _execute_failure_case(
            request,
            mode,
            work_directory,
        )
    finally:
        cleanup_failed = await _has_arm_cleanup_failure(
            receipt,
            label=cleanup_label,
            schema_name=schema_name,
            work_directory=work_directory,
        )
        if cleanup_failed and probe_error is not None:
            raise RuntimeError(f"ABBA {mode} probe cleanup was incomplete")
    return {
        "mode": mode,
        "error_type": type(probe_error).__name__,
        "state_before_cleanup": state_by_field,
        "cleanup_complete": not cleanup_failed,
    }


async def _run_failure_probes(
    dsn: str,
    root: Path,
    receipt: dict[str, Any],
) -> None:
    artifacts = generate_artifacts(root / "failure-artifacts", failure_probe_shape())
    try:
        receipt["failure_probes"] = [
            await _run_failure_case(dsn, root, mode, artifacts, receipt)
            for mode in (
                "cancel",
                "ownership_fence_loss",
                "stale_build_token",
                "terminal_callback",
            )
        ]
    finally:
        artifacts.cleanup()
    receipt["cleanup"]["failure_artifact_directory_removed"] = not (
        root / "failure-artifacts"
    ).exists()


async def _cleanup_run(
    receipt: dict[str, Any],
    *,
    root: Path,
    artifacts: BenchmarkArtifacts | None,
) -> None:
    if artifacts is not None:
        try:
            artifacts.cleanup()
        except BaseException as exc:
            _record_error(receipt, "cleanup_artifacts", exc)
    try:
        shutil.rmtree(root)
    except BaseException as exc:
        _record_error(receipt, "cleanup_local_root", exc)
    receipt["cleanup"]["local_root_removed"] = not root.exists()
    receipt["cleanup"]["artifact_directory_removed"] = not (
        root / "artifacts"
    ).exists()
    receipt["cleanup"]["external_artifacts_preserved"] = None
    if artifacts is not None and not artifacts.owned_by_run:
        try:
            artifacts.assert_external_inputs_unchanged()
        except BaseException as exc:
            receipt["cleanup"]["external_artifacts_preserved"] = False
            _record_error(receipt, "reauthenticate_external_inputs", exc)
        else:
            receipt["cleanup"]["external_artifacts_preserved"] = True
    for label, state in receipt["cleanup"]["work_directories"].items():
        state["removed"] = not (root / f"work-{label}").exists()
    try:
        await db.disconnect()
    except BaseException as exc:
        _record_error(receipt, "disconnect_database", exc)


async def _run_abba(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if os.getenv(OPT_IN_ENV) != "1":
        raise RuntimeError(f"set {OPT_IN_ENV}=1 to authorize the disposable ABBA run")
    dsn = os.environ[DSN_ENV]
    dsn_identity = configure_database(dsn)
    artifacts = (
        load_representative_artifacts(args.artifacts, args.source_receipt)
        if args.artifacts is not None and args.source_receipt is not None
        else None
    )
    if args.artifacts is not None and artifacts is None:
        raise ValueError("--artifacts requires --source-receipt")
    if args.source_receipt is not None and args.artifacts is None:
        raise ValueError("--source-receipt requires --artifacts")
    shape = artifacts.shape if artifacts is not None else load_shape(args.shape)
    run_token = uuid.uuid4().hex[:12]
    root = Path(tempfile.mkdtemp(prefix=f"ptg-packed-abba-{run_token}-"))
    harness_path = Path(__file__)
    source_identity = capture_source_identity(harness_path)
    receipt = initial_receipt(shape, source_identity)
    resource_configuration = _load_v3_finalizer_resource_configuration()
    receipt["finalizer_resource_configuration"] = {
        **resource_configuration.contract_metadata(),
        **resource_configuration.validation_metadata(),
    }
    exit_code = 1
    try:
        await db.connect()
        receipt["environment"] = await verify_benchmark_environment(dsn_identity)
        if artifacts is None:
            artifacts = generate_artifacts(root / "artifacts", shape)
        receipt["artifact_input"] = {
            "manifest_sha256": artifacts.manifest_sha256,
            "owned_by_run": artifacts.owned_by_run,
            "source_receipt_sha256": artifacts.source_receipt_sha256,
        }
        await _run_failure_probes(dsn, root, receipt)
        receipt["release_blockers"] = tuple(
            blocker
            for blocker in receipt["release_blockers"]
            if blocker != "runtime failure probes not executed by this receipt"
        )
        for label, packed in ABBA_ARMS:
            assert_source_identity_unchanged(harness_path, source_identity)
            receipt["source_boundaries"].append(f"{label}:before")
            await _run_arm(dsn, root, run_token, label, packed, artifacts, receipt)
            assert_source_identity_unchanged(harness_path, source_identity)
            receipt["source_boundaries"].append(f"{label}:after")
        assert_source_identity_unchanged(harness_path, source_identity)
        receipt["source_boundaries"].append("terminal")
        receipt["gates"] = _mechanism_gates(receipt["arms"])
        receipt["status"] = "complete" if receipt["gates"]["passed"] else "gate_failed"
        exit_code = 0 if receipt["gates"]["passed"] else 2
    except BaseException as exc:
        _record_error(receipt, "run", exc)
    finally:
        await _cleanup_run(receipt, root=root, artifacts=artifacts)
    return receipt, 1 if receipt["status"] == "failed" else exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--shape", type=Path, help="declared synthetic shape JSON")
    inputs.add_argument("--artifacts", type=Path, help="representative artifact manifest")
    parser.add_argument("--source-receipt", type=Path)
    parser.add_argument("--receipt", type=Path, required=True)
    return parser


def main() -> int:
    """Run the opted-in screen and persist a receipt even on failure."""

    args = _parser().parse_args()
    receipt_by_field: dict[str, Any] = {
        "contract": RECEIPT_CONTRACT,
        "status": "failed",
        "accepted": False,
    }
    exit_code = 1
    try:
        receipt_by_field, exit_code = asyncio.run(_run_abba(args))
    except BaseException as exc:
        receipt_by_field["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
    write_receipt(args.receipt.resolve(), receipt_by_field)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
