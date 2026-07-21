# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed native retention boundary for UHC provider and plan JSON."""

from __future__ import annotations

import asyncio
import json
import math
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from process.ptg_parts.config import PTG2_RUST_SCANNER_BIN_ENV
from process.ptg_parts.rust_scanner import (
    _await_cancellation_resistant_cleanup,
    _subprocess_session_options,
)
from process.uhc_retained_native_process import (
    cleanup_native_process as _cleanup_native_process,
    collect_process_output as _collect_process_output,
)
from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
    RANGE_CONTRACT_VERSION,
    load_verified_range_manifest,
    range_manifest_path,
    retained_raw_path,
)
from process.uhc_retained_types import (
    UHCRetainedAdmissionError,
    VerifiedRetainedSource,
    _reject_duplicate_keys,
    _validate_artifact_identity,
    _validate_range_count,
)


_SUMMARY_FIELDS = {
    "record_kind",
    "contract_id",
    "contract_version",
    "canonicalization_id",
    "producer_build_id",
    "verifier_build_id",
    "raw_artifact_path",
    "raw_artifact_sha256",
    "raw_artifact_byte_count",
    "record_count",
    "range_count",
    "manifest_path",
    "manifest_sha256",
    "manifest_byte_count",
    "raw_reused",
    "manifest_reused",
    "timings_seconds",
}
_SUMMARY_RECORD_KIND = "uhc_retained_summary"
_MAX_STDOUT_BYTES = 1024 * 1024
_MAX_STDERR_BYTES = 64 * 1024
_DEFAULT_TIMEOUT_SECONDS = 60 * 60
_MAX_TIMING_FIELDS = 32
_ATTESTATION_AUTHORITY = object()


FileIdentity = tuple[str, int, int, int, int, int, int, int]


@dataclass
class _NativeAttestation:
    authority: object
    proof_identity: tuple[object, ...]
    file_identities: tuple[FileIdentity, FileIdentity]
    consumed: bool = False


@dataclass(frozen=True)
class _NativeSummaryProof:
    producer_build_id: str
    verifier_build_id: str
    record_count: int
    manifest_sha256: str
    manifest_byte_count: int
    raw_path: Path
    manifest_path: Path
    raw_reused: bool
    manifest_reused: bool
    timings: tuple[tuple[str, float], ...]


def _proof_identity(source: VerifiedRetainedSource) -> tuple[object, ...]:
    return (
        source.raw_artifact,
        source.ranges,
        source.raw_reused,
        source.manifest_reused,
        source.verifier_build_id,
        source.timings_seconds,
    )


def consume_native_attestation(
    source: VerifiedRetainedSource,
) -> tuple[FileIdentity, FileIdentity]:
    """Consume a private one-use proof before one database transaction."""

    attestation = source.attestation
    if (
        not isinstance(attestation, _NativeAttestation)
        or attestation.authority is not _ATTESTATION_AUTHORITY
        or attestation.consumed
        or attestation.proof_identity != _proof_identity(source)
    ):
        raise UHCRetainedAdmissionError(
            "retained UHC source lacks a fresh native attestation"
        )
    attestation.consumed = True
    return attestation.file_identities


def is_native_verified_source(source: object) -> bool:
    """Return whether a source carries an unconsumed, unmodified native proof."""

    if not isinstance(source, VerifiedRetainedSource):
        return False
    attestation = source.attestation
    return bool(
        isinstance(attestation, _NativeAttestation)
        and attestation.authority is _ATTESTATION_AUTHORITY
        and not attestation.consumed
        and attestation.proof_identity == _proof_identity(source)
    )


def _file_identity(path: Path) -> FileIdentity:
    try:
        path_stat = os.stat(path, follow_symlinks=False)
    except OSError as error:
        raise UHCRetainedAdmissionError(
            "native-retained UHC source is unavailable"
        ) from error
    if (
        not stat.S_ISREG(path_stat.st_mode)
        or path_stat.st_nlink != 1
        or path_stat.st_mode & 0o022
    ):
        raise UHCRetainedAdmissionError(
            "native-retained UHC source permissions or link count are unsafe"
        )
    return (
        str(path),
        path_stat.st_dev,
        path_stat.st_ino,
        path_stat.st_size,
        path_stat.st_mtime_ns,
        path_stat.st_ctime_ns,
        path_stat.st_mode,
        path_stat.st_nlink,
    )


def _native_binary() -> Path:
    configured_binary = os.getenv(PTG2_RUST_SCANNER_BIN_ENV)
    if not configured_binary:
        raise UHCRetainedAdmissionError(
            "UHC retained admission requires HLTHPRT_PTG2_RUST_SCANNER_BIN"
        )
    binary = Path(configured_binary).resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise UHCRetainedAdmissionError(
            "configured UHC retained native scanner is unavailable"
        )
    return binary


def _timeout_seconds() -> float:
    configured = os.getenv("HLTHPRT_UHC_RETAIN_TIMEOUT_SECONDS")
    if configured is None:
        return float(_DEFAULT_TIMEOUT_SECONDS)
    try:
        timeout = float(configured)
    except ValueError as error:
        raise UHCRetainedAdmissionError(
            "UHC native retention timeout is invalid"
        ) from error
    if not math.isfinite(timeout) or timeout < 1 or timeout > 24 * 60 * 60:
        raise UHCRetainedAdmissionError("UHC native retention timeout is invalid")
    return timeout


async def _run_native(arguments: tuple[str, ...]) -> bytes:
    process: asyncio.subprocess.Process | None = None
    spawn_task = asyncio.create_task(
        asyncio.create_subprocess_exec(
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **_subprocess_session_options(asyncio.create_subprocess_exec),
        )
    )
    try:
        process = await asyncio.shield(spawn_task)
        stdout, stderr, return_code = await asyncio.wait_for(
            _collect_process_output(
                process,
                stdout_limit=_MAX_STDOUT_BYTES,
                stderr_limit=_MAX_STDERR_BYTES,
            ),
            timeout=_timeout_seconds(),
        )
        if return_code != 0:
            stderr_tail = stderr.decode("utf-8", errors="replace")[-2000:]
            raise UHCRetainedAdmissionError(
                "UHC native retention failed "
                f"with exit code {return_code}: {stderr_tail}"
            )
        return stdout
    except BaseException as error:
        cleanup_task = asyncio.create_task(
            _cleanup_native_process(process, spawn_task)
        )
        await _await_cancellation_resistant_cleanup(cleanup_task)
        if isinstance(error, TimeoutError):
            raise UHCRetainedAdmissionError(
                "UHC native retention timed out"
            ) from error
        if isinstance(error, OSError):
            raise UHCRetainedAdmissionError(
                "UHC native retention could not start"
            ) from error
        raise


def _required_string(summary: dict[str, object], field: str) -> str:
    value = summary.get(field)
    if not isinstance(value, str) or not value:
        raise UHCRetainedAdmissionError(
            f"UHC native retention summary {field} is invalid"
        )
    return value


def _required_integer(
    summary: dict[str, object],
    field: str,
    *,
    minimum: int = 1,
) -> int:
    value = summary.get(field)
    if type(value) is not int or value < minimum or value > 2**63 - 1:
        raise UHCRetainedAdmissionError(
            f"UHC native retention summary {field} is invalid"
        )
    return value


def _build_id(summary: dict[str, object], field: str) -> str:
    build_id = _required_string(summary, field)
    if (
        len(build_id) > 256
        or not build_id.isascii()
        or not build_id.isprintable()
    ):
        raise UHCRetainedAdmissionError(
            f"UHC native retention summary {field} is invalid"
        )
    return build_id


def _timings(summary: dict[str, object]) -> tuple[tuple[str, float], ...]:
    timing_map = summary.get("timings_seconds")
    if not isinstance(timing_map, dict) or len(timing_map) > _MAX_TIMING_FIELDS:
        raise UHCRetainedAdmissionError(
            "UHC native retention summary timings are invalid"
        )
    timings = []
    for label, value in timing_map.items():
        if (
            not isinstance(label, str)
            or not label
            or len(label) > 64
            or not label.isascii()
            or not label.isprintable()
            or isinstance(value, bool)
            or not isinstance(value, (float, int))
            or not math.isfinite(float(value))
            or value < 0
            or value > 24 * 60 * 60
        ):
            raise UHCRetainedAdmissionError(
                "UHC native retention summary timings are invalid"
            )
        timings.append((label, float(value)))
    return tuple(sorted(timings))


def _strict_summary(encoded_summary: bytes) -> dict[str, object]:
    def reject_non_json_constant(value: str) -> None:
        """Reject the non-standard numeric constants accepted by json.loads."""

        raise ValueError(f"invalid JSON constant: {value}")

    try:
        summary = json.loads(
            encoded_summary.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=reject_non_json_constant,
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise UHCRetainedAdmissionError(
            "UHC native retention summary is invalid"
        ) from error
    if not isinstance(summary, dict) or set(summary) != _SUMMARY_FIELDS:
        raise UHCRetainedAdmissionError(
            "UHC native retention summary shape is invalid"
        )
    return summary


def _validated_native_paths(
    source_path: str | Path,
    output_root: str | Path,
) -> tuple[Path, Path]:
    source_file_path = Path(os.path.abspath(source_path))
    retained_root = Path(os.path.abspath(output_root))
    try:
        source_stat = os.stat(source_file_path, follow_symlinks=False)
        root_stat = os.stat(retained_root, follow_symlinks=False)
    except OSError as error:
        raise UHCRetainedAdmissionError(
            "UHC native retention input is unavailable"
        ) from error
    if not stat.S_ISREG(source_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise UHCRetainedAdmissionError("UHC native retention input is invalid")
    return source_file_path, retained_root


def _validate_summary_contract(
    summary: dict[str, object],
    *,
    expected_sha256: str,
    expected_byte_count: int,
    range_count: int,
) -> None:
    if (
        summary["record_kind"] != _SUMMARY_RECORD_KIND
        or summary["contract_id"] != RANGE_CONTRACT_ID
        or summary["contract_version"] != RANGE_CONTRACT_VERSION
        or summary["canonicalization_id"] != RANGE_CANONICALIZATION_ID
        or summary["raw_artifact_sha256"] != expected_sha256
        or summary["raw_artifact_byte_count"] != expected_byte_count
        or summary["range_count"] != range_count
        or type(summary["raw_reused"]) is not bool
        or type(summary["manifest_reused"]) is not bool
    ):
        raise UHCRetainedAdmissionError(
            "UHC native retention summary contract is incompatible"
        )


def _summary_proof(
    summary: dict[str, object],
    *,
    retained_root: Path,
    expected_sha256: str,
    range_count: int,
) -> _NativeSummaryProof:
    raw_path = Path(os.path.abspath(_required_string(summary, "raw_artifact_path")))
    manifest_path = Path(os.path.abspath(_required_string(summary, "manifest_path")))
    if raw_path != retained_raw_path(retained_root, expected_sha256) or (
        manifest_path
        != range_manifest_path(retained_root, expected_sha256, range_count)
    ):
        raise UHCRetainedAdmissionError(
            "UHC native retention returned a noncanonical artifact path"
        )
    return _NativeSummaryProof(
        producer_build_id=_build_id(summary, "producer_build_id"),
        verifier_build_id=_build_id(summary, "verifier_build_id"),
        record_count=_required_integer(summary, "record_count"),
        manifest_sha256=_required_string(summary, "manifest_sha256"),
        manifest_byte_count=_required_integer(summary, "manifest_byte_count"),
        raw_path=raw_path,
        manifest_path=manifest_path,
        raw_reused=bool(summary["raw_reused"]),
        manifest_reused=bool(summary["manifest_reused"]),
        timings=_timings(summary),
    )


async def _load_attested_source(
    proof: _NativeSummaryProof,
    *,
    expected_sha256: str,
    expected_byte_count: int,
    range_count: int,
) -> VerifiedRetainedSource:
    identities_before = (
        _file_identity(proof.raw_path),
        _file_identity(proof.manifest_path),
    )
    raw_artifact, retained_ranges = await asyncio.to_thread(
        load_verified_range_manifest,
        raw_path=proof.raw_path,
        manifest_path=proof.manifest_path,
        expected_artifact_sha256=expected_sha256,
        expected_artifact_bytes=expected_byte_count,
        expected_manifest_sha256=proof.manifest_sha256,
        expected_manifest_bytes=proof.manifest_byte_count,
        expected_range_count=range_count,
        producer_build_id=proof.producer_build_id,
        verify_raw_bytes=False,
    )
    if raw_artifact.record_count != proof.record_count:
        raise UHCRetainedAdmissionError(
            "UHC native retention summary record count does not match manifest"
        )
    identities_after = (
        _file_identity(proof.raw_path),
        _file_identity(proof.manifest_path),
    )
    if identities_after != identities_before:
        raise UHCRetainedAdmissionError(
            "native-retained UHC source changed during proof loading"
        )
    attestation = _NativeAttestation(
        authority=_ATTESTATION_AUTHORITY,
        proof_identity=(),
        file_identities=identities_after,
    )
    verified_source = VerifiedRetainedSource(
        raw_artifact=raw_artifact,
        ranges=retained_ranges,
        raw_reused=proof.raw_reused,
        manifest_reused=proof.manifest_reused,
        verifier_build_id=proof.verifier_build_id,
        timings_seconds=proof.timings,
        attestation=attestation,
    )
    attestation.proof_identity = _proof_identity(verified_source)
    return verified_source


async def retain_source_native(
    *,
    source_path: str | Path,
    output_root: str | Path,
    expected_sha256: str,
    expected_byte_count: int,
    range_count: int,
) -> VerifiedRetainedSource:
    """Build or verify one retained raw/range layout with the native scanner."""

    _validate_artifact_identity(expected_sha256, expected_byte_count)
    _validate_range_count(range_count)
    source_file_path, retained_root = _validated_native_paths(
        source_path,
        output_root,
    )
    encoded_summary = await _run_native(
        (
            str(_native_binary()),
            "--uhc-retain",
            str(source_file_path),
            str(retained_root),
            expected_sha256,
            str(expected_byte_count),
            str(range_count),
        )
    )
    summary = _strict_summary(encoded_summary)
    _validate_summary_contract(
        summary,
        expected_sha256=expected_sha256,
        expected_byte_count=expected_byte_count,
        range_count=range_count,
    )
    proof = _summary_proof(
        summary,
        retained_root=retained_root,
        expected_sha256=expected_sha256,
        range_count=range_count,
    )
    return await _load_attested_source(
        proof,
        expected_sha256=expected_sha256,
        expected_byte_count=expected_byte_count,
        range_count=range_count,
    )
