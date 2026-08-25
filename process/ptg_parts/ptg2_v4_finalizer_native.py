# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One-pass native packing for authenticated V4 finalizer COPY files."""

from __future__ import annotations

import asyncio
import json
import math
from pathlib import Path
from typing import Any, Mapping

from process.ptg_parts.ptg2_shared_finalize import (
    _await_cleanup_task,
    _cleanup_unacknowledged_finalizer_attempt,
    _load_v3_finalizer_resource_configuration,
    _remove_finalizer_attempt_path,
)
from process.ptg_parts.ptg2_v4_finalizer_map_sidecars import (
    PackedMapArtifact,
    PackedMapNativeReceipt,
    PackedMapSidecars,
)
from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    v4_finalizer_map_root_digest,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
)
from process.ptg_parts.rust_scanner import (
    _ptg2_rust_scanner_binary,
    _subprocess_session_options,
    _terminate_asyncio_subprocess_group,
)


_INPUT_CONTRACT = "ptg2_v4_finalizer_pack_input_v1"
_OUTPUT_FORMAT = "ptg2_v4_finalizer_pack_v1"
_LANE_NAMES = ("price_dictionary", "serving")
_OUTPUT_FIELDS = frozenset(
    {
        "format",
        "output_directory",
        "coordinates_per_pack",
        "map_digest",
        "canonical_mapping_digest",
        "canonical_mapping_count",
        "canonical_byte_count",
        "target_identity_digest",
        "target_block_count",
        "object_kinds",
        "lanes",
        "elapsed_seconds",
    }
)


def _count(value: Any, label: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < int(positive):
        raise RuntimeError(f"native packed finalizer {label} is invalid")
    return value


def _sha256(value: Any, label: str) -> str:
    normalized = str(value or "")
    if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
        raise RuntimeError(f"native packed finalizer {label} is invalid")
    return normalized


def _digest_bytes(value: Any, label: str) -> bytes:
    return bytes.fromhex(_sha256(value, label))


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"native packed finalizer {label} is invalid")
    return dict(value)


def _framed_summary(stdout: bytes) -> dict[str, Any]:
    header_end = stdout.find(b"\n")
    if header_end < 0:
        raise RuntimeError("native packed finalizer returned an incomplete frame")
    try:
        kind, raw_length = stdout[:header_end].split(b"\t", 1)
        payload_length = int(raw_length)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("native packed finalizer returned an invalid frame") from exc
    if kind != b"v4_finalizer_pack_summary" or payload_length < 2:
        raise RuntimeError("native packed finalizer returned an unexpected record")
    payload_start = header_end + 1
    payload_end = payload_start + payload_length
    if payload_end > len(stdout) or stdout[payload_end:].strip():
        raise RuntimeError("native packed finalizer returned a malformed summary")
    try:
        payload = json.loads(stdout[payload_start:payload_end])
    except json.JSONDecodeError as exc:
        raise RuntimeError("native packed finalizer returned invalid JSON") from exc
    return _mapping(payload, "summary")


def _block_path(finalizer_summary: Mapping[str, Any], block: Mapping[str, Any]) -> Path:
    output = Path(str(finalizer_summary.get("output_directory") or "")).resolve()
    path = (output / str(block.get("path") or "")).resolve()
    if path.parent != output or not path.is_file():
        raise RuntimeError("native packed finalizer source path is invalid")
    return path


def _input_lane(
    name: str,
    finalizer_summary: Mapping[str, Any],
    block: Mapping[str, Any],
) -> dict[str, Any]:
    counts = _mapping(block.get("artifact_record_counts"), f"{name} kinds")
    object_kinds = tuple(
        sorted(kind for kind, count in counts.items() if _count(count, f"{kind} rows") > 0)
    )
    if not object_kinds:
        raise RuntimeError(f"native packed finalizer {name} lane is empty")
    return {
        "name": name,
        "path": str(_block_path(finalizer_summary, block)),
        "byte_count": _count(block.get("copy_bytes"), f"{name} source bytes", positive=True),
        "sha256": _sha256(block.get("copy_sha256"), f"{name} source sha256"),
        "row_count": _count(block.get("row_count"), f"{name} source rows", positive=True),
        "stored_payload_bytes": _count(
            block.get("stored_payload_bytes"), f"{name} stored payload bytes"
        ),
        "object_kinds": list(object_kinds),
    }


def _artifact(
    raw: Any,
    *,
    lane_directory: Path,
    label: str,
) -> PackedMapArtifact:
    fields = _mapping(raw, label)
    raw_path = Path(str(fields.get("path") or ""))
    path = (lane_directory / raw_path).resolve() if not raw_path.is_absolute() else raw_path.resolve()
    if path.parent != lane_directory.resolve() or not path.is_file():
        raise RuntimeError(f"native packed finalizer {label} path is invalid")
    byte_count = _count(fields.get("byte_count"), f"{label} bytes", positive=True)
    if path.stat().st_size != byte_count:
        raise RuntimeError(f"native packed finalizer {label} size changed")
    return PackedMapArtifact(
        path=path,
        row_count=_count(fields.get("row_count"), f"{label} rows", positive=True),
        byte_count=byte_count,
        sha256=_sha256(fields.get("sha256"), f"{label} sha256"),
    )


def _validated_lane_source(
    fields: Mapping[str, Any],
    *,
    output_directory: Path,
    expected_lane: Mapping[str, Any],
) -> tuple[Path, dict[str, Any], tuple[str, ...], dict[str, Any]]:
    name = str(fields.get("name") or "")
    if name != expected_lane["name"]:
        raise RuntimeError("native packed finalizer lane order changed")
    directory = (output_directory / name).resolve()
    if directory.parent != output_directory.resolve() or not directory.is_dir():
        raise RuntimeError("native packed finalizer lane directory is invalid")
    source_by_field = _mapping(fields.get("source"), f"{name} source receipt")
    for field_name in ("path", "byte_count", "sha256", "row_count", "stored_payload_bytes"):
        if source_by_field.get(field_name) != expected_lane[field_name]:
            raise RuntimeError(f"native packed finalizer {name} source receipt changed")
    object_kinds = tuple(fields.get("object_kinds") or ())
    if object_kinds != tuple(expected_lane["object_kinds"]):
        raise RuntimeError(f"native packed finalizer {name} object kinds changed")
    kind_digests = _mapping(fields.get("kind_digests"), f"{name} kind digests")
    if set(kind_digests) != set(object_kinds):
        raise RuntimeError(f"native packed finalizer {name} kind receipt is incomplete")
    return directory, source_by_field, object_kinds, kind_digests


def _sidecar(
    raw: Any,
    *,
    output_directory: Path,
    expected_lane: Mapping[str, Any],
) -> PackedMapSidecars:
    """Validate one native lane and bind its exact sidecar artifacts."""

    fields = _mapping(raw, "lane")
    directory, source_by_field, object_kinds, kind_digests = _validated_lane_source(
        fields,
        output_directory=output_directory,
        expected_lane=expected_lane,
    )
    name = str(fields["name"])
    return PackedMapSidecars(
        directory=directory,
        target_blocks=_artifact(
            fields.get("target_blocks"), lane_directory=directory, label=f"{name} targets"
        ),
        map_blocks=_artifact(
            fields.get("map_blocks"), lane_directory=directory, label=f"{name} map blocks"
        ),
        map_packs=_artifact(
            fields.get("map_packs"), lane_directory=directory, label=f"{name} map packs"
        ),
        object_kinds=object_kinds,
        map_pack_count=_count(fields.get("map_pack_count"), f"{name} packs", positive=True),
        coordinate_count=_count(
            fields.get("coordinate_count"), f"{name} coordinates", positive=True
        ),
        target_block_count=_count(
            fields.get("target_block_count"), f"{name} targets", positive=True
        ),
        entry_count=_count(fields.get("entry_count"), f"{name} entries"),
        logical_byte_count=_count(fields.get("logical_byte_count"), f"{name} logical bytes"),
        stored_byte_count=_count(fields.get("stored_byte_count"), f"{name} stored bytes"),
        stored_map_byte_count=_count(
            fields.get("stored_map_byte_count"), f"{name} stored map bytes", positive=True
        ),
        kind_digests=tuple(
            (kind, _digest_bytes(kind_digests[kind], f"{kind} descriptor digest"))
            for kind in object_kinds
        ),
        source_copy_bytes=_count(
            source_by_field.get("byte_count"),
            f"{name} source bytes",
            positive=True,
        ),
        target_stored_byte_count=_count(
            fields.get("target_stored_byte_count"),
            f"{name} unique target stored bytes",
        ),
    )


def _validate_receipt_aggregates(
    summary: Mapping[str, Any],
    sidecars: tuple[PackedMapSidecars, ...],
) -> None:
    coordinate_count = sum(sidecar.coordinate_count for sidecar in sidecars)
    target_count = sum(sidecar.target_block_count for sidecar in sidecars)
    object_kinds = tuple(sorted(kind for sidecar in sidecars for kind in sidecar.object_kinds))
    kind_digest_by_object_kind = {
        object_kind: digest
        for sidecar in sidecars
        for object_kind, digest in sidecar.kind_digests
    }
    if (
        coordinate_count
        != _count(summary.get("canonical_mapping_count"), "canonical mappings", positive=True)
        or target_count != _count(summary.get("target_block_count"), "target blocks", positive=True)
        or object_kinds != PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
        or _digest_bytes(summary.get("map_digest"), "map digest")
        != v4_finalizer_map_root_digest(
            kind_digest_by_object_kind,
            required_object_kinds=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
        )
    ):
        raise RuntimeError("native packed finalizer aggregate receipt changed")


def _receipt(
    summary: Mapping[str, Any],
    *,
    output_directory: Path,
    expected_lanes: tuple[Mapping[str, Any], ...],
) -> PackedMapNativeReceipt:
    """Validate and bind one complete native packing receipt."""

    if (
        set(summary) != _OUTPUT_FIELDS
        or summary.get("format") != _OUTPUT_FORMAT
        or Path(str(summary.get("output_directory") or "")).resolve()
        != output_directory.resolve()
        or summary.get("coordinates_per_pack") != PTG2_V4_DEFAULT_COORDINATES_PER_PACK
        or tuple(summary.get("object_kinds") or ())
        != PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS
    ):
        raise RuntimeError("native packed finalizer summary contract changed")
    raw_lanes = summary.get("lanes")
    if not isinstance(raw_lanes, list) or len(raw_lanes) != len(expected_lanes):
        raise RuntimeError("native packed finalizer lane receipt is incomplete")
    sidecars = tuple(
        _sidecar(raw, output_directory=output_directory, expected_lane=expected)
        for raw, expected in zip(raw_lanes, expected_lanes, strict=True)
    )
    _validate_receipt_aggregates(summary, sidecars)
    elapsed_seconds = summary.get("elapsed_seconds")
    if (
        isinstance(elapsed_seconds, bool)
        or not isinstance(elapsed_seconds, (int, float))
        or not math.isfinite(float(elapsed_seconds))
        or float(elapsed_seconds) < 0
    ):
        raise RuntimeError("native packed finalizer elapsed seconds is invalid")
    return PackedMapNativeReceipt(
        directory=output_directory,
        sidecars=sidecars,
        canonical_mapping_digest=_digest_bytes(
            summary.get("canonical_mapping_digest"), "canonical mapping digest"
        ),
        canonical_byte_count=_count(
            summary.get("canonical_byte_count"), "canonical bytes", positive=True
        ),
        target_identity_digest=_digest_bytes(
            summary.get("target_identity_digest"), "target identity digest"
        ),
        elapsed_seconds=float(elapsed_seconds),
    )


def _write_input_manifest(
    path: Path,
    expected_lanes: tuple[Mapping[str, Any], ...],
) -> None:
    path.write_text(
        json.dumps(
            {
                "contract": _INPUT_CONTRACT,
                "coordinates_per_pack": PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
                "lanes": expected_lanes,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="ascii",
    )


async def _run_native_packer(
    *,
    binary: Path,
    output_directory: Path,
    manifest_path: Path,
    expected_lanes: tuple[Mapping[str, Any], ...],
    identity_map_max_bytes: int,
) -> tuple[asyncio.subprocess.Process, bytes, bytes]:
    """Run one native packing attempt and clean any unacknowledged output."""
    process: asyncio.subprocess.Process | None = None
    spawn_task: asyncio.Task[asyncio.subprocess.Process] | None = None
    try:
        _write_input_manifest(manifest_path, expected_lanes)
        spawn_task = asyncio.create_task(
            asyncio.create_subprocess_exec(
                str(binary),
                "--pack-v4-finalizer-copies",
                str(output_directory),
                "--identity-map-max-bytes",
                str(identity_map_max_bytes),
                str(manifest_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                **_subprocess_session_options(asyncio.create_subprocess_exec),
            )
        )
        process = await asyncio.shield(spawn_task)
        stdout, stderr = await process.communicate()
        return process, stdout, stderr
    except BaseException:
        if process is None and spawn_task is not None:
            try:
                process = await _await_cleanup_task(spawn_task)
            except BaseException:
                process = None
        try:
            if process is not None:
                termination_task = asyncio.create_task(
                    _terminate_asyncio_subprocess_group(process)
                )
                await _await_cleanup_task(termination_task)
        finally:
            _cleanup_unacknowledged_finalizer_attempt(
                manifest_path=manifest_path,
                output_directory=output_directory,
                process_id=int(process.pid) if process is not None else None,
            )
        raise


def _complete_native_packer(
    process: asyncio.subprocess.Process,
    stdout: bytes,
    stderr: bytes,
    *,
    output_directory: Path,
    manifest_path: Path,
    expected_lanes: tuple[Mapping[str, Any], ...],
) -> PackedMapNativeReceipt:
    if process.returncode != 0:
        _cleanup_unacknowledged_finalizer_attempt(
            manifest_path=manifest_path,
            output_directory=output_directory,
            process_id=int(process.pid),
        )
        raise RuntimeError(
            "native packed finalizer failed with exit "
            f"{process.returncode}: {stderr.decode('utf-8', errors='replace')[-4000:]}"
        )
    try:
        receipt = _receipt(
            _framed_summary(stdout),
            output_directory=output_directory,
            expected_lanes=expected_lanes,
        )
        _remove_finalizer_attempt_path(manifest_path)
        return receipt
    except BaseException:
        _cleanup_unacknowledged_finalizer_attempt(
            manifest_path=manifest_path,
            output_directory=output_directory,
            process_id=int(process.pid),
        )
        raise


async def pack_v4_finalizer_copies(
    finalizer_summary: Mapping[str, Any],
    *,
    work_directory: str | Path,
) -> PackedMapNativeReceipt:
    """Parse both finalizer COPY files once in Rust and return exact artifacts."""

    blocks = _mapping(finalizer_summary.get("blocks"), "block summary")
    expected_lanes = (
        _input_lane(
            "price_dictionary",
            finalizer_summary,
            _mapping(blocks.get("price_dictionary"), "price dictionary blocks"),
        ),
        _input_lane(
            "serving",
            finalizer_summary,
            _mapping(blocks.get("serving"), "serving blocks"),
        ),
    )
    work_root = Path(work_directory)
    output_directory = work_root / "v4-finalizer-packed"
    manifest_path = work_root / "v4-finalizer-pack-input.json"
    if output_directory.exists() or manifest_path.exists():
        raise RuntimeError("native packed finalizer output already exists")
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("native packed finalizer requires the PTG2 Rust scanner")
    resource_configuration = _load_v3_finalizer_resource_configuration()
    process, stdout, stderr = await _run_native_packer(
        binary=binary,
        output_directory=output_directory,
        manifest_path=manifest_path,
        expected_lanes=expected_lanes,
        identity_map_max_bytes=resource_configuration.identity_map_max_bytes,
    )
    return _complete_native_packer(
        process,
        stdout,
        stderr,
        output_directory=output_directory,
        manifest_path=manifest_path,
        expected_lanes=expected_lanes,
    )


__all__ = ("pack_v4_finalizer_copies",)
