# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed contracts for native packed-finalizer execution."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from process.ptg_parts import ptg2_v4_finalizer_native as native
from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    v4_finalizer_map_root_digest,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
)
from tests.test_ptg2_v4_finalizer_native import _summary
from tests.test_ptg2_v4_finalizer_publish import _artifact, _receipt, _sidecars

@pytest.mark.parametrize(
    ("frame", "message"),
    (
        (b"missing newline", "incomplete frame"),
        (b"broken\n", "invalid frame"),
        (b"other\t2\n{}", "unexpected record"),
        (b"v4_finalizer_pack_summary\t2\n{", "malformed summary"),
        (b"v4_finalizer_pack_summary\t2\n{x", "invalid JSON"),
        (b"v4_finalizer_pack_summary\t2\n[]", "summary is invalid"),
    ),
)
def test_native_summary_frames_fail_closed(frame: bytes, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        native._framed_summary(frame)


def test_native_summary_frame_accepts_one_exact_mapping() -> None:
    assert native._framed_summary(b"v4_finalizer_pack_summary\t2\n{}") == {}


@pytest.mark.parametrize(
    ("call", "message"),
    (
        (lambda: native._count(True, "rows"), "rows is invalid"),
        (lambda: native._sha256("z" * 64, "digest"), "digest is invalid"),
        (lambda: native._mapping((), "mapping"), "mapping is invalid"),
    ),
)
def test_native_scalar_receipts_reject_wrong_types(call, message: str) -> None:
    with pytest.raises(RuntimeError, match=message):
        call()


def test_native_lane_input_rejects_missing_file_and_empty_lane(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="source path is invalid"):
        native._block_path({"output_directory": str(tmp_path)}, {"path": "missing"})
    with pytest.raises(RuntimeError, match="lane is empty"):
        native._input_lane(
            "serving",
            {"output_directory": str(tmp_path)},
            {"artifact_record_counts": {"kind": 0}},
        )


@pytest.mark.parametrize("case", ("path", "size"))
def test_native_artifact_rejects_path_or_size_drift(tmp_path: Path, case: str) -> None:
    lane = tmp_path / "lane"
    lane.mkdir()
    path = lane / "artifact.copy"
    path.write_bytes(b"payload")
    artifact_by_field = {
        "path": path.name,
        "row_count": 1,
        "byte_count": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    if case == "path":
        artifact_by_field["path"] = "missing.copy"
    else:
        artifact_by_field["byte_count"] += 1
    with pytest.raises(RuntimeError, match="path is invalid|size changed"):
        native._artifact(artifact_by_field, lane_directory=lane, label="lane artifact")


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("name", "lane order changed"),
        ("directory", "lane directory is invalid"),
        ("source", "source receipt changed"),
        ("kinds", "object kinds changed"),
        ("digests", "kind receipt is incomplete"),
    ),
)
def test_native_lane_receipt_rejects_identity_drift(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    output = tmp_path / "packed"
    output.mkdir()
    lane = output / "serving"
    lane.mkdir()
    expected_lane_by_field = {
        "name": "serving",
        "path": "/source.copy",
        "byte_count": 10,
        "sha256": "a" * 64,
        "row_count": 2,
        "stored_payload_bytes": 3,
        "object_kinds": ["kind"],
    }
    lane_by_field = {
        "name": "serving",
        "source": {key: expected_lane_by_field[key] for key in (
            "path", "byte_count", "sha256", "row_count", "stored_payload_bytes"
        )},
        "object_kinds": ["kind"],
        "kind_digests": {"kind": "b" * 64},
    }
    if case == "name":
        lane_by_field["name"] = "other"
    elif case == "directory":
        lane.rmdir()
    elif case == "source":
        lane_by_field["source"]["row_count"] = 3
    elif case == "kinds":
        lane_by_field["object_kinds"] = ["other"]
    else:
        lane_by_field["kind_digests"] = {}
    with pytest.raises(RuntimeError, match=message):
        native._validated_lane_source(
            lane_by_field,
            output_directory=output,
            expected_lane=expected_lane_by_field,
        )


def _native_summary_fields(output_directory: Path) -> dict[str, object]:
    return {
        "format": native._OUTPUT_FORMAT,
        "output_directory": str(output_directory),
        "coordinates_per_pack": PTG2_V4_DEFAULT_COORDINATES_PER_PACK,
        "map_digest": "d" * 64,
        "canonical_mapping_digest": "c" * 64,
        "canonical_mapping_count": 12,
        "canonical_byte_count": 120,
        "target_identity_digest": "e" * 64,
        "target_block_count": 8,
        "object_kinds": list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "lanes": [{}, {}],
        "elapsed_seconds": 0.5,
    }


@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("contract", "summary contract changed"),
        ("lanes", "lane receipt is incomplete"),
        ("elapsed", "elapsed seconds is invalid"),
    ),
)
def test_native_complete_receipt_rejects_contract_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    message: str,
) -> None:
    summary_by_field = _native_summary_fields(tmp_path)
    if case == "contract":
        summary_by_field["format"] = "wrong"
    elif case == "lanes":
        summary_by_field["lanes"] = []
    else:
        summary_by_field["elapsed_seconds"] = True
        sidecars = iter(_sidecars(tmp_path))
        monkeypatch.setattr(native, "_sidecar", lambda *_args, **_kwargs: next(sidecars))
        monkeypatch.setattr(native, "_validate_receipt_aggregates", lambda *_args: None)
    with pytest.raises(RuntimeError, match=message):
        native._receipt(summary_by_field, output_directory=tmp_path, expected_lanes=({}, {}))


def test_native_aggregate_receipt_rejects_count_drift(tmp_path: Path) -> None:
    sidecars = _sidecars(tmp_path)
    kind_digest_by_object_kind = {
        object_kind: digest
        for sidecar in sidecars
        for object_kind, digest in sidecar.kind_digests
    }
    summary_by_field = {
        "canonical_mapping_count": 13,
        "target_block_count": 8,
        "map_digest": v4_finalizer_map_root_digest(
            kind_digest_by_object_kind,
            required_object_kinds=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
        ).hex(),
    }
    with pytest.raises(RuntimeError, match="aggregate receipt changed"):
        native._validate_receipt_aggregates(summary_by_field, sidecars)


@pytest.mark.asyncio
async def test_native_spawn_failure_cleans_unacknowledged_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_spawn(*_args, **_kwargs):
        raise OSError("spawn failed")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fail_spawn)
    manifest = tmp_path / "manifest.json"
    output = tmp_path / "output"
    with pytest.raises(OSError, match="spawn failed"):
        await native._run_native_packer(
            binary=Path("scanner"),
            output_directory=output,
            manifest_path=manifest,
            expected_lanes=(),
            identity_map_max_bytes=1,
        )
    assert not manifest.exists()
    assert not output.exists()


@pytest.mark.parametrize("case", ("process", "summary"))
def test_native_completion_failure_cleans_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    cleanup = Mock()
    monkeypatch.setattr(native, "_cleanup_unacknowledged_finalizer_attempt", cleanup)
    process = SimpleNamespace(returncode=1 if case == "process" else 0, pid=73)
    with pytest.raises(RuntimeError, match="failed with exit|incomplete frame"):
        native._complete_native_packer(
            process,
            b"",
            b"failure",
            output_directory=tmp_path / "output",
            manifest_path=tmp_path / "manifest.json",
            expected_lanes=(),
        )
    cleanup.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("existing", "binary"))
async def test_native_pack_rejects_unsafe_attempt_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    work = tmp_path / "work"
    work.mkdir()
    if case == "existing":
        (work / "v4-finalizer-packed").mkdir()
        message = "output already exists"
    else:
        monkeypatch.setattr(native, "_ptg2_rust_scanner_binary", lambda: None)
        message = "requires the PTG2 Rust scanner"
    with pytest.raises(RuntimeError, match=message):
        await native.pack_v4_finalizer_copies(_summary(source), work_directory=work)
