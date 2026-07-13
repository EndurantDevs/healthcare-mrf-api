from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any, Callable

import pytest

from process.ptg_parts import ptg2_shared_snapshot_publish, rust_scanner
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    write_global_membership_sidecar,
)
from process.ptg_parts.ptg2_shared_graph import (
    MembershipArtifact,
    SharedGraphShardBundle,
)
from process.ptg_parts.ptg2_shared_publish import shared_graph_bundles_from_artifacts


_OUTPUT_NAMES = {
    "block_copy_path": "graph-blocks.copy",
    "owner_copy_path": "graph-owners.copy",
    "group_copy_path": "provider-groups.copy",
    "npi_copy_path": "npi-scope.copy",
    "block_spool_path": "graph-blocks.spool",
    "owner_spool_path": "graph-owners.spool",
    "group_map_path": "provider-group.map",
    "reference_path": "graph-references.run",
}


class _FakeAsyncProcess:
    def __init__(self, stdout: bytes, stderr: bytes = b"", returncode: int = 0):
        self._stdout = stdout
        self._stderr = stderr
        self.returncode: int | None = returncode

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, self._stderr

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        assert self.returncode is not None
        return self.returncode


class _FakeSyncProcess:
    def __init__(self, stdout: bytes):
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO()

    def poll(self) -> int:
        return 0

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def terminate(self) -> None:
        raise AssertionError("completed fake process must not be terminated")

    def kill(self) -> None:
        raise AssertionError("completed fake process must not be killed")


def _artifact(path: Path, marker: bytes) -> MembershipArtifact:
    path.write_bytes(marker)
    return MembershipArtifact(
        path=path,
        metadata={
            "record_format": PTG2_MANIFEST_MEMBERSHIP_FORMAT,
            "sha256": hashlib.sha256(marker).hexdigest(),
            "byte_count": len(marker),
            "owner_count": 1,
            "member_count": 1,
        },
    )


def _graph_bundle(tmp_path: Path) -> SharedGraphShardBundle:
    return SharedGraphShardBundle(
        shard_id="shard-01",
        group_npi=_artifact(tmp_path / "group-npi.sidecar", b"a"),
        npi_group=_artifact(tmp_path / "npi-group.sidecar", b"b"),
        group_provider_set=_artifact(tmp_path / "group-provider.sidecar", b"c"),
        provider_set_group=_artifact(tmp_path / "provider-group.sidecar", b"d"),
    )


def _write_converter_outputs(output_directory: Path) -> None:
    output_directory.mkdir()
    for output_name in (
        "graph-blocks.copy",
        "graph-owners.copy",
        "provider-groups.copy",
        "npi-scope.copy",
    ):
        (output_directory / output_name).write_bytes(b"x" * 21)
    (output_directory / "graph-blocks.spool").write_bytes(b"blocks")
    (output_directory / "graph-owners.spool").write_bytes(b"\0" * (4 * 25))
    (output_directory / "provider-group.map").write_bytes(b"\0" * 20)
    (output_directory / "graph-references.run").write_bytes(b"\0" * (4 * 57))


def _summary(output_directory: Path) -> dict[str, Any]:
    directions = (
        (1, "graph_npi_groups_v1", 4),
        (2, "graph_group_npis_v1", 8),
        (3, "graph_group_provider_sets_v1", 4),
        (4, "graph_provider_set_groups_v1", 4),
    )
    return {
        "format": "ptg2_v3_shared_graph_summary_v1",
        "scratch_directory": str(output_directory),
        "output_directory": str(output_directory),
        **{
            field_name: str(output_directory / output_name)
            for field_name, output_name in _OUTPUT_NAMES.items()
        },
        "block_count": 4,
        "owner_count": 4,
        "provider_group_count": 1,
        "npi_count": 1,
        "support_digest": "ab" * 32,
        "direction_metrics": [
            {
                "direction": direction,
                "object_kind": object_kind,
                "member_width": member_width,
                "owner_count": 1,
                "member_count": 1,
                "empty_owner_count": 0,
                "block_count": 1,
                "raw_byte_count": member_width,
            }
            for direction, object_kind, member_width in directions
        ],
        "edge_metrics": [
            {
                "edge_kind": edge_kind,
                "input_edge_count": 1,
                "unique_edge_count": 1,
                "duplicate_edge_count": 0,
            }
            for edge_kind in ("group_npi", "group_provider_set")
        ],
        "input_byte_count": 4,
        "raw_block_byte_count": 20,
        "stored_block_byte_count": 20,
        "integrity": {
            "shard_count": 1,
            "artifact_count": 4,
            "checksum_byte_count": 4,
            "reciprocal_pair_count": 2,
            "reciprocal_edge_count": 2,
            "input_edge_count": 2,
            "unique_edge_count": 2,
            "duplicate_edge_count": 0,
        },
    }


def _frame(payload: dict[str, Any]) -> bytes:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return b"v3_shared_graph_summary\t" + str(len(encoded)).encode() + b"\n" + encoded + b"\n"


def _install_fake_converter(
    monkeypatch,
    *,
    binary: Path,
    capture: dict[str, Any],
    mutate_stdout: Callable[[bytes], bytes] | None = None,
) -> None:
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)

    async def create_subprocess_exec(*arguments, **kwargs):
        capture["arguments"] = arguments
        capture["kwargs"] = kwargs
        manifest_path = Path(arguments[2])
        manifest = json.loads(manifest_path.read_text(encoding="ascii"))
        capture["manifest_path"] = manifest_path
        capture["manifest"] = manifest
        output_directory = Path(manifest["output_directory"])
        _write_converter_outputs(output_directory)
        stdout = _frame(_summary(output_directory))
        if mutate_stdout is not None:
            stdout = mutate_stdout(stdout)
        return _FakeAsyncProcess(stdout)

    monkeypatch.setattr(
        rust_scanner.asyncio,
        "create_subprocess_exec",
        create_subprocess_exec,
    )


@pytest.mark.asyncio
async def test_native_shared_graph_bridge_writes_manifest_and_validates_result(
    tmp_path,
    monkeypatch,
):
    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    bundle = _graph_bundle(tmp_path)
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{'01' * 16}\t1\n", encoding="ascii")
    output_directory = tmp_path / "native-output"
    capture: dict[str, Any] = {}
    _install_fake_converter(monkeypatch, binary=binary, capture=capture)

    result = (
        await rust_scanner.convert_v3_provider_membership_shards_to_shared_graph_rust(
            shards=(bundle,),
            provider_set_key_map_path=provider_map,
            output_directory=output_directory,
        )
    )

    assert capture["arguments"] == (
        str(binary.resolve()),
        "--convert-shared-graph",
        str(capture["manifest_path"]),
    )
    assert capture["kwargs"] == {
        "stdout": rust_scanner.asyncio.subprocess.PIPE,
        "stderr": rust_scanner.asyncio.subprocess.PIPE,
    }
    manifest = capture["manifest"]
    assert Path(manifest["provider_set_key_map_path"]) == provider_map.resolve()
    assert Path(manifest["output_directory"]) == output_directory.resolve()
    assert capture["manifest_path"].parent == tmp_path
    assert manifest["shards"][0]["shard_id"] == bundle.shard_id
    assert set(manifest["shards"][0]) == {
        "shard_id",
        "group_npi",
        "npi_group",
        "group_provider_set",
        "provider_set_group",
    }
    assert result.block_count == 4
    assert result.owner_count == 4
    assert result.support_digest == bytes.fromhex("ab" * 32)
    assert result.scratch_directory == output_directory.resolve()
    result.cleanup()
    assert not output_directory.exists()


@pytest.mark.asyncio
async def test_native_shared_graph_bridge_rejects_trailing_stdout_and_cleans(
    tmp_path,
    monkeypatch,
):
    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{'01' * 16}\t1\n", encoding="ascii")
    output_directory = tmp_path / "native-output"
    capture: dict[str, Any] = {}
    _install_fake_converter(
        monkeypatch,
        binary=binary,
        capture=capture,
        mutate_stdout=lambda stdout: stdout + b"unexpected",
    )

    with pytest.raises(RuntimeError, match="truncated or trailing frame"):
        await rust_scanner.convert_v3_provider_membership_shards_to_shared_graph_rust(
            shards=(_graph_bundle(tmp_path),),
            provider_set_key_map_path=provider_map,
            output_directory=output_directory,
        )

    assert not output_directory.exists()


@pytest.mark.asyncio
async def test_native_shared_graph_bridge_matches_real_cli_when_built(
    tmp_path,
    monkeypatch,
):
    binary = (
        Path(__file__).resolve().parents[1]
        / "support"
        / "ptg2_scanner"
        / "target"
        / "debug"
        / "ptg2_scanner"
    )
    if not binary.is_file() or not os.access(binary, os.X_OK):
        pytest.skip("build the PTG2 Rust scanner to run the native bridge integration")
    provider_set_id = bytes.fromhex("10" * 16)
    provider_group_id = bytes.fromhex("20" * 16)
    npi_id = b"\0" * 8 + (1234567890).to_bytes(8, "big", signed=False)
    mappings = {
        "provider_group_npi": {provider_group_id: (npi_id,)},
        "provider_npi_group": {npi_id: (provider_group_id,)},
        "provider_inverted": {provider_group_id: (provider_set_id,)},
        "provider_forward": {provider_set_id: (provider_group_id,)},
    }
    artifact_entries = []
    artifact_directory = tmp_path / "artifacts"
    artifact_directory.mkdir()
    for artifact_name, mapping in mappings.items():
        manifest = write_global_membership_sidecar(
            artifact_directory,
            artifact_name,
            mapping,
        )
        sidecar = dict(manifest["sidecars"][0])
        sidecar.update(
            {
                "name": artifact_name,
                "path": str(artifact_directory / str(sidecar["path"])),
                "source_shard_id": "shard-01",
            }
        )
        artifact_entries.append(sidecar)
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{provider_set_id.hex()}\t1\n", encoding="ascii")
    output_directory = tmp_path / "native-output"
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)

    result = (
        await rust_scanner.convert_v3_provider_membership_shards_to_shared_graph_rust(
            shards=shared_graph_bundles_from_artifacts(artifact_entries),
            provider_set_key_map_path=provider_map,
            output_directory=output_directory,
        )
    )

    assert result.block_count == 4
    assert result.owner_count == 4
    assert result.provider_group_count == 1
    assert result.npi_count == 1
    assert len(result.support_digest) == 32
    assert len(tuple(result.iter_references())) == 4
    result.cleanup()


@pytest.mark.asyncio
async def test_snapshot_publish_passes_bundles_and_key_map_to_native_bridge(
    tmp_path,
    monkeypatch,
):
    bundle = _graph_bundle(tmp_path)
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{'01' * 16}\t1\n", encoding="ascii")
    field_names = {
        "provider_group_npi": "group_npi",
        "provider_npi_group": "npi_group",
        "provider_inverted": "group_provider_set",
        "provider_forward": "provider_set_group",
    }
    entries = []
    for artifact_name, field_name in field_names.items():
        artifact = getattr(bundle, field_name)
        entries.append(
            {
                **dict(artifact.metadata),
                "name": artifact_name,
                "path": str(artifact.path),
                "source_shard_id": bundle.shard_id,
            }
        )
    sentinel = object()
    observed: dict[str, Any] = {}

    async def native_bridge(*, shards, provider_set_key_map_path, output_directory):
        observed["shards"] = shards
        observed["provider_set_key_map_path"] = provider_set_key_map_path
        observed["output_directory"] = output_directory
        return sentinel

    monkeypatch.setattr(
        ptg2_shared_snapshot_publish,
        "convert_v3_provider_membership_shards_to_shared_graph_rust",
        native_bridge,
    )

    result = await ptg2_shared_snapshot_publish._convert_shared_graph_natively(
        graph_artifact_entries=entries,
        provider_set_key_map_path=provider_map,
        work_directory=tmp_path,
    )

    assert result is sentinel
    assert isinstance(observed["shards"], tuple)
    assert len(observed["shards"]) == 1
    assert observed["shards"][0].shard_id == bundle.shard_id
    assert observed["shards"][0].group_npi.path == bundle.group_npi.path
    assert observed["shards"][0].npi_group.path == bundle.npi_group.path
    assert observed["provider_set_key_map_path"] == provider_map
    assert isinstance(observed["provider_set_key_map_path"], Path)
    assert observed["output_directory"] == tmp_path / "provider-graph-native"


def test_compact_scanner_never_exports_source_network_labels(tmp_path, monkeypatch):
    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    config = {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": "ptg2_v3_serving_run",
        "serving_run_version": 1,
    }
    framed_stdout = _frame_named("scanner_config", config) + _frame_named(
        "scanner_summary", {}
    )
    captured_env: dict[str, str] = {}

    def popen(arguments, *, stdout, stderr, env):
        del arguments, stdout, stderr
        captured_env.update(env)
        return _FakeSyncProcess(stdout=framed_stdout)

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", popen)
    monkeypatch.setenv(
        "HLTHPRT_PTG2_SOURCE_NETWORK_NAMES_JSON",
        '["ambient-label"]',
    )

    records = list(
        rust_scanner._iter_compact_serving_records_rust(
            tmp_path / "input.json",
            snapshot_id="snapshot",
            plan_id="plan",
            coverage_scope_id="cc" * 32,
            plan_month_id="month",
            source_trace_set_hash="trace",
            v3_serving_run_directory=tmp_path / "runs",
            source_network_names=["caller-label"],
        )
    )

    assert [record_kind for record_kind, _payload in records] == [
        "scanner_config",
        "scanner_summary",
    ]
    assert "HLTHPRT_PTG2_SOURCE_NETWORK_NAMES_JSON" not in captured_env


def _frame_named(record_kind: str, payload: dict[str, Any]) -> bytes:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return (
        record_kind.encode("ascii")
        + b"\t"
        + str(len(encoded)).encode("ascii")
        + b"\n"
        + encoded
        + b"\n"
    )
