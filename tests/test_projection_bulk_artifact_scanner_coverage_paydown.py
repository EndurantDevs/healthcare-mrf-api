# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts
from process.ptg_parts import rust_scanner
from process.ptg_parts.ptg2_shared_graph import MembershipArtifact, SharedGraphShardBundle


def _membership_artifact(path: Path, marker: bytes, **overrides: Any) -> MembershipArtifact:
    path.write_bytes(marker)
    metadata: Any = {
        "record_format": artifacts.PTG2_MANIFEST_MEMBERSHIP_FORMAT,
        "sha256": hashlib.sha256(marker).hexdigest(),
        "byte_count": len(marker),
        "owner_count": 1,
        "member_count": 2,
        "member_global_count": 2,
        "name": path.stem,
        "source_shard_id": "source",
        "shard_id": "shard",
    }
    metadata.update(overrides)
    return MembershipArtifact(path, metadata)


def _graph_bundle(tmp_path: Path, shard_id: str = "shard-b") -> SharedGraphShardBundle:
    return SharedGraphShardBundle(
        shard_id,
        *(
            _membership_artifact(tmp_path / f"{shard_id}-{index}", bytes([index]))
            for index in range(1, 5)
        ),
    )


def test_shared_graph_manifest_is_sorted_complete_and_counted(tmp_path: Path):
    manifest, expected = rust_scanner._shared_graph_manifest(
        shards=(_graph_bundle(tmp_path, "b"), _graph_bundle(tmp_path, "a")),
        provider_set_key_map_path=tmp_path / "map",
        output_directory=tmp_path / "output",
    )
    assert [shard["shard_id"] for shard in manifest["shards"]] == ["a", "b"]
    assert expected == rust_scanner._SharedGraphExpected(2, 8, 4, 4)
    artifact_metadata = manifest["shards"][0]["group_npi"]["metadata"]
    assert artifact_metadata["member_global_count"] == 2
    assert artifact_metadata["name"] == "a-1"


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("record_format", None),
        ("record_format", ""),
        ("sha256", None),
        ("sha256", "not-hex"),
        ("sha256", "00"),
        ("byte_count", True),
        ("byte_count", -1),
        ("owner_count", "1"),
        ("member_count", None),
        ("member_global_count", -1),
        ("name", 1),
    ),
)
def test_shared_graph_artifact_manifest_rejects_invalid_metadata(
    tmp_path: Path, field: str, value: Any
):
    artifact = _membership_artifact(tmp_path / "artifact", b"x", **{field: value})
    with pytest.raises(RuntimeError):
        rust_scanner._shared_graph_artifact_manifest(artifact)


def test_shared_graph_artifact_manifest_rejects_descriptor_path_and_size(tmp_path: Path):
    with pytest.raises(RuntimeError, match="descriptor"):
        rust_scanner._shared_graph_artifact_manifest(object())
    with pytest.raises(RuntimeError, match="metadata must be an object"):
        rust_scanner._shared_graph_artifact_manifest(MembershipArtifact(tmp_path / "x", []))
    missing = MembershipArtifact(tmp_path / "missing", {})
    with pytest.raises(RuntimeError, match="unavailable"):
        rust_scanner._shared_graph_artifact_manifest(missing)
    wrong_size = _membership_artifact(tmp_path / "wrong-size", b"x", byte_count=2)
    with pytest.raises(RuntimeError, match="byte count"):
        rust_scanner._shared_graph_artifact_manifest(wrong_size)


def test_shared_graph_manifest_rejects_missing_invalid_and_duplicate_shards(tmp_path: Path):
    manifest_option_map = {
        "provider_set_key_map_path": tmp_path / "map",
        "output_directory": tmp_path / "out",
    }
    with pytest.raises(RuntimeError, match="requires graph shards"):
        rust_scanner._shared_graph_manifest(shards=(), **manifest_option_map)
    invalid_shard = type("InvalidShard", (), {"shard_id": "invalid"})()
    with pytest.raises(RuntimeError, match="invalid shard"):
        rust_scanner._shared_graph_manifest(
            shards=(invalid_shard,), **manifest_option_map
        )
    base = _graph_bundle(tmp_path, "empty")
    empty = SharedGraphShardBundle(" ", base.group_npi, base.npi_group, base.group_provider_set, base.provider_set_group)
    with pytest.raises(RuntimeError, match="unique shard ids"):
        rust_scanner._shared_graph_manifest(shards=(empty,), **manifest_option_map)
    bundle = _graph_bundle(tmp_path, "duplicate")
    with pytest.raises(RuntimeError, match="unique shard ids"):
        rust_scanner._shared_graph_manifest(
            shards=(bundle, bundle), **manifest_option_map
        )


def _write_summary_outputs(output: Path) -> None:
    output.mkdir()
    for name in ("graph-blocks.copy", "graph-owners.copy", "provider-groups.copy", "npi-scope.copy"):
        (output / name).write_bytes(b"x" * 21)
    (output / "graph-blocks.spool").write_bytes(b"blocks")
    (output / "graph-owners.spool").write_bytes(b"\0" * 100)
    (output / "provider-group.map").write_bytes(b"\0" * 20)
    (output / "graph-references.run").write_bytes(b"\0" * 228)


def _valid_summary(output: Path) -> dict[str, Any]:
    directions = ((1, "graph_npi_groups_v1", 4), (2, "graph_group_npis_v1", 8),
                  (3, "graph_group_provider_sets_v1", 4), (4, "graph_provider_set_groups_v1", 4))
    return {
        "format": rust_scanner._V3_SHARED_GRAPH_SUMMARY_FORMAT,
        "scratch_directory": str(output),
        "output_directory": str(output),
        **{field: str(output / name) for field, name in rust_scanner._V3_SHARED_GRAPH_OUTPUT_NAMES.items()},
        "block_count": 4, "owner_count": 4, "provider_group_count": 1, "npi_count": 1,
        "support_digest": "ab" * 32,
        "direction_metrics": [
            {"direction": direction, "object_kind": kind, "member_width": width, "owner_count": 1,
             "member_count": 1, "empty_owner_count": 0, "block_count": 1, "raw_byte_count": width}
            for direction, kind, width in directions
        ],
        "edge_metrics": [
            {"edge_kind": kind, "input_edge_count": 1, "unique_edge_count": 1, "duplicate_edge_count": 0}
            for kind in ("group_npi", "group_provider_set")
        ],
        "input_byte_count": 4, "raw_block_byte_count": 20, "stored_block_byte_count": 20,
        "integrity": {"shard_count": 1, "artifact_count": 4, "checksum_byte_count": 4,
                      "reciprocal_pair_count": 2, "reciprocal_edge_count": 2, "input_edge_count": 2,
                      "unique_edge_count": 2, "duplicate_edge_count": 0},
    }


def _summary_frame(summary: dict[str, Any]) -> bytes:
    encoded = json.dumps(summary, separators=(",", ":")).encode()
    return rust_scanner._V3_SHARED_GRAPH_SUMMARY_FRAME + b"\t" + str(len(encoded)).encode() + b"\n" + encoded + b"\n"


def test_shared_graph_summary_frame_validates_complete_output(tmp_path: Path):
    output = tmp_path / "out"
    _write_summary_outputs(output)
    result = rust_scanner._parse_shared_graph_summary_frame(
        _summary_frame(_valid_summary(output)),
        expected_output_directory=output,
        expected=rust_scanner._SharedGraphExpected(1, 4, 1, 1),
    )
    assert result.block_count == 4
    assert result.support_digest == bytes.fromhex("ab" * 32)
    assert [metric.direction for metric in result.direction_metrics] == [1, 2, 3, 4]
    assert [metric.edge_kind for metric in result.edge_metrics] == ["group_npi", "group_provider_set"]


@pytest.mark.parametrize(
    "payload",
    (
        b"",
        b"invalid\n",
        b"unknown\t2\n{}\n",
        b"v3_shared_graph_summary\t\n",
        b"v3_shared_graph_summary\tno\n{}\n",
        b"v3_shared_graph_summary\t0\n\n",
        f"v3_shared_graph_summary\t{rust_scanner._V3_SHARED_GRAPH_SUMMARY_MAX_BYTES + 1}\n".encode(),
        b"v3_shared_graph_summary\t2\n{\n",
        b"v3_shared_graph_summary\t2\n{}\ntrailing",
        b'v3_shared_graph_summary\t13\n{"x":1,"x":2}\n',
    ),
)
def test_shared_graph_summary_frame_rejects_malformed_frames(tmp_path: Path, payload: bytes):
    with pytest.raises(RuntimeError):
        rust_scanner._parse_shared_graph_summary_frame(
            payload,
            expected_output_directory=tmp_path / "out",
            expected=rust_scanner._SharedGraphExpected(1, 4, 1, 1),
        )


@pytest.mark.parametrize(
    "contract_mutation",
    (
        lambda summary_map, _output_path: summary_map.__setitem__(
            "unexpected", None
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "format", "wrong"
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "scratch_directory", "relative"
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "input_byte_count", 5
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "stored_block_byte_count", 19
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "support_digest", "AB" * 32
        ),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "direction_metrics", []
        ),
        lambda summary_map, _output_path: summary_map["direction_metrics"][
            1
        ].__setitem__("direction", 1),
        lambda summary_map, _output_path: summary_map["direction_metrics"][
            0
        ].__setitem__("object_kind", "wrong"),
        lambda summary_map, _output_path: summary_map["direction_metrics"][
            0
        ].__setitem__("empty_owner_count", 2),
        lambda summary_map, _output_path: summary_map.__setitem__("block_count", 5),
        lambda summary_map, _output_path: summary_map.__setitem__(
            "edge_metrics", []
        ),
        lambda summary_map, _output_path: summary_map["edge_metrics"][
            0
        ].__setitem__("edge_kind", "wrong"),
        lambda summary_map, _output_path: summary_map["edge_metrics"][
            1
        ].__setitem__("edge_kind", "group_npi"),
        lambda summary_map, _output_path: summary_map["edge_metrics"][
            0
        ].__setitem__("input_edge_count", 2),
        lambda summary_map, _output_path: summary_map["integrity"].__setitem__(
            "artifact_count", 99
        ),
        lambda _summary_map, output_path: (
            output_path / "graph-owners.spool"
        ).write_bytes(b"x" * 99),
        lambda _summary_map, output_path: (
            output_path / "graph-blocks.copy"
        ).write_bytes(b"x" * 20),
        lambda _summary_map, output_path: (
            output_path / "graph-blocks.spool"
        ).write_bytes(b""),
        lambda _summary_map, output_path: (output_path / "unexpected").write_bytes(
            b"x"
        ),
    ),
    ids=(
        "extra",
        "format",
        "scratch",
        "input",
        "stored",
        "digest",
        "directions",
        "duplicate-direction",
        "direction-kind",
        "direction-counts",
        "blocks",
        "edges",
        "edge-kind",
        "duplicate-edge",
        "edge-counts",
        "integrity",
        "owner-size",
        "copy-size",
        "empty-spool",
        "extra-output",
    ),
)
def test_shared_graph_summary_rejects_contract_drift(
    tmp_path: Path,
    contract_mutation,
):
    """Reject one independently corrupted summary field per case."""

    output_path = tmp_path / "out"
    _write_summary_outputs(output_path)
    summary_map = _valid_summary(output_path)
    contract_mutation(summary_map, output_path)
    with pytest.raises(RuntimeError):
        rust_scanner._shared_graph_result_from_summary(
            summary_map,
            expected_output_directory=output_path,
            expected=rust_scanner._SharedGraphExpected(1, 4, 1, 1),
        )


def _valid_file_frame(*, partition: bool = True) -> dict[str, Any]:
    file_frame_map = {"path": "/tmp/file", "row_count": 2, "bytes": 3, "format": "fmt", "version": 1,
                      "sha256": "ab" * 32}
    if partition:
        file_frame_map.update(partition=0, partition_count=2)
    return file_frame_map


def test_strict_scanner_file_frames_and_config_accept_canonical_values():
    fields = tuple(_valid_file_frame())
    assert rust_scanner._validated_v3_file_frame(
        _valid_file_frame(), label="partition", fields=fields, expected_format="fmt", expected_version=1
    )["partition"] == 0
    no_partition = _valid_file_frame(partition=False)
    assert "partition" not in rust_scanner._validated_v3_file_frame(
        no_partition, label="dictionary", fields=tuple(no_partition), expected_format="fmt", expected_version=1
    )
    scanner_config_map = {"snapshot_arch": "postgres_binary_v3", "storage_generation": "shared_blocks_v3",
                          "serving_row_semantics": "source_multiset_v1", "serving_run_format": "ptg2_v3_serving_run",
                          "serving_run_version": 1}
    assert rust_scanner._validate_v3_scanner_config(scanner_config_map) is scanner_config_map
    assert rust_scanner._strict_non_negative_int("2", "count") == 2
    assert rust_scanner._strict_sha256("ab" * 32, "digest") == "ab" * 32


@pytest.mark.parametrize(
    ("field", "value"),
    (("path", ""), ("format", "bad"), ("version", 2), ("row_count", 0), ("bytes", 0),
     ("sha256", "AB" * 32), ("partition", -1), ("partition_count", 0), ("partition", 2)),
)
def test_strict_scanner_file_frame_rejects_invalid_fields(field: str, value: Any):
    frame = _valid_file_frame()
    frame[field] = value
    with pytest.raises(RuntimeError):
        rust_scanner._validated_v3_file_frame(
            frame, label="partition", fields=tuple(frame), expected_format="fmt", expected_version=1
        )


@pytest.mark.parametrize("value", (None, True, -1, "bad"))
def test_strict_scanner_integer_rejects_invalid_values(value: Any):
    with pytest.raises(RuntimeError):
        rust_scanner._strict_non_negative_int(value, "count")


@pytest.mark.parametrize("value", (None, "", "ab" * 31, "AB" * 32, "gg" * 32))
def test_strict_scanner_sha_rejects_invalid_values(value: Any):
    with pytest.raises(RuntimeError):
        rust_scanner._strict_sha256(value, "digest")


@pytest.mark.parametrize(
    ("field", "value"),
    (("snapshot_arch", "v2"), ("storage_generation", "v2"),
     ("serving_row_semantics", "set"), ("serving_run_format", "other"), ("serving_run_version", 2)),
)
def test_strict_scanner_config_rejects_incompatible_values(field: str, value: Any):
    scanner_config_map = {"snapshot_arch": "postgres_binary_v3", "storage_generation": "shared_blocks_v3",
                          "serving_row_semantics": "source_multiset_v1", "serving_run_format": "ptg2_v3_serving_run",
                          "serving_run_version": 1}
    scanner_config_map[field] = value
    with pytest.raises(RuntimeError):
        rust_scanner._validate_v3_scanner_config(scanner_config_map)
    with pytest.raises(RuntimeError, match="invalid scanner_config"):
        rust_scanner._validate_v3_scanner_config([])
