# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import struct
import tracemalloc
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_shared_graph as shared_graph_module
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
    PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    PTG2ManifestArtifactError,
)
from process.ptg_parts.ptg2_shared_graph import (
    MembershipArtifact,
    SharedGraphShardBundle,
    convert_v3_provider_membership_shards_to_shared_graph,
    convert_v3_provider_memberships_to_shared_graph,
)


_STANDARD_HEADER = struct.Struct("<8sIQ")
_DENSE_HEADER = struct.Struct("<8sIQQ")
_OWNER = struct.Struct("<16sQI")
_U32 = struct.Struct("<I")


def _postgres_copy_row_count(path: Path) -> int:
    with path.open("rb") as source:
        assert source.read(19) == b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
        row_count = 0
        while True:
            field_count = struct.unpack(">h", source.read(2))[0]
            if field_count == -1:
                assert source.read() == b""
                return row_count
            for _ in range(field_count):
                field_size = struct.unpack(">i", source.read(4))[0]
                if field_size >= 0:
                    assert len(source.read(field_size)) == field_size
            row_count += 1


def _global(value: int) -> bytes:
    return int(value).to_bytes(16, "big")


def _npi(value: int) -> bytes:
    return b"\x00" * 8 + int(value).to_bytes(8, "big")


def _metadata(path: Path, *, dense: bool, owner_count: int, member_count: int, dictionary_count: int = 0):
    payload = path.read_bytes()
    result = {
        "record_format": (
            PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT if dense else PTG2_MANIFEST_MEMBERSHIP_FORMAT
        ),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_count": len(payload),
        "owner_count": owner_count,
        "member_count": member_count,
    }
    if dense:
        result["member_global_count"] = dictionary_count
    return result


def _write_artifact(path: Path, mapping: dict[bytes, list[bytes]], *, dense: bool) -> MembershipArtifact:
    entries = sorted((owner, sorted(set(members))) for owner, members in mapping.items())
    member_count = sum(len(members) for _owner, members in entries)
    member_dictionary = sorted({member for _owner, members in entries for member in members})
    encoded_artifact = bytearray()
    if dense:
        encoded_artifact.extend(
            _DENSE_HEADER.pack(
                PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
                1,
                len(entries),
                len(member_dictionary),
            )
        )
    else:
        encoded_artifact.extend(
            _STANDARD_HEADER.pack(
                PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, len(entries)
            )
        )
    offset = 0
    for owner, members in entries:
        encoded_artifact.extend(_OWNER.pack(owner, offset, len(members)))
        offset += len(members)
    if dense:
        for member in member_dictionary:
            encoded_artifact.extend(member)
        local_ids = {member: key for key, member in enumerate(member_dictionary)}
        for _owner, members in entries:
            for member in members:
                encoded_artifact.extend(_U32.pack(local_ids[member]))
    else:
        for _owner, members in entries:
            for member in members:
                encoded_artifact.extend(member)
    path.write_bytes(encoded_artifact)
    return MembershipArtifact(
        path=path,
        metadata=_metadata(
            path,
            dense=dense,
            owner_count=len(entries),
            member_count=member_count,
            dictionary_count=len(member_dictionary),
        ),
    )


def _fixtures(tmp_path: Path, *, dense_directions=frozenset(), boundary=False):
    group_a = _global(0xA0)
    group_b = _global(0xB0)
    group_empty = _global(0xC0)
    provider_a = _global(0x1000)
    provider_b = _global(0x2000)
    npis = [_npi(1_000_000_000 + index) for index in range(8193 if boundary else 3)]
    group_npi = (
        {group_a: [npis[0]], group_b: npis, group_empty: []}
        if boundary
        else {group_a: npis, group_b: [npis[0]], group_empty: []}
    )
    groups_by_npi = {
        npi: (
            [group_a, group_b]
            if index == 0
            else ([group_b] if boundary else [group_a])
        )
        for index, npi in enumerate(npis)
    }
    providers_by_group = {
        group_a: [provider_a, provider_b],
        group_b: [provider_b],
        group_empty: [],
    }
    groups_by_provider = {provider_a: [group_a], provider_b: [group_a, group_b]}
    mappings = (group_npi, groups_by_npi, providers_by_group, groups_by_provider)
    names = ("group-npi", "npi-group", "group-provider", "provider-group")
    artifacts = tuple(
        _write_artifact(tmp_path / f"{name}.bin", mapping, dense=index in dense_directions)
        for index, (name, mapping) in enumerate(zip(names, mappings))
    )
    return artifacts, {provider_a: 0, provider_b: 1}, (group_a, group_b, group_empty), npis


def _convert(artifacts, provider_keys, **kwargs):
    return convert_v3_provider_memberships_to_shared_graph(
        group_npi=artifacts[0],
        npi_group=artifacts[1],
        group_provider_set=artifacts[2],
        provider_set_group=artifacts[3],
        provider_set_key_by_global_id=provider_keys,
        **kwargs,
    )


def _reverse(mapping: dict[bytes, list[bytes]]) -> dict[bytes, list[bytes]]:
    reversed_mapping: dict[bytes, list[bytes]] = {}
    for owner, members in mapping.items():
        for member in members:
            reversed_mapping.setdefault(member, []).append(owner)
    return reversed_mapping


def _with_shard_metadata(
    artifact: MembershipArtifact,
    *,
    shard_id: str,
    name: str,
) -> MembershipArtifact:
    metadata = dict(artifact.metadata)
    metadata.update({"source_shard_id": shard_id, "name": name})
    return MembershipArtifact(artifact.path, metadata)


def _write_bundle(
    directory: Path,
    shard_id: str,
    *,
    group_npi: dict[bytes, list[bytes]],
    group_provider_set: dict[bytes, list[bytes]],
    dense_directions=frozenset(),
) -> SharedGraphShardBundle:
    directory.mkdir()
    mappings = (
        group_npi,
        _reverse(group_npi),
        group_provider_set,
        _reverse(group_provider_set),
    )
    names = (
        "provider_group_npi",
        "provider_npi_group",
        "provider_inverted",
        "provider_forward",
    )
    artifacts = tuple(
        _with_shard_metadata(
            _write_artifact(
                directory / f"direction-{index}.bin",
                mapping,
                dense=index in dense_directions,
            ),
            shard_id=shard_id,
            name=names[index],
        )
        for index, mapping in enumerate(mappings)
    )
    return SharedGraphShardBundle(shard_id, *artifacts)


def _overlapping_bundles(tmp_path: Path):
    group_a = _global(0xA0)
    group_b = _global(0xB0)
    group_empty = _global(0xC0)
    provider_a = _global(0x1000)
    provider_b = _global(0x2000)
    npi_a_id = _npi(1_000_000_000)
    npi_b_id = _npi(1_000_000_001)
    npi_c_id = _npi(1_000_000_002)
    npi_d_id = _npi(1_000_000_003)
    first = _write_bundle(
        tmp_path / "first",
        "source-a",
        group_npi={group_a: [npi_a_id, npi_b_id], group_empty: []},
        group_provider_set={group_a: [provider_a], group_empty: []},
        dense_directions=frozenset({0, 2}),
    )
    second = _write_bundle(
        tmp_path / "second",
        "source-b",
        group_npi={group_a: [npi_b_id, npi_c_id], group_b: [npi_d_id]},
        group_provider_set={group_a: [provider_a, provider_b], group_b: [provider_b]},
        dense_directions=frozenset({1, 3}),
    )
    return (first, second), {provider_a: 0, provider_b: 1}, (group_a, group_b, group_empty)


@pytest.mark.parametrize(
    "dense_directions",
    [frozenset(), frozenset({0, 1, 2, 3}), frozenset({0, 2}), frozenset({1, 3})],
)
def test_converter_emits_all_directions_for_standard_and_dense_sources(tmp_path, dense_directions):
    artifacts, provider_keys, groups, _npis = _fixtures(
        tmp_path, dense_directions=dense_directions
    )

    graph_result = _convert(artifacts, provider_keys, external_sort_chunk_bytes=32)

    assert dict(graph_result.iter_group_key_items()) == {
        groups[0]: 0,
        groups[1]: 1,
        groups[2]: 2,
    }
    assert [metric.object_kind for metric in graph_result.direction_metrics] == [
        "graph_npi_groups_v1",
        "graph_group_npis_v1",
        "graph_group_provider_sets_v1",
        "graph_provider_set_groups_v1",
    ]
    assert [metric.member_width for metric in graph_result.direction_metrics] == [4, 8, 4, 4]
    assert [metric.member_count for metric in graph_result.direction_metrics] == [4, 4, 3, 3]
    assert [metric.owner_count for metric in graph_result.direction_metrics] == [3, 3, 3, 2]
    assert [metric.empty_owner_count for metric in graph_result.direction_metrics] == [0, 1, 1, 0]
    assert graph_result.integrity.reciprocal_edge_count == 7
    assert graph_result.integrity.input_edge_count == 7
    assert graph_result.integrity.unique_edge_count == 7
    assert graph_result.integrity.duplicate_edge_count == 0
    assert graph_result.integrity.shard_count == 1
    assert graph_result.integrity.artifact_count == 4
    assert graph_result.input_byte_count == graph_result.integrity.checksum_byte_count
    assert graph_result.raw_block_byte_count == graph_result.stored_block_byte_count
    assert all(block.codec == "none" for block in graph_result.iter_shared_blocks())
    assert all(
        block.raw_byte_count <= 64 * 1024
        for block in graph_result.iter_shared_blocks()
    )


def test_boundary_spanning_owner_has_fetch_compatible_locator(tmp_path):
    artifacts, provider_keys, groups, npis = _fixtures(
        tmp_path, dense_directions=frozenset({0, 1, 2, 3}), boundary=True
    )

    graph_result = _convert(artifacts, provider_keys, external_sort_chunk_bytes=1024)

    group_npi_blocks = [
        block
        for block in graph_result.iter_shared_blocks()
        if block.object_kind == "graph_group_npis_v1"
    ]
    owner = next(
        owner_row
        for owner_row in graph_result.iter_owner_rows()
        if owner_row.direction == 2 and owner_row.owner_key == 1
    )
    member_bytes = b"".join(
        block.payload
        for block in group_npi_blocks[owner.first_chunk : owner.first_chunk + 2]
    )
    selected = member_bytes[
        owner.member_offset : owner.member_offset + owner.member_count * 8
    ]

    assert owner.member_count == 8193
    assert owner.first_chunk == 0
    assert owner.member_offset == 8
    assert [block.raw_byte_count for block in group_npi_blocks] == [65536, 16]
    assert len(selected) == 8193 * 8
    assert int.from_bytes(selected[:8], "little") == int.from_bytes(npis[0][8:], "big")
    assert int.from_bytes(selected[-8:], "little") == int.from_bytes(npis[-1][8:], "big")
    empty_owner = next(
        owner_row
        for owner_row in graph_result.iter_owner_rows()
        if owner_row.direction == 2 and owner_row.owner_key == 2
    )
    assert empty_owner.member_count == 0
    assert dict(graph_result.iter_group_key_items())[groups[2]] == 2


def test_output_is_deterministic_across_formats_and_spill_sizes(tmp_path):
    left_dir = tmp_path / "left"
    right_dir = tmp_path / "right"
    left_dir.mkdir()
    right_dir.mkdir()
    left_artifacts, provider_keys, _groups, _npis = _fixtures(left_dir)
    right_artifacts, _, _groups, _npis = _fixtures(
        right_dir, dense_directions=frozenset({0, 1, 2, 3})
    )

    left = _convert(left_artifacts, provider_keys, external_sort_chunk_bytes=32)
    right = _convert(right_artifacts, provider_keys, external_sort_chunk_bytes=4096)

    assert tuple(left.iter_shared_blocks()) == tuple(right.iter_shared_blocks())
    assert tuple(left.iter_owner_rows()) == tuple(right.iter_owner_rows())
    assert dict(left.iter_group_key_items()) == dict(right.iter_group_key_items())
    assert left.direction_metrics == right.direction_metrics


def test_truncated_artifact_fails_after_matching_metadata_is_recomputed(tmp_path):
    artifacts, provider_keys, _groups, _npis = _fixtures(tmp_path)
    target = artifacts[0].path
    target.write_bytes(target.read_bytes()[:-1])
    malformed = MembershipArtifact(
        target,
        _metadata(target, dense=False, owner_count=3, member_count=4),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="layout size mismatch"):
        _convert((malformed,) + artifacts[1:], provider_keys)


@pytest.mark.parametrize("missing_field", ["sha256", "byte_count", "owner_count", "member_count"])
def test_incomplete_metadata_fails_closed(tmp_path, missing_field):
    artifacts, provider_keys, _groups, _npis = _fixtures(tmp_path)
    metadata = dict(artifacts[0].metadata)
    metadata.pop(missing_field)
    incomplete = MembershipArtifact(artifacts[0].path, metadata)

    with pytest.raises(PTG2ManifestArtifactError, match="metadata requires"):
        _convert((incomplete,) + artifacts[1:], provider_keys)


def test_nonreciprocal_group_npi_edges_fail_even_when_counts_match(tmp_path):
    artifacts, provider_keys, groups, npis = _fixtures(tmp_path)
    bad_reverse = _write_artifact(
        tmp_path / "bad-npi-group.bin",
        {npis[0]: [groups[0]], npis[1]: [groups[0], groups[1]], npis[2]: [groups[0]]},
        dense=True,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not reciprocal"):
        _convert((artifacts[0], bad_reverse, artifacts[2], artifacts[3]), provider_keys)


def test_nonreciprocal_group_provider_edges_fail(tmp_path):
    artifacts, provider_keys, groups, _npis = _fixtures(tmp_path)
    provider_ids = sorted(provider_keys)
    bad_reverse = _write_artifact(
        tmp_path / "bad-provider-group.bin",
        {provider_ids[0]: [groups[1]], provider_ids[1]: [groups[0], groups[1]]},
        dense=False,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not reciprocal"):
        _convert((artifacts[0], artifacts[1], artifacts[2], bad_reverse), provider_keys)


@pytest.mark.parametrize(
    "provider_keys,error",
    [
        ({_global(0x1000): 1, _global(0x2000): 0}, "follow sorted"),
        ({_global(0x1000): 0}, "absent from the authoritative"),
        ({_global(0x1000): 4, _global(0x2000): 5}, "dense"),
    ],
)
def test_authoritative_provider_set_dictionary_is_required_and_validated(
    tmp_path, provider_keys, error
):
    artifacts, _valid_keys, _groups, _npis = _fixtures(tmp_path)

    with pytest.raises((ValueError, PTG2ManifestArtifactError), match=error):
        _convert(artifacts, provider_keys)


def test_invalid_npi_global_id_fails_closed(tmp_path):
    artifacts, provider_keys, groups, _npis = _fixtures(tmp_path)
    invalid_npi = _global(2**80)
    group_npi = _write_artifact(
        tmp_path / "invalid-group-npi.bin", {groups[0]: [invalid_npi]}, dense=False
    )
    npi_group = _write_artifact(
        tmp_path / "invalid-npi-group.bin", {invalid_npi: [groups[0]]}, dense=True
    )

    with pytest.raises(PTG2ManifestArtifactError, match="invalid global ID"):
        _convert((group_npi, npi_group, artifacts[2], artifacts[3]), provider_keys)


def test_multi_shard_merge_deduplicates_overlapping_graphs(tmp_path):
    bundles, provider_keys, groups = _overlapping_bundles(tmp_path)

    graph_result = convert_v3_provider_membership_shards_to_shared_graph(
        shards=bundles,
        provider_set_key_by_global_id=provider_keys,
        external_sort_chunk_bytes=32,
    )

    assert dict(graph_result.iter_group_key_items()) == {
        groups[0]: 0,
        groups[1]: 1,
        groups[2]: 2,
    }
    assert [metric.member_count for metric in graph_result.direction_metrics] == [4, 4, 3, 3]
    assert [metric.owner_count for metric in graph_result.direction_metrics] == [4, 3, 3, 2]
    assert [metric.empty_owner_count for metric in graph_result.direction_metrics] == [0, 1, 1, 0]
    assert graph_result.edge_metrics == (
        shared_graph_module.SharedGraphEdgeMetrics("group_npi", 5, 4, 1),
        shared_graph_module.SharedGraphEdgeMetrics("group_provider_set", 4, 3, 1),
    )
    assert graph_result.integrity.shard_count == 2
    assert graph_result.integrity.artifact_count == 8
    assert graph_result.integrity.input_edge_count == 9
    assert graph_result.integrity.unique_edge_count == 7
    assert graph_result.integrity.duplicate_edge_count == 2
    assert graph_result.integrity.reciprocal_edge_count == 7


def test_duplicate_only_shard_contributes_no_new_edges(tmp_path):
    group = _global(0xA0)
    provider = _global(0x1000)
    npis = [_npi(1_000_000_001), _npi(1_000_000_002)]
    first = _write_bundle(
        tmp_path / "first",
        "source-a",
        group_npi={group: npis},
        group_provider_set={group: [provider]},
    )
    duplicate = _write_bundle(
        tmp_path / "duplicate",
        "source-b",
        group_npi={group: list(reversed(npis))},
        group_provider_set={group: [provider]},
        dense_directions=frozenset({0, 1, 2, 3}),
    )

    graph_result = convert_v3_provider_membership_shards_to_shared_graph(
        shards=(first, duplicate),
        provider_set_key_by_global_id={provider: 0},
    )

    assert [metric.member_count for metric in graph_result.direction_metrics] == [2, 2, 1, 1]
    assert graph_result.edge_metrics == (
        shared_graph_module.SharedGraphEdgeMetrics("group_npi", 4, 2, 2),
        shared_graph_module.SharedGraphEdgeMetrics("group_provider_set", 2, 1, 1),
    )
    assert graph_result.integrity.input_edge_count == 6
    assert graph_result.integrity.unique_edge_count == 3
    assert graph_result.integrity.duplicate_edge_count == 3


def test_incomplete_multi_shard_bundle_fails_closed(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)
    incomplete = SharedGraphShardBundle(
        shard_id="incomplete",
        group_npi=bundles[0].group_npi,
        npi_group=bundles[0].npi_group,
        group_provider_set=bundles[0].group_provider_set,
        provider_set_group=None,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="incomplete.*four directions"):
        convert_v3_provider_membership_shards_to_shared_graph(
            shards=(incomplete,),
            provider_set_key_by_global_id=provider_keys,
        )


def test_contradictory_multi_shard_directions_fail_before_global_merge(tmp_path):
    bundles, provider_keys, groups = _overlapping_bundles(tmp_path)
    bad_reverse = _with_shard_metadata(
        _write_artifact(
            tmp_path / "bad-reverse.bin",
            {_npi(1_000_000_000): [groups[1]], _npi(1_000_000_001): [groups[0]]},
            dense=True,
        ),
        shard_id="source-a",
        name="provider_npi_group",
    )
    contradictory = SharedGraphShardBundle(
        shard_id="source-a",
        group_npi=bundles[0].group_npi,
        npi_group=bad_reverse,
        group_provider_set=bundles[0].group_provider_set,
        provider_set_group=bundles[0].provider_set_group,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="source-a.*not reciprocal"):
        convert_v3_provider_membership_shards_to_shared_graph(
            shards=(contradictory,),
            provider_set_key_by_global_id=provider_keys,
            external_sort_chunk_bytes=32,
        )


def test_multi_shard_output_is_independent_of_bundle_order(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)

    forward = convert_v3_provider_membership_shards_to_shared_graph(
        shards=bundles,
        provider_set_key_by_global_id=provider_keys,
        external_sort_chunk_bytes=32,
    )
    reverse = convert_v3_provider_membership_shards_to_shared_graph(
        shards=tuple(reversed(bundles)),
        provider_set_key_by_global_id=provider_keys,
        external_sort_chunk_bytes=4096,
    )

    assert tuple(forward.iter_shared_blocks()) == tuple(reverse.iter_shared_blocks())
    assert tuple(forward.iter_owner_rows()) == tuple(reverse.iter_owner_rows())
    assert dict(forward.iter_group_key_items()) == dict(reverse.iter_group_key_items())
    assert forward.direction_metrics == reverse.direction_metrics
    assert forward.edge_metrics == reverse.edge_metrics
    assert forward.integrity == reverse.integrity
    assert forward.support_digest == reverse.support_digest


def test_conversion_keeps_cardinality_dependent_outputs_on_disk(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)
    spill = tmp_path / "spill"

    graph_result = convert_v3_provider_membership_shards_to_shared_graph(
        shards=bundles,
        provider_set_key_by_global_id=provider_keys,
        spill_directory=spill,
        external_sort_chunk_bytes=32,
    )

    assert graph_result.block_count > 0
    assert graph_result.owner_count > 0
    assert graph_result.block_copy_path.stat().st_size > 0
    assert graph_result.owner_copy_path.stat().st_size > 0
    assert graph_result.group_copy_path.stat().st_size > 0
    assert graph_result.npi_copy_path.stat().st_size > 0
    assert graph_result.reference_path.stat().st_size > 0
    assert _postgres_copy_row_count(graph_result.block_copy_path) == graph_result.block_count
    assert _postgres_copy_row_count(graph_result.owner_copy_path) == graph_result.owner_count
    assert _postgres_copy_row_count(graph_result.group_copy_path) == graph_result.provider_group_count
    assert _postgres_copy_row_count(graph_result.npi_copy_path) == graph_result.npi_count
    scratch = graph_result.scratch_directory
    graph_result.cleanup()
    assert not scratch.exists()
    assert list(spill.iterdir()) == []


def test_boundary_conversion_peak_python_memory_is_spill_bounded(tmp_path):
    artifacts, provider_keys, _groups, _npis = _fixtures(
        tmp_path,
        dense_directions=frozenset({0, 1, 2, 3}),
        boundary=True,
    )

    tracemalloc.start()
    try:
        result = _convert(
            artifacts,
            provider_keys,
            spill_directory=tmp_path / "spill",
            external_sort_chunk_bytes=32 * 1024,
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    try:
        assert result.integrity.unique_edge_count == 8_197
        assert peak < 4 * 1024 * 1024
    finally:
        result.cleanup()


def test_multi_shard_external_runs_respect_spill_bound(monkeypatch, tmp_path):
    group = _global(0xA0)
    provider = _global(0x1000)
    npi = _npi(1_000_000_001)
    bundles = tuple(
        _write_bundle(
            tmp_path / f"source-{index}",
            f"source-{index}",
            group_npi={group: [npi]},
            group_provider_set={group: [provider]},
            dense_directions=frozenset({index % 4}),
        )
        for index in range(35)
    )
    spill_dir = tmp_path / "spill"
    observed_run_bytes = []
    observed_merge_fan_in = []
    original_write = shared_graph_module._write_sorted_run
    original_merge = shared_graph_module._merge_runs

    def tracking_write(path, records):
        observed_run_bytes.append(sum(len(record) for record in records))
        return original_write(path, records)

    def tracking_merge(paths, destination, record_size):
        observed_merge_fan_in.append(len(paths))
        return original_merge(paths, destination, record_size)

    monkeypatch.setattr(shared_graph_module, "_write_sorted_run", tracking_write)
    monkeypatch.setattr(shared_graph_module, "_merge_runs", tracking_merge)

    graph_result = convert_v3_provider_membership_shards_to_shared_graph(
        shards=bundles,
        provider_set_key_by_global_id={provider: 0},
        spill_directory=spill_dir,
        external_sort_chunk_bytes=32,
    )

    assert graph_result.integrity.input_edge_count == 70
    assert graph_result.integrity.unique_edge_count == 2
    assert graph_result.integrity.duplicate_edge_count == 68
    assert len(observed_run_bytes) > 100
    assert max(observed_run_bytes) <= 32
    assert max(observed_merge_fan_in) <= 32
    graph_result.cleanup()
    assert list(spill_dir.iterdir()) == []


def test_multi_shard_identity_must_be_unique_and_match_metadata(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)

    with pytest.raises(PTG2ManifestArtifactError, match="duplicate.*identity"):
        convert_v3_provider_membership_shards_to_shared_graph(
            shards=(bundles[0], bundles[0]),
            provider_set_key_by_global_id=provider_keys,
        )

    mismatched = SharedGraphShardBundle(
        shard_id="wrong-source",
        group_npi=bundles[0].group_npi,
        npi_group=bundles[0].npi_group,
        group_provider_set=bundles[0].group_provider_set,
        provider_set_group=bundles[0].provider_set_group,
    )
    with pytest.raises(PTG2ManifestArtifactError, match="identity mismatch"):
        convert_v3_provider_membership_shards_to_shared_graph(
            shards=(mismatched,),
            provider_set_key_by_global_id=provider_keys,
        )
