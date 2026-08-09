# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_ptg2_shared_graph import (
    PTG2ManifestArtifactError,
    SharedGraphShardBundle,
    _convert,
    _fixtures,
    _global,
    _npi,
    _overlapping_bundles,
    _postgres_copy_row_count,
    _with_shard_metadata,
    _write_artifact,
    _write_bundle,
    convert_membership_shards_to_shared_graph,
    pytest,
    shared_graph_module,
    tracemalloc,
)


def test_multi_shard_merge_deduplicates_overlapping_graphs(tmp_path):
    bundles, provider_keys, groups = _overlapping_bundles(tmp_path)

    graph_result = convert_membership_shards_to_shared_graph(
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

    graph_result = convert_membership_shards_to_shared_graph(
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
        convert_membership_shards_to_shared_graph(
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
        convert_membership_shards_to_shared_graph(
            shards=(contradictory,),
            provider_set_key_by_global_id=provider_keys,
            external_sort_chunk_bytes=32,
        )


def test_multi_shard_output_is_independent_of_bundle_order(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)

    forward = convert_membership_shards_to_shared_graph(
        shards=bundles,
        provider_set_key_by_global_id=provider_keys,
        external_sort_chunk_bytes=32,
    )
    reverse = convert_membership_shards_to_shared_graph(
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

    graph_result = convert_membership_shards_to_shared_graph(
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
    observed_merge_fan_in_counts = []
    original_write = shared_graph_module._write_sorted_run
    original_merge = shared_graph_module._merge_runs

    def tracking_write(path, records):
        observed_run_bytes.append(sum(len(record) for record in records))
        return original_write(path, records)

    def tracking_merge(paths, destination, record_size):
        observed_merge_fan_in_counts.append(len(paths))
        return original_merge(paths, destination, record_size)

    monkeypatch.setattr(shared_graph_module, "_write_sorted_run", tracking_write)
    monkeypatch.setattr(shared_graph_module, "_merge_runs", tracking_merge)

    graph_result = convert_membership_shards_to_shared_graph(
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
    assert max(observed_merge_fan_in_counts) <= 32
    graph_result.cleanup()
    assert list(spill_dir.iterdir()) == []


def test_multi_shard_identity_must_be_unique_and_match_metadata(tmp_path):
    bundles, provider_keys, _groups = _overlapping_bundles(tmp_path)

    with pytest.raises(PTG2ManifestArtifactError, match="duplicate.*identity"):
        convert_membership_shards_to_shared_graph(
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
        convert_membership_shards_to_shared_graph(
            shards=(mismatched,),
            provider_set_key_by_global_id=provider_keys,
        )
