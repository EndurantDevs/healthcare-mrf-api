from __future__ import annotations

import importlib

from process.ptg_parts.ptg2_shared_publish import (
    shared_graph_bundles_from_artifacts,
)


process_ptg = importlib.import_module("process.ptg")


def test_graph_artifacts_are_grouped_by_complete_source_shard(tmp_path):
    entries = []
    for shard_id in ("file:2", "file:1"):
        for name in (
            "provider_group_npi",
            "provider_npi_group",
            "provider_inverted",
            "provider_forward",
        ):
            entries.append(
                {
                    "name": name,
                    "source_shard_id": shard_id,
                    "path": str(tmp_path / f"{shard_id.replace(':', '-')}-{name}"),
                }
            )

    bundles = shared_graph_bundles_from_artifacts(reversed(entries))

    assert [bundle.shard_id for bundle in bundles] == ["file:1", "file:2"]
    assert bundles[0].group_npi.metadata["name"] == "provider_group_npi"
    assert bundles[0].provider_set_group.metadata["name"] == "provider_forward"


def test_graph_artifact_grouping_fails_closed_on_missing_direction(tmp_path):
    entries = [
        {
            "name": name,
            "source_shard_id": "file:1",
            "path": str(tmp_path / name),
        }
        for name in (
            "provider_group_npi",
            "provider_npi_group",
            "provider_inverted",
        )
    ]

    try:
        shared_graph_bundles_from_artifacts(entries)
    except RuntimeError as exc:
        assert "provider_set_group" in str(exc)
    else:
        raise AssertionError("incomplete strict V3 graph shard was accepted")


def test_strict_v3_graph_artifacts_are_import_scratch_not_serving_storage(
    tmp_path,
    monkeypatch,
):
    artifact_root = tmp_path / "artifacts"
    snapshot_dir = artifact_root / "serving" / "snapshot-token"
    snapshot_dir.mkdir(parents=True)
    generated = snapshot_dir / "provider-graph.ptg2sc"
    generated.write_bytes(b"graph")
    outside = tmp_path / "outside.ptg2sc"
    outside.write_bytes(b"keep")
    monkeypatch.setattr(process_ptg, "resolve_ptg2_artifact_dir", lambda: artifact_root)

    process_ptg._cleanup_strict_v3_graph_artifacts(
        {
            "sidecars": [
                {"path": str(generated)},
                {"path": str(outside)},
                {"path": "db://artifact/ignored"},
            ]
        }
    )

    assert not generated.exists()
    assert not snapshot_dir.exists()
    assert outside.exists()
