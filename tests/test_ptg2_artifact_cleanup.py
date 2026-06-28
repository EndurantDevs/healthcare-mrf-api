from pathlib import Path

from process.ptg_parts.ptg2_artifact_cleanup import (
    build_ptg2_artifact_cleanup_plan,
    execute_ptg2_artifact_cleanup_plan,
)


def test_ptg2_artifact_cleanup_plan_keeps_manifest_referenced_files(tmp_path):
    root = tmp_path / "sidecars"
    keep = root / "snapshot_a" / "provider.bin"
    stale = root / "snapshot_b" / "provider.bin"
    keep.parent.mkdir(parents=True)
    stale.parent.mkdir(parents=True)
    keep.write_bytes(b"keep")
    stale.write_bytes(b"stale")

    plan = build_ptg2_artifact_cleanup_plan(root=root, referenced_paths=[keep])

    assert plan.referenced_files == (keep.resolve(),)
    assert plan.unreferenced_files == (stale.resolve(),)
    assert plan.missing_referenced_files == ()
    assert plan.referenced_bytes == 4
    assert plan.unreferenced_bytes == 5


def test_ptg2_artifact_cleanup_execute_deletes_only_unreferenced_files(tmp_path):
    root = tmp_path / "sidecars"
    keep = root / "snapshot_a" / "provider.bin"
    stale = root / "snapshot_b" / "provider.bin"
    keep.parent.mkdir(parents=True)
    stale.parent.mkdir(parents=True)
    keep.write_bytes(b"keep")
    stale.write_bytes(b"stale")
    plan = build_ptg2_artifact_cleanup_plan(root=root, referenced_paths=[keep])

    execute_ptg2_artifact_cleanup_plan(plan)

    assert keep.exists()
    assert not stale.exists()
    assert not stale.parent.exists()


def test_ptg2_artifact_cleanup_reports_missing_referenced_files(tmp_path):
    root = tmp_path / "sidecars"
    root.mkdir()
    missing = root / "snapshot_a" / "provider.bin"

    plan = build_ptg2_artifact_cleanup_plan(root=root, referenced_paths=[Path(missing)])

    assert plan.referenced_files == ()
    assert plan.unreferenced_files == ()
    assert plan.missing_referenced_files == (missing.resolve(),)


def test_ptg2_artifact_cleanup_remaps_stale_default_artifact_root(tmp_path):
    artifact_root = tmp_path / "ptg2-artifacts"
    root = artifact_root / "serving"
    keep = root / "snapshot_a" / "provider_npi_current.ptg2sc"
    keep.parent.mkdir(parents=True)
    keep.write_bytes(b"keep")

    stale_path = "/tmp/healthporta-ptg2-artifacts/serving/stale/provider_npi_old.ptg2sc"
    plan = build_ptg2_artifact_cleanup_plan(root=root, referenced_paths=[stale_path])

    assert plan.referenced_files == (keep.resolve(),)
    assert plan.unreferenced_files == ()
    assert plan.missing_referenced_files == ()
