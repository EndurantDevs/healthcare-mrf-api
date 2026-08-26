# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transient source-scratch safety proof for hospital-price orchestration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from tests.hospital_price_orchestration_support import (
    ArtifactStore as _ArtifactStore,
    orchestrator_module as _orchestrator_module,
)

def test_source_cleanup_rejects_symlink(tmp_path):
    orchestrator = _orchestrator_module()
    target = tmp_path / "target.json"
    source_link = tmp_path / "hospital-mrf-source-stale" / "raw" / "source.json"
    source_link.parent.mkdir(parents=True)
    target.write_bytes(b"{}")
    source_link.symlink_to(target)

    with pytest.raises(RuntimeError, match="regular non-symlink"):
        orchestrator._unlink_transient_source(
            _ArtifactStore(tmp_path), SimpleNamespace(raw_path=str(source_link))
        )

    assert source_link.is_symlink()
    assert target.read_bytes() == b"{}"


def test_source_cleanup_uses_anchored_directory_unlink(tmp_path, monkeypatch):
    orchestrator = _orchestrator_module()
    raw_path = tmp_path / "hospital-mrf-source-owned" / "raw" / "source.json"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_bytes(b"{}")
    unlink = orchestrator.os.unlink
    calls: list[tuple[Any, Any]] = []

    def anchored_unlink(path: Any, *, dir_fd: Any = None) -> None:
        calls.append((path, dir_fd))
        unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(orchestrator.os, "unlink", anchored_unlink)
    orchestrator._unlink_transient_source(
        _ArtifactStore(tmp_path), SimpleNamespace(raw_path=str(raw_path))
    )

    assert not raw_path.exists()
    assert len(calls) == 1
    assert calls[0][0] == raw_path.name
    assert calls[0][1] is not None


def test_source_sweep_rejects_an_external_tmp_symlink(tmp_path):
    orchestrator = _orchestrator_module()
    store_root = tmp_path / "store"
    external = tmp_path / "external"
    store_root.mkdir()
    external.mkdir()
    (external / "hospital-mrf-source-preserve").mkdir()
    tmp_link = store_root / "tmp"
    tmp_link.symlink_to(external, target_is_directory=True)

    with pytest.raises(RuntimeError, match="tmp directory is unsafe"):
        orchestrator._sweep_transient_source_roots(
            SimpleNamespace(root=store_root, tmp_dir=tmp_link)
        )

    assert (external / "hospital-mrf-source-preserve").is_dir()


@pytest.mark.asyncio
async def test_import_sweeps_only_stale_hospital_source_roots_before_work(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    stale_root = tmp_path / "hospital-mrf-source-stale"
    (stale_root / "raw").mkdir(parents=True)
    (stale_root / "raw" / "source.json").write_bytes(b"{}")
    sentinel = tmp_path / "unrelated.tmp"
    sentinel.write_bytes(b"preserve")

    monkeypatch.setattr(
        orchestrator.shutil.rmtree, "avoids_symlink_attacks", False
    )
    with pytest.raises(RuntimeError, match="symlink-resistant removal"):
        orchestrator._sweep_transient_source_roots(_ArtifactStore(tmp_path))
    assert stale_root.is_dir()
    assert sentinel.read_bytes() == b"preserve"
    monkeypatch.setattr(
        orchestrator.shutil.rmtree, "avoids_symlink_attacks", True
    )

    async def sync_registry(*_args: Any) -> None:
        assert not stale_root.exists()
        assert sentinel.read_bytes() == b"preserve"
        raise RuntimeError("stop after sweep")

    monkeypatch.setattr(orchestrator, "sync_registry", sync_registry)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)
    monkeypatch.setattr(
        orchestrator, "_resource_limits",
        lambda *_args: (1, 1, 1024, 4096, 2048, 1),
    )

    with pytest.raises(RuntimeError, match="stop after sweep"):
        await orchestrator._run_import(
            {}, {}, (), _ArtifactStore(tmp_path), [], "hospital-prices:test", 300
        )

    reserved_file = tmp_path / "hospital-mrf-source-invalid"
    reserved_file.write_bytes(b"do not delete")
    with pytest.raises(RuntimeError, match="not a regular directory"):
        orchestrator._sweep_transient_source_roots(_ArtifactStore(tmp_path))
    assert reserved_file.read_bytes() == b"do not delete"
    assert sentinel.read_bytes() == b"preserve"
