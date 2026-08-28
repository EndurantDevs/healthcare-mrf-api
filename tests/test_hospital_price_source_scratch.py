# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transient source-scratch safety proof for hospital-price orchestration."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from process import hospital_price_scratch as scratch
from tests.hospital_price_orchestration_support import (
    ArtifactStore as _ArtifactStore,
    orchestrator_module as _orchestrator_module,
)


def test_tmp_root_rejects_sibling_directory(tmp_path):
    root = tmp_path / "root"
    sibling = tmp_path / "sibling"
    root.mkdir()
    sibling.mkdir()

    with pytest.raises(RuntimeError, match="tmp directory is unsafe"):
        scratch.owned_tmp_root(SimpleNamespace(root=root, tmp_dir=sibling))


def test_source_cleanup_requires_anchored_unlink(monkeypatch):
    monkeypatch.setattr(scratch, "_SAFE_DIR_FD_UNLINK", False)

    with pytest.raises(RuntimeError, match="anchored directory unlink"):
        scratch.unlink_transient_source(object(), object())


def test_source_cleanup_rejects_non_regular_file(tmp_path):
    raw_path = tmp_path / "hospital-mrf-source-owned" / "raw" / "source.json"
    raw_path.mkdir(parents=True)

    with pytest.raises(RuntimeError, match="regular non-symlink"):
        scratch.unlink_transient_source(
            _ArtifactStore(tmp_path), SimpleNamespace(raw_path=str(raw_path))
        )

    assert raw_path.is_dir()


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


@pytest.mark.asyncio
async def test_import_preserves_another_runs_active_source_root(
    tmp_path, monkeypatch
):
    orchestrator = _orchestrator_module()
    active_root = tmp_path / "hospital-mrf-source-active-run"
    (active_root / "raw").mkdir(parents=True)
    (active_root / "raw" / "source.json").write_bytes(b"{}")
    sentinel = tmp_path / "unrelated.tmp"
    sentinel.write_bytes(b"preserve")

    async def sync_registry(*_args: Any) -> None:
        assert active_root.is_dir()
        assert sentinel.read_bytes() == b"preserve"
        raise RuntimeError("stop after scratch check")

    monkeypatch.setattr(orchestrator, "sync_registry", sync_registry)
    monkeypatch.setattr(orchestrator, "_progress", lambda *_args: None)
    monkeypatch.setattr(
        orchestrator, "_resource_limits",
        lambda *_args: (1, 1, 1, 1, 1024, 4096, 2048, 1),
    )

    with pytest.raises(RuntimeError, match="stop after scratch check"):
        await orchestrator._run_import(
            {}, {}, (), _ArtifactStore(tmp_path), [], "hospital-prices:test", 300
        )

    assert active_root.is_dir()
    assert sentinel.read_bytes() == b"preserve"
