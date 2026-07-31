"""Portability margin for strict V4 import helper boundaries."""

from __future__ import annotations

import datetime
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


ptg = importlib.import_module("process.ptg")


def _scope_summary(path: Path) -> dict[str, object]:
    return {
        "provider_npi_scope_copy_path": str(path),
        "provider_npi_scope_copy_format": ptg._PTG2_PROVIDER_NPI_SCOPE_FORMAT,
        "provider_npi_scope_copy_rows": 0,
        "provider_npi_scope_copy_bytes": path.stat().st_size,
        "provider_npi_group_bytes": 1,
    }


def _provider_group() -> dict[str, int]:
    return {"owner_count": 0, "member_count": 0, "byte_count": 1}


def test_provider_scope_artifact_rejects_file_and_summary_drift(tmp_path: Path) -> None:
    regular = tmp_path / "scope.copy"
    regular.write_bytes(ptg._PTG2_PG_BINARY_COPY_HEADER + b"\xff\xff")
    symlink = tmp_path / "scope.link"
    symlink.symlink_to(regular)
    with pytest.raises(RuntimeError, match="not a regular file"):
        ptg._validated_provider_npi_scope_artifact(
            symlink,
            summary=_scope_summary(regular),
            provider_npi_group=_provider_group(),
        )
    with pytest.raises(RuntimeError, match="summary is invalid"):
        ptg._validated_provider_npi_scope_artifact(
            regular,
            summary={},
            provider_npi_group=_provider_group(),
        )
    with pytest.raises(RuntimeError, match="inconsistent"):
        ptg._validated_provider_npi_scope_artifact(
            regular,
            summary={**_scope_summary(regular), "provider_npi_scope_copy_rows": 1},
            provider_npi_group=_provider_group(),
        )


def test_graph_cleanup_removes_only_local_import_artifacts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    serving_root = artifact_root / "serving"
    nested = serving_root / "run" / "graph.copy"
    nested.parent.mkdir(parents=True)
    nested.write_bytes(b"graph")
    outside = tmp_path / "outside.copy"
    outside.write_bytes(b"outside")
    monkeypatch.setattr(ptg, "resolve_ptg2_artifact_dir", lambda: artifact_root)

    ptg._cleanup_strict_v3_graph_artifacts(
        {
            "sidecars": [
                "not-a-map",
                {},
                {"path": "https://example.test/graph.copy"},
                {"path": str(outside)},
                {"path": str(nested)},
            ]
        }
    )
    assert not nested.exists()
    assert not nested.parent.exists()
    assert outside.exists()


def test_manifest_copy_cleanup_swallows_filesystem_races(
    monkeypatch,
    tmp_path: Path,
) -> None:
    copy_path = tmp_path / "rates.copy"
    sibling = tmp_path / "rates.copy.worker1"
    sibling.write_bytes(b"")
    original_is_file = Path.is_file

    def is_regular_file_or_raise(path: Path) -> bool:
        if path == sibling:
            raise OSError("raced")
        return original_is_file(path)

    monkeypatch.setattr(Path, "is_file", is_regular_file_or_raise)
    ptg._cleanup_empty_manifest_copy_siblings(copy_path)
    ptg._cleanup_manifest_copy_family(copy_path)
    assert sibling.exists()


def test_allowed_reuse_evidence_rejects_incomplete_inputs(monkeypatch) -> None:
    assert ptg._validated_allowed_reuse_evidence(
        SimpleNamespace(allowed_context=None)
    ) == ({}, [])
    with pytest.raises(RuntimeError, match="missing allowed results"):
        ptg._validated_allowed_reuse_evidence(
            SimpleNamespace(
                allowed_context=SimpleNamespace(successful_files=[]),
            )
        )
    monkeypatch.setattr(
        ptg,
        "_allowed_amount_metrics_from_results",
        lambda _files: {"allowed_amount_evidence": False},
    )
    with pytest.raises(RuntimeError, match="no allowed payment evidence"):
        ptg._validated_allowed_reuse_evidence(
            SimpleNamespace(
                allowed_context=SimpleNamespace(successful_files=[{"ok": True}]),
            )
        )


@pytest.mark.asyncio
async def test_reused_source_evidence_requires_complete_in_network_downloads() -> None:
    incomplete = SimpleNamespace(
        error="failed",
        raw_artifact=None,
        logical_artifact=None,
        job={"type": "in_network"},
    )
    with pytest.raises(RuntimeError, match="incomplete download"):
        await ptg._collect_reused_source_evidence(
            SimpleNamespace(downloaded_jobs=[incomplete])
        )
    wrong_domain = SimpleNamespace(
        error=None,
        raw_artifact=object(),
        logical_artifact=object(),
        job={"type": "allowed_amounts"},
    )
    with pytest.raises(RuntimeError, match="in-network-only"):
        await ptg._collect_reused_source_evidence(
            SimpleNamespace(downloaded_jobs=[wrong_domain])
        )


def _allowed_context():
    return ptg._AllowedFileProcessingContext(
        classes={},
        test_mode=True,
        reuse_raw_artifacts=False,
        max_bytes=None,
        max_items=None,
        import_run_id="run",
        snapshot_id="snapshot",
        keep_partial_artifacts=False,
    )


@pytest.mark.asyncio
async def test_allowed_file_results_preserve_download_failures(monkeypatch) -> None:
    failed = await ptg._load_allowed_file_result(
        SimpleNamespace(
            job={"url": "https://example.test/allowed.json"},
            error="download failed",
            raw_artifact=None,
            logical_artifact=None,
        ),
        _allowed_context(),
    )
    assert failed.error == "download failed"
    incomplete = await ptg._load_allowed_file_result(
        SimpleNamespace(
            job={"url": "https://example.test/allowed.json"},
            error=None,
            raw_artifact=None,
            logical_artifact=None,
        ),
        _allowed_context(),
    )
    assert "both raw and logical" in incomplete.error
    monkeypatch.setattr(
        ptg,
        "_process_allowed_amounts_file",
        AsyncMock(side_effect=RuntimeError("parse failed")),
    )
    parsed = await ptg._load_allowed_file_result(
        SimpleNamespace(
            job={"url": "https://example.test/allowed.json"},
            error=None,
            raw_artifact=object(),
            logical_artifact=object(),
        ),
        _allowed_context(),
    )
    assert parsed.error == "parse failed"


def test_allowed_result_and_publish_preparation_fail_closed(monkeypatch) -> None:
    with pytest.raises(RuntimeError, match="failed 1 of 1"):
        ptg._validate_allowed_file_results([], [{"error": "bad"}], 1)
    with pytest.raises(RuntimeError, match="zero files"):
        ptg._validate_allowed_file_results([], [], 0)
    monkeypatch.setattr(
        ptg,
        "_allowed_amount_metrics_from_results",
        lambda _files: {"allowed_amount_evidence": False},
    )
    context = ptg._AllowedSnapshotPublishContext(
        snapshot_id="snapshot",
        import_run_id="run",
        source_key="source",
        previous_snapshot_id=None,
        import_month=datetime.date(2026, 7, 1),
        started_at=datetime.datetime.now(datetime.timezone.utc),
        options_by_name={},
        import_started_monotonic=1.0,
        data_started_monotonic=2.0,
    )
    with pytest.raises(RuntimeError, match="no payment evidence"):
        ptg._prepare_allowed_snapshot_publish([{"ok": True}], context)


@pytest.mark.asyncio
async def test_allowed_snapshot_persistence_requires_published_state(
    monkeypatch,
) -> None:
    monkeypatch.setattr(ptg, "_push_ptg2_objects", AsyncMock(return_value=None))
    context = ptg._AllowedSnapshotPublishContext(
        snapshot_id="snapshot",
        import_run_id="run",
        source_key="source",
        previous_snapshot_id=None,
        import_month=datetime.date(2026, 7, 1),
        started_at=datetime.datetime.now(datetime.timezone.utc),
        options_by_name={},
        import_started_monotonic=1.0,
        data_started_monotonic=2.0,
    )
    with pytest.raises(RuntimeError, match="did not persist published state"):
        await ptg._persist_allowed_snapshot(
            context,
            {},
            datetime.datetime.now(datetime.timezone.utc),
        )
