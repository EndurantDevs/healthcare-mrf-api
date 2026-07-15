from __future__ import annotations

import asyncio
import datetime
import importlib
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts.domain import PTG2SourceVersion


process_ptg = importlib.import_module("process.ptg")


def _write_empty_dense_graph(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(struct.pack("<8sIQQ", b"PTG2MNDS", 1, 0, 0))


def _run_parse(tmp_path: Path):
    return process_ptg._parse_in_network_file_strict_v3(
        str(tmp_path / "source.json.gz"),
        1,
        {"reporting_entity_name": "Synthetic payer"},
        [
            {
                "plan_id": "synthetic-plan",
                "plan_id_type": "ein",
                "plan_name": "Synthetic plan",
            }
        ],
        test_mode=True,
        import_log_cls=SimpleNamespace(__name__="ImportLog"),
        source_url="https://example.test/source.json.gz",
        source_version=PTG2SourceVersion(
            source_identity_hash="source-identity",
            source_file_version_id="source-version",
            original_url="https://example.test/source.json.gz",
            canonical_url="https://example.test/source.json.gz",
            raw_sha256="a" * 64,
        ),
        snapshot_id="ptg2:synthetic",
        import_month=datetime.date(2026, 7, 1),
        coverage_scope_id=(b"\xcc" * 32).hex(),
        ptg2_manifest_stage_table="ptg2_manifest_stage_serving_synthetic",
    )


def _install_scratch_test_environment(tmp_path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setattr(process_ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(process_ptg, "resolve_ptg2_artifact_dir", lambda: artifact_root)

    async def no_push(*_args, **_kwargs):
        return None

    monkeypatch.setattr(process_ptg, "_push_ptg2_objects", no_push)
    return artifact_root


def test_scanner_failure_removes_all_strict_v3_file_scratch(tmp_path, monkeypatch):
    artifact_root = _install_scratch_test_environment(tmp_path, monkeypatch)

    async def failed_scanner(*_args, **_kwargs):
        if False:
            yield None
        raise RuntimeError("synthetic scanner failure")

    monkeypatch.setattr(
        process_ptg,
        "_aiter_compact_serving_records_rust",
        failed_scanner,
    )

    with pytest.raises(RuntimeError, match="synthetic scanner failure"):
        asyncio.run(_run_parse(tmp_path))

    assert not list(tmp_path.glob("ptg2-v3-runs-*"))
    assert not list(tmp_path.glob("ptg2_manifest_*.copy*"))
    assert not list((artifact_root / "serving").rglob("*"))


def test_graph_conversion_failure_removes_scanner_outputs(tmp_path, monkeypatch):
    artifact_root = _install_scratch_test_environment(tmp_path, monkeypatch)

    async def scanner(*_args, **kwargs):
        membership_path = Path(kwargs["manifest_provider_group_member_copy_path"])
        membership_path.parent.mkdir(parents=True, exist_ok=True)
        membership_path.write_bytes(b"synthetic")
        yield "scanner_summary", {
            "serving_run_rows": 1,
            "serving_run_partition_files": [],
            "serving_run_code_dictionary_files": [],
        }

    async def failed_graph(**_kwargs):
        raise RuntimeError("synthetic graph failure")

    monkeypatch.setattr(process_ptg, "_aiter_compact_serving_records_rust", scanner)
    monkeypatch.setattr(
        process_ptg,
        "_build_ptg2_provider_membership_sidecars",
        failed_graph,
    )

    with pytest.raises(RuntimeError, match="synthetic graph failure"):
        asyncio.run(_run_parse(tmp_path))

    assert not list(tmp_path.glob("ptg2-v3-runs-*"))
    assert not list(tmp_path.glob("ptg2_manifest_*.copy*"))
    assert not list((artifact_root / "serving").rglob("*"))


def test_fresh_layout_collects_all_four_graph_directions(tmp_path, monkeypatch):
    _install_scratch_test_environment(tmp_path, monkeypatch)

    async def scanner(*_args, **kwargs):
        for argument_name in (
            "manifest_provider_forward_sidecar_path",
            "manifest_provider_inverted_sidecar_path",
        ):
            graph_path = Path(kwargs[argument_name])
            _write_empty_dense_graph(graph_path)
        yield "scanner_summary", {
            "serving_run_rows": 1,
            "serving_run_partition_files": [],
            "serving_run_code_dictionary_files": [],
        }

    async def build_reverse_graphs(**kwargs):
        _write_empty_dense_graph(Path(kwargs["provider_group_npi_path"]))
        _write_empty_dense_graph(Path(kwargs["provider_npi_group_path"]))
        return {"graph_directions": 2}

    monkeypatch.setattr(process_ptg, "_aiter_compact_serving_records_rust", scanner)
    monkeypatch.setattr(
        process_ptg,
        "_build_ptg2_provider_membership_sidecars",
        build_reverse_graphs,
    )
    monkeypatch.setattr(process_ptg, "flush_error_log", lambda *_args: asyncio.sleep(0))

    summary = asyncio.run(_run_parse(tmp_path))

    graph_entries = summary["manifest"]["sidecars"]
    assert set(graph_entries) == {
        "provider_forward",
        "provider_group_npi",
        "provider_inverted",
        "provider_npi_group",
    }
    assert all(entry["byte_count"] > 0 for entry in graph_entries.values())


def test_repeated_snapshot_scans_use_attempt_owned_sidecar_directories(
    tmp_path,
    monkeypatch,
):
    artifact_root = _install_scratch_test_environment(tmp_path, monkeypatch)
    sidecar_directories: list[Path] = []

    async def scanner(*_args, **kwargs):
        provider_forward = Path(kwargs["manifest_provider_forward_sidecar_path"])
        provider_inverted = Path(kwargs["manifest_provider_inverted_sidecar_path"])
        sidecar_directories.append(provider_forward.parent)
        _write_empty_dense_graph(provider_forward)
        _write_empty_dense_graph(provider_inverted)
        yield "scanner_summary", {
            "serving_run_rows": 1,
            "serving_run_partition_files": [],
            "serving_run_code_dictionary_files": [],
        }

    async def build_reverse_graphs(**kwargs):
        _write_empty_dense_graph(Path(kwargs["provider_group_npi_path"]))
        _write_empty_dense_graph(Path(kwargs["provider_npi_group_path"]))
        return {"graph_directions": 2}

    monkeypatch.setattr(process_ptg, "_aiter_compact_serving_records_rust", scanner)
    monkeypatch.setattr(
        process_ptg,
        "_build_ptg2_provider_membership_sidecars",
        build_reverse_graphs,
    )
    monkeypatch.setattr(process_ptg, "flush_error_log", lambda *_args: asyncio.sleep(0))

    asyncio.run(_run_parse(tmp_path))
    asyncio.run(_run_parse(tmp_path))

    assert len(sidecar_directories) == 2
    assert sidecar_directories[0] != sidecar_directories[1]
    assert all(
        directory.parent == artifact_root / "serving"
        for directory in sidecar_directories
    )
