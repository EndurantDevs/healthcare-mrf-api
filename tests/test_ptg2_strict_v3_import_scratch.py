from __future__ import annotations

import asyncio
import datetime
import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest


process_ptg = importlib.import_module("process.ptg")


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
        source_version=None,
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
            graph_path.parent.mkdir(parents=True, exist_ok=True)
            graph_path.write_bytes(b"synthetic graph")
        yield "scanner_summary", {
            "serving_run_rows": 1,
            "serving_run_partition_files": [],
            "serving_run_code_dictionary_files": [],
        }

    async def build_reverse_graphs(**kwargs):
        Path(kwargs["provider_group_npi_path"]).write_bytes(b"synthetic graph")
        Path(kwargs["provider_npi_group_path"]).write_bytes(b"synthetic graph")
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
