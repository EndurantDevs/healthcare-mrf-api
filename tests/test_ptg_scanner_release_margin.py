# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewer-facing margin for strict scanner artifact transitions."""

from __future__ import annotations

import datetime as dt
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts.domain import PTG2SourceVersion


ptg = importlib.import_module("process.ptg")


def _source_version() -> PTG2SourceVersion:
    """Return a verified synthetic input identity."""

    url = "https://rates.example.test/rates.json.gz"
    return PTG2SourceVersion(
        source_identity_hash="source-identity",
        source_file_version_id="source-version",
        original_url=url,
        canonical_url=url,
        raw_sha256="a" * 64,
    )


async def _scanner_records(*_args, **paths_by_name):
    """Emit every authenticated strict-scanner artifact class once."""

    witness_path = Path(paths_by_name["manifest_price_atom_copy_path"]).with_name(
        "witness.copy"
    )
    witness_path.write_text("witness\n", encoding="utf-8")
    yield "dedupe_summary", {"duplicates_removed": 1}
    yield "scanner_config", {"worker_count": 2}
    yield "scanner_summary", {"serving_run_rows": 5}
    yield "source_audit_witness_file", {
        "path": str(witness_path),
        "raw_source_sha256": "a" * 64,
    }
    yield "manifest_provider_group_tax_identity_sidecar_file", {
        "path": str(witness_path.with_name("tax.ptg2tax")),
    }
    copy_kind_by_argument = {
        "manifest_price_atom_copy_file": "manifest_price_atom_copy_path",
        "manifest_price_set_atom_copy_file": (
            "manifest_price_set_atom_copy_path"
        ),
        "manifest_price_set_summary_copy_file": (
            "manifest_price_set_summary_copy_path"
        ),
        "manifest_provider_group_member_copy_file": (
            "manifest_provider_group_member_copy_path"
        ),
        "manifest_provider_set_dictionary_copy_file": (
            "manifest_provider_set_dictionary_copy_path"
        ),
    }
    for record_kind, argument_name in copy_kind_by_argument.items():
        copy_path = Path(paths_by_name[argument_name])
        copy_path.write_text(f"{record_kind}\n", encoding="utf-8")
        yield record_kind, {"path": str(copy_path), "row_count": 1}
    yield "procedure", {"procedure_hash": "procedure-hash"}


def _install_scanner_boundaries(
    monkeypatch,
    tmp_path,
):
    """Install deterministic scanner, storage, and database boundaries."""

    artifact_root = tmp_path / "artifacts"
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "1")
    monkeypatch.setattr(ptg, "ptg2_temp_parent", lambda: tmp_path)
    monkeypatch.setattr(
        ptg,
        "resolve_ptg2_artifact_dir",
        lambda: artifact_root,
    )
    monkeypatch.setattr(ptg, "_push_ptg2_objects", AsyncMock())
    monkeypatch.setattr(ptg, "flush_error_log", AsyncMock())
    monkeypatch.setattr(
        ptg,
        "_collect_ptg2_manifest_sidecar_artifacts",
        lambda *_args, **_kwargs: {"tax_identity": {"byte_count": 1}},
    )
    monkeypatch.setattr(
        ptg,
        "_aiter_compact_serving_records_rust",
        _scanner_records,
    )
    monkeypatch.setattr(
        ptg,
        "_build_ptg2_provider_membership_sidecars",
        AsyncMock(return_value={"edge_count": 1}),
    )


async def _parse_scanner_fixture(tmp_path):
    """Parse one synthetic scanner stream through the strict manifest path."""

    return await ptg._parse_strict_v3_file(
        str(tmp_path / "rates.json.gz"),
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
        source_url="https://rates.example.test/rates.json.gz",
        source_version=_source_version(),
        snapshot_id="ptg2:synthetic",
        coverage_scope_id="c" * 64,
        import_month=dt.date(2026, 7, 1),
        max_items=1,
        ptg2_manifest_stage_table="ptg2_manifest_stage_synthetic",
    )


@pytest.mark.asyncio
async def test_strict_scanner_records_each_authenticated_artifact_kind(
    monkeypatch,
    tmp_path,
):
    """Retain every scanner proof class in one successful manifest summary."""

    _install_scanner_boundaries(monkeypatch, tmp_path)
    manifest_summary = await _parse_scanner_fixture(tmp_path)

    assert manifest_summary["serving_rates"] == 5
    assert manifest_summary["dedupe"] == {"duplicates_removed": 1}
    assert manifest_summary["scanner"]["config"] == {"worker_count": 2}
    assert manifest_summary["manifest"]["membership_graph"] == {
        "edge_count": 1
    }
    assert manifest_summary["in_network_items"] == 1
