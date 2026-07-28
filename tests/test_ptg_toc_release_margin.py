# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewer-facing margin for PTG table-of-contents boundaries."""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts.domain import PTG2SourceCatalogEntry


ptg = importlib.import_module("process.ptg")


def _install_materialized_toc(monkeypatch, toc_path):
    """Install one local logical artifact and inert persistence boundaries."""

    artifact = SimpleNamespace(logical_path=toc_path)
    monkeypatch.setattr(
        ptg,
        "materialize_json_source",
        AsyncMock(return_value=(artifact, artifact)),
    )
    pushed_file_rows = AsyncMock()
    monkeypatch.setattr(ptg, "push_objects", pushed_file_rows)
    monkeypatch.setattr(ptg, "flush_error_log", AsyncMock())
    return pushed_file_rows


def _flat_catalog_entries():
    """Return supported and unsupported flat-catalog evidence."""

    return [
        PTG2SourceCatalogEntry(
            source_type="in-network",
            domain=ptg.PTG2_DOMAIN_IN_NETWORK,
            original_url="https://rates.example.test/in-network.json.gz",
            canonical_url="https://rates.example.test/in-network.json.gz",
            plan_info=(
                {
                    "plan_name": "Synthetic Plan",
                    "plan_id": "123456789",
                    "plan_id_type": "ein",
                    "plan_market_type": "group",
                },
            ),
        ),
        PTG2SourceCatalogEntry(
            source_type="allowed-amounts",
            domain=ptg.PTG2_DOMAIN_ALLOWED_AMOUNT,
            original_url="https://rates.example.test/allowed.json.gz",
            canonical_url="https://rates.example.test/allowed.json.gz",
            plan_info=(
                {"plan_name": "Plan A"},
                {"plan_name": "Plan B"},
            ),
        ),
        PTG2SourceCatalogEntry(
            source_type="unsupported",
            domain="unsupported",
            original_url="https://rates.example.test/unsupported.json",
            canonical_url="https://rates.example.test/unsupported.json",
        ),
    ]


def _structured_toc_document(target_file_name):
    """Return invalid, filtered, selected, and over-limit TOC members."""

    rate_root = "https://rates.example.test/"
    return {
        "reporting_structure": [
            {
                "reporting_plans": [],
                "in_network_files": [
                    {"location": f"{rate_root}ignored.json.gz"}
                ],
            },
            {
                "reporting_plans": [
                    {
                        "plan_name": "Synthetic Plan",
                        "plan_id": "123456789",
                        "plan_market_type": "group",
                    }
                ],
                "in_network_files": [
                    {"location": None},
                    {"location": f"{rate_root}{target_file_name}"},
                    {"location": f"{rate_root}second-{target_file_name}"},
                ],
                "allowed_amount_files": [
                    "not-an-object",
                    {"location": f"{rate_root}filtered-allowed.json.gz"},
                    {"location": f"{rate_root}{target_file_name}?allowed=1"},
                ],
            },
        ]
    }


@pytest.mark.asyncio
async def test_toc_download_errors_distinguish_freshness_and_io(
    monkeypatch,
):
    """Propagate freshness errors but normalize ordinary download failures."""

    freshness_error = ptg.PTG2FullRebuildFreshnessError(
        "freshness mismatch",
        {"full_rebuild": True},
    )
    monkeypatch.setattr(
        ptg,
        "materialize_json_source",
        AsyncMock(side_effect=freshness_error),
    )
    with pytest.raises(ptg.PTG2FullRebuildFreshnessError):
        await ptg._process_table_of_contents(
            "https://catalog.example.test/toc.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
        )

    monkeypatch.setattr(
        ptg,
        "materialize_json_source",
        AsyncMock(side_effect=OSError("catalog unavailable")),
    )
    with pytest.raises(RuntimeError, match="Failed to download"):
        await ptg._process_table_of_contents(
            "https://catalog.example.test/toc.json",
            {"PTGFile": object, "ImportLog": object},
            test_mode=False,
            raise_on_error=True,
        )
    ignored_jobs = await ptg._process_table_of_contents(
        "https://catalog.example.test/toc.json",
        {"PTGFile": object, "ImportLog": object},
        test_mode=False,
    )
    assert ignored_jobs == []


@pytest.mark.asyncio
async def test_flat_toc_persists_catalog_and_respects_physical_limit(
    monkeypatch,
    tmp_path,
):
    """Persist supported catalog rows while limiting physical rate files."""

    toc_path = tmp_path / "flat.json"
    toc_path.write_text("{}", encoding="utf-8")
    pushed_file_rows = _install_materialized_toc(
        monkeypatch,
        toc_path,
    )
    monkeypatch.setattr(
        ptg,
        "parse_toc_catalog_entries",
        lambda *_args, **_kwargs: _flat_catalog_entries(),
    )
    source_version_recorder = AsyncMock()
    pushed_catalog_rows = AsyncMock()
    monkeypatch.setattr(
        ptg,
        "_record_source_version",
        source_version_recorder,
    )
    monkeypatch.setattr(ptg, "_push_ptg2_objects", pushed_catalog_rows)

    selected_jobs = await ptg._process_table_of_contents(
        "https://catalog.example.test/flat.json",
        {"PTGFile": object, "ImportLog": object},
        test_mode=True,
        import_run_id="ptg2:flat",
        max_files=1,
    )

    assert [(job["type"], job["url"]) for job in selected_jobs] == [
        ("in_network", "https://rates.example.test/in-network.json.gz")
    ]
    source_version_recorder.assert_awaited_once()
    assert len(pushed_catalog_rows.await_args.args[0]) == 2
    assert [
        row_by_field["file_type"]
        for row_by_field in pushed_file_rows.await_args.args[0]
    ] == ["table-of-contents", "in-network"]


@pytest.mark.asyncio
async def test_structured_toc_filters_invalid_and_over_limit_files(
    monkeypatch,
    tmp_path,
):
    """Skip invalid, unmatched, planless, and over-limit TOC entries."""

    target_file_name = "target-in-network.json.gz"
    toc_path = tmp_path / "structured.json"
    toc_path.write_text(
        json.dumps(_structured_toc_document(target_file_name)),
        encoding="utf-8",
    )
    pushed_file_rows = _install_materialized_toc(
        monkeypatch,
        toc_path,
    )

    selected_jobs = await ptg._process_table_of_contents(
        "https://catalog.example.test/structured.json",
        {"PTGFile": object, "ImportLog": object},
        test_mode=False,
        file_url_contains=[target_file_name],
        max_files=1,
    )

    assert len(selected_jobs) == 1
    assert selected_jobs[0]["type"] == "in_network"
    assert [
        row_by_field["file_type"]
        for row_by_field in pushed_file_rows.await_args.args[0]
    ] == ["table-of-contents", "in-network"]
