# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
import asyncio
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts" / "devops"


def _load_script(module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, SCRIPT_DIR / f"{module_name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ptg_compare_sql_helper_returns_text_clause():
    module = _load_script("ptg2_compare_snapshots")

    clause = module._sql_text("SELECT 1")

    assert str(clause) == "SELECT 1"


def test_ptg_compare_parser_accepts_latency_benchmark_options():
    module = _load_script("ptg2_compare_snapshots")

    args = module._build_parser().parse_args(
        [
            "--old-snapshot-id",
            "ptg2:old",
            "--new-snapshot-id",
            "ptg2:new",
            "--benchmark-cases",
            "5",
            "--benchmark-iterations",
            "3",
            "--benchmark-limit",
            "7",
        ]
    )

    assert args.benchmark_cases == 5
    assert args.benchmark_iterations == 3
    assert args.benchmark_limit == 7


def test_ptg_compare_summarize_ms_reports_percentile():
    module = _load_script("ptg2_compare_snapshots")

    summary = module._summarize_ms([10.0, 20.0, 30.0])

    assert summary == {
        "count": 3,
        "min_ms": 10.0,
        "avg_ms": 20.0,
        "p95_ms": 30.0,
        "max_ms": 30.0,
    }


def test_ptg_devops_scripts_import_without_database_connection():
    for module_name in (
        "ptg2_compare_snapshots",
        "ptg2_rebuild_snapshot_from_options",
        "ptg2_remove_source_snapshot",
    ):
        assert _load_script(module_name).__name__ == module_name


def test_ptg_rebuild_expands_uhc_toc_url_candidates():
    module = _load_script("ptg2_rebuild_snapshot_from_options")

    candidates = module._uhc_toc_url_candidates(
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale"
    )

    assert candidates == [
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json",
        "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json",
    ]


def test_ptg_rebuild_uses_first_reachable_uhc_toc_candidate(monkeypatch):
    module = _load_script("ptg2_rebuild_snapshot_from_options")
    checked_urls = []

    async def is_toc_head_reachable(url):
        checked_urls.append(url)
        return "blobs/download" in url

    monkeypatch.setattr(module, "_is_toc_head_reachable", is_toc_head_reachable)

    resolved = asyncio.run(
        module._resolve_toc_url(
            "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
            refresh_toc_urls=True,
        )
    )

    assert resolved == "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json"
    assert checked_urls == [
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json",
        "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json",
    ]
