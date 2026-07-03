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
    checked = []

    async def fake_head_ok(url):
        checked.append(url)
        return "blobs/download" in url

    monkeypatch.setattr(module, "_toc_head_ok", fake_head_ok)

    resolved = asyncio.run(
        module._resolve_toc_url(
            "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
            refresh_toc_urls=True,
        )
    )

    assert resolved == "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json"
    assert checked == [
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json?sig=stale",
        "https://mrfstore.uhc.com/public-mrf/2026-07-01/example_index.json",
        "https://transparency-in-coverage.uhc.com/api/v1/uhc/blobs/download/2026-07-01/example_index.json",
    ]
