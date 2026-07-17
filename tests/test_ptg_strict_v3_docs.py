from __future__ import annotations

import importlib.util
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PTG_DOCS = (
    REPO_ROOT / "docs" / "imports" / "ptg.md",
    REPO_ROOT / "docs" / "research" / "ptg2_optimization.md",
)


def _normalized_text(path: Path) -> str:
    return " ".join(path.read_text(encoding="utf-8").split())


def test_strict_ptg_docs_define_sharded_writer_and_bounded_cost_pages():
    for path in PTG_DOCS:
        text = _normalized_text(path)
        assert re.search(
            r"emits only `by_code_provider_shard_v1`", text, re.IGNORECASE
        )
        assert "`shard_id = provider_set_key // 1024`" in text
        assert "`block_key = (code_key << 31) | shard_id`" in text
        assert re.search(r"contigu\w* from `0`", text)
        assert "`by_code_price_page_v4`" in text
        assert "64" in text
        assert "progressive exact selection" in text.lower()
        assert "sparse reverse completion" in text.lower()
        assert "adjacent continuation" in text.lower()
        assert "globally ordered" in text.lower()
        assert "bounded" in text.lower()
        assert "`has_more`" in text
        assert "`total_is_exact`" in text
        assert "`total_lower_bound`" in text


def test_strict_ptg_docs_define_bounded_candidate_audit_contract():
    for path in PTG_DOCS:
        text = _normalized_text(path)
        assert "10,000" in text
        assert "1,000" in text
        assert "10,001" in text
        assert "independent" in text.lower()
        assert re.search(r"55[- ]second", text, re.IGNORECASE)
        assert "`aiohttp`" in text
        assert "`uvloop`" in text
        assert "PostgreSQL" in text
        assert "never rereads or decompresses complete source files" in text
        assert "sole automated release verifier" in text
        assert "`ptg-candidate-audit`" in text
        assert "Automatic job orchestration remains pending" not in text
        assert "matched-positive" in text
        assert "negative" in text
        assert "deterministic-random" in text
        assert "40 ms" in text


def test_strict_ptg_docs_pass_public_repository_hygiene():
    checker_path = REPO_ROOT / "scripts" / "ci" / "public_hygiene.py"
    spec = importlib.util.spec_from_file_location("ptg_docs_public_hygiene", checker_path)
    assert spec is not None and spec.loader is not None
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)

    assert checker.check_content(list(PTG_DOCS)) == []
