from __future__ import annotations

import sys
import textwrap
from importlib import util
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "readability_budget.py"
SPEC = util.spec_from_file_location("readability_function_names_budget", SCRIPT_PATH)
readability_budget = util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = readability_budget
SPEC.loader.exec_module(readability_budget)


def _build_readability_snapshot_by_category(
    repo_root: Path,
    source_by_path: dict[str, str],
    exceptions: list[str] | None = None,
) -> dict:
    for relative, source in source_by_path.items():
        path = repo_root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source), encoding="utf-8")
    readability_options_by_name = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {},
        "readability": {
            "confusable_function_name_exceptions": exceptions or [],
        },
        "inline_suppression_patterns": [],
    }
    return readability_budget.build_snapshot(repo_root, readability_options_by_name)


def test_confusable_function_names_reports_plurality_only_difference(tmp_path):
    snapshot = _build_readability_snapshot_by_category(
        tmp_path,
        {
            "pkg/module.py": """
                def register_care_code_tools():
                    return None

                def _register_care_codes_tool():
                    return None
            """,
        },
    )

    assert snapshot["issue_counts"]["confusable_function_names"] == 1
    assert snapshot["issues"]["confusable_function_names"][0]["functions"] == [
        "_register_care_codes_tool",
        "register_care_code_tools",
    ]


def test_confusable_function_names_accepts_explicit_cardinality_and_owner_scope(tmp_path):
    snapshot = _build_readability_snapshot_by_category(
        tmp_path,
        {
            "pkg/module.py": """
                def refresh_node_health():
                    return None

                def refresh_all_node_health():
                    return None

                class FirstStore:
                    def load_record(self):
                        return None

                class SecondStore:
                    def load_records(self):
                        return []
            """,
        },
    )

    assert snapshot["issue_counts"]["confusable_function_names"] == 0


def test_confusable_function_names_handles_ids_and_clause_plurals(tmp_path):
    snapshot = _build_readability_snapshot_by_category(
        tmp_path,
        {
            "pkg/module.py": """
                def current_manifest_id():
                    return None

                def current_manifest_ids():
                    return []

                def name_like_clause():
                    return None

                def name_like_clauses():
                    return []
            """,
        },
    )

    assert snapshot["issue_counts"]["confusable_function_names"] == 2


def test_confusable_function_names_supports_split_modules_and_exact_exceptions(tmp_path):
    identifier = "confusable_function_name:pkg/catalog:<module>:build_record:build_record|build_records"
    source_by_path = {
        "pkg/catalog_part_01.py": "def build_record():\n    return None\n",
        "pkg/catalog_part_02.py": "\n\n\ndef build_records():\n    return []\n",
    }

    snapshot = _build_readability_snapshot_by_category(tmp_path, source_by_path)
    issue = snapshot["issues"]["confusable_function_names"][0]
    assert issue["id"] == identifier
    assert issue["path"] == "pkg/catalog_part_01.py"
    assert issue["line"] == 1

    excepted_snapshot = _build_readability_snapshot_by_category(
        tmp_path,
        source_by_path,
        exceptions=[identifier],
    )
    assert excepted_snapshot["issue_counts"]["confusable_function_names"] == 0
