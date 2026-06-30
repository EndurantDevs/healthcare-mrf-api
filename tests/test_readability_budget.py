import json
import sys
import textwrap
from importlib import util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "readability_budget.py"
SPEC = util.spec_from_file_location("readability_budget", SCRIPT_PATH)
readability_budget = util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = readability_budget
SPEC.loader.exec_module(readability_budget)

NOQA_FIXTURE = "# no" + "qa: E123"


def _write_config(repo_root: Path) -> None:
    config = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
        },
        "inline_suppression_patterns": [
            {"name": "python_noqa", "pattern": "#\\s*noqa\\b"},
        ],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config), encoding="utf-8")


def test_readability_budget_allows_existing_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def existing():
                return 1  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    assert readability_budget.main(["--repo-root", str(repo_root)]) == 0


def test_readability_budget_rejects_new_inline_suppression(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text("def clean():\n    return 1\n", encoding="utf-8")
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0

    module.write_text(
        textwrap.dedent(
            """
            def clean():
                return 1

            def new_debt():
                return 2  {noqa_fixture}
            """
        ).format(noqa_fixture=NOQA_FIXTURE),
        encoding="utf-8",
    )

    assert readability_budget.main(["--repo-root", str(repo_root)]) == 1


def test_readability_budget_reports_long_functions(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def too_long():
                first = 1
                second = 2
                third = 3
                fourth = 4
                return first + second + third + fourth
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["long_functions"] == 1
    assert snapshot["issues"]["long_functions"][0]["function"] == "too_long"


def test_readability_budget_does_not_parse_non_python_files(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "route.rs").write_text(
        "fn main() {\n    println!(\"not python\");\n}\n",
        encoding="utf-8",
    )
    config = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py", ".rs"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
        },
        "inline_suppression_patterns": [],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(repo_root, config)

    assert snapshot["issue_counts"]["syntax_errors"] == 0
