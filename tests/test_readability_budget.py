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
readability_cli = sys.modules["readability.cli"]

NOQA_FIXTURE = "# no" + "qa: E123"
COMMENT_NOISE_FIXTURE = "# return" + " result"


def test_ci_readability_ratchet_requires_python_debt_reduction():
    workflow_text = (
        Path(__file__).resolve().parents[1] / ".github" / "workflows" / "ci.yml"
    ).read_text(encoding="utf-8")

    assert "types: [opened, synchronize, reopened, labeled, unlabeled]" in workflow_text
    assert 'git diff --quiet "$BASE_SHA" HEAD -- \\' in workflow_text
    for source_root in ("api", "db", "process", "service"):
        assert f"':(glob){source_root}/**/*.py'" in workflow_text
    assert "required_reduction_percent=1" in workflow_text
    assert (
        "contains(github.event.pull_request.labels.*.name, "
        "'readability-zero-growth-approved')"
    ) in workflow_text
    assert 'elif [ "$READABILITY_ZERO_GROWTH_APPROVED" = "true" ]; then' in workflow_text
    assert workflow_text.count("required_reduction_percent=0") == 2
    assert '--required-reduction-percent "$required_reduction_percent"' in workflow_text


def _write_config(repo_root: Path) -> None:
    config_dict = {
        "source_roots": ["pkg"],
        "include_suffixes": [".py"],
        "exclude_globs": [],
        "thresholds": {
            "max_file_lines": 8,
            "max_function_lines": 4,
            "max_nesting_depth": 1,
            "max_function_name_tokens": 6,
            "max_class_name_tokens": 6,
            "min_generic_function_lines": 4,
            "min_ambiguous_variable_scope_lines": 4,
            "max_single_letter_scope_lines": 2,
            "min_docstring_function_lines": 4,
            "max_parameters": 3,
            "max_locals": 3,
        },
        "readability": {
            "ambiguous_function_names": ["process_data"],
            "ambiguous_class_names": ["Manager"],
            "ambiguous_variable_names": ["data", "row", "result"],
            "allowed_short_names": ["_", "i"],
            "always_bad_short_names": ["l", "O"],
            "boolean_prefixes": ["is_", "has_", "should_"],
            "dict_name_markers": ["_by_", "_map"],
            "collection_singular_exceptions": ["data"],
            "comment_noise_patterns": ["^return\\b"],
        },
        "inline_suppression_patterns": [
            {"name": "python_noqa", "pattern": "#\\s*noqa\\b"},
        ],
    }
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")


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


def test_readability_budget_attributes_nonlocal_to_nested_scope_only(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def outer():
                count = 0

                def inner():
                    nonlocal count
                    count += 1
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["global_state_usage"] == 1
    assert snapshot["issues"]["global_state_usage"][0]["function"] == "outer.inner"


def test_readability_budget_ignores_response_factory_calls(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def _route_response(response):
                response_headers_by_name = {"Allow": "OPTIONS"}
                preflight_response = response.empty(status=204, headers=response_headers_by_name)
                return preflight_response
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["boolean_name_mismatch"] == 0
    assert snapshot["issue_counts"]["collection_name_mismatch"] == 0


def test_readability_budget_does_not_parse_non_python_files(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "route.rs").write_text(
        "fn main() {\n    println!(\"not python\");\n}\n",
        encoding="utf-8",
    )
    config_dict = {
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
    (repo_root / "readability-budget.json").write_text(json.dumps(config_dict), encoding="utf-8")

    snapshot = readability_budget.build_snapshot(repo_root, config_dict)

    assert snapshot["issue_counts"]["syntax_errors"] == 0


def test_readability_budget_reports_naming_and_contract_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            class Manager:
                pass

            def process_data(a, b, c, d):
                {comment_noise_fixture}
                data = [1, 2, 3]
                row = {{"a": 1}}
                result = a == b
                l = 1
                extra = 2
                another = 3
                return result
            """
        ).format(comment_noise_fixture=COMMENT_NOISE_FIXTURE),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["ambiguous_function_names"] == 1
    assert snapshot["issue_counts"]["ambiguous_variable_names"] == 3
    assert snapshot["issue_counts"]["boolean_name_mismatch"] == 1
    assert snapshot["issue_counts"]["class_name_shape"] == 1
    assert snapshot["issue_counts"]["comment_noise"] == 1
    assert snapshot["issue_counts"]["missing_contract_docstrings"] == 1
    assert snapshot["issue_counts"]["single_letter_names"] == 5
    assert snapshot["issue_counts"]["too_many_locals"] == 1
    assert snapshot["issue_counts"]["too_many_parameters"] == 1


def test_readability_budget_reports_collection_and_global_state_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text(
        textwrap.dedent(
            """
            def build_lookup():
                global CACHE
                names = {"a": 1}
                thing = []
                ...
            """
        ),
        encoding="utf-8",
    )
    _write_config(repo_root)

    snapshot = readability_budget.build_snapshot(
        repo_root,
        json.loads((repo_root / "readability-budget.json").read_text(encoding="utf-8")),
    )

    assert snapshot["issue_counts"]["collection_name_mismatch"] == 2
    assert snapshot["issue_counts"]["global_state_usage"] == 1
    assert snapshot["issue_counts"]["pass_placeholders"] == 1


def test_readability_ratchet_requires_reduction_and_synced_baseline(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text(f"def existing():\n    return 1  {NOQA_FIXTURE}\n", encoding="utf-8")
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    base_baseline = repo_root / "readability-base.json"
    base_baseline.write_text(
        (repo_root / "readability-baseline.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    ratchet_args = [
        "--repo-root",
        str(repo_root),
        "--ratchet-baseline",
        str(base_baseline),
        "--required-reduction-percent",
        "1",
    ]

    assert readability_budget.main(ratchet_args) == 1
    module.write_text("def existing():\n    return 1\n", encoding="utf-8")
    assert readability_budget.main(ratchet_args) == 1
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    assert readability_budget.main(ratchet_args) == 0


def test_readability_ratchet_rejects_replacement_debt(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text(
        f"def old_one():\n    return 1  {NOQA_FIXTURE}\n\n"
        f"def old_two():\n    return 2  {NOQA_FIXTURE}\n",
        encoding="utf-8",
    )
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    base_baseline = repo_root / "readability-base.json"
    base_baseline.write_text(
        (repo_root / "readability-baseline.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    module.write_text(f"def replacement():\n    return 3  {NOQA_FIXTURE}\n", encoding="utf-8")
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0

    assert readability_budget.main(
        ["--repo-root", str(repo_root), "--ratchet-baseline", str(base_baseline)]
    ) == 1


def test_readability_ratchet_allows_ordinary_replacement_with_net_reduction(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text(
        "def first_long_function():\n"
        + "    first_value = 1\n" * 65
        + "\ndef second_long_function():\n"
        + "    second_value = 2\n" * 65,
        encoding="utf-8",
    )
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    base_baseline = repo_root / "readability-base.json"
    base_baseline.write_text(
        (repo_root / "readability-baseline.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    module.write_text(
        "def replacement_long_function():\n" + "    replacement_value = 3\n" * 65,
        encoding="utf-8",
    )
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0

    assert readability_budget.main(
        ["--repo-root", str(repo_root), "--ratchet-baseline", str(base_baseline)]
    ) == 0


def test_readability_ratchet_holds_at_zero(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    (package / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0

    assert readability_budget.main(
        [
            "--repo-root",
            str(repo_root),
            "--ratchet-baseline",
            str(repo_root / "readability-baseline.json"),
        ]
    ) == 0


def test_readability_ratchet_zero_percent_prevents_net_growth(tmp_path):
    repo_root = tmp_path
    package = repo_root / "pkg"
    package.mkdir()
    module = package / "module.py"
    module.write_text(
        f"def existing():\n    return 1  {NOQA_FIXTURE}\n",
        encoding="utf-8",
    )
    _write_config(repo_root)
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    base_baseline = repo_root / "readability-base.json"
    base_baseline.write_text(
        (repo_root / "readability-baseline.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    ratchet_args = [
        "--repo-root",
        str(repo_root),
        "--ratchet-baseline",
        str(base_baseline),
        "--required-reduction-percent",
        "0",
    ]

    assert readability_budget.main(ratchet_args) == 0
    module.write_text(
        f"def existing():\n    return 1  {NOQA_FIXTURE}\n\n"
        f"def added():\n    return 2  {NOQA_FIXTURE}\n",
        encoding="utf-8",
    )
    assert readability_budget.main(["--repo-root", str(repo_root), "--write-baseline"]) == 0
    assert readability_budget.main(ratchet_args) == 1


def _synthetic_readability_snapshot(issue_count):
    issues = [
        {
            "id": f"long_function:pkg/module.py:function_{index}",
            "line": index + 1,
            "path": "pkg/module.py",
        }
        for index in range(issue_count)
    ]
    return {
        "version": 1,
        "rules": {"readability": {}, "thresholds": {}},
        "thresholds": {},
        "issue_counts": {"long_functions": issue_count},
        "issues": {"long_functions": issues},
    }


def _baseline_with_readability_reset(snapshot, reference):
    baseline_by_field = readability_cli._baseline_snapshot(snapshot)
    baseline_by_field["one_time_debt_reset"] = {
        "maximum_increase_basis_points": 200,
        "reason": "PTG V4 adaptive graph representation migration",
        "reference_baseline_sha256": readability_cli._baseline_anchor_sha256(
            reference
        ),
        "reference_total": readability_cli._total_issue_count(reference),
    }
    return baseline_by_field


def test_readability_reset_is_two_percent_base_anchored_and_non_repeatable(tmp_path):
    reference_snapshot = _synthetic_readability_snapshot(50)
    reference_baseline = readability_cli._baseline_snapshot(reference_snapshot)
    reference_path = tmp_path / "readability-base.json"
    reference_path.write_text(json.dumps(reference_baseline), encoding="utf-8")

    boundary_snapshot = _synthetic_readability_snapshot(51)
    boundary_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    assert (
        readability_cli._check_readability_ratchet(
            boundary_snapshot,
            boundary_baseline,
            reference_path,
            1,
        )
        == 0
    )

    over_limit_snapshot = _synthetic_readability_snapshot(52)
    over_limit_baseline = _baseline_with_readability_reset(
        over_limit_snapshot,
        reference_baseline,
    )
    assert (
        readability_cli._check_readability_ratchet(
            over_limit_snapshot,
            over_limit_baseline,
            reference_path,
            1,
        )
        == 1
    )

    stale_anchor_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    stale_anchor_baseline["one_time_debt_reset"][
        "reference_baseline_sha256"
    ] = "0" * 64
    assert (
        readability_cli._check_readability_ratchet(
            boundary_snapshot,
            stale_anchor_baseline,
            reference_path,
            1,
        )
        == 2
    )
    stale_total_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    stale_total_baseline["one_time_debt_reset"]["reference_total"] = 49
    assert (
        readability_cli._check_readability_ratchet(
            boundary_snapshot,
            stale_total_baseline,
            reference_path,
            1,
        )
        == 2
    )

    reference_path.write_text(json.dumps(boundary_baseline), encoding="utf-8")
    repeated_baseline = readability_cli._baseline_snapshot(over_limit_snapshot)
    repeated_baseline["one_time_debt_reset"] = boundary_baseline[
        "one_time_debt_reset"
    ]
    assert (
        readability_cli._check_readability_ratchet(
            over_limit_snapshot,
            repeated_baseline,
            reference_path,
            0,
        )
        == 1
    )


def test_write_readability_baseline_preserves_established_reset_marker(tmp_path):
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "module.py").write_text("def clean():\n    return 1\n", encoding="utf-8")
    _write_config(tmp_path)
    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0

    baseline_path = tmp_path / "readability-baseline.json"
    baseline_by_field = json.loads(baseline_path.read_text(encoding="utf-8"))
    marker_by_field = {
        "maximum_increase_basis_points": 200,
        "reason": "test migration",
        "reference_baseline_sha256": "1" * 64,
        "reference_total": 0,
    }
    baseline_by_field["one_time_debt_reset"] = marker_by_field
    baseline_path.write_text(json.dumps(baseline_by_field), encoding="utf-8")

    assert readability_budget.main(["--repo-root", str(tmp_path), "--write-baseline"]) == 0
    refreshed_by_field = json.loads(baseline_path.read_text(encoding="utf-8"))
    assert refreshed_by_field["one_time_debt_reset"] == marker_by_field


def test_readability_ratchet_allows_one_time_confusable_name_rule_migration():
    base_rules_by_section = {
        "readability": {"ambiguous_function_names": ["process_data"]},
        "thresholds": {"max_function_lines": 60},
    }
    current_rules_by_section = {
        "readability": {
            "ambiguous_function_names": ["process_data"],
            "confusable_function_name_exceptions": [
                "confusable_function_name:pkg/module:<module>:entry:entry|entries"
            ],
        },
        "thresholds": {"max_function_lines": 60},
    }
    base_snapshot_by_field = {"rules": base_rules_by_section, "issue_counts": {"long_functions": 1}}
    current_snapshot_by_field = {"rules": current_rules_by_section, "issue_counts": {"long_functions": 1}}

    assert readability_cli._has_compatible_ratchet_rules_by_section(base_snapshot_by_field, current_snapshot_by_field)

    migrated_base_by_field = dict(base_snapshot_by_field)
    migrated_base_by_field["issue_counts"] = {
        "confusable_function_names": 0,
        "long_functions": 1,
    }
    assert not readability_cli._has_compatible_ratchet_rules_by_section(migrated_base_by_field, current_snapshot_by_field)


def test_protected_issue_comparison_ignores_line_only_relocations():
    baseline_by_field = {
        "issue_ids": {
            "global_state_usage": [
                "global_state_usage:process/example.py:load_rows:global:10:cache"
            ],
            "pass_placeholders": [
                "pass_placeholder:process/example.py:load_rows:pass:20"
            ],
        }
    }
    current_by_field = {
        "issues": {
            "global_state_usage": [
                {
                    "id": "global_state_usage:process/example.py:load_rows:global:14:cache"
                }
            ],
            "pass_placeholders": [
                {
                    "id": "pass_placeholder:process/example.py:load_rows:pass:24"
                }
            ],
        }
    }

    assert readability_cli._new_issues(current_by_field, baseline_by_field) == {}

    new_global_issue_by_field = {
        "id": "global_state_usage:process/example.py:load_rows:global:14:new_cache"
    }
    current_by_field["issues"]["global_state_usage"] = [
        new_global_issue_by_field
    ]
    assert readability_cli._new_issues(current_by_field, baseline_by_field) == {
        "global_state_usage": [new_global_issue_by_field]
    }
