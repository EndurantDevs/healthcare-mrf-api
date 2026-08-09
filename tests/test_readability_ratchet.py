# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused tests split from a shared contract fixture module."""

from __future__ import annotations

from tests.test_readability_budget import (
    NOQA_FIXTURE,
    _write_config,
    json,
    readability_budget,
    readability_cli,
)


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


def _assert_reset_ratchet(
    snapshot,
    baseline,
    reference_path,
    expected_status,
    required_reduction=1,
):
    assert (
        readability_cli._check_readability_ratchet(
            snapshot,
            baseline,
            reference_path,
            required_reduction,
        )
        == expected_status
    )


def _assert_stale_reset_anchors(
    boundary_snapshot,
    reference_baseline,
    reference_path,
):
    stale_anchor_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    stale_anchor_baseline["one_time_debt_reset"][
        "reference_baseline_sha256"
    ] = "0" * 64
    _assert_reset_ratchet(
        boundary_snapshot,
        stale_anchor_baseline,
        reference_path,
        2,
    )
    stale_total_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    stale_total_baseline["one_time_debt_reset"]["reference_total"] = 49
    _assert_reset_ratchet(
        boundary_snapshot,
        stale_total_baseline,
        reference_path,
        2,
    )


def test_readability_reset_is_two_percent_base_anchored_and_non_repeatable(tmp_path):
    """Reject stale, repeated, or incorrectly sized readability resets."""

    reference_snapshot = _synthetic_readability_snapshot(50)
    reference_baseline = readability_cli._baseline_snapshot(reference_snapshot)
    reference_path = tmp_path / "readability-base.json"
    reference_path.write_text(json.dumps(reference_baseline), encoding="utf-8")

    boundary_snapshot = _synthetic_readability_snapshot(51)
    boundary_baseline = _baseline_with_readability_reset(
        boundary_snapshot,
        reference_baseline,
    )
    _assert_reset_ratchet(
        boundary_snapshot,
        boundary_baseline,
        reference_path,
        0,
    )

    over_limit_snapshot = _synthetic_readability_snapshot(52)
    over_limit_baseline = _baseline_with_readability_reset(
        over_limit_snapshot,
        reference_baseline,
    )
    _assert_reset_ratchet(
        over_limit_snapshot,
        over_limit_baseline,
        reference_path,
        1,
    )

    _assert_stale_reset_anchors(
        boundary_snapshot,
        reference_baseline,
        reference_path,
    )

    reference_path.write_text(json.dumps(boundary_baseline), encoding="utf-8")
    repeated_baseline = readability_cli._baseline_snapshot(over_limit_snapshot)
    repeated_baseline["one_time_debt_reset"] = boundary_baseline[
        "one_time_debt_reset"
    ]
    _assert_reset_ratchet(
        over_limit_snapshot,
        repeated_baseline,
        reference_path,
        1,
        required_reduction=0,
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
