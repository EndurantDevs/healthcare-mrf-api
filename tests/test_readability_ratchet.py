# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused contracts for baseline-only readability comparisons."""

from __future__ import annotations

from scripts.readability import cli as readability_cli


def test_protected_issue_comparison_ignores_line_only_relocations():
    baseline = {
        "issue_ids": {
            "global_state_usage": [
                "global_state_usage:process/example.py:load_rows:global:10:cache"
            ],
            "pass_placeholders": [
                "pass_placeholder:process/example.py:load_rows:pass:20"
            ],
        }
    }
    current = {
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

    assert readability_cli._new_issues(current, baseline) == {}

    new_global_issue = {
        "id": "global_state_usage:process/example.py:load_rows:global:14:new_cache"
    }
    current["issues"]["global_state_usage"] = [new_global_issue]
    assert readability_cli._new_issues(current, baseline) == {
        "global_state_usage": [new_global_issue]
    }


def test_soft_file_length_findings_never_block():
    current = {
        "issues": {
            "long_files": [
                {"id": "long_file:process/example.py", "path": "process/example.py"}
            ]
        }
    }

    assert readability_cli._new_issues(current, {"issue_ids": {}}) == {}
