"""Contracts for healthcare's CI-equivalent coverage forecast wiring."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest
import yaml


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _workflow() -> str:
    """Load the workflow as text so expression syntax needs no YAML extension."""

    return (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )


def _prepush() -> str:
    """Load the executable shared by local pre-push and the workflow."""

    return (REPOSITORY_ROOT / "scripts" / "ci" / "prepush").read_text(
        encoding="utf-8"
    )


def _trusted_caller() -> str:
    return (
        REPOSITORY_ROOT / ".github" / "workflows" / "trusted-pr-ci.yml"
    ).read_text(encoding="utf-8")


def _base_coverage_run(**overrides) -> dict:
    return {
        "id": 101, "head_sha": "1" * 40, "head_branch": "main",
        "event": "push", "status": "completed", "conclusion": "success",
        "run_started_at": "2026-01-01T00:00:00Z", **overrides,
    }


def _run_base_resolution(tmp_path, responses):
    """Execute the real workflow step with local API replies and a virtual wait."""
    response_path = tmp_path / "responses"
    response_path.write_text("\n".join(
        "API_ERROR" if response is None else json.dumps({
            "workflow_runs": response if isinstance(response, list) else [response]
        })
        for response in responses
    ) + "\n")
    call_path = tmp_path / "calls"
    poll_path = tmp_path / "polls"
    output_path = tmp_path / "output"
    call_path.write_text("0")
    poll_path.write_text("")
    output_path.write_text("")
    step = next(
        entry for entry in yaml.safe_load(_workflow())["jobs"]["test-coverage"]["steps"]
        if entry.get("name") == "Resolve exact base coverage run"
    )
    harness = r'''
git() { printf '%s\n' '{"machine_artifact_required":true}'; }
curl() {
  call_count=$(($(cat "$CALL_PATH") + 1))
  printf '%s' "$call_count" > "$CALL_PATH"
  response=$(sed -n "${call_count}p" "$RESPONSE_PATH")
  if [ "$response" = API_ERROR ]; then echo "synthetic API failure" >&2; return 22; fi
  printf '%s\n' "$response"
}
sleep() { printf '%s\n' "$1" >> "$POLL_PATH"; SECONDS=$((SECONDS + $1)); }
'''
    completed = subprocess.run(
        ["bash", "-e", "-o", "pipefail", "-c", harness + step["run"]],
        env={**os.environ, "BASE_SHA": "1" * 40, "GH_TOKEN": "synthetic-token",
             "GITHUB_API_URL": "https://api.example.invalid", "GITHUB_REPOSITORY": "example/repo",
             "GITHUB_OUTPUT": str(output_path), "RESPONSE_PATH": str(response_path),
             "CALL_PATH": str(call_path), "POLL_PATH": str(poll_path)},
        capture_output=True, text=True, timeout=5,
    )
    return completed, output_path.read_text(), int(call_path.read_text()), poll_path.read_text().splitlines()


@pytest.mark.parametrize(("responses", "error", "poll_count"), [
    ([_base_coverage_run()], None, 0),
    ([_base_coverage_run(status="queued", conclusion=None), _base_coverage_run()], None, 1),
    ([_base_coverage_run(status="waiting", conclusion=None), _base_coverage_run()], None, 1),
    ([_base_coverage_run(head_sha="2" * 40), _base_coverage_run()], None, 1),
    ([_base_coverage_run(head_sha="2" * 40, status="in_progress", conclusion=None),
      _base_coverage_run()], None, 1),
    ([[], _base_coverage_run()], None, 1),
    ([_base_coverage_run(event="workflow_dispatch"), _base_coverage_run()], None, 1),
    ([_base_coverage_run(head_branch="feature"), _base_coverage_run()], None, 1),
    ([_base_coverage_run(conclusion="failure")], "no successful exact-base", 0),
    ([_base_coverage_run(status="in_progress", conclusion=None),
      _base_coverage_run(conclusion="cancelled")], "no successful exact-base", 1),
    ([None], "synthetic API failure", 0),
    ([_base_coverage_run(status="in_progress", conclusion=None)] * 18,
     "timed out waiting for exact-base CI run", 18),
])
def test_base_coverage_waits_only_for_exact_producer(tmp_path, responses, error, poll_count):
    completed, output, call_count, polls = _run_base_resolution(tmp_path, responses)
    if error and error.startswith("timed out"):
        assert 0 < len(polls) <= poll_count
        assert call_count == len(polls)
    else:
        assert len(polls) == poll_count
        assert call_count == len(responses)
    assert all(0 < int(seconds) <= 10 for seconds in polls)
    if error is None:
        assert completed.returncode == 0, completed.stderr
        assert "run_id=101\n" in output
        assert "reference_baseline=coverage-data/baseline/test-coverage-baseline.json\n" in output
    else:
        assert completed.returncode != 0
        assert error in completed.stderr
        assert "run_id=" not in output


def test_coverage_forecast_requires_one_exact_base_and_head_for_all_producers() -> None:
    """No coverage job may quietly substitute an unrelated target or checkout."""

    workflow = _workflow()

    assert "workflow_dispatch:\n    inputs:\n      base_sha:" in workflow
    assert "COVERAGE_BASE_SHA:" in workflow
    assert "origin/main" not in workflow
    assert "HEAD^" not in workflow
    assert workflow.count("ref: ${{ github.sha }}") >= 5
    assert workflow.count('fetch-depth: 0') >= 6
    assert '--data-urlencode "head_sha=$BASE_SHA"' in workflow
    assert "--data-urlencode status=success" not in workflow
    assert "--data-urlencode per_page=100" in workflow
    assert "sort_by([.run_started_at, .id])" in workflow
    assert 'elif length == 0 then "pending"' in workflow
    assert 'error("no successful exact-base CI run")' in workflow
    assert "expected exactly one successful exact-base CI run" not in workflow
    assert 'echo "base_sha=$BASE_SHA" >> "$GITHUB_OUTPUT"' in workflow
    assert (
        "healthcare-mrf-api-coverage-baseline-"
        "${{ steps.base-coverage.outputs.base_sha }}" in workflow
    )


def test_coverage_forecast_binds_every_healthcare_python_producer() -> None:
    """The aggregation job accepts exactly the four-plus-one-plus-three producers."""

    workflow = _workflow()
    prepush = _prepush()

    assert "--kind main" in prepush
    assert 'python-main "${{ matrix.shard-index }}"' in workflow
    assert "--kind capacity" in prepush
    assert "--shard capacity" in prepush
    assert "--kind postgres" in prepush
    assert 'postgres "${{ matrix.shard }}"' in workflow
    assert ".coverage-provenance.main.${{ matrix.shard-index }}.json" in workflow
    assert ".coverage-provenance.capacity.json" in workflow
    assert ".coverage-provenance.postgres.${{ matrix.shard }}.json" in workflow
    assert "forecast-python" in prepush
    assert "timeout --foreground 295s python scripts/coverage_forecast.py forecast-python" in prepush
    assert "PREPUSH_MAIN_ARTIFACTS:-coverage-data/main" in prepush
    assert "PREPUSH_CAPACITY_ARTIFACTS:-coverage-data/capacity" in prepush
    assert "PREPUSH_POSTGRES_ARTIFACTS:-coverage-data/postgres" in prepush


def test_coverage_forecast_combines_full_rust_data_and_publishes_a_machine_baseline() -> None:
    """Rust stays a separate producer but joins Python in the final policy owner."""

    workflow = _workflow()
    prepush = _prepush()

    assert "write-report-provenance" in prepush
    assert "coverage-artifacts/rust" in prepush
    assert "test-coverage-rust.json" in prepush
    assert "coverage-provenance-rust.json" in prepush
    assert "--all-targets --features python --json" in prepush
    assert "--summary-only" not in prepush
    assert "timeout --foreground 295s python scripts/coverage_forecast.py forecast \\" in prepush
    assert "--rust-artifacts" in prepush
    assert "--baseline-output" in prepush
    assert "PREPUSH_REFERENCE_BASELINE" in prepush
    assert "--cargo-llvm-cov-version \"$(cargo llvm-cov --version" in prepush
    assert "$(cargo-llvm-cov --version" not in prepush
    assert "--rust-version \"$(rustc --version" in prepush
    assert "name: mrf-rust-coverage" in workflow
    assert "Download Rust coverage data" in workflow
    assert "Resolve exact base coverage run" in workflow
    assert "actions: read" in _trusted_caller()
    assert "packages: read" in _trusted_caller()
    assert "healthcare-mrf-api-coverage-baseline-${{ github.sha }}" in workflow
    assert "retention-days: 90" in workflow
    assert "name: mrf-coverage-forecast" in workflow
    assert 'git show "$COVERAGE_BASE_SHA:test-coverage-baseline.json"' in prepush
    assert 'if [ "$machine_artifact_required" = true ]; then' in prepush
    assert "combined coverage requires the exact Rust report artifact" in prepush
    assert "machine_artifact_required" in workflow
    assert "if: always()" in workflow
