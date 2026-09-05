"""Static contracts for healthcare's CI-equivalent coverage forecast wiring."""

from __future__ import annotations

from pathlib import Path


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


def test_coverage_forecast_requires_one_exact_base_and_head_for_all_producers() -> None:
    """No coverage job may quietly substitute an unrelated target or checkout."""

    workflow = _workflow()

    assert "workflow_dispatch:\n    inputs:\n      base_sha:" in workflow
    assert "COVERAGE_BASE_SHA:" in workflow
    assert "origin/main" not in workflow
    assert "HEAD^" not in workflow
    assert workflow.count("ref: ${{ github.sha }}") >= 5
    assert workflow.count('fetch-depth: 0') >= 6


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
    assert "healthcare-mrf-api-coverage-baseline-${{ github.sha }}" in workflow
    assert "retention-days: 90" in workflow
    assert "name: mrf-coverage-forecast" in workflow
    assert "if: always()" in workflow
