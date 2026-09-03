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
    """The aggregation job accepts exactly the five-plus-one-plus-three producers."""

    workflow = _workflow()
    prepush = _prepush()

    assert "--kind main" in prepush
    assert 'python-main "${{ matrix.shard-index }}" 5' in workflow
    assert "scripts/ci/prepush python-coverage 5" in workflow
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


def test_coverage_forecast_keeps_rust_separate_and_uploads_diagnostics() -> None:
    """Rust stays independently ratcheted while both forecast artifacts survive failure."""

    workflow = _workflow()
    prepush = _prepush()

    assert "write-report-provenance" in prepush
    assert "coverage-artifacts/rust" in prepush
    assert "test-coverage-rust.json" in prepush
    assert "coverage-provenance-rust.json" in prepush
    assert "forecast-rust" in prepush
    assert "timeout --foreground 295s python scripts/coverage_forecast.py forecast-rust" in prepush
    assert "--cargo-llvm-cov-version \"$(cargo llvm-cov --version" in prepush
    assert "$(cargo-llvm-cov --version" not in prepush
    assert "--rust-version \"$(rustc --version" in prepush
    assert "mrf-python-coverage-forecast" in workflow
    assert "mrf-rust-coverage-forecast" in workflow
    assert "if: always()" in workflow
