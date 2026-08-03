"""Static contracts for healthcare's CI-equivalent coverage forecast wiring."""

from __future__ import annotations

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _workflow() -> str:
    """Load the workflow as text so expression syntax needs no YAML extension."""

    return (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(
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
    """The aggregation job accepts exactly the four-plus-one-plus-three producers."""

    workflow = _workflow()

    assert "--kind main" in workflow
    assert "--shard \"${{ matrix.shard-index }}\"" in workflow
    assert "--kind capacity" in workflow
    assert "--shard capacity" in workflow
    assert "--kind postgres" in workflow
    assert "--shard \"${{ matrix.shard }}\"" in workflow
    assert ".coverage-provenance.main.${{ matrix.shard-index }}.json" in workflow
    assert ".coverage-provenance.capacity.json" in workflow
    assert ".coverage-provenance.postgres.${{ matrix.shard }}.json" in workflow
    assert "forecast-python" in workflow
    assert "timeout --foreground 295s python scripts/coverage_forecast.py forecast-python" in workflow
    assert "--main-artifacts coverage-data/main" in workflow
    assert "--capacity-artifacts coverage-data/capacity" in workflow
    assert "--postgres-artifacts coverage-data/postgres" in workflow


def test_coverage_forecast_keeps_rust_separate_and_uploads_diagnostics() -> None:
    """Rust stays independently ratcheted while both forecast artifacts survive failure."""

    workflow = _workflow()

    assert "write-report-provenance" in workflow
    assert "coverage-artifacts/rust/test-coverage-rust.json" in workflow
    assert "coverage-artifacts/rust/coverage-provenance-rust.json" in workflow
    assert "forecast-rust" in workflow
    assert "timeout --foreground 295s python scripts/coverage_forecast.py forecast-rust" in workflow
    assert "--cargo-llvm-cov-version \"$(cargo llvm-cov --version" in workflow
    assert "$(cargo-llvm-cov --version" not in workflow
    assert "--rust-version \"$(rustc --version" in workflow
    assert "mrf-python-coverage-forecast" in workflow
    assert "mrf-rust-coverage-forecast" in workflow
    assert "if: always()" in workflow
