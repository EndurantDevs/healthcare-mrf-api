"""Contracts for the provenance-bound healthcare coverage forecast."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
import subprocess
import sys

import pytest


SCRIPTS_DIRECTORY = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIRECTORY))

artifacts = importlib.import_module("coverage_forecast_artifacts")
combine = importlib.import_module("coverage_forecast_combine")
forecast = importlib.import_module("coverage_forecast")
reporting = importlib.import_module("coverage_forecast_reporting")
ratchet = importlib.import_module("coverage_ratchet")
CoverageRatchetError = importlib.import_module("coverage_reports").CoverageRatchetError


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


def _report_config(report_path: Path, report_format: str) -> dict:
    """Build one compact report configuration with the real growth shape."""

    metric_by_name = {"lines": {"covered": 80, "total": 100}}
    if report_format == "coverage.py":
        metric_by_name["branches"] = {"covered": 80, "total": 100}
    policy_by_field = {"branch": report_format == "coverage.py", "coverage": "7.15.2"}
    if report_format == "llvm-cov":
        policy_by_field.update({"cargo_llvm_cov": "0.8.7", "rust": "1.97.1"})
    return {
        "format": report_format,
        "path": str(report_path),
        "scope": {
            "include": ["api/*.py"],
            "exclude": [],
            "policy": policy_by_field,
        },
        "files": ["api/sample.py"],
        "metrics": metric_by_name,
        "growth": {
            "changed_line_divisor": 10,
            "debt_reduction_percent": 1,
            "target_percent_by_metric": {name: 95 for name in metric_by_name},
        },
    }


def _baseline(report_path: Path) -> dict:
    """Return the two-report baseline contract expected by healthcare CI."""

    return {
        "schema_version": 1,
        "reports": {
            "python": _report_config(report_path, "coverage.py"),
            "rust": _report_config(report_path.with_name("test-coverage-rust.json"), "llvm-cov"),
        },
    }


def _artifact_baseline() -> dict:
    """Return the fixed report-path contract used by CI provenance."""

    return _baseline(Path("test-coverage-python.json"))


def _write_python_report(root: Path, covered_count: int) -> Path:
    """Write a compact coverage.py report at the real scope path."""

    source_path = root / "api" / "sample.py"
    source_path.parent.mkdir()
    source_path.write_text("value = 1\n", encoding="utf-8")
    report_path = root / "test-coverage-python.json"
    report_path.write_text(
        json.dumps(
            {
                "files": {
                    str(source_path): {
                        "summary": {
                            "covered_lines": covered_count,
                            "num_statements": 100,
                            "covered_branches": covered_count,
                            "num_branches": 100,
                        },
                        "missing_branches": [[1, 2]],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return report_path


def _write_coverage_data(root: Path, coverage_path: Path) -> None:
    """Write compact coverage.py data without replacing pytest-cov's tracer."""

    coverage_module = importlib.import_module("coverage")
    source_path = root / "api" / "sample.py"
    source_path.parent.mkdir(exist_ok=True)
    source_path.write_text("value = 1\n", encoding="utf-8")
    coverage_data = coverage_module.CoverageData(basename=str(coverage_path))
    coverage_data.add_lines({str(source_path): {1}})
    coverage_data.write()


def _write_shard_artifacts(
    root: Path,
    artifact_directory: Path,
    kind: str,
    base_sha: str,
    head_sha: str,
) -> None:
    """Create one complete synthetic producer family with valid sidecars."""

    artifact_directory.mkdir()
    for shard in artifacts.SHARD_SPEC_BY_KIND[kind]["shards"]:
        coverage_name, provenance_name = artifacts.shard_file_names(kind, shard)
        coverage_path = artifact_directory / coverage_name
        _write_coverage_data(root, coverage_path)
        provenance = artifacts._expected_shard_provenance(
            root, kind, shard, coverage_path, base_sha, head_sha
        )
        (artifact_directory / provenance_name).write_text(json.dumps(provenance), encoding="utf-8")


def _staged_gate(
    root: Path,
    candidate_path: Path,
    reference_path: Path,
    _base_sha: str,
    _report_name: str,
) -> subprocess.CompletedProcess[str]:
    """Run the production ratchet helpers against forecast-staged files."""

    candidate = ratchet._load_baseline(candidate_path)
    reference = ratchet._load_baseline(reference_path)
    errors = ratchet._compare_baselines(candidate, reference, {"python": 17})
    errors.extend(
        ratchet._check_current_report(root, "python", candidate["reports"]["python"])
    )
    stdout = "".join(f"ERROR: {error}\n" for error in errors)
    return subprocess.CompletedProcess(["coverage_ratchet"], int(bool(errors)), stdout, "")


def _run_python_forecast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    covered_count: int,
) -> tuple[int, dict]:
    """Exercise actual report-derived staging with a controlled production gate."""

    report_path = _write_python_report(tmp_path, covered_count)
    candidate = _baseline(report_path)
    reference = _baseline(report_path)
    output_path = tmp_path / "forecast.json"
    monkeypatch.setattr(forecast, "resolve_forecast_base", lambda *_: (BASE_SHA, HEAD_SHA))
    monkeypatch.setattr(forecast, "_baseline", lambda *_: json.loads(json.dumps(candidate)))
    monkeypatch.setattr(forecast, "_base_baseline", lambda *_: json.loads(json.dumps(reference)))
    monkeypatch.setattr(forecast, "_run_ratchet", _staged_gate)
    monkeypatch.setattr(
        reporting,
        "collect_growth_evidence",
        lambda *_: ({"python": 17}, []),
    )
    exit_code = forecast._forecast_one_report(
        tmp_path,
        BASE_SHA,
        "python",
        report_path,
        output_path,
        {"producer_files": {}},
    )
    return exit_code, json.loads(output_path.read_text(encoding="utf-8"))


def test_forecast_stages_report_metrics_and_files_for_the_real_ratchet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stale committed baseline does not make an at-cap CI report falsely red."""

    exit_code, output = _run_python_forecast(tmp_path, monkeypatch, covered_count=81)

    assert exit_code == 0
    assert output["ratchet_exit_code"] == 0
    assert output["ratchet_errors"] == []
    assert output["reports"]["python"]["metrics"]["branches"] == {
        "base_missing": 20,
        "current_missing": 19,
        "effective_missing_cap": 19,
        "margin": 0,
        "required_growth_reduction": 1,
        "target_percent": 95,
    }


def test_forecast_keeps_a_true_report_debt_failure_red(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging live metrics does not weaken the real debt-paydown requirement."""

    exit_code, output = _run_python_forecast(tmp_path, monkeypatch, covered_count=80)

    assert exit_code == 1
    assert output["ratchet_exit_code"] == 1
    assert any("uncovered debt must fall" in error for error in output["ratchet_errors"])
    assert output["reports"]["python"]["metrics"]["lines"]["margin"] == -1


def test_policy_projection_preserves_the_floor_reset_marker(tmp_path: Path) -> None:
    """The healthcare one-time floor-reset contract is never rewritten by forecast."""

    candidate = _baseline(tmp_path / "test-coverage-python.json")
    reference = json.loads(json.dumps(candidate))
    floor_reset_by_field = {
        "maximum_drop_basis_points": 200,
        "reason": "release reset",
        "reference_metrics": reference["reports"]["python"]["metrics"],
    }
    candidate["reports"]["python"]["one_time_floor_reset"] = floor_reset_by_field
    reference["reports"]["python"]["one_time_floor_reset"] = floor_reset_by_field

    projected_candidate, projected_reference = forecast._policy_projection(
        candidate, reference, "python"
    )

    assert (
        projected_candidate["reports"]["python"]["one_time_floor_reset"]
        == floor_reset_by_field
    )
    assert ratchet._compare_baselines(projected_candidate, projected_reference) == []


def test_forecast_refuses_mutable_or_short_base_identifiers(tmp_path: Path) -> None:
    """The CI adapter never falls back to an ambiguous target revision."""

    for value in ("", "HEAD^", "origin/main", "a" * 39):
        with pytest.raises(artifacts.CoverageForecastError, match="SHA"):
            artifacts.resolve_forecast_base(tmp_path, value)


def test_python_artifact_inventory_rejects_an_extra_producer_file(tmp_path: Path) -> None:
    """The eight expected Python producers are an exact, not prefix, contract."""

    baseline_path = tmp_path / artifacts.BASELINE_NAME
    baseline_path.write_text(json.dumps(_artifact_baseline()), encoding="utf-8")
    artifact_directory = tmp_path / "main"
    artifact_directory.mkdir()
    for shard in ("0", "1", "2", "3"):
        coverage_name, provenance_name = artifacts.shard_file_names("main", shard)
        coverage_path = artifact_directory / coverage_name
        coverage_path.write_bytes(b"coverage")
        provenance = artifacts._expected_shard_provenance(
            tmp_path, "main", shard, coverage_path, BASE_SHA, HEAD_SHA
        )
        (artifact_directory / provenance_name).write_text(json.dumps(provenance), encoding="utf-8")
    (artifact_directory / "unexpected").write_text("x", encoding="utf-8")

    with pytest.raises(artifacts.CoverageForecastError, match="exact producer set"):
        artifacts.verify_shard_artifacts(tmp_path, artifact_directory, "main", BASE_SHA, HEAD_SHA)


def test_rust_artifact_inventory_rejects_a_mixed_report_set(tmp_path: Path) -> None:
    """Rust diagnostics cannot consume an unrelated downloaded artifact."""

    baseline_path = tmp_path / artifacts.BASELINE_NAME
    baseline_path.write_text(json.dumps(_artifact_baseline()), encoding="utf-8")
    artifact_directory = tmp_path / "rust"
    artifact_directory.mkdir()
    report_path = artifact_directory / "test-coverage-rust.json"
    report_path.write_text("{}", encoding="utf-8")
    provenance = artifacts._report_provenance(
        tmp_path, "rust", report_path, BASE_SHA, HEAD_SHA
    )
    (artifact_directory / artifacts.report_provenance_name("rust")).write_text(
        json.dumps(provenance), encoding="utf-8"
    )
    (artifact_directory / "other.json").write_text("{}", encoding="utf-8")

    with pytest.raises(artifacts.CoverageForecastError, match="exact report set"):
        artifacts.verify_report_artifact(tmp_path, artifact_directory, "rust", BASE_SHA, HEAD_SHA)


def test_rust_provenance_refuses_a_producer_tool_version_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scanner run on a changed toolchain cannot create an accepted sidecar."""

    (tmp_path / artifacts.BASELINE_NAME).write_text(
        json.dumps(_artifact_baseline()), encoding="utf-8"
    )
    report_path = tmp_path / "test-coverage-rust.json"
    report_path.write_text("{}", encoding="utf-8")
    output_path = tmp_path / artifacts.report_provenance_name("rust")
    monkeypatch.setattr(artifacts, "resolve_forecast_base", lambda *_: (BASE_SHA, HEAD_SHA))

    with pytest.raises(artifacts.CoverageForecastError, match="tool versions differ"):
        artifacts.write_report_provenance(
            tmp_path,
            "rust",
            report_path,
            BASE_SHA,
            output_path,
            "0.8.8",
            "1.97.1",
        )


def test_cli_writes_shard_provenance_without_rust_versions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Python shard producer sends only the arguments its writer accepts."""

    shard_calls: list[tuple[object, ...]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        forecast, "write_shard_provenance", lambda *values: shard_calls.append(values)
    )
    arguments = [
        "write-shard-provenance", "--base", BASE_SHA, "--kind", "main", "--shard", "0",
        "--coverage", "main.coverage", "--output", "main.provenance.json",
    ]

    assert forecast.run_coverage_forecast_cli(arguments) == 0
    assert shard_calls == [
        (tmp_path, "main", "0", Path("main.coverage"), BASE_SHA, Path("main.provenance.json"))
    ]


def test_cli_writes_rust_provenance_with_actual_producer_versions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Rust report writer receives the version pair it must validate."""

    report_calls: list[tuple[object, ...]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        forecast, "write_report_provenance", lambda *values: report_calls.append(values)
    )
    arguments = [
        "write-report-provenance", "--base", BASE_SHA, "--report-name", "rust", "--report",
        "rust.json", "--output", "rust.provenance.json", "--cargo-llvm-cov-version", "0.8.7",
        "--rust-version", "1.97.1",
    ]
    assert forecast.run_coverage_forecast_cli(arguments) == 0
    assert report_calls == [
        (tmp_path, "rust", Path("rust.json"), BASE_SHA, Path("rust.provenance.json"), "0.8.7", "1.97.1")
    ]


def test_python_forecast_combines_only_the_eight_bound_coverage_files(tmp_path: Path) -> None:
    """The healthcare topology replays all four main, capacity, and PG producers."""

    (tmp_path / artifacts.BASELINE_NAME).write_text(
        json.dumps(_artifact_baseline()), encoding="utf-8"
    )
    (tmp_path / "test-coverage.ini").write_text(
        "[run]\nbranch = True\nsource_dirs =\n    api\n\n[report]\ninclude =\n    api/*.py\n",
        encoding="utf-8",
    )
    _write_shard_artifacts(tmp_path, tmp_path / "main", "main", BASE_SHA, HEAD_SHA)
    _write_shard_artifacts(
        tmp_path, tmp_path / "capacity", "capacity", BASE_SHA, HEAD_SHA
    )
    _write_shard_artifacts(tmp_path, tmp_path / "postgres", "postgres", BASE_SHA, HEAD_SHA)

    report_path, producer_files = combine.combine_python_coverage(
        tmp_path,
        tmp_path / "combined",
        tmp_path / "main",
        tmp_path / "capacity",
        tmp_path / "postgres",
        BASE_SHA,
        HEAD_SHA,
    )

    assert report_path.is_file()
    assert producer_files == {
        "main": [f".coverage.main.{index}" for index in range(4)],
        "capacity": [".coverage.capacity"],
        "postgres": [
            ".coverage.postgres.core",
            ".coverage.postgres.provider-directory",
            ".coverage.postgres.provider-profile",
        ],
    }


def test_synthetic_coverage_data_preserves_an_outer_tracer(tmp_path: Path) -> None:
    """Fixture coverage data must not stop the process-wide pytest-cov tracer."""

    child_program = """
import importlib.util
from pathlib import Path
import sys

import coverage

module_path = Path(sys.argv[1])
root = Path(sys.argv[2])
spec = importlib.util.spec_from_file_location("forecast_test_helper", module_path)
module = importlib.util.module_from_spec(spec)
assert spec is not None and spec.loader is not None
spec.loader.exec_module(module)

probe_path = root / "api" / "outer_probe.py"
probe_path.parent.mkdir(parents=True, exist_ok=True)
probe_path.write_text("outer_probe = 1\\n", encoding="utf-8")
outer_path = root / ".coverage.outer"
outer = coverage.Coverage(data_file=str(outer_path), branch=True, source=[str(root / "api")])
outer.start()
module._write_coverage_data(root, root / ".coverage.synthetic")
exec(compile(probe_path.read_text(encoding="utf-8"), str(probe_path), "exec"), {})
outer.stop()
outer.save()

recorded = coverage.CoverageData(basename=str(outer_path))
recorded.read()
if str(probe_path) not in recorded.measured_files():
    raise SystemExit("outer coverage tracer did not record its post-fixture probe")
"""

    subprocess.run(
        [sys.executable, "-c", child_program, str(Path(__file__).resolve()), str(tmp_path)],
        check=True,
    )


def test_diagnostics_rejects_a_report_path_other_than_the_staged_ratchet_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing-arc diagnostics must describe the exact report ratcheted above."""

    report_path = _write_python_report(tmp_path, 81)
    baseline = _baseline(report_path)
    staged = forecast._with_report_snapshot(tmp_path, baseline, "python", report_path)
    reference = forecast._with_report_path(baseline, "python", report_path)
    monkeypatch.setattr(reporting, "collect_growth_evidence", lambda *_: ({"python": 0}, []))

    with pytest.raises(CoverageRatchetError, match="differs from the staged"):
        reporting.build_forecast_diagnostics(
            tmp_path,
            BASE_SHA,
            HEAD_SHA,
            staged,
            reference,
            "python",
            tmp_path / "other.json",
        )
