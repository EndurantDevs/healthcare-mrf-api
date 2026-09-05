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
growth = importlib.import_module("coverage_growth")
reports = importlib.import_module("coverage_reports")
CoverageRatchetError = reports.CoverageRatchetError


BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


def _report_config(report_path: Path, report_format: str) -> dict:
    """Build one compact report configuration with the real policy shape."""

    metric_by_name = {"lines": {"covered": 80, "total": 100}}
    if report_format == "coverage.py":
        metric_by_name["branches"] = {"covered": 80, "total": 100}
    policy_by_field = {"branch": report_format == "coverage.py", "coverage": "7.15.2"}
    if report_format == "llvm-cov":
        policy_by_field.update({"cargo_llvm_cov": "0.8.7", "rust": "1.97.1"})
    include = ["api/*.py"]
    files = ["api/sample.py"]
    threshold = 85
    if report_format == "llvm-cov":
        include = ["support/ptg2_scanner/src/*.rs"]
        files = ["support/ptg2_scanner/src/sample.rs"]
        threshold = 80
    return {
        "format": report_format,
        "path": str(report_path),
        "scope": {
            "include": include,
            "exclude": [],
            "policy": policy_by_field,
        },
        "files": files,
        "metrics": metric_by_name,
        "growth": {
            "debt_reduction_percent": 0,
            "diff_coverage_percent": threshold,
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


def test_generated_coverage_docs_reject_reversed_markers() -> None:
    """The docs writer cannot replace an ambiguous or inverted section."""

    with pytest.raises(CoverageRatchetError, match="out of order"):
        reports._updated_docs(
            "<!-- coverage-baseline:end -->\n<!-- coverage-baseline:start -->\n",
            _artifact_baseline(),
        )


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
                        "executed_lines": list(range(1, covered_count + 1)),
                        "missing_lines": list(range(covered_count + 1, 101)),
                        "missing_branches": [[1, 2]],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    return report_path


def _write_rust_report(root: Path, covered_count: int) -> Path:
    """Write a compact full llvm-cov report at the real Rust scope path."""

    source_path = root / "support" / "ptg2_scanner" / "src" / "sample.rs"
    source_path.parent.mkdir(parents=True)
    source_path.write_text("fn sample() {}\n", encoding="utf-8")
    report_path = root / "test-coverage-rust.json"
    report_path.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "files": [
                            {
                                "filename": str(source_path),
                                "segments": [],
                                "summary": {
                                    "lines": {"covered": covered_count, "count": 100}
                                },
                            }
                        ]
                    }
                ]
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
    errors = ratchet._compare_baselines(candidate, reference)
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
        "collect_diff_coverage",
        lambda *_: (
            {
                "python": {
                    "changed": 17,
                    "covered": 17,
                    "total": 17,
                    "percent": 100.0,
                    "threshold": 85,
                    "uncovered_lines": [],
                }
            },
            [],
        ),
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


def test_forecast_stages_report_metrics_and_files_for_the_ratio_ratchet(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A measured ratio above the base remains green without debt headroom."""

    exit_code, output = _run_python_forecast(tmp_path, monkeypatch, covered_count=81)

    assert exit_code == 0
    assert output["ratchet_exit_code"] == 0
    assert output["ratchet_errors"] == []
    assert output["reports"]["python"]["metrics"]["branches"] == {
        "current_covered": 81,
        "current_total": 100,
        "current_percent": 81.0,
        "reference_covered": 80,
        "reference_total": 100,
        "reference_percent": 80.0,
    }


def test_forecast_keeps_a_true_ratio_regression_red(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staging live metrics does not weaken the base ratio floor."""

    exit_code, output = _run_python_forecast(tmp_path, monkeypatch, covered_count=79)

    assert exit_code == 1
    assert output["ratchet_exit_code"] == 1
    assert any("coverage fell" in error for error in output["ratchet_errors"])
    assert output["reports"]["python"]["metrics"]["lines"]["current_percent"] == 79.0


def test_policy_projection_preserves_the_machine_artifact_marker(tmp_path: Path) -> None:
    """The exact-base artifact requirement remains protected by forecast."""

    candidate = _baseline(tmp_path / "test-coverage-python.json")
    reference = json.loads(json.dumps(candidate))
    candidate["machine_artifact_required"] = True
    reference["machine_artifact_required"] = True

    projected_candidate, projected_reference = forecast._policy_projection(
        candidate, reference, "python"
    )

    assert projected_candidate["machine_artifact_required"] is True
    assert ratchet._compare_baselines(projected_candidate, projected_reference) == []


def test_machine_artifact_transition_allows_only_the_coverage_tool_upgrade() -> None:
    """Bootstrap corrects the stale tool pin without opening a lasting bypass."""

    reference = _artifact_baseline()
    candidate = json.loads(json.dumps(reference))
    candidate["machine_artifact_required"] = True
    candidate["reports"]["python"]["scope"]["policy"]["coverage"] = "7.16.0"

    assert ratchet._compare_baselines(candidate, reference) == []
    reference["machine_artifact_required"] = True
    assert "python: measurement policy changed coverage" in ratchet._compare_baselines(
        candidate, reference
    )


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
    monkeypatch.setattr(
        reporting,
        "collect_diff_coverage",
        lambda *_: (
            {
                "python": {
                    "changed": 0,
                    "covered": 0,
                    "total": 0,
                    "percent": 100.0,
                    "threshold": 85,
                    "uncovered_lines": [],
                }
            },
            [],
        ),
    )

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


def test_python_diff_coverage_counts_only_executable_changed_lines(tmp_path: Path) -> None:
    """Blank and comment additions stay outside the Coverage.py denominator."""

    report_path = tmp_path / "coverage.json"
    report_path.write_text(
        json.dumps(
            {
                "files": {
                    "api/sample.py": {
                        "executed_lines": [2],
                        "missing_lines": [],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = _report_config(report_path, "coverage.py")

    result = growth._report_diff_coverage(
        tmp_path,
        "python",
        config,
        {"api/sample.py": {1, 2, 3}},
    )

    assert result == {
        "changed": 3,
        "covered": 1,
        "total": 1,
        "percent": 100.0,
        "threshold": 85,
        "uncovered_lines": [],
    }


def test_diff_coverage_fails_closed_when_a_changed_product_file_is_absent(
    tmp_path: Path,
) -> None:
    """A missing report record cannot turn changed product code into an exemption."""

    report_path = tmp_path / "coverage.json"
    report_path.write_text(json.dumps({"files": {}}), encoding="utf-8")

    with pytest.raises(CoverageRatchetError, match="absent from coverage"):
        growth._report_diff_coverage(
            tmp_path,
            "python",
            _report_config(report_path, "coverage.py"),
            {"api/new_file.py": {1}},
        )


def test_diff_coverage_exempts_only_configured_generated_files(tmp_path: Path) -> None:
    """Checked-in generated data does not create an untestable diff denominator."""

    report_path = tmp_path / "coverage.json"
    report_path.write_text(json.dumps({"files": {}}), encoding="utf-8")
    config = _report_config(report_path, "coverage.py")
    config["growth"]["diff_exclude"] = ["process/ext/address_pub28.py"]

    result = growth._report_diff_coverage(
        tmp_path,
        "python",
        config,
        {"process/ext/address_pub28.py": {1, 2}},
    )

    assert result["changed"] == 0
    assert result["total"] == 0


def test_rust_diff_coverage_uses_full_llvm_segments_at_the_80_percent_boundary(
    tmp_path: Path,
) -> None:
    """LLVM half-open segments produce executable and covered Rust line sets."""

    report_path = tmp_path / "rust.json"
    report_path.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "files": [
                            {
                                "filename": "support/ptg2_scanner/src/sample.rs",
                                "segments": [
                                    [1, 1, 1, True, True, False],
                                    [17, 1, 0, True, True, False],
                                    [21, 1, 0, False, False, False],
                                ],
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    config = _report_config(report_path, "llvm-cov")

    result = growth._report_diff_coverage(
        tmp_path,
        "rust",
        config,
        {"support/ptg2_scanner/src/sample.rs": set(range(1, 21))},
    )

    assert result["covered"] == 16
    assert result["total"] == 20
    assert result["percent"] == 80.0
    assert growth._diff_coverage_errors("rust", result) == []


def test_rust_diff_coverage_matches_llvm_cov_default_test_exclusions(
    tmp_path: Path,
) -> None:
    """Test-only Rust files omitted by cargo-llvm-cov stay outside the denominator."""

    report_path = tmp_path / "rust.json"
    report_path.write_text(json.dumps({"data": [{"files": []}]}), encoding="utf-8")
    config = _report_config(report_path, "llvm-cov")
    nested_test_path = "support/ptg2_scanner/src/main_tests/tests/cases.rs"
    config["scope"]["include"].append("support/ptg2_scanner/src/**/tests/*.rs")
    assert growth._is_path_in_scope(nested_test_path, config)

    result = growth._report_diff_coverage(
        tmp_path,
        "rust",
        config,
        {
            "support/ptg2_scanner/src/main_tests.rs": {1},
            nested_test_path: {1},
        },
    )

    assert result["changed"] == 0
    assert result["total"] == 0


def test_rust_diff_coverage_rejects_malformed_llvm_segments(tmp_path: Path) -> None:
    """Summary-only or malformed LLVM JSON cannot silently pass diff coverage."""

    report_path = tmp_path / "rust.json"
    report_path.write_text(
        json.dumps(
            {
                "data": [
                    {
                        "files": [
                            {
                                "filename": "support/ptg2_scanner/src/sample.rs",
                                "summary": {},
                            }
                        ]
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CoverageRatchetError, match="segments must be a list"):
        growth._report_diff_coverage(
            tmp_path,
            "rust",
            _report_config(report_path, "llvm-cov"),
            {"support/ptg2_scanner/src/sample.rs": {1}},
        )


def test_diff_coverage_failure_lists_every_uncovered_changed_line() -> None:
    """A below-threshold result is actionable in the job log."""

    errors = growth._diff_coverage_errors(
        "rust",
        {
            "covered": 3,
            "total": 5,
            "percent": 60.0,
            "threshold": 80,
            "uncovered_lines": ["src/a.rs:4", "src/a.rs:5"],
        },
    )

    assert errors == [
        "rust: diff coverage 60.00% is below 80% (3/5)",
        "rust: uncovered changed line src/a.rs:4",
        "rust: uncovered changed line src/a.rs:5",
    ]


def test_machine_baseline_bootstraps_once_then_requires_the_exact_base_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a legacy tracked base may proceed without its machine artifact."""

    legacy = _artifact_baseline()
    monkeypatch.setattr(artifacts, "base_baseline", lambda *_: legacy)
    assert artifacts.reference_baseline(tmp_path, BASE_SHA, None) is legacy

    required = json.loads(json.dumps(legacy))
    required["machine_artifact_required"] = True
    monkeypatch.setattr(artifacts, "base_baseline", lambda *_: required)
    with pytest.raises(artifacts.CoverageForecastError, match="requires its 90-day"):
        artifacts.reference_baseline(tmp_path, BASE_SHA, None)

    artifact_path = tmp_path / "machine.json"
    artifact_path.write_text(
        json.dumps({**required, "source_sha": HEAD_SHA}),
        encoding="utf-8",
    )
    with pytest.raises(artifacts.CoverageForecastError, match="source_sha"):
        artifacts.reference_baseline(tmp_path, BASE_SHA, artifact_path)

    artifact_path.write_text(
        json.dumps({**required, "source_sha": BASE_SHA}),
        encoding="utf-8",
    )
    assert artifacts.reference_baseline(tmp_path, BASE_SHA, artifact_path)[
        "source_sha"
    ] == BASE_SHA


def test_measured_machine_baseline_restores_canonical_report_paths(tmp_path: Path) -> None:
    """Temporary combine paths never leak into the reusable main artifact."""

    configured = _artifact_baseline()
    measured = json.loads(json.dumps(configured))
    measured["reports"]["python"]["path"] = "/tmp/combined-python.json"
    measured["reports"]["rust"]["path"] = "/tmp/downloaded-rust.json"
    output_path = tmp_path / "baseline.json"

    forecast._write_measured_baseline(output_path, measured, configured, HEAD_SHA)

    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["source_sha"] == HEAD_SHA
    assert output["machine_artifact_required"] is True
    assert output["reports"]["python"]["path"] == "test-coverage-python.json"
    assert output["reports"]["rust"]["path"] == "test-coverage-rust.json"


def test_combined_forecast_stages_both_reports_and_writes_one_machine_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final coverage owner ratchets and publishes Python and Rust together."""

    python_report = _write_python_report(tmp_path, 81)
    rust_report = _write_rust_report(tmp_path, 82)
    configured = _artifact_baseline()
    reference = json.loads(json.dumps(configured))
    observed_candidates: list[dict] = []

    def run_ratchet(
        _root: Path,
        candidate_path: Path,
        _reference_path: Path,
        _base_sha: str,
        report_name: str | None,
    ) -> subprocess.CompletedProcess[str]:
        assert report_name is None
        observed_candidates.append(json.loads(candidate_path.read_text(encoding="utf-8")))
        return subprocess.CompletedProcess(["coverage_ratchet"], 0, "", "")

    monkeypatch.setattr(forecast, "resolve_forecast_base", lambda *_: (BASE_SHA, HEAD_SHA))
    monkeypatch.setattr(forecast, "_baseline", lambda *_: json.loads(json.dumps(configured)))
    monkeypatch.setattr(
        forecast,
        "load_reference_baseline",
        lambda *_: json.loads(json.dumps(reference)),
    )
    monkeypatch.setattr(
        forecast,
        "combine_python_coverage",
        lambda *_: (python_report, {"main": ["bound"]}),
    )
    monkeypatch.setattr(forecast, "verify_report_artifact", lambda *_: rust_report)
    monkeypatch.setattr(forecast, "_run_ratchet", run_ratchet)
    monkeypatch.setattr(
        forecast,
        "build_combined_forecast_diagnostics",
        lambda *_: {
            "schema_version": 1,
            "base_sha": BASE_SHA,
            "head_sha": HEAD_SHA,
            "reports": {},
            "policy_errors": [],
        },
    )
    monkeypatch.setattr(forecast, "_print_summary", lambda *_: None)
    baseline_output = tmp_path / "machine-baseline.json"

    exit_code = forecast.forecast_coverage(
        tmp_path,
        BASE_SHA,
        tmp_path / "main",
        tmp_path / "capacity",
        tmp_path / "postgres",
        tmp_path / "rust",
        tmp_path / "forecast.json",
        tmp_path / "base-artifact.json",
        baseline_output,
    )

    assert exit_code == 0
    assert observed_candidates[0]["reports"]["python"]["metrics"]["lines"] == {
        "covered": 81,
        "total": 100,
    }
    assert observed_candidates[0]["reports"]["rust"]["metrics"]["lines"] == {
        "covered": 82,
        "total": 100,
    }
    machine = json.loads(baseline_output.read_text(encoding="utf-8"))
    assert machine["source_sha"] == HEAD_SHA
    assert set(machine["reports"]) == {"python", "rust"}
