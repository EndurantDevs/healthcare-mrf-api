"""Run the complete aggregation CLI with only coverage.py and the standard library."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import venv

import coverage

from tests.test_coverage_forecast import (
    SCRIPTS_DIRECTORY,
    _artifact_baseline,
    _write_rust_report,
    _write_shard_artifacts,
    artifacts,
)


def _run(root: Path, environment: dict[str, str], *command: str):
    return subprocess.run(
        command, cwd=root, env=environment, capture_output=True, text=True, timeout=30,
    )


def _git(root: Path, environment: dict[str, str], *arguments: str) -> str:
    completed = _run(
        root, environment, "git", "-c", "user.name=Synthetic CI",
        "-c", "user.email=synthetic@example.invalid", "-c", "commit.gpgsign=false",
        "-c", f"core.hooksPath={os.devnull}", *arguments,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout.strip()


def _prepare_repository(root: Path, environment: dict[str, str]) -> tuple[str, str, Path]:
    """Reuse producer fixtures, then bind their sidecars to two real commits."""
    root.mkdir()
    scripts = root / "scripts"
    scripts.mkdir()
    for path in SCRIPTS_DIRECTORY.glob("coverage_*.py"):
        shutil.copyfile(path, scripts / path.name)
    baseline = _artifact_baseline()
    baseline["machine_artifact_required"] = True
    baseline["reports"]["python"]["metrics"].pop("branches")
    for report in baseline["reports"].values():
        report["scope"]["policy"]["coverage"] = coverage.__version__
    (root / artifacts.BASELINE_NAME).write_text(json.dumps(baseline))
    (root / "test-coverage.ini").write_text(
        "[run]\nbranch = True\nsource_dirs =\n    api\n"
        "[report]\ninclude =\n    api/*.py\n"
    )
    originals = root / "originals"
    originals.mkdir()
    for kind in artifacts.SHARD_SPEC_BY_KIND:
        _write_shard_artifacts(root, originals / kind, kind, "a" * 40, "b" * 40)
    rust_report = _write_rust_report(root, 100)
    rust_directory = originals / "rust"
    rust_directory.mkdir()
    rust_report = Path(shutil.move(rust_report, rust_directory / rust_report.name))
    _git(root, environment, "init", "--quiet", "--template=")
    _git(root, environment, "add", "scripts", "api", "support", "test-coverage.ini", artifacts.BASELINE_NAME)
    _git(root, environment, "commit", "--quiet", "-m", "Synthetic coverage base")
    base_sha = _git(root, environment, "rev-parse", "HEAD")
    (root / "api" / "sample.py").write_text("value = 2\n")
    _git(root, environment, "add", "api/sample.py")
    _git(root, environment, "commit", "--quiet", "-m", "Synthetic covered change")
    head_sha = _git(root, environment, "rev-parse", "HEAD")
    for kind, specification in artifacts.SHARD_SPEC_BY_KIND.items():
        for shard in specification["shards"]:
            coverage_name, provenance_name = artifacts.shard_file_names(kind, shard)
            provenance = artifacts._expected_shard_provenance(
                root, kind, shard, originals / kind / coverage_name, base_sha, head_sha,
            )
            (originals / kind / provenance_name).write_text(json.dumps(provenance))
    artifacts.write_report_provenance(
        root, "rust", rust_report, base_sha,
        rust_directory / artifacts.report_provenance_name("rust"), "0.8.7", "1.97.1",
    )
    reference = root / "reference.json"
    reference.write_text(json.dumps({**baseline, "source_sha": base_sha}))
    return base_sha, head_sha, reference


def _coverage_only_python(tmp_path: Path, environment: dict[str, str]) -> str:
    """Create a child interpreter whose only installed package is coverage.py."""
    lean_environment = tmp_path / "lean"
    venv.EnvBuilder(with_pip=False).create(lean_environment)
    python = str(lean_environment / "bin" / "python")
    site_result = _run(tmp_path, environment, python, "-I", "-c",
                       "import sysconfig; print(sysconfig.get_path('purelib'))")
    assert site_result.returncode == 0, site_result.stderr
    site = Path(site_result.stdout.strip())
    (site / "coverage").symlink_to(Path(coverage.__file__).resolve().parent, target_is_directory=True)
    assert {path.name for path in site.iterdir()} == {"coverage"}
    return python


def _assert_forecast_outputs(
    root: Path, environment: dict[str, str], python: str, output: Path,
    base_sha: str, head_sha: str,
) -> None:
    """Verify source-bound diagnostics, canonical baseline paths, and rendered docs."""
    forecast = json.loads((output / "forecast.json").read_text())
    assert forecast["base_sha"] == base_sha and forecast["head_sha"] == head_sha
    assert forecast["ratchet_exit_code"] == 0 and forecast["ratchet_errors"] == []
    assert forecast["reports"]["python"]["diff_coverage"]["covered"] == 1
    assert set(forecast["reports"]) == {"python", "rust"}
    assert {kind: len(files) for kind, files in forecast["inputs"]["python_producer_files"].items()} == {
        "main": 4, "capacity": 1, "postgres": 3,
    }
    baseline = json.loads((output / "baseline.json").read_text())
    assert baseline["source_sha"] == head_sha and baseline["machine_artifact_required"] is True
    for name, report in baseline["reports"].items():
        assert report["path"] == f"test-coverage-{name}.json"
    docs = output / "coverage.md"
    docs.write_text("<!-- coverage-baseline:start -->\n<!-- coverage-baseline:end -->\n")
    rendered = _run(root, environment, python, "scripts/coverage_reports.py",
                    "--baseline", str(output / "baseline.json"), "--docs", str(docs),
                    "--write-docs", "--check")
    assert rendered.returncode == 0, rendered.stdout + rendered.stderr
    assert "### Python" in docs.read_text() and "### Rust" in docs.read_text()


def test_forecast_and_docs_run_in_a_coverage_only_environment(tmp_path: Path) -> None:
    """The real gate succeeds, rejects tampering, and preserves reusable input copies."""
    environment_by_name = {
        key: environment_value for key, environment_value in os.environ.items()
        if not key.startswith(("PYTHON", "GIT_"))
    }
    environment_by_name.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1", PYTHONPATH="")
    python = _coverage_only_python(tmp_path, environment_by_name)
    root = tmp_path / "repository"
    base_sha, head_sha, reference = _prepare_repository(root, environment_by_name)
    originals = root / "originals"
    original_hash_by_path = {
        path.relative_to(originals): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in originals.rglob("*") if path.is_file()
    }
    temporary = tmp_path / "aggregation-temp"
    temporary.mkdir()
    environment_by_name["TMPDIR"] = str(temporary)
    for tampered in (False, True):
        inputs = root / ("tampered-inputs" if tampered else "inputs")
        shutil.copytree(originals, inputs)
        if tampered:
            sidecar = inputs / "main" / ".coverage-provenance.main.0.json"
            document = json.loads(sidecar.read_text())
            document["head_sha"] = "0" * 40
            sidecar.write_text(json.dumps(document))
        output = root / ("tampered-output" if tampered else "output")
        commands = [python, "scripts/coverage_forecast.py", "forecast", "--base", base_sha,
                   "--reference-baseline", str(reference), "--output", str(output / "forecast.json"),
                   "--baseline-output", str(output / "baseline.json")]
        for kind in (*artifacts.SHARD_SPEC_BY_KIND, "rust"):
            commands.extend((f"--{kind}-artifacts", str(inputs / kind)))
        completed = _run(root, environment_by_name, *commands)
        forecast = json.loads((output / "forecast.json").read_text())
        assert not list(temporary.iterdir()), "aggregation leaked temporary report files"
        if tampered:
            assert completed.returncode == 2, completed.stdout + completed.stderr
            assert "coverage provenance differs from the exact CI input" in forecast["error"]
            assert not (output / "baseline.json").exists()
        else:
            assert completed.returncode == 0, completed.stdout + completed.stderr
            assert "Test coverage satisfies ratio and changed-line coverage policy." in completed.stdout
            _assert_forecast_outputs(root, environment_by_name, python, output, base_sha, head_sha)
    assert original_hash_by_path == {
        path.relative_to(originals): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in originals.rglob("*") if path.is_file()
    }
