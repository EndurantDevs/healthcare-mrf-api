"""Bind healthcare coverage artifacts to one exact source and target base."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from coverage import __version__ as coverage_package_version

from coverage_reports import CoverageRatchetError


BASELINE_NAME = "test-coverage-baseline.json"
PROVENANCE_SCHEMA_VERSION = 1
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")

REPORT_SPEC_BY_NAME = {
    "python": {
        "format": "coverage.py",
        "path": "test-coverage-python.json",
    },
    "rust": {
        "format": "llvm-cov",
        "path": "test-coverage-rust.json",
    },
}
RUST_TOOL_VERSION_FIELDS = ("cargo_llvm_cov", "rust")

SHARD_SPEC_BY_KIND = {
    "main": {
        "coverage_prefix": ".coverage.main.",
        "provenance_prefix": ".coverage-provenance.main.",
        "report_name": "python",
        "shards": ("0", "1", "2", "3"),
    },
    "capacity": {
        "coverage_prefix": ".coverage.capacity",
        "provenance_prefix": ".coverage-provenance.capacity",
        "report_name": "python",
        "shards": ("capacity",),
    },
    "postgres": {
        "coverage_prefix": ".coverage.postgres.",
        "provenance_prefix": ".coverage-provenance.postgres.",
        "report_name": "python",
        "shards": ("core", "provider-directory", "provider-profile"),
    },
}


class CoverageForecastError(CoverageRatchetError):
    """Raised when forecast inputs cannot reproduce the CI measurement."""


def _git_output(root: Path, *arguments: str) -> str:
    """Return normalized Git output or a controlled forecast error."""

    try:
        completed = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageForecastError(
            "could not resolve coverage forecast Git identity: "
            + " ".join(arguments)
        ) from exc
    return completed.stdout.strip()


def _normalize_sha(value: str, label: str) -> str:
    """Validate a full lower-case Git SHA used by this contract."""

    normalized = value.strip().lower()
    if SHA_PATTERN.fullmatch(normalized) is None:
        raise CoverageForecastError(f"{label} must be a lowercase 40-character SHA")
    return normalized


def resolve_forecast_base(root: Path, base_revision: str) -> tuple[str, str]:
    """Require the checked out head to contain the exact target base."""

    requested_base_sha = _normalize_sha(base_revision, "target base SHA")
    base_sha = _normalize_sha(
        _git_output(root, "rev-parse", f"{requested_base_sha}^{{commit}}"),
        "resolved base SHA",
    )
    head_sha = _normalize_sha(_git_output(root, "rev-parse", "HEAD"), "HEAD SHA")
    merge_base = _normalize_sha(
        _git_output(root, "merge-base", base_sha, head_sha),
        "merge base SHA",
    )
    if merge_base != base_sha:
        raise CoverageForecastError(
            "coverage forecast head does not contain the exact target base"
        )
    return base_sha, head_sha


def _read_json(path: Path, label: str) -> dict[str, Any]:
    """Load one JSON object with a stable, actionable error."""

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoverageForecastError(f"invalid {label}: {path}") from exc
    if not isinstance(document, dict):
        raise CoverageForecastError(f"{label} must be a JSON object: {path}")
    return document


def load_baseline_bytes(raw_document: bytes, label: str) -> dict[str, Any]:
    """Load the versioned baseline shape from Git or an artifact."""

    try:
        document = json.loads(raw_document.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CoverageForecastError(f"{label} is not valid JSON") from exc
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise CoverageForecastError(f"{label} has an unsupported schema version")
    reports_by_name = document.get("reports")
    if not isinstance(reports_by_name, dict) or not reports_by_name:
        raise CoverageForecastError(f"{label} has no coverage reports")
    return document


def load_baseline(path: Path) -> dict[str, Any]:
    """Load one baseline file with a controlled missing-file error."""

    try:
        raw_document = path.read_bytes()
    except OSError as exc:
        raise CoverageForecastError(f"coverage baseline is unavailable: {path}") from exc
    return load_baseline_bytes(raw_document, str(path))


def base_baseline(root: Path, base_sha: str) -> dict[str, Any]:
    """Read the tracked coverage baseline from the exact target commit."""

    try:
        completed = subprocess.run(
            ["git", "show", f"{base_sha}:{BASELINE_NAME}"],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CoverageForecastError(
            "target base has no readable coverage baseline"
        ) from exc
    return load_baseline_bytes(completed.stdout, f"{base_sha}:{BASELINE_NAME}")


def reference_baseline(
    root: Path,
    base_sha: str,
    artifact_path: Path | None,
) -> dict[str, Any]:
    """Load the exact-base machine artifact, with one legacy bootstrap."""

    tracked = base_baseline(root, base_sha)
    if tracked.get("machine_artifact_required") is not True:
        return tracked
    if artifact_path is None:
        raise CoverageForecastError(
            f"base {base_sha} requires its 90-day coverage baseline artifact"
        )
    artifact = load_baseline(artifact_path)
    if artifact.get("source_sha") != base_sha:
        raise CoverageForecastError(
            f"coverage baseline artifact source_sha must equal base {base_sha}"
        )
    return artifact


def _sha256_file(path: Path) -> str:
    """Return a regular-file SHA-256 digest without loading it all at once."""

    digest = hashlib.sha256()
    try:
        with path.open("rb") as source_file:
            for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CoverageForecastError(f"missing coverage artifact: {path}") from exc
    return digest.hexdigest()


def _baseline(root: Path) -> dict[str, Any]:
    """Load and validate the current versioned coverage baseline."""

    baseline = load_baseline(root / BASELINE_NAME)
    reports = baseline.get("reports")
    if not isinstance(reports, dict):
        raise CoverageForecastError("coverage baseline reports are malformed")
    if set(reports) != set(REPORT_SPEC_BY_NAME):
        raise CoverageForecastError(
            "coverage baseline reports differ from the forecast artifact contract"
        )
    for report_name, expected in REPORT_SPEC_BY_NAME.items():
        report = reports.get(report_name)
        if not isinstance(report, dict):
            raise CoverageForecastError(f"coverage baseline is missing {report_name}")
        for field_name, expected_value in expected.items():
            if report.get(field_name) != expected_value:
                raise CoverageForecastError(
                    f"{report_name}: baseline {field_name} differs from "
                    "the forecast artifact contract"
                )
    return baseline


def _shard_spec(kind: str) -> dict[str, Any]:
    """Return one fixed producer family specification."""

    try:
        return SHARD_SPEC_BY_KIND[kind]
    except KeyError as exc:
        raise CoverageForecastError(f"unsupported coverage producer kind: {kind}") from exc


def _validate_shard(kind: str, shard: str) -> dict[str, Any]:
    """Validate one stable producer identifier before using it in a path."""

    specification = _shard_spec(kind)
    if shard not in specification["shards"]:
        raise CoverageForecastError(f"{kind}: unsupported coverage shard {shard!r}")
    return specification


def shard_file_names(kind: str, shard: str) -> tuple[str, str]:
    """Return the exact coverage and provenance filenames for one shard."""

    specification = _validate_shard(kind, shard)
    if kind == "capacity":
        return (
            specification["coverage_prefix"],
            f"{specification['provenance_prefix']}.json",
        )
    return (
        f"{specification['coverage_prefix']}{shard}",
        f"{specification['provenance_prefix']}{shard}.json",
    )


def _coverage_version() -> str:
    """Return the exact coverage.py version used by this CI invocation."""

    return coverage_package_version


def _rust_tool_versions(
    baseline: dict[str, Any], supplied_versions: dict[str, str] | None = None
) -> dict[str, str]:
    """Bind Rust reports to the versions frozen by the Rust baseline policy."""

    try:
        policy = baseline["reports"]["rust"]["scope"]["policy"]
    except (KeyError, TypeError) as exc:
        raise CoverageForecastError("rust: baseline measurement policy is malformed") from exc
    expected_versions_by_name = {
        field: policy.get(field) for field in RUST_TOOL_VERSION_FIELDS
    }
    if not all(
        isinstance(value, str) and value
        for value in expected_versions_by_name.values()
    ):
        raise CoverageForecastError("rust: baseline tool versions are malformed")
    if supplied_versions is None:
        return expected_versions_by_name
    if set(supplied_versions) != set(RUST_TOOL_VERSION_FIELDS):
        raise CoverageForecastError("rust: producer tool versions are incomplete")
    normalized_versions_by_name = {
        field: supplied_versions[field].strip() for field in expected_versions_by_name
    }
    if normalized_versions_by_name != expected_versions_by_name:
        raise CoverageForecastError("rust: producer tool versions differ from the baseline")
    return normalized_versions_by_name


def _shard_provenance(
    root: Path,
    kind: str,
    shard: str,
    coverage_path: Path,
    base_sha: str,
    head_sha: str,
) -> dict[str, str | int]:
    """Build the immutable identity record for one Python coverage producer."""

    specification = _validate_shard(kind, shard)
    expected_coverage, _ = shard_file_names(kind, shard)
    if coverage_path.name != expected_coverage:
        raise CoverageForecastError(
            f"{kind}/{shard}: coverage file must be named {expected_coverage}"
        )
    _baseline(root)
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "kind": kind,
        "shard": shard,
        "report_name": specification["report_name"],
        "head_sha": head_sha,
        "base_sha": base_sha,
        "coverage_version": _coverage_version(),
        "baseline_sha256": _sha256_file(root / BASELINE_NAME),
        "coverage_file": coverage_path.name,
        "coverage_sha256": _sha256_file(coverage_path),
    }


def _write_json(path: Path, document: dict[str, Any]) -> None:
    """Write canonical JSON for a CI sidecar or diagnostic artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_shard_provenance(
    root: Path,
    kind: str,
    shard: str,
    coverage_path: Path,
    base_revision: str,
    output_path: Path,
) -> None:
    """Write provenance after exactly one coverage producer finishes."""

    _validate_shard(kind, shard)
    _, expected_output = shard_file_names(kind, shard)
    if output_path.name != expected_output:
        raise CoverageForecastError(
            f"{kind}/{shard}: provenance file must be named {expected_output}"
        )
    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    _write_json(
        output_path,
        _shard_provenance(root, kind, shard, coverage_path, base_sha, head_sha),
    )


def _expected_shard_provenance(
    root: Path,
    kind: str,
    shard: str,
    coverage_path: Path,
    base_sha: str,
    head_sha: str,
) -> dict[str, str | int]:
    """Return the exact sidecar shape accepted from downloaded CI artifacts."""

    return _shard_provenance(root, kind, shard, coverage_path, base_sha, head_sha)


def verify_shard_artifacts(
    root: Path,
    artifact_directory: Path,
    kind: str,
    base_sha: str,
    head_sha: str,
) -> list[Path]:
    """Reject missing, stale, renamed, or mixed Python coverage producers."""

    specification = _shard_spec(kind)
    expected_names = {
        file_name
        for shard in specification["shards"]
        for file_name in shard_file_names(kind, shard)
    }
    try:
        actual_names = {path.name for path in artifact_directory.iterdir() if path.is_file()}
    except OSError as exc:
        raise CoverageForecastError(
            f"{kind}: coverage artifact directory is unavailable: {artifact_directory}"
        ) from exc
    if actual_names != expected_names:
        raise CoverageForecastError(
            f"{kind}: coverage artifact files differ from the exact producer set"
        )
    coverage_paths: list[Path] = []
    for shard in specification["shards"]:
        coverage_name, provenance_name = shard_file_names(kind, shard)
        coverage_path = artifact_directory / coverage_name
        provenance = _read_json(artifact_directory / provenance_name, "coverage provenance")
        expected = _expected_shard_provenance(
            root,
            kind,
            shard,
            coverage_path,
            base_sha,
            head_sha,
        )
        if provenance != expected:
            raise CoverageForecastError(
                f"{kind}/{shard}: coverage provenance differs from the exact CI input"
            )
        coverage_paths.append(coverage_path)
    return coverage_paths


def report_provenance_name(report_name: str) -> str:
    """Return the stable sidecar filename for one non-sharded report."""

    if report_name not in REPORT_SPEC_BY_NAME:
        raise CoverageForecastError(f"unsupported coverage report {report_name!r}")
    return f"coverage-provenance-{report_name}.json"


def _report_provenance(
    root: Path,
    report_name: str,
    report_path: Path,
    base_sha: str,
    head_sha: str,
    supplied_rust_tool_versions: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the immutable identity record for one complete report."""

    try:
        specification = REPORT_SPEC_BY_NAME[report_name]
    except KeyError as exc:
        raise CoverageForecastError(f"unsupported coverage report {report_name!r}") from exc
    if report_path.name != specification["path"]:
        raise CoverageForecastError(
            f"{report_name}: report must be named {specification['path']}"
        )
    baseline = _baseline(root)
    provenance_by_field: dict[str, Any] = {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "report_name": report_name,
        "report_format": specification["format"],
        "head_sha": head_sha,
        "base_sha": base_sha,
        "baseline_sha256": _sha256_file(root / BASELINE_NAME),
        "report_file": report_path.name,
        "report_sha256": _sha256_file(report_path),
    }
    if report_name == "rust":
        provenance_by_field["producer_tool_versions"] = _rust_tool_versions(
            baseline, supplied_rust_tool_versions
        )
    else:
        provenance_by_field["coverage_version"] = _coverage_version()
    return provenance_by_field


def write_report_provenance(
    root: Path,
    report_name: str,
    report_path: Path,
    base_revision: str,
    output_path: Path,
    cargo_llvm_cov_version: str | None = None,
    rust_version: str | None = None,
) -> None:
    """Write provenance for a complete direct report such as Rust coverage."""

    if output_path.name != report_provenance_name(report_name):
        raise CoverageForecastError(
            f"{report_name}: provenance file has an unexpected name"
        )
    base_sha, head_sha = resolve_forecast_base(root, base_revision)
    supplied_rust_tool_versions_by_name = None
    if report_name == "rust":
        if cargo_llvm_cov_version is None or rust_version is None:
            raise CoverageForecastError("rust: producer tool versions are required")
        supplied_rust_tool_versions_by_name = {
            "cargo_llvm_cov": cargo_llvm_cov_version,
            "rust": rust_version,
        }
    _write_json(
        output_path,
        _report_provenance(
            root,
            report_name,
            report_path,
            base_sha,
            head_sha,
            supplied_rust_tool_versions_by_name,
        ),
    )


def verify_report_artifact(
    root: Path,
    artifact_directory: Path,
    report_name: str,
    base_sha: str,
    head_sha: str,
) -> Path:
    """Reject a stale or altered complete report artifact."""

    try:
        specification = REPORT_SPEC_BY_NAME[report_name]
    except KeyError as exc:
        raise CoverageForecastError(f"unsupported coverage report {report_name!r}") from exc
    try:
        actual_names = {path.name for path in artifact_directory.iterdir() if path.is_file()}
    except OSError as exc:
        raise CoverageForecastError(
            f"{report_name}: coverage artifact directory is unavailable: {artifact_directory}"
        ) from exc
    provenance_path = artifact_directory / report_provenance_name(report_name)
    if actual_names != {specification["path"], provenance_path.name}:
        raise CoverageForecastError(
            f"{report_name}: coverage artifact files differ from the exact report set"
        )
    report_path = artifact_directory / specification["path"]
    expected = _report_provenance(root, report_name, report_path, base_sha, head_sha)
    provenance = _read_json(provenance_path, "coverage provenance")
    if provenance != expected:
        raise CoverageForecastError(
            f"{report_name}: coverage provenance differs from the exact CI input"
        )
    return report_path
