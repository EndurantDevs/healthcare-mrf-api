"""Bind healthcare coverage artifacts to one exact source and target base."""

from __future__ import annotations

import hashlib
import json
import re
import stat
import subprocess
from pathlib import Path
from typing import Any

from coverage import __version__ as coverage_package_version
from coverage_forecast_cohort import validate_cohort_data
from coverage_reports import CoverageRatchetError

BASELINE_NAME = "test-coverage-baseline.json"
PROVENANCE_SCHEMA_VERSION = 1
SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
POSTGRES_COHORT_NAME = ".coverage-postgres-cohort.json"
POSTGRES_COVERAGE_NAME = ".coverage.postgres.combined"
MAX_COHORT_FILE_BYTES = 32 * 1024 * 1024
MAX_COHORT_TOTAL_BYTES = 128 * 1024 * 1024

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
        "shards": ("0", "1", "2", "3", "4", "5"),
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
        "shards": (
            "core-services",
            "core-services-2",
            "core-imports",
            "core-imports-2",
            "core-snapshots",
            "core-snapshots-2",
            "core-ptg",
            "registry-0",
            "registry-1",
            "registry-2",
            "registry-3",
            "registry-4",
            "registry-5",
            "registry-6",
            "registry-7",
            "registry-8",
            "registry-9",
            "registry-10",
            "registry-11",
            "registry-12",
            "registry-13",
            "registry-14",
            "registry-15",
            "registry-16",
            "registry-17",
            "registry-18",
            "registry-19",
            "registry-20",
            "registry-21",
            "registry-22",
            "registry-23",
            "registry-24",
            "registry-25",
            "registry-26",
            "registry-27",
            "registry-28",
            "registry-29",
            "registry-30",
            "registry-31",
            "registry-32",
            "registry-33",
            "registry-34",
            "registry-35",
            "registry-36",
            "registry-37",
            "registry-38",
            "registry-39",
            "registry-40",
            "registry-41",
            "registry-42",
            "registry-43",
            "registry-44",
            "registry-45",
            "registry-46",
            "registry-47",
            "directory-source",
            "directory-storage",
            "directory-address",
            "profile-storage",
            "profile-publication",
        ),
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
        raise CoverageForecastError("could not resolve coverage forecast Git identity: " + " ".join(arguments)) from exc
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
        raise CoverageForecastError("coverage forecast head does not contain the exact target base")
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
        raise CoverageForecastError("target base has no readable coverage baseline") from exc
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
        raise CoverageForecastError(f"base {base_sha} requires its 90-day coverage baseline artifact")
    artifact = load_baseline(artifact_path)
    if artifact.get("source_sha") != base_sha:
        raise CoverageForecastError(f"coverage baseline artifact source_sha must equal base {base_sha}")
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
        raise CoverageForecastError("coverage baseline reports differ from the forecast artifact contract")
    for report_name, expected in REPORT_SPEC_BY_NAME.items():
        report = reports.get(report_name)
        if not isinstance(report, dict):
            raise CoverageForecastError(f"coverage baseline is missing {report_name}")
        for field_name, expected_value in expected.items():
            if report.get(field_name) != expected_value:
                raise CoverageForecastError(
                    f"{report_name}: baseline {field_name} differs from the forecast artifact contract"
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


def _rust_tool_versions(baseline: dict[str, Any], supplied_versions: dict[str, str] | None = None) -> dict[str, str]:
    """Bind Rust reports to the versions frozen by the Rust baseline policy."""

    try:
        policy = baseline["reports"]["rust"]["scope"]["policy"]
    except (KeyError, TypeError) as exc:
        raise CoverageForecastError("rust: baseline measurement policy is malformed") from exc
    expected_versions_by_name = {field: policy.get(field) for field in RUST_TOOL_VERSION_FIELDS}
    if not all(isinstance(value, str) and value for value in expected_versions_by_name.values()):
        raise CoverageForecastError("rust: baseline tool versions are malformed")
    if supplied_versions is None:
        return expected_versions_by_name
    if set(supplied_versions) != set(RUST_TOOL_VERSION_FIELDS):
        raise CoverageForecastError("rust: producer tool versions are incomplete")
    normalized_versions_by_name = {field: supplied_versions[field].strip() for field in expected_versions_by_name}
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
        raise CoverageForecastError(f"{kind}/{shard}: coverage file must be named {expected_coverage}")
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
        raise CoverageForecastError(f"{kind}/{shard}: provenance file must be named {expected_output}")
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


def _cohort_files(artifact_directory: Path) -> dict[str, Path]:
    """Require one bounded, closed PostgreSQL cohort with every original sidecar."""

    expected_names = {
        POSTGRES_COHORT_NAME,
        POSTGRES_COVERAGE_NAME,
        *(shard_file_names("postgres", shard)[1] for shard in SHARD_SPEC_BY_KIND["postgres"]["shards"]),
    }
    try:
        paths_by_name = {path.name: path for path in artifact_directory.iterdir()}
        if artifact_directory.is_symlink() or set(paths_by_name) != expected_names:
            raise CoverageForecastError("postgres: cohort files differ from the exact producer set")
        total_bytes = 0
        for path in paths_by_name.values():
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode) or not 0 < metadata.st_size <= MAX_COHORT_FILE_BYTES:
                raise CoverageForecastError("postgres: cohort members must be bounded regular files")
            total_bytes += metadata.st_size
        if total_bytes > MAX_COHORT_TOTAL_BYTES:
            raise CoverageForecastError("postgres: cohort exceeds the aggregate limit")
    except OSError as exc:
        raise CoverageForecastError("postgres: cohort files are unavailable") from exc
    return paths_by_name


def _read_cohort_json(path: Path) -> dict[str, Any]:
    """Reject ambiguous JSON before comparing immutable cohort provenance."""

    def unique_fields(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        """Reject duplicate object fields at every nesting level."""

        fields_by_name: dict[str, Any] = {}
        for name, value in pairs:
            if name in fields_by_name:
                raise ValueError("duplicate JSON key")
            fields_by_name[name] = value
        return fields_by_name

    def reject_constant(value: str) -> None:
        """Refuse values outside the JSON number grammar."""

        raise ValueError(f"unsupported JSON constant: {value}")

    try:
        document = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=unique_fields, parse_constant=reject_constant
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise CoverageForecastError(f"postgres: invalid cohort JSON: {path.name}") from exc
    if not isinstance(document, dict):
        raise CoverageForecastError("postgres: cohort JSON must be an object")
    return document


def _is_cohort_digest(value: Any) -> bool:
    """Recognize only complete lowercase SHA-256 strings, without coercion."""

    return isinstance(value, str) and SHA256_PATTERN.fullmatch(value) is not None


def _verify_cohort_sidecars(
    paths_by_name: dict[str, Path], manifest: dict[str, Any], header_by_name: dict[str, Any]
) -> None:
    """Bind each unchanged original producer sidecar without requiring its raw database."""

    inputs_by_shard = manifest.get("inputs")
    shards = SHARD_SPEC_BY_KIND["postgres"]["shards"]
    if not isinstance(inputs_by_shard, dict) or set(inputs_by_shard) != set(shards):
        raise CoverageForecastError("postgres: cohort inputs differ from the exact producer set")
    for shard in shards:
        original_coverage_name, provenance_name = shard_file_names("postgres", shard)
        input_hashes = inputs_by_shard[shard]
        if (
            not isinstance(input_hashes, dict)
            or set(input_hashes) != {"coverage_sha256", "provenance_sha256"}
            or not all(_is_cohort_digest(digest) for digest in input_hashes.values())
        ):
            raise CoverageForecastError(f"postgres/{shard}: malformed cohort input hashes")
        provenance_path = paths_by_name[provenance_name]
        provenance = _read_cohort_json(provenance_path)
        expected_by_field = {
            "schema_version": PROVENANCE_SCHEMA_VERSION,
            "kind": "postgres",
            "shard": shard,
            "report_name": "python",
            **header_by_name,
            "coverage_file": original_coverage_name,
            "coverage_sha256": input_hashes["coverage_sha256"],
        }
        if "common_test_execution" in provenance:
            if not isinstance(provenance["common_test_execution"], dict):
                raise CoverageForecastError(f"postgres/{shard}: common test execution must be an object")
            expected_by_field["common_test_execution"] = provenance["common_test_execution"]
        if type(provenance.get("schema_version")) is not int or provenance != expected_by_field:
            raise CoverageForecastError(f"postgres/{shard}: cohort provenance differs from the exact CI input")
        if _sha256_file(provenance_path) != input_hashes["provenance_sha256"]:
            raise CoverageForecastError(f"postgres/{shard}: cohort provenance digest differs")


def _verify_postgres_cohort(root: Path, artifact_directory: Path, base_sha: str, head_sha: str) -> list[Path]:
    """Accept one provenance-bound PostgreSQL union while preserving all producer identities."""

    paths_by_name = _cohort_files(artifact_directory)
    manifest = _read_cohort_json(paths_by_name[POSTGRES_COHORT_NAME])
    _baseline(root)
    header_by_name = {
        "head_sha": head_sha,
        "base_sha": base_sha,
        "baseline_sha256": _sha256_file(root / BASELINE_NAME),
        "coverage_version": _coverage_version(),
    }
    expected_by_field = {
        "schema": "python-coverage-cohort/v1",
        "kind": "postgres",
        **header_by_name,
        "coverage_file": POSTGRES_COVERAGE_NAME,
        "coverage_sha256": _sha256_file(paths_by_name[POSTGRES_COVERAGE_NAME]),
        "inputs": manifest.get("inputs"),
    }
    if manifest != expected_by_field:
        raise CoverageForecastError("postgres: cohort manifest differs from the exact CI input")
    _verify_cohort_sidecars(paths_by_name, manifest, header_by_name)
    try:
        validate_cohort_data(paths_by_name[POSTGRES_COVERAGE_NAME])
    except ValueError as exc:
        raise CoverageForecastError("postgres: invalid cohort coverage data") from exc
    return [paths_by_name[POSTGRES_COVERAGE_NAME]]


def verify_shard_artifacts(
    root: Path,
    artifact_directory: Path,
    kind: str,
    base_sha: str,
    head_sha: str,
) -> list[Path]:
    """Reject missing, stale, renamed, or mixed Python coverage producers."""

    specification = _shard_spec(kind)
    cohort_path = artifact_directory / POSTGRES_COHORT_NAME
    if kind == "postgres" and (cohort_path.exists() or cohort_path.is_symlink()):
        return _verify_postgres_cohort(root, artifact_directory, base_sha, head_sha)
    expected_names = {file_name for shard in specification["shards"] for file_name in shard_file_names(kind, shard)}
    try:
        actual_names = {path.name for path in artifact_directory.iterdir() if path.is_file()}
    except OSError as exc:
        raise CoverageForecastError(
            f"{kind}: coverage artifact directory is unavailable: {artifact_directory}"
        ) from exc
    if actual_names != expected_names:
        raise CoverageForecastError(f"{kind}: coverage artifact files differ from the exact producer set")
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
            raise CoverageForecastError(f"{kind}/{shard}: coverage provenance differs from the exact CI input")
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
        raise CoverageForecastError(f"{report_name}: report must be named {specification['path']}")
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
        provenance_by_field["producer_tool_versions"] = _rust_tool_versions(baseline, supplied_rust_tool_versions)
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
        raise CoverageForecastError(f"{report_name}: provenance file has an unexpected name")
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
        raise CoverageForecastError(f"{report_name}: coverage artifact files differ from the exact report set")
    report_path = artifact_directory / specification["path"]
    expected = _report_provenance(root, report_name, report_path, base_sha, head_sha)
    provenance = _read_json(provenance_path, "coverage provenance")
    if provenance != expected:
        raise CoverageForecastError(f"{report_name}: coverage provenance differs from the exact CI input")
    return report_path
