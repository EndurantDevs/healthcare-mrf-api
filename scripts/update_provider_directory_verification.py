#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Update the tracked Provider Directory terminal verification snapshot."""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

try:
    from scripts import generate_provider_directory_support_docs as generator
except ModuleNotFoundError:
    import generate_provider_directory_support_docs as generator


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
DEFAULT_REPORT = ROOT / "reports/provider-directory-endpoint-acquisition/report.json"
DEFAULT_SNAPSHOT = ROOT / "specs/provider_directory_endpoint_verification.json"
DEFAULT_OUTPUT = ROOT / "docs/imports/provider-directory-endpoint-support.md"
TERMINAL_STATUSES = generator.VERIFICATION_STATUSES
SENSITIVE_NAME_PARTS = ("token", "secret", "password", "authorization", "api_key", "credential")


class VerificationUpdateError(ValueError):
    """Raised when a live report cannot be imported safely."""


def _read_object(path: Path) -> dict[str, Any]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise VerificationUpdateError(f"{path} must contain a JSON object")
    return decoded


def _checked_timestamp(report: dict[str, Any]) -> str:
    value = report.get("generated_at")
    if not isinstance(value, str) or not value.strip():
        raise VerificationUpdateError("report.generated_at is required")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VerificationUpdateError("report.generated_at must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise VerificationUpdateError("report.generated_at must include a timezone")
    return value


def _safe_environment(environment: str) -> str:
    normalized = environment.strip()
    if not normalized:
        raise VerificationUpdateError("verification environment must be non-empty")
    lowered = normalized.lower().replace("-", "_")
    if any(part in lowered for part in SENSITIVE_NAME_PARTS) or "\n" in normalized:
        raise VerificationUpdateError("verification environment is not credential-safe")
    return normalized


def _report_run_id(entry_id: str, report_entry: dict[str, Any]) -> str:
    last_run = report_entry.get("last_run")
    last_run_id = last_run.get("run_id") if isinstance(last_run, dict) else None
    candidates = [
        value
        for value in (
            report_entry.get("current_run_id"),
            report_entry.get("external_run_id"),
            last_run_id,
        )
        if value
    ]
    unique_candidates = set(candidates)
    if len(unique_candidates) > 1:
        raise VerificationUpdateError(f"{entry_id}: report run identifiers do not agree")
    run_id = str(next(iter(unique_candidates), ""))
    if not run_id.startswith("run_"):
        raise VerificationUpdateError(f"{entry_id}: terminal report entry has no valid run_id")
    return run_id


def _access_verification(report_entry: dict[str, Any], terminal_status: str) -> str:
    last_run = report_entry.get("last_run")
    run_status = last_run.get("status") if isinstance(last_run, dict) else None
    if run_status:
        return "verified" if run_status == "succeeded" else "not_verified"
    return "verified" if terminal_status in {"succeeded", "external_completed"} else "not_verified"


def _terminal_record(
    entry_id: str,
    report_entry: Any,
    checked_at: str,
) -> dict[str, Any] | None:
    if not isinstance(report_entry, dict):
        raise VerificationUpdateError(f"{entry_id}: report entry must be an object")
    declared_entry_id = report_entry.get("entry_id")
    if declared_entry_id is not None and declared_entry_id != entry_id:
        raise VerificationUpdateError(f"{entry_id}: report entry identity does not match its key")
    terminal_status = report_entry.get("status")
    if terminal_status not in TERMINAL_STATUSES:
        return None
    return {
        "terminal_status": terminal_status,
        "run_id": _report_run_id(entry_id, report_entry),
        "access_verification": _access_verification(report_entry, terminal_status),
        "checked_at": checked_at,
    }


def _empty_snapshot(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "environment": generator.NOT_RECORDED,
        "campaign_id": manifest["campaign_id"],
        "checked_at": None,
        "entries": {
            entry["entry_id"]: {
                "terminal_status": None,
                "run_id": None,
                "access_verification": "not_recorded",
                "checked_at": None,
            }
            for entry in manifest["entries"]
        },
    }


def _manifest_sha256(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def update_verification_snapshot(
    manifest: dict[str, Any],
    report: dict[str, Any],
    prior_snapshot: dict[str, Any],
    environment: str,
) -> dict[str, Any]:
    """Merge terminal, non-sensitive report fields into the tracked snapshot."""
    generator.validate_manifest(manifest)
    if report.get("campaign_id") != manifest["campaign_id"]:
        raise VerificationUpdateError("report campaign_id does not match the manifest")
    if report.get("manifest_sha256") != _manifest_sha256(manifest):
        raise VerificationUpdateError("report manifest_sha256 does not match the manifest")
    report_entries = report.get("entries")
    if not isinstance(report_entries, dict):
        raise VerificationUpdateError("report.entries must be an object")
    entry_ids = [entry["entry_id"] for entry in manifest["entries"]]
    unknown_entries = sorted(set(report_entries) - set(entry_ids))
    if unknown_entries:
        raise VerificationUpdateError("report contains unknown entries: " + ", ".join(unknown_entries))
    generator.validate_verification_snapshot(prior_snapshot, entry_ids, manifest["campaign_id"])
    checked_at = _checked_timestamp(report)
    snapshot_by_field = {
        "schema_version": 1,
        "environment": _safe_environment(environment),
        "campaign_id": manifest["campaign_id"],
        "checked_at": checked_at,
        "entries": copy.deepcopy(prior_snapshot["entries"]),
    }
    for entry_id, report_entry in report_entries.items():
        terminal_record = _terminal_record(entry_id, report_entry, checked_at)
        if terminal_record is not None:
            snapshot_by_field["entries"][entry_id] = terminal_record
    generator.validate_verification_snapshot(snapshot_by_field, entry_ids, manifest["campaign_id"])
    return snapshot_by_field


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary_name = handle.name
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def update_files(
    manifest_path: Path = DEFAULT_MANIFEST,
    report_path: Path = DEFAULT_REPORT,
    snapshot_path: Path = DEFAULT_SNAPSHOT,
    output_path: Path = DEFAULT_OUTPUT,
    environment: str = "",
) -> dict[str, Any]:
    """Update the snapshot and regenerate the support documentation."""
    manifest = generator.load_manifest(manifest_path)
    report = _read_object(report_path)
    prior_snapshot = generator.load_verification_snapshot(snapshot_path) if snapshot_path.exists() else _empty_snapshot(manifest)
    snapshot = update_verification_snapshot(manifest, report, prior_snapshot, environment)
    _write_json(snapshot_path, snapshot)
    blocker_path = ROOT / manifest["support_documentation"]["blocker_registry"]
    rendered = generator.render_markdown(
        manifest,
        generator.load_blocker_registry(blocker_path),
        snapshot,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    return snapshot


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse safe snapshot update arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--environment", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Update the tracked snapshot and generated support document."""
    args = parse_args(argv)
    update_files(args.manifest, args.report, args.snapshot, args.output, args.environment)
    print(f"Updated Provider Directory verification snapshot: {args.snapshot}")
    print(f"Regenerated Provider Directory support documentation: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
