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
import re
import sys
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
ACTIVE_RUN_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
RAW_TERMINAL_STATUSES = {"succeeded", "failed", "canceled", "cancelled", "dead_letter"}
SENSITIVE_TEXT_PATTERN = re.compile(
    r"(?i)(?:bearer\s+\S+|token|secret|password|authorization|api[_-]?key|credential)"
)

class VerificationUpdateError(ValueError):
    """Raised when a live report cannot be imported safely."""

def _load_audited_manifest(manifest_path: Path) -> dict[str, Any]:
    needs_repo_root = str(ROOT) not in sys.path
    if needs_repo_root:
        sys.path.insert(0, str(ROOT))
    try:
        from scripts.research import provider_directory_endpoint_acquisition_harness

        return provider_directory_endpoint_acquisition_harness.load_manifest(manifest_path)
    finally:
        if needs_repo_root:
            sys.path.remove(str(ROOT))

def _read_object(path: Path) -> dict[str, Any]:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise VerificationUpdateError(f"{path} must contain a JSON object")
    return decoded

def _parsed_timestamp(value: Any, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value.strip():
        raise VerificationUpdateError(f"{label} is required")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VerificationUpdateError(f"{label} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise VerificationUpdateError(f"{label} must include a timezone")
    return parsed

def _checked_timestamp(report: dict[str, Any]) -> str:
    value = report.get("generated_at")
    _parsed_timestamp(value, "report.generated_at")
    return str(value)

def _safe_environment(environment: str) -> str:
    normalized = environment.strip()
    if not normalized:
        raise VerificationUpdateError("verification environment must be non-empty")
    lowered = normalized.lower().replace("-", "_")
    if any(part in lowered for part in SENSITIVE_NAME_PARTS) or "\n" in normalized:
        raise VerificationUpdateError("verification environment is not credential-safe")
    return normalized

def _report_run_id(entry_id: str, report_entry: dict[str, Any], required: bool = True) -> str | None:
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
    if not run_id and not required:
        return None
    if not run_id.startswith("run_"):
        raise VerificationUpdateError(f"{entry_id}: terminal report entry has no valid run_id")
    return run_id

def _access_verification(report_entry: dict[str, Any], terminal_status: str) -> str:
    last_run = report_entry.get("last_run")
    run_status = last_run.get("status") if isinstance(last_run, dict) else None
    if run_status:
        return "verified" if run_status == "succeeded" else "not_verified"
    return "verified" if terminal_status in {"succeeded", "external_completed"} else "not_verified"

def _safe_text(value: Any) -> str | None:
    if not isinstance(value, (str, int, float, bool)):
        return None
    normalized = str(value).strip()
    if not normalized or SENSITIVE_TEXT_PATTERN.search(normalized):
        return None
    return normalized[:500]

def _safe_terminal_error(last_run: dict[str, Any]) -> dict[str, str] | None:
    error = last_run.get("terminal_error", last_run.get("error"))
    if isinstance(error, str):
        message = _safe_text(error)
        return {"message": message} if message else None
    if not isinstance(error, dict):
        return None
    terminal_error_by_field = {
        field_name: safe_value
        for field_name in ("code", "type", "status", "reason", "message")
        if (safe_value := _safe_text(error.get(field_name))) is not None
    }
    return terminal_error_by_field or None

def _evidence(
    entry_id: str,
    manifest_entry: dict[str, Any],
    report_entry: dict[str, Any],
) -> dict[str, Any] | None:
    last_run = report_entry.get("last_run")
    if not isinstance(last_run, dict):
        return None
    metrics_by_name = last_run.get("metrics") if isinstance(last_run.get("metrics"), dict) else {}
    source_ids = last_run.get("source_ids", metrics_by_name.get("source_ids"))
    if source_ids is not None and source_ids != manifest_entry["source_ids"]:
        raise VerificationUpdateError(f"{entry_id}: report source_ids do not match the manifest")
    resource_outcomes = last_run.get("resource_outcomes", metrics_by_name.get("resource_fetch_stats"))
    if resource_outcomes is not None and (not isinstance(resource_outcomes, dict) or set(resource_outcomes) - generator.RESOURCE_TYPES or any(not isinstance(resource_metrics, dict) for resource_metrics in resource_outcomes.values())):
        raise VerificationUpdateError(f"{entry_id}: resource outcomes are invalid")
    effective_acquisition_dict = last_run.get("effective_acquisition")
    if effective_acquisition_dict is None and metrics_by_name:
        effective_acquisition_dict = {"sources_probed": metrics_by_name.get("sources_probed"), "selected_sources": metrics_by_name.get("source_import_sources_selected"), "selected_groups": metrics_by_name.get("source_import_groups_attempted"), "completed_source_ids": metrics_by_name.get("resource_fetch_completed_source_ids"), "bulk_export": metrics_by_name.get("bulk_export_mode")}
    if effective_acquisition_dict is not None and not isinstance(effective_acquisition_dict, dict):
        raise VerificationUpdateError(f"{entry_id}: effective acquisition is invalid")
    terminal_error = _safe_terminal_error(last_run)
    bounded_reasons = [
        safe_reason
        for reason in report_entry.get("metric_errors", [])
        if (safe_reason := _safe_text(reason)) is not None
    ] if isinstance(report_entry.get("metric_errors", []), list) else []
    evidence_by_field = {"source_ids": source_ids, "resource_outcomes": resource_outcomes, "effective_acquisition": effective_acquisition_dict, "terminal_error": terminal_error, "bounded_reasons": bounded_reasons}
    evidence_by_field = {field_name: field_value for field_name, field_value in evidence_by_field.items() if field_value not in (None, {}, [])}
    return evidence_by_field or None

def _validate_terminal_run_state(entry_id: str, terminal_status: str, run_status: Any) -> None:
    if run_status is None:
        return
    if not isinstance(run_status, str) or not run_status:
        raise VerificationUpdateError(f"{entry_id}: last run status is invalid")
    if run_status in ACTIVE_RUN_STATUSES:
        raise VerificationUpdateError(
            f"{entry_id}: nonterminal run status {run_status!r} cannot be recorded as terminal"
        )
    expected_by_status = {
        "succeeded": {"succeeded"},
        "external_completed": {"succeeded"},
        "external_validation_failed": {"succeeded"},
        "metric_validation_failed": {"succeeded"},
        "failed": {"failed", "dead_letter"},
        "dead_letter": {"dead_letter"},
        "resume_required": {"failed"},
        "canceled": {"canceled", "cancelled"},
        "cancelled": {"canceled", "cancelled"},
    }
    expected = expected_by_status.get(terminal_status)
    if expected is not None and run_status not in expected:
        raise VerificationUpdateError(
            f"{entry_id}: terminal status {terminal_status!r} conflicts with run status {run_status!r}"
        )
    if terminal_status == "unknown_terminal" and run_status in RAW_TERMINAL_STATUSES:
        raise VerificationUpdateError(f"{entry_id}: known terminal run cannot be unknown_terminal")

def _current_observation(
    entry_id: str,
    manifest_entry: dict[str, Any],
    report_entry: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    last_run = report_entry.get("last_run")
    last_run_dict = last_run if isinstance(last_run, dict) else {}
    observation_dict = {
        "run_id": _report_run_id(entry_id, report_entry, required=False),
        "state_status": str(report_entry.get("status") or "unknown"),
        "run_status": last_run_dict.get("status"),
        "observed_at": checked_at,
    }
    evidence = _evidence(entry_id, manifest_entry, report_entry)
    if evidence:
        observation_dict["evidence"] = evidence
    return observation_dict

def _terminal_record(
    entry_id: str,
    manifest_entry: dict[str, Any],
    report_entry: Any,
    checked_at: str,
) -> dict[str, Any] | None:
    if not isinstance(report_entry, dict):
        raise VerificationUpdateError(f"{entry_id}: report entry must be an object")
    declared_entry_id = report_entry.get("entry_id")
    if declared_entry_id is not None and declared_entry_id != entry_id:
        raise VerificationUpdateError(f"{entry_id}: report entry identity does not match its key")
    terminal_status = report_entry.get("status")
    if not isinstance(terminal_status, str) or not terminal_status:
        raise VerificationUpdateError(f"{entry_id}: report entry status is invalid")
    if terminal_status not in TERMINAL_STATUSES:
        return None
    last_run = report_entry.get("last_run")
    run_status = last_run.get("status") if isinstance(last_run, dict) else None
    _validate_terminal_run_state(entry_id, terminal_status, run_status)
    terminal_record_dict = {
        "terminal_status": terminal_status,
        "run_id": _report_run_id(entry_id, report_entry),
        "access_verification": _access_verification(report_entry, terminal_status),
        "checked_at": checked_at,
        "proof_state": "current",
        "current_observation": _current_observation(
            entry_id, manifest_entry, report_entry, checked_at
        ),
    }
    terminal_evidence = _evidence(entry_id, manifest_entry, report_entry)
    if terminal_evidence:
        terminal_record_dict["terminal_evidence"] = terminal_evidence
    return terminal_record_dict

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

def _report_identity(report: dict[str, Any], checked_at: str) -> dict[str, Any]:
    schema_version = report.get("schema_version")
    if schema_version not in {None, 1}:
        raise VerificationUpdateError("report schema_version is unsupported")
    mode = report.get("mode", "unknown")
    if mode not in {"apply", "dry-run", "unknown"}:
        raise VerificationUpdateError("report mode is invalid")
    return {
        "schema_version": schema_version,
        "generated_at": checked_at,
        "mode": mode,
        "manifest_sha256": report["manifest_sha256"],
        "report_sha256": hashlib.sha256(
            json.dumps(report, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }

def _validate_integration_metadata(
    report: dict[str, Any],
    entry_ids: set[str],
) -> set[str]:
    integration = report.get("verification_update")
    if integration is None:
        return set(report["entries"])
    if not isinstance(integration, dict):
        raise VerificationUpdateError("report verification_update must be an object")
    selected = integration.get("selected_entry_ids")
    terminal = integration.get("terminal_entry_ids")
    nonterminal = integration.get("nonterminal_entry_ids")
    eligible = integration.get("eligible")
    argv = integration.get("argv")
    entry_id_groups = (selected, terminal, nonterminal)
    if not all(isinstance(entry_id_group, list) for entry_id_group in entry_id_groups):
        raise VerificationUpdateError("report verification_update entry lists are required")
    if any(not isinstance(entry_id, str) for entry_id_group in entry_id_groups for entry_id in entry_id_group):
        raise VerificationUpdateError("report verification_update entry identities are invalid")
    if any(len(entry_id_group) != len(set(entry_id_group)) for entry_id_group in entry_id_groups):
        raise VerificationUpdateError("report verification_update entry identities are duplicated")
    if not set(selected).issubset(entry_ids) or set(selected) != set(terminal) | set(nonterminal):
        raise VerificationUpdateError("report verification_update entry identities do not agree")
    report_entries = report["entries"]
    if not set(selected).issubset(report_entries) or any(
        not isinstance(report_entries[entry_id], dict) for entry_id in selected
    ):
        raise VerificationUpdateError("report verification_update entries are missing")
    if set(terminal) & set(nonterminal) or not isinstance(eligible, bool) or eligible != (not nonterminal):
        raise VerificationUpdateError("report verification_update eligibility is inconsistent")
    if (
        not isinstance(argv, list)
        or not all(isinstance(argument, str) for argument in argv)
        or "scripts/update_provider_directory_verification.py" not in argv
    ):
        raise VerificationUpdateError("report verification_update argv is invalid")
    incorrectly_terminal_ids = [
        entry_id
        for entry_id in terminal
        if not isinstance(report_entries.get(entry_id), dict)
        or report_entries[entry_id].get("status") not in TERMINAL_STATUSES
    ]
    if incorrectly_terminal_ids:
        raise VerificationUpdateError("report verification_update marks nonterminal entries as terminal")
    expected_terminal_ids = {
        entry_id
        for entry_id in selected
        if report_entries[entry_id].get("status") in TERMINAL_STATUSES
    }
    if set(terminal) != expected_terminal_ids:
        raise VerificationUpdateError("report verification_update terminal identities do not agree")
    return set(selected)

def _ensure_report_is_fresh(
    prior_snapshot: dict[str, Any],
    report_identity: dict[str, Any],
) -> None:
    prior_checked_at = prior_snapshot.get("checked_at")
    if prior_checked_at is None:
        return
    report_time = _parsed_timestamp(report_identity["generated_at"], "report.generated_at")
    prior_time = _parsed_timestamp(
        prior_checked_at, "verification snapshot checked_at"
    )
    if report_time < prior_time:
        raise VerificationUpdateError("report is older than the verification snapshot")
    prior_identity = prior_snapshot.get("report_identity")
    if (
        report_time == prior_time
        and isinstance(prior_identity, dict)
        and prior_identity.get("report_sha256") != report_identity["report_sha256"]
    ):
        raise VerificationUpdateError("a different report must be newer than the verification snapshot")

def _merge_nonterminal_observation(
    entry_id: str,
    manifest_entry: dict[str, Any],
    report_entry: dict[str, Any],
    prior_record: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    merged = copy.deepcopy(prior_record)
    observation = _current_observation(entry_id, manifest_entry, report_entry, checked_at)
    merged["current_observation"] = observation
    prior_run_id = prior_record.get("run_id")
    current_run_id = observation.get("run_id")
    current_status = observation.get("run_status") or observation["state_status"]
    prior_checked_at = prior_record.get("checked_at")
    has_newer_active_run = (
        current_status in ACTIVE_RUN_STATUSES
        and current_run_id is not None
        and current_run_id != prior_run_id
        and (
            prior_checked_at is None
            or _parsed_timestamp(checked_at, f"{entry_id}: observed_at")
            > _parsed_timestamp(prior_checked_at, f"{entry_id}: checked_at")
        )
    )
    if prior_record.get("terminal_status") is not None:
        merged["proof_state"] = "superseded" if has_newer_active_run else merged.get("proof_state", "current")
    else:
        merged["proof_state"] = "not_recorded"
    return merged

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
    manifest_entry_by_id = {entry["entry_id"]: entry for entry in manifest["entries"]}
    unknown_entries = sorted(set(report_entries) - set(entry_ids))
    if unknown_entries:
        raise VerificationUpdateError("report contains unknown entries: " + ", ".join(unknown_entries))
    generator.validate_verification_snapshot(prior_snapshot, entry_ids, manifest["campaign_id"])
    checked_at = _checked_timestamp(report)
    report_identity = _report_identity(report, checked_at)
    _ensure_report_is_fresh(prior_snapshot, report_identity)
    selected_entry_ids = _validate_integration_metadata(report, set(entry_ids))
    snapshot_by_field = {
        "schema_version": 1,
        "environment": _safe_environment(environment),
        "campaign_id": manifest["campaign_id"],
        "checked_at": checked_at,
        "report_identity": report_identity,
        "entries": copy.deepcopy(prior_snapshot["entries"]),
    }
    for entry_id in selected_entry_ids:
        report_entry = report_entries[entry_id]
        terminal_record = _terminal_record(
            entry_id, manifest_entry_by_id[entry_id], report_entry, checked_at
        )
        if terminal_record is not None:
            snapshot_by_field["entries"][entry_id] = terminal_record
        elif isinstance(report_entry, dict):
            snapshot_by_field["entries"][entry_id] = _merge_nonterminal_observation(
                entry_id,
                manifest_entry_by_id[entry_id],
                report_entry,
                snapshot_by_field["entries"][entry_id],
                checked_at,
            )
    generator.validate_verification_snapshot(snapshot_by_field, entry_ids, manifest["campaign_id"])
    return snapshot_by_field

def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary_name = handle.name
            handle.write(content)
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
    manifest = _load_audited_manifest(manifest_path)
    report = _read_object(report_path)
    prior_snapshot = generator.load_verification_snapshot(snapshot_path) if snapshot_path.exists() else _empty_snapshot(manifest)
    snapshot = update_verification_snapshot(manifest, report, prior_snapshot, environment)
    blocker_path = ROOT / manifest["support_documentation"]["blocker_registry"]
    rendered = generator.render_markdown(
        manifest,
        generator.load_blocker_registry(blocker_path),
        snapshot,
    )
    if output_path == snapshot_path:
        raise VerificationUpdateError("snapshot and documentation paths must be distinct")
    _atomic_write_text(output_path, rendered)
    _atomic_write_text(snapshot_path, json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
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
