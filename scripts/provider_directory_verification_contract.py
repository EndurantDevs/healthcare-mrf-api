"""Per-source acquisition-spec binding for Provider Directory terminal proof."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any

try:
    from scripts.provider_directory_support_contract import SupportDocumentationError
except ModuleNotFoundError:
    from provider_directory_support_contract import SupportDocumentationError


ACTIVE_RUN_STATUSES = {"queued", "starting", "running", "finalizing", "canceling"}
ACCESS_VERIFICATION_VALUES = {"verified", "not_verified", "not_recorded"}
PROOF_STATES = {"current", "superseded", "not_recorded"}
SUPERSEDED_REASONS = {"manifest_entry_changed", "newer_active_run"}
VERIFICATION_STATUSES = {
    "bounded",
    "canceled",
    "cancelled",
    "dead_letter",
    "external_completed",
    "external_incomplete",
    "external_validation_failed",
    "failed",
    "metric_validation_failed",
    "resume_required",
    "succeeded",
    "unknown_terminal",
}
SENSITIVE_TEXT_PATTERN = re.compile(
    r"(?i)(?:bearer\s+\S+|token|secret|password|authorization|api[_-]?key|credential)"
)


class VerificationUpdateError(ValueError):
    """Raised when a live report cannot be imported safely."""


def provider_directory_entry_sha256(entry: dict[str, Any]) -> str:
    """Return the stable acquisition-contract fingerprint for one source entry."""
    serialized_entry = json.dumps(entry, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized_entry.encode("utf-8")).hexdigest()


def manifest_sha256(manifest: dict[str, Any]) -> str:
    """Return the harness-compatible fingerprint for a complete manifest."""
    serialized_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized_manifest.encode("utf-8")).hexdigest()


def verification_report_identity(
    report: dict[str, Any],
    checked_at: str,
) -> dict[str, Any]:
    """Return the validated immutable identity for one campaign report."""
    schema_version = report.get("schema_version")
    if schema_version not in {None, 1}:
        raise VerificationUpdateError("report schema_version is unsupported")
    mode = report.get("mode", "unknown")
    if mode not in {"apply", "dry-run", "unknown"}:
        raise VerificationUpdateError("report mode is invalid")
    serialized_report = json.dumps(report, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": schema_version,
        "generated_at": checked_at,
        "mode": mode,
        "manifest_sha256": report["manifest_sha256"],
        "report_sha256": hashlib.sha256(serialized_report.encode("utf-8")).hexdigest(),
    }


def _required_timestamp(value: Any, label: str) -> dt.datetime:
    if not isinstance(value, str) or not value.strip():
        raise VerificationUpdateError(f"{label} is required")
    try:
        parsed_timestamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VerificationUpdateError(f"{label} must be ISO-8601") from exc
    if parsed_timestamp.tzinfo is None:
        raise VerificationUpdateError(f"{label} must include a timezone")
    return parsed_timestamp


def ensure_verification_report_is_fresh(
    prior_snapshot: dict[str, Any],
    report_identity: dict[str, Any],
) -> None:
    """Reject reports older than the currently tracked verification snapshot."""
    prior_checked_at = prior_snapshot.get("checked_at")
    if prior_checked_at is None:
        return
    report_time = _required_timestamp(
        report_identity["generated_at"],
        "report.generated_at",
    )
    prior_time = _required_timestamp(
        prior_checked_at,
        "verification snapshot checked_at",
    )
    if report_time < prior_time:
        raise VerificationUpdateError("report is older than the verification snapshot")
    prior_identity = prior_snapshot.get("report_identity")
    if (
        report_time == prior_time
        and isinstance(prior_identity, dict)
        and prior_identity.get("report_sha256") != report_identity["report_sha256"]
    ):
        raise VerificationUpdateError(
            "a different report must be newer than the verification snapshot"
        )


def validate_optional_verification_timestamp(value: Any, label: str) -> None:
    """Validate an optional timezone-qualified verification timestamp."""
    if value is None:
        return
    if not isinstance(value, str) or not value.strip():
        raise SupportDocumentationError(f"{label} must be ISO-8601 or null")
    try:
        parsed_timestamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SupportDocumentationError(f"{label} must be ISO-8601 or null") from exc
    if parsed_timestamp.tzinfo is None:
        raise SupportDocumentationError(f"{label} must include a timezone")


def _validate_terminal_record_identity(
    entry_id: str,
    terminal_status: Any,
    run_id: Any,
    access_verification: Any,
    checked_at: Any,
) -> None:
    if terminal_status is not None and terminal_status not in VERIFICATION_STATUSES:
        raise SupportDocumentationError(f"{entry_id}: invalid terminal verification status")
    if run_id is not None and (
        not isinstance(run_id, str) or not run_id.startswith("run_")
    ):
        raise SupportDocumentationError(
            f"{entry_id}: run_id must be null or a run_ identifier"
        )
    if access_verification not in ACCESS_VERIFICATION_VALUES:
        raise SupportDocumentationError(
            f"{entry_id}: invalid access verification value"
        )
    validate_optional_verification_timestamp(checked_at, f"{entry_id}: checked_at")
    if terminal_status is None and (
        run_id is not None
        or checked_at is not None
        or access_verification != "not_recorded"
    ):
        raise SupportDocumentationError(
            f"{entry_id}: unverified entries must remain not recorded"
        )
    if terminal_status is not None and (
        run_id is None or checked_at is None or access_verification == "not_recorded"
    ):
        raise SupportDocumentationError(
            f"{entry_id}: terminal entries need run_id and access verification"
        )


def _validate_verification_observation(entry_id: str, observation: Any) -> None:
    controlled_fields = {
        "run_id",
        "state_status",
        "run_status",
        "observed_at",
        "evidence",
    }
    if observation is not None and (
        not isinstance(observation, dict) or set(observation) - controlled_fields
    ):
        raise SupportDocumentationError(
            f"{entry_id}: current observation fields are not controlled"
        )
    if not isinstance(observation, dict):
        return
    validate_optional_verification_timestamp(
        observation.get("observed_at"),
        f"{entry_id}: observed_at",
    )
    evidence = observation.get("evidence")
    if evidence is not None and SENSITIVE_TEXT_PATTERN.search(
        json.dumps(evidence, sort_keys=True)
    ):
        raise SupportDocumentationError(
            f"{entry_id}: current observation evidence is not credential-safe"
        )


def validate_verification_record(
    entry_id: str,
    verification_record: Any,
    expected_entry_sha256: str,
    *,
    allow_current_spec_mismatch: bool,
) -> None:
    """Validate one credential-safe terminal verification record."""
    required_fields = {"terminal_status", "run_id", "access_verification", "checked_at"}
    optional_fields = {
        "proof_state",
        "superseded_reason",
        "entry_spec_sha256",
        "current_observation",
        "terminal_evidence",
    }
    if (
        not isinstance(verification_record, dict)
        or not required_fields.issubset(verification_record)
        or not set(verification_record).issubset(required_fields | optional_fields)
    ):
        raise SupportDocumentationError(
            f"{entry_id}: verification record fields are not controlled"
        )
    terminal_status = verification_record["terminal_status"]
    run_id = verification_record["run_id"]
    proof_state = verification_record.get(
        "proof_state", "current" if terminal_status is not None else "not_recorded"
    )
    _validate_terminal_record_identity(
        entry_id,
        terminal_status,
        run_id,
        verification_record["access_verification"],
        verification_record["checked_at"],
    )
    if proof_state not in PROOF_STATES:
        raise SupportDocumentationError(f"{entry_id}: invalid proof state")
    if proof_state == "superseded" and terminal_status is None:
        raise SupportDocumentationError(
            f"{entry_id}: superseded proof requires terminal evidence"
        )
    observation = verification_record.get("current_observation")
    _validate_verification_observation(entry_id, observation)
    terminal_evidence = verification_record.get("terminal_evidence")
    if terminal_evidence is not None and (
        not isinstance(terminal_evidence, dict)
        or SENSITIVE_TEXT_PATTERN.search(json.dumps(terminal_evidence, sort_keys=True))
    ):
        raise SupportDocumentationError(f"{entry_id}: verification evidence is invalid")
    validate_entry_proof_binding(
        entry_id,
        verification_record,
        expected_entry_sha256,
        proof_state,
        terminal_status,
        observation,
        allow_current_spec_mismatch=allow_current_spec_mismatch,
    )


def validate_entry_proof_binding(
    entry_id: str,
    verification_record: dict[str, Any],
    expected_entry_sha256: str,
    proof_state: str,
    terminal_status: str | None,
    observation: Any,
    *,
    allow_current_spec_mismatch: bool,
) -> None:
    """Validate that terminal proof belongs to the current source contract."""
    entry_spec_sha256 = verification_record.get("entry_spec_sha256")
    if terminal_status is None and entry_spec_sha256 is not None:
        raise SupportDocumentationError(
            f"{entry_id}: unverified entry cannot have a spec fingerprint"
        )
    if terminal_status is not None and (
        not isinstance(entry_spec_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", entry_spec_sha256) is None
    ):
        raise SupportDocumentationError(
            f"{entry_id}: terminal proof needs an entry spec fingerprint"
        )
    if (
        terminal_status is not None
        and entry_spec_sha256 != expected_entry_sha256
        and proof_state != "superseded"
        and not allow_current_spec_mismatch
    ):
        raise SupportDocumentationError(
            f"{entry_id}: terminal proof does not match the current manifest entry"
        )
    superseded_reason = verification_record.get("superseded_reason")
    if superseded_reason not in {None, *SUPERSEDED_REASONS}:
        raise SupportDocumentationError(f"{entry_id}: invalid superseded reason")
    if proof_state != "superseded" and superseded_reason is not None:
        raise SupportDocumentationError(
            f"{entry_id}: superseded reason requires superseded proof"
        )
    if proof_state != "superseded":
        return
    if superseded_reason == "manifest_entry_changed":
        if entry_spec_sha256 == expected_entry_sha256:
            raise SupportDocumentationError(
                f"{entry_id}: manifest-change supersession needs a changed entry spec"
            )
        return
    if not isinstance(observation, dict):
        raise SupportDocumentationError(
            f"{entry_id}: superseded proof requires a current observation"
        )
    observed_status = observation.get("run_status") or observation.get("state_status")
    if (
        observed_status not in ACTIVE_RUN_STATUSES
        or observation.get("run_id") == verification_record.get("run_id")
    ):
        raise SupportDocumentationError(
            f"{entry_id}: superseded proof requires a newer active run"
        )


def supersede_changed_entry_proofs(
    verification_records_by_entry: dict[str, dict[str, Any]],
    manifest_entry_by_id: dict[str, dict[str, Any]],
) -> None:
    """Mark prior terminal evidence stale after a source-contract change."""
    for entry_id, verification_record in verification_records_by_entry.items():
        if verification_record.get("terminal_status") is None:
            continue
        expected_entry_sha256 = provider_directory_entry_sha256(
            manifest_entry_by_id[entry_id]
        )
        if verification_record.get("entry_spec_sha256") == expected_entry_sha256:
            continue
        verification_record["proof_state"] = "superseded"
        verification_record["superseded_reason"] = "manifest_entry_changed"
