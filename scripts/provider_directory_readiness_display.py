"""Render Provider Directory publication readiness for generated documentation."""

from __future__ import annotations

from typing import Any

NOT_RECORDED_DISPLAY = "Not recorded"
LABEL_BY_COUNT_FIELD = {
    "source_rows": "Source rows",
    "location_rows": "Location rows",
    "address_rows": "Address rows",
    "address_keys": "Address keys",
    "phone_rows": "Phone rows",
    "coordinate_rows": "Coordinate rows",
    "role_to_plan_refs": "Role-to-plan refs",
}


def _display_state(state_name: str) -> str:
    return state_name.replace("_", " ").title()


def display_verification(value: str | None) -> str:
    """Render a controlled verification state for generated documentation."""
    if value is None or value in {"not_recorded", "not recorded"}:
        return NOT_RECORDED_DISPLAY
    if value == "not_verified":
        return "Not verified"
    if value == "verified":
        return "Verified"
    return value.replace("_", " ").title()


def observation_display(record: dict[str, Any]) -> str:
    """Render the latest raw run observation without duplicating terminal evidence."""
    observation = record.get("current_observation")
    if not isinstance(observation, dict):
        terminal_status = record.get("terminal_status")
        run_id = record.get("run_id")
        observed_at = record.get("checked_at")
        if not all(isinstance(value, str) and value for value in (terminal_status, run_id, observed_at)):
            return NOT_RECORDED_DISPLAY
        return f"{display_verification(terminal_status)} (`{run_id}`) at `{observed_at}`"
    status = observation.get("run_status") or observation["state_status"]
    run_id = observation.get("run_id") or NOT_RECORDED_DISPLAY
    return f"{display_verification(str(status))} (`{run_id}`) at `{observation['observed_at']}`"


def publication_readiness_display(
    verification_record: dict[str, Any],
) -> tuple[str, str, str, str]:
    """Render downstream readiness independently from terminal acquisition proof."""
    readiness_record = verification_record.get("publication_readiness")
    if not isinstance(readiness_record, dict):
        return (NOT_RECORDED_DISPLAY,) * 4
    artifact_state = _display_state(readiness_record["derived_artifact_state"])
    api_state = _display_state(readiness_record["unified_api_state"])
    if readiness_record.get("proof_state") == "superseded":
        artifact_state = f"Superseded ({artifact_state})"
        api_state = f"Superseded ({api_state})"
    observed_at = readiness_record["observed_at"]
    readiness_evidence = readiness_record.get("evidence")
    if not isinstance(readiness_evidence, dict):
        return (artifact_state, api_state, observed_at, NOT_RECORDED_DISPLAY)
    evidence_labels: list[str] = []
    readiness_counts = readiness_evidence.get("counts")
    if isinstance(readiness_counts, dict):
        evidence_labels.extend(
            f"{LABEL_BY_COUNT_FIELD[field_name]}: {readiness_counts[field_name]:,}"
            for field_name in LABEL_BY_COUNT_FIELD
            if field_name in readiness_counts
        )
    readiness_signals = readiness_evidence.get("signals")
    if isinstance(readiness_signals, dict):
        evidence_labels.extend(
            f"{field_name.replace('_', ' ').title()}: {_display_state(signal_state)}"
            for field_name, signal_state in readiness_signals.items()
        )
    return (
        artifact_state,
        api_state,
        observed_at,
        "<br>".join(evidence_labels) or "Evidence recorded",
    )
