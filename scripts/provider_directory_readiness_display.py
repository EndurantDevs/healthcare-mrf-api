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


def publication_readiness_display(
    verification_record: dict[str, Any],
) -> tuple[str, str, str, str]:
    """Render downstream readiness independently from terminal acquisition proof."""
    readiness_record = verification_record.get("publication_readiness")
    if not isinstance(readiness_record, dict):
        return (NOT_RECORDED_DISPLAY,) * 4
    artifact_state = _display_state(readiness_record["derived_artifact_state"])
    api_state = _display_state(readiness_record["unified_api_state"])
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
