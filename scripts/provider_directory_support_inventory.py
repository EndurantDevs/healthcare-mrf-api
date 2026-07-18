#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render the concise inventory portion of Provider Directory support docs."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any, Callable

try:
    from scripts.provider_directory_support_contract import (
        SupportDocumentationError,
        parse_review_date,
        review_valid_through,
    )
except ModuleNotFoundError:
    from provider_directory_support_contract import (
        SupportDocumentationError,
        parse_review_date,
        review_valid_through,
    )


DisplayValue = Callable[[str], str]
NOT_RECORDED_DISPLAY = "Not recorded"
CURRENT_DATASET_STATES = {
    "current-published": "Current published",
    "no-current-dataset": "No current dataset",
    "acquisition-active": "Acquisition active",
    "probe-only": "Probe-only",
    "not-importable": "Not importable",
}
DOWNSTREAM_EVIDENCE_STATES = {
    "snapshot-ready": "Snapshot-ready",
    "not-proven": "Not proven",
    "contract-live-mismatch": "Contract/live mismatch",
    "not-applicable": "Not applicable",
}
CURRENT_DATASET_REQUIRED_FIELDS = {
    "source", "dataset_state", "downstream_evidence", "note"
}
CURRENT_DATASET_ALLOWED_FIELDS = CURRENT_DATASET_REQUIRED_FIELDS | {
    "dataset_id", "entry_id", "entry_ids", "observed_at", "resource_count"
}


def _markdown_cell(value: str) -> str:
    """Escape text for a single Markdown table cell."""
    return value.replace("|", "\\|").replace("\n", "<br>")


def load_current_dataset_audit(audit_path: Path) -> dict[str, Any]:
    """Load the dated operational dataset audit independently of acquisition config."""
    decoded = json.loads(audit_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise SupportDocumentationError(f"{audit_path} must contain a JSON object")
    return decoded


def _validated_audit_entry_ids(
    audit_record: dict[str, Any],
    manifest_entry_ids: set[str],
) -> list[str]:
    """Validate and return manifest entries represented by one audit record."""
    entry_id = audit_record.get("entry_id")
    entry_ids = audit_record.get("entry_ids")
    if entry_id is not None and entry_ids is not None:
        raise SupportDocumentationError("current dataset audit record cannot use entry_id and entry_ids")
    represented_entry_ids = [entry_id] if entry_id is not None else entry_ids or []
    if not isinstance(represented_entry_ids, list) or any(
        not isinstance(item, str) or item not in manifest_entry_ids
        for item in represented_entry_ids
    ):
        source_label = audit_record.get("source", "unknown source")
        raise SupportDocumentationError(f"invalid audit entry_id for {source_label}")
    if len(represented_entry_ids) != len(set(represented_entry_ids)):
        raise SupportDocumentationError("duplicate entry_id within current dataset audit record")
    return represented_entry_ids


def _validate_snapshot_ready_record(
    audit_record: dict[str, Any],
    represented_entry_ids: list[str],
    verification_snapshot: dict[str, Any] | None,
) -> None:
    """Require promoted artifacts and API readiness behind snapshot-ready claims."""
    if audit_record["downstream_evidence"] != "snapshot-ready" or verification_snapshot is None:
        return
    if len(represented_entry_ids) != 1:
        raise SupportDocumentationError("snapshot-ready audit records must map to one entry_id")
    readiness = verification_snapshot.get("entries", {}).get(
        represented_entry_ids[0], {}
    ).get("publication_readiness", {})
    if readiness.get("derived_artifact_state") != "promoted" or readiness.get(
        "unified_api_state"
    ) != "ready":
        raise SupportDocumentationError(
            f"snapshot-ready audit record lacks ready verification for {represented_entry_ids[0]}"
        )


def _validate_current_dataset_record_shape(audit_record: Any) -> str:
    """Validate scalar fields for one current-dataset observation."""
    if not isinstance(audit_record, dict) or set(audit_record) - CURRENT_DATASET_ALLOWED_FIELDS:
        raise SupportDocumentationError("current dataset audit has unsupported fields")
    if not all(
        isinstance(audit_record.get(field_name), str)
        and audit_record[field_name].strip()
        for field_name in CURRENT_DATASET_REQUIRED_FIELDS
    ):
        raise SupportDocumentationError("current dataset audit has missing fields")
    source_label = audit_record["source"]
    if audit_record["dataset_state"] not in CURRENT_DATASET_STATES:
        raise SupportDocumentationError(f"invalid dataset state for {source_label}")
    if audit_record["downstream_evidence"] not in DOWNSTREAM_EVIDENCE_STATES:
        raise SupportDocumentationError(f"invalid evidence state for {source_label}")
    if audit_record["dataset_state"] != "current-published":
        return source_label
    if not str(audit_record.get("dataset_id", "")).startswith("pdds_"):
        raise SupportDocumentationError(
            f"current dataset audit lacks dataset_id for {source_label}"
        )
    if not isinstance(audit_record.get("resource_count"), int) or audit_record[
        "resource_count"
    ] < 0:
        raise SupportDocumentationError(
            f"current dataset audit lacks resource_count for {source_label}"
        )
    try:
        dt.datetime.fromisoformat(str(audit_record.get("observed_at", "")))
    except ValueError as exc:
        raise SupportDocumentationError(
            f"current dataset audit lacks observed_at for {source_label}"
        ) from exc
    return source_label


def _validated_current_dataset_audit_records(
    current_dataset_audit: dict[str, Any],
    acquisition_manifest: dict[str, Any],
    verification_snapshot: dict[str, Any] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    """Validate one dated operational snapshot independently of acquisition config."""
    if current_dataset_audit.get("schema_version") != 1:
        raise SupportDocumentationError("current dataset audit schema_version must be 1")
    audit_date = current_dataset_audit.get("as_of")
    try:
        dt.date.fromisoformat(audit_date)
    except (TypeError, ValueError) as exc:
        raise SupportDocumentationError("current dataset audit as_of must be an ISO date") from exc
    audit_records = current_dataset_audit.get("records")
    if not isinstance(audit_records, list) or not audit_records:
        raise SupportDocumentationError("current dataset audit records must be non-empty")
    manifest_entry_ids = {
        entry["entry_id"] for entry in acquisition_manifest["entries"]
    }
    source_labels = set()
    represented_manifest_entry_ids: set[str] = set()
    for audit_record in audit_records:
        source_label = _validate_current_dataset_record_shape(audit_record)
        if source_label in source_labels:
            raise SupportDocumentationError(f"duplicate current dataset source {source_label!r}")
        source_labels.add(source_label)
        represented_entry_ids = _validated_audit_entry_ids(
            audit_record, manifest_entry_ids
        )
        duplicate_entry_ids = represented_manifest_entry_ids.intersection(
            represented_entry_ids
        )
        if duplicate_entry_ids:
            raise SupportDocumentationError(
                f"duplicate current dataset entry_id {sorted(duplicate_entry_ids)[0]}"
            )
        represented_manifest_entry_ids.update(represented_entry_ids)
        _validate_snapshot_ready_record(
            audit_record, represented_entry_ids, verification_snapshot
        )
    missing_entry_ids = manifest_entry_ids - represented_manifest_entry_ids
    if missing_entry_ids:
        raise SupportDocumentationError(
            "current dataset audit misses manifest entry_ids: "
            + ", ".join(sorted(missing_entry_ids))
        )
    return audit_date, audit_records


def render_current_dataset_audit_section(
    current_dataset_audit: dict[str, Any],
    acquisition_manifest: dict[str, Any],
    verification_snapshot: dict[str, Any] | None = None,
) -> list[str]:
    """Render dated dataset observations without changing acquisition support."""
    audit_date, audit_records = _validated_current_dataset_audit_records(
        current_dataset_audit, acquisition_manifest, verification_snapshot
    )
    audit_lines = [
        "",
        "## Current Published Dataset Audit",
        "",
        f"Audit as of `{audit_date}`. A current published dataset is distinct from configured acquisition capability, terminal campaign proof, and downstream publication readiness. `Snapshot-ready` means the separate verification snapshot records readiness; `Not proven` means no downstream publication proof is asserted here.",
        "",
        "| Source | Dataset state | Resources | Downstream evidence | Audit note |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for audit_record in audit_records:
        source_label = audit_record["source"]
        represented_entry_ids = audit_record.get("entry_ids") or [
            audit_record.get("entry_id")
        ]
        represented_entry_ids = [entry_id for entry_id in represented_entry_ids if entry_id]
        if represented_entry_ids:
            source_label += " (" + ", ".join(
                f"`{entry_id}`" for entry_id in represented_entry_ids
            ) + ")"
        dataset_state = CURRENT_DATASET_STATES[audit_record["dataset_state"]]
        resource_count = audit_record.get("resource_count")
        if audit_record.get("dataset_id"):
            dataset_state += f" (`{audit_record['dataset_id'][:17]}...`)"
        audit_lines.append(
            "| "
            + " | ".join(
                _markdown_cell(cell)
                for cell in (
                    source_label,
                    dataset_state,
                    f"{resource_count:,}" if resource_count is not None else "-",
                    DOWNSTREAM_EVIDENCE_STATES[
                        audit_record["downstream_evidence"]
                    ],
                    audit_record["note"],
                )
            )
            + " |"
        )
    return audit_lines


def _format_credential_row(
    source_label: str,
    support_label: str,
    access_label: str,
    registration_required: bool,
) -> str:
    """Render one non-public or registration-required source row."""
    cells = (
        source_label,
        support_label,
        access_label,
        "Required" if registration_required else "Not required",
    )
    return "| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |"


def _credential_rows(
    manifest: dict[str, Any],
    support_by_entry: dict[str, dict[str, Any]],
    blockers: list[dict[str, Any]],
    display_value: DisplayValue,
) -> list[str]:
    """Return credential or registration rows from runnable and blocked sources."""
    rows = []
    for entry in manifest["entries"]:
        support_record = support_by_entry[entry["entry_id"]]
        if support_record["access_requirement"] == "none" and not support_record["requires_registration"]:
            continue
        rows.append(_format_credential_row(
            f"{entry['display_name']} (`{entry['entry_id']}`)",
            display_value(support_record["support_level"]),
            display_value(support_record["access_requirement"]),
            support_record["requires_registration"],
        ))
    for blocker in blockers:
        if blocker["access_requirement"] == "none" and not blocker["requires_registration"]:
            continue
        rows.append(_format_credential_row(
            f"{blocker['display_name']} (`{blocker['id']}`)",
            display_value("not-supported"),
            display_value(blocker["access_requirement"]),
            blocker["requires_registration"],
        ))
    return rows


def render_inventory_summary(
    manifest: dict[str, Any],
    support_by_entry: dict[str, dict[str, Any]],
    blockers: list[dict[str, Any]],
    display_value: DisplayValue,
) -> list[str]:
    """Render support totals and the credentialed-access source list."""
    count_by_level = {
        level: sum(
            support_record["support_level"] == level
            for support_record in support_by_entry.values()
        )
        for level in ("acquisition-configured", "externally-supported", "probe-only")
    }
    summary_lines = [
        "", "## Inventory Summary", "", "| Category | Count |", "| --- | ---: |",
        f"| {display_value('acquisition-configured')} | {count_by_level['acquisition-configured']} |",
        f"| {display_value('externally-supported')} | {count_by_level['externally-supported']} |",
        f"| {display_value('probe-only')} | {count_by_level['probe-only']} |",
        f"| Known not importable | {len(blockers)} |",
        f"| Total tracked | {len(manifest['entries']) + len(blockers)} |",
        "", "### Credentialed Or Registered Access", "",
        "| Source | Support | Access | Registration |", "| --- | --- | --- | --- |",
    ]
    summary_lines.extend(_credential_rows(manifest, support_by_entry, blockers, display_value))
    summary_lines.extend([
        "",
        "## Configured Sources",
        "",
        "| Source | Configured support | Configured access requirement | Method | Resources | Canonical base | Source IDs | Registration | Reviewed at | Review valid through | Known blocker or limitation |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    return summary_lines


def render_blocked_support_section(
    blocker_entries: list[dict[str, Any]],
    maximum_age_days: int,
    display_value: DisplayValue,
    display_verification: DisplayValue,
) -> list[str]:
    """Render known sources that have no runnable acquisition base."""
    markdown_lines = [
        "",
        "## Known Not Importable",
        "",
        "These sources are intentionally retained as blocked catalog evidence. They are not probe-only entries and have no runnable acquisition base.",
        "",
        "| Source | Plan | Support | Required access | Registration | Method | Resources | Canonical base | Operational state | Reviewed at | Review valid through | Live verification | Primary evidence | Blocker |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for blocker_entry in blocker_entries:
        reviewed_on = parse_review_date(
            blocker_entry["reviewed_at"],
            f"{blocker_entry['id']}: reviewed_at",
        )
        cells = [
            f"{blocker_entry['display_name']} (`{blocker_entry['id']}`)",
            blocker_entry["plan_name"],
            display_value("not-supported"),
            display_value(blocker_entry["access_requirement"]),
            "Required" if blocker_entry["requires_registration"] else "Not required",
            display_value(blocker_entry["acquisition_method"]),
            ", ".join(blocker_entry["documented_resources"]) or "None confirmed",
            blocker_entry["canonical_base"] or "None confirmed",
            blocker_entry["operational_status"].replace("-", " ").title(),
            blocker_entry["reviewed_at"],
            review_valid_through(reviewed_on, maximum_age_days),
            display_verification(blocker_entry["live_verification"]["status"]),
            blocker_entry["source_url"],
            blocker_entry["note"],
        ]
        markdown_lines.append(
            "| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |"
        )
    return markdown_lines


def resource_completion_display(
    entry: dict[str, Any],
    support_record: dict[str, Any],
    verification_record: dict[str, Any],
) -> str:
    """Summarize whether terminal evidence covers every configured resource."""
    expected_resources = list(
        support_record.get("documented_resources") or entry["resources"]
    )
    if not expected_resources:
        return "Not applicable"
    terminal_evidence = verification_record.get("terminal_evidence")
    resource_outcomes = (
        terminal_evidence.get("resource_outcomes")
        if isinstance(terminal_evidence, dict)
        else None
    )
    if not isinstance(resource_outcomes, dict):
        return NOT_RECORDED_DISPLAY
    for resource_name in expected_resources:
        resource_outcome = resource_outcomes.get(resource_name)
        if not isinstance(resource_outcome, dict):
            return "Incomplete"
        attempted_sources = resource_outcome.get("sources_attempted")
        if (
            not isinstance(attempted_sources, int)
            or attempted_sources <= 0
            or resource_outcome.get("sources_completed") != attempted_sources
            or resource_outcome.get("sources_bounded") != 0
            or resource_outcome.get("sources_failed") != 0
        ):
            return "Incomplete"
    return "Complete"
