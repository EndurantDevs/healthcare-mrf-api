#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render the concise inventory portion of Provider Directory support docs."""
from __future__ import annotations

from typing import Any, Callable

try:
    from scripts.provider_directory_support_contract import (
        parse_review_date,
        review_valid_through,
    )
except ModuleNotFoundError:
    from provider_directory_support_contract import (
        parse_review_date,
        review_valid_through,
    )


DisplayValue = Callable[[str], str]
NOT_RECORDED_DISPLAY = "Not recorded"


def _markdown_cell(value: str) -> str:
    """Escape text for a single Markdown table cell."""
    return value.replace("|", "\\|").replace("\n", "<br>")


def _credential_row(
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
        rows.append(_credential_row(
            f"{entry['display_name']} (`{entry['entry_id']}`)",
            display_value(support_record["support_level"]),
            display_value(support_record["access_requirement"]),
            support_record["requires_registration"],
        ))
    for blocker in blockers:
        if blocker["access_requirement"] == "none" and not blocker["requires_registration"]:
            continue
        rows.append(_credential_row(
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
