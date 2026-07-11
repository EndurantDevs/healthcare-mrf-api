#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render the concise inventory portion of Provider Directory support docs."""
from __future__ import annotations

from typing import Any, Callable


DisplayValue = Callable[[str], str]


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
        "| Source | Configured support | Configured access requirement | Method | Resources | Canonical base | Source IDs | Registration | Reviewed at | Known blocker or limitation |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ])
    return summary_lines
