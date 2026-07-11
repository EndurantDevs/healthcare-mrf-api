"""Validation and rendering for the tracked Provider Directory catalog snapshot."""

from __future__ import annotations

import datetime as dt
from typing import Any

try:
    from scripts.provider_directory_support_contract import SupportDocumentationError
except ModuleNotFoundError:
    from provider_directory_support_contract import SupportDocumentationError


CATALOG_PROBE_STATUSES = (
    "auth_required",
    "valid",
    "valid_non_fhir",
    "dns_failure",
    "timeout",
    "no_api",
    "server_error",
    "unreachable",
)
CATALOG_CONFIRMATION_FIELDS = {
    "environment",
    "checked_at",
    "relation",
    "source_count",
    "probe_status_counts",
    "never_probed_source_count",
    "valid_canonical_base_count",
    "represented_valid_canonical_base_count",
    "unrepresented_valid_canonical_base_count",
    "collapsed_valid_alias_source_count",
    "coverage_note",
}


def _text_fields(confirmation: dict[str, Any]) -> dict[str, str]:
    text_by_field = {
        field_name: str(confirmation.get(field_name) or "").strip()
        for field_name in ("environment", "checked_at", "relation")
    }
    if not all(text_by_field.values()):
        raise SupportDocumentationError("catalog_confirmation fields must be non-empty")
    try:
        checked_at = dt.datetime.fromisoformat(
            text_by_field["checked_at"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise SupportDocumentationError(
            "catalog_confirmation.checked_at must be ISO-8601"
        ) from exc
    if checked_at.tzinfo is None:
        raise SupportDocumentationError(
            "catalog_confirmation.checked_at must include a timezone"
        )
    return text_by_field


def _catalog_size_fields(confirmation: dict[str, Any]) -> dict[str, Any]:
    source_count = confirmation["source_count"]
    never_probed_source_count = confirmation["never_probed_source_count"]
    if type(source_count) is not int or source_count <= 0:
        raise SupportDocumentationError(
            "catalog_confirmation.source_count must be a positive integer"
        )
    if type(never_probed_source_count) is not int or never_probed_source_count < 0:
        raise SupportDocumentationError(
            "catalog_confirmation.never_probed_source_count must be a non-negative integer"
        )
    probe_status_counts = confirmation["probe_status_counts"]
    if (
        not isinstance(probe_status_counts, dict)
        or set(probe_status_counts) != set(CATALOG_PROBE_STATUSES)
    ):
        raise SupportDocumentationError(
            "catalog_confirmation.probe_status_counts must contain exactly "
            + ", ".join(CATALOG_PROBE_STATUSES)
        )
    if not all(
        type(status_count) is int and status_count >= 0
        for status_count in probe_status_counts.values()
    ):
        raise SupportDocumentationError(
            "catalog_confirmation.probe_status_counts values must be non-negative integers"
        )
    if sum(probe_status_counts.values()) + never_probed_source_count != source_count:
        raise SupportDocumentationError(
            "catalog_confirmation source_count must equal probe_status_counts plus "
            "never_probed_source_count"
        )
    return {
        "source_count": source_count,
        "probe_status_counts": probe_status_counts,
        "never_probed_source_count": never_probed_source_count,
    }


def _canonical_coverage_fields(confirmation: dict[str, Any]) -> dict[str, Any]:
    field_names = (
        "valid_canonical_base_count",
        "represented_valid_canonical_base_count",
        "unrepresented_valid_canonical_base_count",
        "collapsed_valid_alias_source_count",
    )
    count_by_field = {
        field_name: confirmation[field_name]
        for field_name in field_names
    }
    if not all(
        type(field_count) is int and field_count >= 0
        for field_count in count_by_field.values()
    ):
        raise SupportDocumentationError(
            "catalog_confirmation canonical coverage counts must be non-negative integers"
        )
    represented_count = count_by_field["represented_valid_canonical_base_count"]
    unrepresented_count = count_by_field["unrepresented_valid_canonical_base_count"]
    canonical_base_count = count_by_field["valid_canonical_base_count"]
    if represented_count + unrepresented_count != canonical_base_count:
        raise SupportDocumentationError(
            "catalog_confirmation represented and unrepresented bases must equal "
            "valid canonical bases"
        )
    valid_source_count = confirmation["probe_status_counts"]["valid"]
    alias_count = count_by_field["collapsed_valid_alias_source_count"]
    if canonical_base_count + alias_count != valid_source_count:
        raise SupportDocumentationError(
            "catalog_confirmation valid bases plus aliases must equal valid sources"
        )
    coverage_note = str(confirmation.get("coverage_note") or "").strip()
    if not coverage_note:
        raise SupportDocumentationError(
            "catalog_confirmation.coverage_note must be non-empty"
        )
    return {**count_by_field, "coverage_note": coverage_note}


def catalog_confirmation_fields(manifest: dict[str, Any]) -> dict[str, Any]:
    """Validate and return the complete live-catalog confirmation."""
    confirmation = manifest.get("catalog_confirmation")
    if (
        not isinstance(confirmation, dict)
        or set(confirmation) != CATALOG_CONFIRMATION_FIELDS
    ):
        raise SupportDocumentationError(
            f"catalog_confirmation must contain {sorted(CATALOG_CONFIRMATION_FIELDS)}"
        )
    return {
        **_text_fields(confirmation),
        **_catalog_size_fields(confirmation),
        **_canonical_coverage_fields(confirmation),
    }


def render_catalog_inventory_snapshot(
    manifest: dict[str, Any],
    blocker_entries: list[dict[str, Any]],
) -> list[str]:
    """Render the full-catalog snapshot separately from curated support entries."""
    confirmation = catalog_confirmation_fields(manifest)
    support_by_entry = manifest["support_documentation"]["entry_support"]
    curated_entry_count = len(manifest["entries"]) + len(blocker_entries)
    acquisition_entry_count = sum(
        support_record["support_level"] == "acquisition-configured"
        for support_record in support_by_entry.values()
    )
    status_rows = ["| Probe status | Sources |", "| --- | ---: |"]
    status_rows.extend(
        f"| `{status}` | {confirmation['probe_status_counts'][status]:,} |"
        for status in CATALOG_PROBE_STATUSES
    )
    status_rows.append(
        f"| Never probed | {confirmation['never_probed_source_count']:,} |"
    )
    return [
        "",
        "## Catalog Inventory Snapshot",
        "",
        _catalog_scope_text(confirmation, curated_entry_count, acquisition_entry_count),
        "",
        _canonical_coverage_text(confirmation),
        "",
        *status_rows,
    ]


def _catalog_scope_text(
    confirmation: dict[str, Any],
    curated_entry_count: int,
    acquisition_entry_count: int,
) -> str:
    return (
        f"This snapshot covers the entire live catalog: `{confirmation['source_count']:,}` "
        f"sources confirmed in `{confirmation['environment']}` against "
        f"`{confirmation['relation']}` at `{confirmation['checked_at']}`. It is not the "
        f"curated support matrix below, which tracks `{curated_entry_count}` entries, "
        f"including `{acquisition_entry_count}` acquisition-configured entries."
    )


def _canonical_coverage_text(confirmation: dict[str, Any]) -> str:
    return (
        f"The `{confirmation['probe_status_counts']['valid']:,}` valid source rows "
        f"collapse to `{confirmation['valid_canonical_base_count']:,}` canonical bases "
        f"after removing `{confirmation['collapsed_valid_alias_source_count']:,}` aliases. "
        f"`{confirmation['represented_valid_canonical_base_count']:,}` bases are represented "
        f"by maintained entries; `{confirmation['unrepresented_valid_canonical_base_count']:,}` "
        f"is not. {confirmation['coverage_note']}"
    )
