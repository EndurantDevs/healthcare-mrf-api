#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generate the maintained Provider Directory endpoint support matrix."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
DEFAULT_OUTPUT = ROOT / "docs/imports/provider-directory-endpoint-support.md"
SUPPORT_LEVELS = {
    "supported",
    "current-connector",
    "acquisition-configured",
    "probe-only",
    "blocked",
}
ACCESS_REQUIREMENTS = {
    "none",
    "oauth2-client-credentials",
    "private-connector",
    "unknown",
}
METHODS = {"rest", "bulk", "graphql", "probe"}
RESOURCE_TYPES = {
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
}
DISPLAY_VALUES = {
    "supported": "Supported",
    "current-connector": "Current connector",
    "acquisition-configured": "Acquisition-configured",
    "probe-only": "Probe-only",
    "blocked": "Blocked",
    "none": "None",
    "oauth2-client-credentials": "OAuth2 client credentials",
    "private-connector": "Private connector",
    "unknown": "Unknown",
    "rest": "REST",
    "bulk": "Bulk",
    "graphql": "GraphQL",
    "probe": "Probe",
}


class SupportDocumentationError(ValueError):
    """Raised when generated support documentation cannot be trusted."""


def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load the endpoint manifest as a JSON object."""
    decoded = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise SupportDocumentationError(f"{manifest_path} must contain a JSON object")
    return decoded


def _display(value: str) -> str:
    return DISPLAY_VALUES[value]


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


def _entry_ids(entries: Any) -> list[str]:
    if not isinstance(entries, list) or not entries:
        raise SupportDocumentationError("entries must be a non-empty list")
    entry_ids = [str(entry.get("entry_id") or "") for entry in entries if isinstance(entry, dict)]
    if len(entry_ids) != len(entries) or not all(entry_ids) or len(set(entry_ids)) != len(entry_ids):
        raise SupportDocumentationError("entries must have unique non-empty entry_id values")
    return entry_ids


def _validate_entry_support(entry: dict[str, Any], support: Any) -> None:
    entry_id = str(entry["entry_id"])
    required_fields = {"support_level", "access_requirement", "method", "limitation"}
    optional_fields = {"documented_resources"}
    if not isinstance(support, dict) or not required_fields.issubset(support) or not set(support).issubset(required_fields | optional_fields):
        raise SupportDocumentationError(f"{entry_id}: support metadata must contain {sorted(required_fields)} and optional documented_resources only")
    support_level = support["support_level"]
    access_requirement = support["access_requirement"]
    method = support["method"]
    limitation = support["limitation"]
    if support_level not in SUPPORT_LEVELS:
        raise SupportDocumentationError(f"{entry_id}: invalid support level {support_level!r}")
    if access_requirement not in ACCESS_REQUIREMENTS:
        raise SupportDocumentationError(f"{entry_id}: invalid access requirement {access_requirement!r}")
    if method not in METHODS:
        raise SupportDocumentationError(f"{entry_id}: invalid method {method!r}")
    if not isinstance(limitation, str) or not limitation.strip():
        raise SupportDocumentationError(f"{entry_id}: limitation must be non-empty text")
    documented_resources = support.get("documented_resources")
    if documented_resources is not None and (
        not isinstance(documented_resources, list)
        or not documented_resources
        or not all(resource in RESOURCE_TYPES for resource in documented_resources)
    ):
        raise SupportDocumentationError(f"{entry_id}: documented_resources must contain known resource types")
    classification = entry.get("classification")
    if classification == "probe_only" and (support_level != "probe-only" or method != "probe"):
        raise SupportDocumentationError(f"{entry_id}: probe_only entries require probe-only support and probe method")
    if classification == "bulk_acquisition" and method != "bulk":
        raise SupportDocumentationError(f"{entry_id}: bulk_acquisition entries require bulk method")
    if classification == "acquisition" and method != "rest":
        raise SupportDocumentationError(f"{entry_id}: acquisition entries require REST method")


def validate_manifest(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate complete top-level support metadata without altering run entries."""
    entries = manifest.get("entries")
    documentation = manifest.get("support_documentation")
    if not isinstance(documentation, dict):
        raise SupportDocumentationError("support_documentation must be an object")
    if documentation.get("schema_version") != 1:
        raise SupportDocumentationError("support_documentation.schema_version must be 1")
    report_path = documentation.get("runtime_status_report")
    if not isinstance(report_path, str) or not report_path.startswith("reports/"):
        raise SupportDocumentationError("runtime_status_report must be a reports/ path")
    support_by_entry = documentation.get("entry_support")
    if not isinstance(support_by_entry, dict):
        raise SupportDocumentationError("entry_support must be an object")
    entry_ids = _entry_ids(entries)
    missing_ids = sorted(set(entry_ids) - set(support_by_entry))
    extra_ids = sorted(set(support_by_entry) - set(entry_ids))
    if missing_ids or extra_ids:
        problems = []
        if missing_ids:
            problems.append("missing metadata for " + ", ".join(missing_ids))
        if extra_ids:
            problems.append("metadata without manifest entry " + ", ".join(extra_ids))
        raise SupportDocumentationError("; ".join(problems))
    for entry in entries:
        _validate_entry_support(entry, support_by_entry[str(entry["entry_id"])])
    return support_by_entry


def render_markdown(manifest: dict[str, Any]) -> str:
    """Render stable Markdown from manifest entries and support metadata."""
    support_by_entry = validate_manifest(manifest)
    report_path = manifest["support_documentation"]["runtime_status_report"]
    markdown_lines = [
        "# Provider Directory Endpoint Support",
        "",
        "This matrix describes maintained implementation and campaign configuration. It does not claim that a live probe succeeded, that an import ran, or that a dataset is current. Runtime and import status are written locally or on dev by the endpoint-acquisition harness to `" + report_path + "`, or to its selected `--report` path; the report is not tracked.",
        "",
        "`None` access means the configuration expects public access, not that the endpoint is currently reachable. `Probe-only` entries have no resource acquisition configured and must not be treated as imported.",
        "",
        "| Source | Configured support | Access requirement | Method | Resources | Canonical base | Source IDs | Known blocker or limitation |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in manifest["entries"]:
        support_record = support_by_entry[entry["entry_id"]]
        documented_resources = support_record.get("documented_resources")
        resources = documented_resources or entry["resources"]
        resource_text = ", ".join(resources) if resources else "None configured"
        source_label = f"{entry['display_name']} (`{entry['entry_id']}`)"
        cells = [
            source_label,
            _display(support_record["support_level"]),
            _display(support_record["access_requirement"]),
            _display(support_record["method"]),
            resource_text,
            entry["canonical_base"],
            ", ".join(entry["source_ids"]),
            support_record["limitation"],
        ]
        markdown_lines.append("| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |")
    markdown_lines.extend(["", "Generated by `scripts/generate_provider_directory_support_docs.py`; do not edit this file directly.", ""])
    return "\n".join(markdown_lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse deterministic generation arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true", help="Fail when the generated document is missing or stale.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write the support matrix or check it for drift."""
    args = parse_args(argv)
    rendered = render_markdown(load_manifest(args.manifest))
    current = args.output.read_text(encoding="utf-8") if args.output.exists() else None
    if args.check:
        if current != rendered:
            print(f"Provider Directory support documentation is stale: {args.output}")
            return 1
        print(f"Provider Directory support documentation is current: {args.output}")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"Wrote Provider Directory support documentation: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
