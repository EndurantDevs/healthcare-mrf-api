#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generate the maintained Provider Directory endpoint support matrix."""
from __future__ import annotations
import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

try:
    from scripts.provider_directory_catalog_confirmation import (
        catalog_confirmation_fields as _catalog_confirmation_fields,
        render_catalog_inventory_snapshot as _catalog_inventory_snapshot,
    )
    from scripts.provider_directory_support_contract import (
        ACCESS_REQUIREMENTS,
        RESOURCE_TYPES,
        SUPPORT_LEVELS,
        SupportDocumentationError,
        validate_access_review_metadata,
        validate_blocker_registry,
        parse_review_date,
        parse_timestamp_date,
        review_valid_through,
        validate_configured_endpoint,
        validate_freshness_policy,
        validate_support_freshness,
    )
    from scripts.provider_directory_support_inventory import (
        render_blocked_support_section,
        render_inventory_summary,
        resource_completion_display,
    )
    from scripts.provider_directory_verification_contract import (
        VERIFICATION_STATUSES,
        provider_directory_entry_sha256,
        validate_optional_verification_timestamp,
        validate_verification_record,
    )
except ModuleNotFoundError:
    from provider_directory_catalog_confirmation import (
        catalog_confirmation_fields as _catalog_confirmation_fields,
        render_catalog_inventory_snapshot as _catalog_inventory_snapshot,
    )
    from provider_directory_support_contract import (
        ACCESS_REQUIREMENTS,
        RESOURCE_TYPES,
        SUPPORT_LEVELS,
        SupportDocumentationError,
        validate_access_review_metadata,
        validate_blocker_registry,
        parse_review_date,
        parse_timestamp_date,
        review_valid_through,
        validate_configured_endpoint,
        validate_freshness_policy,
        validate_support_freshness,
    )
    from provider_directory_support_inventory import (
        render_blocked_support_section,
        render_inventory_summary,
        resource_completion_display,
    )
    from provider_directory_verification_contract import (
        VERIFICATION_STATUSES,
        provider_directory_entry_sha256,
        validate_optional_verification_timestamp,
        validate_verification_record,
    )
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
DEFAULT_OUTPUT = ROOT / "docs/imports/provider-directory-endpoint-support.md"
DEFAULT_BLOCKER_REGISTRY = ROOT / "specs/provider_directory_blocker_registry.json"
DEFAULT_VERIFICATION_SNAPSHOT = ROOT / "specs/provider_directory_endpoint_verification.json"
METHODS = {"rest", "bulk", "graphql", "probe"}
DISPLAY_VALUES = {
    "supported": "Supported",
    "externally-supported": "Externally supported",
    "current-connector": "Current connector",
    "acquisition-configured": "Acquisition-configured",
    "probe-only": "Probe-only",
    "blocked": "Blocked",
    "not-supported": "Not supported",
    "none": "None",
    "oauth2-client-credentials": "OAuth2 client credentials",
    "private-connector": "Private connector",
    "user-token": "User token",
    "unknown": "Unknown",
    "rest": "REST",
    "bulk": "Bulk",
    "graphql": "GraphQL",
    "probe": "Probe",
    "not-importable": "Not importable",
}
NOT_RECORDED = "not recorded"
NOT_RECORDED_DISPLAY = "Not recorded"
def load_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load the endpoint manifest as a JSON object."""
    decoded = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise SupportDocumentationError(f"{manifest_path} must contain a JSON object")
    return decoded
def load_blocker_registry(registry_path: Path) -> dict[str, Any]:
    """Load the maintained non-importable source registry."""
    decoded = json.loads(registry_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise SupportDocumentationError(f"{registry_path} must contain a JSON object")
    return decoded
def load_verification_snapshot(snapshot_path: Path) -> dict[str, Any]:
    """Load the tracked terminal live-verification snapshot."""
    decoded = json.loads(snapshot_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise SupportDocumentationError(f"{snapshot_path} must contain a JSON object")
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
    required_fields = {"support_level", "access_requirement", "requires_registration", "reviewed_at", "method", "limitation"}
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
    validate_access_review_metadata(entry_id, support)
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
    validate_configured_endpoint(entry, support)
def validate_manifest(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate complete top-level support metadata without altering run entries."""
    entries = manifest.get("entries")
    documentation = manifest.get("support_documentation")
    if not isinstance(documentation, dict):
        raise SupportDocumentationError("support_documentation must be an object")
    if documentation.get("schema_version") != 1:
        raise SupportDocumentationError("support_documentation.schema_version must be 1")
    validate_freshness_policy(manifest)
    _catalog_confirmation_fields(manifest)
    report_path = documentation.get("runtime_status_report")
    if not isinstance(report_path, str) or not report_path.startswith("reports/"):
        raise SupportDocumentationError("runtime_status_report must be a reports/ path")
    blocker_path = documentation.get("blocker_registry")
    if (
        not isinstance(blocker_path, str)
        or not blocker_path.startswith("specs/")
        or ".." in Path(blocker_path).parts
    ):
        raise SupportDocumentationError("blocker_registry must be a safe specs/ path")
    verification_path = documentation.get("verification_snapshot")
    if (
        not isinstance(verification_path, str)
        or not verification_path.startswith("specs/")
        or ".." in Path(verification_path).parts
    ):
        raise SupportDocumentationError("verification_snapshot must be a safe specs/ path")
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


def validate_verification_snapshot(
    snapshot: dict[str, Any],
    manifest: dict[str, Any],
    *,
    allow_current_spec_mismatch: bool = False,
) -> dict[str, dict[str, Any]]:
    """Validate the tracked snapshot and return its per-entry records."""
    if snapshot.get("schema_version") != 1:
        raise SupportDocumentationError("verification snapshot schema_version must be 1")
    if snapshot.get("campaign_id") != manifest["campaign_id"]:
        raise SupportDocumentationError("verification snapshot campaign_id does not match the manifest")
    environment = snapshot.get("environment")
    if not isinstance(environment, str) or not environment.strip():
        raise SupportDocumentationError("verification snapshot environment must be non-empty text")
    validate_optional_verification_timestamp(
        snapshot.get("checked_at"),
        "verification snapshot checked_at",
    )
    entries = snapshot.get("entries")
    manifest_entries = manifest["entries"]
    entry_ids = [entry["entry_id"] for entry in manifest_entries]
    if not isinstance(entries, dict):
        raise SupportDocumentationError("verification snapshot entries must be an object")
    if set(entries) != set(entry_ids):
        raise SupportDocumentationError("verification snapshot entries must match manifest entries exactly")
    for manifest_entry in manifest_entries:
        entry_id = manifest_entry["entry_id"]
        validate_verification_record(
            entry_id,
            entries[entry_id],
            provider_directory_entry_sha256(manifest_entry),
            allow_current_spec_mismatch=allow_current_spec_mismatch,
        )
    return entries
def _support_document_header(manifest: dict[str, Any]) -> list[str]:
    report_path = manifest["support_documentation"]["runtime_status_report"]
    confirmation_by_field = _catalog_confirmation_fields(manifest)
    policy = validate_freshness_policy(manifest)
    return [
        "# Provider Directory Endpoint Support",
        "",
        "This matrix describes maintained implementation and campaign configuration. It does not claim that a live probe succeeded, that an import ran, or that a dataset is current. Runtime and import status are written locally or on dev by the endpoint-acquisition harness to `" + report_path + "`, or to its selected `--report` path; the report is not tracked.",
        "",
        "The live catalog and curated support matrix are distinct: the catalog inventory covers every source in `" + confirmation_by_field["relation"] + "`, while this maintained matrix tracks only sources with curated support records. The tracked verification snapshot remains the authority for terminal per-endpoint live status.",
        "",
        "`None` access means the configuration expects public access, not that the endpoint is currently reachable. `Probe-only` entries have no resource acquisition configured and must not be treated as imported.",
        "",
        "Freshness policy: catalog confirmation expires after `"
        + str(policy["catalog_confirmation_max_age_days"])
        + "` days, source reviews after `"
        + str(policy["source_review_max_age_days"])
        + "` days, and current terminal proof after `"
        + str(policy["terminal_verification_max_age_days"])
        + "` days. CI rejects expired evidence.",
    ]


def _configured_support_rows(
    manifest: dict[str, Any],
    support_by_entry: dict[str, dict[str, Any]],
) -> list[str]:
    markdown_rows = []
    maximum_age_days = validate_freshness_policy(manifest)[
        "source_review_max_age_days"
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
            "Required" if support_record["requires_registration"] else "Not required",
            support_record["reviewed_at"],
            review_valid_through(
                parse_review_date(
                    support_record["reviewed_at"],
                    f"{entry['entry_id']}: reviewed_at",
                ),
                maximum_age_days,
            ),
            support_record["limitation"],
        ]
        markdown_rows.append("| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |")
    return markdown_rows
def _display_verification(value: str | None) -> str:
    if value is None or value in {"not_recorded", NOT_RECORDED}:
        return NOT_RECORDED_DISPLAY
    if value == "not_verified":
        return "Not verified"
    if value == "verified":
        return "Verified"
    return value.replace("_", " ").title()
def _observation_display(record: dict[str, Any]) -> str:
    observation = record.get("current_observation")
    if not isinstance(observation, dict):
        return NOT_RECORDED_DISPLAY
    status = observation.get("run_status") or observation["state_status"]
    run_id = observation.get("run_id") or NOT_RECORDED_DISPLAY
    return f"{_display_verification(str(status))} (`{run_id}`) at `{observation['observed_at']}`"


def _terminal_resource_rows_display(
    entry: dict[str, Any],
    verification_record: dict[str, Any],
) -> str:
    terminal_evidence = verification_record.get("terminal_evidence")
    resource_outcomes = (
        terminal_evidence.get("resource_outcomes")
        if isinstance(terminal_evidence, dict)
        else None
    )
    if not isinstance(resource_outcomes, dict):
        return NOT_RECORDED_DISPLAY
    row_count_labels = []
    for resource_name in entry["resources"]:
        resource_outcome = resource_outcomes.get(resource_name)
        rows_fetched = (
            resource_outcome.get("rows_fetched")
            if isinstance(resource_outcome, dict)
            else None
        )
        if not isinstance(rows_fetched, int) or rows_fetched < 0:
            continue
        row_count_labels.append(f"{resource_name}: {rows_fetched:,}")
    return "<br>".join(row_count_labels) or "Evidence recorded"


def _observed_verification_section(
    manifest: dict[str, Any],
    snapshot: dict[str, Any],
) -> list[str]:
    verification_records = validate_verification_snapshot(snapshot, manifest)
    checked_at = snapshot["checked_at"] or NOT_RECORDED
    support_by_entry = manifest["support_documentation"]["entry_support"]
    verification_age = validate_freshness_policy(manifest)[
        "terminal_verification_max_age_days"
    ]
    return_lines = [
        "",
        "## Observed Live Verification",
        "",
        "This tracked snapshot is separate from configured support. It records credential-safe terminal proof and the latest observed run state. Every terminal record is bound to the fingerprint of its source entry, so changing an endpoint, resource set, or acquisition parameters invalidates only that source's current proof. Support-note edits do not invalidate unrelated acquisition evidence. When a newer active run supersedes older terminal proof, the old proof remains visible as `Superseded` and is not presented as current.",
        "",
        "After a terminal campaign, use the report's `verification_update.argv` or run `python scripts/update_provider_directory_verification.py --report <credential-safe-report.json> --environment <environment>`. The updater rejects stale reports, manifest or campaign mismatches, and terminal labels backed by nonterminal runs.",
        "",
        f"Verification environment: `{snapshot['environment']}`. Campaign: `{snapshot['campaign_id']}`. Snapshot checked at `{checked_at}`.",
        "",
        "| Source | Proof state | Terminal status | Resource completion | Terminal run ID | Current observation | Access verification | Terminal checked at | Proof valid through | Rows by resource |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for entry in manifest["entries"]:
        verification_record = verification_records[entry["entry_id"]]
        cells = [
            f"{entry['display_name']} (`{entry['entry_id']}`)",
            _display_verification(verification_record.get("proof_state", "current" if verification_record["terminal_status"] else "not_recorded")),
            _display_verification(verification_record["terminal_status"]),
            resource_completion_display(
                entry, support_by_entry[entry["entry_id"]], verification_record
            ),
            verification_record["run_id"] or NOT_RECORDED_DISPLAY,
            _observation_display(verification_record),
            _display_verification(verification_record["access_verification"]),
            verification_record["checked_at"] or NOT_RECORDED_DISPLAY,
            (
                review_valid_through(
                    parse_timestamp_date(
                        verification_record["checked_at"],
                        f"{entry['entry_id']}: checked_at",
                    ),
                    verification_age,
                )
                if verification_record["checked_at"]
                else NOT_RECORDED_DISPLAY
            ),
            _terminal_resource_rows_display(entry, verification_record),
        ]
        return_lines.append("| " + " | ".join(_markdown_cell(str(cell)) for cell in cells) + " |")
    return return_lines
def render_markdown(
    manifest: dict[str, Any],
    blocker_registry: dict[str, Any] | None = None,
    verification_snapshot: dict[str, Any] | None = None,
) -> str:
    """Render stable Markdown from manifest entries and support metadata."""
    snapshot = verification_snapshot
    if snapshot is None:
        snapshot_path = ROOT / manifest["support_documentation"]["verification_snapshot"]
        snapshot = load_verification_snapshot(snapshot_path)
    support_by_entry = validate_manifest(manifest)
    blockers = validate_blocker_registry(blocker_registry if blocker_registry is not None else load_blocker_registry(DEFAULT_BLOCKER_REGISTRY))
    overlapping_ids = sorted({entry["entry_id"] for entry in manifest["entries"]} & {blocker["id"] for blocker in blockers})
    if overlapping_ids:
        raise SupportDocumentationError("blocker registry IDs overlap runnable manifest entries: " + ", ".join(overlapping_ids))
    markdown_lines = _support_document_header(manifest)
    markdown_lines.extend(_catalog_inventory_snapshot(manifest, blockers))
    markdown_lines.extend(render_inventory_summary(manifest, support_by_entry, blockers, _display))
    markdown_lines.extend(_configured_support_rows(manifest, support_by_entry))
    markdown_lines.extend(
        render_blocked_support_section(
            blockers,
            validate_freshness_policy(manifest)["source_review_max_age_days"],
            _display,
            _display_verification,
        )
    )
    markdown_lines.extend(_observed_verification_section(manifest, snapshot))
    markdown_lines.extend(["", "Generated by `scripts/generate_provider_directory_support_docs.py`; do not edit this file directly.", ""])
    return "\n".join(markdown_lines)
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse deterministic generation arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--blocker-registry", type=Path, default=None)
    parser.add_argument("--verification-snapshot", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true", help="Fail when the generated document is missing or stale.")
    parser.add_argument(
        "--as-of",
        type=dt.date.fromisoformat,
        default=None,
        help="Freshness date for --check (defaults to current UTC date).",
    )
    return parser.parse_args(argv)
def main(argv: list[str] | None = None) -> int:
    """Write the support matrix or check it for drift."""
    args = parse_args(argv)
    manifest = load_manifest(args.manifest)
    blocker_path = args.blocker_registry or ROOT / manifest["support_documentation"]["blocker_registry"]
    verification_path = args.verification_snapshot or ROOT / manifest["support_documentation"]["verification_snapshot"]
    rendered = render_markdown(
        manifest,
        load_blocker_registry(blocker_path),
        load_verification_snapshot(verification_path),
    )
    blockers = validate_blocker_registry(load_blocker_registry(blocker_path))
    current = args.output.read_text(encoding="utf-8") if args.output.exists() else None
    if args.check:
        if current != rendered:
            print(f"Provider Directory support documentation is stale: {args.output}")
            return 1
        validate_support_freshness(
            manifest,
            blockers,
            load_verification_snapshot(verification_path),
            args.as_of or dt.datetime.now(dt.UTC).date(),
        )
        print(f"Provider Directory support documentation is current: {args.output}")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"Wrote Provider Directory support documentation: {args.output}")
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
