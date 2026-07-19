#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline Provider Directory endpoint acquisition research harness."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import tempfile
import urllib.parse
from pathlib import Path
from typing import Any

try:
    from scripts.research.provider_directory_endpoint_acquisition_support import (
        acquisition_metric_errors,
        bulk_acquisition_metric_errors,
        external_result_errors,
    )
except ModuleNotFoundError:
    from provider_directory_endpoint_acquisition_support import (
        acquisition_metric_errors,
        bulk_acquisition_metric_errors,
        external_result_errors,
    )


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
SOURCE_ID_PATTERN = re.compile(r"^pdfhir_[0-9a-f]{24}$")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
RESOURCE_PROFILES = {
    "G4": [
        "Practitioner",
        "Organization",
        "Location",
        "PractitionerRole",
    ],
    "R8": [
        "InsurancePlan",
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
        "Endpoint",
    ],
    "M5": [
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ],
    "H5": [
        "InsurancePlan",
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    ],
    "I6": [
        "HealthcareService",
        "InsurancePlan",
        "Location",
        "Organization",
        "Practitioner",
        "PractitionerRole",
    ],
    "A6": [
        "InsurancePlan",
        "Location",
        "Organization",
        "OrganizationAffiliation",
        "Practitioner",
        "PractitionerRole",
    ],
    "A7": [
        "InsurancePlan",
        "PractitionerRole",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "OrganizationAffiliation",
    ],
    "NONE": [],
}
ACQUISITION_CLASSIFICATIONS = {"acquisition", "bulk_acquisition"}
SUPPORTED_CLASSIFICATIONS = ACQUISITION_CLASSIFICATIONS | {
    "probe_only",
    "external",
}
PROBE_OMITTED_KEYS = {
    "resources",
    "resource_limit",
    "resource_deadline_seconds",
    "linked_resource_limit",
    "linked_resource_deadline_seconds",
    "page_limit",
    "page_count",
    "stream_batch_size",
    "bulk_export",
    "source_concurrency",
}
PUBLICATION_FLAGS = (
    "stale_cleanup",
    "publish_artifacts",
    "publish_after_acquisition",
    "publish_corroboration",
)


class ManifestError(ValueError):
    """Raised when local research input fails closed."""


def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json_hash(json_value: Any) -> str:
    encoded = json.dumps(json_value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _read_json(json_path: Path) -> dict[str, Any]:
    decoded = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ManifestError(f"{json_path} must contain a JSON object")
    return decoded


def _reject_sensitive_content(json_value: Any, location: str = "manifest") -> None:
    if isinstance(json_value, dict):
        for field_name, field_value in json_value.items():
            normalized_name = str(field_name).lower().replace("-", "_")
            sensitive_fragments = (
                "token",
                "secret",
                "password",
                "authorization",
                "api_key",
                "credential",
            )
            if any(fragment in normalized_name for fragment in sensitive_fragments):
                raise ManifestError(
                    f"{location}.{field_name} may not contain credentials"
                )
            _reject_sensitive_content(field_value, f"{location}.{field_name}")
    elif isinstance(json_value, list):
        for position, member in enumerate(json_value):
            _reject_sensitive_content(member, f"{location}[{position}]")
    elif isinstance(json_value, str) and "..." in json_value:
        raise ManifestError(f"{location} may not contain abbreviated values")


def write_json(json_path: Path, json_value: dict[str, Any]) -> None:
    """Atomically write a credential-safe local artifact."""

    _reject_sensitive_content(json_value, str(json_path))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=json_path.parent,
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(json_value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, json_path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _credential_free_https_url(value: Any, label: str) -> str:
    normalized = str(value or "").rstrip("/")
    parsed = urllib.parse.urlsplit(normalized)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username
        or parsed.password
    ):
        raise ManifestError(f"{label} must be a credential-free HTTPS URL")
    return normalized


def _validate_manifest_header(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != 1:
        raise ManifestError("schema_version must be 1")
    if not SLUG_PATTERN.fullmatch(str(manifest.get("campaign_id") or "")):
        raise ManifestError("campaign_id must be a lowercase slug")
    if manifest.get("importer") != "provider-directory-fhir":
        raise ManifestError("manifest importer must target provider-directory-fhir")
    if not SLUG_PATTERN.fullmatch(str(manifest.get("engine") or "")):
        raise ManifestError("manifest engine must be a lowercase slug")
    _credential_free_https_url(manifest.get("retest_results_url"), "retest_results_url")
    parameter_profiles = manifest.get("parameter_profiles")
    if not isinstance(parameter_profiles, dict):
        raise ManifestError("parameter_profiles must be an object")
    for classification in (
        "acquisition",
        "bulk_acquisition",
        "probe_only",
    ):
        if not isinstance(parameter_profiles.get(classification), dict):
            raise ManifestError(
                f"parameter_profiles.{classification} must be an object"
            )


def _validate_manifest_entry(
    entry: dict[str, Any], seen_by_kind: dict[str, set[str]]
) -> None:
    entry_id = str(entry.get("entry_id") or "")
    owner_id = str(entry.get("owner_id") or "")
    source_ids = entry.get("source_ids")
    canonical_base = _credential_free_https_url(
        entry.get("canonical_base"), f"{entry_id or '<entry>'}.canonical_base"
    )
    classification = str(entry.get("classification") or "")
    if not SLUG_PATTERN.fullmatch(entry_id) or not SLUG_PATTERN.fullmatch(owner_id):
        raise ManifestError(f"entry_id/owner_id must be lowercase slugs: {entry_id!r}")
    if (
        not isinstance(source_ids, list)
        or not source_ids
        or not all(
            SOURCE_ID_PATTERN.fullmatch(str(source_id)) for source_id in source_ids
        )
    ):
        raise ManifestError(f"{entry_id}: source_ids must contain full pdfhir IDs")
    if classification not in SUPPORTED_CLASSIFICATIONS:
        raise ManifestError(f"{entry_id}: unsupported classification")
    expected_resources = RESOURCE_PROFILES.get(str(entry.get("resource_profile") or ""))
    if expected_resources is None or entry.get("resources") != expected_resources:
        raise ManifestError(
            f"{entry_id}: resources do not exactly match resource_profile"
        )
    if classification not in ACQUISITION_CLASSIFICATIONS and entry.get("resources"):
        raise ManifestError(f"{entry_id}: only acquisition entries may list resources")
    unique_values_by_kind = {
        "entry": [entry_id],
        "owner": [owner_id],
        "base": [canonical_base],
        "source": list(source_ids),
    }
    for kind_name, identifier_list in unique_values_by_kind.items():
        if seen_by_kind[kind_name].intersection(identifier_list):
            raise ManifestError(f"{entry_id}: duplicate {kind_name} identifier")
        seen_by_kind[kind_name].update(identifier_list)


def load_manifest(manifest_path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    """Load and validate a credential-free local acquisition manifest."""

    manifest = _read_json(manifest_path)
    _reject_sensitive_content(manifest)
    _validate_manifest_header(manifest)
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ManifestError("entries must be a non-empty list")
    seen_by_kind = {
        kind_name: set() for kind_name in ("entry", "owner", "base", "source")
    }
    for entry in entries:
        if not isinstance(entry, dict):
            raise ManifestError("every entry must be an object")
        _validate_manifest_entry(entry, seen_by_kind)
    return manifest


def entry_params(manifest: dict[str, Any], entry: dict[str, Any]) -> dict[str, Any]:
    """Build exact importer parameters for one manifest entry."""

    classification = str(entry["classification"])
    if classification == "external":
        return {}
    params_by_name = dict(manifest["parameter_profiles"][classification])
    params_by_name["retest_results_url"] = manifest["retest_results_url"]
    params_by_name["source_ids"] = list(entry["source_ids"])
    if classification in ACQUISITION_CLASSIFICATIONS:
        params_by_name["resources"] = ",".join(entry["resources"])
    return params_by_name


def _entry_fingerprint(manifest: dict[str, Any], entry: dict[str, Any]) -> str:
    return _json_hash({"entry": entry, "params": entry_params(manifest, entry)})


def _selected_entries(
    manifest: dict[str, Any], selected_entry_ids: frozenset[str]
) -> list[dict[str, Any]]:
    entries_by_id = {str(entry["entry_id"]): entry for entry in manifest["entries"]}
    unknown_entry_ids = selected_entry_ids - entries_by_id.keys()
    if unknown_entry_ids:
        raise ManifestError(
            "unknown manifest entries: " + ",".join(sorted(unknown_entry_ids))
        )
    if not selected_entry_ids:
        return list(manifest["entries"])
    return [
        entry
        for entry in manifest["entries"]
        if entry["entry_id"] in selected_entry_ids
    ]


def build_operator_plan(
    manifest: dict[str, Any],
    selected_entry_ids: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Build deterministic local inputs for an operator-selected importer."""

    entries = _selected_entries(manifest, selected_entry_ids)
    return {
        "schema_version": 1,
        "campaign_id": manifest["campaign_id"],
        "manifest_sha256": _json_hash(manifest),
        "importer": manifest["importer"],
        "engine": manifest["engine"],
        "entries": [
            {
                "entry_id": entry["entry_id"],
                "owner_id": entry["owner_id"],
                "canonical_base": entry["canonical_base"],
                "classification": entry["classification"],
                "spec_sha256": _entry_fingerprint(manifest, entry),
                "params": entry_params(manifest, entry),
            }
            for entry in entries
        ],
    }


def _result_param_errors(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    result: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    if result.get("importer") != manifest["importer"]:
        errors.append("result importer does not match the manifest")
    actual_params = result.get("params")
    actual_params = actual_params if isinstance(actual_params, dict) else {}
    for param_name, expected_value in entry_params(manifest, entry).items():
        if actual_params.get(param_name) != expected_value:
            errors.append(f"params.{param_name} does not match the manifest")
    if entry["classification"] == "probe_only":
        forbidden_keys = sorted(PROBE_OMITTED_KEYS.intersection(actual_params))
        if forbidden_keys:
            errors.append(
                "probe-only result contains resource/pagination params: "
                + ",".join(forbidden_keys)
            )
    return errors


def terminal_metric_errors(
    manifest: dict[str, Any],
    entry: dict[str, Any],
    operator_result_by_field: dict[str, Any],
) -> list[str]:
    """Return completeness failures for one local operator result."""

    classification = str(entry["classification"])
    if classification == "external":
        errors = external_result_errors(
            entry, operator_result_by_field,
            expected_importer=str(manifest["importer"]),
        )
    else:
        errors = _result_param_errors(manifest, entry, operator_result_by_field)
    metrics = operator_result_by_field.get("metrics")
    metrics = metrics if isinstance(metrics, dict) else {}
    for flag_name in PUBLICATION_FLAGS:
        if metrics.get(flag_name) is not False:
            errors.append(f"metrics.{flag_name} must be false")
    if metrics.get("pagination_resume_required"):
        errors.append("pagination resume is still required")
    if classification in ACQUISITION_CLASSIFICATIONS:
        errors.extend(acquisition_metric_errors(entry, metrics))
        if classification == "bulk_acquisition":
            errors.extend(bulk_acquisition_metric_errors(entry, metrics))
    elif classification == "probe_only":
        if metrics.get("source_ids") != entry["source_ids"] or metrics.get(
            "sources_probed"
        ) != len(entry["source_ids"]):
            errors.append("probe did not inspect the exact source")
        if metrics.get("resource_rows") not in ({}, None) or metrics.get(
            "source_import_sources_selected", 0
        ):
            errors.append("probe-only result imported resources")
    return errors


def load_operator_input(input_path: Path) -> dict[str, Any]:
    """Load credential-free result records supplied by a local operator."""

    operator_input = _read_json(input_path)
    _reject_sensitive_content(operator_input, str(input_path))
    if operator_input.get("schema_version") != 1:
        raise ManifestError("operator input schema_version must be 1")
    results = operator_input.get("results")
    if not isinstance(results, dict):
        raise ManifestError("operator input results must be an object")
    for entry_id, result in results.items():
        if not SLUG_PATTERN.fullmatch(str(entry_id)):
            raise ManifestError("operator result entry ids must be lowercase slugs")
        if not isinstance(result, dict):
            raise ManifestError(f"operator result for {entry_id!r} must be an object")
    return operator_input


def evaluate_operator_input(
    manifest: dict[str, Any],
    operator_input: dict[str, Any],
    selected_entry_ids: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Evaluate local result records against the selected manifest entries."""

    selected_entries = _selected_entries(manifest, selected_entry_ids)
    entries_by_id = {str(entry["entry_id"]): entry for entry in manifest["entries"]}
    operator_results_by_entry_id = operator_input.get("results")
    if not isinstance(operator_results_by_entry_id, dict):
        raise ManifestError("operator input results must be an object")
    unknown_result_ids = set(operator_results_by_entry_id) - entries_by_id.keys()
    if unknown_result_ids:
        raise ManifestError(
            "operator input contains unknown entries: "
            + ",".join(sorted(unknown_result_ids))
        )

    report_entry_by_id: dict[str, dict[str, Any]] = {}
    for entry in selected_entries:
        entry_id = str(entry["entry_id"])
        operator_result_by_field = operator_results_by_entry_id.get(entry_id)
        if not isinstance(operator_result_by_field, dict):
            report_entry_by_id[entry_id] = {
                "status": "missing",
                "errors": ["operator result is missing"],
            }
            continue
        errors = []
        if operator_result_by_field.get("status") != "succeeded":
            errors.append("operator result status must be succeeded")
        errors.extend(terminal_metric_errors(manifest, entry, operator_result_by_field))
        report_entry_by_id[entry_id] = {
            "status": "failed" if errors else "passed",
            "result_status": operator_result_by_field.get("status"),
            "errors": errors,
        }

    return {
        "schema_version": 1,
        "generated_at": _utc_now(),
        "campaign_id": manifest["campaign_id"],
        "manifest_sha256": _json_hash(manifest),
        "ok": all(
            report_entry["status"] == "passed"
            for report_entry in report_entry_by_id.values()
        ),
        "entries": report_entry_by_id,
    }


def run_cli(argv: list[str] | None = None) -> int:
    """Delegate command-line handling without loading operator input early."""

    try:
        from scripts.research.provider_directory_endpoint_acquisition_cli import (
            run_acquisition_cli,
        )
    except ModuleNotFoundError:
        from provider_directory_endpoint_acquisition_cli import (
            run_acquisition_cli,
        )

    return run_acquisition_cli(argv)


if __name__ == "__main__":
    raise SystemExit(run_cli())
