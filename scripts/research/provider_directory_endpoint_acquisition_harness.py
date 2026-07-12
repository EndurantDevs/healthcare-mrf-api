#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resumable serial Provider Directory endpoint acquisition through import-control."""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import re
import tempfile
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
try:
    from scripts.research.provider_directory_endpoint_acquisition_adoption import AdoptionManager, parse_adopt_runs
    from scripts.research.provider_directory_endpoint_acquisition_concurrency import campaign_state_lock, is_active_run_conflicting_with_entry, reject_polled_run_identity_change
    from scripts.research.provider_directory_endpoint_acquisition_reporting import _has_bounded_metrics, _run_summary
    from scripts.research.provider_directory_endpoint_acquisition_restart import ACTIVE_STATUSES, TERMINAL_STATUSES, HarnessConflict, archive_restart, archived_run_ids, clear_launch_lineage, fresh_root_generation, restart_lineage, validate_restart_run
    from scripts.research.provider_directory_endpoint_acquisition_support import ImportControlHttpClient, acquisition_metric_errors, bulk_acquisition_metric_errors, external_run_errors
except ModuleNotFoundError:
    from provider_directory_endpoint_acquisition_adoption import AdoptionManager, parse_adopt_runs
    from provider_directory_endpoint_acquisition_concurrency import campaign_state_lock, is_active_run_conflicting_with_entry, reject_polled_run_identity_change
    from provider_directory_endpoint_acquisition_reporting import _has_bounded_metrics, _run_summary
    from provider_directory_endpoint_acquisition_restart import ACTIVE_STATUSES, TERMINAL_STATUSES, HarnessConflict, archive_restart, archived_run_ids, clear_launch_lineage, fresh_root_generation, restart_lineage, validate_restart_run
    from provider_directory_endpoint_acquisition_support import ImportControlHttpClient, acquisition_metric_errors, bulk_acquisition_metric_errors, external_run_errors
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = ROOT / "specs/provider_directory_endpoint_acquisition_manifest.json"
DEFAULT_STATE = ROOT / "reports/provider-directory-endpoint-acquisition/state.json"
DEFAULT_REPORT = ROOT / "reports/provider-directory-endpoint-acquisition/report.json"
RETEST_RESULTS_URL = "https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/retest_results.json"
AUDITED_PROFILE_SHA256 = "cd616fcd2d0d81350b69d76cae088ffd0f4866a1501aacef1c4eae02114afe5e"
AUDITED_ENTRIES_SHA256 = "b5e91ba37cfeb16774455641cd434e5a090591c8804b5f0b0605bbe1cb9d00e2"
FINISHED_STATE_STATUSES = {"succeeded", "external_completed"}
VERIFICATION_TERMINAL_STATUSES = {"bounded", "canceled", "cancelled", "dead_letter", "external_completed", "external_incomplete", "external_validation_failed", "failed", "metric_validation_failed", "resume_required", "succeeded", "unknown_terminal"}
SOURCE_ID_PATTERN = re.compile(r"^pdfhir_[0-9a-f]{24}$")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
PAGINATION_ERROR = "provider_directory_pagination_resume_required"
RESOURCE_PROFILES = {
    "R8": ["InsurancePlan", "PractitionerRole", "Practitioner", "Organization", "Location", "HealthcareService", "OrganizationAffiliation", "Endpoint"],
    "M5": ["Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole"],
    "H5": ["InsurancePlan", "Location", "Organization", "Practitioner", "PractitionerRole"],
    "I6": ["HealthcareService", "InsurancePlan", "Location", "Organization", "Practitioner", "PractitionerRole"],
    "A6": ["InsurancePlan", "Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole"],
    "A7": ["InsurancePlan", "PractitionerRole", "Practitioner", "Organization", "Location", "HealthcareService", "OrganizationAffiliation"],
    "NONE": [],
}
ACQUISITION_CLASSIFICATIONS = {"acquisition", "bulk_acquisition"}
PROBE_OMITTED_KEYS = {
    "resources", "resource_limit", "resource_deadline_seconds", "linked_resource_limit",
    "linked_resource_deadline_seconds", "page_limit", "page_count", "stream_batch_size",
    "bulk_export", "source_concurrency",
}
VALID_CLASSIFICATION_MODES = {
    ("acquisition", "create"), ("acquisition", "attach"),
    ("bulk_acquisition", "create"), ("bulk_acquisition", "attach"),
    ("probe_only", "create"), ("probe_only", "attach"),
    ("external", "external_completed"),
}
class ManifestError(ValueError):
    """Raised when the audited manifest fails closed."""
@dataclass(frozen=True)
class HarnessConfig:
    state_path: Path
    report_path: Path
    apply: bool = False
    selected_entry_ids: frozenset[str] = frozenset()
    poll_interval_seconds: float = 30.0
    retry_wait_seconds: float = 900.0
    restart_entry_ids: frozenset[str] = frozenset()
    adopt_run_ids: tuple[tuple[str, str], ...] = ()
def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
def _json_hash(json_value: Any) -> str:
    return hashlib.sha256(json.dumps(json_value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
def _read_json(json_path: Path) -> dict[str, Any]:
    decoded = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise ManifestError(f"{json_path} must contain a JSON object")
    return decoded
def _atomic_write_json(json_path: Path, json_value: dict[str, Any]) -> None:
    _reject_sensitive_content(json_value, str(json_path))
    json_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=json_path.parent, delete=False) as handle:
            temporary_name = handle.name
            json.dump(json_value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, json_path)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
def _reject_sensitive_content(json_value: Any, location: str = "manifest") -> None:
    if isinstance(json_value, dict):
        for field_name, field_value in json_value.items():
            normalized_name = str(field_name).lower().replace("-", "_")
            if any(fragment in normalized_name for fragment in ("token", "secret", "password", "authorization", "api_key", "credential")):
                raise ManifestError(f"{location}.{field_name} may not contain credentials")
            _reject_sensitive_content(field_value, f"{location}.{field_name}")
    elif isinstance(json_value, list):
        for position, member in enumerate(json_value):
            _reject_sensitive_content(member, f"{location}[{position}]")
    elif isinstance(json_value, str) and "..." in json_value:
        raise ManifestError(f"{location} may not contain abbreviated values")
def _validate_manifest_header(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != 1:
        raise ManifestError("schema_version must be 1")
    if not SLUG_PATTERN.fullmatch(str(manifest.get("campaign_id") or "")):
        raise ManifestError("campaign_id must be a lowercase slug")
    if manifest.get("importer") != "provider-directory-fhir" or manifest.get("engine") != "healthcare-mrf-api":
        raise ManifestError("manifest importer/engine must target provider-directory-fhir")
    if manifest.get("retest_results_url") != RETEST_RESULTS_URL:
        raise ManifestError("retest_results_url drifted from the current GitHub retest catalog")
    if _json_hash(manifest.get("parameter_profiles")) != AUDITED_PROFILE_SHA256:
        raise ManifestError("parameter_profiles drifted from the audited safe values")
def _validate_manifest_entry(entry: dict[str, Any], seen_by_kind: dict[str, set[str]]) -> None:
    entry_id = str(entry.get("entry_id") or "")
    owner_id = str(entry.get("owner_id") or "")
    source_ids = entry.get("source_ids")
    canonical_base = str(entry.get("canonical_base") or "").rstrip("/")
    classification = str(entry.get("classification") or "")
    launch_mode = str(entry.get("launch_mode") or "")
    if not SLUG_PATTERN.fullmatch(entry_id) or not SLUG_PATTERN.fullmatch(owner_id):
        raise ManifestError(f"entry_id/owner_id must be lowercase slugs: {entry_id!r}")
    if not isinstance(source_ids, list) or not source_ids or not all(SOURCE_ID_PATTERN.fullmatch(str(source_id)) for source_id in source_ids):
        raise ManifestError(f"{entry_id}: source_ids must contain full pdfhir IDs")
    parsed_base = urllib.parse.urlsplit(canonical_base)
    if parsed_base.scheme != "https" or not parsed_base.netloc or parsed_base.username or parsed_base.password:
        raise ManifestError(f"{entry_id}: canonical_base must be a credential-free HTTPS URL")
    if (classification, launch_mode) not in VALID_CLASSIFICATION_MODES:
        raise ManifestError(f"{entry_id}: invalid classification/launch_mode")
    expected_resources = RESOURCE_PROFILES.get(str(entry.get("resource_profile") or ""))
    if expected_resources is None or entry.get("resources") != expected_resources:
        raise ManifestError(f"{entry_id}: resources do not exactly match resource_profile")
    if classification not in ACQUISITION_CLASSIFICATIONS and entry.get("resources"):
        raise ManifestError(f"{entry_id}: only acquisition entries may list resources")
    if launch_mode == "attach" and not str(entry.get("attached_run_id") or "").startswith("run_"):
        raise ManifestError(f"{entry_id}: attach mode requires a full attached_run_id")
    if launch_mode == "external_completed" and not str(entry.get("external_run_id") or "").startswith("run_"):
        raise ManifestError(f"{entry_id}: external_completed requires external_run_id")
    unique_values_by_kind = {"entry": [entry_id], "owner": [owner_id], "base": [canonical_base], "source": source_ids}
    for kind_name, identifier_list in unique_values_by_kind.items():
        if seen_by_kind[kind_name].intersection(identifier_list):
            raise ManifestError(f"{entry_id}: duplicate {kind_name} identifier")
        seen_by_kind[kind_name].update(identifier_list)
def load_manifest(manifest_path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    """Load and fail-closed validate the audited acquisition manifest."""
    manifest = _read_json(manifest_path)
    _reject_sensitive_content(manifest)
    _validate_manifest_header(manifest)
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ManifestError("entries must be a non-empty list")
    seen_by_kind = {kind_name: set() for kind_name in ("entry", "owner", "base", "source")}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ManifestError("every entry must be an object")
        _validate_manifest_entry(entry, seen_by_kind)
    if _json_hash(entries) != AUDITED_ENTRIES_SHA256:
        raise ManifestError("entries drifted from the audited endpoint matrix")
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
def _client_id(manifest: dict[str, Any], entry: dict[str, Any]) -> str:
    return f"{manifest['campaign_id']}:{entry['entry_id']}"
def create_run_payload(manifest: dict[str, Any], entry: dict[str, Any], fresh_root_generation: int = 0) -> dict[str, Any]:
    """Build a deterministic import-control request without credentials."""
    idempotency_seed = f"{manifest['campaign_id']}:{_entry_fingerprint(manifest, entry)}"
    if fresh_root_generation:
        idempotency_seed = f"{idempotency_seed}:fresh-root:{fresh_root_generation}"
    return {
        "importer": manifest["importer"], "engine": manifest["engine"],
        "params": entry_params(manifest, entry),
        "provider_directory_endpoint_scope": entry["canonical_base"],
        "idempotency_key": hashlib.sha256(idempotency_seed.encode("utf-8")).hexdigest()[:48],
        "client_id": _client_id(manifest, entry), "triggered_by": "endpoint_acquisition",
    }
def _load_state(state_path: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    state = _read_json(state_path) if state_path.exists() else {"schema_version": 1, "entries": {}}
    if state.get("schema_version") != 1 or not isinstance(state.get("entries"), dict):
        raise HarnessConflict("state file has an unsupported schema")
    state_entries = state["entries"]
    for entry in manifest["entries"]:
        fingerprint = _entry_fingerprint(manifest, entry)
        entry_state = state_entries.setdefault(entry["entry_id"], {"status": "pending", "run_ids": []})
        previous_fingerprint = entry_state.get("spec_sha256")
        has_started = bool(entry_state.get("root_run_id") or entry_state.get("current_run_id"))
        if previous_fingerprint and previous_fingerprint != fingerprint and (has_started or entry_state.get("status") in FINISHED_STATE_STATUSES):
            raise HarnessConflict(f"{entry['entry_id']}: manifest changed after execution started")
        entry_state["spec_sha256"] = fingerprint
    state["manifest_sha256"] = _json_hash(manifest)
    state["campaign_id"] = manifest["campaign_id"]
    return state
def _run_param_errors(manifest: dict[str, Any], entry: dict[str, Any], run_record: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if run_record.get("importer") != manifest["importer"]:
        errors.append("run importer does not match provider-directory-fhir")
    actual_params = run_record.get("params") if isinstance(run_record.get("params"), dict) else {}
    for param_name, expected_value in entry_params(manifest, entry).items():
        if actual_params.get(param_name) != expected_value:
            errors.append(f"params.{param_name} does not match the manifest")
    actual_endpoint_scope = str(actual_params.get("provider_directory_endpoint_scope") or "").rstrip("/")
    if actual_endpoint_scope and actual_endpoint_scope != str(entry["canonical_base"]).rstrip("/"):
        errors.append("params.provider_directory_endpoint_scope does not match the manifest")
    if entry["classification"] == "probe_only":
        forbidden_keys = sorted(PROBE_OMITTED_KEYS.intersection(actual_params))
        if forbidden_keys:
            errors.append(f"probe-only run contains resource/pagination params: {','.join(forbidden_keys)}")
    return errors


def terminal_metric_errors(manifest: dict[str, Any], entry: dict[str, Any], run_record: dict[str, Any]) -> list[str]:
    """Return terminal safety and completeness failures for one run."""
    errors = _run_param_errors(manifest, entry, run_record)
    metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    for flag_name in ("stale_cleanup", "publish_artifacts", "publish_after_acquisition", "publish_corroboration"):
        if metrics.get(flag_name) is not False:
            errors.append(f"metrics.{flag_name} must be false")
    if metrics.get("pagination_resume_required"):
        errors.append("pagination resume is still required")
    if entry["classification"] in ACQUISITION_CLASSIFICATIONS:
        errors.extend(acquisition_metric_errors(entry, metrics))
        if entry["classification"] == "bulk_acquisition":
            errors.extend(bulk_acquisition_metric_errors(entry, metrics))
    elif entry["classification"] == "probe_only":
        if metrics.get("source_ids") != entry["source_ids"] or metrics.get("sources_probed") != len(entry["source_ids"]):
            errors.append("probe did not inspect the exact source")
        if metrics.get("resource_rows") not in ({}, None) or metrics.get("source_import_sources_selected", 0):
            errors.append("probe-only run imported resources")
    return errors


def _is_resume_required(run_record: dict[str, Any]) -> bool:
    metrics = run_record.get("metrics") if isinstance(run_record.get("metrics"), dict) else {}
    if metrics.get("pagination_resume_required"):
        return True
    return PAGINATION_ERROR in json.dumps(run_record.get("error") or "", sort_keys=True).lower()
class AcquisitionHarness:
    def __init__(self, manifest: dict[str, Any], client: Any, config: HarnessConfig, sleeper: Callable[[float], None] = time.sleep):
        self.manifest = manifest
        self.client = client
        self.config = config
        self.sleeper = sleeper
        self.state = _load_state(config.state_path, manifest)
    def execute_campaign(self) -> dict[str, Any]:
        """Execute selected entries serially, stopping at the first apply failure."""
        with campaign_state_lock(self.config.state_path):
            return self._execute_locked_campaign()
    def _execute_locked_campaign(self) -> dict[str, Any]:
        """Execute while this harness exclusively owns its state/report pair."""
        AdoptionManager(
            self.manifest,
            self.state,
            self.client,
            lambda entry, run_record: _run_param_errors(self.manifest, entry, run_record),
            self._remember_run,
            lambda entry, run_record: is_active_run_conflicting_with_entry(
                entry,
                entry_params(self.manifest, entry),
                run_record,
            ),
        ).apply(self.config.adopt_run_ids, self.config.restart_entry_ids, self.config.apply)
        self._restart_requested_entries()
        self._persist()
        adopted_entry_ids = {entry_id for entry_id, _run_id in self.config.adopt_run_ids}
        selected_entry_ids = self.config.selected_entry_ids | self.config.restart_entry_ids | adopted_entry_ids
        for entry in sorted(self.manifest["entries"], key=lambda candidate: candidate["launch_mode"] == "create"):
            if selected_entry_ids and entry["entry_id"] not in selected_entry_ids:
                continue
            entry_state = self.state["entries"][entry["entry_id"]]
            if entry_state.get("status") in FINISHED_STATE_STATUSES:
                continue
            try:
                should_continue = self._should_continue_entry(entry, entry_state)
            except HarnessConflict as exc:
                entry_state.update({"status": "active_conflict", "message": str(exc)})
                should_continue = False
            self._persist()
            if self.config.apply and not should_continue:
                break
        return self.state
    def _restart_requested_entries(self) -> None:
        if not self.config.restart_entry_ids:
            return
        if not self.config.apply:
            raise HarnessConflict("--restart-entry requires --apply")
        entries_by_id = {entry["entry_id"]: entry for entry in self.manifest["entries"]}
        for entry_id in sorted(self.config.restart_entry_ids):
            self._restart_entry(entries_by_id[entry_id], self.state["entries"][entry_id])
    def _restart_entry(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> None:
        lineage = restart_lineage(entry, entry_state)
        run_record = self.client.get_run(lineage.current_run_id)
        metric_errors = terminal_metric_errors(self.manifest, entry, run_record)
        validate_restart_run(
            str(entry["entry_id"]),
            lineage,
            run_record,
            metric_errors,
            resume_required=_is_resume_required(run_record),
            has_retry_child=self._retry_child(run_record) is not None,
            has_bounded_metrics=_has_bounded_metrics(run_record),
        )
        archive_restart(entry_state, lineage, _run_summary(run_record), metric_errors, _utc_now())
        clear_launch_lineage(entry_state)
    def _should_continue_entry(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> bool:
        if entry["launch_mode"] == "external_completed":
            external_run = self.client.get_run(entry["external_run_id"])
            if external_run.get("status") != "succeeded":
                entry_state.update({"status": "external_incomplete", "last_run": _run_summary(external_run)})
                return False
            validation_errors = external_run_errors(entry, external_run)
            entry_state.update(
                {
                    "status": "external_validation_failed" if validation_errors else "external_completed",
                    "external_run_id": entry["external_run_id"],
                    "last_run": _run_summary(external_run),
                    "metric_errors": validation_errors,
                }
            )
            return not validation_errors
        if not self.config.apply:
            return self._should_continue_preview(entry, entry_state)
        return self._should_continue_apply(entry, entry_state)
    def _should_continue_preview(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> bool:
        run_record = self._resolve_starting_run(entry, entry_state)
        if run_record is None:
            entry_state.update({"status": "planned", "action": "create", "request": self._create_run_payload(entry, entry_state)})
        else:
            entry_state.update({"status": "planned", "action": "attach", "current_run_id": run_record["run_id"], "last_run": _run_summary(run_record)})
        return True
    def _should_continue_apply(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> bool:
        run_record = self._resolve_starting_run(entry, entry_state)
        if run_record is None:
            if entry["launch_mode"] == "attach":
                raise HarnessConflict(f"{entry['entry_id']}: attached run could not be resolved")
            request_body = self._create_run_payload(entry, entry_state)
            entry_state.update({"status": "launching", "action": "create", "request": request_body})
            self._persist()
            run_record = self.client.create_run(request_body)
        param_errors = _run_param_errors(self.manifest, entry, run_record)
        if param_errors:
            raise HarnessConflict(f"{entry['entry_id']}: " + "; ".join(param_errors))
        self._remember_run(entry_state, run_record)
        self._persist()
        return self._is_lineage_successful(entry, entry_state, run_record)
    def _create_run_payload(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> dict[str, Any]:
        return create_run_payload(self.manifest, entry, fresh_root_generation(entry_state))
    def _resolve_starting_run(self, entry: dict[str, Any], entry_state: dict[str, Any]) -> dict[str, Any] | None:
        attached_run_id = entry.get("attached_run_id")
        if attached_run_id: entry_state["root_run_id"] = attached_run_id
        known_run_id = attached_run_id or entry_state.get("current_run_id")
        if known_run_id: return self.client.get_run(str(known_run_id))
        run_list = self.client.list_runs()
        campaign_id = _client_id(self.manifest, entry)
        prior_run_ids = archived_run_ids(entry_state)
        campaign_runs = [
            run for run in run_list
            if str(run.get("run_id") or "") not in prior_run_ids
            and campaign_id in ((run.get("metrics") or {}).get("client_ids") or [])
            and not _run_param_errors(self.manifest, entry, run)
        ]
        if campaign_runs:
            roots = [run for run in campaign_runs if not (run.get("params") or {}).get("retry_of_run_id")]
            if len(roots) != 1:
                raise HarnessConflict(f"{entry['entry_id']}: campaign has {len(roots)} root runs")
            return self._latest_lineage_run(roots[0], run_list)
        active_runs = [run for run in run_list if run.get("status") in ACTIVE_STATUSES]
        matching_runs = [run for run in active_runs if not _run_param_errors(self.manifest, entry, run)]
        unrelated_runs = [run for run in active_runs if run not in matching_runs]
        blocking_runs = [
            run
            for run in unrelated_runs
            if is_active_run_conflicting_with_entry(
                entry,
                entry_params(self.manifest, entry),
                run,
            )
        ]
        if blocking_runs or len(matching_runs) > 1:
            conflict_ids = ",".join(
                str(run.get("run_id")) for run in [*matching_runs, *blocking_runs]
            )
            raise HarnessConflict(f"{entry['entry_id']}: active Provider Directory conflict: {conflict_ids}")
        return matching_runs[0] if matching_runs else None
    def _latest_lineage_run(self, root_run: dict[str, Any], run_list: list[dict[str, Any]]) -> dict[str, Any]:
        current_run = root_run
        while True:
            child_runs = [run for run in run_list if (run.get("params") or {}).get("retry_of_run_id") == current_run.get("run_id")]
            if len(child_runs) > 1:
                raise HarnessConflict(f"retry lineage branches after {current_run.get('run_id')}")
            if not child_runs:
                return current_run
            current_run = child_runs[0]
    def _is_lineage_successful(self, entry: dict[str, Any], entry_state: dict[str, Any], run_record: dict[str, Any]) -> bool:
        while True:
            run_record = self.client.get_run(str(run_record["run_id"]))
            reject_polled_run_identity_change(
                entry["entry_id"], _run_param_errors(self.manifest, entry, run_record)
            )
            self._remember_run(entry_state, run_record)
            self._persist()
            run_status = str(run_record.get("status") or "")
            if run_status in ACTIVE_STATUSES:
                self.sleeper(self.config.poll_interval_seconds)
                continue
            if run_status not in TERMINAL_STATUSES:
                entry_state.update({"status": "unknown_terminal", "message": f"unexpected run status {run_status!r}"})
                return False
            if _is_resume_required(run_record):
                entry_state["status"] = "resume_required"
                self._persist()
                child_run = self._wait_for_retry_child(run_record)
            else:
                child_run = self._retry_child(run_record)
            if child_run is not None:
                self._remember_run(entry_state, child_run)
                run_record = child_run
                continue
            return self._is_terminal_successful(entry, entry_state, run_record)
    def _retry_child(self, run_record: dict[str, Any]) -> dict[str, Any] | None:
        child_runs = [candidate for candidate in self.client.list_runs() if (candidate.get("params") or {}).get("retry_of_run_id") == run_record.get("run_id")]
        if len(child_runs) > 1:
            raise HarnessConflict(f"retry lineage branches after {run_record.get('run_id')}")
        return child_runs[0] if child_runs else None
    def _wait_for_retry_child(self, run_record: dict[str, Any]) -> dict[str, Any] | None:
        wait_started = time.monotonic()
        while True:
            child_run = self._retry_child(run_record)
            if child_run is not None:
                return child_run
            if time.monotonic() - wait_started >= self.config.retry_wait_seconds:
                return None
            self.sleeper(min(self.config.poll_interval_seconds, self.config.retry_wait_seconds))
    def _is_terminal_successful(self, entry: dict[str, Any], entry_state: dict[str, Any], run_record: dict[str, Any]) -> bool:
        run_status = str(run_record.get("status") or "")
        if run_status in {"canceled", "cancelled"}:
            entry_state["status"] = "canceled"
            return False
        if _is_resume_required(run_record):
            entry_state["status"] = "resume_required"
            return False
        if run_status != "succeeded":
            entry_state["status"] = "bounded" if _has_bounded_metrics(run_record) else "failed"
            return False
        metric_errors = terminal_metric_errors(self.manifest, entry, run_record)
        entry_state["metric_errors"] = metric_errors
        entry_state["status"] = "bounded" if metric_errors and _has_bounded_metrics(run_record) else ("metric_validation_failed" if metric_errors else "succeeded")
        return not metric_errors
    def _remember_run(self, entry_state: dict[str, Any], run_record: dict[str, Any]) -> None:
        run_id = str(run_record.get("run_id") or "")
        if not run_id:
            raise HarnessConflict("import-control run response omitted run_id")
        run_ids = entry_state.setdefault("run_ids", [])
        if run_id not in run_ids:
            run_ids.append(run_id)
        entry_state.setdefault("root_run_id", run_id)
        entry_state.update({"current_run_id": run_id, "status": str(run_record.get("status") or "unknown"), "last_run": _run_summary(run_record)})
    def _persist(self) -> None:
        self.state["updated_at"] = _utc_now()
        configured_ids = set(self.config.selected_entry_ids) | set(self.config.restart_entry_ids) | {entry_id for entry_id, _run_id in self.config.adopt_run_ids}
        selected_ids = sorted(configured_ids or {entry["entry_id"] for entry in self.manifest["entries"]})
        terminal_ids = sorted(entry_id for entry_id in selected_ids if self.state["entries"][entry_id].get("status") in VERIFICATION_TERMINAL_STATUSES)
        report_dict = {
            "schema_version": 1, "generated_at": self.state["updated_at"],
            "mode": "apply" if self.config.apply else "dry-run",
            "campaign_id": self.manifest["campaign_id"], "manifest_sha256": self.state["manifest_sha256"],
            "entries": self.state["entries"],
            "verification_update": {
                "eligible": len(terminal_ids) == len(selected_ids), "selected_entry_ids": selected_ids,
                "terminal_entry_ids": terminal_ids, "nonterminal_entry_ids": sorted(set(selected_ids) - set(terminal_ids)),
                "argv": ["python", "scripts/update_provider_directory_verification.py", "--report", str(self.config.report_path), "--environment", "<environment>"],
            },
        }
        _atomic_write_json(self.config.state_path, self.state)
        _atomic_write_json(self.config.report_path, report_dict)
def run_cli(argv: list[str] | None = None) -> int:
    """Delegate CLI execution without importing credentials at module load."""
    try:
        from scripts.research.provider_directory_endpoint_acquisition_cli import run_acquisition_cli
    except ModuleNotFoundError:
        from provider_directory_endpoint_acquisition_cli import run_acquisition_cli

    return run_acquisition_cli(argv)
if __name__ == "__main__":
    raise SystemExit(run_cli())
