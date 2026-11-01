# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded operator telemetry; source admission and sealed accounting stay separate."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from time import monotonic

from process.control_cancel import ImportCancelledError
from process.live_progress import current_live_progress_context, enqueue_live_progress
from process.registry_fhir_catalog_extraction import DEFAULT_BYTES, fhir_catalog_json_bytes

_CURRENT = ContextVar("registry_fhir_catalog_run_report", default=None)
_COUNTERS = ("source_rows", "observation_rows", "networks", "created_networks", "relationships")
_ROW_RESERVE = 512  # Reserve five int64 counters and fixed terminal fields before candidate writes.


class FHIRCatalogRunReport:
    """Retain each inspected source outcome without copying native policy or payloads."""

    def __init__(self):
        self.document = {
            "contract_version": "fhir-catalog-run-report-v1",
            "inventory_scope": "configured_profiles_and_retained_bindings",
            "global_source_count": None,
            "global_network_count": None,
            "inventory_complete": False,
            "run_state": "running",
            "publication_state": "not_attempted",
            "phase": "discovery",
            "profiles": [],
            "sources": [],
        }
        self._reserved_bytes = len(fhir_catalog_json_bytes(self.document)) + _ROW_RESERVE
        self._rows_by_generation = {}
        self._observed_rows = 0
        self._last_observation_progress = 0.0
        self.audit = {"fhir_catalog_sync": self.document}

    def add_profile(self, profile):
        """Identify the registration inventory namespace without exposing its read policy."""
        scope_by_field = {
            "schema_name": profile.schema_name,
            "catalog_schema": profile.catalog_schema,
            "complete": False,
        }
        self._reserve(scope_by_field)
        self.document["profiles"].append(scope_by_field)
        return len(self.document["profiles"]) - 1

    def select_endpoints(self, endpoint_ids):
        """Complete selected inventory is distinct from a complete namespace inventory."""
        scope_by_field = {
            "inventory_scope": "selected_endpoints_and_retained_bindings",
            "selected_endpoint_ids": list(endpoint_ids),
        }
        self._reserve(scope_by_field)
        self.document.update(scope_by_field)

    def add_sources(self, dispositions, *, profile_index=None, generations=()):
        """Reserve complete source identities before any catalog candidate writes."""
        generations = iter(generations)
        for disposition in dispositions:
            coordinates = disposition.descriptor["coordinates"] if disposition.descriptor else {}
            generation = next(generations, None)
            outcome_by_field = {
                "inventory_kind": "registered" if profile_index is not None else "retained",
                "profile_index": profile_index,
                "endpoint_id": disposition.endpoint_id,
                "dataset_id": disposition.dataset_id,
                "source_id": coordinates.get("source_id"),
                "source_aliases": list(disposition.source_aliases),
                "admitted_source_aliases": list(disposition.descriptor.get("source_aliases", ()))
                if disposition.descriptor
                else [],
                "alias_count": disposition.alias_count,
                "aliases_complete": len(disposition.source_aliases) == disposition.alias_count,
                "source_generation": generation,
                "discovery_status": disposition.status,
                "reason": disposition.reason,
                "outcome": "not_started" if disposition.status == "accepted" else "unavailable",
                "count_scope": "source_organization_projection",
                "catalog_network_count": None,
                "counts": dict.fromkeys(_COUNTERS),
            }
            self._add_row(outcome_by_field)

    def _reserve(self, row):
        required = len(fhir_catalog_json_bytes(row)) + _ROW_RESERVE
        if self._reserved_bytes + required > DEFAULT_BYTES:
            self.document["report_error"] = "source_report_bounds"
            raise ValueError("registry_fhir_catalog_source_report_bounds")
        self._reserved_bytes += required

    def _add_row(self, row):
        self._reserve(row)
        self.document["sources"].append(row)
        if row["source_generation"] is not None:
            self._rows_by_generation.setdefault(row["source_generation"], []).append(row)

    def inventory_ready(self, current, previous):
        """Describe carried generations separately from freshly inspected source inventory."""
        for entry in previous["source_vector_json"] if previous else ():
            if entry["source_generation"] not in current:
                self._add_row(
                    {
                        "inventory_kind": "carried",
                        "profile_index": None,
                        "endpoint_id": None,
                        "dataset_id": None,
                        "source_id": entry["source_id"],
                        "source_aliases": [],
                        "admitted_source_aliases": [],
                        "alias_count": None,
                        "aliases_complete": False,
                        "source_generation": entry["source_generation"],
                        "discovery_status": "retained_last_good",
                        "reason": None,
                        "outcome": "carried",
                        "count_scope": "source_organization_projection",
                        "catalog_network_count": None,
                        "counts": dict.fromkeys(_COUNTERS),
                    }
                )
        self.document["inventory_complete"] = True
        self.progress("preparing")

    def source_started(self, generation):
        """Expose one source boundary rather than issuing progress per resource row."""
        for row in self._rows_by_generation.get(generation, ()):
            row["outcome"] = "preparing"
        self.progress("preparing")

    def source_prepared(self, generation, extraction, projection):
        """Keep unknown replay counts null and record only measured integral counters."""
        counts_by_kind = {**extraction, **projection}
        for row in self._rows_by_generation.get(generation, ()):
            for key in _COUNTERS:
                value = counts_by_kind.get(key)
                if value is not None and (type(value) is not int or not 0 <= value < 2**63):
                    raise ValueError("registry_fhir_catalog_source_report_counter_invalid")
                row["counts"][key] = value
            row["outcome"] = "prepared"
        self.progress("preparing")

    def observation_progress(self, dataset_id, rows):
        """Count inspected resource pages without treating them as admitted source rows."""
        self._observed_rows += rows
        now = monotonic()
        if rows and now - self._last_observation_progress < 2.0:
            return
        self._last_observation_progress = now
        self.progress(self.document["phase"], dataset_id=dataset_id)

    def progress(self, phase, *, dataset_id=None):
        """Use existing attempt-fenced dashboard progress with fixed aggregate fields."""
        self.document["phase"] = phase
        if not current_live_progress_context().get("run_id"):
            return
        source_outcomes = self.document["sources"]
        done = sum(source_outcome["outcome"] not in {"not_started", "preparing"} for source_outcome in source_outcomes)
        total = len(source_outcomes) if self.document["inventory_complete"] else None
        message = (
            f"FHIR catalog: {phase}; {len(source_outcomes)} sources inspected; {done} source outcomes recorded; "
            f"{self._observed_rows} resource rows scanned"
        )
        if dataset_id is not None:
            message += f"; observing {dataset_id}"
        # shortcut: delayed callbacks may show older diagnostic counts; add ordering if observed.
        enqueue_live_progress(
            importer="registry-fhir-catalog-sync",
            status="running",
            phase=phase,
            stage_id=phase,
            source="fhir-catalog-progress",
            unit="source",
            basis="semantic_work",
            denominator_state="known" if total is not None else "unknown",
            done=done,
            total=total,
            pct=100 * done / total if total else None,
            counters={"sources_inspected": len(source_outcomes), "resource_rows_scanned": self._observed_rows},
            file_name=dataset_id or "",
            message=message,
        )

    def publication_started(self):
        """A missing publication response must remain unknown rather than imply rollback."""
        self.document["publication_state"] = "in_progress"
        self.progress("publishing")

    def finish(self, state):
        """Mark published rows only after the existing publisher returns completion."""
        self.document["run_state"] = state
        if state in {"completed", "unchanged"}:
            self.document["publication_state"] = "committed" if state == "completed" else "unchanged"
            for row in self.document["sources"]:
                if row["outcome"] in {"not_started", "prepared"}:
                    row["outcome"] = "published" if state == "completed" else "unchanged"
        self.document["phase"] = "finished"

    def failed(self, error):
        """Preserve completed prefixes and fixed failure state without exception text."""
        self.document["run_state"] = (
            "canceled" if isinstance(error, (asyncio.CancelledError, ImportCancelledError)) else "failed"
        )
        if self.document["publication_state"] == "in_progress":
            self.document["publication_state"] = "unknown"
        for row in self.document["sources"]:
            if row["outcome"] == "preparing":
                row["outcome"] = self.document["run_state"]


def current_fhir_catalog_report():
    """Return optional run-local telemetry without admitting a source read."""
    return _CURRENT.get()


@contextmanager
def fhir_catalog_run_reporting(control_context=None):
    """Share one report with the normal control failure/cancellation metrics seam."""
    report = current_fhir_catalog_report() or FHIRCatalogRunReport()
    token = _CURRENT.set(report)
    if control_context is not None:
        report.audit = control_context.setdefault("audit", {})
        report.audit["fhir_catalog_sync"] = report.document
    try:
        yield report
    except BaseException as error:
        report.failed(error)
        raise
    finally:
        _CURRENT.reset(token)
