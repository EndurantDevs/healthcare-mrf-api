# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary network rebuild progress shared by the workers of one attempt."""

import asyncio
import re
from contextvars import ContextVar
from dataclasses import dataclass

from process.live_progress import write_live_progress
from process.uhc_flex_practitioner_async_safety import drain_operation

NETWORK_PROGRESS = ContextVar("registry_network_import_progress", default=None)
STAGES = (
    "Preparing network rebuild",
    "Source inventory",
    "Catalog preparation",
    "FHIR graph capture",
    "Native source preparation",
    "Membership publication",
)
_FAILURES = {
    "fhir_source_worker_membership_header_bounds",
    "fhir_source_worker_membership_profile_unsupported",
    "fhir_source_worker_membership_publication_unavailable",
    "fhir_source_worker_membership_selection_changed",
    "fhir_source_worker_membership_source_changed",
    "registry_imported_preparation_source_changed",
    "registry_imported_catalog_preparation_changed",
    "registry_imported_preparation_graph_policy_changed",
    "registry_imported_publication_incomplete",
    "registry_publication_request_refused",
    "registry_source_connection_unavailable",
}
_DATABASE_FAILURES = {"23505", "23514", "40001", "40P01", "53100", "53400", "55P03", "57014"}


class NetworkImportFailure(RuntimeError):
    """Return only this attempt's bounded operator message and terminal progress."""

    def __init__(self, code, progress):
        super().__init__(progress["message"])
        self.control_error_code = code
        self.progress = progress


@dataclass
class NetworkImportProgress:
    stage: str = STAGES[0]
    total: int | None = None
    committed: int = 0
    reused: int = 0
    source_failure: tuple[str, tuple[str, str]] | None = None
    publication_step: str | None = None
    publication_unit: str = "phase"
    publication_done: int = 0
    publication_total: int | None = None

    def fields(self):
        """Report bounded work in the current step with overall completion unknown."""
        progress_by_name = {
            "phase": self.stage,
            "stage_id": self.stage.lower().replace(" ", "_"),
            "stage_ordinal": STAGES.index(self.stage) + 1,
            "message": self.stage,
            "unit": "phase",
            "done": 0,
            "basis": "semantic_work",
            "denominator_state": "unknown",
        }
        if self.total is not None:
            ready = self.committed + self.reused
            summary = f"{ready} of {self.total} FHIR graphs ready ({self.committed} committed, {self.reused} reused)"
            progress_by_name.update(
                detail=summary, counters={"graphs_committed": self.committed, "graphs_reused": self.reused}
            )
            if self.stage == "FHIR graph capture":
                # The dashboard's overall denominator stays unknown while this phase has a known graph count.
                progress_by_name.update(
                    unit="graph",
                    done=ready,
                    total=self.total,
                    basis="phase",
                    phase_pct=100 * ready / self.total if self.total else 0,
                    message=summary,
                )
        if self.publication_step is not None:
            progress_by_name.update(
                message=self.publication_step,
                unit=self.publication_unit,
                done=self.publication_done,
                stage_id="membership_publication_" + self.publication_step.lower().replace(" ", "_"),
            )
            if self.publication_total is not None:
                done, total = self.publication_done, self.publication_total
                progress_by_name.update(
                    total=total,
                    basis="phase",
                    phase_pct=100 * done / total if total else 100,
                    message=f"{self.publication_step} ({done} of {total})",
                )
        return progress_by_name

    async def emit(self):
        """A diagnostic write runs after the commit and cannot fail the import."""
        fields = self.fields()
        try:
            await drain_operation(
                asyncio.to_thread(write_live_progress, status="running", source="network-rebuild-progress", **fields),
                preserve_cancellation=True,
            )
        except Exception:
            return

    def source_failed(self, index, descriptor, error):
        """Remember the first failed edition without exposing free-form registration fields."""
        if self.source_failure is None:
            from process.registry_publication_worker import _failure_causes

            source_id = descriptor["coordinates"]["source_id"]
            source = f"source {index + 1}"
            if isinstance(source_id, str) and re.fullmatch(r"pdfhir_[0-9a-f]{24}", source_id):
                source = source_id
            self.source_failure = source, _operator_reason(_failure_causes(error))

    def failure(self, causes):
        """Keep source and cause paired even if two captures fail together."""
        source, (code, reason) = self.source_failure or ("", _operator_reason(causes))
        subject = f" for {source}" if source else ""
        progress_by_name = self.fields()
        summary = progress_by_name.pop("detail", None)
        label = self.publication_step or self.stage
        progress_by_name["message"] = ". ".join(filter(None, (f"{label} failed{subject}", reason, summary)))
        return NetworkImportFailure(code, progress_by_name)


def _operator_reason(causes):
    for cause in reversed(causes):
        if cause.get("sqlstate") in _DATABASE_FAILURES:
            return "registry_network_database_failed", "SQLSTATE " + cause["sqlstate"]
        if cause.get("code") in _FAILURES:
            return cause["code"], cause["code"]
    return "registry_network_rebuild_failed", ""


async def network_import_stage(stage):
    """Standalone catalog and exact queued workers retain their own reporting."""
    if (progress := NETWORK_PROGRESS.get()) is not None:
        progress.stage = stage
        await progress.emit()


async def network_publication_step(label, *, unit="phase", done=0, total=None):
    """Report milestones only for the ordinary rebuild publishing a generation."""
    progress = NETWORK_PROGRESS.get()
    if progress is not None and progress.stage == STAGES[-1]:
        progress.publication_step = label
        progress.publication_unit = unit
        progress.publication_done = done
        progress.publication_total = total
        await progress.emit()


async def network_source_mapped(*, batch=False):
    """Count completed mappings and coalesce status writes from small native sources."""
    progress = NETWORK_PROGRESS.get()
    if progress is not None and progress.publication_unit == "source":
        progress.publication_done += 1
        done = progress.publication_done
        if not batch or done % 10 == 0 or done == progress.publication_total:
            await progress.emit()
