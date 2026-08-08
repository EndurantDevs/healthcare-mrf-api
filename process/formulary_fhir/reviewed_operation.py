# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed identities and fail-closed gates for reviewed formulary operations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import os

from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.reviewed_source import reviewed_source_manifest


ACQUISITION_ENABLED_ENV = (
    "HLTHPRT_FHIR_FORMULARY_REVIEWED_ACQUISITION_ENABLED"
)
PUBLICATION_ENABLED_ENV = (
    "HLTHPRT_FHIR_FORMULARY_REVIEWED_PUBLICATION_ENABLED"
)
OPERATION_CONTRACT_VERSION = "reviewed-twin-v1"
ERROR_MESSAGES = {
    "acquisition": "FHIR formulary reviewed acquisition failed",
    "busy": "FHIR formulary reviewed source is busy",
    "disabled": "FHIR formulary reviewed operation is disabled",
    "evidence": "FHIR formulary reviewed operation evidence is invalid",
    "gate_conflict": "FHIR formulary reviewed operation gates conflict",
    "invalid_request": "FHIR formulary reviewed operation request is invalid",
    "mismatch": "FHIR formulary reviewed acquisitions do not match",
    "missing": "FHIR formulary reviewed admission is missing",
    "publication": "FHIR formulary reviewed publication failed",
}


class ReviewedOperationError(RuntimeError):
    """Expose one stable operation failure without source-specific detail."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "evidence"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True, repr=False)
class ReviewedRunIdentities:
    """Bind both internal acquisition roots to one canonical cutoff."""

    baseline_run_id: str
    candidate_run_id: str
    cutoff_at: dt.datetime
    cutoff_text: str

    def __repr__(self) -> str:
        return (
            "ReviewedRunIdentities("
            f"cutoff_at={self.cutoff_at!r}, roots=<redacted>)"
        )


def _is_enabled(variable_name: str) -> bool:
    return os.getenv(variable_name, "") == "true"


def _require_gate(operation: str) -> None:
    is_acquisition_enabled = _is_enabled(ACQUISITION_ENABLED_ENV)
    is_publication_enabled = _is_enabled(PUBLICATION_ENABLED_ENV)
    if is_acquisition_enabled and is_publication_enabled:
        raise ReviewedOperationError("gate_conflict")
    has_expected_gate = (
        is_acquisition_enabled
        if operation == "acquire"
        else is_publication_enabled
    )
    if operation not in {"acquire", "publish"}:
        raise ReviewedOperationError("invalid_request")
    if not has_expected_gate:
        raise ReviewedOperationError("disabled")


def require_acquisition_gate() -> None:
    """Require acquisition-only mode before any external activity."""

    _require_gate("acquire")


def require_publication_gate() -> None:
    """Require publication-only mode before any database activity."""

    _require_gate("publish")


def _cutoff_text(cutoff_at: dt.datetime) -> str:
    return cutoff_at.isoformat().replace("+00:00", "Z")


def reviewed_run_identities(cutoff: object) -> ReviewedRunIdentities:
    """Derive both opaque run identities from the fixed source and cutoff."""

    try:
        cutoff_at = utc_timestamp(cutoff, "reviewed operation cutoff")
        if cutoff_at > dt.datetime.now(dt.UTC):
            raise ValueError("future cutoff")
        cutoff_text = _cutoff_text(cutoff_at)
        source_id = reviewed_source_manifest().source_id
        baseline_run_id = stable_id(
            "ffra_",
            source_id,
            OPERATION_CONTRACT_VERSION,
            cutoff_text,
        )
        candidate_run_id = stable_id(
            "ffrb_",
            source_id,
            OPERATION_CONTRACT_VERSION,
            cutoff_text,
        )
        return ReviewedRunIdentities(
            baseline_run_id,
            candidate_run_id,
            cutoff_at,
            cutoff_text,
        )
    except (TypeError, ValueError):
        raise ReviewedOperationError("invalid_request") from None


__all__ = (
    "ACQUISITION_ENABLED_ENV",
    "PUBLICATION_ENABLED_ENV",
    "ReviewedOperationError",
    "ReviewedRunIdentities",
    "require_acquisition_gate",
    "require_publication_gate",
    "reviewed_run_identities",
)
