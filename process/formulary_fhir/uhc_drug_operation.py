# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed identities and default-off gates for UHC formulary operations."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import os
from pathlib import Path
import stat
from process.formulary_fhir.repository_shared import json_text
from process.formulary_fhir.repository_shared import stable_id
from process.formulary_fhir.repository_shared import strict_hash
from process.formulary_fhir.repository_shared import strict_text
from process.formulary_fhir.repository_shared import utc_timestamp
from process.formulary_fhir.uhc_drug_operation_evidence import (
    UHCDrugReceiptOperationEvidence,
)
from process.formulary_fhir.uhc_drug_operation_evidence import (
    receipt_operation_evidence,
)
from process.formulary_fhir.uhc_drug_operation_evidence import (
    receipt_operation_payload,
)
from process.formulary_fhir.uhc_source import UHC_FORMULARY_SOURCE_ID


ACQUISITION_ENABLED_ENV = "HLTHPRT_UHC_FORMULARY_ACQUISITION_ENABLED"
PUBLICATION_ENABLED_ENV = "HLTHPRT_UHC_FORMULARY_PUBLICATION_ENABLED"
WORK_DIRECTORY_ENV = "HLTHPRT_UHC_FORMULARY_WORK_DIRECTORY"
OPERATION_CONTRACT_VERSION = "uhc-formulary-operator-v1"

ERROR_MESSAGES = {
    "acquisition": "UHC formulary acquisition failed",
    "busy": "UHC formulary source is busy",
    "disabled": "UHC formulary operation is disabled",
    "evidence": "UHC formulary operation evidence is invalid",
    "gate_conflict": "UHC formulary operation gates conflict",
    "invalid_request": "UHC formulary operation request is invalid",
    "mismatch": "UHC formulary independent builds do not match",
    "missing": "UHC formulary admission receipt is missing",
    "publication": "UHC formulary publication failed",
}


class UHCDrugOperationError(RuntimeError):
    """Expose one fixed operator failure without source details."""

    def __init__(self, code: str) -> None:
        self.code = code if code in ERROR_MESSAGES else "evidence"
        super().__init__(ERROR_MESSAGES[self.code])


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugRunIdentities:
    """Bind both roots to one acquired observation and canonical cutoff."""

    baseline_run_id: str
    candidate_run_id: str
    cutoff_at: dt.datetime
    cutoff_text: str

    def __post_init__(self) -> None:
        strict_text(self.baseline_run_id, "baseline run id", 64)
        strict_text(self.candidate_run_id, "candidate run id", 64)
        if self.baseline_run_id == self.candidate_run_id:
            raise ValueError("UHC formulary run identities are invalid")
        normalized_cutoff = utc_timestamp(self.cutoff_at, "operation cutoff")
        if self.cutoff_text != normalized_cutoff.isoformat().replace(
            "+00:00",
            "Z",
        ):
            raise ValueError("UHC formulary run identities are invalid")

    def __repr__(self) -> str:
        return (
            "UHCDrugRunIdentities("
            f"cutoff_at={self.cutoff_at!r}, roots=<redacted>)"
        )


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugAdmissionOperationResult:
    """Expose bounded safe evidence from acquisition through receipt."""

    evidence: UHCDrugReceiptOperationEvidence
    downloaded_file_count: int
    reused_file_count: int
    downloaded_byte_count: int

    def __post_init__(self) -> None:
        counts = (
            self.downloaded_file_count,
            self.reused_file_count,
            self.downloaded_byte_count,
        )
        if not (
            type(self.evidence) is UHCDrugReceiptOperationEvidence
            and all(type(count) is int and count >= 0 for count in counts)
            and self.downloaded_file_count + self.reused_file_count
            == self.evidence.file_count
        ):
            raise ValueError("UHC formulary admission result is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class UHCDrugPublicationOperationResult:
    """Expose one receipt-qualified current-pointer result."""

    evidence: UHCDrugReceiptOperationEvidence
    generation: int
    published_at: dt.datetime

    def __post_init__(self) -> None:
        if (
            type(self.evidence) is not UHCDrugReceiptOperationEvidence
            or type(self.generation) is not int
            or self.generation <= 0
        ):
            raise ValueError("UHC formulary publication result is invalid")
        published_at = utc_timestamp(self.published_at, "publication timestamp")
        if published_at < self.evidence.admitted_at:
            raise ValueError("UHC formulary publication result is invalid")


def _is_enabled(variable_name: str) -> bool:
    return os.getenv(variable_name, "") == "true"


def _require_gate(operation: str) -> None:
    is_acquisition_enabled = _is_enabled(ACQUISITION_ENABLED_ENV)
    is_publication_enabled = _is_enabled(PUBLICATION_ENABLED_ENV)
    if is_acquisition_enabled and is_publication_enabled:
        raise UHCDrugOperationError("gate_conflict")
    if operation == "acquire":
        is_expected_enabled = is_acquisition_enabled
    elif operation == "publish":
        is_expected_enabled = is_publication_enabled
    else:
        raise UHCDrugOperationError("invalid_request")
    if not is_expected_enabled:
        raise UHCDrugOperationError("disabled")


def require_uhc_acquisition_gate() -> None:
    """Require the acquisition-only gate before any database or network use."""

    _require_gate("acquire")


def require_uhc_publication_gate() -> None:
    """Require the publication-only gate before any database use."""

    _require_gate("publish")


def uhc_drug_work_directory() -> Path:
    """Require one configured existing private directory for both spools."""

    raw_path = os.getenv(WORK_DIRECTORY_ENV)
    try:
        work_directory = Path(raw_path) if raw_path else None
        resolved_directory = (
            work_directory.resolve(strict=True) if work_directory else None
        )
        metadata = work_directory.lstat() if work_directory else None
    except (OSError, TypeError, ValueError):
        raise UHCDrugOperationError("invalid_request") from None
    if not (
        work_directory is not None
        and resolved_directory is not None
        and metadata is not None
        and work_directory.is_absolute()
        and work_directory == resolved_directory
        and stat.S_ISDIR(metadata.st_mode)
        and metadata.st_uid == os.geteuid()
        and stat.S_IMODE(metadata.st_mode) & 0o077 == 0
    ):
        raise UHCDrugOperationError("invalid_request")
    return work_directory


def uhc_drug_run_identities(
    source_observation_sha256: str,
    source_file_set_sha256: str,
    artifact_set_sha256: str,
    cutoff_at: dt.datetime,
) -> UHCDrugRunIdentities:
    """Derive stable opaque roots from exact downloaded evidence."""

    try:
        normalized_cutoff = utc_timestamp(cutoff_at, "operation cutoff")
        if normalized_cutoff > dt.datetime.now(dt.UTC):
            raise ValueError("future cutoff")
        cutoff_text = normalized_cutoff.isoformat().replace("+00:00", "Z")
        identity_parts = (
            OPERATION_CONTRACT_VERSION,
            strict_hash(source_observation_sha256, "source observation hash"),
            strict_hash(source_file_set_sha256, "source file set hash"),
            strict_hash(artifact_set_sha256, "artifact set hash"),
            cutoff_text,
        )
        return UHCDrugRunIdentities(
            stable_id("ffua_", UHC_FORMULARY_SOURCE_ID, *identity_parts),
            stable_id("ffub_", UHC_FORMULARY_SOURCE_ID, *identity_parts),
            normalized_cutoff,
            cutoff_text,
        )
    except (TypeError, ValueError):
        raise UHCDrugOperationError("invalid_request") from None


def uhc_operation_error(
    error: BaseException,
    default_code: str,
) -> UHCDrugOperationError:
    """Normalize one internal failure to a fixed public operator code."""

    error_code = getattr(error, "code", default_code)
    if error_code not in {"busy", "invalid_request", "mismatch", "missing"}:
        error_code = default_code
    return UHCDrugOperationError(error_code)


def admission_result_json(
    operation_result: UHCDrugAdmissionOperationResult,
) -> str:
    """Serialize safe aggregate acquisition and admission evidence."""

    if type(operation_result) is not UHCDrugAdmissionOperationResult:
        raise UHCDrugOperationError("evidence")
    try:
        operation_result.__post_init__()
        payload_by_field = receipt_operation_payload(
            operation_result.evidence
        )
    except (TypeError, ValueError):
        raise UHCDrugOperationError("evidence") from None
    payload_by_field.update(
        {
            "downloaded_byte_count": operation_result.downloaded_byte_count,
            "downloaded_file_count": operation_result.downloaded_file_count,
            "reused_file_count": operation_result.reused_file_count,
            "status": "admitted",
        }
    )
    return json_text(payload_by_field)


def publication_result_json(
    operation_result: UHCDrugPublicationOperationResult,
) -> str:
    """Serialize one safe current-pointer result."""

    if type(operation_result) is not UHCDrugPublicationOperationResult:
        raise UHCDrugOperationError("evidence")
    try:
        operation_result.__post_init__()
        payload_by_field = receipt_operation_payload(
            operation_result.evidence
        )
    except (TypeError, ValueError):
        raise UHCDrugOperationError("evidence") from None
    payload_by_field.update(
        {
            "generation": operation_result.generation,
            "published_at": operation_result.published_at.isoformat().replace(
                "+00:00",
                "Z",
            ),
            "status": "published",
        }
    )
    return json_text(payload_by_field)


__all__ = (
    "ACQUISITION_ENABLED_ENV",
    "OPERATION_CONTRACT_VERSION",
    "PUBLICATION_ENABLED_ENV",
    "UHCDrugAdmissionOperationResult",
    "UHCDrugOperationError",
    "UHCDrugPublicationOperationResult",
    "UHCDrugReceiptOperationEvidence",
    "UHCDrugRunIdentities",
    "WORK_DIRECTORY_ENV",
    "admission_result_json",
    "publication_result_json",
    "receipt_operation_evidence",
    "require_uhc_acquisition_gate",
    "require_uhc_publication_gate",
    "uhc_drug_run_identities",
    "uhc_drug_work_directory",
    "uhc_operation_error",
)
