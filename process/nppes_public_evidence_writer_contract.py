# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Value-safe result and error types for NPPES archive admission."""

from __future__ import annotations

from dataclasses import dataclass


_INVALID = "nppes_public_evidence_writer_invalid"


class NppesPublicEvidenceWriterError(RuntimeError):
    """One value-free persistence or admission failure."""


def writer_error() -> NppesPublicEvidenceWriterError:
    """Return one fresh value-free writer failure."""

    return NppesPublicEvidenceWriterError(_INVALID)


@dataclass(frozen=True, slots=True, repr=False)
class NppesRegistryAdmissionReceipt:
    """Value-safe result of one exact admission or idempotent replay."""

    admission_ref: str
    source_release_ref: str
    artifact_sha256: str
    manifest_sha256: str
    source_record_count: int
    projected_record_count: int
    excluded_record_count: int
    write_state: str

    def __repr__(self) -> str:
        return "<nppes-registry-admission-receipt>"


__all__ = (
    "NppesPublicEvidenceWriterError",
    "NppesRegistryAdmissionReceipt",
    "writer_error",
)
