# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared value-free errors and archive-candidate shape for NPPES evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


_INVALID = "nppes_public_evidence_archive_invalid"


class NppesPublicEvidenceArchiveError(RuntimeError):
    """One value-free archive acquisition or traversal failure."""


def archive_error() -> NppesPublicEvidenceArchiveError:
    """Return a fresh public failure without source values or context."""

    return NppesPublicEvidenceArchiveError(_INVALID)


@dataclass(frozen=True, slots=True, repr=False)
class NppesArchiveCandidate:
    """One exact official monthly or weekly archive link."""

    source_url: str
    archive_name: str
    archive_kind: str
    period_start: date
    period_end: date | None

    def __repr__(self) -> str:
        return "<nppes-archive-candidate>"


__all__ = (
    "NppesArchiveCandidate",
    "NppesPublicEvidenceArchiveError",
    "archive_error",
)
