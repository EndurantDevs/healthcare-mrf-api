# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FHIRCoding:
    system: str | None
    code: str | None
    display: str | None
    version: str | None = None


@dataclass(frozen=True)
class CoveragePlanRecord:
    upstream_list_id: str
    public_id: str
    canonical_identity: str
    upstream_version_id: str | None
    upstream_last_updated: str | None
    status: str | None
    title: str | None
    name: str | None
    upstream_date: str | None
    period_start: str | None
    period_end: str | None
    source_plan_identifiers: tuple[str, ...]
    raw_identifiers: tuple[dict[str, Any], ...]
    raw_extensions: tuple[dict[str, Any], ...]
    content_hash: str


@dataclass(frozen=True)
class MedicationRecord:
    upstream_medication_id: str
    upstream_version_id: str | None
    upstream_last_updated: str | None
    status: str | None
    drug_name: str | None
    rxnorm_id: str | None
    ndc11: str | None
    codings: tuple[FHIRCoding, ...]
    raw_extensions: tuple[dict[str, Any], ...]
    source_plan_identifiers: tuple[str, ...]
    drug_tier: str | None
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    alternative_references: tuple[str, ...]
    content_hash: str


@dataclass(frozen=True)
class AlternativeEvidence:
    raw_reference: str
    corrected_reference: str | None
    resolved_medication_id: str | None
    resolved: bool
    rule_version: str | None
