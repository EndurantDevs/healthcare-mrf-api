# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure data contracts for dormant FHIR formulary acquisition."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any


USDF_STRUCTURE_DEFINITION_BASE = (
    "http://hl7.org/fhir/us/davinci-drug-formulary/StructureDefinition"
)
COVERAGE_PLAN_PROFILE_URI = f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-CoveragePlan"
FORMULARY_DRUG_PROFILE_URI = f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-FormularyDrug"
PLAN_ID_EXTENSION_URI = f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-PlanID-extension"
DRUG_TIER_EXTENSION_URI = f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-DrugTierID-extension"
PRIOR_AUTH_EXTENSION_URI = (
    f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-PriorAuthorization-extension"
)
STEP_THERAPY_EXTENSION_URI = (
    f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-StepTherapyLimit-extension"
)
QUANTITY_LIMIT_EXTENSION_URI = (
    f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-QuantityLimit-extension"
)
ALTERNATIVES_EXTENSION_URI = (
    f"{USDF_STRUCTURE_DEFINITION_BASE}/usdf-DrugAlternatives-extension"
)
RXNORM_SYSTEM_URI = "http://www.nlm.nih.gov/research/umls/rxnorm"
NDC_SYSTEM_URI = "http://hl7.org/fhir/sid/ndc"

SOURCE_RUNTIME_FIELDS = frozenset(
    {
        "timeout_seconds",
        "max_attempts",
        "page_size",
        "max_pages",
        "max_total_resources",
        "max_response_bytes",
    }
)
SOURCE_RUNTIME_BOUNDS = {
    "timeout_seconds": (1, 120),
    "max_attempts": (1, 3),
    "page_size": (1, 100),
    "max_pages": (1, 100_000),
    "max_total_resources": (1, 10_000_000),
    "max_response_bytes": (1_024, 20 * 1_024 * 1_024),
}


class FHIRSourceConfigurationError(ValueError):
    """Report a source configuration failure without echoing source values."""


def _bounded_integer(runtime_config: dict[str, Any], field_name: str) -> int:
    configured_value = runtime_config[field_name]
    if type(configured_value) is not int:
        raise FHIRSourceConfigurationError(
            "FHIR formulary runtime configuration types are invalid"
        )
    minimum_value, maximum_value = SOURCE_RUNTIME_BOUNDS[field_name]
    if not minimum_value <= configured_value <= maximum_value:
        raise FHIRSourceConfigurationError(
            "FHIR formulary runtime configuration bounds are invalid"
        )
    return configured_value


def _canonical_source_base(canonical_base: object) -> str:
    from process.formulary_fhir.identity import canonical_fhir_base

    try:
        return canonical_fhir_base(canonical_base)
    except ValueError:
        raise FHIRSourceConfigurationError(
            "FHIR base configuration is invalid"
        ) from None


@dataclass(frozen=True, slots=True, repr=False)
class FormularySourceConfig:
    """Validated settings from one explicitly enabled dormant source row."""

    canonical_base: str = field(repr=False)
    is_enabled: bool
    timeout_seconds: int
    max_attempts: int
    page_size: int
    max_pages: int
    max_total_resources: int
    max_response_bytes: int

    def __post_init__(self) -> None:
        if self.is_enabled is not True:
            raise FHIRSourceConfigurationError(
                "FHIR formulary source must be explicitly enabled"
            )
        _canonical_source_base(self.canonical_base)
        runtime_config_by_name = {
            field_name: getattr(self, field_name)
            for field_name in SOURCE_RUNTIME_FIELDS
        }
        settings_by_name = {
            field_name: _bounded_integer(runtime_config_by_name, field_name)
            for field_name in SOURCE_RUNTIME_FIELDS
        }
        if (
            settings_by_name["max_total_resources"]
            > settings_by_name["page_size"] * settings_by_name["max_pages"]
        ):
            raise FHIRSourceConfigurationError(
                "FHIR formulary page and total bounds are inconsistent"
            )

    def __repr__(self) -> str:
        return (
            "FormularySourceConfig(enabled=True, "
            f"page_size={self.page_size}, max_pages={self.max_pages}, "
            f"max_total_resources={self.max_total_resources})"
        )


def enabled_source_config(
    *,
    canonical_base: object,
    enabled: object,
    runtime_config_json: object,
) -> FormularySourceConfig:
    """Validate the three F1 source fields required by the pure client."""

    if enabled is not True:
        raise FHIRSourceConfigurationError(
            "FHIR formulary source must be explicitly enabled"
        )
    if type(runtime_config_json) is not dict:
        raise FHIRSourceConfigurationError(
            "FHIR formulary runtime configuration must be an exact object"
        )
    if set(runtime_config_json) != SOURCE_RUNTIME_FIELDS:
        raise FHIRSourceConfigurationError(
            "FHIR formulary runtime configuration fields are invalid"
        )
    normalized_base = _canonical_source_base(canonical_base)
    settings_by_name = {
        field_name: _bounded_integer(runtime_config_json, field_name)
        for field_name in SOURCE_RUNTIME_FIELDS
    }
    if (
        settings_by_name["max_total_resources"]
        > settings_by_name["page_size"] * settings_by_name["max_pages"]
    ):
        raise FHIRSourceConfigurationError(
            "FHIR formulary page and total bounds are inconsistent"
        )
    return FormularySourceConfig(
        canonical_base=normalized_base,
        is_enabled=True,
        **settings_by_name,
    )


@dataclass(frozen=True, slots=True)
class FHIRCoding:
    system: str
    code: str
    display: str | None
    version: str | None = None


@dataclass(frozen=True, slots=True)
class CoveragePlanRecord:
    upstream_list_id: str
    public_id: str
    canonical_identity: str
    upstream_version_id: str | None
    upstream_last_updated: dt.datetime
    status: str | None
    title: str | None
    name: str | None
    upstream_date: dt.datetime | None
    period_start: dt.datetime | None
    period_end: dt.datetime | None
    source_plan_identifiers: tuple[str, ...] = field(repr=False)
    raw_identifiers: tuple[dict[str, Any], ...] = field(repr=False)
    raw_extensions: tuple[dict[str, Any], ...] = field(repr=False)
    content_hash: str


@dataclass(frozen=True, slots=True)
class MedicationRecord:
    upstream_medication_id: str
    upstream_version_id: str | None
    upstream_last_updated: dt.datetime
    status: str | None
    drug_name: str | None
    rxnorm_id: str | None
    ndc11: str | None
    codings: tuple[FHIRCoding, ...]
    raw_extensions: tuple[dict[str, Any], ...] = field(repr=False)
    source_plan_identifiers: tuple[str, ...] = field(repr=False)
    drug_tier: str | None
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    alternative_references: tuple[str, ...] = field(repr=False)
    content_hash: str


@dataclass(frozen=True, slots=True, repr=False)
class MedicationPolicyFields:
    tier: str | None
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    alternative_references: tuple[str, ...] = field(repr=False)


@dataclass(frozen=True, slots=True, repr=False)
class AlternativeCorrection:
    prefix: str = field(repr=False)
    rule_version: str

    def __repr__(self) -> str:
        return f"AlternativeCorrection(rule_version={self.rule_version!r})"


@dataclass(frozen=True, slots=True, repr=False)
class AlternativeEvidence:
    raw_reference: str = field(repr=False)
    corrected_reference: str | None = field(repr=False)
    resolved_medication_id: str | None = field(repr=False)
    is_resolved: bool
    rule_version: str | None

    def __repr__(self) -> str:
        return (
            "AlternativeEvidence(reference=<redacted>, "
            f"resolved={self.is_resolved}, rule_version={self.rule_version!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class CurrentVersionCensus:
    """One bounded census of resource versions current at observation time."""

    resource_type: str
    cutoff_at: dt.datetime
    exact_total: int
    resources: tuple[dict[str, Any], ...] = field(repr=False)
    search_contract_hash: str = field(repr=False)

    def __repr__(self) -> str:
        return (
            "CurrentVersionCensus("
            f"resource_type={self.resource_type!r}, exact_total={self.exact_total})"
        )
