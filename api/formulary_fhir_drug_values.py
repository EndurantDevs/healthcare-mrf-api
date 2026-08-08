# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed public values for alias-scoped FHIR formulary drugs."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
import re

from api.formulary_fhir_catalog import FHIR_FORMULARY_ALIAS_ID_PATTERN
from api.formulary_fhir_serving import FHIR_FORMULARY_PUBLIC_ID_PATTERN
from api.formulary_fhir_serving import FHIRFormularyInvalidRequestError
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError
from api.formulary_fhir_serving import _optional_text
from api.formulary_fhir_serving import _required_timestamp
from api.formulary_fhir_serving import _timestamp_text


FHIR_FORMULARY_DRUG_ID_PATTERN = re.compile(r"^ffm_[0-9a-f]{48}$")
_RXNORM_PATTERN = re.compile(r"[0-9]{1,64}\Z")
_NDC11_PATTERN = re.compile(r"[0-9]{11}\Z")
_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,512}\Z")


@dataclass(frozen=True, slots=True)
class FHIRFormularyDrugFilters:
    """Normalized exact filters for one alias-scoped medication page."""

    rxnorm_id: str | None = None
    ndc11: str | None = None
    tier: str | None = None
    prior_authorization: bool | None = None
    step_therapy: bool | None = None
    quantity_limit: bool | None = None

    def __post_init__(self) -> None:
        if self.rxnorm_id is not None and (
            type(self.rxnorm_id) is not str
            or _RXNORM_PATTERN.fullmatch(self.rxnorm_id) is None
        ):
            raise FHIRFormularyInvalidRequestError(
                "FHIR formulary RxNorm filter is invalid"
            )
        if self.ndc11 is not None and (
            type(self.ndc11) is not str
            or _NDC11_PATTERN.fullmatch(self.ndc11) is None
        ):
            raise FHIRFormularyInvalidRequestError(
                "FHIR formulary NDC filter is invalid"
            )
        if self.tier is not None:
            try:
                _optional_text(self.tier, 256)
            except FHIRFormularyServingUnavailableError:
                raise FHIRFormularyInvalidRequestError(
                    "FHIR formulary tier filter is invalid"
                ) from None
        for flag in (
            self.prior_authorization,
            self.step_therapy,
            self.quantity_limit,
        ):
            if flag is not None and type(flag) is not bool:
                raise FHIRFormularyInvalidRequestError(
                    "FHIR formulary policy filter is invalid"
                )

    def scope_fields(self) -> dict[str, object]:
        """Return exact normalized fields for authenticated cursor scope."""

        return {
            "ndc11": self.ndc11,
            "prior_authorization": self.prior_authorization,
            "quantity_limit": self.quantity_limit,
            "rxnorm_id": self.rxnorm_id,
            "step_therapy": self.step_therapy,
            "tier": self.tier,
        }


@dataclass(frozen=True, slots=True)
class CurrentFHIRFormularyAliasContext:
    """Private current-alias ownership retained only inside serving code."""

    source_id: str
    dataset_id: str
    formulary_id: str
    alias_id: str
    alias_version_id: str
    generation: int
    published_at: dt.datetime


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyAlternatives:
    """Resolved public alternatives plus a count of unresolved references."""

    resolved_drug_ids: tuple[str, ...]
    unresolved_count: int


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyDrug:
    """One current alias membership with source-hidden medication identity."""

    formulary_id: str
    alias_id: str
    drug_id: str
    status: str | None
    name: str | None
    rxnorm_id: str | None
    ndc11: str | None
    last_updated: dt.datetime
    tier: str | None
    prior_authorization: bool | None
    step_therapy: bool | None
    quantity_limit: bool | None
    alternatives: PublicFHIRFormularyAlternatives


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyDrugPage:
    """One alias-scoped drug page and its opaque continuation."""

    items: tuple[PublicFHIRFormularyDrug, ...]
    next_cursor: str | None


def _optional_code(
    value: object,
    pattern: re.Pattern[str],
) -> str | None:
    if value is None:
        return None
    if type(value) is not str or pattern.fullmatch(value) is None:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary medication code evidence is invalid"
        )
    return value


def _optional_boolean(value: object) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary policy evidence is invalid"
        )
    return value


def validate_public_fhir_formulary_alternatives(
    alternatives: object,
) -> PublicFHIRFormularyAlternatives:
    """Require a bounded, sorted, unique alternative projection."""

    if type(alternatives) is not PublicFHIRFormularyAlternatives:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alternative evidence is invalid"
        )
    resolved_ids = alternatives.resolved_drug_ids
    if (
        type(resolved_ids) is not tuple
        or len(resolved_ids) > 100
        or any(
            type(drug_id) is not str
            or FHIR_FORMULARY_DRUG_ID_PATTERN.fullmatch(drug_id) is None
            for drug_id in resolved_ids
        )
        or type(alternatives.unresolved_count) is not int
        or not 0 <= alternatives.unresolved_count <= 100
        or len(resolved_ids) + alternatives.unresolved_count > 100
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alternative evidence is invalid"
        )
    if tuple(sorted(set(resolved_ids))) != resolved_ids:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alternative evidence is invalid"
        )
    return alternatives


def validate_public_fhir_formulary_drug(
    drug: object,
) -> PublicFHIRFormularyDrug:
    """Revalidate one public medication before response serialization."""

    if type(drug) is not PublicFHIRFormularyDrug:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary drug evidence is invalid"
        )
    if (
        type(drug.formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(drug.formulary_id) is None
        or type(drug.alias_id) is not str
        or FHIR_FORMULARY_ALIAS_ID_PATTERN.fullmatch(drug.alias_id) is None
        or type(drug.drug_id) is not str
        or FHIR_FORMULARY_DRUG_ID_PATTERN.fullmatch(drug.drug_id) is None
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary drug identity is invalid"
        )
    return PublicFHIRFormularyDrug(
        formulary_id=drug.formulary_id,
        alias_id=drug.alias_id,
        drug_id=drug.drug_id,
        status=_optional_text(drug.status, 32),
        name=_optional_text(drug.name, 2_048),
        rxnorm_id=_optional_code(drug.rxnorm_id, _RXNORM_PATTERN),
        ndc11=_optional_code(drug.ndc11, _NDC11_PATTERN),
        last_updated=_required_timestamp(drug.last_updated),
        tier=_optional_text(drug.tier, 256),
        prior_authorization=_optional_boolean(drug.prior_authorization),
        step_therapy=_optional_boolean(drug.step_therapy),
        quantity_limit=_optional_boolean(drug.quantity_limit),
        alternatives=validate_public_fhir_formulary_alternatives(
            drug.alternatives
        ),
    )


def public_fhir_formulary_drug_payload(
    drug: PublicFHIRFormularyDrug,
) -> dict[str, object]:
    """Shape one closed medication response without upstream identity."""

    validated_drug = validate_public_fhir_formulary_drug(drug)
    alternatives = validated_drug.alternatives
    return {
        "formulary_id": validated_drug.formulary_id,
        "alias_id": validated_drug.alias_id,
        "drug_id": validated_drug.drug_id,
        "status": validated_drug.status,
        "name": validated_drug.name,
        "rxnorm_id": validated_drug.rxnorm_id,
        "ndc11": validated_drug.ndc11,
        "last_updated": _timestamp_text(validated_drug.last_updated),
        "tier": validated_drug.tier,
        "prior_authorization": validated_drug.prior_authorization,
        "step_therapy": validated_drug.step_therapy,
        "quantity_limit": validated_drug.quantity_limit,
        "alternatives": {
            "resolved_drug_ids": list(alternatives.resolved_drug_ids),
            "unresolved_count": alternatives.unresolved_count,
        },
    }


def public_fhir_formulary_drug_page_payload(
    page: PublicFHIRFormularyDrugPage,
) -> dict[str, object]:
    """Shape one closed alias-scoped medication page."""

    if (
        type(page) is not PublicFHIRFormularyDrugPage
        or type(page.items) is not tuple
        or len(page.items) > 100
        or (
            page.next_cursor is not None
            and (
                type(page.next_cursor) is not str
                or _CURSOR_PATTERN.fullmatch(page.next_cursor) is None
            )
        )
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary drug page evidence is invalid"
        )
    drug_payloads = [
        public_fhir_formulary_drug_payload(drug) for drug in page.items
    ]
    drug_ids = [drug_payload["drug_id"] for drug_payload in drug_payloads]
    if drug_ids != sorted(set(drug_ids)):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary drug page evidence is invalid"
        )
    return {
        "items": drug_payloads,
        "next_cursor": page.next_cursor,
    }


__all__ = (
    "FHIR_FORMULARY_DRUG_ID_PATTERN",
    "CurrentFHIRFormularyAliasContext",
    "FHIRFormularyDrugFilters",
    "PublicFHIRFormularyAlternatives",
    "PublicFHIRFormularyDrug",
    "PublicFHIRFormularyDrugPage",
    "public_fhir_formulary_drug_page_payload",
    "public_fhir_formulary_drug_payload",
    "validate_public_fhir_formulary_drug",
)
