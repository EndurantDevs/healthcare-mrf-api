# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public FHIR formulary catalog payload contracts."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

from api.formulary_fhir_serving import FHIR_FORMULARY_PUBLIC_ID_PATTERN
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError
from api.formulary_fhir_serving import PublicFHIRFormularyCoverage
from api.formulary_fhir_serving import PublicFHIRFormularyDetail
from api.formulary_fhir_serving import public_fhir_formulary_coverage_payload
from api.formulary_fhir_serving import public_fhir_formulary_payload
from api.formulary_fhir_serving import validate_public_fhir_formulary_coverage


FHIR_FORMULARY_ALIAS_ID_PATTERN = re.compile(r"^ffa_[0-9a-f]{48}$")
MAX_FHIR_FORMULARY_PAGE_SIZE = 100
_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,512}\Z")


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyPage:
    """One current-formulary page and its opaque continuation."""

    items: tuple[PublicFHIRFormularyDetail, ...]
    next_cursor: str | None


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyAlias:
    """One opaque current DrugPlan alias without its upstream identifier."""

    formulary_id: str
    alias_id: str
    drug_count: int
    coverage: PublicFHIRFormularyCoverage | None = None


@dataclass(frozen=True, slots=True)
class PublicFHIRFormularyAliasPage:
    """One alias page and its opaque continuation."""

    items: tuple[PublicFHIRFormularyAlias, ...]
    next_cursor: str | None


def _alias_from_record(
    record: Mapping[str, Any],
    coverage: PublicFHIRFormularyCoverage | None = None,
) -> PublicFHIRFormularyAlias:
    formulary_id = record.get("formulary_id")
    alias_id = record.get("alias_id")
    drug_count = record.get("drug_count")
    if (
        type(formulary_id) is not str
        or FHIR_FORMULARY_PUBLIC_ID_PATTERN.fullmatch(formulary_id) is None
        or type(alias_id) is not str
        or FHIR_FORMULARY_ALIAS_ID_PATTERN.fullmatch(alias_id) is None
        or type(drug_count) is not int
        or drug_count < 0
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias evidence is invalid"
        )
    return PublicFHIRFormularyAlias(
        formulary_id=formulary_id,
        alias_id=alias_id,
        drug_count=drug_count,
        coverage=validate_public_fhir_formulary_coverage(coverage),
    )


def public_fhir_formulary_page_payload(
    page: PublicFHIRFormularyPage,
) -> dict[str, object]:
    """Shape one closed current-formulary collection response."""

    if (
        type(page) is not PublicFHIRFormularyPage
        or type(page.items) is not tuple
        or len(page.items) > MAX_FHIR_FORMULARY_PAGE_SIZE
        or (
            page.next_cursor is not None
            and (
                type(page.next_cursor) is not str
                or _CURSOR_PATTERN.fullmatch(page.next_cursor) is None
            )
        )
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary page evidence is invalid"
        )
    formulary_payloads = [
        public_fhir_formulary_payload(detail) for detail in page.items
    ]
    formulary_ids = [
        formulary_payload["formulary_id"]
        for formulary_payload in formulary_payloads
    ]
    if formulary_ids != sorted(set(formulary_ids)):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary page evidence is invalid"
        )
    return {
        "items": formulary_payloads,
        "next_cursor": page.next_cursor,
    }


def public_fhir_formulary_alias_page_payload(
    page: PublicFHIRFormularyAliasPage,
) -> dict[str, object]:
    """Shape one closed alias collection without upstream identifiers."""

    if (
        type(page) is not PublicFHIRFormularyAliasPage
        or type(page.items) is not tuple
        or len(page.items) > MAX_FHIR_FORMULARY_PAGE_SIZE
        or (
            page.next_cursor is not None
            and (
                type(page.next_cursor) is not str
                or _CURSOR_PATTERN.fullmatch(page.next_cursor) is None
            )
        )
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias page evidence is invalid"
        )
    validated_aliases = []
    for alias_detail in page.items:
        if type(alias_detail) is not PublicFHIRFormularyAlias:
            raise FHIRFormularyServingUnavailableError(
                "FHIR formulary alias page evidence is invalid"
            )
        validated_aliases.append(
            _alias_from_record(
                {
                    "formulary_id": alias_detail.formulary_id,
                    "alias_id": alias_detail.alias_id,
                    "drug_count": alias_detail.drug_count,
                },
                alias_detail.coverage,
            )
        )
    validated_aliases = tuple(validated_aliases)
    alias_ids = [alias_detail.alias_id for alias_detail in validated_aliases]
    if alias_ids != sorted(set(alias_ids)):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary alias page evidence is invalid"
        )
    return {
        "items": [
            {
                "formulary_id": alias_detail.formulary_id,
                "alias_id": alias_detail.alias_id,
                "drug_count": alias_detail.drug_count,
                "coverage": public_fhir_formulary_coverage_payload(
                    alias_detail.coverage
                ),
            }
            for alias_detail in validated_aliases
        ],
        "next_cursor": page.next_cursor,
    }


__all__ = (
    "FHIR_FORMULARY_ALIAS_ID_PATTERN",
    "MAX_FHIR_FORMULARY_PAGE_SIZE",
    "PublicFHIRFormularyAlias",
    "PublicFHIRFormularyAliasPage",
    "PublicFHIRFormularyPage",
    "public_fhir_formulary_alias_page_payload",
    "public_fhir_formulary_page_payload",
)
