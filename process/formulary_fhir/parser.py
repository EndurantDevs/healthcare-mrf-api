# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure parsers for Da Vinci CoveragePlan and FormularyDrug resources."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from typing import Any

from process.formulary_fhir.identity import (
    canonical_list_identity,
    public_formulary_id,
)
from process.formulary_fhir.types import (
    AlternativeEvidence,
    CoveragePlanRecord,
    FHIRCoding,
    MedicationRecord,
)


RXNORM_SYSTEM_MARKERS = ("rxnorm", "rxnav")
PLAN_ID_EXTENSION_SUFFIX = "usdf-planid-extension"
DRUG_TIER_EXTENSION_SUFFIX = "usdf-drugtierid-extension"
PRIOR_AUTH_EXTENSION_SUFFIX = "usdf-priorauthorization-extension"
STEP_THERAPY_EXTENSION_SUFFIX = "usdf-steptherapylimit-extension"
QUANTITY_LIMIT_EXTENSION_SUFFIX = "usdf-quantitylimit-extension"
ALTERNATIVES_EXTENSION_SUFFIX = "usdf-drugalternatives-extension"
CA_ALTERNATIVE_RULE_VERSION = "kaiser-ca-mi-prefix-v1"


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _extensions(resource: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = resource.get("extension")
    return [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _extension_suffix(extension: Mapping[str, Any]) -> str:
    url = _clean(extension.get("url")) or ""
    return url.rstrip("/").rsplit("/", 1)[-1].lower()


def _extension_values(resource: Mapping[str, Any], suffix: str, field: str) -> list[Any]:
    return [
        extension[field]
        for extension in _extensions(resource)
        if _extension_suffix(extension) == suffix and field in extension
    ]


def _first_boolean(resource: Mapping[str, Any], suffix: str) -> bool | None:
    values = _extension_values(resource, suffix, "valueBoolean")
    return values[0] if values and isinstance(values[0], bool) else None


def _coding_rows(resource: Mapping[str, Any]) -> tuple[FHIRCoding, ...]:
    code = resource.get("code")
    raw_codings = code.get("coding") if isinstance(code, Mapping) else None
    if not isinstance(raw_codings, list):
        return ()
    rows: list[FHIRCoding] = []
    for raw in raw_codings:
        if not isinstance(raw, Mapping):
            continue
        rows.append(
            FHIRCoding(
                system=_clean(raw.get("system")),
                code=_clean(raw.get("code")),
                display=_clean(raw.get("display")),
                version=_clean(raw.get("version")),
            )
        )
    return tuple(rows)


def _unambiguous_code(codings: Iterable[FHIRCoding], predicate) -> str | None:
    values = {coding.code for coding in codings if coding.code and predicate(coding)}
    return next(iter(values)) if len(values) == 1 else None


def _rxnorm(codings: Iterable[FHIRCoding]) -> str | None:
    return _unambiguous_code(
        codings,
        lambda coding: any(
            marker in (coding.system or "").lower() for marker in RXNORM_SYSTEM_MARKERS
        )
        and bool(re.fullmatch(r"[0-9]+", coding.code or "")),
    )


def _ndc11(codings: Iterable[FHIRCoding]) -> str | None:
    candidates: set[str] = set()
    for coding in codings:
        if "ndc" not in (coding.system or "").lower() or not coding.code:
            continue
        digits = re.sub(r"[^0-9]", "", coding.code)
        if len(digits) == 11:
            candidates.add(digits)
    return next(iter(candidates)) if len(candidates) == 1 else None


def _plan_identifiers(
    resource: Mapping[str, Any],
    *,
    include_list_identifiers: bool = False,
) -> tuple[str, ...]:
    values = {
        value
        for value in (
            _clean(item)
            for item in _extension_values(resource, PLAN_ID_EXTENSION_SUFFIX, "valueString")
        )
        if value
    }
    if include_list_identifiers:
        for identifier in resource.get("identifier", []):
            if not isinstance(identifier, Mapping):
                continue
            value = _clean(identifier.get("value"))
            if value:
                values.add(value)
    return tuple(sorted(values))


def _tier(resource: Mapping[str, Any]) -> str | None:
    concepts = _extension_values(resource, DRUG_TIER_EXTENSION_SUFFIX, "valueCodeableConcept")
    for concept in concepts:
        if not isinstance(concept, Mapping):
            continue
        raw_codings = concept.get("coding")
        if not isinstance(raw_codings, list):
            continue
        for coding in raw_codings:
            if not isinstance(coding, Mapping):
                continue
            return _clean(coding.get("display")) or _clean(coding.get("code"))
    return None


def parse_coverage_plan(resource: Mapping[str, Any], *, canonical_base: str) -> CoveragePlanRecord:
    """Normalize one validated CoveragePlan List without losing raw evidence."""

    if resource.get("resourceType") != "List":
        raise ValueError("coverage plan resource must be a FHIR List")
    list_id = _clean(resource.get("id"))
    if not list_id:
        raise ValueError("coverage plan List is missing id")
    aliases = _plan_identifiers(resource, include_list_identifiers=True)
    if not aliases:
        raise ValueError("coverage plan List has no DrugPlan aliases")
    meta = resource.get("meta") if isinstance(resource.get("meta"), Mapping) else {}
    upstream_date = _clean(resource.get("date"))
    raw_identifiers = tuple(
        dict(identifier_data)
        for identifier_data in resource.get("identifier", [])
        if isinstance(identifier_data, Mapping)
    )
    raw_extensions = tuple(
        dict(extension_data) for extension_data in _extensions(resource)
    )
    normalized_by_field = {
        "id": list_id,
        "meta": meta,
        "status": resource.get("status"),
        "title": resource.get("title"),
        "name": resource.get("name"),
        "date": upstream_date,
        "identifiers": raw_identifiers,
        "extensions": raw_extensions,
        "source_plan_identifiers": aliases,
    }
    return CoveragePlanRecord(
        upstream_list_id=list_id,
        public_id=public_formulary_id(canonical_base, list_id),
        canonical_identity=canonical_list_identity(canonical_base, list_id),
        upstream_version_id=_clean(meta.get("versionId")),
        upstream_last_updated=_clean(meta.get("lastUpdated")),
        status=_clean(resource.get("status")),
        title=_clean(resource.get("title")),
        name=_clean(resource.get("name")),
        upstream_date=upstream_date,
        period_start=None,
        period_end=None,
        source_plan_identifiers=aliases,
        raw_identifiers=raw_identifiers,
        raw_extensions=raw_extensions,
        content_hash=_content_hash(normalized_by_field),
    )


def parse_medication_knowledge(resource: Mapping[str, Any]) -> MedicationRecord:
    """Normalize one FormularyDrug while preserving codings and extensions."""

    if resource.get("resourceType") != "MedicationKnowledge":
        raise ValueError("formulary drug must be MedicationKnowledge")
    medication_id = _clean(resource.get("id"))
    if not medication_id:
        raise ValueError("MedicationKnowledge is missing id")
    meta = resource.get("meta") if isinstance(resource.get("meta"), Mapping) else {}
    codings = _coding_rows(resource)
    raw_extensions = tuple(
        dict(extension_data) for extension_data in _extensions(resource)
    )
    alternatives: set[str] = set()
    for reference_data in _extension_values(
        resource,
        ALTERNATIVES_EXTENSION_SUFFIX,
        "valueReference",
    ):
        if isinstance(reference_data, Mapping):
            reference = _clean(reference_data.get("reference"))
            if reference:
                alternatives.add(reference)
    drug_name = None
    for coding in codings:
        if coding.display:
            drug_name = coding.display
            if coding.system and "rxnorm" in coding.system.lower():
                break
    normalized_by_field = {
        "id": medication_id,
        "meta": meta,
        "status": resource.get("status"),
        "codings": [coding.__dict__ for coding in codings],
        "extensions": raw_extensions,
        "source_plan_identifiers": _plan_identifiers(resource),
        "drug_tier": _tier(resource),
        "prior_authorization": _first_boolean(resource, PRIOR_AUTH_EXTENSION_SUFFIX),
        "step_therapy": _first_boolean(resource, STEP_THERAPY_EXTENSION_SUFFIX),
        "quantity_limit": _first_boolean(resource, QUANTITY_LIMIT_EXTENSION_SUFFIX),
        "alternative_references": sorted(alternatives),
    }
    return MedicationRecord(
        upstream_medication_id=medication_id,
        upstream_version_id=_clean(meta.get("versionId")),
        upstream_last_updated=_clean(meta.get("lastUpdated")),
        status=_clean(resource.get("status")),
        drug_name=drug_name,
        rxnorm_id=_rxnorm(codings),
        ndc11=_ndc11(codings),
        codings=codings,
        raw_extensions=raw_extensions,
        source_plan_identifiers=_plan_identifiers(resource),
        drug_tier=_tier(resource),
        prior_authorization=_first_boolean(resource, PRIOR_AUTH_EXTENSION_SUFFIX),
        step_therapy=_first_boolean(resource, STEP_THERAPY_EXTENSION_SUFFIX),
        quantity_limit=_first_boolean(resource, QUANTITY_LIMIT_EXTENSION_SUFFIX),
        alternative_references=tuple(sorted(alternatives)),
        content_hash=_content_hash(normalized_by_field),
    )


def _reference_medication_id(reference: str) -> str | None:
    match = re.fullmatch(r"MedicationKnowledge/([^/?#]+)", reference)
    return match.group(1) if match else None


def resolve_alternative_references(
    raw_references: Iterable[str],
    *,
    known_medication_ids: set[str],
    apply_california_rule: bool,
) -> tuple[AlternativeEvidence, ...]:
    """Resolve same-source references, preserving every raw and corrected value."""

    resolved_alternatives: list[AlternativeEvidence] = []
    for raw in sorted(
        {_clean(raw_reference) for raw_reference in raw_references} - {None}
    ):
        medication_id = _reference_medication_id(raw)
        if medication_id in known_medication_ids:
            resolved_alternatives.append(
                AlternativeEvidence(raw, None, medication_id, True, None)
            )
            continue
        corrected_reference = None
        corrected_id = None
        if apply_california_rule and medication_id and not medication_id.startswith("MI-"):
            candidate_id = f"MI-{medication_id}"
            if candidate_id in known_medication_ids:
                corrected_id = candidate_id
                corrected_reference = f"MedicationKnowledge/{candidate_id}"
        resolved_alternatives.append(
            AlternativeEvidence(
                raw_reference=raw,
                corrected_reference=corrected_reference,
                resolved_medication_id=corrected_id,
                resolved=corrected_id is not None,
                rule_version=(CA_ALTERNATIVE_RULE_VERSION if corrected_id else None),
            )
        )
    return tuple(resolved_alternatives)
