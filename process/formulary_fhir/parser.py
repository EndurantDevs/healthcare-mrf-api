# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict pure parsers for approved Da Vinci formulary resource fields."""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from typing import Any

from process.formulary_fhir.continuation import validated_alias
from process.formulary_fhir.identity import (
    ALTERNATIVE_REFERENCE_PATTERN,
    CODING_FIELDS,
    CORRECTION_PREFIX_PATTERN,
    canonical_list_identity,
    fhir_content_hash,
    fhir_json_snapshot,
    fhir_resource_metadata,
    optional_fhir_instant,
    preferred_coding_display,
    public_formulary_id,
    strict_fhir_resource,
    strict_fhir_text,
    validated_fhir_id,
)
from process.formulary_fhir.types import (
    ALTERNATIVES_EXTENSION_URI,
    DRUG_TIER_EXTENSION_URI,
    NDC_SYSTEM_URI,
    PLAN_ID_EXTENSION_URI,
    PRIOR_AUTH_EXTENSION_URI,
    QUANTITY_LIMIT_EXTENSION_URI,
    RXNORM_SYSTEM_URI,
    STEP_THERAPY_EXTENSION_URI,
    AlternativeCorrection,
    AlternativeEvidence,
    CoveragePlanRecord,
    FHIRCoding,
    MedicationPolicyFields,
    MedicationRecord,
)


def _extension_rows(resource: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_extensions = resource.get("extension", [])
    if type(raw_extensions) is not list:
        raise ValueError("FHIR extension field must be a list")
    extension_rows: list[dict[str, Any]] = []
    for raw_extension in raw_extensions:
        if type(raw_extension) is not dict:
            raise ValueError("FHIR extension entry must be an object")
        extension_snapshot = fhir_json_snapshot(raw_extension)
        strict_fhir_text(
            extension_snapshot.get("url"),
            "extension URL",
            maximum_length=2_048,
            is_required=True,
        )
        extension_rows.append(extension_snapshot)
    return tuple(extension_rows)


def _extension_values(
    extensions: tuple[dict[str, Any], ...],
    extension_uri: str,
    value_field: str,
) -> tuple[Any, ...]:
    matching_values: list[Any] = []
    for extension in extensions:
        if extension.get("url") != extension_uri:
            continue
        if set(extension) != {"url", value_field}:
            raise ValueError("approved FHIR extension fields are invalid")
        matching_values.append(extension[value_field])
    return tuple(matching_values)


def _plan_aliases(
    extensions: tuple[dict[str, Any], ...],
    *,
    is_required: bool,
) -> tuple[str, ...]:
    aliases = {
        validated_alias(raw_alias)
        for raw_alias in _extension_values(
            extensions,
            PLAN_ID_EXTENSION_URI,
            "valueString",
        )
    }
    if is_required and not aliases:
        raise ValueError("FHIR coverage plan has no approved plan alias")
    return tuple(sorted(aliases))


def _single_boolean_extension(
    extensions: tuple[dict[str, Any], ...],
    extension_uri: str,
) -> bool | None:
    boolean_values = _extension_values(
        extensions,
        extension_uri,
        "valueBoolean",
    )
    if len(boolean_values) > 1 or any(type(flag) is not bool for flag in boolean_values):
        raise ValueError("approved FHIR boolean extension is invalid")
    return boolean_values[0] if boolean_values else None


def _coding_rows(resource: dict[str, Any]) -> tuple[FHIRCoding, ...]:
    codeable_concept = resource.get("code")
    if type(codeable_concept) is not dict or not set(codeable_concept).issubset(
        {"coding", "text"}
    ):
        raise ValueError("FHIR MedicationKnowledge code is invalid")
    if "text" in codeable_concept:
        strict_fhir_text(codeable_concept["text"], "code.text", maximum_length=2_048)
    raw_codings = codeable_concept.get("coding")
    if type(raw_codings) is not list or not raw_codings:
        raise ValueError("FHIR MedicationKnowledge codings are required")
    return tuple(_coding_from_object(raw_coding) for raw_coding in raw_codings)


def _coding_from_object(raw_coding: object) -> FHIRCoding:
    if type(raw_coding) is not dict or not set(raw_coding).issubset(CODING_FIELDS):
        raise ValueError("FHIR coding fields are invalid")
    if "userSelected" in raw_coding and type(raw_coding["userSelected"]) is not bool:
        raise ValueError("FHIR coding userSelected primitive is invalid")
    system = strict_fhir_text(
        raw_coding.get("system"),
        "coding.system",
        maximum_length=2_048,
        is_required=True,
    )
    code = strict_fhir_text(
        raw_coding.get("code"),
        "coding.code",
        maximum_length=256,
        is_required=True,
    )
    assert system is not None and code is not None
    return FHIRCoding(
        system=system,
        code=code,
        display=strict_fhir_text(
            raw_coding.get("display"),
            "coding.display",
            maximum_length=2_048,
        ),
        version=strict_fhir_text(
            raw_coding.get("version"),
            "coding.version",
            maximum_length=256,
        ),
    )


def _unambiguous_code(
    codings: tuple[FHIRCoding, ...],
    *,
    system_uri: str,
    code_pattern: re.Pattern[str],
) -> str | None:
    matching_codes = {
        coding.code
        for coding in codings
        if coding.system == system_uri and code_pattern.fullmatch(coding.code)
    }
    return next(iter(matching_codes)) if len(matching_codes) == 1 else None


def _tier_value(extensions: tuple[dict[str, Any], ...]) -> str | None:
    tier_concepts = _extension_values(
        extensions,
        DRUG_TIER_EXTENSION_URI,
        "valueCodeableConcept",
    )
    if len(tier_concepts) > 1:
        raise ValueError("FHIR drug tier extension is ambiguous")
    if not tier_concepts:
        return None
    concept = tier_concepts[0]
    if type(concept) is not dict or set(concept) != {"coding"}:
        raise ValueError("FHIR drug tier concept is invalid")
    raw_codings = concept["coding"]
    if type(raw_codings) is not list or not raw_codings:
        raise ValueError("FHIR drug tier coding is invalid")
    tier_names = {_tier_coding_name(raw_coding) for raw_coding in raw_codings}
    if len(tier_names) != 1:
        raise ValueError("FHIR drug tier coding is ambiguous")
    return next(iter(tier_names))


def _tier_coding_name(raw_coding: object) -> str:
    if type(raw_coding) is not dict or not set(raw_coding).issubset(
        {"system", "code", "display"}
    ):
        raise ValueError("FHIR drug tier coding fields are invalid")
    if "system" in raw_coding:
        strict_fhir_text(raw_coding["system"], "drug tier system", maximum_length=2_048)
    display = strict_fhir_text(
        raw_coding.get("display"),
        "drug tier display",
        maximum_length=256,
    )
    code = strict_fhir_text(
        raw_coding.get("code"),
        "drug tier code",
        maximum_length=256,
    )
    if display is None and code is None:
        raise ValueError("FHIR drug tier coding has no value")
    return display or code or ""


def _alternative_references(
    extensions: tuple[dict[str, Any], ...],
) -> tuple[str, ...]:
    references: set[str] = set()
    reference_objects = _extension_values(
        extensions,
        ALTERNATIVES_EXTENSION_URI,
        "valueReference",
    )
    for reference_object in reference_objects:
        if type(reference_object) is not dict or set(reference_object) != {"reference"}:
            raise ValueError("FHIR alternative reference object is invalid")
        reference_text = strict_fhir_text(
            reference_object["reference"],
            "alternative reference",
            maximum_length=96,
            is_required=True,
        )
        assert reference_text is not None
        if not ALTERNATIVE_REFERENCE_PATTERN.fullmatch(reference_text):
            raise ValueError("FHIR alternative reference is invalid")
        references.add(reference_text)
    return tuple(sorted(references))


def _raw_identifiers(resource: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_identifiers = resource.get("identifier", [])
    if type(raw_identifiers) is not list:
        raise ValueError("FHIR identifier field must be a list")
    identifier_rows: list[dict[str, Any]] = []
    for raw_identifier in raw_identifiers:
        if type(raw_identifier) is not dict:
            raise ValueError("FHIR identifier entry must be an object")
        identifier_rows.append(fhir_json_snapshot(raw_identifier))
    return tuple(identifier_rows)


def parse_coverage_plan(
    resource: object,
    *,
    canonical_base: object,
) -> CoveragePlanRecord:
    """Normalize one strict CoveragePlan List and preserve raw evidence."""

    resource_object = strict_fhir_resource(resource, "List")
    list_id = validated_fhir_id(
        resource_object.get("id"),
        label="coverage plan List id",
    )
    version_id, last_updated = fhir_resource_metadata(resource_object)
    extensions = _extension_rows(resource_object)
    aliases = _plan_aliases(extensions, is_required=True)
    identifiers = _raw_identifiers(resource_object)
    status = strict_fhir_text(resource_object.get("status"), "status", maximum_length=32)
    title = strict_fhir_text(resource_object.get("title"), "title", maximum_length=2_048)
    name = strict_fhir_text(resource_object.get("name"), "name", maximum_length=2_048)
    upstream_date = optional_fhir_instant(
        resource_object.get("date"),
        field_name="date",
    )
    normalized_fields_by_name = {
        "id": list_id,
        "version_id": version_id,
        "last_updated": last_updated.isoformat(),
        "status": status,
        "title": title,
        "name": name,
        "date": upstream_date.isoformat() if upstream_date else None,
        "identifiers": list(identifiers),
        "extensions": list(extensions),
        "aliases": list(aliases),
    }
    return CoveragePlanRecord(
        upstream_list_id=list_id,
        public_id=public_formulary_id(canonical_base, list_id),
        canonical_identity=canonical_list_identity(canonical_base, list_id),
        upstream_version_id=version_id,
        upstream_last_updated=last_updated,
        status=status,
        title=title,
        name=name,
        upstream_date=upstream_date,
        period_start=None,
        period_end=None,
        source_plan_identifiers=aliases,
        raw_identifiers=identifiers,
        raw_extensions=extensions,
        content_hash=fhir_content_hash(normalized_fields_by_name),
    )


def _medication_policy(
    extensions: tuple[dict[str, Any], ...],
) -> MedicationPolicyFields:
    return MedicationPolicyFields(
        tier=_tier_value(extensions),
        prior_authorization=_single_boolean_extension(
            extensions,
            PRIOR_AUTH_EXTENSION_URI,
        ),
        step_therapy=_single_boolean_extension(
            extensions,
            STEP_THERAPY_EXTENSION_URI,
        ),
        quantity_limit=_single_boolean_extension(
            extensions,
            QUANTITY_LIMIT_EXTENSION_URI,
        ),
        alternative_references=_alternative_references(extensions),
    )


def _medication_content_hash(
    medication_id: str,
    version_id: str | None,
    last_updated: dt.datetime,
    status: str | None,
    codings: tuple[FHIRCoding, ...],
    extensions: tuple[dict[str, Any], ...],
    aliases: tuple[str, ...],
    policy: MedicationPolicyFields,
) -> str:
    normalized_codings = [
        {
            "system": coding.system,
            "code": coding.code,
            "display": coding.display,
            "version": coding.version,
        }
        for coding in codings
    ]
    normalized_fields_by_name = {
        "id": medication_id,
        "version_id": version_id,
        "last_updated": last_updated.isoformat(),
        "status": status,
        "codings": normalized_codings,
        "extensions": list(extensions),
        "aliases": list(aliases),
        "tier": policy.tier,
        "prior_authorization": policy.prior_authorization,
        "step_therapy": policy.step_therapy,
        "quantity_limit": policy.quantity_limit,
        "alternatives": list(policy.alternative_references),
    }
    return fhir_content_hash(normalized_fields_by_name)


def parse_medication_knowledge(resource: object) -> MedicationRecord:
    """Normalize one strict FormularyDrug and preserve approved evidence."""

    resource_object = strict_fhir_resource(resource, "MedicationKnowledge")
    medication_id = validated_fhir_id(
        resource_object.get("id"),
        label="MedicationKnowledge id",
    )
    version_id, last_updated = fhir_resource_metadata(resource_object)
    extensions = _extension_rows(resource_object)
    codings = _coding_rows(resource_object)
    aliases = _plan_aliases(extensions, is_required=False)
    policy = _medication_policy(extensions)
    status = strict_fhir_text(resource_object.get("status"), "status", maximum_length=32)
    rxnorm_id = _unambiguous_code(
        codings,
        system_uri=RXNORM_SYSTEM_URI,
        code_pattern=re.compile(r"[0-9]+\Z"),
    )
    ndc11 = _unambiguous_code(
        codings,
        system_uri=NDC_SYSTEM_URI,
        code_pattern=re.compile(r"[0-9]{11}\Z"),
    )
    return MedicationRecord(
        upstream_medication_id=medication_id,
        upstream_version_id=version_id,
        upstream_last_updated=last_updated,
        status=status,
        drug_name=preferred_coding_display(
            codings,
            preferred_system=RXNORM_SYSTEM_URI,
        ),
        rxnorm_id=rxnorm_id,
        ndc11=ndc11,
        codings=codings,
        raw_extensions=extensions,
        source_plan_identifiers=aliases,
        drug_tier=policy.tier,
        prior_authorization=policy.prior_authorization,
        step_therapy=policy.step_therapy,
        quantity_limit=policy.quantity_limit,
        alternative_references=policy.alternative_references,
        content_hash=_medication_content_hash(
            medication_id,
            version_id,
            last_updated,
            status,
            codings,
            extensions,
            aliases,
            policy,
        ),
    )


def _validated_correction(
    correction: AlternativeCorrection | None,
) -> AlternativeCorrection | None:
    if correction is None:
        return None
    if type(correction) is not AlternativeCorrection or not CORRECTION_PREFIX_PATTERN.fullmatch(
        correction.prefix
    ):
        raise ValueError("FHIR alternative correction policy is invalid")
    strict_fhir_text(
        correction.rule_version,
        "alternative correction version",
        maximum_length=64,
        is_required=True,
    )
    return correction


def resolve_alternative_references(
    raw_references: Iterable[object],
    *,
    known_medication_ids: set[str],
    correction: AlternativeCorrection | None = None,
) -> tuple[AlternativeEvidence, ...]:
    """Resolve valid same-generation references with optional generic evidence."""

    if isinstance(raw_references, (str, bytes)) or type(known_medication_ids) is not set:
        raise ValueError("FHIR alternative reference collection is invalid")
    validated_known_ids = {
        validated_fhir_id(medication_id, label="known MedicationKnowledge id")
        for medication_id in known_medication_ids
    }
    validated_policy = _validated_correction(correction)
    validated_references: set[str] = set()
    for raw_reference in raw_references:
        reference_text = strict_fhir_text(
            raw_reference,
            "alternative reference",
            maximum_length=96,
            is_required=True,
        )
        assert reference_text is not None
        if not ALTERNATIVE_REFERENCE_PATTERN.fullmatch(reference_text):
            raise ValueError("FHIR alternative reference is invalid")
        validated_references.add(reference_text)
    return tuple(
        _alternative_evidence(
            reference_text,
            known_medication_ids=validated_known_ids,
            correction=validated_policy,
        )
        for reference_text in sorted(validated_references)
    )


def _alternative_evidence(
    reference_text: str,
    *,
    known_medication_ids: set[str],
    correction: AlternativeCorrection | None,
) -> AlternativeEvidence:
    reference_match = ALTERNATIVE_REFERENCE_PATTERN.fullmatch(reference_text)
    assert reference_match is not None
    medication_id = reference_match.group(1)
    if medication_id in known_medication_ids:
        return AlternativeEvidence(reference_text, None, medication_id, True, None)
    corrected_id = None
    if correction is not None and not medication_id.startswith(correction.prefix):
        candidate_id = f"{correction.prefix}{medication_id}"
        if validated_fhir_id(candidate_id) in known_medication_ids:
            corrected_id = candidate_id
    return AlternativeEvidence(
        raw_reference=reference_text,
        corrected_reference=(
            f"MedicationKnowledge/{corrected_id}" if corrected_id else None
        ),
        resolved_medication_id=corrected_id,
        is_resolved=corrected_id is not None,
        rule_version=(correction.rule_version if correction and corrected_id else None),
    )
