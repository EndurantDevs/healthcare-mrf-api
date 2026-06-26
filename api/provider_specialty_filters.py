# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping


PTG2_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")

PRIMARY_CARE_TAXONOMY_CODES: tuple[str, ...] = (
    "207Q00000X",  # Family Medicine
    "207R00000X",  # Internal Medicine
    "208000000X",  # Pediatrics
    "208D00000X",  # General Practice
    "363LA2200X",  # Adult Health Nurse Practitioner
    "363LF0000X",  # Family Nurse Practitioner
    "363LP0200X",  # Pediatrics Nurse Practitioner
    "363LP2300X",  # Primary Care Nurse Practitioner
)

FAMILY_MEDICINE_TAXONOMY_CODES: tuple[str, ...] = (
    "207Q00000X",  # Family Medicine
    "208D00000X",  # General Practice
)

# Full NUCC 207X "Orthopaedic Surgery" family (base + subspecialties), so that a
# request for an orthopedic procedure (e.g. ACL reconstruction, CPT 29888) scopes to
# every orthopedic surgeon regardless of subspecialty.
ORTHOPAEDIC_SURGERY_TAXONOMY_CODES: tuple[str, ...] = (
    "207X00000X",  # Orthopaedic Surgery
    "207XP3100X",  # Pediatric Orthopaedic Surgery
    "207XS0106X",  # Hand Surgery (Orthopaedic)
    "207XS0114X",  # Adult Reconstructive Orthopaedic Surgery
    "207XS0117X",  # Orthopaedic Surgery of the Spine
    "207XX0004X",  # Foot and Ankle Orthopaedic Surgery
    "207XX0005X",  # Sports Medicine (Orthopaedic Surgery)
    "207XX0801X",  # Orthopaedic Trauma
)

_SPECIALTY_TAXONOMY_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    "primary care": PRIMARY_CARE_TAXONOMY_CODES,
    "pcp": PRIMARY_CARE_TAXONOMY_CODES,
    "primary care physician": PRIMARY_CARE_TAXONOMY_CODES,
    "primary care provider": PRIMARY_CARE_TAXONOMY_CODES,
    "family doctor": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family medicine": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family physician": FAMILY_MEDICINE_TAXONOMY_CODES,
    "family practice": FAMILY_MEDICINE_TAXONOMY_CODES,
    "general practice": ("208D00000X",),
    "general practitioner": ("208D00000X",),
    "pediatrics": ("208000000X",),
    "pediatrician": ("208000000X",),
    "dermatology": ("207N00000X",),
    "dermatologist": ("207N00000X",),
    "cardiology": ("207RC0000X",),
    "cardiologist": ("207RC0000X",),
    "emergency medicine": ("207P00000X",),
    "emergency room": ("207P00000X",),
    "er": ("207P00000X",),
    "spinal surgeon": ("207XS0117X", "207T00000X"),
    "spine surgeon": ("207XS0117X", "207T00000X"),
    "orthopedic surgery": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic surgery": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedic surgeon": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic surgeon": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedics": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedics": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedist": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedist": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopedic": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "orthopaedic": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "ortho": ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
    "neurology": ("2084N0400X",),
    "neurologist": ("2084N0400X",),
    "multiple sclerosis": ("2084N0400X",),
    "ent": ("207Y00000X",),
    "otolaryngology": ("207Y00000X",),
    "otolaryngologist": ("207Y00000X",),
    "gastroenterology": ("207RG0100X",),
    "gastroenterologist": ("207RG0100X",),
    "physical therapist": ("225100000X",),
    "physical therapy": ("225100000X",),
    "dietitian": ("133V00000X",),
    "registered dietitian": ("133V00000X",),
    "mental health": ("101YP2500X", "103TC0700X", "1041C0700X", "106H00000X"),
    "therapist": ("101YP2500X", "103TC0700X", "1041C0700X", "106H00000X"),
    "psychologist": ("103T00000X", "103TC0700X"),
    "rheumatology": ("207RR0500X",),
    "rheumatologist": ("207RR0500X",),
    "ob-gyn": ("207V00000X",),
    "obgyn": ("207V00000X",),
    "infertility specialist": ("207VE0102X",),
    "ophthalmology": ("207W00000X",),
    "ophthalmologist": ("207W00000X",),
    "optometry": ("152W00000X",),
    "optometrist": ("152W00000X",),
    "laboratory": ("291U00000X",),
    "lab": ("291U00000X",),
    "clinical lab": ("291U00000X",),
    "dme": ("332B00000X",),
    "dme supplier": ("332B00000X",),
    "durable medical equipment": ("332B00000X",),
    "urgent care": ("261QU0200X",),
    "acupuncturist": ("171100000X",),
    "acupuncture": ("171100000X",),
    "massage therapist": ("225700000X",),
    "massage therapy": ("225700000X",),
    "hospice": ("251G00000X",),
    "home hospice": ("251G00000X", "251E00000X"),
    "hospital": ("282N00000X",),
}

_SPECIALTY_CLASSIFICATION_ALIASES: dict[str, str] = {
    "internal medicine": "Internal Medicine",
    "internist": "Internal Medicine",
    "dentist": "Dentist",
    "dental": "Dentist",
    "nurse practitioner": "Nurse Practitioner",
    "physician assistant": "Physician Assistant",
}

_CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES: dict[str, tuple[str, ...]] = {
    "family medicine": FAMILY_MEDICINE_TAXONOMY_CODES,
    "internal medicine": ("207R00000X",),
    "pediatrics": ("208000000X",),
    "general practice": ("208D00000X",),
    "dermatology": ("207N00000X",),
    "emergency medicine": ("207P00000X",),
    "dentist": ("122300000X",),
}


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _normalize_taxonomy_codes(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        raw_items = value.replace(";", ",").split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]
    codes: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        code = str(item or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return tuple(codes)


@dataclass(frozen=True)
class ProviderSpecialtyFilter:
    specialty: str | None = None
    classification: str | None = None
    taxonomy_codes: tuple[str, ...] = ()
    include_subspecialties: bool = False
    primary_only: bool = True
    use_classification_predicate: bool = True

    @property
    def active(self) -> bool:
        return bool(self.classification or self.taxonomy_codes)

    def response_payload(self) -> dict[str, Any] | None:
        if not self.active:
            return None
        payload: dict[str, Any] = {
            "primary_only": self.primary_only,
            "include_subspecialties": self.include_subspecialties,
        }
        if self.specialty:
            payload["specialty"] = self.specialty
        if self.classification:
            payload["classification"] = self.classification
        if self.taxonomy_codes:
            payload["taxonomy_code_set"] = list(self.taxonomy_codes)
        return payload


def resolve_provider_specialty_filter(args: Mapping[str, Any]) -> ProviderSpecialtyFilter:
    specialty = str(args.get("specialty") or "").strip()
    classification = str(args.get("classification") or "").strip()
    taxonomy_codes = _normalize_taxonomy_codes(args.get("taxonomy_codes"))
    include_subspecialties = _normalize_bool(args.get("include_subspecialties"))
    primary_only = True
    if args.get("primary_only") not in (None, ""):
        primary_only = _normalize_bool(args.get("primary_only"))

    key = specialty.lower()
    if not taxonomy_codes and key in _SPECIALTY_TAXONOMY_CODE_ALIASES:
        taxonomy_codes = _SPECIALTY_TAXONOMY_CODE_ALIASES[key]
    if not classification and key in _SPECIALTY_CLASSIFICATION_ALIASES:
        classification = _SPECIALTY_CLASSIFICATION_ALIASES[key]
    use_classification_predicate = True
    classification_key = classification.lower()
    if (
        classification_key
        and not taxonomy_codes
        and not include_subspecialties
        and classification_key in _CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES
    ):
        taxonomy_codes = _CLASSIFICATION_BASE_TAXONOMY_CODE_ALIASES[classification_key]
        use_classification_predicate = False

    return ProviderSpecialtyFilter(
        specialty=specialty or None,
        classification=classification or None,
        taxonomy_codes=taxonomy_codes,
        include_subspecialties=include_subspecialties,
        primary_only=primary_only,
        use_classification_predicate=use_classification_predicate,
    )


def provider_specialty_taxonomy_exists_sql(
    npi_sql: str,
    params: dict[str, Any],
    param_prefix: str,
    specialty_filter: ProviderSpecialtyFilter,
    *,
    schema: str = PTG2_SCHEMA,
    nt_alias: str | None = None,
    nucc_alias: str | None = None,
) -> str:
    if not specialty_filter.active:
        return ""

    nt = nt_alias or f"{param_prefix}_nt"
    nucc = nucc_alias or f"{param_prefix}_nucc"
    clauses = [f"{nt}.npi = {npi_sql}"]
    joins = ""

    if specialty_filter.primary_only:
        clauses.append(f"UPPER(COALESCE({nt}.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'")

    if specialty_filter.taxonomy_codes:
        code_placeholders: list[str] = []
        for idx, code in enumerate(specialty_filter.taxonomy_codes):
            key = f"{param_prefix}_taxonomy_code_{idx}"
            params[key] = code
            code_placeholders.append(f":{key}")
        clauses.append(
            f"UPPER(COALESCE({nt}.healthcare_provider_taxonomy_code, '')) IN ({', '.join(code_placeholders)})"
        )

    if specialty_filter.classification and specialty_filter.use_classification_predicate:
        classification_key = f"{param_prefix}_classification"
        params[classification_key] = specialty_filter.classification
        joins = f"\n                    JOIN {schema}.nucc_taxonomy {nucc} ON {nucc}.code = {nt}.healthcare_provider_taxonomy_code"
        clauses.append(f"LOWER(COALESCE({nucc}.classification, '')) = LOWER(:{classification_key})")
        if not specialty_filter.include_subspecialties:
            clauses.append(f"NULLIF(BTRIM(COALESCE({nucc}.specialization, '')), '') IS NULL")

    where = "\n                      AND ".join(clauses)
    return f"""EXISTS (
                    SELECT 1
                    FROM {schema}.npi_taxonomy {nt}{joins}
                    WHERE {where}
                )"""
