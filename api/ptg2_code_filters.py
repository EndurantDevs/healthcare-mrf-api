# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 code-filter and inferred-provider taxonomy helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from api.code_systems import (
    EQUIVALENT_PROCEDURE_CODE_SYSTEMS,
    EXTERNAL_PROCEDURE_CODE_SYSTEMS,
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    PROCEDURE_CODE_SYSTEMS,
    canonical_catalog_code,
    catalog_code_lookup_values,
    normalize_code_system,
)
from api.ptg2_response import _include_ptg2_details

PTG2_CODE_EXPANSION_HOPS = 2


@dataclass(frozen=True)
class InferredProviderTaxonomyRule:
    ranges: tuple[tuple[int, int], ...]
    taxonomy_codes: tuple[str, ...]
    display_terms: tuple[str, ...]

    def matches(self, code_value: int) -> bool:
        """Return whether a numeric procedure code falls in this rule."""

        return any(start <= code_value <= end for start, end in self.ranges)


INFERRED_PROVIDER_TAXONOMY_RULES = (
    InferredProviderTaxonomyRule(
        ranges=((77261, 77799),),
        taxonomy_codes=("2085R0001X",),
        display_terms=("radiation oncology",),
    ),
    InferredProviderTaxonomyRule(
        # CPT musculoskeletal-system surgery (20000-29999), e.g. arthroscopic ACL
        # reconstruction 29888 -> orthopaedic surgery (NUCC 207X family). Scopes a
        # procedure search to orthopedic surgeons and narrows the in-network provider
        # expansion so plan+location lookups stay fast instead of scanning the whole
        # (national) network.
        ranges=((20000, 29999),),
        taxonomy_codes=(
            "207X00000X",  # Orthopaedic Surgery
            "207XP3100X",  # Pediatric Orthopaedic Surgery
            "207XS0106X",  # Hand Surgery (Orthopaedic)
            "207XS0114X",  # Adult Reconstructive Orthopaedic Surgery
            "207XS0117X",  # Orthopaedic Surgery of the Spine
            "207XX0004X",  # Foot and Ankle Orthopaedic Surgery
            "207XX0005X",  # Sports Medicine (Orthopaedic Surgery)
            "207XX0801X",  # Orthopaedic Trauma
        ),
        display_terms=("orthopaedic surgery", "orthopedic surgery"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((70000, 77260), (77800, 79999)),
        taxonomy_codes=(
            "2084D0003X",
            "2085B0100X",
            "2085D0003X",
            "2085N0700X",
            "2085N0904X",
            "2085P0229X",
            "2085R0202X",
            "2085R0204X",
            "2085U0001X",
        ),
        display_terms=(
            "diagnostic radiology",
            "neuroradiology",
            "body imaging",
            "nuclear radiology",
            "pediatric radiology",
            "interventional radiology",
            "diagnostic ultrasound",
            "diagnostic neuroimaging",
            "breast imaging",
        ),
    ),
    InferredProviderTaxonomyRule(
        ranges=((100, 1999), (99100, 99140)),
        taxonomy_codes=("207L00000X", "367500000X", "367H00000X"),
        display_terms=("anesthesiology", "nurse anesthetist", "anesthesiologist assistant"),
    ),
    InferredProviderTaxonomyRule(
        # Psychiatric diagnostic/interview and psychotherapy services (90785-90899)
        # are commonly billed by behavioral-health clinicians. Without this curated
        # range, sparse Medicare evidence can leave the resolver in validate-only
        # mode and callers may surface unrelated specialties from broad plan searches.
        ranges=((90785, 90899),),
        taxonomy_codes=(
            "101YM0800X",  # Counselor, Mental Health
            "101YP2500X",  # Counselor, Professional
            "103T00000X",  # Psychologist
            "103TC0700X",  # Clinical Psychologist
            "1041C0700X",  # Clinical Social Worker
            "106H00000X",  # Marriage & Family Therapist
            "2084P0800X",  # Psychiatry
            "363LP0808X",  # Nurse Practitioner, Psych/Mental Health
            "364SP0808X",  # Clinical Nurse Specialist, Psych/Mental Health
        ),
        display_terms=(
            "psychotherapy",
            "behavioral health",
            "mental health",
            "psychology",
            "clinical social work",
            "psychiatry",
        ),
    ),
    InferredProviderTaxonomyRule(
        ranges=((80000, 87999),),
        taxonomy_codes=(
            "1223P0106X",
            "207ND0900X",
            "207SM0001X",
            "207ZC0006X",
            "207ZC0008X",
            "207ZC0500X",
            "207ZD0900X",
            "207ZF0201X",
            "207ZH0000X",
            "207ZI0100X",
            "207ZN0500X",
            "207ZP0007X",
            "207ZP0101X",
            "207ZP0102X",
            "207ZP0104X",
            "207ZP0105X",
            "207ZP0213X",
            "235Z00000X",
            "246Q00000X",
            "246QC1000X",
            "246QI0000X",
            "246R00000X",
            "291900000X",
            "291U00000X",
        ),
        display_terms=("clinical medical laboratory", "pathology", "pathologist", "laboratory medicine"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((97000, 97799),),
        taxonomy_codes=(
            "103TR0400X",
            "111NR0400X",
            "152WL0500X",
            "163WC3500X",
            "163WR0400X",
            "208100000X",
            "2081H0002X",
            "2081N0008X",
            "2081P0010X",
            "2081P0301X",
            "2081P2900X",
            "2081S0010X",
            "224Z00000X",
            "225100000X",
            "2251C2600X",
            "2251E1200X",
            "2251E1300X",
            "2251G0304X",
            "2251H1200X",
            "2251H1300X",
            "2251N0400X",
            "2251P0200X",
            "2251S0007X",
            "2251X0800X",
            "225200000X",
            "225400000X",
            "2255R0406X",
            "225C00000X",
            "225CA2400X",
            "225CA2500X",
            "225CX0006X",
            "225X00000X",
            "225XE0001X",
            "225XE1200X",
            "225XF0002X",
            "225XG0600X",
            "225XH1200X",
            "225XH1300X",
            "225XL0004X",
            "225XM0800X",
            "225XN1300X",
            "225XP0019X",
            "225XP0200X",
            "225XR0403X",
            "2278P1005X",
            "2279P1005X",
            "261QP2000X",
            "261QR0400X",
            "261QR0401X",
            "261QR0404X",
            "261QR0405X",
            "273Y00000X",
            "276400000X",
            "283X00000X",
            "283XC2000X",
            "324500000X",
            "3245S0500X",
            "364SR0400X",
        ),
        display_terms=("physical therapist", "physical therapy", "occupational therapist", "rehabilitation"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((65091, 68899), (92002, 92499)),
        taxonomy_codes=("207W00000X", "152W00000X"),
        display_terms=("ophthalmology", "ophthalmologist", "optometrist", "vision therapy"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((43200, 45399),),
        taxonomy_codes=("163WG0100X", "207RG0100X", "2080P0206X", "208C00000X", "261QE0800X"),
        display_terms=("gastroenterology", "colon & rectal", "colon and rectal", "endoscopy"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((99281, 99285),),
        taxonomy_codes=("207P00000X", "2080P0204X"),
        display_terms=("emergency medicine", "pediatric emergency medicine"),
    ),
)


def _normalize_code(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_code_system(value: Any) -> str | None:
    text = normalize_code_system(value)
    return text or None


def _normalize_code_for_system(value: Any, code_system: str | None) -> str:
    if code_system:
        return canonical_catalog_code(code_system, value)
    return _normalize_code(value)


def _is_signed_int_text(value: str) -> bool:
    return value.lstrip("-").isdigit() and value not in {"", "-"}


def _reported_code_values_for_system(code_system: str | None, code: Any) -> tuple[str, ...]:
    if code_system:
        return catalog_code_lookup_values(code_system, code)
    value = _normalize_code(code)
    return (value,) if value else ()


def _append_reported_code_values_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    values: tuple[str, ...],
) -> None:
    params["reported_code"] = values[0]
    if len(values) == 1:
        filters.append("reported_code = :reported_code")
        return
    placeholders = [":reported_code"]
    for idx, value in enumerate(values[1:], start=1):
        key = f"reported_code_{idx}"
        params[key] = value
        placeholders.append(f":{key}")
    filters.append("reported_code IN (" + ", ".join(placeholders) + ")")


def _append_code_filter(filters: list[str], params: dict[str, Any], *, code: Any, code_system: Any) -> None:
    requested_system = _normalize_code_system(code_system)
    requested_code = _normalize_code_for_system(code, requested_system)
    if not requested_code:
        return
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        if _is_signed_int_text(requested_code):
            filters.append("procedure_code = :procedure_code")
            params["procedure_code"] = int(requested_code)
        else:
            filters.append("FALSE")
        return
    code_filters: list[str] = []
    _append_reported_code_values_filter(
        code_filters,
        params,
        values=_reported_code_values_for_system(requested_system, code),
    )
    if requested_system:
        filters.append(
            f"""
            (
                reported_code_system = :reported_code_system
            AND {code_filters[0]}
            )
            """
        )
        params["reported_code_system"] = requested_system
        return

    code_clauses = code_filters
    if _is_signed_int_text(requested_code) and (requested_code.startswith("-") or len(requested_code) > 5):
        code_clauses.append("procedure_code = :procedure_code")
        params["procedure_code"] = int(requested_code)
    filters.append("(" + " OR ".join(code_clauses) + ")")


def _is_external_procedure_code_text(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{5}", value))


def _ptg2_equivalent_external_pairs(system: str, code: str) -> set[tuple[str, str]]:
    if system not in EQUIVALENT_PROCEDURE_CODE_SYSTEMS or not _is_external_procedure_code_text(code):
        return set()
    if re.fullmatch(r"\d{5}", code):
        systems = {"CPT", "HCPCS"}
    elif re.fullmatch(r"D\d{4}", code):
        systems = {"CDT", "HCPCS"}
    else:
        systems = {system}
    return {(candidate_system, code) for candidate_system in systems}


def _ptg2_code_context(
    *,
    input_system: str | None,
    input_code: str,
    resolved_pairs: set[tuple[str, str]],
    internal_codes: set[int],
    matched_via: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "input_code": {"code_system": input_system, "code": input_code},
        "resolved_codes": [
            {"code_system": system, "code": code}
            for system, code in sorted(resolved_pairs, key=lambda item: (item[0], item[1]))
        ],
        "internal_codes": sorted(internal_codes),
        "matched_via": matched_via or [],
        "code_match_mode": "equivalent_procedure_codes",
    }


def _append_resolved_code_filter(
    filters: list[str],
    params: dict[str, Any],
    *,
    code: Any,
    code_system: Any,
    code_context: dict[str, Any] | None = None,
) -> None:
    requested_system = _normalize_code_system(code_system)
    requested_code = _normalize_code_for_system(code, requested_system)
    if not requested_code:
        return
    if code_context is None:
        _append_code_filter(filters, params, code=code, code_system=code_system)
        return

    clauses: list[str] = []
    external_codes_by_value: dict[str, list[str]] = {}
    for resolved_code in code_context.get("resolved_codes") or []:
        system = _normalize_code_system(resolved_code.get("code_system")) or ""
        if system == INTERNAL_PROCEDURE_CODE_SYSTEM:
            continue
        for resolved_value in _reported_code_values_for_system(system, resolved_code.get("code")):
            external_codes_by_value.setdefault(resolved_value, []).append(system)
    for idx, (resolved_value, systems) in enumerate(sorted(external_codes_by_value.items())):
        params[f"reported_code_{idx}"] = resolved_value
        system_placeholders = []
        for system_idx, system in enumerate(sorted(set(systems))):
            key = f"reported_code_system_{idx}_{system_idx}"
            params[key] = system
            system_placeholders.append(f":{key}")
        clauses.append(
            f"""
            (
                reported_code = :reported_code_{idx}
            AND reported_code_system IN ({", ".join(system_placeholders)})
            )
            """
        )
    internal_codes = [int(value) for value in code_context.get("internal_codes") or []]
    if internal_codes:
        internal_placeholders = []
        for idx, internal_code in enumerate(internal_codes):
            key = f"procedure_code_{idx}"
            params[key] = internal_code
            internal_placeholders.append(f":{key}")
        clauses.append("procedure_code IN (" + ", ".join(internal_placeholders) + ")")
    if not clauses and requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        filters.append("FALSE")
        return
    if clauses:
        filters.append("(" + " OR ".join(clauses) + ")")


def _ptg2_code_query_fields(code_context: dict[str, Any] | None, args: dict[str, Any]) -> dict[str, Any]:
    if not code_context or not _include_ptg2_details(args):
        return {}
    return {
        "input_code": code_context.get("input_code"),
        "resolved_codes": code_context.get("resolved_codes") or [],
        "matched_via": code_context.get("matched_via") or [],
        "code_match_mode": code_context.get("code_match_mode"),
    }


def _qualify_compact_filters(filters: list[str]) -> list[str]:
    qualified = []
    replacements = {
        "reported_code_system": "r.reported_code_system",
        "procedure_code": "r.procedure_code",
        "reported_code": "r.reported_code",
        "snapshot_id": "r.snapshot_id",
        "plan_id": "r.plan_id",
    }
    for value in filters:
        text_value = value
        for source, target in replacements.items():
            text_value = re.sub(rf"(?<![:.])\b{re.escape(source)}\b", target, text_value)
        qualified.append(text_value)
    return qualified


def _normalize_taxonomy_code(value: Any) -> str | None:
    text_value = str(value or "").strip().upper()
    if not text_value:
        return None
    if not re.fullmatch(r"[A-Z0-9X]{10}", text_value):
        return None
    return text_value


def _normalize_npi(value: Any) -> int | None:
    text_value = str(value or "").strip()
    if not text_value or not text_value.isdigit():
        return None
    parsed = int(text_value)
    return parsed if parsed > 0 else None


def _inferred_provider_taxonomy_sql(args: dict[str, Any], *, nt_alias: str, nucc_alias: str) -> str:
    requested_system = _normalize_code_system(args.get("code_system"))
    requested_code = _normalize_code(args.get("code"))
    if requested_system != "CPT" or not requested_code or not requested_code.isdigit():
        return ""
    code_value = int(requested_code)
    matching_rule = next((rule for rule in INFERRED_PROVIDER_TAXONOMY_RULES if rule.matches(code_value)), None)
    if matching_rule:
        code_sql = ",\n                    ".join(f"'{code}'" for code in matching_rule.taxonomy_codes)
        display_sql = "\n".join(
            f"             OR LOWER(COALESCE({nucc_alias}.display_name, '')) LIKE '%{term}%'"
            for term in matching_rule.display_terms
        )
        return f"""
            (
                {nt_alias}.healthcare_provider_taxonomy_code IN (
                    {code_sql}
                )
{display_sql}
            )
        """
    return ""
