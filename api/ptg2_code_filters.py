# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 code-filter and inferred-provider taxonomy helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from api.ptg2_response import _include_ptg2_details

INTERNAL_PROCEDURE_CODE_SYSTEM = "HP_PROCEDURE_CODE"
EXTERNAL_PROCEDURE_CODE_SYSTEMS = {"CPT", "HCPCS"}
PROCEDURE_CODE_SYSTEMS = {*EXTERNAL_PROCEDURE_CODE_SYSTEMS, INTERNAL_PROCEDURE_CODE_SYSTEM}
PTG2_CODE_EXPANSION_HOPS = 2


@dataclass(frozen=True)
class InferredProviderTaxonomyRule:
    ranges: tuple[tuple[int, int], ...]
    taxonomy_codes: tuple[str, ...]
    display_terms: tuple[str, ...]

    def matches(self, code_value: int) -> bool:
        return any(start <= code_value <= end for start, end in self.ranges)


INFERRED_PROVIDER_TAXONOMY_RULES = (
    InferredProviderTaxonomyRule(
        ranges=((77261, 77799),),
        taxonomy_codes=("2085R0001X",),
        display_terms=("radiation oncology",),
    ),
    InferredProviderTaxonomyRule(
        ranges=((70000, 77260), (77800, 79999)),
        taxonomy_codes=(
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
        ranges=((80000, 87999),),
        taxonomy_codes=("291U00000X",),
        display_terms=("clinical medical laboratory", "pathology", "pathologist", "laboratory medicine"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((97000, 97799),),
        taxonomy_codes=("225100000X", "225200000X", "225X00000X", "224Z00000X"),
        display_terms=("physical therapist", "physical therapy", "occupational therapist", "rehabilitation"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((65091, 68899), (92002, 92499)),
        taxonomy_codes=("207W00000X", "152W00000X"),
        display_terms=("ophthalmology", "ophthalmologist", "optometrist", "vision therapy"),
    ),
    InferredProviderTaxonomyRule(
        ranges=((43200, 45399),),
        taxonomy_codes=("207RG0100X", "208C00000X"),
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
    text = str(value or "").strip().upper()
    return text or None


def _is_signed_int_text(value: str) -> bool:
    return value.lstrip("-").isdigit() and value not in {"", "-"}


def _append_code_filter(filters: list[str], params: dict[str, Any], *, code: Any, code_system: Any) -> None:
    requested_code = _normalize_code(code)
    if not requested_code:
        return
    requested_system = _normalize_code_system(code_system)
    params["reported_code"] = requested_code
    if requested_system == INTERNAL_PROCEDURE_CODE_SYSTEM:
        if _is_signed_int_text(requested_code):
            filters.append("procedure_code = :procedure_code")
            params["procedure_code"] = int(requested_code)
        else:
            filters.append("FALSE")
        return
    if requested_system:
        filters.append(
            """
            (
                reported_code_system = :reported_code_system
            AND reported_code = :reported_code
            )
            """
        )
        params["reported_code_system"] = requested_system
        return

    code_clauses = ["reported_code = :reported_code"]
    if _is_signed_int_text(requested_code) and (requested_code.startswith("-") or len(requested_code) > 5):
        code_clauses.append("procedure_code = :procedure_code")
        params["procedure_code"] = int(requested_code)
    filters.append("(" + " OR ".join(code_clauses) + ")")


def _is_external_procedure_code_text(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{5}", value))


def _ptg2_equivalent_external_pairs(system: str, code: str) -> set[tuple[str, str]]:
    if system not in EXTERNAL_PROCEDURE_CODE_SYSTEMS or not _is_external_procedure_code_text(code):
        return set()
    return {(candidate_system, code) for candidate_system in EXTERNAL_PROCEDURE_CODE_SYSTEMS}


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
    requested_code = _normalize_code(code)
    if not requested_code:
        return
    requested_system = _normalize_code_system(code_system)
    if code_context is None:
        _append_code_filter(filters, params, code=code, code_system=code_system)
        return

    clauses: list[str] = []
    external_codes_by_value: dict[str, list[str]] = {}
    for resolved_code in code_context.get("resolved_codes") or []:
        system = str(resolved_code.get("code_system") or "").strip().upper()
        resolved_value = str(resolved_code.get("code") or "").strip().upper()
        if system == INTERNAL_PROCEDURE_CODE_SYSTEM:
            continue
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
