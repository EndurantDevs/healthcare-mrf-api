# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import os
from typing import Any

INTERNAL_PROCEDURE_CODE_SYSTEM = "HP_PROCEDURE_CODE"
INTERNAL_RX_CODE_SYSTEM = "HP_RX_CODE"
EQUIVALENT_PROCEDURE_CODE_SYSTEMS = {"CPT", "HCPCS", "CDT"}
EXTERNAL_PROCEDURE_CODE_SYSTEMS = {*EQUIVALENT_PROCEDURE_CODE_SYSTEMS, "MS_DRG"}
PROCEDURE_CODE_SYSTEMS = {*EXTERNAL_PROCEDURE_CODE_SYSTEMS, INTERNAL_PROCEDURE_CODE_SYSTEM}
RX_CODE_SYSTEMS = {"NDC", "RXNORM", "RXCUI", INTERNAL_RX_CODE_SYSTEM}
RESTRICTED_TERMINOLOGY_CODE_SYSTEMS = {"SNOMEDCT_US"}

CODE_SYSTEM_ALIASES = {
    "CLM_REV_CNTR_CD": "RC",
    "PLACE_OF_SERVICE": "POS",
    "REVENUE_CENTER": "RC",
    "REVENUE_CODE": "RC",
    "REV_CNTR": "RC",
    "SERVICE_CODE": "POS",
    "BILLING_CODE_MODIFIER": "MODIFIER",
    "CPT_MODIFIER": "MODIFIER",
    "HCPCS_MODIFIER": "MODIFIER",
    "MOD": "MODIFIER",
    "ICD-10-CM": "ICD10CM",
    "ICD10": "ICD10CM",
    "ICD-10-PCS": "ICD10PCS",
    "MS-DRG": "MS_DRG",
    "MSDRG": "MS_DRG",
    "DRG": "MS_DRG",
    "RXCUI": "RXNORM",
    "SNOMED": "SNOMEDCT_US",
    "SNOMEDCT": "SNOMEDCT_US",
}


def normalize_code_system(raw_system: Any, default: str | None = None) -> str:
    """Return the canonical uppercase identifier for an external code system."""

    system = str(raw_system if raw_system not in (None, "") else (default or "")).strip().upper()
    return CODE_SYSTEM_ALIASES.get(system, system)


def normalize_code(raw_code: Any) -> str:
    """Return a stripped uppercase representation of an external code."""

    return str(raw_code or "").strip().upper()


def catalog_code_system_lookup_values(raw_system: Any) -> tuple[str, ...]:
    """Return canonical and legacy persisted names for one code system."""

    canonical = normalize_code_system(raw_system)
    if not canonical:
        return ()
    aliases = tuple(
        alias
        for alias, alias_canonical in CODE_SYSTEM_ALIASES.items()
        if alias_canonical == canonical and alias != canonical
    )
    return (canonical, *aliases)


def canonical_catalog_code(code_system: str, raw_code: Any) -> str:
    """Normalize one code according to its catalog system's width rules."""

    code = normalize_code(raw_code)
    digits = "".join(ch for ch in code if ch.isdigit())
    if code_system == "RC" and digits:
        return digits.zfill(4)
    if code_system == "POS" and digits:
        return digits.zfill(2)
    if code_system == "MS_DRG" and digits:
        return digits.zfill(3)
    if code_system in {"ICD10CM", "ICD10PCS"}:
        return code.replace(".", "")
    return code


def catalog_code_lookup_values(code_system: Any, raw_code: Any) -> tuple[str, ...]:
    """Return compatible persisted forms for an externally supplied code."""

    system = normalize_code_system(code_system)
    canonical = canonical_catalog_code(system, raw_code) if system else normalize_code(raw_code)
    if not canonical:
        return ()
    values = [canonical]
    if system == "RC":
        normalized_raw = normalize_code(raw_code)
        if normalized_raw and normalized_raw not in values:
            values.append(normalized_raw)
        digits = "".join(ch for ch in canonical if ch.isdigit())
        while len(digits) > 1 and digits.startswith("0"):
            digits = digits[1:]
            if digits not in values:
                values.append(digits)
    return tuple(values)


def restricted_terminology_public_enabled() -> bool:
    """Return whether public responses may expose restricted terminology."""

    return str(os.getenv("HLTHPRT_PUBLIC_RESTRICTED_TERMINOLOGIES") or "").strip().lower() in {"1", "true", "yes"}


def is_restricted_terminology_system(code_system: Any) -> bool:
    """Return whether a code system carries restricted terminology."""

    return normalize_code_system(code_system) in RESTRICTED_TERMINOLOGY_CODE_SYSTEMS


def equivalent_external_procedure_pairs(system: str, code: str) -> set[tuple[str, str]]:
    """Return equivalent external system/code pairs for shared procedure codes."""

    if system not in EQUIVALENT_PROCEDURE_CODE_SYSTEMS or len(code) != 5 or not code.isalnum():
        return set()
    if code.isdigit():
        systems = {"CPT", "HCPCS"}
    elif len(code) == 5 and code.startswith("D") and code[1:].isdigit():
        systems = {"CDT", "HCPCS"}
    else:
        systems = {system}
    return {(candidate, code) for candidate in systems}
