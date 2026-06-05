# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from typing import Any

INTERNAL_PROCEDURE_CODE_SYSTEM = "HP_PROCEDURE_CODE"
INTERNAL_RX_CODE_SYSTEM = "HP_RX_CODE"
EQUIVALENT_PROCEDURE_CODE_SYSTEMS = {"CPT", "HCPCS", "CDT"}
EXTERNAL_PROCEDURE_CODE_SYSTEMS = {*EQUIVALENT_PROCEDURE_CODE_SYSTEMS, "MS_DRG"}
PROCEDURE_CODE_SYSTEMS = {*EXTERNAL_PROCEDURE_CODE_SYSTEMS, INTERNAL_PROCEDURE_CODE_SYSTEM}
RX_CODE_SYSTEMS = {"NDC", "RXNORM", "RXCUI", INTERNAL_RX_CODE_SYSTEM}

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
    system = str(raw_system if raw_system not in (None, "") else (default or "")).strip().upper()
    return CODE_SYSTEM_ALIASES.get(system, system)


def normalize_code(raw_code: Any) -> str:
    return str(raw_code or "").strip().upper()


def canonical_catalog_code(code_system: str, raw_code: Any) -> str:
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


def equivalent_external_procedure_pairs(system: str, code: str) -> set[tuple[str, str]]:
    if system not in EQUIVALENT_PROCEDURE_CODE_SYSTEMS or len(code) != 5 or not code.isalnum():
        return set()
    if code.isdigit():
        systems = {"CPT", "HCPCS"}
    elif len(code) == 5 and code.startswith("D") and code[1:].isdigit():
        systems = {"CDT", "HCPCS"}
    else:
        systems = {system}
    return {(candidate, code) for candidate in systems}
