# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL expression helpers for provider quality materialization."""

from __future__ import annotations


def _provider_class_case_sql(entity_type_expr: str, enrichment_alias: str) -> str:
    return f"""
        CASE
            WHEN {entity_type_expr} = 1 THEN 'clinician'
            WHEN COALESCE({enrichment_alias}.has_hospital_enrollment, FALSE)
              OR COALESCE({enrichment_alias}.has_hha_enrollment, FALSE)
              OR COALESCE({enrichment_alias}.has_hospice_enrollment, FALSE)
              OR COALESCE({enrichment_alias}.has_fqhc_enrollment, FALSE)
              OR COALESCE({enrichment_alias}.has_rhc_enrollment, FALSE)
              OR COALESCE({enrichment_alias}.has_snf_enrollment, FALSE)
            THEN 'facility'
            WHEN {entity_type_expr} = 2 THEN 'organization'
            ELSE 'unknown'
        END::varchar
    """


_US_STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "PUERTO RICO": "PR",
    "GUAM": "GU",
    "AMERICAN SAMOA": "AS",
    "NORTHERN MARIANA ISLANDS": "MP",
    "COMMONWEALTH OF THE NORTHERN MARIANA ISLANDS": "MP",
    "US VIRGIN ISLANDS": "VI",
    "U.S. VIRGIN ISLANDS": "VI",
    "VIRGIN ISLANDS": "VI",
}


def _state_code_sql(expr: str) -> str:
    normalized = f"UPPER(NULLIF(BTRIM(COALESCE({expr}, '')), ''))"
    mapping_cases = "\n".join(
        f"            WHEN {normalized} = '{name}' THEN '{code}'"
        for name, code in _US_STATE_NAME_TO_CODE.items()
    )
    return f"""
        CASE
            WHEN {normalized} IS NULL THEN NULL
            WHEN LENGTH({normalized}) = 2 THEN {normalized}
{mapping_cases}
            ELSE NULL
        END
    """
