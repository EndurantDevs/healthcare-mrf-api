# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""FHIR-reference and evidence-currentness SQL for provider profiles."""

from __future__ import annotations

import re


def fhir_reference_resource_id_sql(
    reference_sql: str,
    resource_type: str,
) -> str:
    """Normalize one bare, relative, absolute, or versioned FHIR reference."""
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", resource_type):
        raise ValueError("invalid FHIR resource type")
    return (
        "CASE "
        f"WHEN BTRIM({reference_sql}) ~ '^[A-Za-z0-9.-]{{1,64}}$' "
        f"THEN BTRIM({reference_sql}) "
        "ELSE substring("
        f"BTRIM({reference_sql}) FROM "
        f"'(?i)(?:^|/){resource_type}/([A-Za-z0-9.-]{{1,64}})"
        "(?:/_history/[A-Za-z0-9.-]{1,64})?/?(?:[?#].*)?$'"
        ") END"
    )


def current_profile_evidence_sql(alias: str = "") -> str:
    """Return the conservative currentness predicate for profile evidence."""
    prefix = f"{alias}." if alias else ""
    active = f"{prefix}active"
    effective_start = f"{prefix}effective_start"
    effective_end = f"{prefix}effective_end"
    profile_as_of = "CAST(:profile_as_of AS varchar)"
    return f"""(
        {active} IS DISTINCT FROM FALSE
        AND (
            {effective_start} IS NULL
            OR {effective_start} !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
            OR LEFT({effective_start}, 10) <= {profile_as_of}
        )
        AND (
            {effective_end} IS NULL
            OR {effective_end} !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
            OR LEFT({effective_end}, 10) >= {profile_as_of}
        )
    )"""
