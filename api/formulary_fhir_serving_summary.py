# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Response shaping for FHIR formulary consensus summaries."""

from __future__ import annotations

from typing import Any

from api.formulary_fhir_serving_common import FHIR_SOURCE_TYPE
from api.formulary_fhir_serving_common import dataset_payload
from api.formulary_fhir_serving_common import json_value
from api.formulary_fhir_serving_common import upstream_payload
from api.tier_utils import normalize_drug_tier_slug


def tier_summary(
    tier_counts: list[tuple[str, int]],
) -> list[dict[str, Any]]:
    """Shape consensus tier counts for the public response."""

    return [
        {
            "tier_slug": normalize_drug_tier_slug(tier_label),
            "tier_label": tier_label,
            "drug_count": drug_count,
        }
        for tier_label, drug_count in tier_counts
    ]


def requirement_summary(
    summary_by_name: dict[str, int],
    prefix: str,
    true_name: str,
    false_name: str,
) -> dict[str, int]:
    """Shape true, false, and conflicting consensus counts."""

    return {
        true_name: summary_by_name[f"{prefix}_true"],
        false_name: summary_by_name[f"{prefix}_false"],
        "conflicting_or_unknown": summary_by_name[f"{prefix}_unknown"],
    }


def summary_payload(
    formulary_id: str,
    selected_alias: str | None,
    header_by_field: dict[str, Any],
    summary_by_name: dict[str, int],
    tier_counts: list[tuple[str, int]],
) -> dict[str, Any]:
    """Shape source metadata and consensus statistics for one formulary."""

    return {
        "formulary_id": formulary_id,
        "formulary_uri": formulary_id,
        "source_type": FHIR_SOURCE_TYPE,
        "source_id": header_by_field.get("source_id"),
        "plan_id": None,
        "year": None,
        "source_plan_identifier": selected_alias,
        "upstream": upstream_payload(header_by_field),
        "coverage_plan": json_value(
            header_by_field.get("metadata_json"),
            {},
        ),
        "dataset": dataset_payload(header_by_field),
        "total_drugs": summary_by_name["total_drugs"],
        "tiers": tier_summary(tier_counts),
        "authorization_requirements": requirement_summary(
            summary_by_name,
            "prior",
            "required",
            "not_required",
        ),
        "step_therapy": requirement_summary(
            summary_by_name,
            "step",
            "required",
            "not_required",
        ),
        "quantity_limits": requirement_summary(
            summary_by_name,
            "quantity",
            "has_limit",
            "no_limit",
        ),
        "pharmacy_types": [],
    }
