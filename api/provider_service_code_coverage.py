# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Coverage metadata for aggregate and public provider-service code counts."""

from __future__ import annotations

from typing import Any


def provider_service_code_coverage(
    source_distinct_code_count: Any,
    published_detail_code_count: int,
) -> dict[str, Any]:
    """Explain CMS aggregate-versus-detail service-code coverage."""

    try:
        source_count_value = (
            None
            if source_distinct_code_count is None
            else float(source_distinct_code_count)
        )
    except (TypeError, ValueError):
        source_count_value = None
    published_count = max(int(published_detail_code_count), 0)
    if source_count_value is None:
        return {
            "source_distinct_code_count": None,
            "published_detail_code_count": published_count,
            "unpublished_detail_code_count": None,
            "detail_coverage_ratio": None,
            "complete": None,
            "status": "unknown",
            "limitation": "source_aggregate_code_count_unavailable",
        }

    source_count = max(int(source_count_value), 0)
    if published_count > source_count:
        status = "inconsistent"
        limitation = "published_detail_exceeds_source_aggregate"
        is_complete = False
    elif published_count < source_count:
        status = "partial"
        limitation = "cms_provider_service_detail_privacy_suppression"
        is_complete = False
    else:
        status = "complete"
        limitation = None
        is_complete = True

    return {
        "source_distinct_code_count": source_count,
        "published_detail_code_count": published_count,
        "unpublished_detail_code_count": max(source_count - published_count, 0),
        "detail_coverage_ratio": (
            round(min(published_count / source_count, 1.0), 6)
            if source_count
            else (1.0 if published_count == 0 else None)
        ),
        "complete": is_complete,
        "status": status,
        "limitation": limitation,
    }


def add_provider_service_summary(
    provider_payload: dict[str, Any],
    published_detail_code_count: Any,
    location_count: Any,
) -> None:
    """Attach provider-service counts and their public-detail coverage."""

    published_count = int(published_detail_code_count or 0)
    provider_payload["summary"] = {
        "service_count": published_count,
        "location_count": int(location_count or 0),
    }
    provider_payload["service_code_coverage"] = provider_service_code_coverage(
        provider_payload.get("total_reported_service_codes"), published_count
    )
