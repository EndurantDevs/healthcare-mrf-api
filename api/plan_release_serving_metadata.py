# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small metadata normalizers for immutable plan-release selection."""

from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


def single_text_value(
    rows: Iterable[Mapping[str, Any]],
    field: str,
) -> str | None:
    """Return one shared non-empty text value across release rows."""

    values = {str(row.get(field) or "").strip() for row in rows}
    if len(values) != 1:
        return None
    value = values.pop()
    return value or None


def canonical_published_at(value: Any) -> str | None:
    """Return a timezone-bound serving publication as canonical UTC text."""

    if isinstance(value, dt.datetime):
        published_at = value
    elif isinstance(value, str) and value == value.strip() and value:
        try:
            published_at = dt.datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except ValueError:
            return None
    else:
        return None
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        return None
    return published_at.astimezone(dt.UTC).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def serving_revision_published_at(
    release_rows: Iterable[Mapping[str, Any]],
) -> str | None:
    """Return one canonical shared publication timestamp when available."""

    canonical_values = {
        canonical_published_at(row.get("serving_revision_published_at"))
        for row in release_rows
    }
    if len(canonical_values) != 1 or None in canonical_values:
        return None
    return next(iter(canonical_values))


def has_expected_binding_count(
    release_rows: list[dict[str, Any]],
) -> bool:
    """Return whether every row declares the complete binding count."""

    try:
        expected_counts = {
            int(release_row.get("expected_binding_count"))
            for release_row in release_rows
        }
    except (TypeError, ValueError):
        return False
    return expected_counts == {len(release_rows)}


@dataclass(frozen=True)
class PlanReleaseHeader:
    """Shared immutable metadata repeated by every frozen binding row."""

    serving_revision_id: str
    serving_revision_published_at: str | None
    plan_release_id: str
    healthporta_plan_id: str
    plan_version_id: str | None
    release_month: str
    release_status: str
    binding_set_digest: str
    pricing_projection_id: str | None
    pricing_projection_contract: str | None


def plan_release_header_from_rows(
    requested_release_id: str,
    release_rows: list[dict[str, Any]],
) -> PlanReleaseHeader | None:
    """Validate shared release metadata without rejecting legacy null times."""

    shared_text_by_field = {
        field: single_text_value(release_rows, field)
        for field in (
            "plan_release_id",
            "serving_revision_id",
            "healthporta_plan_id",
            "release_month",
            "release_status",
            "binding_set_digest",
            "pricing_projection_id",
            "pricing_projection_contract",
        )
    }
    projection_id = shared_text_by_field["pricing_projection_id"]
    if projection_id and re.fullmatch(r"[0-9a-f]{64}", projection_id) is None:
        projection_id = None
    projection_contract = (
        shared_text_by_field["pricing_projection_contract"]
        if projection_id is not None
        else None
    )
    plan_version_values = {
        str(release_row.get("plan_version_id") or "").strip()
        for release_row in release_rows
    }
    if (
        shared_text_by_field["plan_release_id"] != requested_release_id
        or not shared_text_by_field["serving_revision_id"]
        or not shared_text_by_field["healthporta_plan_id"]
        or not shared_text_by_field["release_month"]
        or shared_text_by_field["release_status"] != "published"
        or not shared_text_by_field["binding_set_digest"]
        or len(plan_version_values) != 1
        or not has_expected_binding_count(release_rows)
    ):
        return None
    return PlanReleaseHeader(
        serving_revision_id=shared_text_by_field["serving_revision_id"],
        serving_revision_published_at=serving_revision_published_at(release_rows),
        plan_release_id=shared_text_by_field["plan_release_id"],
        healthporta_plan_id=shared_text_by_field["healthporta_plan_id"],
        plan_version_id=plan_version_values.pop() or None,
        release_month=shared_text_by_field["release_month"],
        release_status=shared_text_by_field["release_status"],
        binding_set_digest=shared_text_by_field["binding_set_digest"],
        pricing_projection_id=projection_id,
        pricing_projection_contract=projection_contract,
    )
