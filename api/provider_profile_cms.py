# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Load and compose source-reported CMS medical education."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping

from sqlalchemy import text

from api.provider_profile_composer_parts import _empty_profile, _source_generation_ids
from db.models import CMSDoctorEducation, db
from process.florida_mqa_profile import PROFILE_SCHEMA_VERSION

CMS_SOURCE_KIND = "cms_doctors"


def _education_fact(education_row: Mapping) -> dict:
    """Render reported school/year values without inferring completed training."""
    source_by_field = education_row["source_json"]
    quality_flags = list(source_by_field.get("quality_flags") or [])
    institution = education_row["medical_school"]
    graduation_year = education_row["graduation_year"]
    display_parts = [f"Reported medical school: {institution}"] if institution else []
    if graduation_year is not None:
        label = "Reported future graduation year" if "graduation_year_in_future" in quality_flags else "Reported graduation year"
        display_parts.append(f"{label}: {graduation_year}")
    source_record_id = f"cms-doctors:{education_row['generation_id']}:{education_row['education_key']}"
    assertion_by_field = {
        "source_kind": CMS_SOURCE_KIND,
        "assertion_type": "cms_reported",
        "verification_status": "not_independently_verified",
        "quality_flags": quality_flags,
    }
    return {
        "type": "education_history",
        "display": "; ".join(display_parts),
        "value": {"institution": institution, "graduation_year": graduation_year},
        "logical_fact_key": education_row["education_key"],
        "source_record_id": source_record_id,
        "source_record_ids": [source_record_id],
        "source_kinds": [CMS_SOURCE_KIND],
        "assertion_type": assertion_by_field["assertion_type"],
        "verification_status": assertion_by_field["verification_status"],
        "assertions": [assertion_by_field],
        "assertion_count": 1,
        "quality_flags": quality_flags,
        "sensitive": False,
        "public_default": True,
    }


async def fetch_cms_education_projection(npi: int) -> dict | None:
    """Read all CMS assertions in one snapshot, tolerating an uninstalled table."""
    schema = CMSDoctorEducation.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    table = f"{schema}.{CMSDoctorEducation.__tablename__}"
    if await db.scalar(text("SELECT to_regclass(:table)"), table=table) is None:
        return None
    education_rows = await db.all(text(
        f"SELECT education_key, medical_school, graduation_year, generation_id, source_json "
        f"FROM {table} WHERE npi = :npi ORDER BY education_key"
    ), npi=npi)
    if not education_rows:
        return None
    return _cms_projection(npi, [education_row._mapping for education_row in education_rows])


def _cms_projection(npi: int, education_rows: list[Mapping]) -> dict:
    """Bind one provider's facts and evidence to its published CMS generation."""
    generation_ids = {education_row["generation_id"] for education_row in education_rows}
    if len(generation_ids) != 1:
        raise RuntimeError("cms_education_profile_generation_mixed")
    generation_id = next(iter(generation_ids))
    if any(education_row["source_json"].get("generation_id") != generation_id for education_row in education_rows):
        raise RuntimeError("cms_education_profile_generation_mismatch")
    profile_items = [_education_fact(education_row) for education_row in education_rows]
    source_by_field = education_rows[0]["source_json"]
    return {
        "generation_id": generation_id,
        "items": profile_items,
        "source": {
            "source_key": source_by_field["source_key"],
            "source_kind": CMS_SOURCE_KIND,
            "agency": "Centers for Medicare & Medicaid Services",
            "jurisdiction": "US",
            **{field: source_by_field[field] for field in (
                "dataset_id", "source_url", "content_sha256", "downloaded_at",
            )},
        },
        "evidence": {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "npi": npi,
            "generation_id": generation_id,
            "records": [
                {**education_row["source_json"], "source_record_id": profile_item["source_record_id"]}
                for education_row, profile_item in zip(education_rows, profile_items, strict=True)
            ],
        },
    }


def merge_cms_education_projection(npi: int, state_projection: Mapping | None, cms_projection: Mapping | None) -> dict | None:
    """Extend the existing projection envelope while preserving state evidence."""
    if cms_projection is None:
        return state_projection
    projection_by_field = copy.deepcopy(state_projection) if state_projection else {"profile": _empty_profile(npi)}
    if not isinstance(projection_by_field.get("profile"), Mapping):
        projection_by_field["profile"] = _empty_profile(npi)
    profile_by_field = projection_by_field["profile"]
    education_group = profile_by_field.setdefault("categories", {}).setdefault("education", {"items": []})
    education_group["items"].extend(copy.deepcopy(cms_projection["items"]))
    education_group["availability"] = "available"
    profile_by_field.setdefault("sources", []).append(copy.deepcopy(cms_projection["source"]))
    profile_by_field.setdefault("important_context", []).append(
        "CMS medical school and graduation years are source-reported. Future years reflect the acquisition date; "
        "they establish neither completed education nor student status. No clinical experience is inferred."
    )
    projection_by_field["source_generations"] = {
        **_source_generation_ids(state_projection, state_projection.get("profile") if state_projection else None, None),
        CMS_SOURCE_KIND: cms_projection["generation_id"],
    }
    projection_by_field["cms_evidence"] = copy.deepcopy(cms_projection["evidence"])
    return projection_by_field
