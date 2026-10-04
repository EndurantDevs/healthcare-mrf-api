# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Load and compose source-reported CMS education and credentials."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping, Sequence

from sqlalchemy import text

from api.provider_profile_composer_parts import _empty_profile, _source_generation_ids
from api.provider_profile_public_facts import _FHIR_CATEGORY_BY_FACT
from api.provider_profile_snapshot import snapshot_cms_serving_generation
from db.models import CMSDoctorEducation, CMSDoctorGroupSite, db
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
    return _reported_fact(
        education_row, "education_history", {"institution": institution, "graduation_year": graduation_year},
        "; ".join(display_parts), education_row["education_key"], quality_flags,
    )


def _reported_fact(source_row, fact_type, value, display, record_key, quality_flags):
    """Reuse source assertions without inferring credential verification or dates."""
    source_record_id = f"cms-doctors:{source_row['generation_id']}:{record_key}"
    assertion_by_field = {
        "source_kind": CMS_SOURCE_KIND,
        "assertion_type": "cms_reported",
        "verification_status": "not_independently_verified",
        "quality_flags": quality_flags,
    }
    return {
        "type": fact_type,
        "display": display,
        "value": value,
        "logical_fact_key": record_key,
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
    """Read both CMS assertion families in the caller's fenced profile snapshot."""
    schema = CMSDoctorEducation.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    table = f"{schema}.{CMSDoctorEducation.__tablename__}"
    education_rows = []
    if await db.scalar(text("SELECT to_regclass(:table)"), table=table) is not None:
        education_rows = await db.all(text(
            f"SELECT education_key, medical_school, graduation_year, generation_id, source_json "
            f"FROM {table} WHERE npi = :npi ORDER BY education_key"
        ), npi=npi)
    group_table = f"{schema}.{CMSDoctorGroupSite.__tablename__}"
    credential_rows = []
    if await db.scalar(text("SELECT to_regclass(:table)"), table=group_table) is not None:
        credential_rows = await db.all(text(
            f"SELECT row_number, generation_id, source_json FROM {group_table} WHERE npi = :npi "
            "AND json_typeof(source_json->'raw_fields'->'cred') = 'string' "
            "AND nullif(btrim(source_json->'raw_fields'->>'cred'), '') IS NOT NULL ORDER BY row_number"
        ), npi=npi)
    if not education_rows and not credential_rows:
        return None
    return _cms_projection(
        npi, [row._mapping for row in education_rows], [row._mapping for row in credential_rows],
    )


def _cms_projection(npi: int, education_rows: list[Mapping], credential_rows: Sequence[Mapping] = ()) -> dict | None:
    """Bind one provider's facts and evidence to its published CMS generation."""
    credential_rows = [
        source_row for source_row in credential_rows
        if isinstance(source_row["source_json"].get("raw_fields", {}).get("cred"), str)
        and source_row["source_json"]["raw_fields"]["cred"].strip()
    ]
    source_rows = [*education_rows, *credential_rows]
    if not source_rows:
        return None
    generation_ids = {source_row["generation_id"] for source_row in source_rows}
    if len(generation_ids) != 1:
        raise RuntimeError("cms_education_profile_generation_mixed")
    generation_id = next(iter(generation_ids))
    if any(source_row["source_json"].get("generation_id") != generation_id for source_row in source_rows):
        raise RuntimeError("cms_education_profile_generation_mismatch")
    profile_items = [_education_fact(education_row) for education_row in education_rows]
    for source_row in credential_rows:
        credential = source_row["source_json"]["raw_fields"]["cred"].strip()
        profile_items.append(_reported_fact(
            source_row, "credential", credential, f"Reported credential: {credential}", f"credential:{source_row['row_number']}", [],
        ))
    source_by_field = source_rows[0]["source_json"]
    serving_generation = snapshot_cms_serving_generation(CMSDoctorEducation.__table__.schema or "mrf")
    composition_generation = generation_id
    if serving_generation is not None:
        composition_generation = hashlib.sha256(
            json.dumps(
                {"source_generation": generation_id, "serving_generation": serving_generation},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
    return {
        "generation_id": composition_generation,
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
                {**source_row["source_json"], "source_record_id": profile_item["source_record_id"]}
                for source_row, profile_item in zip(source_rows, profile_items, strict=True)
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
    for item in cms_projection["items"]:
        category = "education" if item["type"] == "education_history" else _FHIR_CATEGORY_BY_FACT[item["type"]]
        group = profile_by_field.setdefault("categories", {}).setdefault(category, {"items": []})
        group["items"].append(copy.deepcopy(item))
        group["availability"] = "available"
    profile_by_field.setdefault("sources", []).append(copy.deepcopy(cms_projection["source"]))
    profile_by_field.setdefault("important_context", []).append(
        "CMS medical school and graduation years are source-reported. Future years reflect the acquisition date; "
        "they establish neither completed education nor student status. Credentials are reported labels, "
        "not license or board verification. No clinical experience is inferred."
    )
    projection_by_field["source_generations"] = {
        **_source_generation_ids(state_projection, state_projection.get("profile") if state_projection else None, None),
        CMS_SOURCE_KIND: cms_projection["generation_id"],
    }
    projection_by_field["cms_evidence"] = copy.deepcopy(cms_projection["evidence"])
    return projection_by_field
