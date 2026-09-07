# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire CMS education independently of practice-address completeness."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import zipfile
from contextlib import contextmanager
from datetime import datetime
from io import TextIOWrapper

from sqlalchemy import text

from db.models import CMSDoctorEducation, db
from process.control_cancel import raise_if_cancelled
from process.ext.utils import make_class, push_objects
from process.provider_directory_profile import is_valid_npi

CMS_EDUCATION_SOURCE_KEY = "cms-doctors"
CMS_EDUCATION_DATASET_ID = "mj5m-pzi6"
MIN_EDUCATION_ROWS = 10_000
MIN_EDUCATION_PUBLISH_RATIO = 0.80


@contextmanager
def open_doctors_csv(source_path):
    """Read one complete CSV distribution, rejecting ambiguous ZIP contents."""
    if str(source_path).lower().endswith(".zip"):
        with zipfile.ZipFile(source_path) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if len(csv_names) != 1:
                raise ValueError("Expected exactly one CSV inside the CMS Doctors ZIP")
            with archive.open(csv_names[0]) as raw_file:
                with TextIOWrapper(raw_file, encoding="utf-8-sig", newline="") as csv_file:
                    yield csv.DictReader(csv_file, strict=True)
    else:
        with open(source_path, encoding="utf-8-sig", newline="") as csv_file:
            yield csv.DictReader(csv_file, strict=True)


def education_source_manifest(
    source_path, source_url: str, dataset_id: str = CMS_EDUCATION_DATASET_ID,
) -> dict:
    """Bind education assertions to the exact downloaded artifact."""
    with open(source_path, "rb") as source_file:
        content_sha256 = hashlib.file_digest(source_file, "sha256").hexdigest()
    manifest_by_name = {
        "source_key": CMS_EDUCATION_SOURCE_KEY,
        "dataset_id": dataset_id,
        "schema_version": "cms-doctor-education/v1",
        "source_url": source_url,
        "content_sha256": content_sha256,
        "downloaded_at": datetime.utcnow().isoformat(),
    }
    manifest_by_name["generation_id"] = hashlib.sha256(
        json.dumps(manifest_by_name, sort_keys=True).encode()
    ).hexdigest()
    return manifest_by_name


def doctor_education_row(source_row, row_number: int, manifest: dict) -> dict | None:
    """Keep school/year assertions without inferring clinical experience."""
    fields_by_name = {str(field).lower(): field_value for field, field_value in source_row.items()}
    npi_text = str(fields_by_name.get("npi") or "").strip()
    school_text = str(fields_by_name.get("med_sch") or "").strip()
    year_text = str(fields_by_name.get("grd_yr") or "").strip()
    imported_at = datetime.fromisoformat(manifest["downloaded_at"])
    if not is_valid_npi(npi_text):
        raise ValueError(f"cms_education_invalid_npi:row={row_number}")
    medical_school = None if school_text.upper() in {"", "OTHER"} else school_text
    graduation_year = None
    if year_text:
        if (
            len(year_text) != 4
            or not year_text.isascii()
            or not year_text.isdigit()
            or int(year_text) < 1800
        ):
            raise ValueError(f"cms_education_invalid_graduation_year:row={row_number}")
        graduation_year = int(year_text)
    if medical_school is None and graduation_year is None:
        return None
    education_key = hashlib.sha256(
        json.dumps([npi_text, medical_school, graduation_year], ensure_ascii=False).encode()
    ).hexdigest()
    return {
        "npi": int(npi_text),
        "education_key": education_key,
        "medical_school": medical_school,
        "graduation_year": graduation_year,
        "generation_id": manifest["generation_id"],
        "source_json": {
            **manifest,
            "row_number": row_number,
            "raw_fields": {
                "NPI": fields_by_name.get("npi"),
                "Med_sch": fields_by_name.get("med_sch"),
                "Grd_yr": fields_by_name.get("grd_yr"),
            },
            "quality_flags": (
                ["graduation_year_in_future"]
                if graduation_year is not None and graduation_year > imported_at.year
                else []
            ),
        },
        "imported_at": imported_at,
    }


async def discard_education_stage(ctx) -> None:
    """Remove only an unpublished education stage created by this worker."""
    context = ctx.get("context") or {}
    if context.get("education_stage_owned"):
        stage_cls = make_class(CMSDoctorEducation, ctx["import_date"])
        await db.status(f"DROP TABLE IF EXISTS {stage_cls.__table__.schema}.{stage_cls.__tablename__}")
        context.pop("education_stage_owned", None)


async def import_doctor_education(
    source_path, source_url: str, ctx, task, dataset_id: str = CMS_EDUCATION_DATASET_ID,
) -> dict:
    """Stage all distinct education tuples before allowing generation publication."""
    stage_cls = make_class(CMSDoctorEducation, ctx["import_date"])
    manifest = education_source_manifest(source_path, source_url, dataset_id)
    await db.create_table(stage_cls.__table__, checkfirst=False)
    ctx.setdefault("context", {})["education_stage_owned"] = True
    try:
        return await _stage_education_rows(source_path, stage_cls, manifest, ctx, task)
    except BaseException:
        await discard_education_stage(ctx)
        raise


async def _stage_education_rows(source_path, stage_cls, manifest, ctx, task) -> dict:
    """Validate rows, preserving distinct assertions across address and batch boundaries."""
    education_rows = []
    seen_education_keys = set()
    source_rows = 0
    row_limit = (
        int(os.getenv("HLTHPRT_CMS_DOCTORS_TEST_ROWS", "5000"))
        if ctx.get("context", {}).get("test_mode") else None
    )
    with open_doctors_csv(source_path) as reader:
        fieldnames = [str(name).lower() for name in reader.fieldnames or []]
        if len(fieldnames) != len(set(fieldnames)):
            raise ValueError("cms_education_duplicate_headers")
        if not {"npi", "med_sch", "grd_yr"} <= set(fieldnames):
            raise ValueError("cms_education_required_headers_missing")
        for source_rows, source_row in enumerate(reader, start=1):
            if None in source_row or any(field_value is None for field_value in source_row.values()):
                raise ValueError(f"cms_education_row_width_changed:row={source_rows}")
            education_row = doctor_education_row(source_row, source_rows, manifest)
            if education_row and education_row["education_key"] not in seen_education_keys:
                seen_education_keys.add(education_row["education_key"])
                education_rows.append(education_row)
            if source_rows % 5_000 == 0:
                await raise_if_cancelled(ctx, task)
                await push_objects(education_rows, stage_cls)
                education_rows.clear()
            if row_limit is not None and source_rows >= row_limit:
                break
        await raise_if_cancelled(ctx, task)
        await push_objects(education_rows, stage_cls)
    return {"source_rows": source_rows, "education_rows": len(seen_education_keys), **manifest}


async def validate_education_stage(import_date: str, schema: str, manifest: dict) -> None:
    """Reject incomplete or sharply smaller generations before either table swap."""
    stage_cls = make_class(CMSDoctorEducation, import_date)
    counts = await db.first(text(
        f"SELECT count(*) AS rows, count(DISTINCT generation_id) AS generations, "
        f"min(generation_id) AS generation_id FROM {schema}.{stage_cls.__tablename__}"
    ))
    counts_by_name = counts._mapping
    if (
        counts_by_name["rows"] < MIN_EDUCATION_ROWS
        or counts_by_name["rows"] != manifest["education_rows"]
        or counts_by_name["generations"] != 1
        or counts_by_name["generation_id"] != manifest["generation_id"]
    ):
        raise RuntimeError("cms_education_stage_incomplete")
    live_table = f"{schema}.{CMSDoctorEducation.__tablename__}"
    if await db.scalar(text("SELECT to_regclass(:table)"), table=live_table) is not None:
        live_rows = int(await db.scalar(text(f"SELECT count(*) FROM {live_table}")) or 0)
        if counts_by_name["rows"] < live_rows * MIN_EDUCATION_PUBLISH_RATIO:
            raise RuntimeError("cms_education_volume_drop")


async def swap_education_stage(import_date: str, schema: str) -> None:
    """Swap only CMS-owned education tables inside the caller's transaction."""
    stage_cls = make_class(CMSDoctorEducation, import_date)
    table = CMSDoctorEducation.__tablename__
    await db.status(f"DROP TABLE IF EXISTS {schema}.{table}_old")
    await db.status(f"ALTER TABLE IF EXISTS {schema}.{table} RENAME TO {table}_old")
    await db.status(f"ALTER TABLE {schema}.{stage_cls.__tablename__} RENAME TO {table}")
