# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve the CMS Doctors clinician/enrollment/group/address source grain."""

from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import text

from db.models import CMSDoctorGroupSite, db
from process.cms_doctors_education import open_doctors_csv
from process.cms_doctors_rows import doctor_address_row
from process.control_cancel import raise_if_cancelled
from process.ext.utils import make_class, push_objects
from process.provider_directory_profile import is_valid_npi

REQUIRED_HEADERS = {
    "npi",
    "ind_enrl_id",
    "org_pac_id",
    "adrs_id",
    "facility name",
    "num_org_mem",
}


def doctor_group_site_row(source_row: dict, row_number: int, manifest: dict) -> dict:
    """Keep blank group identity and full address ID without inferring membership dates."""
    fields_by_name = {str(key).strip().lower(): field_value for key, field_value in source_row.items()}
    npi = str(fields_by_name.get("npi") or "").strip()
    if not is_valid_npi(npi):
        raise ValueError(f"cms_group_site_invalid_npi:row={row_number}")
    member_count = str(fields_by_name.get("num_org_mem") or "").strip()
    if member_count and (not member_count.isascii() or not member_count.isdigit()):
        raise ValueError(f"cms_group_site_invalid_member_count:row={row_number}")
    observed_at = datetime.fromisoformat(manifest["downloaded_at"])
    address = doctor_address_row(source_row, observed_at)
    raw_fields_by_name = {
        key: fields_by_name.get(key)
        for key in ("npi", "ind_enrl_id", "org_pac_id", "adrs_id", "facility name", "num_org_mem", "cred")
    }
    return {
        "row_number": row_number,
        "npi": int(npi),
        "ind_enrl_id": str(fields_by_name.get("ind_enrl_id") or "").strip() or None,
        "org_pac_id": str(fields_by_name.get("org_pac_id") or "").strip() or None,
        "adrs_id": str(fields_by_name.get("adrs_id") or "").strip() or None,
        "facility_name": str(fields_by_name.get("facility name") or "").strip() or None,
        "num_org_mem": int(member_count) if member_count else None,
        "address_checksum": address["address_checksum"] if address else None,
        "generation_id": manifest["generation_id"],
        "source_json": {
            **manifest,
            "row_number": row_number,
            "raw_fields": raw_fields_by_name,
        },
        "observed_at": observed_at,
        "membership_start_at": None,
        "membership_end_at": None,
    }


async def discard_group_site_stage(ctx) -> None:
    """Drop only the group/site stage owned by this import run."""
    context = ctx.get("context") or {}
    if context.get("group_site_stage_owned"):
        stage = make_class(CMSDoctorGroupSite, ctx["import_date"])
        await db.status(f"DROP TABLE IF EXISTS {stage.__table__.schema}.{stage.__tablename__}")
        context.pop("group_site_stage_owned", None)


async def import_group_site_rows(source_path, ctx, task, manifest: dict) -> dict:
    """Stage every physical source row independently of address deduplication."""
    stage = make_class(CMSDoctorGroupSite, ctx["import_date"])
    await db.create_table(stage.__table__, checkfirst=False)
    ctx.setdefault("context", {})["group_site_stage_owned"] = True
    batch_rows = []
    source_rows = 0
    row_limit = int(os.getenv("HLTHPRT_CMS_DOCTORS_TEST_ROWS", "5000")) if ctx["context"].get("test_mode") else None
    try:
        with open_doctors_csv(source_path) as reader:
            headers = [str(name).strip().lower() for name in reader.fieldnames or []]
            if len(headers) != len(set(headers)) or not REQUIRED_HEADERS <= set(headers):
                raise ValueError("cms_group_site_required_headers_missing_or_duplicate")
            for source_rows, source_row in enumerate(reader, start=1):
                if None in source_row or any(field_value is None for field_value in source_row.values()):
                    raise ValueError(f"cms_group_site_row_width_changed:row={source_rows}")
                batch_rows.append(doctor_group_site_row(source_row, source_rows, manifest))
                if len(batch_rows) >= 5_000:
                    await raise_if_cancelled(ctx, task)
                    await push_objects(batch_rows, stage)
                    batch_rows.clear()
                if row_limit is not None and source_rows >= row_limit:
                    break
            await raise_if_cancelled(ctx, task)
            await push_objects(batch_rows, stage)
        return {"source_rows": source_rows, "generation_id": manifest["generation_id"]}
    except BaseException:
        await discard_group_site_stage(ctx)
        raise


async def validate_group_site_stage(import_date: str, schema: str, receipt: dict) -> None:
    """Confirm one staged row per source row under the expected generation."""
    stage = make_class(CMSDoctorGroupSite, import_date)
    counts = await db.first(
        text(
            f"SELECT count(*) AS rows, count(DISTINCT generation_id) AS generations, "
            f"min(generation_id) AS generation_id FROM {schema}.{stage.__tablename__}"
        )
    )
    values = counts._mapping
    if (
        values["rows"] != receipt["source_rows"]
        or values["generations"] != 1
        or values["generation_id"] != receipt["generation_id"]
    ):
        raise RuntimeError("cms_group_site_stage_incomplete")


async def swap_group_site_stage(import_date: str, schema: str) -> None:
    """Swap inside the caller's address/education/generation transaction."""
    stage = make_class(CMSDoctorGroupSite, import_date)
    table = CMSDoctorGroupSite.__tablename__
    await db.status(f"DROP TABLE IF EXISTS {schema}.{table}_old")
    await db.status(f"ALTER TABLE IF EXISTS {schema}.{table} RENAME TO {table}_old")
    await db.status(f"ALTER TABLE {schema}.{stage.__tablename__} RENAME TO {table}")
    for index_name in ("npi", "org", "adrs"):
        await db.status(f"DROP INDEX IF EXISTS {schema}.{table}_idx_{index_name}_old")
        await db.status(
            f"ALTER INDEX IF EXISTS {schema}.{table}_idx_{index_name} RENAME TO {table}_idx_{index_name}_old"
        )
        await db.status(
            f"ALTER INDEX IF EXISTS {schema}.{stage.__tablename__}_idx_{index_name} RENAME TO {table}_idx_{index_name}"
        )
