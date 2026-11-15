# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical shared-catalog generation fences and portable source content checks."""

from __future__ import annotations

import json
import re
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process.scoped_catalog_named_storage import qualified
from process.scoped_catalog_retention import canonical_metadata

TABLES = {"code-sets": codes.TABLE, "ms-drg": "ms_drg_result_generation"}
FIELDS = ("local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "published_at")


def logical_generation(value):
    """Keep the complete logical revision while ignoring historical database-local IDs."""
    result_by_field = {key: value[key] for key in FIELDS}
    lineage = UUID(str(result_by_field["local_lineage_id"]))
    generation = result_by_field["local_generation"]
    if not lineage.int or type(generation) is not int or not 0 <= generation < 2**63:
        raise RuntimeError("code-set logical generation is invalid")
    result_by_field["local_lineage_id"] = str(lineage)
    origin = (
        result_by_field["origin_lineage_id"],
        result_by_field["origin_generation"],
        result_by_field["published_at"],
    )
    if any(item is None for item in origin):
        if not all(item is None for item in origin):
            raise RuntimeError("code-set logical origin is incomplete")
    else:
        if not UUID(str(origin[0])).int or type(origin[1]) is not int or not 0 < origin[1] < 2**63:
            raise RuntimeError("code-set logical origin is invalid")
        if isinstance(origin[2], datetime):
            if origin[2].tzinfo is None:
                raise RuntimeError("code-set logical publication time is invalid")
        elif type(origin[2]) is not str or datetime.fromisoformat(origin[2]).tzinfo is None:
            raise RuntimeError("code-set logical publication time is invalid")
        result_by_field["origin_lineage_id"] = str(UUID(str(origin[0])))
    return json.loads(canonical_metadata(result_by_field))


def compatible_expected_generations(current, expected, *, contract, importer):
    """Expand only the v1 CodeSets omission of an untouched MS-DRG singleton."""
    expected_by_field = {name: logical_generation(value) for name, value in expected.items()}
    companion = current.get("ms-drg")
    if (
        contract in {"scoped-catalog-handoff.v1", "scoped-catalog-retained.v1"}
        and importer == "code-sets"
        and set(expected_by_field) == {"code-sets"}
        and set(current) == {"code-sets", "ms-drg"}
        and companion["local_generation"] == 0
        and all(companion[field] is None for field in ("origin_lineage_id", "origin_generation", "published_at"))
    ):
        expected_by_field["ms-drg"] = dict(companion)
    return expected_by_field


async def read_generations(session, schema, *, lock=False, importer="code-sets"):
    """Fence every installed source singleton, including the companion MS-DRG generation."""
    result_by_field = {}
    if importer not in TABLES:
        raise RuntimeError("catalog producer is unsupported")
    for family, table in TABLES.items():
        present = await native._relation_oid(session, schema, table)
        if present is None:
            if family == importer:
                raise RuntimeError("catalog generation is unavailable")
            continue
        statement = "SELECT id," + ",".join(FIELDS) + f" FROM {qualified(schema, table)}"
        rows = (await session.execute(text(statement + (" FOR UPDATE" if lock else "")))).mappings().all()
        if len(rows) != 1 or rows[0]["id"] != 1:
            raise RuntimeError("code-set generation singleton differs")
        result_by_field[family] = logical_generation(rows[0])
    return result_by_field


async def validate_published_content(session, schema, generations):
    """Compare source rows and existing data receipts; legacy physical fields confer no authority."""
    if "code-sets" in generations:
        record_by_field = (
            (
                await session.execute(
                    text(f"SELECT row_count,row_sha256 FROM {qualified(schema, codes.TABLE)} WHERE id=1")
                )
            )
            .mappings()
            .one()
        )
        is_active = generations["code-sets"]["origin_generation"] is not None
        count, digest, _ = await codes._slice_identity(session, schema, complete=is_active)
        expected_receipt = (count, digest) if is_active else (None, None)
        if (record_by_field["row_count"], record_by_field["row_sha256"]) != expected_receipt:
            raise RuntimeError("code-set published source content differs")
    if "ms-drg" in generations:
        from process.ms_drg_result_archive import _content
        from process.ms_drg_result_generation import capture_result

        receipt = await session.scalar(text(f"SELECT receipt FROM {qualified(schema, TABLES['ms-drg'])} WHERE id=1"))
        if generations["ms-drg"]["origin_generation"] is None:
            if receipt is not None:
                raise RuntimeError("untracked companion catalog content is unavailable")
            from process import scoped_catalog_named_storage as storage

            await storage.require_tables(session, schema, await storage.current_models(session, schema))
            return
        current = await capture_result(session, schema)
        if (
            type(receipt) is not dict
            or receipt.get("contract") != current["contract"]
            or receipt.get("content_sha256") != current["content_sha256"]
            or _content(receipt) != _content(current)
        ):
            raise RuntimeError("companion catalog source content or structure differs")


async def publish_generation(session, schema, importer="code-sets", *, include_relationships=False):
    """Advance the selected source revision after validating its complete published content."""
    prior = (await read_generations(session, schema, lock=True, importer=importer))[importer]
    if prior["local_generation"] >= 2**63 - 1:
        raise RuntimeError("code-set generation is exhausted")
    if importer == "ms-drg":
        from process.ms_drg_result_generation import capture_result

        receipt = await capture_result(session, schema)
        receipt["tables"] = [
            {key: field_value for key, field_value in entry.items() if key != "relation_oid"}
            for entry in receipt["tables"]
        ]
        await session.execute(
            text(
                f"UPDATE {qualified(schema, TABLES[importer])} SET local_generation=local_generation+1,"
                "origin_lineage_id=local_lineage_id,origin_generation=local_generation+1,published_at=clock_timestamp(),"
                "include_relationships=:include_relationships,receipt=CAST(:receipt AS jsonb) WHERE id=1"
            ),
            {"include_relationships": include_relationships, "receipt": canonical_metadata(receipt)},
        )
        return (await read_generations(session, schema, importer=importer))[importer]
    count, digest, _ = await codes.scope_receipt(session, schema)
    if type(count) is not int or count < 0 or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise RuntimeError("code-set source content is invalid")
    await session.execute(
        text(
            f"UPDATE {qualified(schema, codes.TABLE)} SET local_generation=local_generation+1,"
            "origin_lineage_id=local_lineage_id,origin_generation=local_generation+1,published_at=clock_timestamp(),"
            "code_catalog_oid=NULL,row_count=:count,row_sha256=:digest WHERE id=1"
        ),
        {"count": count, "digest": digest},
    )
    return (await read_generations(session, schema))["code-sets"]
