# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare source-local organization IDs before CMS Doctors publication."""

from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from db.models import db
from process.control_cancel import raise_if_cancelled


async def _lock_group_stage(session, stage_table, expected_oid=None, *, identifier_column="org_pac_id"):
    """Fence each short read against stage replacement and lossy identifier collation."""
    if identifier_column not in {"org_pac_id", "adrs_id"}:
        raise ValueError("cms_group_site_binding_column_invalid")
    qualified_name = postgresql.dialect().identifier_preparer.format_table(stage_table)
    await session.execute(text(f"LOCK TABLE {qualified_name} IN SHARE MODE"))
    stage_identity = (
        await session.execute(
            text(
                "SELECT attribute.attrelid::bigint, collation_info.collisdeterministic "
                "FROM pg_catalog.pg_attribute AS attribute "
                "JOIN pg_catalog.pg_collation AS collation_info ON collation_info.oid=attribute.attcollation "
                "WHERE attribute.attrelid=to_regclass(:stage) AND attribute.attname=:identifier_column "
                "AND NOT attribute.attisdropped"
            ),
            {"stage": qualified_name, "identifier_column": identifier_column},
        )
    ).one()
    if not stage_identity[1] or (expected_oid is not None and stage_identity[0] != expected_oid):
        raise RuntimeError("cms_group_site_binding_stage_changed")
    return stage_identity[0]


async def bind_group_site_organizations(ctx, import_date, schema, receipt):
    """Prepare the complete private group map without publishing canonical IDs."""
    from process.cms_doctors_identity_candidate import prepare_doctors_identity_kind

    context = ctx.get("context") or {}
    if not context.get("group_site_stage_owned"):
        raise RuntimeError("cms_group_site_binding_stage_not_owned")
    task_by_key = {"run_id": context.get("control_run_id") or ctx.get("control_run_id")}

    async def _cancel_check():
        await raise_if_cancelled(ctx, task_by_key)

    await _cancel_check()
    return await prepare_doctors_identity_kind(
        db, ctx, import_date, schema, receipt, kind="group", cancel_check=_cancel_check
    )
