# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare source-local organization IDs before CMS Doctors publication."""

from sqlalchemy import exists, select, text
from sqlalchemy.dialects import postgresql

from db.models import CMSDoctorGroupSite, ProviderDirectoryCMSDoctorsGroupBinding, db
from process.cms_doctors_groups import validate_group_site_stage
from process.control_cancel import raise_if_cancelled
from process.ext.utils import make_class
from process.provider_directory_entity_identity import bind_cms_doctors_group_batch

GROUP_BINDING_BATCH_SIZE = 100


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


async def _read_group_page(stage_table, stage_oid, after_pac_id):
    """Read at most one distinct PAC batch using the stage's organization index."""
    pac_column = stage_table.c.org_pac_id
    statement = select(pac_column).where(pac_column.is_not(None), pac_column != "")
    if after_pac_id is not None:
        statement = statement.where(pac_column > after_pac_id)
    statement = statement.distinct().order_by(pac_column).limit(GROUP_BINDING_BATCH_SIZE)
    async with db.transaction() as session:
        await _lock_group_stage(session, stage_table, stage_oid)
        return list((await session.scalars(statement)).all())


async def _require_complete_group_bindings(stage_table, stage_oid, import_date, schema, receipt):
    """Revalidate the owned stage and reject any nonblank PAC ID lacking an exact binding."""
    binding = ProviderDirectoryCMSDoctorsGroupBinding.__table__
    pac_column = stage_table.c.org_pac_id
    matching_binding = select(binding.c.org_pac_id).where(binding.c.org_pac_id.collate("C") == pac_column.collate("C"))
    missing_binding = (
        select(pac_column)
        .where(
            pac_column.is_not(None),
            pac_column != "",
            ~exists(matching_binding),
        )
        .limit(1)
    )
    async with db.transaction() as session:
        await _lock_group_stage(session, stage_table, stage_oid)
        await validate_group_site_stage(import_date, schema, receipt)
        if await session.scalar(missing_binding) is not None:
            raise RuntimeError("cms_group_site_organization_binding_incomplete")


async def bind_group_site_organizations(ctx, import_date, schema, receipt):
    """Commit bounded, replay-safe identity batches before the serving-table cutover.

    Call only after full source/artifact validation. Each batch owns its read
    and write transactions; cancellation can retain IDs but never publishes a
    stage. Source-local bindings do not imply any FHIR resource relationship.
    """
    context = ctx.get("context") or {}
    if not context.get("group_site_stage_owned"):
        raise RuntimeError("cms_group_site_binding_stage_not_owned")
    task_by_key = {"run_id": context.get("control_run_id") or ctx.get("control_run_id")}
    await raise_if_cancelled(ctx, task_by_key)
    stage_table = make_class(CMSDoctorGroupSite, import_date).__table__
    if stage_table.schema != schema:
        raise RuntimeError("cms_group_site_binding_schema_mismatch")
    async with db.transaction() as session:
        stage_oid = await _lock_group_stage(session, stage_table)
        await validate_group_site_stage(import_date, schema, receipt)
    after_pac_id = None
    bound_groups = 0
    while True:
        await raise_if_cancelled(ctx, task_by_key)
        org_pac_ids = await _read_group_page(stage_table, stage_oid, after_pac_id)
        if not org_pac_ids:
            break
        async with db.session() as session:
            await bind_cms_doctors_group_batch(session, org_pac_ids=org_pac_ids)
            await raise_if_cancelled(ctx, task_by_key)
        bound_groups += len(org_pac_ids)
        after_pac_id = org_pac_ids[-1]
    await _require_complete_group_bindings(stage_table, stage_oid, import_date, schema, receipt)
    await raise_if_cancelled(ctx, task_by_key)
    return bound_groups
