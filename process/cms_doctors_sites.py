# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare exact CMS Doctors site IDs before clinician-family publication."""

from sqlalchemy import exists, select

from db.models import CMSDoctorGroupSite, ProviderDirectoryCMSDoctorsSiteBinding, db
from process.cms_doctors_groups import validate_group_site_stage
from process.cms_doctors_organizations import GROUP_BINDING_BATCH_SIZE, _lock_group_stage
from process.control_cancel import raise_if_cancelled
from process.ext.utils import make_class
from process.provider_directory_entity_identity import bind_cms_doctors_site_batch


async def _read_site_page(stage_table, stage_oid, after_adrs_id):
    adrs_column = stage_table.c.adrs_id
    statement = select(adrs_column).where(adrs_column.is_not(None), adrs_column != "")
    if after_adrs_id is not None:
        statement = statement.where(adrs_column > after_adrs_id)
    statement = statement.distinct().order_by(adrs_column).limit(GROUP_BINDING_BATCH_SIZE)
    async with db.transaction() as session:
        await _lock_group_stage(session, stage_table, stage_oid, identifier_column="adrs_id")
        return list((await session.scalars(statement)).all())


async def _require_complete_site_bindings(stage_table, stage_oid, import_date, schema, receipt):
    binding = ProviderDirectoryCMSDoctorsSiteBinding.__table__
    adrs_column = stage_table.c.adrs_id
    matching_binding = select(binding.c.adrs_id).where(binding.c.adrs_id.collate("C") == adrs_column.collate("C"))
    missing_binding = (
        select(adrs_column).where(adrs_column.is_not(None), adrs_column != "", ~exists(matching_binding)).limit(1)
    )
    async with db.transaction() as session:
        await _lock_group_stage(session, stage_table, stage_oid, identifier_column="adrs_id")
        await validate_group_site_stage(import_date, schema, receipt)
        if await session.scalar(missing_binding) is not None:
            raise RuntimeError("cms_group_site_site_binding_incomplete")


async def bind_cms_doctors_sites(ctx, import_date, schema, receipt):
    """Bind source address IDs in bounded batches without publishing partial stages."""
    context_by_field = ctx.get("context") or {}
    if not context_by_field.get("group_site_stage_owned"):
        raise RuntimeError("cms_group_site_binding_stage_not_owned")
    task_by_field = {"run_id": context_by_field.get("control_run_id") or ctx.get("control_run_id")}
    await raise_if_cancelled(ctx, task_by_field)
    stage_table = make_class(CMSDoctorGroupSite, import_date).__table__
    if stage_table.schema != schema:
        raise RuntimeError("cms_group_site_binding_schema_mismatch")
    async with db.transaction() as session:
        stage_oid = await _lock_group_stage(session, stage_table, identifier_column="adrs_id")
        await validate_group_site_stage(import_date, schema, receipt)
    after_adrs_id = None
    bound_sites = 0
    while True:
        await raise_if_cancelled(ctx, task_by_field)
        adrs_ids = await _read_site_page(stage_table, stage_oid, after_adrs_id)
        if not adrs_ids:
            break
        async with db.session() as session:
            await bind_cms_doctors_site_batch(session, adrs_ids=adrs_ids)
            await raise_if_cancelled(ctx, task_by_field)
        bound_sites += len(adrs_ids)
        after_adrs_id = adrs_ids[-1]
    await _require_complete_site_bindings(stage_table, stage_oid, import_date, schema, receipt)
    await raise_if_cancelled(ctx, task_by_field)
    return bound_sites
