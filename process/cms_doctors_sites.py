# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare exact Doctors site IDs without publishing a partial source family."""

from db.models import db
from process.control_cancel import raise_if_cancelled


async def bind_cms_doctors_sites(ctx, import_date, schema, receipt):
    """Prepare the complete private site map for the same final family cut."""
    from process.cms_doctors_identity_candidate import prepare_doctors_identity_kind

    context = ctx.get("context") or {}
    if not context.get("group_site_stage_owned"):
        raise RuntimeError("cms_group_site_binding_stage_not_owned")
    task_by_field = {"run_id": context.get("control_run_id") or ctx.get("control_run_id")}

    async def _cancel_check():
        await raise_if_cancelled(ctx, task_by_field)

    await _cancel_check()
    return await prepare_doctors_identity_kind(
        db, ctx, import_date, schema, receipt, kind="site", cancel_check=_cancel_check
    )
