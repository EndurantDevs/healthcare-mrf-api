# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit generations excluded from ordinary Florida payload retention."""

import asyncio

from sqlalchemy import text


async def _remove_retained_directories(artifact_root, run_ids):
    """Drain actual filesystem work before cancellation can release the source lock."""
    from process.florida_mqa_profile import _remove_artifact_run_directories

    deletion = asyncio.create_task(asyncio.to_thread(_remove_artifact_run_directories, artifact_root, run_ids))
    try:
        return await asyncio.shield(deletion)
    except asyncio.CancelledError:
        while not deletion.done():
            try:
                await asyncio.shield(deletion)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not deletion.cancelled():
            deletion.exception()
        raise


async def protected_projection_run_ids(database, schema: str, live_name: str, old_name: str) -> set[str]:
    """Protect live, rollback, and archive-pinned generations when tables exist."""
    protected_run_ids: set[str] = set()
    catalog_entries = await database.all(
        text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname=:schema "
            "AND tablename IN (:live_name,:old_name,'provider_profile_source_pin')"
        ),
        schema=schema,
        live_name=live_name,
        old_name=old_name,
    )
    table_names = {entry._mapping["tablename"] for entry in catalog_entries}
    for projection_name in (live_name, old_name):
        if projection_name not in table_names:
            continue
        generation_entries = await database.all(text(f"SELECT DISTINCT generation_id FROM {schema}.{projection_name}"))
        protected_run_ids.update(
            str(entry._mapping["generation_id"]) for entry in generation_entries if entry._mapping["generation_id"]
        )
    if "provider_profile_source_pin" in table_names:
        pin_entries = await database.all(text(f"SELECT DISTINCT run_id FROM {schema}.provider_profile_source_pin"))
        protected_run_ids.update(str(entry._mapping["run_id"]) for entry in pin_entries if entry._mapping["run_id"])
    return protected_run_ids
