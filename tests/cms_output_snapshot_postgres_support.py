# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close native fixture outputs through the original independently checked admin cut."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.plan_release_storage import storage_relation
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS
from process.provider_directory_output_snapshot_provision import provision_output_snapshot_storage
from process.provider_directory_projection_stage import _copy_driver


async def _output_census(connection, policy):
    statement = " UNION ALL ".join(
        f"SELECT '{name}'::text, count(*)::bigint FROM {storage_relation(policy.schema, name)}" for name in OUTPUT_ROOTS
    )
    return tuple(tuple(row) for row in await connection.fetch(statement))


async def provision_native_outputs(component):
    """Preserve output data through the actual native permission transition."""
    async with async_sessionmaker(component.admin_engine)() as session, session.begin():
        await session.execute(text("SELECT 1"))
        driver = await _copy_driver(session)
        before = await _output_census(driver, component.policy)
        receipts = await provision_output_snapshot_storage(session, component.policy)
        assert len(receipts) == len(OUTPUT_ROOTS) and {receipt.target for receipt in receipts} == set(OUTPUT_ROOTS)
        assert await _output_census(driver, component.policy) == before
