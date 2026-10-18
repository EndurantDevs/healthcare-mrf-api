# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close native fixture outputs through the original independently checked admin cut."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.plan_release_storage import storage_relation
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS
from process.provider_directory_output_snapshot_provision import provision_output_snapshot_storage
from process.provider_directory_projection_stage import _copy_driver
from tests.provider_directory_source_postgres_support import verify_native_source_profile


async def _assert_output_writers(connection, policy):
    assert not await connection.fetchval(
        """SELECT EXISTS(SELECT FROM pg_class c JOIN pg_roles r ON r.rolname=ANY($3::text[])
        WHERE c.relnamespace=$1::regnamespace AND c.relname=ANY($2::text[]) AND (
          has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
          OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))""",
        policy.schema,
        list(OUTPUT_ROOTS),
        list(policy.runtime_roles),
    )


async def _output_census(connection, policy):
    statement = " UNION ALL ".join(
        f"SELECT '{name}'::text, count(*)::bigint FROM {storage_relation(policy.schema, name)}" for name in OUTPUT_ROOTS
    )
    return tuple(tuple(row) for row in await connection.fetch(statement))


async def provision_native_outputs(component):
    """Preserve output data while the real gate verifies every native identity and ACL delta."""
    async with async_sessionmaker(component.admin_engine)() as session, session.begin():
        await session.execute(text("SELECT 1"))
        driver = await _copy_driver(session)
        before = await _output_census(driver, component.policy)
        receipts = await provision_output_snapshot_storage(
            session,
            component.policy,
            verify_role_profile=verify_native_source_profile,
            verify_writer_closure=_assert_output_writers,
        )
        assert len(receipts) == len(OUTPUT_ROOTS) and {receipt.target for receipt in receipts} == set(OUTPUT_ROOTS)
        assert await _output_census(driver, component.policy) == before
