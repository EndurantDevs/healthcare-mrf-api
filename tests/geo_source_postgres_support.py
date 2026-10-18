# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native-role Geo component support; never an enrolled runtime proof."""

from contextlib import asynccontextmanager

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import GeoZipLookup
from process import geo_source_provision as provision
from tests import provider_directory_source_postgres_support as source


async def create_geo_indexes(connection):
    """Finish exactly the model's four indexes on an empty bootstrap relation."""
    from process.entity_address_snapshot_restore import _additional_index_sql

    assert await connection.scalar(text("SELECT count(*) FROM mrf.geo_zip_lookup")) == 0
    for index in GeoZipLookup.__my_additional_indexes__:
        await connection.execute(
            text(
                _additional_index_sql(
                    schema_name="mrf",
                    table_name="geo_zip_lookup",
                    stage_table_name="geo_zip_lookup",
                    index=index,
                )
            )
        )


async def geo_preimage(driver, schema):
    """Capture complete native incumbent custody before the real administrator cut."""
    return await driver.fetchval(
        "SELECT jsonb_build_object('relation',to_jsonb(c),'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) "
        "FROM pg_trigger t WHERE t.tgrelid=c.oid),'constraints',(SELECT jsonb_agg(to_jsonb(k) ORDER BY k.oid) "
        "FROM pg_constraint k WHERE k.conrelid=c.oid),'indexes',(SELECT jsonb_agg(pg_get_indexdef(i.indexrelid) "
        "ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid),'functions',(SELECT jsonb_agg("
        "jsonb_build_array(p.oid,p.proowner,p.proacl,pg_get_functiondef(p.oid)) ORDER BY p.oid) FROM pg_proc p "
        "WHERE p.oid IN(SELECT tgfoid FROM pg_trigger WHERE tgrelid=c.oid))) "
        "FROM pg_class c WHERE c.oid=to_regclass($1)",
        schema + ".geo_zip_lookup",
    )


async def provision_geo(engine, policy, verify_role_profile):
    """Use actual product custody and writer checks with the caller's genuine verifier."""
    from process.provider_directory_projection_stage import _copy_driver

    async with engine.connect() as connection, connection.begin():
        async with AsyncSession(bind=connection) as session, session.begin():
            await session.execute(text("SELECT session_user"))
            expected = await geo_preimage(await _copy_driver(session), policy.schema)

            async def legacy(driver, actual, oid):
                assert actual == policy and oid > 0
                assert await geo_preimage(driver, policy.schema) == expected

            async def writers(driver, actual):
                assert actual == policy and GeoZipLookup.__runtime_schema_sync__ is False
                assert GeoZipLookup.__table__.info["managed_immutable_storage"]
                assert not await driver.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_roles r WHERE r.rolname=ANY($2::text[]) AND "
                    "(has_table_privilege(r.oid,to_regclass($1),'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN') "
                    "OR has_any_column_privilege(r.oid,to_regclass($1),'INSERT,UPDATE,REFERENCES')))",
                    policy.schema + ".geo_zip_lookup",
                    list((*policy.runtime_roles, *policy.control_runtime_roles)),
                )

            return await provision.provision_geo_source_storage(
                session,
                policy,
                verify_role_profile=verify_role_profile,
                verify_writer_closure=writers,
                verify_legacy_guards=legacy,
            )


@asynccontextmanager
async def geo_source_component_database(monkeypatch):
    """Reuse the real CMS dependency/kernel and add Geo before native-role custody."""
    from sqlalchemy.ext.asyncio import create_async_engine

    original_bootstrap, original_migrations = source.bootstrap_source_component, source.source_component_migrations

    def migrations():
        prefixes = list(original_migrations())
        position = prefixes.index("20260920100000") + 1
        prefixes[position:position] = ("20260920110000", "20260920120000", "20260920130000")
        return tuple(prefixes)

    async def bootstrap(engine, policy):
        historical = create_async_engine(engine.url.set(username=policy.control_runtime_roles[0], password=None))
        try:
            async with historical.begin() as connection:
                table = GeoZipLookup.__table__.to_metadata(MetaData(), schema=policy.schema)
                assert await connection.scalar(text("SELECT to_regclass('mrf.geo_zip_lookup')")) is None
                await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))
                await create_geo_indexes(connection)
        finally:
            await historical.dispose()
        await original_bootstrap(engine, policy)

    with monkeypatch.context() as settings:
        settings.setattr(source, "source_component_migrations", migrations)
        settings.setattr(source, "bootstrap_source_component", bootstrap)
        async with source.source_component_database(settings) as component:
            await provision_geo(component.admin_engine, component.policy, source.verify_native_source_profile)
            yield component
