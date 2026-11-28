# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native-role Geo component support; never an enrolled runtime proof."""

from contextlib import asynccontextmanager

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import GeoZipLookup
from tests import provider_directory_source_postgres_support as source


async def create_geo_indexes(connection, *, index_profile="current"):
    """Create an exact historical or current index profile on an empty bootstrap heap."""
    from process.entity_address_snapshot_restore import _additional_index_sql

    assert await connection.scalar(text("SELECT count(*) FROM mrf.geo_zip_lookup")) == 0
    assert index_profile in {"current", "legacy-pricing", "legacy-native"}
    if index_profile == "legacy-pricing":
        await connection.execute(
            text("CREATE INDEX plan_pricing_geo_zip_coordinates_idx ON mrf.geo_zip_lookup(latitude,longitude,zip_code)")
        )
        return
    for index in GeoZipLookup.__my_additional_indexes__:
        if index_profile == "legacy-native" and tuple(index["index_elements"]) == ("latitude", "longitude", "zip_code"):
            continue
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


async def provision_geo(engine, policy, verify_role_profile):
    """Hand the bootstrap Geo heap and ledger rights to the Source Owner with plain grants."""
    from db.plan_release_storage import storage_identifier, storage_relation
    from process.code_catalog_snapshot import _relation_access
    from process.geo_source_storage import ROOT
    from process.plan_release_storage_provision import _GRANTEES_SQL
    from process.provider_directory_cms_doctors_provision import GENERATION_SELECT, GENERATION_UPDATE
    from process.provider_directory_projection_stage import _copy_driver

    async with engine.connect() as connection, connection.begin():
        async with AsyncSession(bind=connection) as session, session.begin():
            await session.execute(text("SELECT 1"))
            native = await _copy_driver(session)
            relation = storage_relation(policy.schema, ROOT)
            ledger = storage_relation(policy.schema, "reference_family_result_generation")
            owner = storage_identifier(policy.owner_role)
            await native.execute(f"LOCK TABLE ONLY {relation},{ledger} IN ACCESS EXCLUSIVE MODE")
            oid = await native.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
            prior = await _relation_access(session, oid)
            await native.execute(f"ALTER TABLE {relation} OWNER TO {owner}")
            for grant in await native.fetch(_GRANTEES_SQL, oid):
                principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
                column = grant["column_name"]
                privileges = f"ALL PRIVILEGES ({storage_identifier(column)})" if column else "ALL PRIVILEGES"
                await native.execute(f"REVOKE {privileges} ON TABLE {relation} FROM {principal} CASCADE")
            for grant in prior.grants:
                if grant.privilege_type == "SELECT" and grant.grantee_name != policy.owner_role:
                    principal = "PUBLIC" if grant.grantee_name is None else storage_identifier(grant.grantee_name)
                    column = "" if grant.column_name is None else f" ({storage_identifier(grant.column_name)})"
                    option = " WITH GRANT OPTION" if grant.is_grantable else ""
                    await session.execute(text(f"GRANT SELECT{column} ON TABLE {relation} TO {principal}{option}"))
            select = ",".join(storage_identifier(name) for name in GENERATION_SELECT)
            update = ",".join(storage_identifier(name) for name in GENERATION_UPDATE)
            await native.execute(f"GRANT SELECT({select}),UPDATE({update}) ON {ledger} TO {owner}")
            for hook_row in await native.fetch(
                "SELECT tgname FROM pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal", oid
            ):
                await native.execute(f"DROP TRIGGER {storage_identifier(hook_row['tgname'])} ON {relation}")
            await native.execute(f"UPDATE {ledger} SET source_revision_tracked=FALSE WHERE importer_id='geo'")
            await verify_role_profile(native, policy.schema, **policy.role_arguments())
            return oid


@asynccontextmanager
async def geo_source_component_database(monkeypatch, *, index_profile="current"):
    """Reuse the real CMS dependency/kernel and add Geo before native-role custody."""
    from sqlalchemy.ext.asyncio import create_async_engine

    original_bootstrap, original_migrations = source.bootstrap_source_component, source.source_component_migrations

    def migrations():
        prefixes = list(original_migrations())
        position = prefixes.index("20260920100000") + 1
        prefixes[position:position] = ("20260920110000", "20260920120000", "20260920130000")
        return tuple(prefixes)

    async def bootstrap(engine, policy):
        historical = create_async_engine(engine.url.set(username=policy.control_runtime_roles[0]))
        try:
            async with historical.begin() as connection:
                table = GeoZipLookup.__table__.to_metadata(MetaData(), schema=policy.schema)
                assert await connection.scalar(text("SELECT to_regclass('mrf.geo_zip_lookup')")) is None
                await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))
                await create_geo_indexes(connection, index_profile=index_profile)
        finally:
            await historical.dispose()
        await original_bootstrap(engine, policy)

    with monkeypatch.context() as settings:
        settings.setattr(source, "source_component_migrations", migrations)
        settings.setattr(source, "bootstrap_source_component", bootstrap)
        async with source.source_component_database(settings) as component:
            await provision_geo(component.admin_engine, component.policy, source.verify_native_source_profile)
            yield component
