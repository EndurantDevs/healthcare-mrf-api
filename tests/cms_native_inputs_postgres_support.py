# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Source custody for fixed Doctors and archive component fixtures."""

from contextlib import asynccontextmanager

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models import GeoZipLookup, ProviderDirectoryAPIEndpoint, ProviderDirectoryEndpointDataset
from process import cms_doctors_immutable as doctors
from process import geo_source_storage as geo
from process import reference_family_result_generation as generation
from process.entity_address_snapshot_restore import _additional_index_sql
from process.plan_release_storage_provision import _FAMILY_SQL, _close_relation_grants
from process.provider_directory_projection_stage import _copy_driver
from tests.provider_directory_source_postgres_support import native_source_component


async def create_fixed_input_models(database, schema):
    """Create exact model columns, primary keys and complete native index sets."""
    async with database.engine.begin() as connection:
        for model in (*doctors._models(), GeoZipLookup):
            table = model.__table__.to_metadata(MetaData(), schema=schema)
            for column in table.columns:
                column.autoincrement = False
            await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))
            for spec in getattr(model, "__my_additional_indexes__", ()):
                await connection.execute(
                    text(
                        _additional_index_sql(
                            schema_name=schema, table_name=table.name, stage_table_name=table.name, index=spec
                        )
                    )
                )


def source_header_models(schema):
    """Resolve the complete fixed Header FK closure before compiling any DDL."""
    metadata = MetaData()
    for model in (ProviderDirectoryAPIEndpoint, ProviderDirectoryEndpointDataset):
        model.__table__.to_metadata(metadata, schema=schema)
    return tuple(metadata.sorted_tables)


async def _create_source_header(database, schema):
    """Keep an existing scalar receipt fixture; otherwise create both actual models."""
    async with database.engine.begin() as connection:
        header = await connection.scalar(
            text("SELECT to_regclass(:name)::oid"), {"name": f"{schema}.provider_directory_endpoint_dataset"}
        )
        if header is None:
            for table in source_header_models(schema):
                await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))


@asynccontextmanager
async def source_input_custody(database, schema, monkeypatch, *, fixed_inputs=False):
    """Enroll only exact fixture OIDs under the real native Source role graph."""
    await _create_source_header(database, schema)
    async with native_source_component(database.engine, schema, monkeypatch) as component:
        component.factory.enroll_ordinary_native_inputs()
        names = [ProviderDirectoryEndpointDataset.__tablename__]
        if fixed_inputs:
            names += [model.__tablename__ for model in (*doctors._models(), GeoZipLookup)]
        async with async_sessionmaker(database.engine)() as session, session.begin():
            driver = await _copy_driver(session)
            await driver.execute(f'GRANT SET ON PARAMETER temp_file_limit TO "{component.owner}"')
            assert (
                await driver.fetchval("SELECT has_parameter_privilege($1, 'temp_file_limit', 'SET')", component.owner)
                is True
            )
            assert (
                await driver.fetchval("SELECT has_parameter_privilege($1, 'temp_file_limit', 'SET')", component.reader)
                is False
            )
            assert (
                await driver.fetchval("SELECT pg_has_role($1, $2, 'MEMBER')", component.reader, component.owner)
                is False
            )
            family = await driver.fetch(_FAMILY_SQL, [f'"{schema}"."{name}"' for name in names])
            assert len(family) == len(names) and {relation_by_field["relname"] for relation_by_field in family} == set(
                names
            )
            for relation_by_field in family:
                await _close_relation_grants(driver, relation_by_field, component.owner, component.policy.runtime_roles)
            assert await driver.fetchval(
                "SELECT count(*)=$2 AND bool_and(relowner=(SELECT oid FROM pg_roles WHERE rolname=$3)) "
                "FROM pg_class WHERE oid=ANY($1::oid[])",
                [relation_by_field["oid"] for relation_by_field in family],
                len(names),
                component.owner,
            )
        component.database._input_component = component
        if fixed_inputs:
            async with component.database.transaction() as session:
                current = await doctors.capture_doctors_predecessor(session, schema)
                await doctors.publish_doctors_generation(session, schema_name=schema, expected_authority=current)
                current = await generation.read_reference_family_result_generation_authority(
                    session, importer_id="geo", schema_name=schema
                )
                await geo.publish_geo_generation(session, schema_name=schema, expected_authority=current)
        yield component


@asynccontextmanager
async def ordinary_input_reader(database):
    """Exercise ACL refusal on a real ordinary read-write login, without READ ONLY."""
    component = database._input_component
    engine = create_async_engine(component.admin_engine.url.set(username=component.reader))
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            assert await session.scalar(text("SELECT current_user=session_user")) is True
            assert await session.scalar(text("SHOW transaction_read_only")) == "off"
            yield session
    finally:
        await engine.dispose()


async def enroll_native_input_custody(database, schema, component, *, replacement_name=None):
    """Assign only the fixed input and ledger OIDs before ordinary Source proof."""
    from process import provider_directory_cms_native_inputs as inputs

    names = tuple(
        sorted(
            set(inputs._relations(schema))
            | {
                (schema, inputs.npi.TABLE_NAME),
                (schema, inputs.reference.TABLE_NAME),
                (schema, inputs._TABLE),
            }
        )
    )
    if replacement_name is not None:
        assert (schema, replacement_name) in inputs._relations(schema)
        names = ((schema, replacement_name),)
    async with database.engine.begin() as connection:
        if replacement_name is None:
            await connection.execute(text(f'GRANT USAGE ON SCHEMA tiger TO "{component.owner}"'))
        for namespace, name in names:
            relation = f'"{namespace}"."{name}"'
            oid = await connection.scalar(text("SELECT to_regclass(:name)::oid"), {"name": relation})
            assert type(oid) is int and oid > 0
            await connection.execute(text(f'ALTER TABLE {relation} OWNER TO "{component.owner}"'))
            assert (
                await connection.scalar(
                    text(
                        "SELECT oid=:oid AND relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner) FROM pg_class WHERE oid=to_regclass(:name)"
                    ),
                    {"oid": oid, "owner": component.owner, "name": relation},
                )
                is True
            )
        if replacement_name is None:
            for function in (f'"{schema}".cms_native_input_advance()', "pg_catalog.pg_control_system()"):
                await connection.execute(text(f'GRANT EXECUTE ON FUNCTION {function} TO "{component.owner}"'))
    database._native_input_component = component


@asynccontextmanager
async def native_input_actor(database, *, readonly=False):
    """Use this fixture's actual Factory and preserve read-only capture semantics."""
    from process import provider_directory_generic_source_policy as source

    component = database._native_input_component
    missing = object()
    previous = getattr(component.factory, "readonly", missing)
    component.factory.readonly = readonly
    try:
        async with source.source_transaction(component.database, isolation="repeatable read") as session:
            assert (
                await session.scalar(
                    text(
                        "SELECT current_user=:owner AND session_user=:publisher AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
                    ),
                    {"owner": component.owner, "publisher": component.publisher},
                )
                is True
            )
            assert await session.scalar(text("SHOW transaction_read_only")) == ("on" if readonly else "off")
            yield session
    finally:
        if previous is missing:
            del component.factory.readonly
        else:
            component.factory.readonly = previous
