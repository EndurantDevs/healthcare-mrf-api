# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine ACA completion, protected source capture and accepted ledger fixture."""

import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from db.models import AddressArchiveV2
from db.registry_aca_source_notification import CREATE_TABLE_SQL
from process import mrf_publication_receipt as publication
from process import reference_family_archive as archive
from process import reference_family_result_generation as generations
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_address_capture import native_driver
from process.registry_aca_source_notification_permissions import install_registry_aca_source_notification_permissions
from process.registry_aca_source_notifications import RegistryACASourceNotificationStore
from process.registry_aca_source_recipe import admit_registry_aca_source_recipe, capture_registry_aca_source_metadata
from tests import cms_npd_admission_postgres_support as databases
from tests import test_mrf_publication_receipt_postgres as mrf
from tests.reference_family_generation_fixture import native_reference_source
from tests.test_reference_family_archive_postgres import _create_live_family


async def _roles_and_control(engine, names, created_roles):
    async with engine.begin() as setup:
        for kind in ("retention_owner", "control_owner", "publisher", "reader"):
            login = f"LOGIN PASSWORD '{names.password}'" if kind in {"publisher", "reader"} else "NOLOGIN"
            await setup.execute(text(f'CREATE ROLE "{getattr(names, kind)}" {login}'))
            created_roles.append(getattr(names, kind))
        await setup.execute(text(f'GRANT "{names.retention_owner}" TO "{names.publisher}"'))
        await setup.execute(text(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{names.retention_owner}"'))
        await setup.execute(text(f'CREATE SCHEMA "{names.control_schema}" AUTHORIZATION "{names.control_owner}"'))
        await setup.execute(text(f'REVOKE ALL ON SCHEMA "{names.control_schema}" FROM PUBLIC'))
        await setup.execute(text(CREATE_TABLE_SQL.format(namespace='"' + names.control_schema + '"')))
        await setup.execute(
            text(f'CREATE TABLE "{names.control_schema}".network_serving_control(singleton boolean PRIMARY KEY)')
        )
        for table in ("registry_aca_source_notification", "network_serving_control"):
            await setup.execute(text(f'ALTER TABLE "{names.control_schema}".{table} OWNER TO "{names.control_owner}"'))
        await setup.execute(text(f'GRANT USAGE ON SCHEMA "{names.control_schema}" TO current_user'))
        database = await setup.scalar(text("SELECT quote_ident(current_database())"))
        await setup.execute(text(f'GRANT CREATE ON DATABASE {database} TO "{names.publisher}"'))
        for extension in ("postgis", "intarray", "btree_gin", "pg_trgm"):
            await setup.execute(text(f'CREATE EXTENSION IF NOT EXISTS "{extension}"'))


async def _completed_source(engine, sessions, names, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", names.source_schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(publication, "db", database)
    async with engine.begin() as setup:
        await setup.execute(text("CREATE SCHEMA mrf"))
        await setup.run_sync(lambda sync: AddressArchiveV2.__table__.c.geo_source.type.create(sync, checkfirst=True))
        await _create_live_family(setup, "mrf", names.source_schema)
        # Preserve the existing historical source contract, including its native self edge.
        await mrf._create_canonical_address_source(setup, archive, names.source_schema)
        await mrf._run_migration(setup, mrf._MIGRATION, "upgrade")
        await mrf._configure_summary_tables(setup, monkeypatch, names.source_schema)
        await setup.execute(text(f'GRANT USAGE ON SCHEMA "{names.source_schema}",mrf TO "{names.publisher}"'))
        await setup.execute(
            text(f'GRANT SELECT,MAINTAIN ON ALL TABLES IN SCHEMA "{names.source_schema}" TO "{names.publisher}"')
        )
    attempt = await publication.begin_publication(names.source_schema, "accepted-import")
    async with sessions() as session, session.begin():
        generation = await generations.publish_local_reference_family_generation(
            session, importer_id="mrf", schema_name=names.source_schema
        )
        inputs = await publication.capture_summary_inputs(session, names.source_schema)
        await publication.complete_publication(session, names.source_schema, attempt, generation, inputs, False)


def _metadata(names, coordinates):
    return {
        "network_membership": {
            "source_id": "accepted-aca",
            "release_id": "original-release",
            "import_id": "accepted-import",
            "issuer_id": 12345,
            "year": 2026,
            "source_url": "https://source.example.test/accepted",
            "alias_scope": "medical",
            "reviewed_alias_sha256": "b" * 64,
            "runtime_roles": [names.reader],
        },
        "network_bindings": {**asdict(coordinates), "source_key_kind": "hios_plan_id"},
    }


async def _capture(sessions, publisher_sessions, names, store, custody):
    capture_id = uuid4()
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        "accepted-aca",
        archive.reference_family_stage_schema(capture_id),
        str(capture_id),
        "accepted-producer",
        "original-edition",
    )
    recipes = []

    async def metadata(session):
        return await capture_registry_aca_source_metadata(
            session,
            _metadata(names, coordinates),
            schema=names.source_schema,
            runtime_roles=(names.reader,),
            dataset_id=capture_id,
        )

    async def accepted(session, prepared):
        await custody.retain(session, prepared)
        recipe = await admit_registry_aca_source_recipe(session, prepared, runtime_roles=(names.reader,))
        receipt = await store.append(session, recipe)
        assert receipt["recipe"] == recipe
        recipes.append(recipe)

    await archive.prepare_reference_family_archive_source(
        publisher_sessions,
        importer_id="mrf",
        schema_name=names.source_schema,
        source_metadata=None,
        source_metadata_factory=metadata,
        dataset_id=capture_id,
        on_prepared=accepted,
        source_copy=custody.source_copy,
        on_precreated=custody.precreate,
        dependency_factory=lambda _session: _dependencies(),
    )
    assert len(recipes) == 1
    return recipes[0]


async def _dependencies():
    return {"plan-attributes": "c" * 64}


async def _remove_roles(sessions, created_roles):
    async with sessions() as session, session.begin():
        for role in reversed(created_roles):
            await session.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
            await session.execute(text(f'DROP ROLE "{role}"'))
        assert not await session.scalar(
            text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY(:roles))"), {"roles": created_roles}
        )


@asynccontextmanager
async def accepted_aca_anchor(monkeypatch):
    """Use a disposable database; ledger writes go through actual append/admission APIs."""
    token = uuid4().hex
    names = SimpleNamespace(
        **{
            kind: "aca_anchor_" + kind + "_" + token
            for kind in ("retention_owner", "control_owner", "publisher", "reader")
        },
        control_schema="aca_control_" + token,
        source_schema="aca_source_" + token,
        password=uuid4().hex,
    )
    configured = make_url(os.environ["NETWORK_REGISTRY_TEST_DSN"]).set(drivername="postgresql+asyncpg")
    async with databases._owned_database(configured) as (url, _admin), AsyncExitStack() as stack:
        engine = create_async_engine(url, hide_parameters=True)
        stack.push_async_callback(engine.dispose)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        created_roles = []
        stack.push_async_callback(_remove_roles, sessions, created_roles)
        await _roles_and_control(engine, names, created_roles)
        await _completed_source(engine, sessions, names, monkeypatch)
        sessions_by_kind = {}
        for kind in ("publisher", "reader"):
            client = create_async_engine(
                url.set(username=getattr(names, kind), password=names.password),
                pool_size=2 if kind == "publisher" else 1,
                max_overflow=0,
                hide_parameters=True,
            )
            stack.push_async_callback(client.dispose)
            sessions_by_kind[kind] = async_sessionmaker(client, expire_on_commit=False)
        async with sessions() as session, session.begin():
            await install_registry_aca_source_notification_permissions(
                await native_driver(session),
                control_schema=names.control_schema,
                publisher_role=names.publisher,
                reader_roles=(names.reader,),
                runtime_roles=tuple(sorted((names.publisher, names.reader))),
            )
        store = RegistryACASourceNotificationStore(names.control_schema, names.publisher, (names.reader,))
        custody = await stack.enter_async_context(
            native_reference_source(sessions, readers=(names.reader,), owner=names.retention_owner)
        )
        recipe = await _capture(sessions, sessions_by_kind["publisher"], names, store, custody)
        yield SimpleNamespace(
            sessions=sessions_by_kind["reader"], admin=sessions, recipe=recipe, store=store, names=names
        )
