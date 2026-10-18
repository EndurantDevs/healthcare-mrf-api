# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native component actors authenticate with the disposable database's credential."""

import json
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from tests import provider_directory_source_postgres_support as source_fixture
from tests.public_evidence_storage_postgres_support import database_url


async def _ordinary_login(url, role):
    driver = await asyncpg.connect(
        url.set(drivername="postgresql", username=role).render_as_string(hide_password=False)
    )
    try:
        observed = await driver.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,"
            "rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
            "FROM pg_roles WHERE rolname=current_user"
        )
        assert observed["session"] == observed["current"] == role
        assert not any(
            observed[name] for name in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
    finally:
        await driver.close()


async def _authenticate_component(component):
    url = component.admin_engine.url
    roles = (component.publisher, component.reader, component.builder, component.retained_publisher)
    for role in roles:
        await _ordinary_login(url, role)
        with pytest.raises(asyncpg.InvalidPasswordError):
            await asyncpg.connect(
                url.set(drivername="postgresql", username=role, password="wrong-" + uuid4().hex).render_as_string(
                    hide_password=False
                )
            )
    for role in (component.owner, component.retention_owner):
        with pytest.raises(asyncpg.InvalidAuthorizationSpecificationError):
            await asyncpg.connect(url.set(drivername="postgresql", username=role).render_as_string(hide_password=False))
    async with component.factory.transaction(component.database, isolation="read committed") as session:
        actor = (await session.execute(text("SELECT session_user::text,current_user::text"))).one()
        assert tuple(actor) == (component.publisher, component.owner)


@pytest.mark.asyncio
async def test_native_source_actor_credentials_work_under_scram_and_refuse_wrong_password(monkeypatch, tmp_path):
    url = database_url()
    assert url.password, "SCRAM component proof requires a password-authenticated disposable database"
    engine = create_async_engine(url)
    schema = "source_auth_" + uuid4().hex
    registered_roles = []
    create_roles = source_fixture._create_native_roles

    async def register_before_creation(engine, policy, roles, *args):
        registered_roles.extend(roles)
        (tmp_path / "registered-roles.json").write_text(json.dumps(list(roles)))
        return await create_roles(engine, policy, roles, *args)

    monkeypatch.setattr(source_fixture, "_create_native_roles", register_before_creation)
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SHOW password_encryption")) == "scram-sha-256"
            assert (
                await connection.scalar(
                    text(
                        "SELECT bool_and(auth_method='scram-sha-256') FROM pg_hba_file_rules WHERE type LIKE 'host%' AND error IS NULL"
                    )
                )
                is True
            )
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        async with source_fixture.native_source_component(
            engine, schema, monkeypatch, legacy_builder=True, bind_factory=False
        ) as component:
            await _authenticate_component(component)
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        text(
                            "SELECT bool_and(rolpassword LIKE 'SCRAM-SHA-256$%') FROM pg_authid WHERE rolname=ANY(:roles)"
                        ),
                        {
                            "roles": [
                                component.publisher,
                                component.reader,
                                component.builder,
                                component.retained_publisher,
                            ]
                        },
                    )
                    is True
                )
        async with engine.connect() as connection:
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM pg_roles WHERE rolname=ANY(:roles)"), {"roles": registered_roles}
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" RESTRICT'))
        await engine.dispose()


async def _seed_template_isolation(component):
    """Write a pre-cut sentinel only after proving this clone starts without it."""
    async with component.admin_engine.begin() as connection:
        assert (
            await connection.scalar(text("SELECT count(*) FROM mrf.import_run WHERE run_id='template-isolation'")) == 0
        )
        await connection.execute(
            text(
                "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics) "
                "VALUES('template-isolation','synthetic','synthetic','running','{}','{}')"
            )
        )


async def _observe_source_template_clone(admin, component):
    """Check closed template actors and fresh clone ownership after the real Source cut."""
    from types import SimpleNamespace

    url = component.admin_engine.url
    database_oid = await admin.fetchval("SELECT oid FROM pg_database WHERE datname=$1", url.database)
    roles = (
        component.owner,
        component.publisher,
        component.builder,
        component.retention_owner,
        component.retained_publisher,
        component.reader,
    )
    assert len(source_fixture._SOURCE_COMPONENT_BASELINES) == 1
    baseline = next(iter(source_fixture._SOURCE_COMPONENT_BASELINES.values()))
    assert await admin.fetchval("SELECT NOT datallowconn FROM pg_database WHERE datname=$1", baseline.name)
    assert await admin.fetchval(
        "SELECT bool_and(NOT rolcanlogin) FROM pg_roles WHERE rolname=ANY($1::text[])",
        [baseline.owner, baseline.builder],
    )
    async with component.admin_engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT count(*) FROM mrf.import_run WHERE run_id='template-isolation'")) == 1
        )
        assert (
            await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_shdepend WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                    "AND refclassid='pg_authid'::regclass AND refobjid IN(CAST(:owner AS regrole),CAST(:builder AS regrole))"
                ),
                {"owner": baseline.owner, "builder": baseline.builder},
            )
            == 0
        )
    return SimpleNamespace(database=url.database, database_oid=database_oid, roles=roles, baseline=baseline)


async def _source_template_clone(monkeypatch, admin, prefixes):
    """Keep each actual clone and its fresh roles isolated through verified database cleanup."""
    with monkeypatch.context() as settings:
        async with source_fixture.source_component_database(
            settings, bind_factory=False, before_provision=_seed_template_isolation, migration_prefixes=prefixes
        ) as component:
            observed = await _observe_source_template_clone(admin, component)
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", observed.database)
    return observed


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_native_source_template_clones_adopt_fresh_roles_and_isolate_writes(monkeypatch, legacy):
    """Clone each genuine migration profile twice and preserve exact actor and database cleanup."""
    from tests import cms_npd_admission_postgres_support as admission

    source_url = admission._database_url()
    admin = await asyncpg.connect(source_url.set(drivername="postgresql").render_as_string(hide_password=False))
    prefixes = source_fixture.legacy_source_component_migrations() if legacy else None
    try:
        async with source_fixture.source_component_templates():
            clones = [await _source_template_clone(monkeypatch, admin, prefixes) for _ in range(2)]
            assert clones[0].database_oid != clones[1].database_oid and clones[0].baseline is clones[1].baseline
            roles = [role for clone in clones for role in clone.roles]
            assert len(set(roles)) == len(roles)
        for clone in clones:
            assert not await admin.fetchval(
                "SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", clone.baseline.name
            )
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))",
            [*roles, clones[0].baseline.owner, clones[0].baseline.builder],
        )
    finally:
        await admin.close()
