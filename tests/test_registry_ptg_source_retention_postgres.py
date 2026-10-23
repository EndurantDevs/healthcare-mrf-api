# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retention-principal proof, independent of Registry approval flows."""

from __future__ import annotations

import secrets
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import literal, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_ptg_source_retention as retention
from process.ptg_parts import result_archive_published_authority as published
from process.ptg_parts import result_archive_source_authority as archive
from process.ptg_parts.ptg2_lifecycle_lock import lifecycle_database_sqlstate
from process.registry_ptg_published_provisioning import (
    HEADER_TABLES,
    LOCK_COLUMN,
    PIN_INSERT_COLUMNS,
    published_lock_provisioning_statements,
    published_pin_writer_revocation_statements,
)
from tests.test_result_archive_published_authority_postgres import _database, _seed


async def _create_principals(engine, schema_name, outside_schema, roles_by_actor, resources):
    passwords_by_actor = {actor: secrets.token_hex(24) for actor in roles_by_actor}
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{outside_schema}"'))
        resources.outside_created = True
        await connection.execute(text(f'CREATE TABLE "{outside_schema}".unrelated_fact(value integer)'))
        for actor, role in roles_by_actor.items():
            password_literal = literal(passwords_by_actor[actor]).compile(
                dialect=engine.dialect, compile_kwargs={"literal_binds": True}
            )
            await connection.execute(
                text(
                    f'CREATE ROLE "{role}" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE '
                    f"NOREPLICATION NOBYPASSRLS PASSWORD {password_literal}"
                )
            )
            resources.created_roles.append(role)
        role_sql = ",".join(f'"{role}"' for role in roles_by_actor.values())
        await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema_name}","{outside_schema}" TO {role_sql}'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema_name}" TO {role_sql}'))
        for statement in published_lock_provisioning_statements(
            schema_name, roles_by_actor["reader"], roles_by_actor["approver"]
        ):
            await connection.execute(text(statement))
        # Rehearse revocation of both legacy grant forms on already-installed headers.
        await connection.execute(
            text(
                f'GRANT INSERT, INSERT({",".join(PIN_INSERT_COLUMNS)}) ON "{schema_name}".ptg2_snapshot_pin '
                f'TO "{roles_by_actor["approver"]}"'
            )
        )
        for statement in published_pin_writer_revocation_statements(
            schema_name, roles_by_actor["reader"], roles_by_actor["approver"]
        ):
            await connection.execute(text(statement))
        for statement in retention.source_retention_provisioning_statements(schema_name, roles_by_actor["publisher"]):
            await connection.execute(text(statement))
    return passwords_by_actor


async def _cleanup_principals(engine, schema_name, outside_schema, resources):
    async with engine.begin() as connection:
        surviving_roles = (
            (
                await connection.execute(
                    text("SELECT rolname FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[]))"),
                    {"roles": resources.created_roles},
                )
            )
            .scalars()
            .all()
        )
        for role in surviving_roles:
            await connection.execute(text(f'REVOKE ALL ON SCHEMA "{schema_name}" FROM "{role}"'))
            await connection.execute(text(f'REVOKE SELECT ON ALL TABLES IN SCHEMA "{schema_name}" FROM "{role}"'))
            for table in HEADER_TABLES:
                await connection.execute(
                    text(f'REVOKE UPDATE({LOCK_COLUMN}) ON "{schema_name}"."{table}" FROM "{role}"')
                )
            await connection.execute(
                text(
                    f"REVOKE INSERT({','.join(PIN_INSERT_COLUMNS)}),DELETE "
                    f'ON "{schema_name}".ptg2_snapshot_pin FROM "{role}"'
                )
            )
        outside_oid = await connection.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": outside_schema})
        if resources.outside_created and outside_oid is not None:
            await connection.execute(text(f'DROP TABLE IF EXISTS "{outside_schema}".unrelated_fact RESTRICT'))
            for role in surviving_roles:
                await connection.execute(text(f'REVOKE ALL ON SCHEMA "{outside_schema}" FROM "{role}"'))
            await connection.execute(text(f'DROP SCHEMA "{outside_schema}" RESTRICT'))
        for role in surviving_roles:
            await connection.execute(text(f'DROP ROLE "{role}"'))
        assert (
            not (
                await connection.execute(
                    text("SELECT rolname FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[]))"),
                    {"roles": resources.created_roles},
                )
            )
            .scalars()
            .all()
        )


@asynccontextmanager
async def _retention_principals():
    async with _database() as (engine, schema_name):
        suffix = uuid4().hex
        roles_by_actor = {actor: f"retention_{actor}_{suffix}" for actor in ("reader", "approver", "publisher")}
        outside_schema = "retention_outside_" + suffix
        resources = SimpleNamespace(created_roles=[], outside_created=False)
        actor_engines = []
        try:
            passwords_by_actor = await _create_principals(
                engine, schema_name, outside_schema, roles_by_actor, resources
            )
            sessions_by_actor = {}
            for actor, role in roles_by_actor.items():
                actor_engine = create_async_engine(
                    engine.url.set(username=role, password=passwords_by_actor[actor]),
                    pool_size=1,
                    max_overflow=0,
                    hide_parameters=True,
                )
                actor_engines.append(actor_engine)
                sessions_by_actor[actor] = async_sessionmaker(actor_engine, expire_on_commit=False)
            yield SimpleNamespace(
                engine=engine,
                schema_name=schema_name,
                outside_schema=outside_schema,
                roles_by_actor=roles_by_actor,
                sessions_by_actor=sessions_by_actor,
                service=SimpleNamespace(
                    schema_name=schema_name, source_retention_sessions=sessions_by_actor["publisher"]
                ),
            )
        finally:
            try:
                for actor_engine in reversed(actor_engines):
                    await actor_engine.dispose()
            finally:
                await _cleanup_principals(engine, schema_name, outside_schema, resources)


async def _verify_publisher(fixture):
    async with fixture.sessions_by_actor["publisher"].begin() as session:
        await retention.verify_source_retention_publisher(session, fixture.service)


async def _verify_assumed_role_refused(fixture, role):
    async with fixture.sessions_by_actor["publisher"].begin() as session:
        await session.execute(text(f"SET ROLE {role}"))
        with pytest.raises(RuntimeError, match="source_retention_unprotected"):
            await retention.verify_source_retention_publisher(session, fixture.service)
        await session.execute(text("RESET ROLE"))


@pytest.mark.asyncio
async def test_dedicated_native_publisher_captures_clones_and_releases_only_its_exact_pin():
    async with _retention_principals() as fixture:
        selected = await _seed(fixture.engine, fixture.schema_name)
        async with fixture.sessions_by_actor["publisher"].begin() as session:
            await retention.verify_source_retention_publisher(session, fixture.service)
            first = await published.capture_ptg_published_result_source_authority(
                session, schema_name=fixture.schema_name, operation_id="publisher-first", snapshot_id=selected
            )
            second = await published.capture_ptg_published_result_source_authority(
                session, schema_name=fixture.schema_name, operation_id="publisher-second", snapshot_id=selected
            )
        async with fixture.sessions_by_actor["reader"].begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            assert (
                await archive.lock_ptg_result_archive_for_clone(
                    session, schema_name=fixture.schema_name, authority=first.as_dict()
                )
                == first
            )
        async with fixture.sessions_by_actor["publisher"].begin() as session:
            await retention.verify_source_retention_publisher(session, fixture.service)
            assert (
                await archive.release_ptg_result_archive_source_authority(
                    session, schema_name=fixture.schema_name, authority=first.as_dict()
                )
                == 1
            )
        async with fixture.engine.connect() as connection:
            assert (
                await connection.execute(text(f'SELECT owner_id,reason FROM "{fixture.schema_name}".ptg2_snapshot_pin'))
            ).all() == [(second.owner_id, second.pin_reason)]
        async with fixture.sessions_by_actor["publisher"].begin() as session:
            assert (
                await archive.reconcile_ptg_archive_source_release(
                    session, schema_name=fixture.schema_name, authority=first.as_dict()
                )
                == "already_released"
            )
            assert (
                await archive.release_ptg_result_archive_source_authority(
                    session, schema_name=fixture.schema_name, authority=second.as_dict()
                )
                == 1
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("privilege", ["INSERT", "UPDATE", "DELETE", "UPDATE(value)"])
async def test_unrelated_effective_write_is_refused_and_exact_revoke_restores_profile(privilege):
    async with _retention_principals() as fixture:
        await _verify_publisher(fixture)
        relation = f'"{fixture.outside_schema}".unrelated_fact'
        role = f'"{fixture.roles_by_actor["publisher"]}"'
        try:
            async with fixture.engine.begin() as connection:
                await connection.execute(text(f"GRANT {privilege} ON {relation} TO {role}"))
            with pytest.raises(RuntimeError, match="source_retention_unprotected"):
                await _verify_publisher(fixture)
        finally:
            async with fixture.engine.begin() as connection:
                await connection.execute(text(f"REVOKE {privilege} ON {relation} FROM {role}"))
        await _verify_publisher(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("membership_kind", ["inherit", "set", "admin_only", "inbound"])
async def test_membership_and_set_authority_are_refused_until_exact_revoke(membership_kind):
    async with _retention_principals() as fixture:
        await _verify_publisher(fixture)
        reader = f'"{fixture.roles_by_actor["reader"]}"'
        publisher = f'"{fixture.roles_by_actor["publisher"]}"'
        options = {
            "inherit": "INHERIT TRUE, SET FALSE",
            "set": "INHERIT FALSE, SET TRUE",
            "admin_only": "ADMIN TRUE, INHERIT FALSE, SET FALSE",
            "inbound": "INHERIT FALSE, SET TRUE",
        }[membership_kind]
        grant_role, member_role = (publisher, reader) if membership_kind == "inbound" else (reader, publisher)
        try:
            async with fixture.engine.begin() as connection:
                await connection.execute(text(f"GRANT {grant_role} TO {member_role} WITH {options}"))
            await _probe_membership_capabilities(fixture, membership_kind, reader, publisher)
            with pytest.raises(RuntimeError, match="source_retention_unprotected"):
                await _verify_publisher(fixture)
        finally:
            async with fixture.engine.begin() as connection:
                await connection.execute(text(f"REVOKE {grant_role} FROM {member_role}"))
        await _verify_publisher(fixture)


async def _probe_membership_capabilities(fixture, membership_kind, reader, publisher):
    """Exercise real membership privileges that a simple SET/USAGE check misses."""
    if membership_kind == "admin_only":
        async with fixture.sessions_by_actor["publisher"].begin() as session:
            assert (
                await session.scalar(
                    text("SELECT pg_has_role(current_user,:reader,'SET') OR pg_has_role(current_user,:reader,'USAGE')"),
                    {"reader": fixture.roles_by_actor["reader"]},
                )
                is False
            )
    elif membership_kind == "set":
        await _verify_assumed_role_refused(fixture, reader)
    elif membership_kind == "inbound":
        async with fixture.sessions_by_actor["reader"].begin() as session:
            await session.execute(text(f"SET LOCAL ROLE {publisher}"))
            assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_actor["publisher"]


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", ["reader", "approver"])
async def test_final_reader_and_approver_grants_cannot_insert_source_pins(actor):
    async with _retention_principals() as fixture:
        selected = await _seed(fixture.engine, fixture.schema_name)
        with pytest.raises(DBAPIError) as captured:
            async with fixture.sessions_by_actor[actor].begin() as session:
                await session.execute(
                    text(
                        f'INSERT INTO "{fixture.schema_name}".ptg2_snapshot_pin '
                        "(owner_type,owner_id,snapshot_id,reason,created_at) "
                        "VALUES ('synthetic-owner',:owner,:snapshot,'synthetic pin',CURRENT_TIMESTAMP)"
                    ),
                    {"owner": actor, "snapshot": selected},
                )
        assert lifecycle_database_sqlstate(captured.value) == "42501"
        async with fixture.engine.connect() as connection:
            assert await connection.scalar(text(f'SELECT count(*) FROM "{fixture.schema_name}".ptg2_snapshot_pin')) == 0
