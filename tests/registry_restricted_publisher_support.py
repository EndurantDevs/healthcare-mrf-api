# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A fixed Publisher login freezes only snapshots owned by its configured loading groups."""

from contextlib import AsyncExitStack, asynccontextmanager
from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from process.registry_named_preparation_administration import _freeze_registry_generation


async def _remove_publisher_role(connection, name):
    if await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name):
        await connection.execute('DROP OWNED BY "' + name + '"')
        await connection.execute('DROP ROLE "' + name + '"')
    assert not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", name)


async def _grant_publisher_access(fixture, name):
    connection = fixture.connection
    owner = fixture.preparation_roles["namespace_owner_role"]
    loading = fixture.writer_roles["owner_role"]
    for current_owner in sorted({loading, fixture.executor_role}):
        await connection.execute(f'GRANT "{current_owner}" TO "{name}" WITH ADMIN FALSE, INHERIT TRUE, SET TRUE')
    await connection.execute(f'GRANT "{owner}" TO "{name}" WITH ADMIN TRUE, INHERIT TRUE, SET TRUE')
    await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{name}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{name}"')
    await connection.execute(f'GRANT USAGE ON SCHEMA hp_snapshot_retention TO "{name}"')
    await connection.execute(
        'GRANT SELECT ON hp_snapshot_retention.preparation,hp_snapshot_retention.preparation_relation,hp_snapshot_retention.preparation_claim TO "'
        + name
        + '"'
    )
    await connection.execute(
        'GRANT INSERT(lease_token,preparation_id,attempt,lease_seconds) ON hp_snapshot_retention.preparation_claim TO "'
        + name
        + '"'
    )
    await connection.execute(
        'GRANT UPDATE(state,claim_token,claim_attempt,claim_expires_at,updated_at,frozen_inventory,frozen_sha256) ON hp_snapshot_retention.preparation TO "'
        + name
        + '"'
    )


@asynccontextmanager
async def restricted_source_publisher(fixture):
    """Register cleanup before creating one restricted login and its fixed group grants."""
    name = "snapshot_publisher_" + uuid4().hex
    password = uuid4().hex
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_remove_publisher_role, fixture.connection, name)
        literal = await fixture.connection.fetchval("SELECT quote_literal($1::text)", password)
        await fixture.connection.execute(
            'CREATE ROLE "'
            + name
            + '" LOGIN INHERIT NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '
            + literal
        )
        await _grant_publisher_access(fixture, name)
        url = fixture.engine.url.set(username=name, password=password, drivername="postgresql+asyncpg")
        engine = create_async_engine(url, poolclass=NullPool, hide_parameters=True)
        cleanup.push_async_callback(engine.dispose)
        flags = await fixture.connection.fetchrow(
            "SELECT rolsuper,rolcreaterole,rolcreatedb,rolbypassrls FROM pg_roles WHERE rolname=$1", name
        )
        assert not any(flags.values())

        async def freeze_preparation(handoff, preparation_roles):
            configuration_by_field = {
                key: handoff[key] for key in ("kind", "candidate_id", "catalog_schema", "storage_schema")
            }
            configuration_by_field["preparation_roles"] = preparation_roles
            async with engine.connect() as publisher:
                return await _freeze_registry_generation(publisher, configuration_by_field, handoff["candidate_id"])

        yield {"catalog_export": lambda **options: None, "freeze_preparation": freeze_preparation}
