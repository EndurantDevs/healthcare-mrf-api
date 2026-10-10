# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact disposable principals for the existing opt-in scoped catalog databases."""

from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from process import reference_family_archive as native


async def _remove_catalog_actors(engine, roles, owned_schemas):
    """Remove only caller-registered schemas and namespaces owned by these new UUID roles."""
    if not roles:
        return
    async with engine.begin() as connection:
        namespaces = (
            (
                await connection.execute(
                    text(
                        "SELECT nspname FROM pg_namespace namespace JOIN pg_roles owner ON owner.oid=namespace.nspowner "
                        "WHERE owner.rolname=ANY(CAST(:roles AS text[])) ORDER BY nspname"
                    ),
                    {"roles": list(roles)},
                )
            )
            .scalars()
            .all()
        )
        for name in sorted(set(owned_schemas) | set(namespaces)):
            native._schema_name(name)
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
        for role in reversed(roles):
            if await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            ):
                await connection.execute(text(f'REVOKE CREATE ON DATABASE "{engine.url.database}" FROM "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))
        assert not await connection.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[])))"),
            {"roles": list(roles)},
        )


async def _create_catalog_actors(engine, roles, attempted, login_password, owned_schemas):
    async with engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention')")) is None
        for schema in owned_schemas:
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
        for kind, role in roles.items():
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )
            attempted.append(role)
            await connection.execute(
                text(
                    f'CREATE ROLE "{role}" {"NOLOGIN" if kind == "owner" else "LOGIN"} INHERIT '
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
                )
            )
            if kind != "owner":
                driver = (await connection.get_raw_connection()).driver_connection
                try:
                    await driver.execute(f"ALTER ROLE \"{role}\" PASSWORD '{login_password}'")
                except Exception:
                    raise RuntimeError("catalog principal bootstrap failed") from None
        for kind in ("owner", "builder"):
            await connection.execute(text(f'GRANT "{roles[kind]}" TO "{roles["publisher"]}"'))
            await connection.execute(text(f'GRANT CREATE ON DATABASE "{engine.url.database}" TO "{roles[kind]}"'))
        await connection.execute(text(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{roles["owner"]}"'))


@asynccontextmanager
async def catalog_actors(engine, owned_schemas):
    """Use genuine LOGIN sessions and register bounded cleanup before role creation."""
    suffix = uuid4().hex
    roles_by_kind = {kind: f"catalog_{kind}_{suffix}" for kind in ("owner", "builder", "publisher")}
    attempted_roles = []
    login_password = uuid4().hex
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(_remove_catalog_actors, engine, attempted_roles, owned_schemas)
        await _create_catalog_actors(engine, roles_by_kind, attempted_roles, login_password, owned_schemas)
        sessions_by_kind = {}
        for kind in ("builder", "publisher"):
            actor_engine = create_async_engine(
                engine.url.set(username=roles_by_kind[kind], password=login_password),
                poolclass=NullPool,
                hide_parameters=True,
            )
            cleanup.push_async_callback(actor_engine.dispose)
            sessions_by_kind[kind] = async_sessionmaker(actor_engine, expire_on_commit=False)
        yield SimpleNamespace(engine=engine, roles=roles_by_kind, **sessions_by_kind)


async def seal_catalog(resources, schema, models, generation_table):
    """Transfer a registered fixture family, then run the actual publisher custody guard."""
    owner = resources.roles["owner"]
    builder = resources.roles["builder"]
    publisher = resources.roles["publisher"]
    names = tuple(model.__tablename__ for model in models) + (generation_table,)
    async with resources.engine.begin() as connection:
        await connection.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
        await connection.execute(text(f'REVOKE ALL ON SCHEMA "{schema}" FROM PUBLIC,"{builder}","{publisher}"'))
        await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{builder}"'))
        for name in names:
            await connection.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{owner}"'))
            await connection.execute(text(f'REVOKE ALL ON "{schema}"."{name}" FROM PUBLIC,"{builder}","{publisher}"'))
            await connection.execute(text(f'GRANT SELECT ON "{schema}"."{name}" TO "{builder}"'))
    async with resources.publisher.begin() as session:
        ownership = SimpleNamespace(
            schema_name=schema,
            schema_oid=await native._schema_oid(session, schema),
            relation_oids=tuple([(name, await native._relation_oid(session, schema, name)) for name in names]),
            sequence_oids=(),
        )
        await native.seal_model_family_storage(session, ownership, await native.protected_publisher_owner(session))
    async with resources.builder.begin() as session:
        for name in names:
            assert await session.scalar(
                text("SELECT has_table_privilege(current_user,CAST(:name AS regclass),'SELECT')"),
                {"name": f'"{schema}"."{name}"'},
            )
            assert not await session.scalar(
                text(
                    "SELECT has_table_privilege(current_user,CAST(:name AS regclass),'INSERT,UPDATE,DELETE,TRUNCATE')"
                ),
                {"name": f'"{schema}"."{name}"'},
            )
