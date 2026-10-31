# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native lock and authority paths; synthetic objects do not qualify CMS authority."""

import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from db.migration_cms_snapshot_allocator import allocator_bodies
from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_canonical_writer_fence as writers
from process import provider_directory_generic_source_release_checks as release_checks


@asynccontextmanager
async def _catalog():
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for native lock verification")
    url = make_url(dsn)
    assert url.host in {"127.0.0.1", "localhost"} and url.port is not None and not url.query
    connection = await asyncpg.connect(dsn, command_timeout=5)
    other = None
    schema = "cms_writer_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f'CREATE TABLE "{schema}".parent(id int PRIMARY KEY) PARTITION BY LIST(id)')
        await connection.execute(f'CREATE TABLE "{schema}".child PARTITION OF "{schema}".parent FOR VALUES IN (1)')
        await connection.execute(f'CREATE TABLE "{schema}".history(id int PRIMARY KEY)')
        await connection.execute(f'CREATE TABLE "{schema}".next_child(LIKE "{schema}".parent INCLUDING ALL)')
        await connection.execute(f'INSERT INTO "{schema}".parent VALUES(1)')
        await connection.execute(f'INSERT INTO "{schema}".history VALUES(2)')
        other = await asyncpg.connect(dsn, command_timeout=5)
        await other.execute("SET lock_timeout='75ms'")
        await other.execute("SET statement_timeout='500ms'")
        oids = await connection.fetch(
            "SELECT oid::bigint FROM pg_catalog.pg_class WHERE relnamespace=$1::regnamespace "
            "AND relname=ANY($2::text[]) ORDER BY oid",
            schema,
            ["parent", "child", "history"],
        )
        database_oid = await connection.fetchval(
            "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()"
        )
        native_by_field = {
            "relations": [dict(relation_row) for relation_row in oids],
            "actor": {"database_oid": database_oid},
        }
        yield connection, other, schema, native_by_field
    finally:
        if other is not None:
            await other.close()
        if connection.is_in_transaction():
            await connection.execute("ROLLBACK")
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()


def _lock(schema):
    return (
        'LOCK TABLE ONLY "'
        + schema
        + '".parent,ONLY "'
        + schema
        + '".child,ONLY "'
        + schema
        + '".history IN SHARE ROW EXCLUSIVE MODE NOWAIT'
    )


@asynccontextmanager
async def _allocator_guard_catalog(guard_version):
    async with _catalog() as (connection, other, schema, native):
        role_by_kind = {kind: schema + "_" + kind for kind in ("guard", "namespace")}
        original_owner = await connection.fetchval(
            "SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname=$1", schema
        )
        body_by_name = {
            "prepare_pd_generic_dataset_storage": allocator_bodies(schema)[0],
            "guard_pd_dataset_candidate_parent": _canonical_copy_guard_bodies(schema, "hp_snapshot_retention")[1]
            if guard_version == "v1"
            else _cms_canonical_copy_guard_body(schema, "hp_snapshot_retention"),
        }
        try:
            for role in role_by_kind.values():
                await connection.execute(f"CREATE ROLE {storage_identifier(role)} NOLOGIN")
            await connection.execute(
                f'ALTER SCHEMA "{schema}" OWNER TO {storage_identifier(role_by_kind["namespace"])}'
            )
            for name, body in body_by_name.items():
                signature = storage_relation(schema, name) + "()"
                await connection.execute(
                    f"CREATE FUNCTION {signature} RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER "
                    f"SET search_path=pg_catalog AS $fn${body}$fn$"
                )
                await connection.execute(f"REVOKE EXECUTE ON FUNCTION {signature} FROM PUBLIC")
                owner = role_by_kind["guard" if name.startswith("guard_") else "namespace"]
                await connection.execute(f"ALTER FUNCTION {signature} OWNER TO {storage_identifier(owner)}")
            guard = storage_relation(schema, "guard_pd_dataset_candidate_parent") + "()"
            await connection.execute(
                f"GRANT EXECUTE ON FUNCTION {guard} TO {storage_identifier(role_by_kind['namespace'])}"
            )
            yield connection, other, schema, role_by_kind, native
        finally:
            if connection.is_in_transaction():
                await connection.execute("ROLLBACK")
            for name in body_by_name:
                await connection.execute(f"DROP FUNCTION IF EXISTS {storage_relation(schema, name)}()")
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_catalog.pg_proc WHERE pronamespace=$1::regnamespace "
                "AND proname=ANY($2::text[]))",
                schema,
                list(body_by_name),
            )
            await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO {storage_identifier(original_owner)}')
            assert (
                await connection.fetchval(
                    "SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace WHERE nspname=$1", schema
                )
                == original_owner
            )
            for role in role_by_kind.values():
                await connection.execute(f"DROP ROLE IF EXISTS {storage_identifier(role)}")
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname=ANY($1::text[]))",
                list(role_by_kind.values()),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("guard_version", ["v1", "v2"])
async def test_allocator_guard_native_row_pin_blocks_definition_changes(guard_version):
    """Qualify the allocator compatibility query and row lock, not CMS profile authority."""
    async with _allocator_guard_catalog(guard_version) as (connection, other, schema, role_by_kind, native):
        frontier = writers.callables.cms_callee_signatures(schema)
        relation_oids = [entry["oid"] for entry in native["relations"]]
        original_relations = await connection.fetch(writers._IDENTITY_SQL, relation_oids)
        namespace = await connection.fetchrow(
            "SELECT oid::bigint,nspowner::bigint FROM pg_catalog.pg_namespace WHERE nspname=$1", schema
        )
        owner_by_name = {
            role_record["rolname"]: role_record["oid"]
            for role_record in await connection.fetch(
                "SELECT oid::bigint,rolname FROM pg_catalog.pg_roles WHERE rolname=ANY($1::text[]) AND NOT rolcanlogin",
                list(role_by_kind.values()),
            )
        }
        assert len(owner_by_name) == 2 and namespace["nspowner"] == owner_by_name[role_by_kind["namespace"]]
        guard_owner = owner_by_name[role_by_kind["guard"]]
        assert guard_owner != namespace["nspowner"]
        name = "guard_pd_dataset_candidate_parent"
        allocator_signature = storage_relation(schema, "prepare_pd_generic_dataset_storage") + "()"
        allocator_oid = await connection.fetchval("SELECT $1::regprocedure::oid::bigint", allocator_signature)
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.execute("SET LOCAL search_path=pg_catalog")
            guard_rows = await connection.fetch(
                writers.callables._FUNCTION_SQL, [storage_relation(schema, name) + "()"], schema, [name], []
            )
            assert len(guard_rows) == 1
            guard = guard_rows[0]
            release_checks._require_native_function(
                writers.callables._decoded(guard["definition"]),
                release_checks._LEGACY_FUNCTIONS[name + "()"],
                guard["language"],
                schema,
                namespace["oid"],
                {guard_owner},
            )
            allocator = await writers._guard_allocator(connection, {"catalog_schema": schema}, guard_rows)
            release_checks._require_allocator_guard_acl(
                guard, allocator, schema, namespace["oid"], set(owner_by_name.values()), namespace["nspowner"]
            )
            definition = writers.callables._decoded(allocator["definition"])
            assert int(definition["oid"]) == allocator_oid and int(definition["proowner"]) == namespace["nspowner"]
            assert writers.callables.cms_callee_signatures(schema) == frontier and len(frontier) == 32
            assert allocator_signature not in frontier
            with pytest.raises(asyncpg.LockNotAvailableError) as blocked:
                await other.execute(f"ALTER FUNCTION {allocator_signature} COST 101")
            assert blocked.value.sqlstate == "55P03"
        finally:
            await transaction.rollback()
        assert await other.execute(f"ALTER FUNCTION {allocator_signature} COST 101") == "ALTER FUNCTION"
        await other.execute(f"ALTER FUNCTION {allocator_signature} COST 100")
        assert await connection.fetch(writers._IDENTITY_SQL, relation_oids) == original_relations


@pytest.mark.asyncio
async def test_native_fence_preserves_selects_and_blocks_writers_and_maintenance():
    async with _catalog() as (connection, other, schema, native):
        await connection.execute("BEGIN")
        actor = await connection.fetchrow(writers._ACTOR_SQL)
        assert actor["backend"] == connection.get_server_pid() and actor["isolation"] == "read committed"
        oids = [entry["oid"] for entry in native["relations"]]
        assert await connection.fetchval(writers._OWNERS_SQL, oids, schema, "migration") is False
        assert await connection.fetchval(writers._DEPENDENCIES_SQL, oids, "migration", schema) is True
        await connection.execute(_lock(schema))
        await writers._require_locks(connection, native)
        assert await other.fetchval(f'SELECT count(*) FROM "{schema}".parent') == 1
        assert await other.fetchval(f'SELECT count(*) FROM "{schema}".history') == 1
        for statement in (
            f'UPDATE "{schema}".parent SET id=id',
            f'UPDATE "{schema}".child SET id=id',
            f'TRUNCATE "{schema}".history',
            f'ANALYZE "{schema}".history',
            f'VACUUM "{schema}".history',
            f'REINDEX TABLE "{schema}".history',
            f'ALTER TABLE "{schema}".parent ADD COLUMN value int',
            f'ALTER TABLE "{schema}".parent ATTACH PARTITION "{schema}".next_child FOR VALUES IN (3)',
        ):
            with pytest.raises(asyncpg.LockNotAvailableError):
                await other.execute(statement)
        await connection.execute("ROLLBACK")
        assert await other.execute(f'UPDATE "{schema}".child SET id=id') == "UPDATE 1"


@pytest.mark.asyncio
async def test_busy_savepoint_rollback_and_reader_blocking_upgrade_are_not_valid_fences():
    async with _catalog() as (connection, other, schema, native):
        await other.execute("BEGIN")
        await other.execute(f'UPDATE "{schema}".parent SET id=id')
        await connection.execute("BEGIN")
        with pytest.raises(asyncpg.LockNotAvailableError):
            await connection.execute(_lock(schema))
        await connection.execute("ROLLBACK")
        await other.execute("ROLLBACK")
        await connection.execute("BEGIN")
        await connection.execute("SAVEPOINT fence")
        await connection.execute(_lock(schema))
        await writers._require_locks(connection, native)
        await connection.execute("ROLLBACK TO SAVEPOINT fence")
        with pytest.raises(ValueError, match="reader_compatible_locks_required"):
            await writers._require_locks(connection, native)
        await connection.execute(_lock(schema))
        await connection.execute(f'LOCK TABLE ONLY "{schema}".parent IN ACCESS EXCLUSIVE MODE NOWAIT')
        with pytest.raises(ValueError, match="reader_compatible_locks_required"):
            await writers._require_locks(connection, native)


async def _create_authority(connection, schema, wrapper_schema, role_by_kind):
    for kind, role in role_by_kind.items():
        await connection.execute(
            f'CREATE ROLE "{role}" ' + ("LOGIN" if kind in {"publisher", "migration"} else "NOLOGIN")
        )
    await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{role_by_kind["namespace"]}"')
    for name in ("parent", "child", "history"):
        await connection.execute(f'ALTER TABLE "{schema}".{name} OWNER TO "{role_by_kind["owner"]}"')
    await connection.execute(f'CREATE SCHEMA "{wrapper_schema}"')
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}","{wrapper_schema}" TO PUBLIC')
    await connection.execute(
        f"CREATE FUNCTION \"{schema}\".trusted() RETURNS int LANGUAGE sql IMMUTABLE SECURITY DEFINER AS 'SELECT 1'"
    )
    await connection.execute(f'ALTER FUNCTION "{schema}".trusted() OWNER TO "{role_by_kind["callee"]}"')
    return await connection.fetchval("SELECT $1::regprocedure::oid", f'"{schema}".trusted()')


@asynccontextmanager
async def _authority_catalog():
    async with _catalog() as (connection, _other, schema, native):
        role_by_kind = {
            kind: schema + "_" + kind
            for kind in ("owner", "namespace", "callee", "publisher", "executor", "bridge", "migration")
        }
        wrapper_schema = schema + "_wrappers"
        actor = None
        try:
            trusted_oid = await _create_authority(connection, schema, wrapper_schema, role_by_kind)
            actor = await asyncpg.connect(
                os.environ["HLTHPRT_CMS_CUSTODY_TEST_DSN"], user=role_by_kind["publisher"], command_timeout=5
            )
            yield SimpleNamespace(
                connection=connection,
                actor=actor,
                schema=schema,
                wrappers=wrapper_schema,
                roles=role_by_kind,
                relations=[entry["oid"] for entry in native["relations"]],
                trusted=trusted_oid,
            )
        finally:
            if actor is not None:
                await actor.close()
            await connection.execute(f'DROP SCHEMA IF EXISTS "{wrapper_schema}" CASCADE')
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            for role in reversed(role_by_kind.values()):
                await connection.execute(f'DROP ROLE IF EXISTS "{role}"')
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_roles WHERE rolname=ANY($1::text[])", list(role_by_kind.values())
                )
                == 0
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_namespace WHERE nspname=ANY($1::text[])", [schema, wrapper_schema]
                )
                == 0
            )


async def _wrapper(catalog, name="wrapper", owner="callee", security="DEFINER", public=True, body=None):
    body = body or f"EXECUTE 'ALTER FUNCTION \"{catalog.schema}\".trusted() VOLATILE';"
    signature = f'"{catalog.wrappers}"."{name}"()'
    await catalog.connection.execute(
        f"CREATE FUNCTION {signature} RETURNS void LANGUAGE plpgsql SECURITY {security} SET search_path=pg_catalog AS $wrapper$ BEGIN {body} END $wrapper$"
    )
    await catalog.connection.execute(f'ALTER FUNCTION {signature} OWNER TO "{catalog.roles[owner]}"')
    if not public:
        await catalog.connection.execute(f"REVOKE EXECUTE ON FUNCTION {signature} FROM PUBLIC")
    return await catalog.connection.fetchval("SELECT $1::regprocedure::oid", signature)


async def _definers_safe(catalog):
    # This synthetic trusted OID exercises the catalog gate, not source enrollment.
    # The unit suite authenticates the real fixed frontier before deriving this input.
    return await catalog.connection.fetchval(
        writers._DEFINER_AUTHORITY_SQL,
        catalog.relations,
        [catalog.trusted],
        [catalog.trusted],
        catalog.schema,
        catalog.roles["migration"],
    )


async def _protected_owners_safe(catalog):
    assert (
        await catalog.connection.fetchval(
            writers._OWNERS_SQL, catalog.relations, catalog.schema, catalog.roles["migration"]
        )
        is True
    )
    assert (
        await catalog.connection.fetchval(
            writers.custody._CALLABLE_OWNER_SQL, [catalog.trusted], catalog.roles["migration"]
        )
        is True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("grant", ["public", "direct", "inherited", "set_role"])
async def test_dynamic_wrapper_bypasses_rooted_dependencies(grant):
    async with _authority_catalog() as catalog:
        assert await _definers_safe(catalog) is True
        wrapper_oid = await _wrapper(catalog, public=grant == "public")
        if grant != "public":
            grantee = catalog.roles["publisher" if grant == "direct" else "executor"]
            await catalog.connection.execute(f'GRANT EXECUTE ON FUNCTION "{catalog.wrappers}".wrapper() TO "{grantee}"')
            if grant in {"inherited", "set_role"}:
                options = " WITH INHERIT FALSE" if grant == "set_role" else ""
                await catalog.connection.execute(f'GRANT "{grantee}" TO "{catalog.roles["publisher"]}"{options}')
        await _protected_owners_safe(catalog)
        assert (
            await catalog.connection.fetchval(
                "SELECT pg_has_role($1,$2,'MEMBER')", catalog.roles["publisher"], catalog.roles["callee"]
            )
            is False
        )
        assert (
            await catalog.connection.fetchval(
                "SELECT count(*) FROM pg_depend WHERE classid='pg_proc'::regclass AND refclassid='pg_proc'::regclass AND objid=$1 AND refobjid=$2",
                wrapper_oid,
                catalog.trusted,
            )
            == 0
        )
        rooted = await catalog.connection.fetch(writers.custody._ROUTINES_SQL, catalog.relations, [catalog.trusted])
        assert wrapper_oid not in {entry["oid"] for entry in rooted}
        assert await _definers_safe(catalog) is False
        assert (
            await catalog.actor.fetchval(
                "SELECT session_user=current_user AND NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user)"
            )
            is True
        )
        if grant == "set_role":
            assert (
                await catalog.actor.fetchval("SELECT has_function_privilege($1::oid,'EXECUTE')", wrapper_oid) is False
            )
            await catalog.actor.execute(f'SET ROLE "{catalog.roles["executor"]}"')
        await catalog.actor.execute(f'SELECT "{catalog.wrappers}".wrapper()')
        assert (
            await catalog.connection.fetchval("SELECT provolatile::text FROM pg_proc WHERE oid=$1", catalog.trusted)
            == "v"
        )
        await catalog.connection.execute(f'ALTER FUNCTION "{catalog.schema}".trusted() IMMUTABLE')
        assert await _definers_safe(catalog) is False


@pytest.mark.asyncio
async def test_unknown_invoker_keeps_caller_authority():
    async with _authority_catalog() as catalog:
        await _wrapper(catalog, security="INVOKER")
        assert await _definers_safe(catalog) is True
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await catalog.actor.execute(f'SELECT "{catalog.wrappers}".wrapper()')
        assert (
            await catalog.connection.fetchval("SELECT provolatile::text FROM pg_proc WHERE oid=$1", catalog.trusted)
            == "i"
        )
        assert await _definers_safe(catalog) is True


@pytest.mark.asyncio
async def test_trusted_body_does_not_expand_authority():
    async with _authority_catalog() as catalog:
        await _wrapper(catalog, public=False)
        assert await catalog.actor.fetchval(f'SELECT "{catalog.schema}".trusted()') == 1
        assert await _definers_safe(catalog) is True
        await catalog.connection.execute(
            f'GRANT EXECUTE ON FUNCTION "{catalog.wrappers}".wrapper() TO "{catalog.roles["migration"]}"'
        )
        assert await _definers_safe(catalog) is True
        await catalog.connection.execute(
            f'GRANT EXECUTE ON FUNCTION "{catalog.wrappers}".wrapper() TO "{catalog.roles["publisher"]}"'
        )
        assert await _definers_safe(catalog) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner",
    ["owner", "namespace", "callee", "inherited", "SUPERUSER", "CREATEROLE", "CREATEDB", "REPLICATION", "BYPASSRLS"],
)
async def test_definer_owner_authority_is_closed(owner):
    async with _authority_catalog() as catalog:
        wrapper_owner = owner if owner in {"owner", "namespace", "callee"} else "bridge"
        if owner == "inherited":
            await catalog.connection.execute(f'GRANT "{catalog.roles["callee"]}" TO "{catalog.roles["bridge"]}"')
        elif owner.isupper():
            await catalog.connection.execute(f'ALTER ROLE "{catalog.roles["bridge"]}" {owner}')
        await _wrapper(catalog, owner=wrapper_owner, body="RETURN;")
        await _protected_owners_safe(catalog)
        assert await _definers_safe(catalog) is False


@pytest.mark.asyncio
async def test_definer_chain_reaches_private_grant():
    async with _authority_catalog() as catalog:
        inner_oid = await _wrapper(catalog, name="inner_wrapper", public=False)
        await catalog.connection.execute(
            f'GRANT EXECUTE ON FUNCTION "{catalog.wrappers}".inner_wrapper() TO "{catalog.roles["bridge"]}"'
        )
        await _wrapper(catalog, owner="bridge", body=f'PERFORM "{catalog.wrappers}".inner_wrapper();')
        assert (
            await catalog.connection.fetchval(
                "SELECT has_function_privilege($1::text,$2::oid,'EXECUTE')", catalog.roles["publisher"], inner_oid
            )
            is False
        )
        await _protected_owners_safe(catalog)
        assert await _definers_safe(catalog) is False
        await catalog.actor.execute(f'SELECT "{catalog.wrappers}".wrapper()')
        assert (
            await catalog.connection.fetchval("SELECT provolatile::text FROM pg_proc WHERE oid=$1", catalog.trusted)
            == "v"
        )


@pytest.mark.asyncio
async def test_catalog_namespace_is_not_an_exemption():
    async with _authority_catalog() as catalog:
        name = catalog.schema
        try:
            wrapper_oid = await _wrapper(catalog, name=name)
            await catalog.connection.execute(f'ALTER FUNCTION "{catalog.wrappers}"."{name}"() SET SCHEMA pg_catalog')
            assert wrapper_oid >= 16384
            assert await _definers_safe(catalog) is False
        finally:
            await catalog.connection.execute(f'DROP FUNCTION IF EXISTS pg_catalog."{name}"()')


@pytest.mark.asyncio
async def test_login_owner_remains_disallowed():
    async with _authority_catalog() as catalog:
        await catalog.connection.execute(f'ALTER ROLE "{catalog.roles["callee"]}" LOGIN')
        assert (
            await catalog.connection.fetchval(
                writers.custody._CALLABLE_OWNER_SQL, [catalog.trusted], catalog.roles["migration"]
            )
            is False
        )


@asynccontextmanager
async def _history_catalog():
    async with _catalog() as (connection, other, schema, native):
        witness, relationship = writers.history.ROOTS
        await connection.execute(
            f'CREATE TABLE "{schema}"."{witness}"(id int PRIMARY KEY REFERENCES "{schema}".history(id) ON DELETE CASCADE)'
        )
        await connection.execute(
            f'CREATE TABLE "{schema}"."{relationship}"(id int PRIMARY KEY REFERENCES "{schema}"."{witness}"(id) ON DELETE CASCADE)'
        )
        for root in writers.history.ROOTS:
            await connection.execute(f'INSERT INTO "{schema}"."{root}" VALUES(2)')
        history_oids = await connection.fetchval(
            "SELECT array_agg(oid ORDER BY oid) FROM pg_class WHERE relnamespace=$1::regnamespace AND relname=ANY($2::text[])",
            schema,
            list(writers.history.ROOTS),
        )
        yield connection, other, schema, history_oids, native["actor"]["database_oid"]


def _history_lock(schema):
    return (
        "LOCK TABLE "
        + ",".join(f'ONLY "{schema}"."{root}"' for root in sorted(writers.history.ROOTS))
        + " IN SHARE MODE NOWAIT"
    )


@pytest.mark.asyncio
async def test_history_share_blocks_all_writers():
    async with _history_catalog() as (connection, other, schema, oids, database_oid):
        witness, relationship = writers.history.ROOTS
        await connection.execute("BEGIN")
        await connection.execute(_history_lock(schema))
        assert await connection.fetchval(writers._HISTORY_LOCKS_SQL, oids, database_oid) is True
        held = await connection.fetchval(
            "SELECT array_agg(relation ORDER BY relation) FROM pg_locks WHERE pid=pg_backend_pid() AND mode='ShareLock'"
        )
        assert held == sorted(oids)
        for root in writers.history.ROOTS:
            assert await other.fetchval(f'SELECT count(*) FROM "{schema}"."{root}"') == 1
        for statement in (
            f'UPDATE "{schema}"."{witness}" SET id=id',
            f'INSERT INTO "{schema}"."{relationship}" VALUES(2) ON CONFLICT DO NOTHING',
            f'DELETE FROM "{schema}"."{relationship}"',
            f'DELETE FROM "{schema}".history WHERE id=2',
            f'TRUNCATE "{schema}"."{relationship}"',
            f'TRUNCATE "{schema}".history CASCADE',
            f'ALTER TABLE "{schema}"."{witness}" ADD COLUMN changed int',
            f'DROP TABLE "{schema}"."{relationship}"',
        ):
            with pytest.raises(asyncpg.LockNotAvailableError):
                await other.execute(statement)
        await connection.execute("COMMIT")
        assert await other.execute(f'UPDATE "{schema}"."{witness}" SET id=id') == "UPDATE 1"


@pytest.mark.asyncio
async def test_history_nowait_and_savepoint_release():
    async with _history_catalog() as (connection, other, schema, oids, database_oid):
        await other.execute("BEGIN")
        await other.execute(f'UPDATE "{schema}"."{writers.history.ROOTS[0]}" SET id=id')
        await connection.execute("BEGIN")
        with pytest.raises(asyncpg.LockNotAvailableError):
            await connection.execute(_history_lock(schema))
        await connection.execute("ROLLBACK")
        await other.execute("ROLLBACK")
        await connection.execute("BEGIN")
        await connection.execute("SAVEPOINT history")
        await connection.execute(_history_lock(schema))
        assert await connection.fetchval(writers._HISTORY_LOCKS_SQL, oids, database_oid) is True
        await connection.execute("ROLLBACK TO SAVEPOINT history")
        assert await connection.fetchval(writers._HISTORY_LOCKS_SQL, oids, database_oid) is False
        await connection.execute(_history_lock(schema))
        await connection.execute(_history_lock(schema).replace("SHARE MODE", "SHARE ROW EXCLUSIVE MODE"))
        assert await connection.fetchval(writers._HISTORY_LOCKS_SQL, oids, database_oid) is True


@pytest.mark.asyncio
async def test_history_cancellation_keeps_caller_rollback():
    async with _history_catalog() as (connection, other, schema, _oids, _database_oid):
        ready = asyncio.Event()

        async def hold_history():
            await connection.execute("BEGIN")
            try:
                await connection.execute(_history_lock(schema))
                ready.set()
                await asyncio.Event().wait()
            finally:
                await connection.execute("ROLLBACK")

        task = asyncio.create_task(hold_history())
        try:
            await asyncio.wait_for(ready.wait(), timeout=2)
            with pytest.raises(asyncpg.LockNotAvailableError):
                await other.execute(f'DELETE FROM "{schema}".history WHERE id=2')
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await other.execute(f'DELETE FROM "{schema}".history WHERE id=2') == "DELETE 1"


@asynccontextmanager
async def _borrowed_session(database, session):
    async with database.bind_existing_session(session):
        try:
            yield
        finally:
            # The bridge requires an active transaction at exit; opening this
            # empty cleanup scope does no IO and occurs after the assertions.
            if not session.in_transaction():
                await session.begin()


@asynccontextmanager
async def _borrowed_history():
    url = make_url(os.environ["HLTHPRT_CMS_CUSTODY_TEST_DSN"]).set(drivername="postgresql+asyncpg")
    engine, database, queries = create_async_engine(url), Database(), []
    database._database_override = url.database

    def sqlalchemy_query(_connection, _cursor, statement, _parameters, _context, _many):
        queries.append(statement)

    def native_query(entry):
        queries.append(entry.query)

    event.listen(engine.sync_engine, "before_cursor_execute", sqlalchemy_query)
    try:
        async with AsyncSession(bind=engine) as session:
            await session.begin()
            await session.execute(text("SET LOCAL search_path=pg_catalog"))
            physical = await session.connection()
            connection = (await physical.get_raw_connection()).driver_connection
            connection.add_query_logger(native_query)
            try:
                async with _borrowed_session(database, session):
                    yield database, session, connection, queries
            finally:
                connection.remove_query_logger(native_query)
                await session.rollback()
    finally:
        await engine.dispose()


async def _synthetic_history_preparation(connection, schema):
    """Fixture-owned identities exercise locks/binding, not protected CMS issuance."""
    family = await writers.custody._inventory(connection, schema, ["history", *writers.history.ROOTS])
    native_by_field = dict(
        actor=dict(await connection.fetchrow(writers._ACTOR_SQL)),
        deadline=asyncio.get_running_loop().time() + 30,
        family=family,
        inventory_names=sorted(entry["relname"] for entry in family),
        relations=writers._identity(family),
        arguments={"catalog_schema": schema},
        closure=None,
    )
    return writers._WriterPreparation(connection, asyncio.current_task(), writers._encoded(native_by_field))


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", ["commit", "rollback", "restart"])
async def test_history_genuine_transaction_binding(monkeypatch, finish):
    async with _history_catalog() as (_admin, _other, schema, _oids, _database_oid), _borrowed_history() as transport:
        database, session, connection, queries = transport
        preparation = await _synthetic_history_preparation(connection, schema)
        # Authority qualification is covered by source-bound units and the
        # preserved native SD proof; this fixture tests the real physical bridge.
        monkeypatch.setattr(writers, "_closure", AsyncMock(return_value=None))
        async with writers.cms_canonical_history_fence(
            connection,
            preparation,
            database=database,
            session=session,
            deadline=asyncio.get_running_loop().time() + 20,
        ) as fence:
            await writers.require_cms_canonical_history_fence(connection, fence)
            await asyncio.sleep(0)
            queries.clear()
            with pytest.raises(ValueError, match="original_task_required"):
                await asyncio.create_task(writers.require_cms_canonical_history_fence(connection, fence))
            await asyncio.sleep(0)
            assert queries == []
            await (session.rollback() if finish == "rollback" else session.commit())
            if finish == "restart":
                await session.begin()
                await session.execute(text("SELECT 1"))
            await asyncio.sleep(0)
            queries.clear()
            with pytest.raises(ValueError, match="transaction_required"):
                await writers.require_cms_canonical_history_fence(connection, fence)
            await asyncio.sleep(0)
            assert queries == []
        await asyncio.sleep(0)
        assert fence.history_scope is None and queries == []
