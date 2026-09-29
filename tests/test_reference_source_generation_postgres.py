# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source revision guards on synthetic reference and address families."""

from contextlib import asynccontextmanager
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import mrf_address_source_generation as address_source
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from process import reference_source_generation as source_generation
from tests.test_reference_family_result_generation_postgres import (
    _GEO_MIGRATION_PATH,
    _database_url,
    _migration_module,
    _run_migration,
    _upgrade_reference_generation_chain,
)

_MIGRATION = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260929040000_reference_source_generation_guard.py"
)


@asynccontextmanager
async def _source_family(monkeypatch, *, migrate=True):
    schema = "reference_source_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    engine = create_async_engine(_database_url())
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            for name in (*generation.RELATION_NAMES_BY_IMPORTER["mrf"], "geo_zip_lookup"):
                await connection.execute(text(f'CREATE TABLE "{schema}"."{name}" (id bigint, marker text)'))
                await connection.execute(text(f'INSERT INTO "{schema}"."{name}" VALUES (1, \'initial\')'))
            await _upgrade_reference_generation_chain(connection)
            await _run_migration(connection, _GEO_MIGRATION_PATH, "upgrade")
            if migrate:
                await _run_migration(connection, _MIGRATION, "upgrade")
        yield engine, schema, async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def _observe(session, schema, importer="geo", *, bootstrap=False):
    oids = await generation.current_reference_family_relation_oids(session, importer_id=importer, schema_name=schema)
    if importer == "mrf-address":
        observer = (
            address_source.bootstrap_mrf_address_source_generation
            if bootstrap
            else address_source.require_mrf_address_source_generation
        )
        return await observer(session, schema_name=schema, expected_relation_oids=oids)
    observer = (
        source_generation.bootstrap_reference_source_generation
        if bootstrap
        else source_generation.require_reference_source_generation
    )
    return await observer(session, importer_id=importer, schema_name=schema, expected_relation_oids=oids)


async def _read_authority(connection, schema, importer="geo"):
    return await generation.read_reference_family_result_generation_authority(
        connection, importer_id=importer, schema_name=schema
    )


async def test_legacy_bootstrap_preserves_migration_evidence(monkeypatch):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, sessions):
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f'UPDATE "{schema}".reference_family_result_generation SET local_generation=4, '
                    "origin_lineage_id=local_lineage_id, origin_generation=4, published_at=transaction_timestamp(), "
                    f"relation_oids=ARRAY['\"{schema}\".geo_zip_lookup'::regclass::oid::bigint] WHERE importer_id='geo'"
                )
            )
            before = await _read_authority(connection, schema)
            await _run_migration(connection, _MIGRATION, "upgrade")
            assert await _read_authority(connection, schema) == before
        with pytest.raises(RuntimeError, match="tracking is unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema)
        async with sessions.begin() as session:
            boundary = await _observe(session, schema, bootstrap=True)
            assert boundary.local_generation == 5
            assert boundary.relation_oids == before.relation_oids
            assert await _observe(session, schema, bootstrap=True) == boundary
        async with sessions.begin() as session:
            assert await _observe(session, schema) == boundary
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(await session.connection(), _MIGRATION, "downgrade")


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO {table} VALUES (2, 'inserted')",
        "UPDATE {table} SET marker='updated'",
        "DELETE FROM {table}",
        "TRUNCATE {table}",
    ],
)
async def test_shared_address_writes_advance_both_families(monkeypatch, statement):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            before_by_importer = {
                importer: await _observe(session, schema, importer, bootstrap=True)
                for importer in ("mrf", "mrf-address", "geo")
            }
        async with engine.begin() as connection:
            await connection.execute(text(statement.format(table=f'"{schema}".mrf_address')))
        async with sessions.begin() as session:
            for importer, before in before_by_importer.items():
                after = await _observe(session, schema, importer)
                assert after.relation_oids == before.relation_oids
                assert after.local_generation == before.local_generation + (importer != "geo")
        with pytest.raises(RuntimeError, match="rollback"):
            async with engine.begin() as connection:
                await connection.execute(text(f"UPDATE \"{schema}\".mrf_address SET marker='rollback'"))
                raise RuntimeError("rollback")
        async with sessions.begin() as session:
            assert (await _observe(session, schema, "mrf")).local_generation == before_by_importer[
                "mrf"
            ].local_generation + 1


async def test_publish_and_adopt_install_new_oid_guards(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            first = await _observe(session, schema, bootstrap=True)
        async with sessions.begin() as session:
            await session.execute(text(f'ALTER TABLE "{schema}".geo_zip_lookup RENAME TO retained_geo'))
            await session.execute(text(f'CREATE TABLE "{schema}".geo_zip_lookup (id bigint, marker text)'))
            published = await generation.publish_local_reference_family_generation(
                session, importer_id="geo", schema_name=schema
            )
            assert published.relation_oids != first.relation_oids
            assert await _observe(session, schema) == published
        async with engine.begin() as connection:
            await connection.execute(text(f"UPDATE \"{schema}\".retained_geo SET marker='old'"))
            assert await _read_authority(connection, schema) == published
        incoming_by_field = {
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 17,
            "published_at": "2026-09-01T00:00:00Z",
        }
        async with sessions.begin() as session:
            await session.execute(text(f'ALTER TABLE "{schema}".geo_zip_lookup RENAME TO retained_second_geo'))
            await session.execute(text(f'CREATE TABLE "{schema}".geo_zip_lookup (id bigint, marker text)'))
            adopted = await generation.publish_adopted_reference_family_generation(
                session,
                importer_id="geo",
                schema_name=schema,
                source_generation=incoming_by_field,
                source_revision_tracked=True,
            )
            assert (await _observe(session, schema)).serving_generation == adopted.serving_generation
        async with engine.begin() as connection:
            await connection.execute(text(f"INSERT INTO \"{schema}\".geo_zip_lookup VALUES (1, 'local')"))
        async with sessions.begin() as session:
            changed = await _observe(session, schema)
            assert changed.serving_generation.origin_lineage_id == adopted.local_lineage_id
            assert changed.local_generation == adopted.local_generation + 1
            await generation.publish_adopted_reference_family_generation(
                session, importer_id="geo", schema_name=schema, source_generation=None
            )
        with pytest.raises(RuntimeError, match="tracking is unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema)


async def test_observation_holds_writer_fences(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            await _observe(session, schema, bootstrap=True)
        async with sessions.begin() as session:
            await _observe(session, schema)
            for statement in (
                f"UPDATE \"{schema}\".geo_zip_lookup SET marker='blocked'",
                f'TRUNCATE "{schema}".geo_zip_lookup',
                f'ALTER TABLE "{schema}".geo_zip_lookup RENAME TO moved',
                f'UPDATE "{schema}".reference_family_result_generation SET local_generation=local_generation+1',
            ):
                await _assert_writer_blocked(engine, statement)
        with pytest.raises(ValueError, match="read committed"):
            async with sessions.begin() as session:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                await _observe(session, schema)
        async with engine.begin() as writer:
            await writer.execute(text(f'LOCK TABLE "{schema}".geo_zip_lookup IN ROW EXCLUSIVE MODE'))
            with pytest.raises(DBAPIError) as blocked:
                async with sessions.begin() as session:
                    await _observe(session, schema)
            assert blocked.value.orig.sqlstate == "55P03"


async def _assert_writer_blocked(engine, statement):
    with pytest.raises(DBAPIError) as blocked:
        async with engine.begin() as writer:
            await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
            await writer.execute(text(statement))
    assert blocked.value.orig.sqlstate == "55P03"


@pytest.mark.parametrize("observed,other", [("geo", "mrf"), ("mrf", "geo")])
async def test_source_observation_does_not_block_unrelated_family(monkeypatch, observed, other):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            for importer in (observed, other):
                await _observe(session, schema, importer, bootstrap=True)
        async with sessions.begin() as observer:
            await _observe(observer, schema, observed)
            async with engine.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout='100ms'"))
                await generation.publish_local_reference_family_generation(
                    writer, importer_id=other, schema_name=schema
                )
        async with engine.begin() as writer:
            table = generation.RELATION_NAMES_BY_IMPORTER[other][0]
            await writer.execute(text(f'UPDATE "{schema}"."{table}" SET marker=\'other-family\''))
            async with sessions.begin() as observer:
                await observer.execute(text("SET LOCAL lock_timeout='100ms'"))
                await _observe(observer, schema, observed)


@pytest.mark.parametrize("bootstrap", [False, True])
async def test_source_observation_rejects_absent_authority(monkeypatch, bootstrap):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"DELETE FROM \"{schema}\".reference_family_result_generation WHERE importer_id='geo'")
            )
        with pytest.raises(RuntimeError, match="generation authority is unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema, bootstrap=bootstrap)


@pytest.mark.parametrize("bootstrap", [False, True])
async def test_source_observation_cannot_wait_behind_authority_ddl(monkeypatch, bootstrap):
    async with _source_family(monkeypatch) as (_engine, schema, sessions):
        async with sessions.begin() as migration:
            await migration.execute(
                text(f'LOCK TABLE "{schema}".reference_family_result_generation IN ACCESS EXCLUSIVE MODE')
            )
            with pytest.raises(DBAPIError) as blocked:
                async with sessions.begin() as observer:
                    await observer.execute(text("SET LOCAL statement_timeout='1s'"))
                    await _observe(observer, schema, bootstrap=bootstrap)
            assert blocked.value.orig.sqlstate == "55P03"


async def test_offline_source_guard_upgrade_handles_absent_optional_tables(monkeypatch):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, sessions):
        output = StringIO()
        migration = _migration_module(_MIGRATION)
        migration.op = Operations(
            MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
        )
        migration.upgrade()
        async with engine.begin() as connection:
            assert (
                await connection.scalar(text(f"SELECT to_regclass('\"{schema}\".lodes_workplace_aggregate')")) is None
            )
            raw = await connection.get_raw_connection()
            await raw.driver_connection.execute(output.getvalue())
        async with sessions.begin() as session:
            initial = await _observe(session, schema, bootstrap=True)
        async with engine.begin() as writer:
            await writer.execute(text(f"UPDATE \"{schema}\".geo_zip_lookup SET marker='offline-guarded'"))
        async with sessions.begin() as session:
            assert (await _observe(session, schema)).local_generation == initial.local_generation + 1


@pytest.mark.parametrize("mutation", ["DISABLE TRIGGER", "ENABLE TRIGGER", "DROP TRIGGER"])
async def test_observer_rejects_missing_or_weak_guards(monkeypatch, mutation):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            await _observe(session, schema, bootstrap=True)
        relation = f'"{schema}".geo_zip_lookup'
        trigger = source_generation.REVISION_TRIGGER
        statement = (
            f'DROP TRIGGER "{trigger}" ON {relation}'
            if mutation == "DROP TRIGGER"
            else f'ALTER TABLE {relation} {mutation} "{trigger}"'
        )
        async with engine.begin() as connection:
            await connection.execute(text(statement))
        with pytest.raises(RuntimeError, match="guards are unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema)


@asynccontextmanager
async def _tiger_publisher(engine, schema, *, can_publish=False):
    role = "reference_publisher_" + uuid4().hex
    owner = "reference owner " + uuid4().hex
    has_created_role = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            await connection.execute(text(f'CREATE ROLE "{owner}" NOLOGIN'))
            await connection.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{role}"'))
            for name in (*generation.RELATION_NAMES_BY_IMPORTER["mrf"], "geo_zip_lookup", generation.TABLE_NAME):
                await connection.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{role}"'))
            await connection.execute(text(f'CREATE SCHEMA tiger AUTHORIZATION "{owner}"'))
            for name in ("zip_state", "zcta5"):
                await connection.execute(text(f"CREATE TABLE tiger.{name} (id bigint)"))
                await connection.execute(text(f'ALTER TABLE tiger.{name} OWNER TO "{owner}"'))
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}",tiger TO "{role}"'))
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{owner}"'))
            await connection.execute(
                text(f'GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA tiger TO "{role}"')
            )
            if can_publish:
                await connection.execute(
                    text(f'GRANT SELECT,UPDATE ON "{schema}".reference_family_result_generation TO "{owner}"')
                )
        has_created_role = True
        yield role, owner
    finally:
        if has_created_role:
            async with engine.begin() as connection:
                await connection.execute(text("DROP SCHEMA tiger CASCADE"))
                for owned_role in (owner, role):
                    await connection.execute(text(f'DROP OWNED BY "{owned_role}"'))
                    await connection.execute(text(f'DROP ROLE "{owned_role}"'))


async def _install_tiger_guards(sessions, owner):
    async with sessions.begin() as session:
        await session.execute(text(f'SET LOCAL ROLE "{owner}"'))
        await source_generation.install_reference_revision_guards(session, importer_id="tiger", schema_name="tiger")


async def _migrate_tiger_as_app(engine, schema, role):
    async with engine.begin() as connection:
        namespace_before = (
            await connection.execute(text("SELECT nspowner,nspacl FROM pg_namespace WHERE nspname='tiger'"))
        ).one()
        before = (
            await connection.execute(
                text("SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace='tiger'::regnamespace ORDER BY oid")
            )
        ).all()
        await connection.execute(text(f'SET LOCAL ROLE "{role}"'))
        assert not await connection.scalar(text("SELECT has_schema_privilege(current_user,'tiger','CREATE')"))
        await _run_migration(connection, _MIGRATION, "upgrade")
        assert (
            await connection.execute(
                text("SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace='tiger'::regnamespace ORDER BY oid")
            )
        ).all() == before
        assert (
            await connection.execute(text("SELECT nspowner,nspacl FROM pg_namespace WHERE nspname='tiger'"))
        ).one() == namespace_before
        assert not await connection.scalar(
            text(
                f"SELECT source_revision_tracked FROM \"{schema}\".reference_family_result_generation WHERE importer_id='tiger'"
            )
        )
        assert not await connection.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid IN ('tiger.zip_state'::regclass,'tiger.zcta5'::regclass))"
            )
        )


async def _assert_function_grantees(session, schema, roles):
    acl = (
        await session.execute(
            text(
                "SELECT acl.grantee,acl.privilege_type,acl.is_grantable FROM pg_proc p, "
                "LATERAL aclexplode(p.proacl) acl WHERE p.oid=to_regprocedure(:function)"
            ),
            {"function": f'"{schema}"."{source_generation.REVISION_FUNCTION}"()'},
        )
    ).all()
    owner_ids = (
        (
            await session.execute(
                text("SELECT oid FROM pg_roles WHERE rolname=ANY(CAST(:roles AS text[]))"), {"roles": list(roles)}
            )
        )
        .scalars()
        .all()
    )
    assert len(owner_ids) == len(roles)
    assert sorted(acl) == sorted((owner_id, "EXECUTE", False) for owner_id in owner_ids)


async def test_tiger_upgrade_and_explicit_owner_bootstrap(monkeypatch):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, sessions):
        async with _tiger_publisher(engine, schema) as (role, owner):
            await _migrate_tiger_as_app(engine, schema, role)
            with pytest.raises(RuntimeError, match="tracking is unavailable"):
                async with sessions.begin() as session:
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await _observe(session, "tiger", "tiger")
            with pytest.raises(DBAPIError) as denied:
                async with sessions.begin() as session:
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await _observe(session, "tiger", "tiger", bootstrap=True)
            assert denied.value.orig.sqlstate == "42501"
            function = f'"{schema}"."{source_generation.REVISION_FUNCTION}"()'
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                await _assert_function_grantees(session, schema, (role, owner))
            await _install_tiger_guards(sessions, owner)
            with pytest.raises(RuntimeError, match="tracking is unavailable"):
                async with sessions.begin() as session:
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await _observe(session, "tiger", "tiger")
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                initial = await _observe(session, "tiger", "tiger", bootstrap=True)
                assert await _observe(session, "tiger", "tiger") == initial
                await session.execute(text(f'REVOKE EXECUTE ON FUNCTION {function} FROM "{owner}"'))
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{owner}"'))
                assert not await session.scalar(
                    text(f"SELECT has_function_privilege(current_user, '{function}', 'EXECUTE')")
                )
                assert not await session.scalar(
                    text(
                        f"SELECT has_table_privilege(current_user, '\"{schema}\".reference_family_result_generation', 'UPDATE')"
                    )
                )
                await session.execute(text("INSERT INTO tiger.zip_state VALUES (1)"))
                await session.execute(text("UPDATE tiger.zip_state SET id=2"))
                await session.execute(text("DELETE FROM tiger.zip_state"))
                await session.execute(text("TRUNCATE tiger.zcta5"))
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                changed = await _observe(session, "tiger", "tiger")
                assert changed.relation_oids == initial.relation_oids
                assert changed.local_generation == initial.local_generation + 4


async def test_tiger_preserves_protected_owner_boundary(monkeypatch):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, sessions):
        async with _tiger_publisher(engine, schema, can_publish=True) as (role, owner):
            await _migrate_tiger_as_app(engine, schema, role)
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{owner}"'))
                initial = await _observe(session, "tiger", "tiger", bootstrap=True)
                assert await _observe(session, "tiger", "tiger") == initial
            async with engine.begin() as connection:
                await connection.execute(text(f'SET LOCAL ROLE "{owner}"'))
                await connection.execute(text("ALTER TABLE tiger.zip_state RENAME TO retained_zip_state"))
                await connection.execute(text("CREATE TABLE tiger.zip_state (id bigint)"))
                await connection.execute(text(f'GRANT SELECT,INSERT,UPDATE,DELETE ON tiger.zip_state TO "{role}"'))
            with pytest.raises(DBAPIError) as denied:
                async with sessions.begin() as session:
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await generation.publish_local_reference_family_generation(
                        session, importer_id="tiger", schema_name="tiger"
                    )
            assert denied.value.orig.sqlstate == "42501"
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{owner}"'))
                replacement = await generation.publish_local_reference_family_generation(
                    session, importer_id="tiger", schema_name="tiger"
                )
                assert replacement.relation_oids != initial.relation_oids
            async with sessions.begin() as session:
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                assert await _observe(session, "tiger", "tiger") == replacement


@pytest.mark.parametrize(
    "mutation",
    [
        "DROP TABLE tiger.zcta5",
        "ALTER TABLE tiger.zcta5 SET UNLOGGED",
        "ALTER TABLE tiger.zcta5 ENABLE ROW LEVEL SECURITY",
        "CREATE TABLE tiger.child () INHERITS (tiger.zcta5)",
        "ALTER TABLE tiger.zcta5 RENAME TO retained; CREATE VIEW tiger.zcta5 AS SELECT * FROM tiger.retained",
    ],
)
async def test_tiger_migration_does_not_grant_for_unsupported_family(monkeypatch, mutation):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, sessions):
        async with _tiger_publisher(engine, schema) as (role, owner):
            async with engine.begin() as connection:
                for statement in mutation.split("; "):
                    await connection.execute(text(statement))
                await connection.execute(text(f'SET LOCAL ROLE "{role}"'))
                await _run_migration(connection, _MIGRATION, "upgrade")
                assert not await connection.scalar(
                    text("SELECT has_function_privilege(:owner, :function, 'EXECUTE')"),
                    {"owner": owner, "function": f'"{schema}"."{source_generation.REVISION_FUNCTION}"()'},
                )
            with pytest.raises(RuntimeError, match="tracking is unavailable"):
                async with sessions.begin() as session:
                    await source_generation.require_reference_revision_tracking(
                        session, importer_id="tiger", schema_name="tiger"
                    )


@asynccontextmanager
async def _second_tiger_owner(engine, owner):
    second_owner = "reference_second_owner_" + uuid4().hex
    has_created_role = False
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{second_owner}" NOLOGIN'))
            await connection.execute(text(f'ALTER TABLE tiger.zcta5 OWNER TO "{second_owner}"'))
        has_created_role = True
        yield second_owner
    finally:
        if has_created_role:
            async with engine.begin() as connection:
                await connection.execute(text(f'ALTER TABLE tiger.zcta5 OWNER TO "{owner}"'))
                await connection.execute(text(f'DROP OWNED BY "{second_owner}"'))
                await connection.execute(text(f'DROP ROLE "{second_owner}"'))


async def test_tiger_migration_grants_each_existing_table_owner(monkeypatch):
    async with _source_family(monkeypatch, migrate=False) as (engine, schema, _sessions):
        async with (
            _tiger_publisher(engine, schema) as (role, owner),
            _second_tiger_owner(engine, owner) as second_owner,
        ):
            await _migrate_tiger_as_app(engine, schema, role)
            async with engine.begin() as connection:
                await _assert_function_grantees(connection, schema, (role, owner, second_owner))


async def test_inherited_writes_cannot_bypass_revision(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            await _observe(session, schema, bootstrap=True)
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE TABLE "{schema}".parent (id bigint, marker text)'))
            await connection.execute(text(f'ALTER TABLE "{schema}".geo_zip_lookup INHERIT "{schema}".parent'))
        with pytest.raises(RuntimeError, match="guards are unavailable"):
            async with sessions.begin() as session:
                await _observe(session, schema)


async def test_untracked_incumbent_blocks_automatic_cutover(monkeypatch):
    async with _source_family(monkeypatch) as (_engine, schema, sessions):
        async with sessions.begin() as session:
            authority = await _observe(session, schema, bootstrap=True)
            incoming_by_field = {**authority.serving_generation.as_dict(), "origin_generation": 100}
            expected = SimpleNamespace(
                schema_name=schema, relation_oids=(("geo_zip_lookup", authority.relation_oids[0]),)
            )
            spec = archive.reference_family_spec("geo")
            await archive._require_automatic_cutover_generation(session, spec, expected, incoming_by_field)
            await session.execute(
                text(f'UPDATE "{schema}".reference_family_result_generation SET source_revision_tracked=FALSE')
            )
            with pytest.raises(RuntimeError, match="tracking is unavailable"):
                await archive._require_automatic_cutover_generation(session, spec, expected, incoming_by_field)


async def test_downgrade_cannot_race_bootstrap(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as bootstrap:
            await _observe(bootstrap, schema, bootstrap=True)
            with pytest.raises(DBAPIError) as blocked:
                async with engine.begin() as migration:
                    await migration.execute(text("SET LOCAL statement_timeout='1s'"))
                    await _run_migration(migration, _MIGRATION, "downgrade")
            assert blocked.value.orig.sqlstate == "55P03"
        async with sessions.begin() as session:
            assert await _observe(session, schema)


async def test_publication_holds_complete_source_fence(monkeypatch):
    async with _source_family(monkeypatch) as (engine, schema, sessions):
        async with sessions.begin() as session:
            await generation.publish_local_reference_family_generation(session, importer_id="mrf", schema_name=schema)
            for name in generation.RELATION_NAMES_BY_IMPORTER["mrf"]:
                await _assert_writer_blocked(engine, f'UPDATE "{schema}"."{name}" SET marker=\'blocked\'')
