# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine geocoder inheritance, source fences and protected flattened captures."""

import os
from contextlib import AsyncExitStack, asynccontextmanager
from secrets import token_hex
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import tiger_captured_epoch as captured
from tests.reference_family_generation_fixture import ReferenceSourceCustody


def _capture_admin_url(raw):
    """Admit loopback diagnostics or the explicitly enabled disposable CI service."""
    url = make_url(raw)
    mode = os.getenv("HLTHPRT_TIGER_CAPTURE_CI_TEST")
    if mode is not None:
        if not (
            mode == "1"
            and os.getenv("GITHUB_ACTIONS") == "true"
            and url.host in {"postgres", "127.0.0.1"}
            and url.port == 5432
            and url.username == "postgres"
            and url.database == "postgres"
            and url.drivername == "postgresql+asyncpg"
            and not url.query
        ):
            pytest.fail("captured-epoch CI tests require the explicit disposable PostgreSQL service")
    elif url.host not in {"127.0.0.1", "localhost"}:
        pytest.fail("captured-epoch tests require a local disposable database server")
    return url


@asynccontextmanager
async def owned_tiger_capture_database():
    """Register exact cleanup only after each fixture-owned resource is created."""
    raw = os.getenv("HLTHPRT_TIGER_CAPTURE_TEST_DSN")
    if not raw:
        pytest.skip("set HLTHPRT_TIGER_CAPTURE_TEST_DSN for native captured-epoch tests")
    url = _capture_admin_url(raw)
    suffix = uuid4().hex[:20]
    name = "tiger_capture_test_" + suffix
    role_by_kind = {kind: "capture_" + kind + "_" + suffix for kind in ("owner", "publisher", "reader")}
    password_by_kind = {"source": url.password, **{kind: token_hex(32) for kind in ("publisher", "reader")}}
    async with AsyncExitStack() as cleanup:
        admin = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        cleanup.push_async_callback(admin.close, timeout=5)
        assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name)
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
        )
        for kind, role in role_by_kind.items():
            options = "NOLOGIN" if kind == "owner" else f"LOGIN PASSWORD '{password_by_kind[kind]}'"
            await admin.execute(f'CREATE ROLE "{role}" {options}')
            cleanup.push_async_callback(admin.execute, f'DROP ROLE "{role}"')
        await admin.execute(f'GRANT "{role_by_kind["owner"]}" TO "{role_by_kind["publisher"]}"')
        await admin.execute(f'CREATE DATABASE "{name}"')

        async def drop_database():
            await admin.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", name)

        cleanup.push_async_callback(drop_database)
        database_url = url.set(database=name, drivername="postgresql+asyncpg")
        connection = await asyncpg.connect(
            database_url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        cleanup.push_async_callback(connection.close, timeout=5)
        await _seed_inherited_tiger(connection, name, role_by_kind)
        session_by_kind = {}
        for kind, username in (
            ("source", url.username),
            ("publisher", role_by_kind["publisher"]),
            ("reader", role_by_kind["reader"]),
        ):
            engine = create_async_engine(database_url.set(username=username, password=password_by_kind[kind]))
            cleanup.push_async_callback(engine.dispose)
            session_by_kind[kind] = async_sessionmaker(engine, expire_on_commit=False)
        yield SimpleNamespace(admin=connection, roles=role_by_kind, sessions=session_by_kind, url=database_url)


async def _seed_inherited_tiger(connection, name, role_by_kind):
    """Use actual extension DDL and genuine inherited synthetic source rows."""
    for extension in ("postgis", "fuzzystrmatch", "postgis_tiger_geocoder"):
        await connection.execute(f"CREATE EXTENSION {extension}")
    await connection.execute(f'GRANT CREATE ON DATABASE "{name}" TO "{role_by_kind["publisher"]}"')
    await connection.execute(f'GRANT USAGE ON SCHEMA tiger TO "{role_by_kind["publisher"]}"')
    await connection.execute("CREATE SCHEMA tiger_data")
    for parent in ("zip_state", "zcta5"):
        await connection.execute(f'CREATE TABLE tiger_data."{parent}_synthetic" () INHERITS(tiger."{parent}")')
        await connection.execute(f'ALTER EXTENSION postgis_tiger_geocoder ADD TABLE tiger_data."{parent}_synthetic"')
        await connection.execute(
            f'GRANT SELECT ON tiger."{parent}",tiger_data."{parent}_synthetic" TO "{role_by_kind["publisher"]}"'
        )
    await connection.execute("INSERT INTO tiger_data.zip_state_synthetic VALUES ('12345','AA','01')")
    await connection.execute(
        "INSERT INTO tiger_data.zcta5_synthetic(gid,statefp,zcta5ce,the_geom) "
        "VALUES(7,'01','12345',ST_Multi(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))',4269)))"
    )


@pytest.mark.asyncio
async def test_genuine_inherited_capture_fences_writes_and_keeps_an_immutable_epoch():
    async with owned_tiger_capture_database() as database:
        captured_graphs = []
        custody = ReferenceSourceCustody(database.roles["owner"], (database.roles["reader"],))

        async def protect(session, prepared, graph):
            await custody.retain(session, prepared)
            captured_graphs.append(graph)
            assert len(graph["relations"]) == 4
            async with database.admin.transaction():
                await database.admin.execute("SET LOCAL lock_timeout='100ms'")
                with pytest.raises(asyncpg.LockNotAvailableError):
                    async with database.admin.transaction():
                        await database.admin.execute("UPDATE tiger_data.zip_state_synthetic SET zip='54321'")

        prepared = await captured.prepare_captured_tiger_epoch(
            database.sessions["source"],
            database.sessions["publisher"],
            epoch_id=uuid4(),
            on_prepared=protect,
            source_copy=custody.source_copy,
            on_precreated=custody.precreate,
        )
        manifest = prepared.manifest.as_dict()
        assert manifest["publication_authority"] == "captured-epoch"
        assert "source_serving_generation" not in manifest
        assert archive.validate_reference_family_manifest(manifest) == prepared.manifest
        schema = prepared.ownership.schema_name
        await _assert_reader_cannot_mutate(database, schema)
        await database.admin.execute("UPDATE tiger_data.zip_state_synthetic SET zip='54321'")
        async with database.sessions["source"]() as session, session.begin():
            await session.execute(text("LOCK TABLE tiger.zip_state,tiger.zcta5 IN SHARE MODE"))
            graph = await captured.read_locked_tiger_graph(session)
            assert captured_graphs == [graph]
        copies = []

        async def copied(capture):
            copies.append(capture)

        await archive.export_prepared_reference_family_archive(
            database.sessions["reader"],
            prepared=prepared,
            archive_copy=copied,
            verify_custody=custody.verify,
        )
        assert len(copies) == 1 and copies[0].manifest == prepared.manifest
        assert await database.admin.fetchval(f'SELECT zip FROM "{schema}".zip_state') == "12345"


async def _assert_reader_cannot_mutate(database, schema):
    """Read-only capture export cannot alter rows, sequence state, or object identity."""
    async with database.sessions["reader"]() as session, session.begin():
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".zcta5')) == 1
        for statement in (
            f"UPDATE \"{schema}\".zip_state SET zip='54321'",
            f"SELECT nextval('\"{schema}\".zcta5_gid_seq')",
            f'DROP TABLE "{schema}".zip_state',
        ):
            with pytest.raises(Exception) as denied:
                async with session.begin_nested():
                    await session.execute(text(statement))
            assert getattr(denied.value.orig, "sqlstate", None) == "42501"


@pytest.mark.asyncio
async def test_default_projection_retains_inherited_source_compatibility():
    """Existing NULL bindings keep canonical serving; explicit held roots are refused."""
    from api import ptg2_geo_projection as projection

    async with owned_tiger_capture_database() as database:
        await database.admin.execute("CREATE SCHEMA mrf")
        for table in (
            "entity_address_unified",
            "npi_address",
            "mrf_address",
            "doctor_clinician_address",
            "geo_zip_lookup",
        ):
            await database.admin.execute(f"CREATE TABLE mrf.{table}(id integer)")
        await database.admin.execute(
            "CREATE TABLE mrf.entity_address_geo_assurance_state "
            "(singleton boolean, active_geo_assurance_version integer, active_table_oid oid, "
            "active_relation_signature jsonb, active_dependency_bindings jsonb)"
        )
        signature = projection.projection_relation_signature_sql("mrf")
        await database.admin.execute(
            "INSERT INTO mrf.entity_address_geo_assurance_state VALUES(true,"
            + str(projection.GEO_ASSURANCE_VERSION)
            + ",'mrf.entity_address_unified'::regclass,"
            + signature
            + ",NULL)"
        )
        assert await database.admin.fetchval("SELECT " + projection.projection_state_available_sql("mrf"))
        await database.admin.execute(
            "UPDATE mrf.entity_address_geo_assurance_state SET active_dependency_bindings="
            "(SELECT jsonb_object_agg(key,jsonb_build_object('schema_name',split_part(key,'.',1),"
            "'table_name',split_part(key,'.',2),'relation_oid',value->0,'relfilenode',value->1)) "
            "FROM jsonb_each(active_relation_signature))"
        )
        assert not await database.admin.fetchval("SELECT " + projection.projection_state_available_sql("mrf"))
        await _assert_received_child_requires_held_bindings(database, projection)


async def _assert_received_child_requires_held_bindings(database, projection):
    """The exact received child shape invalidates NULL, not ordinary source inheritance."""
    schema = archive.reference_family_stage_schema(uuid4())
    await database.admin.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{database.roles["owner"]}"')
    for name in ("zip_state", "zcta5"):
        await database.admin.execute(f'CREATE TABLE "{schema}"."{name}" () INHERITS(tiger."{name}")')
    await database.admin.execute("UPDATE mrf.entity_address_geo_assurance_state SET active_dependency_bindings=NULL")
    # A name alone is not the protected received mode.
    assert await database.admin.fetchval("SELECT " + projection.projection_state_available_sql("mrf"))
    for name in ("zip_state", "zcta5"):
        await database.admin.execute(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{database.roles["owner"]}"')
    assert not await database.admin.fetchval("SELECT " + projection.projection_state_available_sql("mrf"))
    bindings_by_name = {}
    for namespace, table in projection._PROJECTION_DEPENDENCIES:
        namespace = namespace or "mrf"
        physical = schema if namespace == "tiger" else namespace
        identity = await database.admin.fetchrow(
            "SELECT oid::bigint,pg_relation_filenode(oid)::bigint AS filenode FROM pg_class WHERE oid=to_regclass($1)",
            f'"{physical}"."{table}"',
        )
        bindings_by_name[f"{namespace}.{table}"] = {
            "schema_name": physical,
            "table_name": table,
            "relation_oid": identity["oid"],
            "relfilenode": identity["filenode"],
        }
    import json

    await database.admin.execute(
        "UPDATE mrf.entity_address_geo_assurance_state SET active_dependency_bindings=$1::jsonb, "
        "active_relation_signature="
        + projection.projection_relation_signature_sql("mrf", dependency_bindings=bindings_by_name),
        json.dumps(bindings_by_name),
    )
    assert await database.admin.fetchval("SELECT " + projection.projection_state_available_sql("mrf"))


@pytest.mark.asyncio
async def test_capture_refuses_different_publisher_database_before_clone():
    """Independent database OIDs cannot authorize a cross-database source/clone join."""

    async def refuse_callback(*_args):
        raise AssertionError("different databases reached protected custody")

    async with owned_tiger_capture_database() as source_database, owned_tiger_capture_database() as publisher_database:
        epoch_id = uuid4()
        custody = ReferenceSourceCustody(publisher_database.roles["owner"], ())
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="databases differ"):
            await captured.prepare_captured_tiger_epoch(
                source_database.sessions["source"],
                publisher_database.sessions["publisher"],
                epoch_id=epoch_id,
                on_prepared=refuse_callback,
                source_copy=custody.source_copy,
                on_precreated=refuse_callback,
            )
        for database in (source_database, publisher_database):
            assert not await database.admin.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
                archive.reference_family_stage_schema(epoch_id),
            )
