# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine extension parents and protected snapshot leaves on disposable PostgreSQL."""

from contextlib import AsyncExitStack, asynccontextmanager
from secrets import token_hex
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import tiger_snapshot_inheritance as inherited
from process.tiger_captured_epoch import prepare_captured_tiger_epoch
from tests.reference_family_generation_fixture import ReferenceSourceCustody
from tests.test_cms_doctors_archive_postgres import _migration
from tests.test_tiger_result_archive_postgres import _command, _database_url


@asynccontextmanager
async def _owned_database():
    source_url = _database_url()
    suffix = uuid4().hex
    database = "tiger_inheritance_test_" + suffix
    owner = "tiger_owner_" + suffix
    publisher = "tiger_publisher_" + suffix
    reader = "tiger_reader_" + suffix
    password_by_role = {source_url.username: source_url.password, publisher: token_hex(32), reader: token_hex(32)}
    roles, is_created = [], False
    try:
        async with AsyncExitStack() as cleanup:
            admin = await asyncpg.connect(source_url.set(drivername="postgresql").render_as_string(hide_password=False))
            cleanup.push_async_callback(admin.close)
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database)
            for name, login in ((owner, False), (publisher, True), (reader, True)):
                assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", name)
                options = f"LOGIN PASSWORD '{password_by_role[name]}'" if login else "NOLOGIN"
                await admin.execute(f'CREATE ROLE "{name}" {options}')
                roles.append(name)
                cleanup.push_async_callback(admin.execute, f'DROP ROLE "{name}"')
            await admin.execute(f'GRANT "{owner}" TO "{publisher}"')
            await admin.execute(f'CREATE DATABASE "{database}"')
            is_created = True
            cleanup.push_async_callback(admin.execute, f'DROP DATABASE "{database}" WITH (FORCE)')
            url = source_url.set(database=database)
            engines = []
            for role in (source_url.username, publisher, reader):
                engine = create_async_engine(url.set(username=role, password=password_by_role[role]))
                cleanup.push_async_callback(engine.dispose)
                engines.append(engine)
            yield (
                url,
                tuple(async_sessionmaker(engine, expire_on_commit=False) for engine in engines),
                (owner, publisher, reader),
            )
    finally:
        if is_created or roles:
            verification = await asyncpg.connect(
                source_url.set(drivername="postgresql").render_as_string(hide_password=False)
            )
            try:
                if is_created:
                    assert not await verification.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", database
                    )
                    assert not await verification.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=$1)", database
                    )
                assert not await verification.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", roles
                )
            finally:
                await verification.close()


async def _captured_stage(admin_sessions, publisher_sessions, owner, *, zip_code):
    """Capture actual inherited source rows, then remove only this temporary source leaf."""
    async with admin_sessions.begin() as session:
        await session.execute(text("CREATE SCHEMA source_leaf"))
        await session.execute(
            text("CREATE TABLE source_leaf.zip_state (LIKE tiger.zip_state INCLUDING ALL) INHERITS(tiger.zip_state)")
        )
        await session.execute(
            text("CREATE TABLE source_leaf.zcta5 (LIKE tiger.zcta5 INCLUDING ALL) INHERITS(tiger.zcta5)")
        )
        await session.execute(text("INSERT INTO source_leaf.zip_state VALUES (:zip,'AA','01')"), {"zip": zip_code})
        await session.execute(
            text(
                "INSERT INTO source_leaf.zcta5 (gid,statefp,zcta5ce,the_geom) VALUES (42,'01',:zip, "
                "ST_GeomFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))',4269))"
            ),
            {"zip": zip_code},
        )

    custody = ReferenceSourceCustody(owner, ())

    async def protect(session, prepared, _graph):
        await custody.retain(session, prepared)

    prepared = await prepare_captured_tiger_epoch(
        admin_sessions,
        publisher_sessions,
        epoch_id=uuid4(),
        on_prepared=protect,
        source_copy=custody.source_copy,
        on_precreated=custody.precreate,
    )
    async with admin_sessions.begin() as session:
        await session.execute(text("DROP TABLE source_leaf.zip_state,source_leaf.zcta5"))
        await session.execute(text("DROP SCHEMA source_leaf"))
    async with publisher_sessions.begin() as session:
        await custody.verify(session, prepared)
        owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:owner"), {"owner": owner})
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=prepared.ownership,
            manifest=prepared.manifest,
            package_id="a" * 64,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return prepared, validation, owner_oid


async def _provision_tiger_roots(url, sessions, roles):
    """Create genuine empty extension roots with separate publisher/read principals."""
    admin_sessions, _publisher_sessions, _reader_sessions = sessions
    owner, publisher, reader = roles
    async with admin_sessions.begin() as session:
        for extension in ("postgis", "fuzzystrmatch", "postgis_tiger_geocoder"):
            await session.execute(text(f"CREATE EXTENSION {extension}"))
        await session.execute(text(f'CREATE SCHEMA mrf AUTHORIZATION "{owner}"'))
        for revision in (
            "20260914110000_reference_family_result_generation",
            "20260914130000_mrf_result_generation",
            "20260920100000_cms_doctors_result_generation",
            "20260920110000_tiger_result_generation",
        ):
            await _migration(session, revision, "upgrade")
        await session.execute(text(f'GRANT CREATE ON DATABASE "{url.database}" TO "{publisher}"'))
        await session.execute(text(f'GRANT USAGE ON SCHEMA tiger TO "{publisher}","{reader}"'))
        await session.execute(text(f'GRANT SELECT ON tiger.zip_state,tiger.zcta5 TO "{publisher}","{reader}"'))


async def _initial_tiger_child_activation(sessions, roles):
    """Reject unprovisioned ownership, then attach the actual protected source clone."""
    admin_sessions, publisher_sessions, _reader_sessions = sessions
    owner, _publisher, reader = roles
    first, validation, owner_oid = await _captured_stage(admin_sessions, publisher_sessions, owner, zip_code="00001")
    cutover = archive.ReferenceFamilyCutoverAuthority("a" * 64, archive.CONTRACT, owner_oid, owner_oid, "manual")
    async with publisher_sessions.begin() as session:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership"):
            await inherited.activate_tiger_snapshot_children(
                session,
                ownership=first.ownership,
                manifest=first.manifest,
                validation_receipt=validation,
                cutover=cutover,
            )
    async with admin_sessions.begin() as session:
        for name in inherited.TABLES:
            await session.execute(text(f'ALTER TABLE ONLY tiger."{name}" OWNER TO "{owner}"'))
    async with publisher_sessions.begin() as session:
        receipt = await inherited.activate_tiger_snapshot_children(
            session,
            ownership=first.ownership,
            manifest=first.manifest,
            validation_receipt=validation,
            cutover=cutover,
        )
        for name in inherited.TABLES:
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{first.ownership.schema_name}" TO "{reader}"'))
            await session.execute(text(f'GRANT SELECT ON "{first.ownership.schema_name}"."{name}" TO "{reader}"'))
    return first, cutover, receipt, owner_oid


async def _assert_tiger_reader_security(reader_sessions, first, owner):
    """Canonical readers see inherited data but cannot mutate or acquire owner authority."""
    async with reader_sessions.begin() as session:
        assert await session.scalar(text("SELECT zip FROM tiger.zip_state")) == "00001"
        assert await session.scalar(text("SELECT ST_SRID(the_geom) FROM tiger.zcta5")) == 4269
        assert await session.scalar(text("SELECT count(*) FROM ONLY tiger.zip_state")) == 0
        for statement in (
            "INSERT INTO tiger.zip_state VALUES ('00002','BB','02')",
            f"UPDATE \"{first.ownership.schema_name}\".zip_state SET zip='00002'",
            f'ALTER TABLE "{first.ownership.schema_name}".zip_state NO INHERIT tiger.zip_state',
            f'ALTER TABLE "{first.ownership.schema_name}".zip_state INHERIT tiger.zip_state',
            "SELECT nextval('tiger.zcta5_gid_seq')",
            f"SELECT nextval('{first.ownership.schema_name}.zcta5_gid_seq')",
            f'SET ROLE "{owner}"',
        ):
            with pytest.raises(Exception):
                async with session.begin_nested():
                    await session.execute(text(statement))


async def _prepare_replacement_tiger_leaf(sessions, first, receipt, owner, owner_oid):
    """Build a second real source epoch without reading protected target data as legacy authority."""
    admin_sessions, publisher_sessions, _reader_sessions = sessions
    # Capture a second genuine source epoch in a separate temporary source
    # graph, while the target leaf remains protected and held unchanged.
    async with publisher_sessions.begin() as session:
        await session.execute(text(f'ALTER TABLE "{first.ownership.schema_name}".zip_state NO INHERIT tiger.zip_state'))
        await session.execute(text(f'ALTER TABLE "{first.ownership.schema_name}".zcta5 NO INHERIT tiger.zcta5'))
    second, second_validation, _owner_oid = await _captured_stage(
        admin_sessions, publisher_sessions, owner, zip_code="00002"
    )
    async with publisher_sessions.begin() as session:
        await inherited.exchange_tiger_snapshot_children(
            session,
            incoming_pairs=first.ownership.relation_oids,
            expected_children=(),
            owner_oid=owner_oid,
            expected_parent_inventory=receipt["parent_inventory"],
        )
    return second, second_validation


async def _exchange_tiger_leaves_and_rollback(
    publisher_sessions, first, second, second_validation, cutover, receipt, owner_oid
):
    """Late failure and committed inverse both preserve the original child OIDs."""
    with pytest.raises(RuntimeError, match="late failure"):
        async with publisher_sessions.begin() as session:
            await inherited.activate_tiger_snapshot_children(
                session,
                ownership=second.ownership,
                manifest=second.manifest,
                validation_receipt=second_validation,
                cutover=cutover,
                expected_children=first.ownership.relation_oids,
                expected_parent_inventory=receipt["parent_inventory"],
            )
            raise RuntimeError("synthetic late failure")
    async with publisher_sessions.begin() as session:
        assert await inherited.current_tiger_snapshot_children(session) == first.ownership.relation_oids
        await inherited.activate_tiger_snapshot_children(
            session,
            ownership=second.ownership,
            manifest=second.manifest,
            validation_receipt=second_validation,
            cutover=cutover,
            expected_children=first.ownership.relation_oids,
            expected_parent_inventory=receipt["parent_inventory"],
        )
    async with publisher_sessions.begin() as session:
        await inherited.exchange_tiger_snapshot_children(
            session,
            incoming_pairs=first.ownership.relation_oids,
            expected_children=second.ownership.relation_oids,
            owner_oid=owner_oid,
            expected_parent_inventory=receipt["parent_inventory"],
        )


async def _assert_tiger_database_backup(url, sessions, receipt, owner_oid, tmp_path):
    """Whole database backup preserves data, not old ledger OIDs or parent-owner admission."""
    _admin_sessions, publisher_sessions, reader_sessions = sessions
    dump = tmp_path / "database.dump"
    dsn = url.set(drivername="postgresql").render_as_string(hide_password=False)
    await _command("pg_dump", "--dbname", dsn, "--format=custom", "--file", str(dump))
    # A standard restore recreates extension parents under the extension
    # owner, not the separately provisioned protected table owner.
    await _command("pg_restore", "--dbname", dsn, "--clean", "--if-exists", "--exit-on-error", str(dump))
    async with reader_sessions.begin() as session:
        assert await session.scalar(text("SELECT zip FROM tiger.zip_state")) == "00001"
        assert await session.scalar(text("SELECT ST_SRID(the_geom) FROM tiger.zcta5")) == 4269
    async with publisher_sessions.begin() as session:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership"):
            await inherited.capture_tiger_snapshot_parents(
                session,
                owner_oid=owner_oid,
                expected_children=await inherited.current_tiger_snapshot_children(session),
                expected_parent_inventory=receipt["parent_inventory"],
            )


@pytest.mark.asyncio
async def test_genuine_tiger_snapshot_children_activation_rollback_security_and_database_backup(tmp_path):
    """Prove the explicit child lifecycle, least privilege and standard backup boundary."""
    async with _owned_database() as (url, sessions, roles):
        await _provision_tiger_roots(url, sessions, roles)
        first, cutover, receipt, owner_oid = await _initial_tiger_child_activation(sessions, roles)
        await _assert_tiger_reader_security(sessions[2], first, roles[0])
        second, validation = await _prepare_replacement_tiger_leaf(sessions, first, receipt, roles[0], owner_oid)
        await _exchange_tiger_leaves_and_rollback(sessions[1], first, second, validation, cutover, receipt, owner_oid)
        await _assert_tiger_database_backup(url, sessions, receipt, owner_oid, tmp_path)
