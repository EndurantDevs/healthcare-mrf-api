# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native grants and transactional seven-table retention, without role-profile admission."""

import json
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.engine import make_url

from process import entity_address_snapshot_adoption as adoption
from process.entity_address_retained_names import retained_address_name
from tests.test_network_address_artifact_postgres import _NativeDB


async def _rotation_tables(target_connection, role_by_kind):
    swaps = []
    for table in adoption.result_generation.RELATION_NAMES:
        stage = table + "_next"
        for relation, row_payload in ((table, "old"), (stage, "new")):
            index = adoption.entity_address_unified._stage_index_name(relation, "primary")
            await target_connection.execute(
                f'CREATE TABLE sample."{relation}"(id serial,payload text,CONSTRAINT "{index}" PRIMARY KEY(id))'
            )
            await target_connection.execute(f'INSERT INTO sample."{relation}"(payload) VALUES($1)', row_payload)
            await target_connection.execute(f'ALTER TABLE sample."{relation}" OWNER TO "{role_by_kind["prior"]}"')
            await target_connection.execute(
                f'GRANT INSERT,DELETE,UPDATE(payload) ON sample."{relation}" TO "{role_by_kind["reader"]}"'
            )
            await target_connection.execute(
                f'GRANT USAGE,UPDATE ON SEQUENCE sample."{relation}_id_seq" TO "{role_by_kind["reader"]}"'
            )
        await target_connection.execute(f'GRANT SELECT ON sample."{table}" TO "{role_by_kind["reader"]}"')
        swaps.append(
            SimpleNamespace(
                live_cls=SimpleNamespace(__main_table__=table),
                stage_cls=SimpleNamespace(__tablename__=stage, __my_additional_indexes__=[]),
            )
        )
    return swaps


async def _rotation_connection(url, role):
    return await asyncpg.connect(url.set(username=role, password=None).render_as_string(hide_password=False))


async def _cleanup_rotation(admin, name, created_roles, connections, is_database_created):
    for connection in reversed(connections):
        await connection.close()
    if is_database_created:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    for role in reversed(created_roles):
        await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles)
    await admin.close()


@pytest_asyncio.fixture
async def named_rotation():
    configured = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not configured:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN is not set")
    url = make_url(configured.replace("postgresql+asyncpg://", "postgresql://"))
    admin = await asyncpg.connect(url.render_as_string(hide_password=False))
    name = "address_rotation_" + uuid4().hex[:16]
    role_by_kind = {key: name + "_" + key for key in ("prior", "retention", "publisher", "reader")}
    created_roles, connections = [], []
    is_database_created = False
    try:
        assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", name)
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", list(role_by_kind.values())
        )
        for kind, role in role_by_kind.items():
            created_roles.append(role)
            await admin.execute(
                f'CREATE ROLE "{role}" {"LOGIN" if kind in {"publisher", "reader", "prior"} else "NOLOGIN"}'
            )
        await admin.execute(
            f'GRANT "{role_by_kind["prior"]}","{role_by_kind["retention"]}" TO "{role_by_kind["publisher"]}"'
        )
        is_database_created = True
        await admin.execute(f'CREATE DATABASE "{name}"')
        target_connection = await asyncpg.connect(url.set(database=name).render_as_string(hide_password=False))
        connections.append(target_connection)
        await target_connection.execute(
            f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{role_by_kind["retention"]}"'
        )
        await target_connection.execute(f'CREATE SCHEMA sample AUTHORIZATION "{role_by_kind["prior"]}"')
        await target_connection.execute(f'GRANT CREATE,USAGE ON SCHEMA sample TO "{role_by_kind["retention"]}"')
        await target_connection.execute(f'GRANT USAGE ON SCHEMA sample TO "{role_by_kind["reader"]}"')
        swaps = await _rotation_tables(target_connection, role_by_kind)
        publisher = await _rotation_connection(url.set(database=name), role_by_kind["publisher"])
        reader = await _rotation_connection(url.set(database=name), role_by_kind["reader"])
        prior = await _rotation_connection(url.set(database=name), role_by_kind["prior"])
        connections.extend((publisher, reader, prior))
        yield SimpleNamespace(
            admin=target_connection,
            publisher=publisher,
            reader=reader,
            prior=prior,
            roles=role_by_kind,
            swaps=swaps,
            url=url.set(database=name),
            generation={
                "origin_lineage_id": str(uuid4()),
                "origin_generation": 4,
                "published_at": datetime(2026, 1, 1, tzinfo=UTC),
            },
        )
    finally:
        await _cleanup_rotation(admin, name, created_roles, connections, is_database_created)


async def _assert_rotation_rows(fixture, interrupted):
    for swap in fixture.swaps:
        table = swap.live_cls.__main_table__
        retained = retained_address_name(table, fixture.generation)
        assert await fixture.reader.fetchval(f'SELECT payload FROM sample."{table}"') == (
            "old" if interrupted else "new"
        )
        if interrupted:
            assert await fixture.admin.fetchval(f'SELECT payload FROM sample."{swap.stage_cls.__tablename__}"') == "new"
            assert not await fixture.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'sample."{retained}"')
        else:
            assert await fixture.reader.fetchval(f'SELECT payload FROM sample."{retained}"') == "old"
            assert await fixture.prior.fetchval(f'SELECT payload FROM sample."{table}"') == "new"
            assert await fixture.prior.fetchval(f'SELECT payload FROM sample."{retained}"') == "old"
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.prior.execute(f"UPDATE sample.\"{table}\" SET payload='changed'")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.reader.execute(f"UPDATE sample.\"{table}\" SET payload='changed'")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.reader.fetchval(
                    "SELECT nextval(pg_get_serial_sequence($1,'id'))", 'sample."' + table + '"'
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted", [False, True])
async def test_named_family_rotation_preserves_reads_and_atomic_rollback(named_rotation, monkeypatch, interrupted):
    fixture = named_rotation
    database = _NativeDB(fixture.publisher)
    monkeypatch.setattr(adoption.entity_address_unified, "db", database)
    for swap in fixture.swaps:
        await fixture.admin.execute(
            f'ALTER TABLE sample."{swap.stage_cls.__tablename__}" OWNER TO "{fixture.roles["retention"]}"'
        )
    original_status = database.status
    renamed_tables = []

    async def status(statement):
        if " RENAME TO " in statement and statement.startswith("ALTER TABLE"):
            renamed_tables.append(statement)
            if interrupted and len(renamed_tables) == 5:
                raise RuntimeError("synthetic interrupted family rotation")
        return await original_status(statement)

    monkeypatch.setattr(database, "status", status)

    async def rotate():
        async with fixture.publisher.transaction():
            tables = [
                name for swap in fixture.swaps for name in (swap.live_cls.__main_table__, swap.stage_cls.__tablename__)
            ]
            await fixture.publisher.execute(
                "LOCK TABLE " + ",".join(f'sample."{name}"' for name in tables) + " IN ACCESS EXCLUSIVE MODE"
            )
            for swap in fixture.swaps:
                await adoption._swap_sealed_stage_table(
                    "sample",
                    swap.live_cls,
                    swap.stage_cls,
                    -999,
                    serving_generation=fixture.generation,
                )

    if interrupted:
        with pytest.raises(RuntimeError, match="interrupted family"):
            await rotate()
    else:
        await rotate()
    await _assert_rotation_rows(fixture, interrupted)


async def _prepare_ordinary_main_rotation(fixture, has_generation, failure):
    """Keep support tables live while creating only the actual main replacement."""
    await fixture.admin.execute("DROP SCHEMA hp_snapshot_retention")
    await fixture.admin.execute(f'REVOKE "{fixture.roles["retention"]}" FROM "{fixture.roles["publisher"]}"')
    assert not await fixture.admin.fetchval(
        "SELECT pg_has_role($1,$2,'SET')", fixture.roles["publisher"], fixture.roles["retention"]
    )
    swap = fixture.swaps[0]
    table = swap.live_cls.__main_table__
    live_oid = await fixture.admin.fetchval("SELECT to_regclass($1)::oid", "sample." + table)
    retained = retained_address_name(table, fixture.generation) if has_generation else f"{table}_retained_{live_oid:x}"
    for support in fixture.swaps[1:]:
        await fixture.admin.execute(f'DROP TABLE sample."{support.stage_cls.__tablename__}"')
    await fixture.admin.execute(
        "CREATE TABLE sample.entity_address_result_generation("
        "singleton boolean,origin_lineage_id uuid,origin_generation bigint,published_at timestamptz)"
    )
    await fixture.admin.execute(
        "INSERT INTO sample.entity_address_result_generation VALUES(true,$1,$2,$3)",
        fixture.generation["origin_lineage_id"] if has_generation else None,
        fixture.generation["origin_generation"] if has_generation else None,
        fixture.generation["published_at"] if has_generation else None,
    )
    await fixture.admin.execute(
        f'ALTER TABLE sample.entity_address_result_generation OWNER TO "{fixture.roles["prior"]}"'
    )
    if failure == "collision":
        await fixture.admin.execute(f'CREATE TABLE sample."{retained}"(payload text)')
        await fixture.admin.execute(f"INSERT INTO sample.\"{retained}\" VALUES('sentinel')")
    return swap, table, retained


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "collision", "rollback"])
@pytest.mark.parametrize("has_generation", [False, True])
async def test_ordinary_main_rotation_preserves_support_without_replacement_stages(
    named_rotation, monkeypatch, failure, has_generation
):
    """Keep support data, native reads and immutability through ordinary commit and rollback."""
    fixture = named_rotation
    swap, table, retained = await _prepare_ordinary_main_rotation(fixture, has_generation, failure)
    database = _NativeDB(fixture.publisher)
    monkeypatch.setattr(adoption.entity_address_unified, "db", database)
    context_by_field = {"result_generation_mode": "ordinary"}

    async def rotate():
        async with fixture.publisher.transaction():
            await fixture.publisher.execute(f'SET LOCAL ROLE "{fixture.roles["prior"]}"')
            await fixture.publisher.execute(
                f'LOCK TABLE sample."{table}",sample."{swap.stage_cls.__tablename__}" IN ACCESS EXCLUSIVE MODE'
            )
            generation = await adoption._prepare_protected_ordinary_rotation("sample", [swap], False, context_by_field)
            await adoption._require_protected_cutover("sample", [swap], context_by_field)
            await adoption._publish_result_swaps("sample", [swap], context_by_field, serving_generation=generation)
            if failure == "rollback":
                raise RuntimeError("synthetic late ordinary rollback")

    if failure:
        with pytest.raises(RuntimeError, match="already exists|late ordinary rollback"):
            await rotate()
    else:
        await rotate()
    assert "snapshot_contract" not in context_by_field
    for support in fixture.swaps[1:]:
        assert await fixture.reader.fetchval(f'SELECT payload FROM sample."{support.live_cls.__main_table__}"') == "old"
    assert await fixture.reader.fetchval(f'SELECT payload FROM sample."{table}"') == ("old" if failure else "new")
    if failure:
        assert await fixture.admin.fetchval(f'SELECT payload FROM sample."{swap.stage_cls.__tablename__}"') == "new"
        assert await fixture.prior.fetchval(
            "SELECT has_table_privilege($1,'UPDATE')", f'sample."{swap.stage_cls.__tablename__}"'
        )
        if failure == "collision":
            assert await fixture.admin.fetchval(f'SELECT payload FROM sample."{retained}"') == "sentinel"
        else:
            assert not await fixture.admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f'sample."{retained}"')
    else:
        assert await fixture.reader.fetchval(f'SELECT payload FROM sample."{retained}"') == "old"
        for connection in (fixture.prior, fixture.publisher, fixture.reader):
            for name in (table, retained):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(f"UPDATE sample.\"{name}\" SET payload='changed'")
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetchval("SELECT nextval(pg_get_serial_sequence($1,'id'))", f'sample."{name}"')
        async with fixture.publisher.transaction():
            await fixture.publisher.execute(f'SET LOCAL ROLE "{fixture.roles["prior"]}"')
            await fixture.publisher.execute(
                f'LOCK TABLE sample."{table}",sample."{retained}" IN ACCESS EXCLUSIVE MODE NOWAIT'
            )


@pytest.mark.asyncio
async def test_valid_names_do_not_bypass_native_permission_denial(named_rotation, monkeypatch):
    fixture = named_rotation
    database = _NativeDB(fixture.reader)
    monkeypatch.setattr(adoption.entity_address_unified, "db", database)
    swap = fixture.swaps[0]
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with fixture.reader.transaction():
            await adoption._swap_sealed_stage_table(
                "sample", swap.live_cls, swap.stage_cls, serving_generation=fixture.generation
            )
    assert await fixture.reader.fetchval("SELECT payload FROM sample.entity_address_unified") == "old"
    assert await fixture.admin.fetchval("SELECT payload FROM sample.entity_address_unified_next") == "new"


async def _ordinary_replay(fixture, caller, swap):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from db.connection import Database
    from process import provider_directory_artifact_read_access as access

    engine = create_async_engine(
        fixture.url.set(drivername="postgresql+asyncpg", username=fixture.roles[caller], password=None)
    )
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    try:
        async with database.transaction() as session:
            await session.execute(
                text(
                    "LOCK TABLE sample.entity_address_unified,sample.entity_address_unified_next IN ACCESS EXCLUSIVE MODE"
                )
            )
            proofs = await access.prepare_ordinary_read_access(database, "sample", [swap])
            await access.recheck_ordinary_read_access(database, proofs)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("helper", ["native", "ordinary"])
@pytest.mark.parametrize("denied", [False, True])
async def test_read_replay_uses_named_locks_and_actual_grants_across_mixed_owners(named_rotation, helper, denied):
    from sqlalchemy.exc import DBAPIError

    from process import native_publication_read_access as access

    fixture = named_rotation
    swap = fixture.swaps[0]
    await fixture.admin.execute(
        f'ALTER TABLE sample.entity_address_unified_next OWNER TO "{fixture.roles["retention"]}"'
    )
    caller = "reader" if denied else "publisher"

    async def replay():
        if helper == "ordinary":
            return await _ordinary_replay(fixture, caller, swap)
        connection = getattr(fixture, caller)
        async with connection.transaction():
            await access.preserve_publication_reads(
                connection,
                schema="sample",
                live_table="entity_address_unified",
                stage_table="entity_address_unified_next",
                expected_stage_owner_oid=-1,
            )

    if denied:
        with pytest.raises((asyncpg.InsufficientPrivilegeError, DBAPIError, RuntimeError)) as failure:
            await replay()
        if isinstance(failure.value, RuntimeError):
            assert "grants_changed" in str(failure.value)
        else:
            native_error = failure.value.orig if isinstance(failure.value, DBAPIError) else failure.value
            assert native_error.sqlstate == "42501"
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await fixture.reader.fetchval("SELECT payload FROM sample.entity_address_unified_next")
    else:
        await replay()
        assert await fixture.reader.fetchval("SELECT payload FROM sample.entity_address_unified_next") == "new"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reference", ["none", "live", "old", "relocated_old", "relocated_away", "malformed", "missing_location"]
)
async def test_legacy_rotation_preserves_current_named_retention_locations(named_rotation, monkeypatch, reference):
    """Actual retained and relocated old-table references fence a destructive legacy rotation."""
    fixture = named_rotation
    await fixture.admin.execute("CREATE TABLE sample.entity_address_unified_old(id integer)")
    await fixture.admin.execute("INSERT INTO sample.entity_address_unified_old VALUES(7)")
    await fixture.admin.execute(
        "CREATE TABLE hp_snapshot_retention.generation(generation_id uuid,state text,inventory jsonb,current_location_fence uuid)"
    )
    await fixture.admin.execute(
        "CREATE TABLE hp_snapshot_retention.generation_relocation(generation_id uuid,fence_token uuid,after_inventory jsonb)"
    )
    await fixture.admin.execute(
        f'GRANT SELECT ON ALL TABLES IN SCHEMA hp_snapshot_retention TO "{fixture.roles["publisher"]}"'
    )
    await fixture.admin.execute(f'GRANT USAGE ON SCHEMA hp_snapshot_retention TO "{fixture.roles["publisher"]}"')
    table = "entity_address_unified" if reference == "live" else "entity_address_unified_old"
    inventory_by_field = {"relations": [{"schema_name": "sample", "relation_name": table}]}
    if reference == "malformed":
        inventory_by_field = {"relations": "invalid"}
    lineage, fence = uuid4(), uuid4() if reference.startswith("relocated") or reference == "missing_location" else None
    await fixture.admin.execute(
        "INSERT INTO hp_snapshot_retention.generation VALUES($1,'retained',$2::jsonb,$3)",
        lineage,
        json.dumps(inventory_by_field),
        fence,
    )
    if reference.startswith("relocated"):
        relocated = (
            inventory_by_field
            if reference == "relocated_old"
            else {"relations": [{"schema_name": "sample", "relation_name": "unrelated_retained"}]}
        )
        await fixture.admin.execute(
            "INSERT INTO hp_snapshot_retention.generation_relocation VALUES($1,$2,$3::jsonb)",
            lineage,
            fence,
            json.dumps(relocated),
        )
    if reference == "none":
        await fixture.admin.execute("DELETE FROM hp_snapshot_retention.generation")
    database = _NativeDB(fixture.publisher)
    monkeypatch.setattr(adoption.entity_address_unified, "db", database)
    async with fixture.publisher.transaction():
        if reference in {"none", "relocated_away"}:
            await adoption._require_protected_cutover("sample", fixture.swaps, {})
        else:
            with pytest.raises(RuntimeError, match="legacy address rotation"):
                await adoption._require_protected_cutover("sample", fixture.swaps, {})
    assert await fixture.admin.fetchval("SELECT id FROM sample.entity_address_unified_old") == 7


async def _rotation_contents(fixture):
    return tuple(
        [
            (name, tuple(await fixture.admin.fetch(f'SELECT * FROM sample."{name}" ORDER BY id')))
            for swap in fixture.swaps
            for name in (swap.live_cls.__main_table__, swap.stage_cls.__tablename__)
        ]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("ledger", ["absent", "missing_relocation", "unreadable"])
async def test_legacy_catalog_probe_does_not_require_privileges_for_absent_tables(named_rotation, monkeypatch, ledger):
    """An empty inaccessible namespace is harmless; real inaccessible retention data refuses."""
    fixture = named_rotation
    assert not await fixture.admin.fetchval(
        "SELECT has_schema_privilege($1,'hp_snapshot_retention','USAGE')", fixture.roles["reader"]
    )
    if ledger != "absent":
        await fixture.admin.execute(
            "CREATE TABLE hp_snapshot_retention.generation(generation_id uuid,state text,inventory jsonb,current_location_fence uuid)"
        )
    if ledger == "unreadable":
        await fixture.admin.execute(
            "CREATE TABLE hp_snapshot_retention.generation_relocation(generation_id uuid,fence_token uuid,after_inventory jsonb)"
        )
    original_rows = await _rotation_contents(fixture)
    monkeypatch.setattr(adoption.entity_address_unified, "db", _NativeDB(fixture.reader))

    async def probe():
        async with fixture.reader.transaction():
            await adoption._require_protected_cutover("sample", fixture.swaps, {})

    if ledger == "absent":
        await probe()
    elif ledger == "missing_relocation":
        with pytest.raises(RuntimeError, match="retained snapshot location catalog is missing"):
            await probe()
    else:
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as failure:
            await probe()
        assert failure.value.sqlstate == "42501"
    assert await _rotation_contents(fixture) == original_rows
    assert not await fixture.admin.fetchval(
        "SELECT has_schema_privilege($1,'hp_snapshot_retention','USAGE')", fixture.roles["reader"]
    )
