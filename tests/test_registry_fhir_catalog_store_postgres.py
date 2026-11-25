# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounded COPY, retained carry-forward and immutable-source protection."""

import asyncio
import json
import os
from contextvars import copy_context
from datetime import datetime, timezone
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_fhir_catalog_store as store
from process.registry_fhir_catalog_custody import PHYSICAL_TABLES, FHIRCatalogTarget

OLD = "a" * 64
NEW = "b" * 64
SOURCE = "c" * 64


class CountConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = 0
        self.copies = 0
        self.copy_tables = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, *arguments, **keywords):
        self.statements += 1
        return await self.connection.execute(*arguments, **keywords)

    async def fetchrow(self, *arguments, **keywords):
        self.statements += 1
        return await self.connection.fetchrow(*arguments, **keywords)

    async def fetchval(self, *arguments, **keywords):
        self.statements += 1
        return await self.connection.fetchval(*arguments, **keywords)

    async def copy_records_to_table(self, *arguments, **keywords):
        self.copies += 1
        self.copy_tables.append(arguments[0])
        return await self.connection.copy_records_to_table(*arguments, **keywords)


@pytest.fixture
async def storage_database():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an isolated test database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    schemas = []
    control = "fhir_store_control_" + uuid4().hex
    schemas.append(control)
    try:
        if not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_extension WHERE extname='pg_trgm')"):
            pytest.skip("isolated database must already contain pg_trgm")
        await connection.execute(f'CREATE SCHEMA "{control}"')
        await connection.execute(
            f'CREATE TABLE "{control}".registry_fhir_catalog_snapshot '
            "(schema_name text PRIMARY KEY,metadata_generation text,published_at timestamptz)"
        )
        yield SimpleNamespace(
            connection=connection,
            control=control,
            schemas=schemas,
            loader=await connection.fetchval("SELECT current_user"),
            dsn=dsn,
        )
    finally:
        for schema in reversed(schemas):
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        await connection.close()


async def target(fixture):
    candidate = uuid4()
    result = FHIRCatalogTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate), "fhir_catalog_" + candidate.hex
    )
    fixture.schemas.append(result.schema_name)
    async with fixture.connection.transaction():
        assert (
            await store.create_fhir_catalog_candidate(
                fixture.connection, result, loader_role=fixture.loader, control_schema=fixture.control
            )
            == 5
        )
    return result


@pytest.mark.asyncio
async def test_discard_unregistered_candidate_is_exact_and_idempotent(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    for expected in (True, False):
        async with fixture.connection.transaction():
            assert (
                await store.discard_fhir_catalog_candidate(
                    fixture.connection, candidate, loader_role=fixture.loader, control_schema=fixture.control
                )
                is expected
            )
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", candidate.schema_name) is None


@pytest.mark.asyncio
async def test_discard_refuses_registered_snapshot(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control}".registry_fhir_catalog_snapshot(schema_name) VALUES($1)',
        candidate.schema_name,
    )
    with pytest.raises(ValueError, match="snapshot_immutable"):
        async with fixture.connection.transaction():
            await store.discard_fhir_catalog_candidate(
                fixture.connection, candidate, loader_role=fixture.loader, control_schema=fixture.control
            )
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", candidate.schema_name) is not None


@pytest.mark.asyncio
async def test_discard_never_cascades_to_external_dependents(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    await fixture.connection.execute(
        f'CREATE VIEW "{fixture.control}".external_reader AS '
        f'SELECT network_id FROM "{candidate.schema_name}".registry_fhir_catalog_network'
    )
    with pytest.raises(asyncpg.DependentObjectsStillExistError):
        async with fixture.connection.transaction():
            await store.discard_fhir_catalog_candidate(
                fixture.connection, candidate, loader_role=fixture.loader, control_schema=fixture.control
            )
    assert await fixture.connection.fetchval("SELECT to_regclass($1) IS NOT NULL", fixture.control + ".external_reader")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_prepare_failure_discards_only_its_unsealed_candidate(storage_database, monkeypatch, failure):
    from process import registry_fhir_catalog_sync as sync

    fixture = storage_database
    candidate = await target(fixture)

    async def failed_build(config, loader, source_pool, sealer_pool, staged):
        staged["target"] = candidate
        raise failure("candidate preparation interrupted")

    monkeypatch.setattr(sync, "_prepare_fhir_catalog_snapshot", failed_build)
    config = SimpleNamespace(loader_role=fixture.loader, grant=SimpleNamespace(control_schema=fixture.control))
    with pytest.raises(failure):
        await sync.prepare_fhir_catalog_snapshot(config, fixture.connection, None, None)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", candidate.schema_name) is None
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", fixture.control) is not None


def network_rows(generation, count):
    return [
        (generation, number, f"Example Network {number}", "[]", False, 1, None, f"example network {number}")
        for number in range(1, count + 1)
    ]


async def load(fixture, candidate, table, rows, **options):
    async with fixture.connection.transaction():
        return await store.write_fhir_catalog_rows(
            fixture.connection,
            candidate,
            table,
            rows,
            loader_role=fixture.loader,
            control_schema=fixture.control,
            **options,
        )


async def populate(fixture, candidate, count=1000):
    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await load(
        fixture,
        candidate,
        "registry_fhir_catalog_source",
        [(SOURCE, "source-one", "{}", "raw_fhir", count, count, instant, instant)],
    )
    await load(fixture, candidate, "registry_fhir_catalog_network", network_rows(OLD, count))
    await load(
        fixture,
        candidate,
        "registry_fhir_catalog_observation",
        [
            (SOURCE, "Organization", f"network-{number:05}", "organization", 0, json.dumps({"label": "x" * 40}))
            for number in range(1, count + 1)
        ],
    )
    binding_ids = [uuid4() for _ in range(count)]
    await load(
        fixture,
        candidate,
        "registry_fhir_catalog_binding",
        [
            (OLD, number, binding_id, "source-one", '{"status":"current"}')
            for number, binding_id in enumerate(binding_ids, 1)
        ],
    )
    await load(
        fixture,
        candidate,
        "registry_fhir_catalog_relationship",
        [
            (OLD, f"{number:064x}", number, "source-one", SOURCE, binding_id, None, None, "", "example", "{}")
            for number, binding_id in enumerate(binding_ids, 1)
        ],
    )


async def register(fixture, candidate):
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.control}".registry_fhir_catalog_snapshot VALUES($1,$2,clock_timestamp())',
        candidate.schema_name,
        OLD,
    )
    return {"schema_name": candidate.schema_name, "metadata_generation": OLD}


async def _assert_catalog_source_indexes(connection, schema_name):
    """Verify the FHIR catalog source and binding source indexes exist with expected definitions."""
    source_index = await connection.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE schemaname=$1 AND indexname='registry_fhir_catalog_binding_source_idx'",
        schema_name,
    )
    assert source_index.endswith("(metadata_generation, source_id)")
    source_id_index = await connection.fetchval(
        "SELECT indexdef FROM pg_indexes WHERE schemaname=$1 AND indexname='registry_fhir_catalog_source_id_idx'",
        schema_name,
    )
    assert source_id_index is not None and "source_id text_pattern_ops" in source_id_index


@pytest.mark.asyncio
async def test_copy_batches_are_bounded(storage_database):
    """Bounded COPY writes batches before building all catalog secondary indexes."""
    fixture = storage_database
    candidate = await target(fixture)
    counted = CountConnection(fixture.connection)
    async with fixture.connection.transaction():
        accounting = await store.write_fhir_catalog_rows(
            counted,
            candidate,
            "registry_fhir_catalog_network",
            network_rows(OLD, 1000),
            loader_role=fixture.loader,
            control_schema=fixture.control,
            max_rows=200,
            max_bytes=65536,
        )
    assert accounting["input_rows"] == accounting["affected_rows"] == 1000
    assert accounting["batches"] == counted.copies == 5
    assert accounting["payload_bytes"] > 1000
    assert counted.statements < 40
    assert (
        await fixture.connection.fetchval("SELECT count(*) FROM pg_indexes WHERE schemaname=$1", candidate.schema_name)
        == 6
    )
    evidence_index = await fixture.connection.fetchrow(
        "SELECT i.indisvalid,i.indisready,i.indnkeyatts,pg_get_expr(i.indexprs,i.indrelid) AS expression,"
        "i.indcollation[0]=(SELECT oid FROM pg_collation WHERE collname='C' AND collnamespace='pg_catalog'::regnamespace) "
        "AS c_collation,index.relowner=heap.relowner AS same_owner "
        "FROM pg_index i JOIN pg_class index ON index.oid=i.indexrelid JOIN pg_class heap ON heap.oid=i.indrelid "
        "WHERE i.indexrelid=$1::regclass",
        candidate.schema_name + ".registry_fhir_catalog_binding_evidence_idx",
    )
    assert evidence_index["indisvalid"] and evidence_index["indisready"]
    assert evidence_index["indnkeyatts"] == 1 and evidence_index["same_owner"] and evidence_index["c_collation"]
    assert "document_json ->> 'evidence_id'::text" in evidence_index["expression"]
    async with fixture.connection.transaction():
        assert (
            await store.create_fhir_catalog_indexes(
                fixture.connection, candidate, loader_role=fixture.loader, control_schema=fixture.control
            )
            == 11
        )
    assert (
        await fixture.connection.fetchval("SELECT count(*) FROM pg_indexes WHERE schemaname=$1", candidate.schema_name)
        == 17
    )
    await _assert_catalog_source_indexes(fixture.connection, candidate.schema_name)
    assert (
        await fixture.connection.fetchval(
            "SELECT reltuples FROM pg_class WHERE oid=$1::regclass",
            candidate.schema_name + ".registry_fhir_catalog_network",
        )
        == 1000
    )


async def native_files(connection, schema_name):
    return await connection.fetch(
        "SELECT c.oid,c.relname,c.relkind,c.relfilenode FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 ORDER BY c.oid",
        schema_name,
    )


async def assert_carried_rows(fixture, candidate):
    for table in PHYSICAL_TABLES:
        assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{candidate.schema_name}".{table}') == (
            1 if table.endswith("_source") else 1000
        )
    for table in ("network", "relationship", "binding"):
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_{table} '
                "WHERE metadata_generation<>$1",
                NEW,
            )
            == 0
        )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_binding '
            "WHERE document_json->>'status'='last_good'"
        )
        == 1000
    )


@pytest.mark.asyncio
async def test_clone_keeps_all_metadata_and_last_good(storage_database, monkeypatch):
    fixture = storage_database
    previous, candidate = await target(fixture), await target(fixture)
    await populate(fixture, previous)
    async with fixture.connection.transaction():
        await store.create_fhir_catalog_indexes(
            fixture.connection, previous, loader_role=fixture.loader, control_schema=fixture.control
        )
    snapshot = await register(fixture, previous)
    verification = AsyncMock()
    monkeypatch.setattr(store, "validate_fhir_catalog_snapshot_record", verification)
    old_files = await native_files(fixture.connection, previous.schema_name)
    counted = CountConnection(fixture.connection)
    async with fixture.connection.transaction():
        accounting = await store.copy_fhir_catalog_snapshot(
            counted,
            snapshot,
            candidate,
            NEW,
            loader_role=fixture.loader,
            control_schema=fixture.control,
        )
    verification.assert_awaited_once_with(counted, snapshot)
    assert accounting["input_rows"] == accounting["affected_rows"] == 4001
    assert accounting["batches"] == 5
    assert counted.statements < 20
    await assert_carried_rows(fixture, candidate)
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{previous.schema_name}".registry_fhir_catalog_binding '
            "WHERE document_json->>'status'='current'"
        )
        == 1000
    )
    assert old_files == await native_files(fixture.connection, previous.schema_name)
    async with fixture.connection.transaction():
        assert (
            await store.replace_fhir_catalog_relationships(
                fixture.connection, candidate, [SOURCE], loader_role=fixture.loader, control_schema=fixture.control
            )
            == 1000
        )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        == 1000
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_observation'
        )
        == 1000
    )


@pytest.mark.asyncio
async def test_failed_batches_preserve_published_source(storage_database, monkeypatch):
    fixture = storage_database
    previous, candidate = await target(fixture), await target(fixture)
    await populate(fixture, previous, count=2)
    await register(fixture, previous)
    with pytest.raises(ValueError, match="snapshot_immutable"):
        await load(fixture, previous, "registry_fhir_catalog_network", network_rows(NEW, 1))
    with pytest.raises(ValueError, match="row_too_large"):
        await load(
            fixture,
            candidate,
            "registry_fhir_catalog_network",
            network_rows(NEW, 1) + [(NEW, 2, "x" * 512, "[]", False, 1, None, "x" * 512)],
            max_rows=1,
            max_bytes=400,
        )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        == 0
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{previous.schema_name}".registry_fhir_catalog_network'
        )
        == 2
    )


@pytest.mark.asyncio
async def test_modes_columns_and_json_are_explicit(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    await load(fixture, candidate, "registry_fhir_catalog_network", network_rows(NEW, 1))
    changed_rows = [(NEW, 1, "Updated Network", "[]", False, 2, ["medical"], "updated")]
    assert (await load(fixture, candidate, "registry_fhir_catalog_network", changed_rows, mode="preserve"))[
        "affected_rows"
    ] == 0
    assert (await load(fixture, candidate, "registry_fhir_catalog_network", changed_rows))["affected_rows"] == 1
    assert (
        await fixture.connection.fetchval(
            f'SELECT display_name FROM "{candidate.schema_name}".registry_fhir_catalog_network'
        )
        == "Updated Network"
    )
    for options, reason in (({"mode": "erase"}, "mode_invalid"), ({"columns": ("network_id",)}, "columns_invalid")):
        with pytest.raises(ValueError, match=reason):
            await load(fixture, candidate, "registry_fhir_catalog_network", changed_rows, **options)
    instant = datetime(2026, 1, 1, tzinfo=timezone.utc)
    columns = store.TABLE_COLUMNS["registry_fhir_catalog_source"][:-1]
    await load(
        fixture,
        candidate,
        "registry_fhir_catalog_source",
        [(SOURCE, "source-one", '{"kind":"synthetic"}', "raw_fhir", 0, 0, instant)],
        columns=columns,
    )
    assert (
        await fixture.connection.fetchval(
            f"SELECT descriptor_json->>'kind' FROM \"{candidate.schema_name}\".registry_fhir_catalog_source"
        )
        == "synthetic"
    )


@pytest.mark.asyncio
async def test_set_based_clone_rolls_back_all_tables_and_can_retry(storage_database, monkeypatch):
    """A late-table copy failure rolls back every table, and retry preserves exact rows."""
    fixture = storage_database
    previous, candidate = await target(fixture), await target(fixture)
    await populate(fixture, previous, count=30)
    snapshot = await register(fixture, previous)
    monkeypatch.setattr(store, "validate_fhir_catalog_snapshot_record", AsyncMock())
    copy_arguments = (fixture.connection, snapshot, candidate, NEW)
    copy_options_by_name = {"loader_role": fixture.loader, "control_schema": fixture.control}
    namespace = '"' + candidate.schema_name + '"'
    await fixture.connection.execute(
        f"ALTER TABLE {namespace}.registry_fhir_catalog_source ADD CONSTRAINT refuse_copy CHECK (source_id='rejected')"
    )
    async with fixture.connection.transaction():
        with pytest.raises(asyncpg.CheckViolationError, match="refuse_copy"):
            await store.copy_fhir_catalog_snapshot(*copy_arguments, **copy_options_by_name)
        for table in PHYSICAL_TABLES:
            assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.{table}") == 0
        await fixture.connection.execute(
            f"ALTER TABLE {namespace}.registry_fhir_catalog_source DROP CONSTRAINT refuse_copy"
        )
        accounting = await store.copy_fhir_catalog_snapshot(*copy_arguments, **copy_options_by_name)
        retried = await store.copy_fhir_catalog_snapshot(*copy_arguments, **copy_options_by_name)
    assert accounting["input_rows"] == accounting["affected_rows"] == 121
    assert accounting["batches"] == len(PHYSICAL_TABLES)
    assert accounting["payload_bytes"] > 0
    assert retried == {**accounting, "affected_rows": 0}

    for table in PHYSICAL_TABLES:
        adjustments_by_field = {}
        if table.endswith(("_network", "_binding", "_relationship")):
            adjustments_by_field["metadata_generation"] = NEW
        previous_relation = f'"{previous.schema_name}".{table}'
        previous_rows = await fixture.connection.fetch(f"SELECT to_jsonb(rows)::text FROM {previous_relation} rows")
        actual_rows = await fixture.connection.fetch(f"SELECT to_jsonb(rows)::text FROM {namespace}.{table} rows")
        expected_rows = []
        for previous_row in previous_rows:
            document_by_field = {**json.loads(previous_row[0]), **adjustments_by_field}
            if table.endswith("_binding"):
                document_by_field["document_json"]["status"] = "last_good"
            expected_rows.append(json.dumps(document_by_field, sort_keys=True))
        assert sorted(json.dumps(json.loads(actual_row[0]), sort_keys=True) for actual_row in actual_rows) == sorted(
            expected_rows
        )


@pytest.mark.asyncio
async def test_native_lock_pins_candidate_ownership(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    observer = None
    try:
        observer = await asyncpg.connect(fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))
        async with fixture.connection.transaction():
            await store.write_fhir_catalog_rows(
                fixture.connection,
                candidate,
                "registry_fhir_catalog_network",
                network_rows(NEW, 1),
                loader_role=fixture.loader,
                control_schema=fixture.control,
            )
            with pytest.raises(asyncpg.LockNotAvailableError):
                async with observer.transaction():
                    await observer.execute("SET LOCAL lock_timeout='30ms'")
                    await observer.execute(
                        f'LOCK TABLE "{candidate.schema_name}".registry_fhir_catalog_network IN ACCESS EXCLUSIVE MODE'
                    )
        async with observer.transaction():
            await observer.execute(
                f'LOCK TABLE "{candidate.schema_name}".registry_fhir_catalog_network IN ACCESS EXCLUSIVE MODE'
            )
    finally:
        if observer is not None:
            await observer.close()


@pytest.mark.asyncio
async def test_registered_catalog_cannot_be_reopened_for_candidate_writes(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    rows = network_rows(NEW, 2)
    await load(fixture, candidate, "registry_fhir_catalog_network", rows)
    await register(fixture, candidate)
    before = await fixture.connection.fetch(
        f'SELECT * FROM "{candidate.schema_name}".registry_fhir_catalog_network ORDER BY network_id'
    )
    with pytest.raises(ValueError, match="snapshot_immutable"):
        await load(fixture, candidate, "registry_fhir_catalog_network", network_rows(NEW, 3))
    async with fixture.connection.transaction():
        with pytest.raises(ValueError, match="snapshot_immutable"):
            await store.replace_fhir_catalog_relationships(
                fixture.connection, candidate, (SOURCE,), loader_role=fixture.loader, control_schema=fixture.control
            )
    assert (
        await fixture.connection.fetch(
            f'SELECT * FROM "{candidate.schema_name}".registry_fhir_catalog_network ORDER BY network_id'
        )
        == before
    )


@pytest.mark.asyncio
async def test_reused_landing_keeps_each_write_atomic_and_mutable(storage_database):
    fixture = storage_database
    candidate = await target(fixture)
    connection = CountConnection(fixture.connection)
    table = "registry_fhir_catalog_network"
    write = partial(
        store.write_fhir_catalog_rows,
        connection,
        candidate,
        table,
        loader_role=fixture.loader,
        control_schema=fixture.control,
    )
    relation = f'"{candidate.schema_name}".{table}'
    first, second = network_rows(NEW, 2)
    invalid = (*second[:2], " ", *second[3:])
    updated = (*first[:2], "Updated", *first[3:])
    async with connection.transaction(isolation="repeatable_read"):
        async with store.fhir_catalog_copy_landings(connection, candidate):
            await write([first])
            with pytest.raises(asyncpg.CheckViolationError):
                await write([second, invalid], max_rows=1)
            assert await connection.fetchval(f"SELECT count(*) FROM {relation}") == 1
            assert (await write([updated], mode="preserve"))["affected_rows"] == 0
            assert (await write([first, updated, second]))["affected_rows"] == 2
            assert await connection.fetchval(f"SELECT display_name FROM {relation} WHERE network_id=1") == "Updated"
            await register(fixture, candidate)
            with pytest.raises(ValueError, match="snapshot_immutable"):
                await write([first])
    assert len(set(connection.copy_tables)) == 1
    assert await connection.fetchval("SELECT to_regclass($1)", "pg_temp." + connection.copy_tables[0]) is None


@pytest.mark.asyncio
async def test_copy_landings_follow_only_the_candidate_and_session(storage_database):
    fixture = storage_database
    candidate, other = await target(fixture), await target(fixture)
    connection = CountConnection(fixture.connection)
    write = partial(
        store.write_fhir_catalog_rows,
        connection,
        table="registry_fhir_catalog_network",
        copy_rows=network_rows(NEW, 1),
        loader_role=fixture.loader,
        control_schema=fixture.control,
    )
    peer = await asyncpg.connect(fixture.dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        async with connection.transaction():
            async with store.fhir_catalog_copy_landings(connection, candidate):
                inherited = copy_context()
                await write(candidate)
                await write(other)
                await write(candidate)
                async with peer.transaction():
                    await store.write_fhir_catalog_rows(
                        peer,
                        candidate,
                        "registry_fhir_catalog_network",
                        network_rows(NEW, 2)[1:],
                        loader_role=fixture.loader,
                        control_schema=fixture.control,
                    )
        async with connection.transaction():
            await asyncio.create_task(write(candidate), context=inherited)
    finally:
        await peer.close()
    primary, separate, reused, standalone = connection.copy_tables
    assert primary == reused
    assert len({primary, separate, standalone}) == 3
