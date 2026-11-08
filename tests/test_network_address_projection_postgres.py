# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native candidate-only projection and exact-site isolation checks."""

import asyncio
import os
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

from process.network_address_projection import (
    NetworkAddressProjectionError,
    PinnedAddressSource,
    project_network_address_arrays,
)
from process.network_membership_copy import MembershipCopyTarget
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def projection_db(serving_schema):
    connection, control_schema, _ = serving_schema
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    candidate_schema = copy_target.schema_name
    address_source = PinnedAddressSource(control_schema, "retained_addresses", "address-edition/α")
    location_ids = [uuid4() for _ in range(3)]
    observer = None
    try:
        await connection.execute(f'CREATE SCHEMA "{candidate_schema}"')
        await _prepare_tables(connection, control_schema, candidate_schema)
        await _seed_addresses(connection, control_schema, candidate_schema, location_ids)
        await connection.execute(
            f'INSERT INTO "{control_schema}".network_membership_candidate '
            "(candidate_id,dataset_id,schema_id,producer_id,schema_name,state,source_generations,"
            "approved_custom_revision,expected_head,expected_rows,accepted_rows) "
            "VALUES($1,$2,$3,$4,$5,'sealed',jsonb_build_object('unified_address',$6::text),0,0,4,4)",
            candidate_id,
            *[getattr(copy_target, name) for name in ("dataset_id", "schema_id", "producer_id")],
            candidate_schema,
            address_source.generation_id,
        )
        observer = await asyncpg.connect(
            os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
        )
        yield SimpleNamespace(
            connection=connection,
            observer=observer,
            control_schema=control_schema,
            copy_target=copy_target,
            address_source=address_source,
            location_ids=location_ids,
        )
    finally:
        if observer is not None:
            await observer.close()
        await connection.execute(f'DROP SCHEMA IF EXISTS "{candidate_schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", candidate_schema) is None


async def _prepare_tables(connection, control_schema, candidate_schema):
    await connection.execute(f"""CREATE TABLE "{control_schema}".retained_addresses(
        location_key varchar(64) PRIMARY KEY, entity_type varchar(64) NOT NULL, entity_id varchar(128) NOT NULL,
        postal_address text, plans_network_array integer[] NOT NULL, canonical_network_ids integer[])
    """)
    await connection.execute(f"""CREATE TABLE "{candidate_schema}".network_membership(
        network_id integer NOT NULL CHECK(network_id>0), provider_system text NOT NULL, provider_id text NOT NULL,
        location_id uuid NOT NULL, evidence_id text NOT NULL)
    """)
    await connection.execute(f"""CREATE TABLE "{candidate_schema}".provider_location_binding(
        provider_system text NOT NULL, provider_id text NOT NULL, location_id uuid NOT NULL,
        location_key varchar(64) NOT NULL, entity_type text NOT NULL, entity_id text NOT NULL,
        UNIQUE(provider_system,provider_id,location_id))
    """)


async def _seed_addresses(connection, control_schema, candidate_schema, location_ids):
    await connection.copy_records_to_table(
        "retained_addresses",
        schema_name=control_schema,
        records=[
            ("a" * 64, "npi", "provider-a", "shared postal address", [42], [0, None, 999]),
            ("b" * 64, "npi", "provider-a", "second office", [0], [999]),
            ("c" * 64, "npi", "provider-b", "shared postal address", [42], None),
            ("d" * 64, "npi", "provider-c", "other office", [99], [999]),
        ],
    )
    await connection.copy_records_to_table(
        "provider_location_binding",
        schema_name=candidate_schema,
        records=[
            ("npi", "provider-a", location_ids[0], "a" * 64, "npi", "provider-a"),
            ("npi", "provider-a", location_ids[1], "b" * 64, "npi", "provider-a"),
            ("npi", "provider-b", location_ids[2], "c" * 64, "npi", "provider-b"),
        ],
    )
    for membership_batch in [
        [
            (42, "npi", "provider-a", location_ids[0], "batch-a/42"),
            (7, "npi", "provider-a", location_ids[0], "batch-a/7"),
        ],
        [
            (42, "npi", "provider-a", location_ids[0], "batch-b/42"),
            (88, "npi", "provider-b", location_ids[2], "batch-b/88"),
        ],
    ]:
        await connection.copy_records_to_table(
            "network_membership", schema_name=candidate_schema, records=membership_batch
        )


async def _project(fixture, **changes):
    return await project_network_address_arrays(
        fixture.connection,
        changes.get("copy_target", fixture.copy_target),
        changes.get("address_source", fixture.address_source),
        control_schema=fixture.control_schema,
    )


@pytest.mark.asyncio
async def test_exact_sites_and_namespace_isolation(projection_db):
    fixture = projection_db
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        before_arrays = await fixture.observer.fetch(
            f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
        )
        async with fixture.connection.transaction():
            await asyncio.sleep(0)
            logged_queries.clear()
            receipt = await _project(fixture)
            await asyncio.sleep(0)
            projection_queries = list(logged_queries)
            assert (
                await fixture.observer.fetchval(
                    "SELECT to_regclass($1)", fixture.copy_target.schema_name + ".entity_address_unified"
                )
                is None
            )
        assert (
            await fixture.observer.fetch(
                f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
            )
            == before_arrays
        )
    projected_addresses = await fixture.connection.fetch(
        f'SELECT * FROM "{fixture.copy_target.schema_name}".entity_address_unified ORDER BY location_key'
    )
    assert [entry["canonical_network_ids"] for entry in projected_addresses] == [[7, 42], [], [88], []]
    assert [entry["plans_network_array"] for entry in projected_addresses] == [[42], [0], [42], [99]]
    assert (
        receipt.address_rows,
        receipt.membership_rows,
        receipt.distinct_memberships,
        receipt.projected_locations,
    ) == (4, 4, 3, 2)
    assert receipt.orphan_bindings == 0 and receipt.source_generation == fixture.address_source.generation_id
    assert not any(
        query.query.lstrip().startswith("UPDATE") or "TYPE INTEGER[] USING" in query.query
        for query in projection_queries
    )
    assert any("WITH NO DATA" in query.query for query in projection_queries)
    print(
        {
            "projection_driver_roundtrips": len(projection_queries),
            "driver_elapsed_seconds": round(sum(query.elapsed for query in projection_queries), 6),
        }
    )
    assert (
        await fixture.connection.fetchval(f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate')
        == "sealed"
    )
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)", fixture.copy_target.schema_name + ".network_projection_memberships"
        )
        is None
    )


@pytest.mark.asyncio
async def test_candidate_indexes_and_empty_arrays(projection_db):
    fixture = projection_db
    await fixture.connection.execute(f'DELETE FROM "{fixture.copy_target.schema_name}".network_membership')
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=0,accepted_rows=0'
    )
    async with fixture.connection.transaction():
        receipt = await _project(fixture)
    assert receipt.membership_rows == receipt.distinct_memberships == receipt.projected_locations == 0
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM \"{fixture.copy_target.schema_name}\".entity_address_unified WHERE canonical_network_ids='{{}}'"
        )
        == 4
    )
    index_definitions = await fixture.connection.fetch(
        "SELECT indexdef FROM pg_indexes WHERE schemaname=$1 AND tablename='entity_address_unified'",
        fixture.copy_target.schema_name,
    )
    assert any("UNIQUE" in entry["indexdef"] and "location_key" in entry["indexdef"] for entry in index_definitions)
    assert any("canonical_network_ids gin__int_ops" in entry["indexdef"] for entry in index_definitions)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_site", "wrong_entity", "wrong_location", "wrong_provider_system"])
async def test_bad_bindings_roll_back(projection_db, damage):
    fixture = projection_db
    assignments_by_damage = {
        "missing_site": "location_id='00000000-0000-0000-0000-000000000001'::uuid",
        "wrong_entity": "entity_id='different-provider'",
        "wrong_location": "location_key='missing-key'",
        "wrong_provider_system": "provider_system='different-system'",
    }
    await fixture.connection.execute(
        f'UPDATE "{fixture.copy_target.schema_name}".provider_location_binding SET {assignments_by_damage[damage]} '
        "WHERE provider_id='provider-a' AND location_key=$1",
        "a" * 64,
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkAddressProjectionError, match="exact provider/site"):
            await _project(fixture)
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", fixture.copy_target.schema_name + ".entity_address_unified"
            )
            is None
        )
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)", fixture.copy_target.schema_name + ".network_membership_projection_idx"
            )
            is None
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open", "ready", "published", "rejected"])
async def test_closed_candidate_is_rejected(projection_db, state):
    fixture = projection_db
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state=$1,validation_json='{{}}',index_ready=true",
        state,
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkAddressProjectionError, match="sealed or validated"):
            await _project(fixture)


@pytest.mark.asyncio
async def test_pins_and_caller_rollback(projection_db):
    fixture = projection_db
    with pytest.raises(NetworkAddressProjectionError, match="caller-owned"):
        await _project(fixture)
    async with fixture.connection.transaction():
        for field in ("producer_id", "dataset_id", "schema_id"):
            with pytest.raises(NetworkAddressProjectionError, match="ownership"):
                await _project(fixture, copy_target=replace(fixture.copy_target, **{field: str(uuid4())}))
        with pytest.raises(NetworkAddressProjectionError, match="generation mismatch"):
            await _project(fixture, address_source=replace(fixture.address_source, generation_id="stale-generation"))
    outer_transaction = fixture.connection.transaction()
    await outer_transaction.start()
    try:
        await _project(fixture)
    finally:
        await outer_transaction.rollback()
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)", fixture.copy_target.schema_name + ".entity_address_unified"
        )
        is None
    )


@pytest.mark.asyncio
async def test_bitmap_gin_structure(projection_db):
    fixture = projection_db
    await fixture.connection.execute(f"""INSERT INTO "{fixture.control_schema}".retained_addresses
        SELECT lpad(to_hex(sequence_id),64,'0'),'npi','extra-'||sequence_id,'synthetic address','{{42}}'::int[],'{{999}}'::int[]
        FROM generate_series(1,20000) sequence_id""")
    async with fixture.connection.transaction():
        receipt = await _project(fixture)
        await fixture.connection.execute("SET LOCAL enable_seqscan=off")
        query_plan = await fixture.connection.fetch(
            f'EXPLAIN SELECT location_key FROM "{fixture.copy_target.schema_name}".entity_address_unified '
            "WHERE canonical_network_ids && ARRAY[42]::integer[]"
        )
    plan_text = "\n".join(entry[0] for entry in query_plan)
    assert "Bitmap Index Scan on canonical_network_ids_gin" in plan_text
    assert receipt.address_rows == 20004


@pytest.mark.asyncio
@pytest.mark.parametrize("column_mode", ["absent", "wrong_type"])
async def test_canonical_column_is_reset(projection_db, column_mode):
    fixture = projection_db
    alteration = (
        "DROP COLUMN canonical_network_ids"
        if column_mode == "absent"
        else "ALTER COLUMN canonical_network_ids TYPE text[] USING canonical_network_ids::text[]"
    )
    await fixture.connection.execute(f'ALTER TABLE "{fixture.control_schema}".retained_addresses {alteration}')
    async with fixture.connection.transaction():
        await _project(fixture)
    projected_arrays = await fixture.connection.fetch(
        f"SELECT canonical_network_ids,pg_typeof(canonical_network_ids)::text AS array_type "
        f'FROM "{fixture.copy_target.schema_name}".entity_address_unified ORDER BY location_key'
    )
    assert [entry["canonical_network_ids"] for entry in projected_arrays] == [[7, 42], [], [88], []]
    assert all(entry["array_type"] == "integer[]" for entry in projected_arrays)


@pytest.mark.asyncio
async def test_accounting_and_orphan_binding(projection_db):
    fixture = projection_db
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET expected_rows=5'
    )
    async with fixture.connection.transaction():
        with pytest.raises(NetworkAddressProjectionError, match="accounting is incomplete"):
            await _project(fixture)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate '
        "SET expected_rows=4,state='validated',validation_json='{}'"
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.copy_target.schema_name}".provider_location_binding '
        "VALUES('npi','unselected-provider',$1,'missing-key','npi','unselected-provider')",
        uuid4(),
    )
    async with fixture.connection.transaction():
        receipt = await _project(fixture)
    assert receipt.orphan_bindings == 1 and receipt.projected_locations == 2


@pytest.mark.asyncio
async def test_existing_projection_is_preserved(projection_db):
    fixture = projection_db
    async with fixture.connection.transaction():
        await _project(fixture)
        with pytest.raises(asyncpg.DuplicateTableError):
            await _project(fixture)
        assert await fixture.connection.fetchval(
            f'SELECT canonical_network_ids FROM "{fixture.copy_target.schema_name}".entity_address_unified '
            "WHERE location_key=$1",
            "a" * 64,
        ) == [7, 42]


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_name": "x;DROP"},
        {"table_name": "é"},
        {"table_name": "x" * 64},
        {"generation_id": ""},
        {"generation_id": "x" * 129},
        {"generation_id": "bad\nsource"},
        {"generation_id": 42},
    ],
)
def test_pinned_source_bounds(changes):
    with pytest.raises(NetworkAddressProjectionError):
        PinnedAddressSource(
            **({"schema_name": "retained", "table_name": "addresses", "generation_id": "edition-a"} | changes)
        )


@pytest.mark.asyncio
async def test_bounded_copy_preserves_complete_rows_and_maximum_integer(projection_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap
    from tests.test_network_custom_address_source_postgres import BootstrapCopyConnection, _observe_bootstrap_spools

    fixture = projection_db
    await fixture.connection.execute(
        f'UPDATE "{fixture.copy_target.schema_name}".network_membership SET network_id=2147483647 WHERE network_id=7'
    )
    source_rows = await fixture.connection.fetch(
        f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
    )
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_ROWS", 2)
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_BYTES", 512)
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection)
    async with fixture.connection.transaction():
        receipt = await project_network_address_arrays(
            proxy, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
    projected_rows = await fixture.connection.fetch(
        f'SELECT * FROM "{fixture.copy_target.schema_name}".entity_address_unified ORDER BY location_key'
    )
    assert receipt.address_rows == len(source_rows) == 4
    assert [address_row["canonical_network_ids"] for address_row in projected_rows] == [[42, 2147483647], [], [88], []]
    assert list(projected_rows[0].keys()) == list(source_rows[0].keys())
    assert len(proxy.exports) == len(proxy.imports) >= 2
    assert all(size <= 512 for size in proxy.import_bytes)
    assert spools and all(spool.closed for spool in spools)
    assert [
        {
            column_name: field_value
            for column_name, field_value in address_row.items()
            if column_name != "canonical_network_ids"
        }
        for address_row in projected_rows
    ] == [
        {
            column_name: field_value
            for column_name, field_value in address_row.items()
            if column_name != "canonical_network_ids"
        }
        for address_row in source_rows
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["export_count", "import_count"])
async def test_copy_accounting_failure_rolls_back_then_retries(projection_db, monkeypatch, failure):
    import process.network_bootstrap_sources as bootstrap
    from tests.test_network_custom_address_source_postgres import BootstrapCopyConnection, _observe_bootstrap_spools

    fixture = projection_db
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection, failure=failure)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkAddressProjectionError, match="COPY.*accounting"):
            await project_network_address_arrays(
                proxy, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
            )
        for name in ("entity_address_unified", "network_projection_memberships"):
            assert (
                await fixture.connection.fetchval(
                    "SELECT to_regclass($1)", fixture.copy_target.schema_name + "." + name
                )
                is None
            )
        assert await fixture.connection.fetchval("SELECT 1") == 1
        assert spools and all(spool.closed for spool in spools)
        assert (await _project(fixture)).address_rows == 4


@pytest.mark.asyncio
async def test_copy_cancellation_keeps_caller_and_source_usable(projection_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap
    from tests.test_network_custom_address_source_postgres import BootstrapCopyConnection, _observe_bootstrap_spools

    fixture = projection_db
    copying = asyncio.Event()
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection, copying=copying)
    async with fixture.connection.transaction():
        task = asyncio.create_task(
            project_network_address_arrays(
                proxy, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
            )
        )
        await asyncio.wait_for(copying.wait(), 3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert (
            await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".retained_addresses')
            == 4
        )
        for name in ("entity_address_unified", "network_projection_memberships"):
            assert (
                await fixture.connection.fetchval(
                    "SELECT to_regclass($1)", fixture.copy_target.schema_name + "." + name
                )
                is None
            )
        assert spools and all(spool.closed for spool in spools)
        assert (await _project(fixture)).address_rows == 4
