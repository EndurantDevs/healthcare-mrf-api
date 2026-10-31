# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native history data observations, not a conversion, protected issuer or writer cut."""

import os
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_legacy_history as history
from tests.test_provider_directory_cms_canonical_layout_postgres import native_layout

pytestmark = pytest.mark.asyncio(loop_scope="module")


@asynccontextmanager
async def _reader(admin, *, readonly=True, isolation="REPEATABLE READ", assumed=False):
    url = make_url(os.environ["HLTHPRT_CMS_LAYOUT_TEST_DSN"]).set(
        database=await admin.fetchval("SELECT current_database()")
    )
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    database = Database()
    database._database_override = url.database
    try:
        async with AsyncSession(bind=engine) as session, session.begin(), database.bind_existing_session(session):
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL " + isolation + (" READ ONLY" if readonly else " READ WRITE"))
            )
            await session.execute(text("SET LOCAL search_path=pg_catalog"))
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            await session.execute(text("SET LOCAL lock_timeout='100ms'"))
            if assumed:
                await session.execute(text("SET LOCAL ROLE pg_read_all_data"))
            native = await session.connection()
            yield database, session, (await native.get_raw_connection()).driver_connection
    finally:
        await engine.dispose()


async def _expected(admin):
    roots_by_name = {
        row["relname"]: {"oid": row["oid"], "owner": row["owner"]}
        for row in await admin.fetch(
            "SELECT relname,oid::bigint,relowner::bigint AS owner FROM pg_catalog.pg_class "
            "WHERE relnamespace='mrf'::regnamespace AND relname=ANY($1::text[])",
            list(history.ROOTS),
        )
    }
    return {
        "database_oid": await admin.fetchval("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"),
        "schema_oid": await admin.fetchval("SELECT 'mrf'::regnamespace::oid::bigint"),
        "roots": roots_by_name,
    }


async def _read(reader, expected, datasets):
    return await history.read_cms_legacy_history(
        *reader, catalog_schema="mrf", expected_catalog=expected, dataset_ids=datasets
    )


async def _seed_dataset(admin, dataset):
    metadata = '{"source_ids":["cms-npd"],"source_release":{"source_id":"cms-npd","vector_sha256":"' + "a" * 64 + '"}}'
    await admin.execute(
        "INSERT INTO mrf.provider_directory_endpoint_dataset "
        "(dataset_id,endpoint_id,acquisition_root_run_id,status,is_current,resource_count,publication_metadata_json) "
        "VALUES($1,'history-endpoint',$3,'acquiring',false,1,$2::jsonb)",
        dataset,
        metadata,
        dataset + "-root",
    )
    await admin.execute(
        "INSERT INTO mrf.provider_directory_dataset_resource "
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "VALUES($1,'Organization','same-id',repeat('a',64),'{\"id\":\"same-id\"}'::jsonb)",
        dataset,
    )


async def _seed_witness(admin, dataset):
    await admin.execute(
        "INSERT INTO mrf.provider_directory_cms_npd_resource_witness "
        "(dataset_id,source_id,release_id,resource_type,resource_id,raw_payload_sha256,normalized_payload_hash,raw_payload_json) "
        "VALUES($1,'cms-npd',repeat('a',64),'Organization','same-id',repeat('b',64),repeat('a',64),'{}'::jsonb)",
        dataset,
    )


async def test_genuine_history_preserves_snapshot_during_disjoint_writes(native_layout):
    admin = native_layout
    await admin.execute(
        "INSERT INTO mrf.provider_directory_api_endpoint "
        "(endpoint_id,canonical_api_base,credential_descriptor_hash,endpoint_signature_hash) "
        "VALUES('history-endpoint','https://example.test/fhir',repeat('c',64),repeat('d',64))"
    )
    for dataset in ("history-one", "history-two", "history-peer"):
        await _seed_dataset(admin, dataset)
    for dataset in ("history-one", "history-two"):
        await _seed_witness(admin, dataset)
    await admin.execute(
        "INSERT INTO mrf.provider_directory_cms_npd_relationship "
        "(dataset_id,source_id,release_id,resource_type,resource_id,source_payload_hash,raw_payload_sha256,"
        "reference_field,parent_ordinal,reference_ordinal,target_type,resolution_status) "
        "VALUES('history-one','cms-npd',repeat('a',64),'Organization','same-id',repeat('a',64),repeat('b',64),"
        "'partOf',0,1,'Organization','unresolved')"
    )
    expected, datasets = await _expected(admin), ["history-one", "history-two"]
    async with _reader(admin) as reader:
        before = await _read(reader, expected, datasets)
        assert before["evidence"] == "read_only_snapshot"
        assert before["dataset_rows"][history.WITNESS] == dict.fromkeys(datasets, 1)
        assert before["content"][history.WITNESS]["rows"] == 2
        assert before["content"][history.RELATIONSHIP]["rows"] == 1
        assert before["dataset_rows"][history.RELATIONSHIP] == {"history-one": 1, "history-two": 0}
        await _seed_witness(admin, "history-peer")
        assert await _read(reader, expected, datasets) == before
    async with _reader(admin) as reader:
        with pytest.raises(ValueError, match="dataset_outside_bounds"):
            await _read(reader, expected, datasets)
        after = await _read(reader, expected, ["history-one", "history-peer", "history-two"])
        assert after["content"][history.WITNESS]["rows"] == 3
        assert after["content"][history.WITNESS]["sha256"] != before["content"][history.WITNESS]["sha256"]


@pytest.mark.parametrize(
    "search_path,permitted",
    [
        ("pg_catalog", True),
        ("pg_catalog, pg_temp", True),
        ("pg_temp, pg_catalog", False),
        ("pg_catalog, public", False),
        ("mrf, pg_catalog", False),
    ],
)
async def test_history_native_paths_preserve_original_admin_and_settings(native_layout, search_path, permitted):
    async with _reader(native_layout) as reader:
        database, session, connection = reader
        await connection.fetchval("SELECT pg_catalog.set_config('search_path',$1,true)", search_path)
        if permitted:
            actor = await history._state(database, session, connection)
            assert actor["session"] == actor["current"] and actor["readonly"] == "on"
        else:
            with pytest.raises(ValueError, match="search_path"):
                await history._state(database, session, connection)
        assert await connection.fetchval("SHOW search_path") == search_path


@pytest.mark.parametrize("mode", ["write", "read_committed", "assumed", "unbounded"])
async def test_wrong_snapshot_or_actor_refuses_without_changing_settings(native_layout, mode):
    expected = await _expected(native_layout)
    async with _reader(
        native_layout,
        readonly=mode != "write",
        isolation="READ COMMITTED" if mode == "read_committed" else "REPEATABLE READ",
        assumed=mode == "assumed",
    ) as reader:
        connection = reader[2]
        if mode == "unbounded":
            await connection.execute("SET LOCAL statement_timeout=0")
        settings = await connection.fetchrow(
            "SELECT current_setting('statement_timeout'),current_setting('search_path')"
        )
        with pytest.raises(ValueError, match="original_admin_snapshot_required|bounded_settings_required"):
            await _read(reader, expected, [])
        assert settings == await connection.fetchrow(
            "SELECT current_setting('statement_timeout'),current_setting('search_path')"
        )


@pytest.mark.parametrize("field", ["oid", "owner"])
async def test_changed_native_history_identity_refuses(native_layout, field):
    expected = await _expected(native_layout)
    expected["roots"][history.WITNESS][field] += 1
    async with _reader(native_layout) as reader:
        with pytest.raises(ValueError, match="roots_changed"):
            await _read(reader, expected, [])


@pytest.mark.parametrize("field,reason", [("database_oid", "database_changed"), ("schema_oid", "roots_changed")])
async def test_wrong_catalog_identity_refuses(native_layout, field, reason):
    expected = await _expected(native_layout)
    expected[field] += 1
    async with _reader(native_layout) as reader:
        with pytest.raises(ValueError, match=reason):
            await _read(reader, expected, [])


@pytest.mark.parametrize("edge", range(3))
async def test_native_composite_anti_joins_reject_cross_dataset_orphans(native_layout, edge):
    """Probe each SQL edge with temporary FK-free keys; genuine layout gates are tested separately."""
    async with native_layout.transaction():
        relations_by_root = {
            root: storage_relation("pg_temp", "edge_probe_" + str(index)) for index, root in enumerate(history.ROOTS)
        }
        for relation in relations_by_root.values():
            await native_layout.execute(
                f"CREATE TEMP TABLE {relation}(dataset_id text,resource_type text,resource_id text,PRIMARY KEY(dataset_id,resource_type,resource_id))"
            )
        resource_relation = relations_by_root[history.RESOURCE]
        witness_relation = relations_by_root[history.WITNESS]
        graph_relation = relations_by_root[history.RELATIONSHIP]
        await native_layout.execute(f"INSERT INTO {resource_relation} VALUES('a','Organization','same')")
        await native_layout.execute(
            f"INSERT INTO {witness_relation} VALUES($1,'Organization','same')", "b" if edge == 0 else "a"
        )
        await native_layout.execute(
            f"INSERT INTO {graph_relation} VALUES($1,'Organization','same')", "a" if edge == 0 else "b"
        )
        if edge == 2:
            await native_layout.execute(f"INSERT INTO {resource_relation} VALUES('b','Organization','same')")
        with pytest.raises(
            ValueError, match="relationship_changed:" + history._EDGES[edge][0] + ":" + history._EDGES[edge][1]
        ):
            await history._relationships(native_layout, relations_by_root)
        for relation in reversed(list(relations_by_root.values())):
            await native_layout.execute("DROP TABLE " + relation)
