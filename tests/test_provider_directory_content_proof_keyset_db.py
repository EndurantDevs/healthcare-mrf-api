# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import uuid
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError, OperationalError

from db.connection import Database
from db.models import ProviderDirectoryDatasetResource
from process.provider_directory_resource_hash import (
    legacy_resource_payload_sha256,
)
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.test_provider_directory_content_proof_keyset import _cursor_migration

importer = importlib.import_module("process.provider_directory_fhir")
DATASET_ID = "dataset-keyset"
RESOURCE_TYPE = "PractitionerRole"
ROW_COUNT = 20_000
CURSOR_ID = "00015000"
BATCH_SIZE = 7


async def _require_disposable_postgres(database: Database) -> None:
    try:
        database_name = str(await database.scalar("SELECT current_database();") or "")
    except OSError, OperationalError:
        pytest.skip("content-proof keyset test needs disposable Postgres")
    if "test" not in database_name.lower():
        pytest.skip("content-proof keyset test needs a test database")


@asynccontextmanager
async def _content_proof_database(monkeypatch, *, partitioned=False, populated_partitions=True, mixed_case=False):
    prefix = "ProviderDirectoryKeyset" if mixed_case else "provider_directory_keyset"
    schema = f"{prefix}_{uuid.uuid4().hex[:12]}"
    database = Database()
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv("DB_SCHEMA", schema)
        is_schema_created = True
        await database.status(f'CREATE SCHEMA "{schema}";')
        await database.status(
            f"""
            CREATE TABLE "{schema}".provider_directory_dataset_resource (
                dataset_id varchar(96) NOT NULL,
                resource_type varchar(64) COLLATE "en-x-icu" NOT NULL,
                resource_id varchar(256) COLLATE "en-x-icu" NOT NULL,
                payload_hash varchar(64) NOT NULL,
                payload_json jsonb NOT NULL,
                acquired_resource_sha256 varchar(64),
                PRIMARY KEY (dataset_id, resource_type, resource_id)
            ) {"PARTITION BY LIST (dataset_id)" if partitioned else ""};
            """
        )
        if partitioned and populated_partitions:
            for table_name, dataset_id in (
                ("pd_dataset_history", DATASET_ID),
                ("pd_dataset_candidate", "dataset-decoy"),
            ):
                await database.status(
                    f'CREATE TABLE "{schema}".{table_name} PARTITION OF '
                    f"\"{schema}\".provider_directory_dataset_resource FOR VALUES IN ('{dataset_id}')"
                )
        yield database, schema
    finally:
        if is_schema_created:
            await database.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
            assert await database.scalar("SELECT to_regnamespace(:schema)", schema=f'"{schema}"') is None
        await database.disconnect()


async def _seed_content_rows(database: Database, schema: str) -> None:
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id,
            payload_hash, payload_json
        )
        SELECT :dataset_id, :resource_type,
               lpad(row_number::text, 8, '0'),
               repeat('a', 64), '{{}}'::jsonb
          FROM generate_series(1, :row_count) AS row_number;
        """,
        dataset_id=DATASET_ID,
        resource_type=RESOURCE_TYPE,
        row_count=ROW_COUNT,
    )
    await database.status(
        f"""
        INSERT INTO "{schema}".provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id,
            payload_hash, payload_json
        )
        SELECT 'dataset-decoy', :resource_type,
               lpad(row_number::text, 8, '0'),
               repeat('b', 64), '{{}}'::jsonb
          FROM generate_series(1, :row_count) AS row_number;
        """,
        resource_type=RESOURCE_TYPE,
        row_count=ROW_COUNT,
    )
    migration = _cursor_migration()
    await run_migration(database.engine, migration, "upgrade")
    original_index = await _index_state(database, schema)
    await run_migration(database.engine, migration, "upgrade")
    assert await _index_state(database, schema) == original_index
    await database.status(f'ANALYZE "{schema}".provider_directory_dataset_resource;')


def _plan_nodes(raw_plan):
    plan_root = raw_plan[0]["Plan"]
    pending_nodes = [plan_root]
    while pending_nodes:
        plan_node = pending_nodes.pop()
        yield plan_node
        pending_nodes.extend(plan_node.get("Plans", ()))


async def _content_page_and_plan(database: Database):
    query = (
        importer._endpoint_dataset_hash_page_sql(
            True,
            include_payload_json=True,
        )
        .strip()
        .removesuffix(";")
    )
    params_by_name = {
        "dataset_id": DATASET_ID,
        "after_resource_type": RESOURCE_TYPE,
        "after_resource_id": CURSOR_ID,
        "batch_size": BATCH_SIZE,
    }
    async with database.acquire() as connection:
        await connection.status("SET plan_cache_mode = force_generic_plan;")
        page_rows = await connection.all(query, **params_by_name)
        raw_plan = await connection.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query,
            **params_by_name,
        )
    return page_rows, list(_plan_nodes(raw_plan))


async def _acquired_page_and_plan(database: Database):
    query = importer._subset_acquired_page_sql(True).strip().removesuffix(";")
    params_by_name = {
        "dataset_id": DATASET_ID,
        "resource_types": [RESOURCE_TYPE],
        "after_resource_type": RESOURCE_TYPE,
        "after_resource_id": CURSOR_ID,
        "batch_size": BATCH_SIZE,
    }
    async with database.acquire() as connection:
        await connection.status("SET plan_cache_mode = force_generic_plan;")
        page_rows = await connection.all(query, **params_by_name)
        raw_plan = await connection.scalar(
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query,
            **params_by_name,
        )
    return page_rows, list(_plan_nodes(raw_plan))


async def _seed_overlapping_proof_rows(
    database: Database,
    schema: str,
) -> list[tuple[str, str, str]]:
    target_payload_by_field = {}
    target_hash = legacy_resource_payload_sha256(target_payload_by_field)
    target_rows = [
        ("Location", "location-1", target_hash),
        ("LU:PractitionerRole:pass:1", "excluded-1", target_hash),
        ("Organization", "organization-1", target_hash),
        ("PractitionerRole", "Role-A", target_hash),
        ("PractitionerRole", "Role-a", target_hash),
        ("PractitionerRole", "role-A", target_hash),
        ("PractitionerRole", "role-a", target_hash),
    ]
    for dataset_id, payload_by_field in (
        (DATASET_ID, target_payload_by_field),
        ("dataset-decoy", {"decoy": True}),
    ):
        for resource_type, resource_id, _payload_hash in target_rows:
            await database.status(
                f"""
                INSERT INTO "{schema}".provider_directory_dataset_resource (
                    dataset_id, resource_type, resource_id,
                    payload_hash, payload_json
                ) VALUES (
                    :dataset_id, :resource_type, :resource_id,
                    :payload_hash, CAST(:payload_json AS jsonb)
                );
                """,
                dataset_id=dataset_id,
                resource_type=resource_type,
                resource_id=resource_id,
                payload_hash=legacy_resource_payload_sha256(payload_by_field),
                payload_json=json.dumps(
                    payload_by_field,
                    sort_keys=True,
                ),
            )
    return [target_row for target_row in target_rows if not target_row[0].startswith("LU:")]


def _expected_proof_identity(
    target_rows: list[tuple[str, str, str]],
) -> tuple[str, dict[str, str], dict[str, int]]:
    identity_by_type: dict[str, list[str]] = {}
    ordered_identities = []
    for target_row in sorted(target_rows):
        stable_identity = importer._stable_identity_json(target_row)
        ordered_identities.append(stable_identity)
        identity_by_type.setdefault(target_row[0], []).append(stable_identity)
    dataset_hash = hashlib.sha256("\n".join(ordered_identities).encode()).hexdigest()
    return (
        dataset_hash,
        {
            resource_type: hashlib.sha256("\n".join(identities).encode()).hexdigest()
            for resource_type, identities in identity_by_type.items()
        },
        {resource_type: len(identities) for resource_type, identities in identity_by_type.items()},
    )


@pytest.mark.asyncio
async def test_postgres_content_proof_pages_one_dataset_without_leakage(
    monkeypatch,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        target_rows = await _seed_overlapping_proof_rows(database, schema)
        monkeypatch.setattr(
            importer,
            "ENDPOINT_DATASET_HASH_BATCH_SIZE",
            2,
        )
        proof = await importer._endpoint_dataset_content_proof(
            database,
            DATASET_ID,
            verify_payload_hashes=True,
            resource_hash_contract=importer.LEGACY_RESOURCE_HASH_CONTRACT,
        )

    dataset_hash, hashes_by_type, counts_by_type = _expected_proof_identity(target_rows)
    assert proof.dataset_hash == dataset_hash
    assert proof.resource_count == len(target_rows)
    assert proof.resource_hashes == hashes_by_type
    assert proof.resource_counts == counts_by_type


@pytest.mark.asyncio
async def test_postgres_content_proof_cursor_is_a_bounded_index_range(
    monkeypatch,
    record_testsuite_property,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        await _seed_content_rows(database, schema)
        page_rows, plan_nodes = await _content_page_and_plan(database)

    assert [page_record.resource_id for page_record in page_rows] == [
        f"{resource_id:08d}" for resource_id in range(15_001, 15_001 + BATCH_SIZE)
    ]
    resource_nodes = [
        plan_node for plan_node in plan_nodes if plan_node.get("Relation Name") == "provider_directory_dataset_resource"
    ]
    assert resource_nodes
    assert all(plan_node.get("Node Type") != "Seq Scan" for plan_node in resource_nodes)
    index_conditions = " ".join(str(plan_node.get("Index Cond", "")) for plan_node in resource_nodes)
    rows_inspected = sum(
        int(plan_node.get("Actual Rows", 0)) + int(plan_node.get("Rows Removed by Filter", 0))
        for plan_node in resource_nodes
    )
    assert rows_inspected < 100
    index_names = {str(plan_node.get("Index Name", "")) for plan_node in resource_nodes}
    assert _cursor_migration().INDEX_NAME in index_names
    assert all(
        identity_column in index_conditions for identity_column in ("dataset_id", "resource_type", "resource_id")
    )
    assert not any("resource_id" in str(plan_node.get("Filter", "")) for plan_node in resource_nodes)
    record_testsuite_property("content_cursor_rows_inspected", rows_inspected)
    record_testsuite_property("content_cursor_index", _cursor_migration().INDEX_NAME)
    record_testsuite_property("content_cursor_index_conditions", index_conditions)


@pytest.mark.asyncio
async def test_postgres_subset_acquired_cursor_is_a_bounded_index_range(
    monkeypatch,
    record_testsuite_property,
):
    async with _content_proof_database(monkeypatch) as (database, schema):
        await _seed_content_rows(database, schema)
        page_rows, plan_nodes = await _acquired_page_and_plan(database)

    assert [page_record.resource_id for page_record in page_rows] == [
        f"{resource_id:08d}" for resource_id in range(15_001, 15_001 + BATCH_SIZE)
    ]
    resource_nodes = [
        plan_node for plan_node in plan_nodes if plan_node.get("Relation Name") == "provider_directory_dataset_resource"
    ]
    assert resource_nodes
    assert all(plan_node.get("Node Type") != "Seq Scan" for plan_node in resource_nodes)
    index_conditions = " ".join(str(plan_node.get("Index Cond", "")) for plan_node in resource_nodes)
    rows_inspected = sum(
        int(plan_node.get("Actual Rows", 0)) + int(plan_node.get("Rows Removed by Filter", 0))
        for plan_node in resource_nodes
    )
    assert rows_inspected < 100
    assert any(str(plan_node.get("Index Name", "")) == _cursor_migration().INDEX_NAME for plan_node in resource_nodes)
    assert all(
        identity_column in index_conditions for identity_column in ("dataset_id", "resource_type", "resource_id")
    )
    record_testsuite_property("acquired_cursor_rows_inspected", rows_inspected)
    record_testsuite_property("acquired_cursor_index", _cursor_migration().INDEX_NAME)
    record_testsuite_property("acquired_cursor_index_conditions", index_conditions)


async def _index_state(database, schema):
    return await database.first(
        "SELECT c.oid,i.indisvalid,i.indisready,i.indislive FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_index i ON i.indexrelid=c.oid "
        "WHERE n.nspname=:schema AND c.relname=:name",
        schema=schema,
        name=_cursor_migration().INDEX_NAME,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("partitioned", [False, True])
async def test_cursor_migration_lifecycle_in_quoted_schema(monkeypatch, partitioned):
    async with _content_proof_database(monkeypatch, partitioned=partitioned, mixed_case=True) as (database, schema):
        migration = _cursor_migration()
        await run_migration(database.engine, migration, "upgrade")
        original = await _index_state(database, schema)
        assert original is not None and original.indisvalid and original.indisready and original.indislive
        await run_migration(database.engine, migration, "upgrade")
        assert await _index_state(database, schema) == original
        await run_migration(database.engine, migration, "downgrade")
        assert await _index_state(database, schema) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["collation", "keys", "predicate", "other_table"])
async def test_cursor_migration_rejects_same_name_shape_drift(monkeypatch, damage):
    async with _content_proof_database(monkeypatch) as (database, schema):
        migration = _cursor_migration()
        table = migration.TABLE_NAME
        keys = migration.INDEX_KEYS
        predicate = ""
        if damage == "collation":
            keys = "dataset_id,resource_type,resource_id"
        elif damage == "keys":
            keys = 'dataset_id,resource_id COLLATE "C",resource_type COLLATE "C"'
        elif damage == "predicate":
            predicate = " WHERE resource_type='Location'"
        else:
            table = "decoy_resource"
            await database.status(f'CREATE TABLE "{schema}".{table} (LIKE "{schema}".{migration.TABLE_NAME})')
        await database.status(f'CREATE INDEX "{migration.INDEX_NAME}" ON "{schema}".{table} ({keys}){predicate}')
        original = await _index_state(database, schema)
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_index_mismatch$"):
            await run_migration(database.engine, migration, "upgrade")
        assert await _index_state(database, schema) == original


@pytest.mark.asyncio
async def test_fresh_model_index_is_adopted_without_rebuild(monkeypatch):
    async with _content_proof_database(monkeypatch) as (database, schema):
        migration = _cursor_migration()
        table = ProviderDirectoryDatasetResource.__table__.to_metadata(MetaData(), schema=schema)
        declared = next(index for index in table.indexes if index.name == migration.INDEX_NAME)
        async with database.engine.begin() as connection:
            await connection.run_sync(lambda current: declared.create(current))
        original = await _index_state(database, schema)
        await run_migration(database.engine, migration, "upgrade")
        assert await _index_state(database, schema) == original
        await run_migration(database.engine, migration, "downgrade")
        assert await _index_state(database, schema) is None


async def _wait_for_invalid_index(database, schema, task, *, index_name=None):
    for _attempt in range(100):
        if task.done():
            await task
            pytest.fail("Concurrent build finished before the held writer released")
        state = await database.first(
            "SELECT c.oid,i.indisvalid,i.indisready,i.indislive FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_index i ON i.indexrelid=c.oid "
            "WHERE n.nspname=:schema AND c.relname=:name",
            schema=schema,
            name=index_name or _cursor_migration().INDEX_NAME,
        )
        if state is not None and not state.indisvalid:
            return state
        await asyncio.sleep(0.02)
    pytest.fail("Concurrent build did not reach its registered invalid state")


@pytest.mark.asyncio
async def test_interrupted_concurrent_index_is_retried_with_readers_and_writers(monkeypatch):
    async with _content_proof_database(monkeypatch) as (database, schema):
        migration = _cursor_migration()
        async with database.engine.connect() as writer:
            transaction = await writer.begin()
            await writer.execute(
                text(f'UPDATE "{schema}".{migration.TABLE_NAME} SET payload_hash=payload_hash WHERE false')
            )
            build = asyncio.create_task(run_migration(database.engine, migration, "upgrade"))
            try:
                invalid = await _wait_for_invalid_index(database, schema, build)
                await asyncio.wait_for(database.scalar(f'SELECT count(*) FROM "{schema}".{migration.TABLE_NAME}'), 1)
                await asyncio.wait_for(
                    database.status(
                        f'UPDATE "{schema}".{migration.TABLE_NAME} SET payload_hash=payload_hash WHERE false'
                    ),
                    1,
                )
                build_pids = await database.all(
                    "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                    "AND state='active' AND query LIKE :query AND pid<>pg_backend_pid() LIMIT 2",
                    query=migration._create_index_sql(schema) + "%",
                )
                assert len(build_pids) == 1
                assert await database.scalar("SELECT pg_cancel_backend(:pid)", pid=build_pids[0].pid)
                with pytest.raises(DBAPIError) as interrupted:
                    await build
                assert interrupted.value.orig.sqlstate == "57014"
            finally:
                if not build.done():
                    build.cancel()
                    await asyncio.gather(build, return_exceptions=True)
                await transaction.rollback()
        assert not (await _index_state(database, schema)).indisvalid
        await run_migration(database.engine, migration, "upgrade")
        rebuilt = await _index_state(database, schema)
        assert rebuilt.oid != invalid.oid
        assert rebuilt.indisvalid and rebuilt.indisready and rebuilt.indislive


async def _cursor_tree(database, schema):
    return await database.all(
        "SELECT c.oid,c.relname,i.indrelid,i.indisvalid,i.indisready,i.indislive "
        "FROM pg_partition_tree(CAST(:name AS regclass)) tree "
        "JOIN pg_class c ON c.oid=tree.relid JOIN pg_index i ON i.indexrelid=c.oid ORDER BY c.oid",
        name=f'"{schema}"."{_cursor_migration().INDEX_NAME}"',
    )


@pytest.mark.asyncio
async def test_partitioned_populated_history_cursor_and_future_partition(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        await _seed_content_rows(database, schema)
        migration = _cursor_migration()
        original = await _cursor_tree(database, schema)
        assert len(original) == 3
        assert all(
            index_record.indisvalid and index_record.indisready and index_record.indislive for index_record in original
        )
        for query_page in (_content_page_and_plan, _acquired_page_and_plan):
            page_rows, plan = await query_page(database)
            assert [index_record.resource_id for index_record in page_rows] == [
                f"{number:08d}" for number in range(15_001, 15_008)
            ]
            scans = [node for node in plan if node.get("Relation Name") == "pd_dataset_history"]
            assert scans and all(node["Node Type"] != "Seq Scan" for node in scans)
            assert sum(node["Actual Rows"] + node.get("Rows Removed by Filter", 0) for node in scans) < 100
            assert all(
                node.get("Index Name") == migration._leaf_index_name(schema, "pd_dataset_history") for node in scans
            )
        await database.status(
            f'CREATE TABLE "{schema}".future_candidate (LIKE "{schema}".{migration.TABLE_NAME} INCLUDING ALL)'
        )
        await database.status(
            f'ALTER TABLE "{schema}".{migration.TABLE_NAME} ATTACH PARTITION "{schema}".future_candidate '
            "FOR VALUES IN ('dataset-future')"
        )
        await run_migration(database.engine, migration, "upgrade")
        successor = await _cursor_tree(database, schema)
        assert len(successor) == 4
        assert {index_record.oid for index_record in original}.issubset(
            {index_record.oid for index_record in successor}
        )
        assert all(
            index_record.indisvalid and index_record.indisready and index_record.indislive for index_record in successor
        )
        await run_migration(database.engine, migration, "downgrade")
        assert await _index_state(database, schema) is None


@pytest.mark.asyncio
async def test_partitioned_model_index_adoption_preserves_complete_native_tree(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        migration = _cursor_migration()
        table = ProviderDirectoryDatasetResource.__table__.to_metadata(MetaData(), schema=schema)
        declared = next(index for index in table.indexes if index.name == migration.INDEX_NAME)
        async with database.engine.begin() as connection:
            await connection.run_sync(lambda current: declared.create(current))
        original = await _cursor_tree(database, schema)
        await run_migration(database.engine, migration, "upgrade")
        assert await _cursor_tree(database, schema) == original
        assert len(original) == 3


@pytest.mark.asyncio
async def test_empty_partition_parent_is_valid_and_propagates_future_index(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True, populated_partitions=False) as (database, schema):
        migration = _cursor_migration()
        await run_migration(database.engine, migration, "upgrade")
        original = await _index_state(database, schema)
        assert original.indisvalid and original.indisready and original.indislive
        await database.status(
            f'CREATE TABLE "{schema}".future_leaf PARTITION OF "{schema}".{migration.TABLE_NAME} '
            "FOR VALUES IN ('future')"
        )
        await run_migration(database.engine, migration, "upgrade")
        assert await _index_state(database, schema) == original
        assert len(await _cursor_tree(database, schema)) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["collation", "predicate", "other_table"])
async def test_partition_leaf_shape_drift_is_rejected_without_rebuild(monkeypatch, damage):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        migration = _cursor_migration()
        name = migration._leaf_index_name(schema, "pd_dataset_history")
        keys = migration.INDEX_KEYS if damage != "collation" else "dataset_id,resource_type,resource_id"
        predicate = " WHERE resource_type='Location'" if damage == "predicate" else ""
        target = "pd_dataset_candidate" if damage == "other_table" else "pd_dataset_history"
        await database.status(f'CREATE INDEX "{name}" ON "{schema}".{target} ({keys}){predicate}')
        original = await database.scalar("SELECT to_regclass(:name)::oid", name=f'"{schema}"."{name}"')
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_index_mismatch$"):
            await run_migration(database.engine, migration, "upgrade")
        assert await database.scalar("SELECT to_regclass(:name)::oid", name=f'"{schema}"."{name}"') == original


@pytest.mark.asyncio
async def test_partition_invalid_parent_and_concurrent_leaf_resume_preserve_valid_child(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        migration = _cursor_migration()
        await database.status(migration._create_index_sql(schema, metadata_only=True))
        history_name = migration._leaf_index_name(schema, "pd_dataset_history")
        await database.status(
            migration._create_index_sql(schema, "pd_dataset_history", history_name).replace("CONCURRENTLY ", "")
        )
        await database.status(
            f'ALTER INDEX "{schema}"."{migration.INDEX_NAME}" ATTACH PARTITION "{schema}"."{history_name}"'
        )
        original = await _cursor_tree(database, schema)
        parent = await _index_state(database, schema)
        assert not parent.indisvalid
        candidate_name = migration._leaf_index_name(schema, "pd_dataset_candidate")
        async with database.engine.connect() as writer:
            transaction = await writer.begin()
            await writer.execute(
                text(f'UPDATE "{schema}".pd_dataset_candidate SET payload_hash=payload_hash WHERE false')
            )
            build = asyncio.create_task(run_migration(database.engine, migration, "upgrade"))
            try:
                invalid = await _wait_for_invalid_index(database, schema, build, index_name=candidate_name)
                await asyncio.wait_for(database.scalar(f'SELECT count(*) FROM "{schema}".{migration.TABLE_NAME}'), 1)
                await asyncio.wait_for(
                    database.status(
                        f'UPDATE "{schema}".pd_dataset_candidate SET payload_hash=payload_hash WHERE false'
                    ),
                    1,
                )
                pids = await database.all(
                    "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                    "AND state='active' AND query LIKE :query AND pid<>pg_backend_pid()",
                    query=migration._create_index_sql(schema, "pd_dataset_candidate", candidate_name) + "%",
                )
                assert len(pids) == 1
                assert await database.scalar("SELECT pg_cancel_backend(:pid)", pid=pids[0].pid)
                with pytest.raises(DBAPIError) as interrupted:
                    await build
                assert interrupted.value.orig.sqlstate == "57014"
            finally:
                if not build.done():
                    build.cancel()
                    await asyncio.gather(build, return_exceptions=True)
                await transaction.rollback()
        await run_migration(database.engine, migration, "upgrade")
        complete = await _cursor_tree(database, schema)
        assert len(complete) == 3
        assert {index_record.oid for index_record in original}.issubset({index_record.oid for index_record in complete})
        assert invalid.oid not in {index_record.oid for index_record in complete}
        assert all(
            index_record.indisvalid and index_record.indisready and index_record.indislive for index_record in complete
        )


@pytest.mark.asyncio
async def test_partition_inventory_change_is_rejected(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        migration = _cursor_migration()
        native_inventory = migration._partition_inventory
        calls_by_phase = {"reads": 0}

        def changed_inventory(current_schema):
            calls_by_phase["reads"] += 1
            if calls_by_phase["reads"] == 2:
                migration.op.get_bind().exec_driver_sql(
                    f'CREATE TABLE "{schema}".new_leaf PARTITION OF "{schema}".{migration.TABLE_NAME} '
                    "FOR VALUES IN ('new-dataset')"
                )
            return native_inventory(current_schema)

        monkeypatch.setattr(migration, "_partition_inventory", changed_inventory)
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_partition_inventory_mismatch$"):
            await run_migration(database.engine, migration, "upgrade")
        monkeypatch.setattr(migration, "_partition_inventory", native_inventory)
        await run_migration(database.engine, migration, "upgrade")
        assert len(await _cursor_tree(database, schema)) == 4


@pytest.mark.asyncio
async def test_nested_partition_is_explicitly_unsupported(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True, populated_partitions=False) as (database, schema):
        migration = _cursor_migration()
        await database.status(
            f'CREATE TABLE "{schema}".nested PARTITION OF "{schema}".{migration.TABLE_NAME} '
            "FOR VALUES IN ('nested') PARTITION BY RANGE (resource_id)"
        )
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_partition_shape_unsupported$"):
            await run_migration(database.engine, migration, "upgrade")
        assert await _index_state(database, schema) is None


@pytest.mark.asyncio
async def test_partition_leaf_oid_change_is_rejected(monkeypatch):
    async with _content_proof_database(monkeypatch, partitioned=True) as (database, schema):
        migration = _cursor_migration()
        native_inventory = migration._partition_inventory
        calls_by_phase = {"reads": 0}

        def changed_inventory(current_schema):
            calls_by_phase["reads"] += 1
            if calls_by_phase["reads"] == 2:
                name = migration._leaf_index_name(schema, "pd_dataset_history")
                bind = migration.op.get_bind()
                bind.exec_driver_sql(migration._drop_index_sql(schema, name))
                bind.exec_driver_sql(migration._create_index_sql(schema, "pd_dataset_history", name))
            return native_inventory(current_schema)

        monkeypatch.setattr(migration, "_partition_inventory", changed_inventory)
        with pytest.raises(RuntimeError, match="^provider_directory_content_cursor_partition_index_mismatch$"):
            await run_migration(database.engine, migration, "upgrade")
        monkeypatch.setattr(migration, "_partition_inventory", native_inventory)
        await run_migration(database.engine, migration, "upgrade")
        assert len(await _cursor_tree(database, schema)) == 3


@pytest.mark.asyncio
async def test_historical_conversion_cursor_preserves_rows(monkeypatch):
    from tests.test_provider_dataset_candidates_postgres import _candidate_migration
    from tests.test_provider_directory_rooted_graph_publication_postgres import _lifecycle_scope

    database = Database()
    try:
        await database.connect()
        await _require_disposable_postgres(database)
        monkeypatch.setenv(
            "HLTHPRT_FHIR_FORMULARY_MIGRATION_POSTGRES_DSN",
            database.engine.url.render_as_string(hide_password=False),
        )
    finally:
        await database.disconnect()
    async with _lifecycle_scope(monkeypatch, dataset_candidates=False, request_failure_budget=False) as context:
        migration = _cursor_migration()
        table = f'{context.schema}."{migration.TABLE_NAME}"'
        old_oid = await context.connection.fetchval("SELECT $1::regclass::oid", table)
        original_rows = await context.connection.fetch(
            f"SELECT tableoid,* FROM {table} ORDER BY dataset_id,resource_type,resource_id"
        )
        assert original_rows
        await run_migration(context.engine, _candidate_migration(), "upgrade")
        assert await context.connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_partition_tree($1::regclass) WHERE relid=$2 AND isleaf)", table, old_oid
        )
        await run_migration(context.engine, migration, "upgrade")
        assert (
            await context.connection.fetch(
                f"SELECT tableoid,* FROM {table} ORDER BY dataset_id,resource_type,resource_id"
            )
            == original_rows
        )
        tree = await _cursor_tree(context.database, context.schema_name)
        assert len(tree) >= 2
        assert all(
            index_record.indisvalid and index_record.indisready and index_record.indislive for index_record in tree
        )
