# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL acquisition admission and dataset-lock regressions."""

import asyncio
import json
import uuid
from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.ext.asyncio import create_async_engine

from db.connection import Database
from process import mrf_discovery_acquisition_guard as guard
from tests.test_mrf_discovery_acquisition_guard import replacement, reserved_run
from tests.test_mrf_discovery_checkpoints_postgres import _connect, _database_url


async def _create_guard_relations(connection, control, relation_by_model):
    await connection.execute(
        f"CREATE TABLE {control} (run_id text PRIMARY KEY, importer text, engine text, status text, params jsonb, metrics jsonb); "
        f"CREATE TABLE {relation_by_model[guard.ImportRun]} (run_id text PRIMARY KEY, importer text, engine text, status text, params jsonb, idempotency_key text); "
        f"CREATE TABLE {relation_by_model[guard.MRFSource]} (id integer PRIMARY KEY, payload text); "
        f"CREATE TABLE {relation_by_model[guard.MRFPayer]} (id integer PRIMARY KEY); "
        f"CREATE TABLE {relation_by_model[guard.MRFFile]} (id integer PRIMARY KEY)"
    )
    await connection.execute(f"INSERT INTO {relation_by_model[guard.MRFSource]} VALUES (1,'original')")


def _native_guard_database(database_url):
    """Use the same one-slot engine and native connection settings."""
    return Database(
        engine=create_async_engine(
            database_url.set(drivername="postgresql+asyncpg"),
            pool_size=1,
            max_overflow=0,
            pool_timeout=0.5,
            connect_args={"server_settings": {"application_name": "synthetic-mrf-acquisition-single-slot"}},
        )
    )


async def _seed_guard_run(first, control, relation_by_model):
    """Keep the admitted reservation identical in control and local relations."""
    dataset = await guard.native_dataset_identity(first)
    run = reserved_run()
    run["metrics"][guard.MARKER]["dataset"] = dataset
    await first.execute(
        f"INSERT INTO {control} VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb)",
        run["run_id"],
        guard.IMPORTER,
        guard.ENGINE,
        run["status"],
        json.dumps(run["params"]),
        json.dumps(run["metrics"]),
    )
    await first.execute(
        f"INSERT INTO {relation_by_model[guard.ImportRun]} VALUES ($1,$2,$3,'running',$4::jsonb,$5)",
        run["run_id"],
        guard.IMPORTER,
        guard.ENGINE,
        json.dumps(run["params"]),
        run["idempotency_key"],
    )
    return dataset, run


@pytest.fixture
async def native_guard(monkeypatch):
    """Create and always remove the exact native control/export relation set."""
    database_url = _database_url()
    first = await _connect(database_url)
    second = None
    database = None
    has_schema = False
    schema = "mrf_acquisition_test_" + uuid.uuid4().hex
    quoted_schema = '"' + schema + '"'
    relation_by_model = {
        guard.ImportRun: f'{quoted_schema}."local_run"',
        guard.MRFSource: f'{quoted_schema}."export_source"',
        guard.MRFPayer: f'{quoted_schema}."export_payer"',
        guard.MRFFile: f'{quoted_schema}."export_file"',
    }
    control = f'{quoted_schema}."control_run"'
    try:
        second = await _connect(database_url)
        await first.execute(f"CREATE SCHEMA {quoted_schema}")
        has_schema = True
        await _create_guard_relations(first, control, relation_by_model)
        monkeypatch.setenv(guard.CONTROL_RELATION_ENV, f"{schema}.control_run")
        monkeypatch.setattr(guard, "_relation", relation_by_model.__getitem__)

        database = _native_guard_database(database_url)
        monkeypatch.setattr(guard, "db", database)
        dataset, run = await _seed_guard_run(first, control, relation_by_model)
        yield SimpleNamespace(
            first=first,
            second=second,
            control=control,
            models=relation_by_model,
            dataset=dataset,
            run=run,
            database=database,
            schema=schema,
        )
    finally:
        try:
            if database is not None:
                await database.disconnect()
        finally:
            try:
                if has_schema:
                    await first.execute(f"DROP SCHEMA {quoted_schema} CASCADE")
            finally:
                await first.close()
                if second is not None:
                    await second.close()


async def command(crawl=False, dry_run=False, run_id=None):
    return None


def command_params(case):
    return {"crawl": True, "dry_run": False, "run_id": case.run["run_id"]}


@pytest.mark.parametrize("catalog_status", ["running", "failed", ""])
async def test_native_unresolved_consumer_survives_history_noise_without_mutation(native_guard, catalog_status):
    case = native_guard
    await case.first.executemany(
        f"INSERT INTO {case.control} (run_id,importer,status,metrics) VALUES ($1,'synthetic-other','succeeded','{{}}')",
        [(f"noise-{index}",) for index in range(501)],
    )
    await case.first.execute(
        f"INSERT INTO {case.control} (run_id,importer,status,metrics) VALUES ('old-consumer',$1,'succeeded',$2::jsonb)",
        guard.IMPORTER,
        json.dumps({"catalog_export_version": 2, "catalog_sync_status": catalog_status}),
    )
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(command, command_params(case)):
            await case.first.execute(f"UPDATE {case.models[guard.MRFSource]} SET payload='overwritten'")
    assert await case.first.fetchval(f"SELECT payload FROM {case.models[guard.MRFSource]} WHERE id=1") == "original"


@pytest.mark.parametrize("cancel", [False, True])
async def test_native_lock_spans_mutable_body_and_releases_on_failure_or_cancel(native_guard, cancel):
    case = native_guard
    entered, finish = asyncio.Event(), asyncio.Event()

    async def acquire():
        async with guard.acquisition_command(command, command_params(case)):
            entered.set()
            await finish.wait()
            raise RuntimeError("synthetic failed acquisition")

    task = asyncio.create_task(acquire())
    lock_key = "mrf-discovery:dataset:" + ":".join(str(case.dataset[key]) for key in guard.DATASET_KEYS)
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert await case.second.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key) is False
        if cancel:
            task.cancel()
        else:
            finish.set()
        with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
            await task
        assert await case.second.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key) is True
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await case.second.fetchval("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key)


async def test_native_reciprocal_replacement_leaves_old_failed_and_other_importers_unblocked(native_guard):
    case = native_guard
    await case.first.execute(
        f"INSERT INTO {case.control} (run_id,importer,status,metrics) VALUES ('other-active','synthetic-other','running','{{}}')"
    )
    fake = SimpleNamespace(source=deepcopy(case.run))
    replacement(fake)
    fake.old["metrics"][guard.REPLACEMENT]["dataset"] = case.dataset
    fake.old["metrics"]["catalog_export_version"] = 2
    await case.first.execute(
        f"UPDATE {case.control} SET metrics=$2::jsonb WHERE run_id=$1",
        case.run["run_id"],
        json.dumps(fake.source["metrics"]),
    )
    await case.first.execute(
        f"INSERT INTO {case.control} VALUES ($1,$2,$3,$4,'{{}}',$5::jsonb)",
        fake.old["run_id"],
        guard.IMPORTER,
        guard.ENGINE,
        "succeeded",
        json.dumps(fake.old["metrics"]),
    )
    async with guard.acquisition_command(command, command_params(case)):
        assert await case.first.fetchval(f"SELECT payload FROM {case.models[guard.MRFSource]} WHERE id=1") == "original"
    assert (
        await case.first.fetchval(
            f"SELECT metrics->>'catalog_sync_status' FROM {case.control} WHERE run_id=$1", fake.old["run_id"]
        )
        == "failed"
    )
    await case.first.execute(
        f"UPDATE {case.control} SET metrics=jsonb_set(metrics,'{{mrf_acquisition_replacement,command_id}}','\"different\"') WHERE run_id=$1",
        fake.old["run_id"],
    )
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(command, command_params(case)):
            pytest.fail("mismatched replacement entered mutable work")


async def test_native_execution_rejects_replaced_exporter_relation(native_guard):
    case = native_guard
    source = case.models[guard.MRFSource]
    await case.first.execute(f"ALTER TABLE {source} RENAME TO export_source_original")
    await case.first.execute(f"CREATE TABLE {source} (id integer PRIMARY KEY, payload text)")
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(command, command_params(case)):
            pytest.fail("replacement relation reused a saved reservation")


async def test_native_execution_rejects_replaced_control_relation(native_guard):
    case = native_guard
    original = case.control.rsplit(".", 1)[0] + '."control_run_original"'
    await case.first.execute(f"ALTER TABLE {case.control} RENAME TO control_run_original")
    await case.first.execute(f"CREATE TABLE {case.control} (LIKE {original} INCLUDING ALL)")
    await case.first.execute(f"INSERT INTO {case.control} SELECT * FROM {original}")
    with pytest.raises(ValueError, match="mrf_acquisition_not_admitted"):
        async with guard.acquisition_command(command, command_params(case)):
            pytest.fail("replacement control relation reused a saved reservation")
    assert await case.first.fetchval(f"SELECT payload FROM {case.models[guard.MRFSource]} WHERE id=1") == "original"


async def test_native_verified_foreign_dataset_reservation_does_not_block(native_guard):
    case = native_guard
    foreign = deepcopy(case.run["metrics"][guard.MARKER])
    foreign["run_id"] = "synthetic-foreign"
    schema = case.control.rsplit(".", 1)[0]
    for kind in ("source", "payer", "file"):
        relation = f'{schema}."foreign_{kind}"'
        await case.first.execute(f"CREATE TABLE {relation} (id integer PRIMARY KEY)")
        foreign["dataset"][kind + "_oid"] = await case.first.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    await case.first.execute(
        f"INSERT INTO {case.control} (run_id,importer,status,metrics) VALUES ($1,$2,'running',$3::jsonb)",
        foreign["run_id"],
        guard.IMPORTER,
        json.dumps({guard.MARKER: foreign}),
    )
    async with guard.acquisition_command(command, command_params(case)):
        assert await case.first.fetchval(f"SELECT payload FROM {case.models[guard.MRFSource]} WHERE id=1") == "original"


async def test_native_guard_keeps_a_one_connection_data_pool_available(native_guard, monkeypatch):
    from process import mrf_source_discovery as discovery

    case = native_guard
    table = Table("single_pool_catalog", MetaData(), Column("id", Integer, primary_key=True), schema=case.schema)
    monkeypatch.setattr(discovery, "db", case.database)
    monkeypatch.setattr(discovery, "DISCOVERY_TABLES", (SimpleNamespace(__table__=table),))
    async with asyncio.timeout(5):
        async with guard.acquisition_command(command, command_params(case)):
            assert case.database.engine.pool.checkedout() == 0
            assert (
                await case.first.fetchval(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname=current_database() "
                    "AND application_name=$1",
                    "synthetic-mrf-acquisition-single-slot",
                )
                == 1
            )
            lock_key = "mrf-discovery:dataset:" + ":".join(str(case.dataset[key]) for key in guard.DATASET_KEYS)
            assert await case.second.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key) is False
            await discovery._ensure_catalog_tables()
            assert case.database.engine.pool.checkedout() == 0
    assert await case.first.fetchval("SELECT to_regclass($1) IS NOT NULL", f"{case.schema}.single_pool_catalog")
