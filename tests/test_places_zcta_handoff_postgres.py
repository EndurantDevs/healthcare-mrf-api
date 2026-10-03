# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from db.models import ImportRun
from process import control_lifecycle
from process import places_zcta_handoff as handoff
from tests.test_reference_family_archive_postgres import _database_url


@pytest.fixture
async def places_database():
    engine = create_async_engine(_database_url())
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    schema = "places_handoff_" + uuid4().hex
    has_created_schema = False
    try:
        await database.status(f'CREATE SCHEMA "{schema}"')
        has_created_schema = True
        await database.status(
            f'CREATE TABLE "{schema}".import_run (run_id text PRIMARY KEY, importer text, status text, '
            "phase_detail text, params jsonb, metrics jsonb, progress jsonb, error jsonb, "
            "finished_at timestamp, heartbeat_at timestamp)"
        )
        await database.status(f'CREATE TABLE "{schema}".pricing_places_zcta (zcta text PRIMARY KEY)')
        await database.status(f'CREATE TABLE "{schema}".reference_family_result_generation (relation_oids bigint[])')
        await database.status(f"INSERT INTO \"{schema}\".pricing_places_zcta VALUES ('00001')")
        yield database, schema
    finally:
        if has_created_schema:
            await database.status(f'DROP SCHEMA "{schema}" CASCADE')
        await engine.dispose()


async def _staged_attempt(database, schema):
    run_id = "synthetic-" + uuid4().hex
    attempt_id = run_id + ":" + uuid4().hex
    table = "pricing_places_zcta_" + hashlib.sha256(attempt_id.encode()).hexdigest()[:20]
    job_context_by_field = {
        "control_run_id": run_id,
        "context": {
            "_control_attempt_id": attempt_id,
            "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
            "_places_incumbent_oid": await database.scalar(
                "SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.pricing_places_zcta"
            ),
            "audit": {
                "source_url": "https://example.org/places.csv",
                "latest_year": 2025,
                "processed_rows": 1,
                "accepted_rows": 1,
            },
        },
    }
    progress_by_field = {
        "attempt_id": attempt_id,
        "attempt_started_at": job_context_by_field["context"]["_control_attempt_started_at"],
    }
    await database.status(
        f'INSERT INTO "{schema}".import_run (run_id,importer,status,params,metrics,progress) '
        "VALUES (:run_id,'places-zcta','running','{}','{}',CAST(:progress AS jsonb))",
        run_id=run_id,
        progress=json.dumps(progress_by_field),
    )
    await database.status(update(ImportRun).values(error=None).execution_options(schema_translate_map={"mrf": schema}))
    await database.status(f'CREATE TABLE "{schema}"."{table}" (LIKE "{schema}".pricing_places_zcta INCLUDING ALL)')
    await database.status(f'INSERT INTO "{schema}"."{table}" VALUES (\'00002\')')
    job_context_by_field["context"]["_places_stage_oid"] = await database.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{table}"
    )
    job_context_by_field["context"]["_places_stage_schema"] = schema
    job_context_by_field["context"]["_places_stage_table"] = table
    return job_context_by_field, table


@pytest.mark.asyncio
async def test_handoff_commits_identity_and_fences_all_ordinary_updates(places_database):
    database, schema = places_database
    ctx, table = await _staged_attempt(database, schema)
    outcome = await handoff.handoff_places_stage(database, ctx, schema=schema, table_name=table, row_count=1)
    receipt = outcome["places_handoff"]
    native_run = await database.first(f'SELECT status,phase_detail,finished_at,metrics FROM "{schema}".import_run')
    assert tuple(native_run[:3]) == ("finalizing", handoff.HANDOFF_PHASE, None)
    assert native_run[3]["places_handoff"] == receipt
    assert receipt["indexes"] and receipt["stage_oid"] != receipt["incumbent_oid"]
    marker = await database.scalar("SELECT obj_description(:oid,'pg_class')", oid=receipt["stage_oid"])
    assert json.loads(marker)["handoff_sha256"] == receipt["handoff_sha256"]
    assert await database.scalar(f'SELECT zcta FROM "{schema}".pricing_places_zcta') == "00001"
    for status in ("running", "succeeded", "failed", "canceled"):
        statement = control_lifecycle._where_no_places_handoff(update(ImportRun)).values(status=status)
        assert await database.status(statement.execution_options(schema_translate_map={"mrf": schema})) == 0
    with pytest.raises(RuntimeError, match="attempt changed"):
        await handoff.handoff_places_stage(database, ctx, schema=schema, table_name=table, row_count=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["attempt", "canceling", "count", "oid"])
async def test_handoff_rejects_stale_cancelled_or_changed_stage(places_database, changed):
    database, schema = places_database
    ctx, table = await _staged_attempt(database, schema)
    if changed == "attempt":
        await database.status(f"UPDATE \"{schema}\".import_run SET progress='{{}}'")
    elif changed == "canceling":
        await database.status(f"UPDATE \"{schema}\".import_run SET status='canceling'")
    elif changed == "oid":
        ctx["context"]["_places_stage_oid"] += 1
    with pytest.raises(RuntimeError, match="changed"):
        await handoff.handoff_places_stage(
            database, ctx, schema=schema, table_name=table, row_count=2 if changed == "count" else 1
        )
    assert await database.scalar(f"SELECT metrics->'places_handoff' FROM \"{schema}\".import_run") is None
    assert await database.scalar(f'SELECT zcta FROM "{schema}".pricing_places_zcta') == "00001"


@pytest.mark.asyncio
@pytest.mark.parametrize("stage_state", ["unpublished", "handed_off", "rebound", "marked", "current_bound"])
async def test_cleanup_removes_only_original_unpublished_stage(places_database, stage_state):
    database, schema = places_database
    ctx, table = await _staged_attempt(database, schema)
    if stage_state == "handed_off":
        await handoff.handoff_places_stage(database, ctx, schema=schema, table_name=table, row_count=1)
        ctx["context"].pop("control_run_handoff_committed")
    elif stage_state == "rebound":
        await database.status(f'DROP TABLE "{schema}"."{table}"')
        await database.status(f'CREATE TABLE "{schema}"."{table}" (unrelated integer)')
    elif stage_state == "marked":
        await database.status(f'COMMENT ON TABLE "{schema}"."{table}" IS \'publication pending\'')
    elif stage_state == "current_bound":
        await database.status(
            f'INSERT INTO "{schema}".reference_family_result_generation VALUES (ARRAY[:stage_oid]::bigint[])',
            stage_oid=ctx["context"]["_places_stage_oid"],
        )
    has_cleaned = await handoff.cleanup_places_attempt(database, ctx)
    assert has_cleaned is (stage_state == "unpublished")
    assert bool(await database.scalar("SELECT to_regclass(:relation)", relation=f"{schema}.{table}")) is (
        not has_cleaned
    )
    assert await database.scalar(f'SELECT zcta FROM "{schema}".pricing_places_zcta') == "00001"


@pytest.mark.asyncio
async def test_handoff_rolls_back_stage_marker_with_failed_cas(places_database, monkeypatch):
    database, schema = places_database
    ctx, table = await _staged_attempt(database, schema)
    original_write = handoff._write_handoff

    async def fail_after_write(*args):
        await original_write(*args)
        raise RuntimeError("transaction rolled back")

    monkeypatch.setattr(handoff, "_write_handoff", fail_after_write)
    with pytest.raises(RuntimeError, match="rolled back"):
        await handoff.handoff_places_stage(database, ctx, schema=schema, table_name=table, row_count=1)
    assert await database.scalar(f'SELECT status FROM "{schema}".import_run') == "running"
    assert (
        await database.scalar("SELECT obj_description(to_regclass(:relation),'pg_class')", relation=f"{schema}.{table}")
        is None
    )


@pytest.mark.asyncio
async def test_stage_indexes_have_distinct_valid_catalog_definitions(places_database, monkeypatch):
    places = importlib.import_module("process.places_zcta")
    database, schema = places_database
    stage = "pricing_places_zcta_" + uuid4().hex[:20]
    await database.status(f'CREATE TABLE "{schema}"."{stage}" (zcta text, year integer, measure_id text)')
    monkeypatch.setattr(places, "db", database)
    await places._create_places_stage_indexes(type("Stage", (), {"__tablename__": stage}), schema)
    indexes = await database.all(
        "SELECT ci.relname, pg_get_indexdef(i.indexrelid), i.indisvalid, i.indisready "
        "FROM pg_index i JOIN pg_class ci ON ci.oid=i.indexrelid "
        "WHERE i.indrelid=to_regclass(:relation) ORDER BY ci.relname",
        relation=f"{schema}.{stage}",
    )
    assert [row[0] for row in indexes] == [f"{stage}_idx_{i}" for i in range(3)]
    assert all(len(row[0].encode()) <= 63 and row[2] and row[3] for row in indexes)
    assert [row[1].split(" USING btree ", 1)[1] for row in indexes] == [
        "(year, zcta)",
        "(year, measure_id)",
        "(zcta, measure_id, year)",
    ]


@pytest.mark.asyncio
async def test_normal_cutover_renames_compact_stage_indexes_to_canonical_names(places_database, monkeypatch):
    places = importlib.import_module("process.places_zcta")
    database, schema = places_database
    stage = "pricing_places_zcta_" + uuid4().hex[:20]
    await database.status(f'CREATE TABLE "{schema}"."{stage}" (zcta text, year integer, measure_id text)')
    await database.status(f"INSERT INTO \"{schema}\".\"{stage}\" VALUES ('00002',2025,'M1')")
    await database.status(f'CREATE UNIQUE INDEX "{stage}_idx_primary" ON "{schema}"."{stage}" (zcta,year,measure_id)')
    monkeypatch.setattr(places, "db", database)
    monkeypatch.setattr(places, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        places, "make_class", lambda *_args: SimpleNamespace(__tablename__=stage, __main_table__="pricing_places_zcta")
    )
    monkeypatch.setattr(places, "publish_local_reference_family_generation", AsyncMock())
    monkeypatch.setattr(places, "print_time_info", lambda _start: None)
    monkeypatch.setenv("HLTHPRT_PLACES_ZCTA_PROTECTED_PUBLICATION", "false")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    await places.publish_places_zcta_generation({"import_date": "synthetic", "context": {"run": 1, "test_mode": True}})
    assert await database.scalar(f'SELECT zcta FROM "{schema}".pricing_places_zcta') == "00002"
    indexes = await database.all(
        "SELECT ci.relname, pg_get_indexdef(i.indexrelid), i.indisvalid, i.indisready "
        "FROM pg_index i JOIN pg_class ci ON ci.oid=i.indexrelid "
        "WHERE i.indrelid=to_regclass(:relation) AND ci.relname LIKE 'pricing_places_zcta_idx_%' "
        "ORDER BY ci.relname",
        relation=f"{schema}.pricing_places_zcta",
    )
    assert {index_row[0] for index_row in indexes} == {
        "pricing_places_zcta_idx_primary",
        *("pricing_places_zcta_idx_" + index["name"] for index in places.PricingPlacesZcta.__my_additional_indexes__),
    }
    assert all(index_row[2] and index_row[3] for index_row in indexes)
    assert {index_row[1].split(" USING btree ", 1)[1] for index_row in indexes} == {
        "(zcta, year, measure_id)",
        "(year, zcta)",
        "(year, measure_id)",
        "(zcta, measure_id, year)",
    }
