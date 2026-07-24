# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

lodes = importlib.import_module("process.lodes")


class _TransactionSpy:
    def __init__(self):
        self.entered = 0
        self.exited = 0
        self.exception_type = None

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exception_type, _exception, _traceback):
        self.exited += 1
        self.exception_type = exception_type
        return False


@pytest.mark.asyncio
async def test_worker_task_tracks_processed_and_skipped_states(
    monkeypatch,
):
    fake_client = SimpleNamespace(close=AsyncMock())
    ensure_database = AsyncMock()
    create_table = AsyncMock()
    process_state = AsyncMock(return_value=3)
    state_years = iter((2021, None))
    stage_class = SimpleNamespace(
        __tablename__="lodes_stage",
        __table__=SimpleNamespace(name="lodes_stage"),
        __my_index_elements__=["zcta_code"],
    )
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: fake_client),
    )
    monkeypatch.setattr(lodes, "TEST_STATES", ["il", "ca"])
    monkeypatch.setattr(lodes, "ensure_database", ensure_database)
    monkeypatch.setattr(lodes, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        lodes,
        "_load_tract_to_zip_crosswalk",
        AsyncMock(return_value={"17031010100": "60654"}),
    )
    monkeypatch.setattr(
        lodes,
        "_resolve_state_year",
        AsyncMock(side_effect=lambda *_args: next(state_years)),
    )
    monkeypatch.setattr(lodes, "_process_lodes_state", process_state)
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage_class)
    monkeypatch.setattr(lodes.db, "create_table", create_table)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tenant")

    worker_context_by_key = {"import_date": "run01", "context": {}}
    await lodes.process_data(
        worker_context_by_key,
        {"test_mode": True},
    )

    ensure_database.assert_awaited_once_with(True)
    assert worker_context_by_key["context"]["run"] == 1
    assert worker_context_by_key["context"]["processed_states"] == {"il": 2021}
    assert worker_context_by_key["context"]["skipped_states"] == ["ca"]
    process_state.assert_awaited_once()
    fake_client.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_worker_task_closes_client_when_required_crosswalk_is_missing(
    monkeypatch,
):
    fake_client = SimpleNamespace(close=AsyncMock())
    monkeypatch.setitem(
        sys.modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: fake_client),
    )
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "_ensure_schema_exists", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(
        lodes,
        "_load_tract_to_zip_crosswalk",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        lodes,
        "make_class",
        lambda *_args: SimpleNamespace(__tablename__="lodes_stage"),
    )
    monkeypatch.setenv("HLTHPRT_LODES_REQUIRE_CROSSWALK", "true")

    with pytest.raises(RuntimeError, match="crosswalk is required"):
        await lodes.process_data(
            {"import_date": "run01", "context": {}},
            {"test_mode": True},
        )

    fake_client.close.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_startup_creates_fresh_indexed_stage(monkeypatch):
    initialize_database = AsyncMock()
    ensure_database = AsyncMock()
    ensure_schema = AsyncMock()
    execute_status = AsyncMock()
    create_table = AsyncMock()
    stage_class = SimpleNamespace(
        __tablename__="lodes_stage_run01",
        __table__=SimpleNamespace(name="lodes_stage_run01"),
        __my_index_elements__=["zcta_code", "year"],
    )
    monkeypatch.setattr(lodes, "my_init_db", initialize_database)
    monkeypatch.setattr(lodes, "ensure_database", ensure_database)
    monkeypatch.setattr(lodes, "_ensure_schema_exists", ensure_schema)
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage_class)
    monkeypatch.setattr(lodes.db, "status", execute_status)
    monkeypatch.setattr(lodes.db, "create_table", create_table)
    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "run-01")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tenant")

    worker_context_by_key = {}
    await lodes.startup(worker_context_by_key)

    assert worker_context_by_key["import_date"] == "run01"
    assert worker_context_by_key["context"]["run"] == 0
    ensure_database.assert_awaited_once_with(False)
    assert execute_status.await_args_list[0].args[0] == (
        "DROP TABLE IF EXISTS tenant.lodes_stage_run01;"
    )
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in (
        execute_status.await_args_list[1].args[0]
    )


@pytest.mark.asyncio
async def test_test_mode_shutdown_reports_missing_stage_without_publish(
    monkeypatch,
):
    mark_run = AsyncMock()
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(lodes, "mark_control_run", mark_run)
    monkeypatch.setattr(
        lodes,
        "make_class",
        lambda *_args: SimpleNamespace(__tablename__="lodes_stage"),
    )

    await lodes.shutdown(
        {
            "import_date": "run01",
            "context": {
                "run": 1,
                "test_mode": True,
                "control_run_id": "control01",
            },
        }
    )

    mark_run.assert_awaited_once_with(
        "control01",
        status="succeeded",
        phase_detail="lodes test mode no rows",
        progress_message="succeeded",
        metrics={
            "stage_rows": 0,
            "distinct_zctas": 0,
            "geo_match_ratio": 0.0,
        },
    )


@pytest.mark.asyncio
async def test_shutdown_atomically_publishes_valid_stage(monkeypatch):
    transaction_spy = _TransactionSpy()
    execute_status = AsyncMock()
    mark_run = AsyncMock()
    scalar_values = iter((6000, 5500, 5000))
    stage_class = SimpleNamespace(__tablename__="lodes_stage")
    monkeypatch.setattr(lodes, "ensure_database", AsyncMock())
    monkeypatch.setattr(lodes, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(lodes, "make_class", lambda *_args: stage_class)
    monkeypatch.setattr(
        lodes.db,
        "scalar",
        AsyncMock(side_effect=lambda *_args, **_kwargs: next(scalar_values)),
    )
    monkeypatch.setattr(lodes.db, "status", execute_status)
    monkeypatch.setattr(lodes.db, "transaction", lambda: transaction_spy)
    monkeypatch.setattr(lodes, "mark_control_run", mark_run)
    monkeypatch.setattr(lodes, "print_time_info", lambda _start: None)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tenant")

    await lodes.shutdown(
        {
            "import_date": "run01",
            "context": {
                "run": 1,
                "start": "start",
                "control_run_id": "control01",
            },
        }
    )

    assert transaction_spy.entered == 1
    assert transaction_spy.exited == 1
    assert execute_status.await_count == 6
    mark_run.assert_awaited_once()
    published_metrics = mark_run.await_args.kwargs["metrics"]
    assert published_metrics == {
        "stage_rows": 6000,
        "distinct_zctas": 5500,
        "matched_zctas": 5000,
        "geo_match_ratio": 5000 / 5500,
    }


@pytest.mark.asyncio
async def test_entry_point_enqueues_explicit_test_mode(monkeypatch):
    redis_pool = SimpleNamespace(enqueue_job=AsyncMock())
    create_pool = AsyncMock(return_value=redis_pool)
    monkeypatch.setattr(lodes, "create_pool", create_pool)
    monkeypatch.setattr(
        lodes,
        "build_redis_settings",
        lambda: "redis-settings",
    )

    await lodes.main(test_mode=True)

    create_pool.assert_awaited_once_with(
        "redis-settings",
        job_serializer=lodes.serialize_job,
        job_deserializer=lodes.deserialize_job,
    )
    redis_pool.enqueue_job.assert_awaited_once_with(
        "process_data",
        {"test_mode": True},
        _queue_name=lodes.LODES_QUEUE_NAME,
    )
