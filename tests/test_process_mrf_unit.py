# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")


@pytest.mark.asyncio
async def test_mrf_main_enqueues_init_file(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))

    await process_initial.main()

    fake_pool.enqueue_job.assert_awaited_once_with("init_file", {"test_mode": False}, _queue_name="arq:MRF")


@pytest.mark.asyncio
async def test_mrf_main_enqueues_init_file_test_mode(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))

    await process_initial.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with("init_file", {"test_mode": True}, _queue_name="arq:MRF")


def test_mrf_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.MRF.functions]
    assert names == [
        "init_file",
        "save_mrf_data",
        "process_plan",
        "process_json_index",
        "process_provider",
        "process_formulary",
    ]
    assert process_pkg.MRF.on_startup.__name__ == "startup"


def test_mrf_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.MRF_finish.functions]
    assert names == ["shutdown"]
    assert process_pkg.MRF_finish.queue_name == "arq:MRF_finish"


def test_claims_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing.functions]
    assert names == ["claims_pricing_start", "claims_pricing_process_chunk"]
    assert process_pkg.ClaimsPricing.queue_name == "arq:ClaimsPricing"


def test_claims_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing_finish.functions]
    assert names == ["claims_pricing_finalize"]
    assert process_pkg.ClaimsPricing_finish.queue_name == "arq:ClaimsPricing_finish"


def test_job_serializer_handles_exceptions():
    encoded = process_pkg.MRF.job_serializer(RuntimeError("boom"))
    decoded = process_pkg.MRF.job_deserializer(encoded)
    assert decoded["__type__"] == "exception"
    assert decoded["name"] == "RuntimeError"
    assert decoded["message"] == "boom"


def test_plan_attributes_cli_accepts_test_flag(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_plan_attributes", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.plan_attributes.callback(test=True)

    fake_initiate.assert_called_once_with(test_mode=True)


def test_claims_pricing_cli_accepts_test_flag(monkeypatch):
    fake_initiate = AsyncMock()
    monkeypatch.setattr(process_pkg, "initiate_claims_pricing", fake_initiate)

    def fake_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(process_pkg.asyncio, "run", fake_run)

    process_pkg.claims_pricing.callback(test=True, import_id="dev1")

    fake_initiate.assert_called_once_with(test_mode=True, import_id="dev1")

@pytest.mark.asyncio
async def test_mrf_startup_sets_utc_time(monkeypatch):
    monkeypatch.setattr(process_initial, "my_init_db", AsyncMock())
    monkeypatch.setattr(process_initial.db, "status", AsyncMock())
    monkeypatch.setattr(process_initial.db, "create_table", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda cls, suffix: SimpleNamespace(
        __main_table__=cls.__tablename__,
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
        __my_index_elements__=["id"]
    ))

    ctx = {}
    await process_initial.startup(ctx)

    delta = datetime.datetime.utcnow() - ctx["context"]["start"]
    assert delta.total_seconds() < 2


@pytest.mark.asyncio
async def test_finish_main_enqueues_shutdown(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))

    await process_initial.finish_main()

    fake_pool.enqueue_job.assert_awaited_once_with("shutdown", _queue_name="arq:MRF_finish")


@pytest.mark.asyncio
async def test_refresh_do_business_as(monkeypatch):
    calls = []

    async def fake_status(sql):
        calls.append(sql)

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", fake_status)
    monkeypatch.setattr(process_npi.db, "scalar", AsyncMock(return_value="mrf.npi_other_identifier"))
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    await process_npi.refresh_do_business_as()

    sql_strings = [str(sql) for sql in calls]
    assert any("SET do_business_as = ARRAY[]::varchar[], do_business_as_text = ''" in s for s in sql_strings)
    assert any("do_business_as_text = COALESCE" in str(sql) for sql in calls)


@pytest.mark.asyncio
async def test_refresh_do_business_as_skips_when_source_missing(monkeypatch):
    status_mock = AsyncMock()
    scalar_mock = AsyncMock(return_value=None)

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", status_mock)
    monkeypatch.setattr(process_npi.db, "scalar", scalar_mock)
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    await process_npi.refresh_do_business_as(
        target_table="npi_20260214",
        source_table="npi_other_identifier_20260214",
    )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_push_objects_rewrite_respects_parameter_limit(monkeypatch):
    status_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            return self

        async def status(self):
            status_calls.append(len(self.chunk))

    def _fake_insert(_table):
        return _FakeStmt()

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="fake_upsert_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    records = []
    for idx in range(8):
        row = {"id": idx, "value": f"v{idx}"}
        for extra in range(38):
            row[f"c{extra}"] = extra
        records.append(row)

    monkeypatch.setattr(utils_module.db, "insert", _fake_insert)
    monkeypatch.setenv("HLTHPRT_MAX_INSERT_PARAMETERS", "120")
    monkeypatch.setenv("HLTHPRT_DRIVER_PARAM_LIMIT", "32767")

    await utils_module.push_objects(records, fake_cls, rewrite=True, use_copy=False)

    assert status_calls == [3, 3, 2]


@pytest.mark.asyncio
async def test_push_objects_rewrite_prefers_copy_first(monkeypatch):
    copy_calls = []

    class _FakeDriver:
        async def copy_records_to_table(self, table_name, schema_name=None, columns=None, records=None):
            rows = []
            async for row in records:
                rows.append(row)
            copy_calls.append(
                {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "columns": list(columns or []),
                    "row_count": len(rows),
                }
            )

    class _FakeRaw:
        def __init__(self):
            self.driver_connection = _FakeDriver()

    class _FakeConn:
        def __init__(self):
            self.raw_connection = _FakeRaw()

    class _AcquireCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _fail_insert(*_args, **_kwargs):
        raise AssertionError("insert fallback should not run when COPY succeeds")

    fake_table = SimpleNamespace(schema="mrf", c=[])
    fake_cls = SimpleNamespace(
        __tablename__="copy_first_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )
    rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]

    monkeypatch.setattr(utils_module.db, "acquire", lambda: _AcquireCtx())
    monkeypatch.setattr(utils_module.db, "insert", _fail_insert)

    await utils_module.push_objects(rows, fake_cls, rewrite=True)

    assert len(copy_calls) == 1
    assert copy_calls[0]["table_name"] == "copy_first_table"
    assert copy_calls[0]["schema_name"] == "mrf"
    assert copy_calls[0]["columns"] == ["id", "value"]
    assert copy_calls[0]["row_count"] == 2


def test_extract_plan_years_from_years_array():
    payload = {"years": [2024, "2025", "2025.0", 2026.0, "bad", None]}
    assert process_initial._extract_plan_years(payload) == [2024, 2025, 2026]


def test_extract_plan_years_from_year_scalar():
    payload = {"year": "2026"}
    assert process_initial._extract_plan_years(payload) == [2026]


def test_extract_plan_years_invalid_or_missing():
    assert process_initial._extract_plan_years({"years": "bad"}) == []
    assert process_initial._extract_plan_years({}) == []
