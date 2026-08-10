# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

from asyncpg.exceptions import DeadlockDetectedError
import pytest
from sqlalchemy import BigInteger

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")

def test_transparency_zip_path_is_unique_per_source(tmp_path):
    first = process_initial._transparency_zip_path(str(tmp_path), 0, {"year": "2026"})
    second = process_initial._transparency_zip_path(str(tmp_path), 1, {"year": "2025"})

    assert first.endswith("transparency_0_2026.zip")
    assert second.endswith("transparency_1_2025.zip")
    assert first != second


def test_claims_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing.functions]
    assert names == ["claims_pricing_start", "claims_pricing_process_chunk"]
    assert process_pkg.ClaimsPricing.queue_name == "arq:ClaimsPricing"


def test_claims_finish_worker_configuration():
    names = [fn.__name__ for fn in process_pkg.ClaimsPricing_finish.functions]
    assert names == ["claims_pricing_finalize"]
    assert process_pkg.ClaimsPricing_finish.queue_name == "arq:ClaimsPricing_finish"
    assert process_pkg.ClaimsPricing_finish.max_tries == 720
    assert process_pkg.DrugClaims_finish.max_tries == 720


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

    monkeypatch.setattr(process_pkg, "_run", fake_run)

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

    monkeypatch.setattr(process_pkg, "_run", fake_run)

    process_pkg.claims_pricing.callback(test=True, import_id="dev1")

    fake_initiate.assert_called_once_with(test_mode=True, import_id="dev1")

@pytest.mark.asyncio
async def test_mrf_startup_sets_utc_time(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", "run_worker_context")
    monkeypatch.setattr(process_initial, "my_init_db", AsyncMock())
    monkeypatch.setattr(process_initial.db, "status", AsyncMock())
    monkeypatch.setattr(process_initial.db, "create_table", AsyncMock())
    monkeypatch.setattr(process_initial, "make_class", lambda cls, suffix: SimpleNamespace(
        __main_table__=cls.__tablename__,
        __tablename__=f"{cls.__tablename__}_{suffix}",
        __table__=SimpleNamespace(name=f"{cls.__tablename__}_{suffix}", schema="mrf"),
        __my_index_elements__=["id"]
    ))

    context_by_field = {}
    await process_initial.startup(context_by_field)

    delta = datetime.datetime.utcnow() - context_by_field["context"]["start"]
    assert delta.total_seconds() < 2
    assert context_by_field["context"]["control_run_id"] == "run_worker_context"


@pytest.mark.asyncio
async def test_finish_main_enqueues_shutdown(monkeypatch):
    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())
    monkeypatch.setattr(process_initial, "create_pool", AsyncMock(return_value=fake_pool))
    monkeypatch.setattr(process_initial.os, "environ", {})

    await process_initial.finish_main(test_mode=True, import_id="20260402")

    fake_pool.enqueue_job.assert_awaited_once_with(
        "shutdown",
        {"context": {"import_date": "20260402", "test_mode": True}, "test_mode": True},
        _queue_name="arq:MRF_finish",
        _job_id="shutdown_mrf_20260402",
    )


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_sets_local_work_mem_and_analyzes(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    def fake_make_class(cls, suffix, schema_override=None):
        return SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}")

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "2GB")
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert statements[0] == "SET LOCAL work_mem = '2GB';"
    assert statements[1] == "ANALYZE mrf.mrf_address_evidence_20260612;"
    assert "INSERT INTO mrf.mrf_address_20260612" in statements[2]
    assert "FROM mrf.mrf_address_evidence_20260612" in statements[2]
    assert "ON CONFLICT (npi, type, checksum) DO UPDATE" in statements[2]
    assert len(statements) == 3


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_defers_source_array_indexes(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    address_cls = SimpleNamespace(
        __tablename__="mrf_address_20260612",
        __my_additional_indexes__=[
            {"index_elements": ("type", "npi"), "name": "type_npi"},
            {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
            {"index_elements": ("source_issuer_ids",), "using": "gin", "name": "source_issuer_ids"},
            {"index_elements": ("source_issuer_names",), "using": "gin", "name": "source_issuer_names"},
        ],
    )
    evidence_cls = SimpleNamespace(__tablename__="mrf_address_evidence_20260612")

    def fake_make_class(cls, suffix, schema_override=None):
        if cls is process_initial.MRFAddress:
            return address_cls
        return evidence_cls

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", "1")
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_DEFER_SOURCE_INDEXES", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    upsert_index = next(i for i, statement in enumerate(statements) if "INSERT INTO mrf.mrf_address_20260612" in statement)
    assert statements[1:4] == [
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_address_sources;",
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_source_issuer_ids;",
        "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_source_issuer_names;",
    ]
    assert upsert_index == 5
    assert statements[6:9] == [
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_address_sources ON mrf.mrf_address_20260612 USING gin (address_sources);",
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_source_issuer_ids ON mrf.mrf_address_20260612 USING gin (source_issuer_ids);",
        "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_source_issuer_names ON mrf.mrf_address_20260612 USING gin (source_issuer_names);",
    ]
    assert statements[9] == "ANALYZE mrf.mrf_address_20260612;"
    assert all("type_npi" not in statement for statement in statements)


@pytest.mark.asyncio
async def test_skipped_aggregate_defers_address_indexes(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    address_cls = SimpleNamespace(
        __tablename__="mrf_address_20260612",
        __my_initial_indexes__=[{"index_elements": ("checksum",)}],
        __my_additional_indexes__=[
            {"index_elements": ("type", "npi"), "name": "type_npi"},
            {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
        ],
    )
    evidence_cls = SimpleNamespace(__tablename__="mrf_address_evidence_20260612")

    def fake_make_class(cls, suffix, schema_override=None):
        if cls is process_initial.MRFAddress:
            return address_cls
        return evidence_cls

    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_AGGREGATE_DURING_INGEST", raising=False)
    monkeypatch.delenv("HLTHPRT_MRF_ADDRESS_SUMMARY_DEFER_SOURCE_INDEXES", raising=False)
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(process_initial, "make_class", fake_make_class)

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_checksum;" in statements
    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_type_npi;" in statements
    assert "DROP INDEX IF EXISTS mrf.mrf_address_20260612_idx_address_sources;" in statements
    upsert_index = next(i for i, statement in enumerate(statements) if "INSERT INTO mrf.mrf_address_20260612" in statement)
    recreate_index = next(i for i, statement in enumerate(statements) if "CREATE INDEX IF NOT EXISTS mrf_address_20260612_idx_checksum" in statement)
    assert upsert_index < recreate_index


@pytest.mark.asyncio
async def test_refresh_mrf_address_summary_accepts_statement_timeout(monkeypatch):
    statements = []

    class FakeSession:
        async def execute(self, stmt, params=None):
            statements.append(str(stmt))
            return SimpleNamespace(rowcount=1)

    @asynccontextmanager
    async def fake_transaction():
        yield FakeSession()

    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_STATEMENT_TIMEOUT", "45min")
    monkeypatch.setattr(process_initial.db, "transaction", fake_transaction)
    monkeypatch.setattr(
        process_initial,
        "make_class",
        lambda cls, suffix, schema_override=None: SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}"),
    )

    await process_initial._refresh_mrf_address_summary("20260612", "mrf")

    assert statements[0] == "SET LOCAL work_mem = '1GB';"
    assert statements[1] == "SET LOCAL statement_timeout = '45min';"
    assert statements[2] == "ANALYZE mrf.mrf_address_evidence_20260612;"
    assert "INSERT INTO mrf.mrf_address_20260612" in statements[3]


def test_postgres_setting_value_rejects_unsafe_env(monkeypatch):
    monkeypatch.setenv("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "1GB'; DROP TABLE mrf.plan; --")

    with pytest.raises(ValueError, match="HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM"):
        process_initial._postgres_setting_value("HLTHPRT_MRF_ADDRESS_SUMMARY_WORK_MEM", "1GB")


@pytest.mark.asyncio
async def test_plan_summary_dependencies_ready(monkeypatch):
    dependency_values_by_name = {
        "mrf.plan_attributes": "mrf.plan_attributes",
        "mrf.plan_benefits": "mrf.plan_benefits",
        "mrf.plan_prices": None,
    }

    async def fake_scalar(stmt, **params):
        assert "to_regclass" in stmt
        return dependency_values_by_name[params["qualified_name"]]

    monkeypatch.setattr(process_initial.db, "scalar", fake_scalar)

    ready, missing = await process_initial._plan_summary_dependencies_ready("mrf")

    assert ready is False
    assert missing == ["plan_prices"]

def test_ptg_timeout_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG_JOB_TIMEOUT", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG_HUGE_JOB_TIMEOUT", raising=False)

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTGHuge.job_timeout == 172800
        assert reloaded.PTGHuge.functions[0].name == "ptg_control_start"
        assert reloaded.PTGHuge.functions[0].timeout_s == 172800
    finally:
        importlib.reload(process_pkg)


def test_provider_directory_timeout_has_six_day_floor(monkeypatch):
    timeout_name = "HLTHPRT_PROVIDER_DIRECTORY_FHIR_JOB_TIMEOUT"
    try:
        monkeypatch.delenv(timeout_name, raising=False)
        assert importlib.reload(process_pkg).ProviderDirectoryFHIR.job_timeout == 518400
        monkeypatch.setenv(timeout_name, "259200")
        assert importlib.reload(process_pkg).ProviderDirectoryFHIR.job_timeout == 518400
        monkeypatch.setenv(timeout_name, "604800")
        assert importlib.reload(process_pkg).ProviderDirectoryFHIR.job_timeout == 604800
    finally:
        monkeypatch.delenv(timeout_name, raising=False)
        importlib.reload(process_pkg)


def test_ptg_timeout_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG_JOB_TIMEOUT", "3456")

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTG.job_timeout == 3456
        assert reloaded.PTG.functions[0].timeout_s == 3456
        assert reloaded.PTGSmall.functions[0].timeout_s == 3456
        assert reloaded.PTGNormal.functions[0].timeout_s == 3456
        assert reloaded.PTGLarge.functions[0].timeout_s == 3456
        assert reloaded.PTGHuge.functions[0].timeout_s == 3456
    finally:
        monkeypatch.delenv("HLTHPRT_PTG_JOB_TIMEOUT", raising=False)
        importlib.reload(process_pkg)


def test_ptg_lane_timeout_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG_JOB_TIMEOUT", "3456")
    monkeypatch.setenv("HLTHPRT_PTG_HUGE_JOB_TIMEOUT", "7890")

    reloaded = importlib.reload(process_pkg)

    try:
        assert reloaded.PTGLarge.job_timeout == 3456
        assert reloaded.PTGLarge.functions[0].timeout_s == 3456
        assert reloaded.PTGHuge.job_timeout == 7890
        assert reloaded.PTGHuge.functions[0].timeout_s == 7890
    finally:
        monkeypatch.delenv("HLTHPRT_PTG_JOB_TIMEOUT", raising=False)
        monkeypatch.delenv("HLTHPRT_PTG_HUGE_JOB_TIMEOUT", raising=False)
        importlib.reload(process_pkg)
