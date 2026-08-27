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

def _plan_drug_statistics_tables():
    from sqlalchemy import Boolean, Column, DateTime, Integer, MetaData, String, Table

    metadata = MetaData()
    plan_drug = Table(
        "plan_drug_raw_test",
        metadata,
        Column("plan_id", String),
        Column("drug_tier", String),
        Column("prior_authorization", Boolean),
        Column("step_therapy", Boolean),
        Column("quantity_limit", Boolean),
        Column("last_updated_on", DateTime),
    )
    stats = Table(
        "plan_drug_stats_test",
        metadata,
        Column("plan_id", String, primary_key=True),
        Column("total_drugs", Integer),
        Column("auth_required", Integer),
        Column("auth_not_required", Integer),
        Column("step_required", Integer),
        Column("step_not_required", Integer),
        Column("quantity_limit", Integer),
        Column("quantity_no_limit", Integer),
        Column("last_updated_on", DateTime),
    )
    tier_stats = Table(
        "plan_drug_tier_stats_test",
        metadata,
        Column("plan_id", String, primary_key=True),
        Column("drug_tier", String, primary_key=True),
        Column("drug_count", Integer),
    )
    return plan_drug, stats, tier_stats


class _AggregateInsertSpy:
    def __init__(self, table, inserts):
        self.table = table
        self.inserts = inserts
        self.excluded = SimpleNamespace(
            **{column.name: f"excluded_{column.name}" for column in table.c}
        )

    def from_select(self, columns, select_stmt):
        self.columns = columns
        self.select_stmt = select_stmt
        return self

    def on_conflict_do_update(self, index_elements=None, set_=None):
        self.index_elements = index_elements
        self.set_ = set_
        return self

    async def status(self):
        self.inserts.append(self)


@pytest.mark.asyncio
async def test_refresh_plan_drug_statistics_upserts_concurrent_refreshes(monkeypatch):
    """Verify refresh plan drug statistics upserts concurrent refreshes."""
    plan_drug, stats, tier_stats = _plan_drug_statistics_tables()
    inserts = []

    def fake_make_class(cls, suffix, schema_override=None):
        table_by_cls = {
            process_initial.PlanDrugRaw: plan_drug,
            process_initial.PlanDrugStats: stats,
            process_initial.PlanDrugTierStats: tier_stats,
        }
        return SimpleNamespace(__table__=table_by_cls[cls])

    def fail_delete(_table):
        raise AssertionError("aggregate refresh should upsert instead of delete then insert")

    monkeypatch.setattr(process_initial, "make_class", fake_make_class)
    monkeypatch.setattr(
        process_initial.db,
        "insert",
        lambda table: _AggregateInsertSpy(table, inserts),
    )
    monkeypatch.setattr(process_initial.db, "delete", fail_delete)

    await process_initial._refresh_plan_drug_statistics({"94529WI0240007"}, "20260612", "mrf")

    assert len(inserts) == 2
    stats_insert, tier_insert = inserts
    assert stats_insert.table is stats
    assert [column.name for column in stats_insert.index_elements] == ["plan_id"]
    assert stats_insert.set_ == {
        "total_drugs": "excluded_total_drugs",
        "auth_required": "excluded_auth_required",
        "auth_not_required": "excluded_auth_not_required",
        "step_required": "excluded_step_required",
        "step_not_required": "excluded_step_not_required",
        "quantity_limit": "excluded_quantity_limit",
        "quantity_no_limit": "excluded_quantity_no_limit",
        "last_updated_on": "excluded_last_updated_on",
    }
    assert tier_insert.table is tier_stats
    assert [column.name for column in tier_insert.index_elements] == ["plan_id", "drug_tier"]
    assert tier_insert.set_ == {"drug_count": "excluded_drug_count"}


@pytest.mark.asyncio
async def test_refresh_all_plan_drug_statistics_batches_from_stage(monkeypatch):
    calls = []

    async def fake_scalar(sql, **params):
        assert "to_regclass" in sql
        assert params["qualified_name"] == "mrf.plan_drug_raw_20260612"
        return "mrf.plan_drug_raw_20260612"

    async def fake_all(sql):
        calls.append(str(sql))
        return [SimpleNamespace(plan_id="94529WI0240007"), ("94529WI0240008",)]

    async def fake_refresh(plan_ids, import_date, db_schema):
        calls.append((set(plan_ids), import_date, db_schema))

    monkeypatch.setattr(
        process_initial,
        "make_class",
        lambda cls, suffix, schema_override=None: SimpleNamespace(__tablename__=f"{cls.__tablename__}_{suffix}"),
    )
    monkeypatch.setattr(process_initial.db, "scalar", fake_scalar)
    monkeypatch.setattr(process_initial.db, "all", fake_all)
    monkeypatch.setattr(process_initial, "_refresh_plan_drug_statistics", fake_refresh)

    await process_initial._refresh_all_plan_drug_statistics("20260612", "mrf")

    assert "SELECT DISTINCT plan_id" in calls[0]
    assert calls[1] == ({"94529WI0240007", "94529WI0240008"}, "20260612", "mrf")


@pytest.mark.asyncio
async def test_refresh_do_business_as(monkeypatch):
    calls = []

    async def fake_scalar(sql):
        calls.append(sql)
        if "to_regclass" in str(sql):
            return "mrf.npi_other_identifier"
        return 0

    monkeypatch.setattr(process_npi, "ensure_database", AsyncMock())
    monkeypatch.setattr(process_npi.db, "status", AsyncMock())
    monkeypatch.setattr(process_npi.db, "scalar", fake_scalar)
    monkeypatch.setattr(os, "getenv", lambda name, default=None: "mrf" if name in {"DB_SCHEMA", "HLTHPRT_DB_SCHEMA"} else default)

    result = await process_npi.refresh_do_business_as()

    sql_strings = [str(sql) for sql in calls]
    assert result == (0, 0)
    assert not any(sql_text.strip().startswith("UPDATE mrf.npi") for sql_text in sql_strings)
    assert any("IS DISTINCT FROM" in sql_text for sql_text in sql_strings)
    assert any("NOT EXISTS" in sql_text for sql_text in sql_strings)
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
async def test_refresh_taxonomy_arrays_uses_deterministic_changed_only_update(monkeypatch):
    calls = []

    async def fake_scalar(sql):
        calls.append(str(sql))
        return 12

    monkeypatch.setattr(process_npi.db, "scalar", fake_scalar)

    updated = await process_npi.refresh_taxonomy_arrays(
        address_table="npi_address_20260613",
        taxonomy_table="npi_taxonomy_20260613",
        schema="mrf",
    )

    assert updated == 12
    sql = calls[0]
    assert "ARRAY_AGG(DISTINCT nucc.int_code ORDER BY nucc.int_code)::int[]" in sql
    assert "addr.taxonomy_array IS DISTINCT FROM sub.res" in sql


@pytest.mark.asyncio
async def test_refresh_npi_search_taxonomy_codes_is_deterministic_and_exact(monkeypatch):
    calls = []

    async def fake_scalar(sql):
        calls.append(str(sql))
        return 34

    monkeypatch.setattr(process_npi.db, "scalar", fake_scalar)

    updated = await process_npi.refresh_npi_search_taxonomy_codes(
        npi_table="npi_20260828",
        taxonomy_table="npi_taxonomy_20260828",
        schema="mrf",
    )

    assert updated == 34
    sql = " ".join(calls[0].split())
    assert "DISTINCT tax.healthcare_provider_taxonomy_code" in sql
    assert "ORDER BY tax.healthcare_provider_taxonomy_code" in sql
    assert "tax.healthcare_provider_taxonomy_code IS NOT NULL" in sql
    assert "UPPER(" not in sql
    assert "BTRIM(" not in sql
    assert ")::varchar[] AS codes" in sql
    assert "provider.search_taxonomy_codes IS DISTINCT FROM projection.codes" in sql


@pytest.mark.asyncio
async def test_npi_search_taxonomy_projection_validation_rejects_mismatch(monkeypatch):
    monkeypatch.setattr(process_npi.db, "scalar", AsyncMock(return_value=True))

    with pytest.raises(
        process_npi.NPIPrerequisiteError,
        match="NPI search taxonomy projection is invalid",
    ):
        await process_npi.validate_npi_search_taxonomy_projection(
            npi_table="npi_20260828",
            taxonomy_table="npi_taxonomy_20260828",
            schema="mrf",
        )

    sql = str(process_npi.db.scalar.await_args.args[0])
    assert "COALESCE(projection.codes, ARRAY[]::varchar[])" in sql
    assert "provider.search_taxonomy_codes IS DISTINCT FROM" in sql


@pytest.mark.asyncio
async def test_npi_search_taxonomy_projection_validates_on_owned_connection(monkeypatch):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    scalar = AsyncMock()
    monkeypatch.setattr(process_npi.db, "scalar", scalar)

    await process_npi.validate_npi_search_taxonomy_projection(
        npi_table="npi_20260828",
        taxonomy_table="npi_taxonomy_20260828",
        schema="mrf",
        connection=connection,
    )

    connection.fetchval.assert_awaited_once()
    scalar.assert_not_awaited()
