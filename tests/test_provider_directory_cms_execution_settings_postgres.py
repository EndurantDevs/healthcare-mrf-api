# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Effective CMS settings on real executing backends and borrowed-session restoration."""

import asyncio
import datetime
import importlib
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_nonprofile_capacity as capacity
from process import provider_directory_cms_preparation as preparation
from tests.provider_directory_profile_delta_test_support import _delta_database, importer

native = importlib.import_module("process.entity_address_cutover_contract")

_SETTINGS = "SELECT current_setting('temp_file_limit'),current_setting('max_parallel_workers_per_gather'),current_setting('max_parallel_maintenance_workers')"


async def _backend_clock(database):
    return await database.scalar("SELECT date_trunc('second',clock_timestamp())")


def _execution_fhir(database):
    return SimpleNamespace(db=database, _profile_capacity_preflight_clock=partial(_backend_clock, database))


def _execution_admission():
    return SimpleNamespace(
        plan=SimpleNamespace(temp_file_limit_bytes_per_backend=1024**2),
        lease=SimpleNamespace(
            max_build_deadline=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=1)
        ),
        paired_profile_lease=None,
    )


async def _previous_settings(database):
    await database.status("SET LOCAL temp_file_limit='4MB'")
    await database.status("SET LOCAL max_parallel_workers_per_gather=3")
    await database.status("SET LOCAL max_parallel_maintenance_workers=2")
    await database.status("SET LOCAL statement_timeout='4s'")
    await database.status("SET LOCAL lock_timeout='3s'")
    return tuple(await database.first(_SETTINGS))


async def _exit_bounded_statement(fhir, admission, outcome):
    try:
        async with preparation.nonprofile_sql_transaction(fhir, admission):
            assert tuple(await fhir.db.first(_SETTINGS)) == ("1MB", "0", "0")
            if outcome == "failure":
                raise RuntimeError("synthetic statement failure")
            if outcome == "cancel":
                raise asyncio.CancelledError()
    except RuntimeError, asyncio.CancelledError:
        assert outcome != "success"


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_borrowed_execution_settings_restore_on_all_exits(monkeypatch, outcome):
    async with _delta_database(monkeypatch) as (database, _schema):
        fhir = _execution_fhir(database)
        admission = _execution_admission()
        async with database.transaction():
            previous = await _previous_settings(database)
            await _exit_bounded_statement(fhir, admission, outcome)
            assert tuple(await database.first(_SETTINGS)) == previous
            assert tuple(
                await database.first("SELECT current_setting('statement_timeout'),current_setting('lock_timeout')")
            ) == ("4s", "3s")


@pytest.mark.asyncio
async def test_expiring_budget_cancels_actual_statement_and_restores_session(monkeypatch):
    from sqlalchemy.exc import DBAPIError

    async with _delta_database(monkeypatch) as (database, schema):
        fhir = _execution_fhir(database)
        now = await _backend_clock(database)
        fhir._profile_capacity_preflight_clock = AsyncMock(return_value=now)
        admission = _execution_admission()
        admission.lease.max_build_deadline = now + datetime.timedelta(seconds=1.05)
        await database.status(f"CREATE TABLE {schema}.deadline_result (value int)")
        async with database.transaction():
            previous = await _previous_settings(database)
            with pytest.raises(DBAPIError):
                async with preparation.nonprofile_sql_transaction(fhir, admission):
                    await database.status(f"INSERT INTO {schema}.deadline_result VALUES (1)")
                    await database.scalar("SELECT pg_sleep(1)")
            assert await database.scalar(f"SELECT count(*) FROM {schema}.deadline_result") == 0
            assert tuple(await database.first(_SETTINGS)) == previous
            assert tuple(
                await database.first("SELECT current_setting('statement_timeout'),current_setting('lock_timeout')")
            ) == ("4s", "3s")


@pytest.mark.asyncio
async def test_expired_paired_deadline_rejects_before_sql_settings(monkeypatch):
    async with _delta_database(monkeypatch) as (database, _schema):
        fhir = _execution_fhir(database)
        admission = _execution_admission()
        admission.paired_profile_lease = SimpleNamespace(max_build_deadline=await _backend_clock(database))
        with pytest.raises(RuntimeError, match="build_deadline_reached"):
            async with preparation.nonprofile_sql_transaction(fhir, admission):
                pytest.fail("expired admission executed a statement")


@pytest.mark.asyncio
async def test_actual_mutating_backend_and_observation_share_bounded_settings(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        admission = _execution_admission()
        await database.status(f"CREATE TABLE {schema}.settings_observation (settings text[],pid int)")
        async with database.transaction():
            previous = await _previous_settings(database)
            backend = await database.scalar("SELECT pg_backend_pid()")
            token = preparation._ACTIVE.set(admission)
            try:
                await importer._provider_directory_profile_capacity_status(
                    f"INSERT INTO {schema}.settings_observation SELECT ARRAY["
                    "current_setting('temp_file_limit'),current_setting('max_parallel_workers_per_gather'),"
                    "current_setting('max_parallel_maintenance_workers')],pg_backend_pid()"
                )
            finally:
                preparation._ACTIVE.reset(token)
            assert tuple(await database.first(_SETTINGS)) == previous
            observed = await database.first(f"SELECT settings,pid FROM {schema}.settings_observation")
            assert tuple(observed[0]) == ("1MB", "0", "0") and observed[1] == backend
            row = await capacity._database_observation(importer, admission.plan)
            assert row["temp_limit_bytes"] == 1024**2
            assert row["query_parallel_workers"] == row["maintenance_parallel_workers"] == 0
            assert tuple(await database.first(_SETTINGS)) == previous


@pytest.mark.asyncio
async def test_denied_settings_stop_before_mutating_statement(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        fhir = _execution_fhir(database)
        admission = _execution_admission()
        await database.status(f"CREATE TABLE {schema}.settings_observation (value int)")
        async with database.transaction():
            previous = await _previous_settings(database)
            monkeypatch.setattr(native, "apply_transaction_sql_settings", AsyncMock(return_value=()))
            with pytest.raises(RuntimeError, match="effective_sql_limits_changed"):
                async with preparation.nonprofile_sql_transaction(fhir, admission):
                    await database.status(f"INSERT INTO {schema}.settings_observation VALUES (1)")
            assert tuple(await database.first(_SETTINGS)) == previous
            assert await database.scalar(f"SELECT count(*) FROM {schema}.settings_observation") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_batch", [False, True])
async def test_projected_read_and_insert_share_finite_backend(monkeypatch, resource_batch):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(importer, "_provider_directory_profile_capacity_admission", lambda: None)
        admission = _execution_admission()
        await database.status(f"CREATE TABLE {schema}.settings_observation (settings text[],pid int)")
        batch = SimpleNamespace(
            source_id="cms-npd",
            projected_rows=1,
            projected_logical_bytes=1,
            last_resource_id="example" if resource_batch else None,
        )
        projection_sql = (
            "SELECT 1 AS projected_rows,1 AS projected_logical_bytes,"
            + ("'example'" if resource_batch else "NULL")
            + "::text AS last_cursor WHERE current_setting('temp_file_limit')='1MB' "
            "AND current_setting('max_parallel_workers_per_gather')='0' "
            "AND current_setting('max_parallel_maintenance_workers')='0'"
        )
        insert_sql = (
            f"INSERT INTO {schema}.settings_observation SELECT ARRAY["
            "current_setting('temp_file_limit'),current_setting('max_parallel_workers_per_gather'),"
            "current_setting('max_parallel_maintenance_workers')],pg_backend_pid()"
        )
        monkeypatch.setattr(importer, "_artifact_resource_batch_projection_sql", lambda *_args: projection_sql)
        async with database.transaction():
            previous = await _previous_settings(database)
            backend = await database.scalar("SELECT pg_backend_pid()")
            token = preparation._ACTIVE.set(admission)
            try:
                if resource_batch:
                    inserted = await importer._execute_artifact_resource_batch(None, schema, insert_sql, {}, batch)
                else:
                    inserted = await importer._execute_artifact_source_batch(
                        batch, object(), projection_sql, insert_sql
                    )
            finally:
                preparation._ACTIVE.reset(token)
            assert inserted == 1 and tuple(await database.first(_SETTINGS)) == previous
            observed = await database.first(f"SELECT settings,pid FROM {schema}.settings_observation")
            assert tuple(observed[0]) == ("1MB", "0", "0") and observed[1] == backend
