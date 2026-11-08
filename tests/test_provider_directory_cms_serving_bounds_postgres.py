# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bounds for retained-input capture and the early artifact backfills."""

import asyncio
import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_serving as serving
from tests.provider_directory_profile_delta_test_support import _delta_database, importer
from tests.test_provider_directory_cms_execution_settings_postgres import (
    _backend_clock,
    _execution_admission,
    _execution_fhir,
    _previous_settings,
)

_SETTINGS = "SELECT current_setting('temp_file_limit'),current_setting('max_parallel_workers_per_gather'),current_setting('max_parallel_maintenance_workers'),current_setting('statement_timeout'),current_setting('lock_timeout')"
_OBSERVATION = (
    "SELECT pg_size_bytes(current_setting('temp_file_limit')) AS temp_bytes,"
    "current_setting('max_parallel_workers_per_gather')::int AS query_workers,"
    "current_setting('max_parallel_maintenance_workers')::int AS maintenance_workers,"
    "extract(epoch FROM current_setting('statement_timeout')::interval)*1000 AS statement_ms,"
    "extract(epoch FROM current_setting('lock_timeout')::interval)*1000 AS lock_ms,"
    "current_setting('transaction_read_only') AS read_only,"
    "current_setting('transaction_isolation') AS isolation,pg_backend_pid() AS pid"
)


async def _observe_limits(database):
    row_by_field = dict((await database.first(_OBSERVATION))._mapping)
    assert row_by_field["temp_bytes"] == 1024**2
    assert row_by_field["query_workers"] == row_by_field["maintenance_workers"] == 0
    assert 0 < row_by_field["statement_ms"] <= 60_000 and 0 < row_by_field["lock_ms"] <= row_by_field["statement_ms"]
    return row_by_field


def _capture_readers(monkeypatch, database, observed, *, delay=False):
    outputs_by_name = {name: object() for name in ("fence", "predecessor", "dependencies", "native_fence", "proof")}

    def reader(name):
        async def read(*_args, **_kwargs):
            observed.append(await _observe_limits(database))
            if delay:
                await database.scalar("SELECT pg_sleep(1)")
            return outputs_by_name[name]

        return read

    monkeypatch.setattr(serving, "prepare_desired_fence", reader("fence"))
    monkeypatch.setattr(serving, "_current_predecessor", reader("predecessor"))
    monkeypatch.setattr(serving.receipts, "capture_native_dependencies", reader("dependencies"))
    monkeypatch.setattr(serving, "capture_native_address_input_fence", reader("native_fence"))
    monkeypatch.setattr(serving, "_candidate_proof", reader("proof"))
    return outputs_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [False, True])
async def test_retained_capture_bounds_same_readonly_backend_and_releases_on_timeout(monkeypatch, deadline):
    async with _delta_database(monkeypatch) as (database, schema):
        fhir = _execution_fhir(database)
        fhir._schema = lambda: schema
        fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS = ("profile", "address_overlay")
        limits, observed = _execution_admission(), []
        now = await _backend_clock(database)
        # One rounded clock tick isolates the SQL clamp from transaction setup expiration.
        fhir._profile_capacity_preflight_clock = (
            AsyncMock(side_effect=(now - datetime.timedelta(seconds=1), now))
            if deadline
            else AsyncMock(return_value=now)
        )
        limits.paired_profile_lease = SimpleNamespace(
            max_build_deadline=now + datetime.timedelta(seconds=1.05 if deadline else 30)
        )
        monkeypatch.setattr(serving, "_verified_capture_limits", AsyncMock(return_value=limits))
        outputs = _capture_readers(monkeypatch, database, observed, delay=deadline)
        previous_setting_values = tuple(await database.first(_SETTINGS))
        operation = serving._capture_publish_inputs(fhir, object(), "run_" + "a" * 32, {}, limits.plan)
        if deadline:
            with pytest.raises((DBAPIError, TimeoutError)):
                await operation
            assert len(observed) == 1
        else:
            assert vars(await operation) == {**outputs, "capacity_lease": limits.lease}
            assert len(observed) == 5
        assert all(
            observation_row["read_only"] == "on" and observation_row["isolation"] == "repeatable read"
            for observation_row in observed
        )
        assert all(observation_row["statement_ms"] <= (50 if deadline else 29_000) for observation_row in observed)
        assert len({observation_row["pid"] for observation_row in observed}) == 1
        assert database._transaction_binding() is None
        assert tuple(await database.first(_SETTINGS)) == previous_setting_values
        assert (
            await database.scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE pid=:pid AND state='active' AND query LIKE 'SELECT pg_sleep(%'",
                pid=observed[0]["pid"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_retained_capture_deadline_cancels_setup_before_any_reader_and_releases_backend(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        fhir = _execution_fhir(database)
        fhir._schema = lambda: schema
        fhir.PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS = ("profile", "address_overlay")
        limits = _execution_admission()
        now = await _backend_clock(database)
        fhir._profile_capacity_preflight_clock = AsyncMock(return_value=now)
        limits.paired_profile_lease = SimpleNamespace(max_build_deadline=now + datetime.timedelta(seconds=1.05))
        monkeypatch.setattr(serving, "_verified_capture_limits", AsyncMock(return_value=limits))
        setup_release = asyncio.Event()

        async def block_temp_limit(*_args, **_kwargs):
            await setup_release.wait()

        monkeypatch.setattr(serving, "apply_temp_file_limit", AsyncMock(side_effect=block_temp_limit))
        reader = AsyncMock(side_effect=AssertionError("capture reader ran before setup completed"))
        for owner, name in (
            (serving, "prepare_desired_fence"),
            (serving, "_current_predecessor"),
            (serving.receipts, "capture_native_dependencies"),
            (serving, "capture_native_address_input_fence"),
            (serving, "_candidate_proof"),
        ):
            monkeypatch.setattr(owner, name, reader)
        previous_setting_values = tuple(await database.first(_SETTINGS))
        watchdog = asyncio.timeout(1)
        async with watchdog:
            with pytest.raises(TimeoutError):
                await serving._capture_publish_inputs(fhir, object(), "run_" + "a" * 32, {}, limits.plan)
        assert not watchdog.expired()
        reader.assert_not_called()
        assert database._transaction_binding() is None
        assert tuple(await database.first(_SETTINGS)) == previous_setting_values


async def _backfill_relations(database, schema, worker):
    if worker == "resource_npi":
        for table in ("provider_directory_practitioner", "provider_directory_organization"):
            await database.status(
                f"CREATE TABLE {schema}.{table} (resource_id text,source_id text,npi bigint,updated_at timestamptz)"
            )
            await database.status(
                f"INSERT INTO {schema}.{table} VALUES ('1234567890','synthetic',NULL,NULL),('1234567891','cms-npd',NULL,NULL)"
            )
        return None
    relation = "location_scratch"
    await database.status(f"CREATE UNLOGGED TABLE {schema}.{relation} (value int)")
    await database.status(f"INSERT INTO {schema}.{relation} VALUES (0)")
    return relation


def _observe_backfill_calls(monkeypatch, database, observed):
    original_first, original_status = database.first, database.status

    async def observe(statement, **params):
        if statement.lstrip().startswith(("UPDATE", "WITH changed")):
            observed.append(await _observe_limits(database))

    async def first(statement, **params):
        await observe(statement, **params)
        return await original_first(statement, **params)

    async def status(statement, **params):
        await observe(statement, **params)
        return await original_status(statement, **params)

    monkeypatch.setattr(database, "first", first)
    monkeypatch.setattr(database, "status", status)


async def _run_backfill(monkeypatch, schema, relation, worker):
    if worker == "location_key":
        with pytest.raises(RuntimeError, match="address_scratch_custody_required"):
            await importer.publish_provider_directory_location_address_keys(schema)
        return None
    if worker == "resource_npi":
        # Ordinary four-column fixtures cannot authorize protected output maintenance.
        monkeypatch.setattr(importer, "_schema", lambda: schema)
        with pytest.raises(RuntimeError, match="npi_unbound_operation_required"):
            await importer.backfill_provider_directory_resource_id_npis(schema)
        return None
    update = f"UPDATE {schema}.{relation} SET value=value+1"
    batch = f"WITH changed AS ({update} RETURNING 1) SELECT 0 AS candidate_rows,count(*) AS updated_rows,NULL AS last_source_id,NULL AS last_resource_id FROM changed"
    monkeypatch.setattr(importer, "_is_table_present", AsyncMock(return_value=True))
    if worker == "contacts":
        monkeypatch.setattr(importer, "_ensure_provider_directory_tables", AsyncMock())
        monkeypatch.setattr(importer, "provider_directory_location_contact_backfill_sql", lambda *_args: update)
        return await importer.backfill_provider_directory_location_contacts()
    if worker == "coordinates":
        monkeypatch.setattr(
            importer, "provider_directory_location_coordinate_batch_sql", lambda *_args, **_kwargs: batch
        )
        return await importer.backfill_provider_directory_location_coordinates(schema)


async def _assert_legacy_npi_unchanged(database, schema):
    """The unsupported ordinary component must retain both original row sets."""
    for table in ("provider_directory_practitioner", "provider_directory_organization"):
        assert await database.scalar(f"SELECT count(*) FROM {schema}.{table} WHERE npi IS NULL") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("worker", ["contacts", "coordinates", "resource_npi", "location_key"])
async def test_early_backfills_bound_actual_statements_and_preserve_owned_location(monkeypatch, worker):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        relation = await _backfill_relations(database, schema, worker)
        oid = (
            None
            if relation is None
            else await database.scalar("SELECT to_regclass(:relation)::oid", relation=f"{schema}.{relation}")
        )
        observed_rows = []
        _observe_backfill_calls(monkeypatch, database, observed_rows)
        async with database.transaction():
            await _previous_settings(database)
            previous_setting_values = tuple(await database.first(_SETTINGS))
            tokens = (
                (preparation._ACTIVE, preparation._ACTIVE.set(_execution_admission())),
                (
                    importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES,
                    importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set(
                        {"provider_directory_location": relation}
                    ),
                ),
                (
                    importer._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS,
                    importer._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.set({"provider_directory_location": oid}),
                ),
            )
            try:
                backfill_result = await _run_backfill(monkeypatch, schema, relation, worker)
            finally:
                for variable, token in reversed(tokens):
                    variable.reset(token)
            assert tuple(await database.first(_SETTINGS)) == previous_setting_values
            assert len(observed_rows) == (0 if worker in {"resource_npi", "location_key"} else 1)
            assert len({observation_row["pid"] for observation_row in observed_rows}) == (
                0 if worker in {"resource_npi", "location_key"} else 1
            )
            if relation is not None:
                assert (
                    await database.scalar("SELECT to_regclass(:relation)::oid", relation=f"{schema}.{relation}") == oid
                )
                assert await database.scalar(f"SELECT value FROM {schema}.{relation}") == (
                    0 if worker == "location_key" else 1
                )
            else:
                assert backfill_result is None
                await _assert_legacy_npi_unchanged(database, schema)
                assert (
                    await database.scalar(
                        f"SELECT npi IS NULL FROM {schema}.provider_directory_practitioner WHERE source_id='cms-npd'"
                    )
                    is True
                )
