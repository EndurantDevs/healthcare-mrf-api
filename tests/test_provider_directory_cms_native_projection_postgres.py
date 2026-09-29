# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real transformed-source parity; this does not prove the full overlay or native builder."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from process import provider_directory_cms_native_projection as projection
from process.entity_address_candidate_preparation import (
    ProviderDirectoryAddressDatasetPin,
    ProviderDirectoryAddressPreparationInput,
)
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_cms_native_projection import _bounds

_SETTINGS_SQL = (
    "SELECT pg_size_bytes(current_setting('temp_file_limit')), "
    "current_setting('max_parallel_workers_per_gather')::int, "
    "current_setting('max_parallel_maintenance_workers')::int, "
    "current_setting('transaction_read_only'), current_setting('transaction_isolation'), pg_backend_pid()"
)


async def _seed_sources(database, schema):
    """Create synthetic native arrays, Unicode/contact inputs and desired overlay membership."""
    native = projection._native()
    await database.status(native._prepare_raw_stage_sql(schema, "npi_address"))
    await database.status(f"""INSERT INTO {schema}.npi_address
        (entity_type,entity_id,npi,type,taxonomy_array,plans_network_array,procedures_array,
         medications_array,source_priority,checksum,first_line,city_name,state_name,postal_code,
         telephone_number,country_code)
        VALUES ('npi','1000000004',1000000004,'primary',ARRAY[1,2,NULL],ARRAY[3],ARRAY[0],ARRAY[0],
                0,1,'  10 Café Road  ','  Example  ','CA','90001','(202) 555-0100 ext 12','')""")
    await database.status(f"""CREATE TABLE {schema}.desired_overlay (
        source_id text,last_seen_run_id text,resource_type text,resource_id text,npi bigint,
        first_line text,second_line text,city_name text,state_name text,state_code text,
        postal_code text,country_code text,telephone_number text,fax_number text,formatted_address text,
        lat numeric,long numeric,address_key uuid,source_updated_at timestamp,published_at timestamp,
        source_record_id text)""")
    await database.status(f"""INSERT INTO {schema}.desired_overlay
        (source_id,last_seen_run_id,resource_type,resource_id,npi,first_line,city_name,state_name,
         postal_code,country_code,telephone_number,source_record_id)
        VALUES ('cms-npd','desired-root','Organization','desired',1000000004,'  20 Second Road  ',
                'Example','CA','90001','US','202-555-0101','cms:desired'),
               ('cms-npd','old-root','Organization','old',1000000004,'Excluded',
                'Example','CA','90001','US',NULL,'cms:old')""")
    await database.status(f"""CREATE TABLE {schema}.provider_directory_endpoint_dataset (
        endpoint_id text,dataset_id text,dataset_hash text,acquisition_root_run_id text,
        validated_at timestamptz,published_at timestamptz,superseded_at timestamptz,
        status text,is_current bool,publication_metadata_json jsonb)""")
    await database.status(
        f"""INSERT INTO {schema}.provider_directory_endpoint_dataset VALUES
        ('cms-endpoint','desired-dataset',:hash,'desired-root',now(),NULL,NULL,'validated',false,
         '{{"source_ids":["cms-npd"]}}')""",
        hash="a" * 64,
    )
    await database.status(
        f"CREATE TABLE {schema}.provider_directory_dataset_resource "
        "(dataset_id text,resource_type text,resource_id text)"
    )
    await database.status(
        f"INSERT INTO {schema}.provider_directory_dataset_resource VALUES ('desired-dataset','Organization','desired')"
    )


async def _inputs(database, schema):
    """Use a real physical overlay only to compare virtual and ordinary builder SQL."""
    oid = await database.scalar("SELECT to_regclass(:name)::oid::bigint", name=f"{schema}.desired_overlay")
    return ProviderDirectoryAddressPreparationInput(
        (ProviderDirectoryAddressDatasetPin("cms-npd", "cms-endpoint", "desired-dataset", "a" * 64, "desired-root"),),
        "desired_overlay",
        oid,
        1,
        semantic_as_of="2026-01-02",
    )


def _source_queries(schema):
    native = projection._native()
    available_by_name = {name: True for name in native.PROVIDER_DIRECTORY_DATASET_FENCE_TABLES}
    available_by_name["npi_address"] = True
    return native._current_provider_directory_source_selects(
        schema, available_by_name, native._source_selects(schema, available_by_name)
    )


async def _verify_parity(database, schema, source_selects, query_inputs):
    """Compare scalar/array measurements to actual raw INSERT output on identical inputs."""
    native = projection._native()
    await database.status(native._prepare_raw_stage_sql(schema, "raw_result"))
    for source_select in source_selects:
        await database.status(native._insert_raw_from_source_sql(schema, "raw_result", source_select))
    with projection.candidate.source_query_scope(query_inputs):
        virtual_selects = _source_queries(schema)
        assert projection.candidate.current() is None
    async with database.session() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        observations = await projection._observe_sources(session, virtual_selects)
        actual = (
            (await session.execute(text(projection.raw_observation_sql(f"SELECT * FROM {schema}.raw_result"))))
            .mappings()
            .one()
        )
    assert sum(observation["row_count"] for observation in observations) == actual["row_count"] == 2
    for category in ("text_octets", "arrays"):
        for field in actual[category]:
            expected_stats = [observation[category][field] for observation in observations]
            if category == "text_octets":
                assert actual[category][field] == _combined(expected_stats)
            else:
                assert actual[category][field] == {
                    metric: _combined([statistic[metric] for statistic in expected_stats])
                    for metric in expected_stats[0]
                }
    assert actual["arrays"]["taxonomy_array"]["elements"]["total"] == 6
    assert actual["arrays"]["taxonomy_array"]["element_octets"]["total"] == 16
    assert actual["text_octets"]["first_line"]["total"] == len("10 Café Road20 Second Road".encode())
    assert await database.scalar(f"SELECT bool_and(updated_at=timestamp '2026-01-02') FROM {schema}.raw_result")


def _combined(statistics):
    return {
        "total": sum(statistic["total"] for statistic in statistics),
        "maximum": max(statistic["maximum"] for statistic in statistics),
    }


@pytest.mark.asyncio
async def test_desired_virtual_overlay_and_native_raw_insert_parity(monkeypatch):
    """Exact desired membership, phone cleanup, Unicode and nullable arrays agree with the builder."""
    async with _delta_database(monkeypatch) as (database, schema):
        await _seed_sources(database, schema)
        from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection

        inputs = await _inputs(database, schema)
        token = projection.candidate._PREPARATION.set(inputs)
        try:
            source_selects = _source_queries(schema)
        finally:
            projection.candidate._PREPARATION.reset(token)
        query_inputs = projection.candidate.ProviderDirectoryAddressSourceQueryInput(
            inputs.dataset_pins,
            inputs.relation_overrides,
            inputs.semantic_as_of,
            DesiredOverlayProjection(f"SELECT * FROM {schema}.desired_overlay", "a" * 64, "b" * 64),
        )
        await _verify_parity(database, schema, source_selects, query_inputs)


async def _observer_fixture(database, schema, monkeypatch):
    """Keep real source aggregates and setting helpers, with synthetic dependency fences."""
    from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection

    native = projection._native()
    monkeypatch.setattr(native, "db", database)
    monkeypatch.setattr(native, "_is_address_canon_available", AsyncMock(return_value=False))
    monkeypatch.setattr(projection, "_availability", AsyncMock(return_value={}))
    monkeypatch.setattr(
        projection, "_source_queries", lambda *args, **kwargs: native._source_selects(schema, {"npi_address": True})
    )
    await _seed_sources(database, schema)
    for name, setting in (
        ("temp_file_limit", "-1"),
        ("max_parallel_workers_per_gather", "2"),
        ("max_parallel_maintenance_workers", "2"),
    ):
        await database.status(f"SET SESSION {name} = '{setting}'")
    address = SimpleNamespace(
        input_hash="a" * 64,
        input_json=json.dumps(_bounds()),
        fhir=SimpleNamespace(db=database, _schema=lambda: schema),
    )
    fence = SimpleNamespace(datasets=())
    overlay = DesiredOverlayProjection("SELECT 1", address.input_hash, projection.desired_fence_hash(fence))
    return address, fence, overlay, tuple(await database.first(_SETTINGS_SQL))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "changed", "cancel"])
async def test_observer_effective_limits_and_restoration_on_both_snapshots(monkeypatch, failure):
    """Both actual read-only backends enforce bound limits and restore their original settings."""
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT", "-1")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER", "4")
    async with _delta_database(monkeypatch) as (database, schema):
        address, fence, overlay, previous = await _observer_fixture(database, schema, monkeypatch)
        probes, restored = [], []
        native = projection._native()
        tuned_transaction = native.entity_address_tuned_transaction

        @asynccontextmanager
        async def observe_restoration(*args, **kwargs):
            try:
                async with tuned_transaction(*args, **kwargs):
                    yield
            finally:
                restored.append(tuple(await database.first(_SETTINGS_SQL)))

        async def assert_snapshot(*args):
            probes.append(tuple(await database.first(_SETTINGS_SQL)))
            if len(probes) == 2 and failure:
                if failure == "cancel":
                    raise asyncio.CancelledError()
                raise RuntimeError("synthetic inputs changed")

        monkeypatch.setattr(native, "entity_address_tuned_transaction", observe_restoration)
        monkeypatch.setattr(projection, "_assert_snapshot", assert_snapshot)
        if failure:
            with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
                await projection.observe_native_raw_projection(address, fence, overlay)
        else:
            projection_result = await projection.observe_native_raw_projection(address, fence, overlay)
            assert projection_result["row_count"] == 1
            assert projection_result["capacity_complete"] is False
        assert len(probes) == len(restored) == 2
        assert all(settings_row[:5] == (32 * 1024, 0, 0, "on", "repeatable read") for settings_row in probes)
        assert all(settings_row[:3] == previous[:3] and settings_row[-1] == previous[-1] for settings_row in restored)
        assert tuple(await database.first(_SETTINGS_SQL)) == previous


@pytest.mark.asyncio
@pytest.mark.parametrize("denied_snapshot", [1, 2])
async def test_denied_limit_stops_actual_observer_before_snapshot_reads(monkeypatch, denied_snapshot):
    """A skipped privileged SET cannot allow either backend to execute unbounded input scans."""
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "1")
    async with _delta_database(monkeypatch) as (database, schema):
        address, fence, overlay, previous = await _observer_fixture(database, schema, monkeypatch)
        status, attempts = database.status, []

        async def deny_setting(statement, **params):
            if statement.startswith("SET LOCAL temp_file_limit = '32kB'"):
                attempts.append(statement)
                if len(attempts) == denied_snapshot:
                    raise RuntimeError("permission denied to set parameter")
            return await status(statement, **params)

        assertions = AsyncMock()
        monkeypatch.setattr(database, "status", deny_setting)
        monkeypatch.setattr(projection, "_assert_snapshot", assertions)
        with pytest.raises(RuntimeError, match="executing_session_settings_changed"):
            await projection.observe_native_raw_projection(address, fence, overlay)
        assert assertions.await_count == denied_snapshot - 1
        assert tuple(await database.first(_SETTINGS_SQL)) == previous
