# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native reader continuity through a fresh census with seeded authority ledgers."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_publication as publication
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from process import provider_directory_profile_selection as selection
from tests import test_provider_directory_profile_initial_migration as fixture
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.test_provider_directory_profile_initial_cutover_postgres import _cutover_fixture, _seed_legacy_snapshot, fhir
from tests.test_provider_directory_profile_initial_migration import _SCHEMA, _database


async def _reader_fixture(engine, monkeypatch):
    publication_values = fixture._publication_values

    async def page_funded_values(engine):
        values = await publication_values(engine)
        for cap in values[3]["capacity_geometry"]["relation_byte_caps"]:
            if cap["relation_name"] in {"evidence_stage", "profile_stage"}:
                cap["max_scratch_bytes"] = 1024**2
        return values

    monkeypatch.setattr(fixture, "_publication_values", page_funded_values)
    database, cutover, stages = await _cutover_fixture(engine, monkeypatch)
    build = cutover["build"]
    async with database.transaction():
        await database.status(
            f'ALTER TABLE "{_SCHEMA}".import_run ADD COLUMN importer text, '
            "ADD COLUMN finished_at timestamptz, ADD COLUMN metrics jsonb"
        )
        await _seed_legacy_snapshot(database, _SCHEMA, "legacy", selection, synthetic_profile_execution)
        await database.status(
            f'''INSERT INTO "{_SCHEMA}".initial_profile_stage
            SELECT npi,profile_json,evidence_json,source_ids,endpoint_ids,dataset_ids,source_count,
            independent_source_count,fact_count,:generation,published_at FROM "{_SCHEMA}".provider_directory_profile''',
            generation=build.generation_id,
        )
        await database.status(
            f'INSERT INTO "{_SCHEMA}".initial_evidence_stage SELECT * FROM "{_SCHEMA}".provider_directory_profile_evidence'
        )
        await database.status(f'CREATE TABLE "{_SCHEMA}".callback_marker (marker integer)')
        initial_targets = await initial.capture_targets(fhir, _SCHEMA)
    admission = replace(cutover["admission"], admitted_identity=SimpleNamespace(initial_targets=initial_targets))
    # The existing fixture seeds authority and ready checkpoints, without runtime admission.
    monkeypatch.setattr(fhir, "_admission_database_guard", AsyncMock())
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_checkpoint_ready", AsyncMock())
    monkeypatch.setattr(fhir, "_verify_active_profile_selection_at_cutover", AsyncMock())
    return database, admission, build, stages


async def _assert_writer_fenced(engine, table):
    async with engine.connect() as writer:
        await writer.execute(text("SET LOCAL lock_timeout='50ms'"))
        with pytest.raises(DBAPIError) as refused:
            await writer.execute(text(f'UPDATE "{_SCHEMA}"."{table}" SET npi=npi WHERE npi=1234567890'))
        assert refused.value.orig.sqlstate == "55P03"


async def _observe_census(engine, census_started, release):
    await asyncio.wait_for(census_started.wait(), 5)
    async with engine.connect() as reader:
        await reader.execute(text("SET LOCAL statement_timeout='250ms'"))
        assert (
            await asyncio.wait_for(
                reader.scalar(text(f'SELECT generation_id FROM "{_SCHEMA}".provider_directory_profile')), 1
            )
            == "pdprofile_" + "a" * 32
        )
    await _assert_writer_fenced(engine, "initial_profile_stage")
    await _assert_writer_fenced(engine, "initial_evidence_stage")
    await _assert_writer_fenced(engine, "provider_directory_profile")
    await _assert_writer_fenced(engine, "provider_directory_profile_evidence")
    # Preparation can outlast the unchanged ordinary cutover wall while readers proceed.
    await asyncio.sleep(2.05)
    release.set()


async def _publish_census(database, admission, stages, composite, observations_by_field):
    fence = fhir.ProviderDirectoryArtifactDatasetFence((), should_select_validated_candidates=composite)
    timeout_seconds = await initial.preparation_timeout_seconds(fhir, stages, 2)
    async with asyncio.timeout(timeout_seconds) as timeout, database.transaction():
        await fhir._apply_provider_directory_profile_capacity_settings(admission)

        async def before_swaps():
            observations_by_field["live_timeout_ms"] = (await fhir._profile_capacity_observed_settings())[
                "statement_timeout_ms"
            ]
            await publication._lock_live_swap_relations(
                fhir, database._transaction_binding().session, SimpleNamespace(stages=stages), None, None
            )
            assert (
                await database.scalar(
                    "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=pg_backend_pid() "
                    "AND l.mode='AccessExclusiveLock' AND l.granted AND n.nspname=:schema "
                    "AND c.relname IN ('provider_directory_profile','provider_directory_profile_evidence')",
                    schema=_SCHEMA,
                )
                == 2
            )
            await database.status(f'INSERT INTO "{_SCHEMA}".callback_marker VALUES (1)')
            observations_by_field["callback_lsn"] = await database.scalar("SELECT pg_current_wal_insert_lsn()::text")

        schema, relations, _lock, _statement = fhir._provider_directory_artifact_bundle_context(stages, None)
        await fhir._apply_locked_provider_directory_artifact_bundle(
            stages, schema, relations, None, fence, timeout, before_swaps if composite else None
        )
        assert timeout.when() - asyncio.get_running_loop().time() <= (12 if composite else 2)
        await database.status("SET CONSTRAINTS ALL IMMEDIATE")


async def _assert_census_commit(database, admission, build, composite, observations_by_field):
    assert observations_by_field["counts"]["profile_rows"] == observations_by_field["counts"]["evidence_rows"] == 1
    assert 8000 < observations_by_field["preparation_timeout_ms"] <= admission.geometry.statement_timeout_ms
    assert 0 < observations_by_field["live_timeout_ms"] <= admission.geometry.statement_timeout_ms
    assert (
        await database.scalar(f'SELECT generation_id FROM "{_SCHEMA}".provider_directory_profile')
        == build.generation_id
    )
    receipt = await database.scalar(f'SELECT payload FROM "{_SCHEMA}".{contract.RECEIPT_TABLE}')
    assert receipt["serving"]["profile_rows"] == receipt["serving"]["evidence_rows"] == 1
    if composite:
        assert observations_by_field["live_timeout_ms"] == 8000
        assert await database.scalar(
            "SELECT CAST(CAST(:start AS text) AS pg_lsn) >= CAST(CAST(:callback AS text) AS pg_lsn)",
            start=receipt["actual"]["wal_start_lsn"],
            callback=observations_by_field["callback_lsn"],
        )


@pytest.mark.parametrize("composite", [False, True], ids=["ordinary", "composite_callback"])
def test_native_initial_census_preserves_readers_and_fences_writers(monkeypatch, composite):
    """Fresh census, real swaps and immutable receipt share one owner transaction."""

    async def exercise():
        async with _database(monkeypatch) as engine:
            database, admission, build, stages = await _reader_fixture(engine, monkeypatch)
            census_started, release = asyncio.Event(), asyncio.Event()
            real_metrics = fhir._provider_directory_profile_stage_metrics
            observations_by_field = {}

            real_live_lock = fhir._lock_provider_directory_artifact_live_tables

            async def lock_live(*args):
                observations_by_field["live_timeout_ms"] = (await fhir._profile_capacity_observed_settings())[
                    "statement_timeout_ms"
                ]
                return await real_live_lock(*args)

            monkeypatch.setattr(fhir, "_lock_provider_directory_artifact_live_tables", lock_live)

            async def census(*args):
                observations_by_field["preparation_timeout_ms"] = (await fhir._profile_capacity_observed_settings())[
                    "statement_timeout_ms"
                ]
                census_started.set()
                await release.wait()
                observations_by_field["counts"] = await real_metrics(*args)
                return observations_by_field["counts"]

            monkeypatch.setattr(fhir, "_provider_directory_profile_stage_metrics", census)
            execution = SimpleNamespace(
                generation=7,
                attestation=SimpleNamespace(
                    profile_schema_version=1, profile_strategy_version=admission.geometry.profile_strategy_version
                ),
            )
            token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
            execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
            owner = asyncio.create_task(_publish_census(database, admission, stages, composite, observations_by_field))
            try:
                await _observe_census(engine, census_started, release)
                await asyncio.wait_for(owner, 5)
                await _assert_census_commit(database, admission, build, composite, observations_by_field)
            finally:
                release.set()
                if not owner.done():
                    owner.cancel()
                await asyncio.gather(owner, return_exceptions=True)
                fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
                fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    asyncio.run(exercise())
