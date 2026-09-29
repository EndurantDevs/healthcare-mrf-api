# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native bulk-before-lock, composite lock-order, and split WAL boundary proofs."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from process import provider_directory_cms_publication as publication
from process.provider_directory_artifact_bundle_preparation import apply_prepared_artifact_bundle
from tests.provider_directory_profile_delta_publication import (
    _assert_delta_publication,
    _delta_capacity_admission,
    _prepared_scenario_delta,
)
from tests.provider_directory_profile_delta_scenario import (
    _delta_capacity_context,
    _delta_lineage,
    _delta_relation_oid_by_name,
    _delta_relation_scenario,
    _insert_delta_checkpoint,
    _insert_delta_serving_generation,
)
from tests.provider_directory_profile_delta_test_support import _delta_database, _geometry_payload, capacity
from tests.test_provider_directory_artifact_cutover import _keep_stage_indexes, _prepared_bundle_stage, importer


@asynccontextmanager
async def _prepared_delta(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        scenario = await _delta_relation_scenario(database, schema)
        lineage = _delta_lineage()
        oid_by_name = await _delta_relation_oid_by_name(database, scenario)
        geometry = capacity.validated_capacity_geometry(
            _geometry_payload(
                evidence_target_oid=oid_by_name["evidence_target"], profile_target_oid=oid_by_name["profile_target"]
            )
        )
        await _insert_delta_checkpoint(database, scenario, lineage, oid_by_name, geometry)
        await _insert_delta_serving_generation(database, scenario, lineage, oid_by_name)
        context = await _delta_capacity_context(database, scenario, lineage, oid_by_name)
        await database.status(
            f"UPDATE {scenario.checkpoint_ref} SET capacity_geometry_hash=:hash, "
            "capacity_geometry_json=CAST(:geometry AS jsonb) WHERE build_id=:build_id",
            hash=capacity.capacity_geometry_hash(context.geometry),
            geometry=capacity.canonical_capacity_geometry_json(context.geometry),
            build_id=lineage.build_id,
        )
        delta = _prepared_scenario_delta(scenario, lineage, oid_by_name, context.geometry)
        admission = await _delta_capacity_admission(database, lineage, context)
        token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
        fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
            importer.ProviderDirectoryArtifactDatasetFence(())
        )
        try:
            yield SimpleNamespace(
                database=database,
                schema=schema,
                scenario=scenario,
                lineage=lineage,
                delta=delta,
                admission=admission,
                oid_by_name=oid_by_name,
            )
        finally:
            importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
            importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


async def _companion_relations(fixture):
    for name in ("entity_address_unified", "provider_directory_address_overlay"):
        for suffix, marker in (("", "old"), ("_next", "new")):
            await fixture.database.status(f'CREATE TABLE "{fixture.schema}".{name}{suffix} (marker text)')
            await fixture.database.status(
                f'INSERT INTO "{fixture.schema}".{name}{suffix} VALUES (:marker)', marker=marker
            )
    stage = await _prepared_bundle_stage(
        fixture.schema,
        "provider_directory_address_overlay_next",
        "provider_directory_address_overlay",
        _keep_stage_indexes,
    )
    address = SimpleNamespace(
        swaps=[SimpleNamespace(live_cls=SimpleNamespace(__main_table__="entity_address_unified"))]
    )
    return (stage,), address


async def _assert_bulk_precedes_live_locks(fixture, session):
    assert (
        await session.scalar(text(f"SELECT generation_id FROM {fixture.scenario.profile_target_ref}"))
        == fixture.delta.generation_id
    )
    assert (
        await session.scalar(text(f"SELECT generation_id FROM {fixture.scenario.serving_ref}"))
        == fixture.delta.from_generation_id
    )
    assert not await session.scalar(
        text("""SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid=l.relation
        WHERE l.pid=pg_backend_pid() AND l.mode='AccessExclusiveLock' AND l.granted
        AND c.relname IN ('entity_address_unified','provider_directory_address_overlay',
                         'provider_directory_profile','provider_directory_profile_evidence'))""")
    )
    async with fixture.database.session_factory() as reader:
        assert (
            await reader.scalar(text(f"SELECT generation_id FROM {fixture.scenario.profile_target_ref}"))
            == fixture.delta.from_generation_id
        )


@pytest.mark.parametrize("abort", [False, True])
async def test_real_delta_bulk_precedes_composite_locks_and_preserves_owner_rollback(monkeypatch, abort):
    async with _prepared_delta(monkeypatch) as fixture:
        stages, address = await _companion_relations(fixture)
        prepared = SimpleNamespace(stages=stages)
        observations = []

        async def before_swaps():
            session = fixture.database._transaction_binding().session
            await _assert_bulk_precedes_live_locks(fixture, session)
            await publication._lock_live_swap_relations(importer, session, prepared, address, None)
            observations.append("bulk-before-live-locks")

        try:
            async with fixture.database.transaction():
                forecast = await apply_prepared_artifact_bundle(
                    importer, stages, profile_delta=fixture.delta, before_swaps=before_swaps
                )
                assert (
                    await fixture.database.scalar(
                        f'SELECT marker FROM "{fixture.schema}".provider_directory_address_overlay'
                    )
                    == "new"
                )
                await importer._validate_profile_delta_total_wal(fixture.admission, forecast)
                if abort:
                    raise RuntimeError("synthetic late owner failure")
        except RuntimeError as error:
            assert abort and str(error) == "synthetic late owner failure"
        assert observations == ["bulk-before-live-locks"]
        if abort:
            assert (
                await fixture.database.scalar(f"SELECT generation_id FROM {fixture.scenario.profile_target_ref}")
                == fixture.delta.from_generation_id
            )
            assert (
                await fixture.database.scalar(
                    f'SELECT marker FROM "{fixture.schema}".provider_directory_address_overlay'
                )
                == "old"
            )
            assert await _delta_relation_oid_by_name(fixture.database, fixture.scenario) == fixture.oid_by_name
        else:
            await _assert_delta_publication(fixture.database, fixture.scenario, fixture.lineage, fixture.delta)


async def _await_queued_lock(observer, writer_pid):
    async with asyncio.timeout(2):
        while not await observer.scalar(
            text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid=:pid AND NOT granted)"), {"pid": writer_pid}
        ):
            await asyncio.sleep(0.001)


async def test_combined_live_lock_order_prevents_cross_family_reader_cycle(monkeypatch):
    """Force the absent-Doctors address/artifact interleaving against actual PostgreSQL locks."""
    async with _delta_database(monkeypatch) as (database, schema):
        fixture = SimpleNamespace(database=database, schema=schema)
        monkeypatch.setattr(importer, "db", database)
        stages, address = await _companion_relations(fixture)
        ready = asyncio.Event()
        writer_pids = []

        async def publish():
            async with database.transaction() as session:
                writer_pids.append(await session.scalar(text("SELECT pg_backend_pid()")))
                ready.set()
                await publication._lock_live_swap_relations(
                    importer, session, SimpleNamespace(stages=stages), address, None
                )
                for name in ("entity_address_unified", "provider_directory_address_overlay"):
                    await session.execute(text(f'ALTER TABLE "{schema}".{name} RENAME TO {name}_old'))
                    await session.execute(text(f'ALTER TABLE "{schema}".{name}_next RENAME TO {name}'))

        async def late_read():
            async with database.session_factory() as reader, reader.begin():
                await reader.execute(text("SET LOCAL lock_timeout='250ms'"))
                await reader.execute(
                    text(
                        f'LOCK TABLE "{schema}".entity_address_unified, "{schema}".provider_directory_address_overlay IN ACCESS SHARE MODE'
                    )
                )
                return tuple(
                    [
                        await reader.scalar(text(f'SELECT marker FROM "{schema}".{name}'))
                        for name in ("entity_address_unified", "provider_directory_address_overlay")
                    ]
                )

        tasks = []
        try:
            async with database.session_factory() as first, first.begin(), database.session_factory() as observer:
                await first.execute(
                    text(f'LOCK TABLE "{schema}".provider_directory_address_overlay IN ACCESS SHARE MODE')
                )
                tasks.append(asyncio.create_task(publish()))
                await ready.wait()
                await _await_queued_lock(observer, writer_pids[0])
                tasks.append(asyncio.create_task(late_read()))
                await asyncio.sleep(0.01)
            assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [None, ("new", "new")]
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _wal_since(database, start):
    return int(
        await database.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
            start=start,
        )
    )


@pytest.mark.parametrize("kind", ["VIEW", "MATERIALIZED VIEW"])
@pytest.mark.parametrize("composite", [False, True])
async def test_live_prelocks_preserve_legacy_view_conversion(monkeypatch, kind, composite):
    """Legacy view replacement must not issue unsupported or recursive table locks."""
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        target_name = importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
        await database.status(f'CREATE {kind} "{schema}".{target_name} AS SELECT 1 AS marker')
        await database.status(f'CREATE TABLE "{schema}".replacement (marker integer)')
        await database.status(f'INSERT INTO "{schema}".replacement VALUES (2)')
        stage = await _prepared_bundle_stage(schema, "replacement", target_name, _keep_stage_indexes)

        async def before_swaps():
            session = database._transaction_binding().session
            await publication._lock_live_swap_relations(importer, session, SimpleNamespace(stages=(stage,)), None, None)

        async with database.transaction():
            await apply_prepared_artifact_bundle(importer, (stage,), before_swaps=before_swaps if composite else None)
        assert await database.scalar(f'SELECT marker FROM "{schema}".{target_name}') == 2
        assert await importer._provider_directory_relation_attribute(schema, target_name, "relkind") == "r"


@pytest.mark.parametrize("overrun", [None, "bulk", "metadata", "final"])
async def test_profile_projection_counts_both_spans_and_final_budget_counts_intervening_writes(monkeypatch, overrun):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        await database.status(f'CREATE TABLE "{schema}".wal_probe (payload text)')
        try:
            async with database.transaction():
                start = await importer._profile_delta_target_wal_start_lsn()
                await database.status(f"INSERT INTO \"{schema}\".wal_probe VALUES ('bulk')")
                projected_bulk_bytes = await _wal_since(database, start)
                if overrun == "bulk":
                    await database.status(
                        f'INSERT INTO "{schema}".wal_probe SELECT md5(i::text) FROM generate_series(1,2048) i'
                    )
                bulk_bytes = await _wal_since(database, start)
                await database.status(
                    f'INSERT INTO "{schema}".wal_probe SELECT md5(i::text) FROM generate_series(1,2048) i'
                )
                metadata_start = await importer._profile_delta_target_wal_start_lsn()
                await database.status(f"INSERT INTO \"{schema}\".wal_probe VALUES ('metadata')")
                metadata_bytes = await _wal_since(database, metadata_start)
                projection = projected_bulk_bytes + metadata_bytes + 8192
                assert await _wal_since(database, start) > projection
                forecast = SimpleNamespace(
                    wal_start_lsn=start,
                    target_projection=SimpleNamespace(wal_bytes=projection),
                    metadata_projection=SimpleNamespace(wal_bytes=0, commit_envelope_bytes=8192),
                )
                admission = SimpleNamespace(
                    initial_wal_lsn=start, geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 10**8})
                )
                if overrun == "metadata":
                    await database.status(
                        f'INSERT INTO "{schema}".wal_probe SELECT md5(i::text) FROM generate_series(1,2048) i'
                    )
                await importer._validate_profile_delta_final_wal(
                    admission,
                    forecast,
                    metadata_wal_start_lsn=metadata_start,
                    bulk_wal_bytes=bulk_bytes,
                )
                if overrun == "final":
                    admission.geometry.reservation_bytes_by_storage_class["wal"] = (
                        await _wal_since(database, start) + 8192
                    )
                    await database.status(f"INSERT INTO \"{schema}\".wal_probe VALUES ('late receipt')")
                await importer._validate_profile_delta_total_wal(admission, forecast)
        except RuntimeError as error:
            assert overrun and ("final_wal_exceeded" if overrun == "final" else "cutover_wal_exceeded") in str(error)
            assert await database.scalar(f'SELECT count(*) FROM "{schema}".wal_probe') == 0
        else:
            assert overrun is None
