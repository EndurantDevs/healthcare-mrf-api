# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native archive staging parity, guarded merge, and exact failure cleanup."""

import datetime
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_cms_archive as archive
from process import provider_directory_cms_native_inputs as native_inputs
from process import provider_directory_cms_preparation as preparation
from process.ext import address_canon
from tests import test_provider_directory_cms_native_inputs_postgres as input_proof
from tests import test_provider_directory_cms_overlay_projection_postgres as overlay
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_address_format_db import _load_migration
from tests.test_provider_directory_cms_preparation import _admission, _signed_lease
from tests.test_provider_directory_cms_serving_receipt_postgres import _apply
from tests.test_provider_directory_practitioner_address_overlay_db import _canonical_migration

fhir = importlib.import_module("process.provider_directory_fhir")
_VOLATILE = "ARRAY['first_seen_at','last_seen_at','geocoded_at']::text[]"


async def _rows(database, schema, name):
    """Compare every semantic archive field, excluding transaction-clock metadata only."""
    return [
        row[0]
        for row in await database.all(
            f"SELECT to_jsonb(archived)-{_VOLATILE} FROM {fhir._unscoped_qt(schema, name)} archived ORDER BY address_key"
        )
    ]


async def _resolve(database, schema, source, target, *, bit=1, priority=4):
    """Exercise the actual canonical resolver with observed ZIP lineage."""
    return await address_canon.resolve_into_archive(
        source,
        archive._SOURCE_FIELDS,
        schema=schema,
        archive_table=target,
        source_bit=bit,
        priority=priority,
        strict_source_predicate="TRUE",
    )


async def _install_archive_schema(database, schema):
    """Install canonical functions, archive columns, revision guards and the write seal."""
    foundation = _canonical_migration()
    async with database.engine.begin() as connection:
        await connection.run_sync(
            lambda sync: foundation._exec_sql_batch(sync, foundation._create_functions_sql(schema))
        )
        await connection.run_sync(lambda sync: foundation._exec_sql_batch(sync, foundation._create_archive_sql(schema)))
        await connection.run_sync(lambda sync: _apply(sync, "20260930120000"))
        await connection.run_sync(lambda sync: _apply(sync, "20260930130000"))
    formatter = _load_migration()
    await database.status(formatter._humanize_component_function_sql(schema))
    await database.status(formatter._formatted_address_function_sql(schema))
    await database.status(
        f"ALTER TABLE {schema}.address_archive_v2 ADD COLUMN strict_source_bits integer NOT NULL DEFAULT 0"
    )
    await database.status(f"ALTER TABLE {schema}.address_archive_v2 ADD COLUMN formatted_address_version smallint")
    await database.status(f"ALTER TABLE {schema}.address_archive_v2 ADD COLUMN formatted_address_source varchar(32)")
    await database.status(f"CREATE TYPE {schema}.address_archive_geo_source AS ENUM ('openaddresses')")
    await database.status(
        f"ALTER TABLE {schema}.address_archive_v2 ADD COLUMN geo_source {schema}.address_archive_geo_source"
    )


async def _seed_archive(database, schema):
    """Use real canonical functions, defaults, formatter, revision guards and write seal."""
    await _install_archive_schema(database, schema)
    await database.status(f"""CREATE TABLE {schema}.address_alias_v1 (
        source_address_key uuid PRIMARY KEY,target_address_key uuid,source_identity_key text,target_identity_key text,
        revoked_at timestamptz)""")
    await database.status(
        f"CREATE TABLE {schema}.address_alias_state_v1 (singleton boolean,schema_version int,active_ruleset_version int,generation bigint)"
    )
    await database.status(f"INSERT INTO {schema}.address_alias_state_v1 VALUES (true,2,1,3)")
    await database.status(f"""CREATE TABLE {schema}.seed_input (
        address_key uuid,first_line text,second_line text,city_name text,state_name text,postal_code text,country_code text)""")
    for line in ("20 Grid Road", "21 Target Road", "10 Café Road", "40 Zero Road", "70 Unaffected Road"):
        await database.status(
            f"""INSERT INTO {schema}.seed_input SELECT
            {schema}.addr_key_v1(:line,NULL,'Example','NY','10001','US'),:line,NULL,'Example','NY','10001','US'""",
            line=line,
        )
    await _resolve(database, schema, "seed_input", "address_archive_v2")
    await database.status(f"""UPDATE {schema}.address_archive_v2 SET
        first_line='Retained display',formatted_address='Retained label',display_priority=0
        WHERE address_key={schema}.addr_key_v1('10 Café Road',NULL,'Example','NY','10001','US')""")
    await database.status(f"""UPDATE {schema}.address_archive_v2 SET source_bits=128,strict_source_bits=0
        WHERE address_key={schema}.addr_key_v1('40 Zero Road',NULL,'Example','NY','10001','US')""")
    await database.status(f"""INSERT INTO {schema}.address_alias_v1 SELECT
        original.address_key,target.address_key,original.identity_key,target.identity_key,NULL
        FROM {schema}.address_archive_v2 original CROSS JOIN {schema}.address_archive_v2 target
        WHERE original.first_line='20 Grid Road' AND target.first_line='21 Target Road'""")
    await database.status(f"""CREATE TABLE {schema}.openaddresses_geocode (
        address_key uuid,lat numeric,long numeric,feature_id text,accuracy text)""")
    for line, lat, longitude in (("21 Target Road", 40, -75), ("30 Coordinates Road", 41, -74), ("40 Zero Road", 0, 0)):
        await database.status(
            f"""INSERT INTO {schema}.openaddresses_geocode SELECT
            {schema}.addr_key_v1(:line,NULL,'Example','NY','10001','US'),:lat,:longitude,'example-feature','exact'""",
            line=line,
            lat=lat,
            longitude=longitude,
        )
    async with database.transaction() as session:
        for name in ("address_archive_v2", "address_alias_v1", "openaddresses_geocode"):
            await native_inputs._register_relation(
                session, schema, schema, name, await archive._oid(fhir.db, schema, name)
            )


async def _typed_sources(database, schema):
    """Materialize the same desired typed inputs used by the actual full overlay builder."""
    await overlay._seed_resources(database, schema)
    await overlay._seed_incumbent(database, schema)
    fence = overlay._fence()
    for resource_type in overlay.projection._INPUT_TYPES.values():
        model = fhir.RESOURCE_MODELS_BY_TYPE[resource_type]
        await database.status(fhir._provider_directory_artifact_scope_table_sql(model, schema, model.__tablename__))
        await database.status(
            fhir._provider_directory_artifact_resource_insert_sql(model, schema, model.__tablename__),
            source_ids=[item.source_id for item in fence.datasets],
            dataset_ids=[item.dataset_id for item in fence.datasets],
            evidence_run_ids=[item.evidence_run_id for item in fence.datasets],
            resource_type=resource_type,
        )
    await database.status(f"""UPDATE {schema}.provider_directory_location SET address_key=
        {schema}.addr_key_v1(first_line,second_line,city_name,state_name,postal_code,country_code)::text""")
    return fence


@asynccontextmanager
async def _fixture(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setattr(address_canon, "db", database)
        await _seed_archive(database, schema)
        fence = await _typed_sources(database, schema)
        admission = _admission()
        admission.plan = replace(
            admission.plan,
            reservation_bytes=(("data", 10_000_000), ("temp", 10_000_000), ("wal", 10_000_000)),
            temp_file_limit_bytes_per_backend=10_000_000 // 1024 * 1024,
        )
        admission.lease = _signed_lease(admission.plan)
        admission._started = True
        checks = []

        async def approve(request):
            checks.append(request)
            for relation in request.relations:
                await archive.capture_archive_layout(fhir, relation)
            return preparation.NonprofileAdmissionReceipt(
                request.phase,
                request.lease.lease_digest,
                request.lease.reservation_id,
                request.plan.capacity_geometry_hash,
                request.relations,
                request.logging_relations,
            )

        async def clock():
            return admission.lease.max_build_deadline - datetime.timedelta(minutes=5)

        admission.check_phase = approve
        monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", clock)
        token = preparation._ACTIVE.set(admission)
        try:
            yield database, schema, fence, admission, checks
        finally:
            preparation._ACTIVE.reset(token)


async def _prepare(schema, fence):
    return await archive.prepare_archive_delta(fhir, schema, source_ids=fence.source_ids)


async def _baseline(database, schema, fence):
    """Run unchanged resolver and OA behavior against a complete incumbent copy."""
    await database.status(f"CREATE TABLE {schema}.baseline (LIKE {schema}.address_archive_v2 INCLUDING DEFAULTS)")
    await database.status(f"INSERT INTO {schema}.baseline SELECT * FROM {schema}.address_archive_v2")
    await database.status(f"ALTER TABLE {schema}.baseline ADD PRIMARY KEY (address_key)")
    await database.status(
        fhir.provider_directory_location_archive_stage_sql(schema, "baseline_input", source_ids=fence.source_ids),
        source_ids=list(fence.source_ids),
    )
    stats = await _resolve(
        database,
        schema,
        "baseline_input",
        "baseline",
        bit=fhir.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT,
        priority=fhir.PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY,
    )
    metrics_by_name = dict(stats.__dict__)
    metrics_by_name["openaddresses_coordinate_backfill_rows"] = await fhir._backfill_archive_openaddresses_coordinates(
        schema, "baseline_input", archive_table="baseline"
    )
    return metrics_by_name


@pytest.mark.asyncio
async def test_seeded_delta_matches_full_archive_and_physical_overlay(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema, fence, admission, checks):
        before = await _rows(database, schema, "address_archive_v2")
        expected_metrics = await _baseline(database, schema, fence)
        prepared, metrics = await _prepare(schema, fence)
        try:
            assert await _rows(database, schema, "address_archive_v2") == before
            assert await archive._revision(fhir.db, schema, prepared.target_oid) == prepared.from_revision
            assert await _rows(database, schema, prepared.effective_relation) == await _rows(
                database, schema, "baseline"
            )
            for name in ("inserted", "provenance_updates", "openaddresses_coordinate_backfill_rows"):
                assert metrics[name] == expected_metrics[name]
            assert metrics["reason_buckets"] == expected_metrics["reason_buckets"]
            assert metrics["openaddresses_coordinate_backfill_rows"] == 1
            assert prepared.delta_rows < len(await _rows(database, schema, prepared.effective_relation))
            assert len(admission._relations) == 1
            assert any(check.phase == "pre_logging" for check in checks)
            for name, archive_name in (
                ("overlay_expected", "baseline"),
                ("overlay_staged", prepared.effective_relation),
            ):
                await database.status(
                    f"CREATE UNLOGGED TABLE {schema}.{name} (LIKE {schema}.provider_directory_address_overlay INCLUDING DEFAULTS)"
                )
                with fhir._provider_directory_artifact_relation_scope({"address_archive_v2": archive_name}):
                    await fhir._populate_address_overlay_stage(
                        schema,
                        name,
                        f"{schema}.{name}",
                        None,
                        list(fence.source_ids),
                        {"source_ids": list(fence.source_ids)},
                    )
            assert await database.scalar(f"""SELECT NOT EXISTS (
                (SELECT to_jsonb(row_data)-'published_at' FROM {schema}.overlay_expected row_data
                 EXCEPT SELECT to_jsonb(row_data)-'published_at' FROM {schema}.overlay_staged row_data)
                UNION ALL
                (SELECT to_jsonb(row_data)-'published_at' FROM {schema}.overlay_staged row_data
                 EXCEPT SELECT to_jsonb(row_data)-'published_at' FROM {schema}.overlay_expected row_data))""")
        finally:
            await prepared.cleanup(fhir)
        assert not admission._relations


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_common_merge_preserves_oid_and_rolls_back_as_one_transaction(monkeypatch, empty):
    async with _fixture(monkeypatch) as (database, schema, fence, admission, _checks):
        if empty:
            await database.status(f"DELETE FROM {schema}.provider_directory_location")
            await database.status(f"DELETE FROM {schema}.provider_directory_organization")
        prepared, _metrics = await _prepare(schema, fence)
        before = await _rows(database, schema, "address_archive_v2")
        try:
            with pytest.raises(RuntimeError, match="write_lock_required"):
                async with database.transaction() as session:
                    await prepared.apply(fhir, session)
            with pytest.raises(RuntimeError, match="synthetic rollback"):
                async with database.transaction() as session:
                    await prepared.before_lock(fhir, session)
                    publication_by_field = await prepared.apply(fhir, session)
                    assert publication_by_field["to_revision"] == publication_by_field["from_revision"] + 2
                    raise RuntimeError("synthetic rollback")
            assert await _rows(database, schema, "address_archive_v2") == before
            assert await archive._revision(fhir.db, schema, prepared.target_oid) == prepared.from_revision
            async with database.transaction() as session:
                await prepared.before_lock(fhir, session)
                publication_by_field = await prepared.apply(fhir, session)
            assert await archive._oid(fhir.db, schema, "address_archive_v2") == prepared.target_oid
            assert await _rows(database, schema, "address_archive_v2") == await _rows(
                database, schema, prepared.effective_relation
            )
            await prepared.mark_committed(fhir, publication_by_field)
            with pytest.raises(RuntimeError, match="preparation_changed"):
                async with database.transaction() as session:
                    await prepared.before_lock(fhir, session)
        finally:
            await prepared.cleanup(fhir)
        assert not admission._relations


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["alias_identity", "missing_target", "merged_target", "stamped_key"])
async def test_invalid_inputs_reject_without_live_mutation_or_leaked_scratch(monkeypatch, corruption):
    async with _fixture(monkeypatch) as (database, schema, fence, admission, _checks):
        statements_by_corruption = {
            "alias_identity": f"UPDATE {schema}.address_alias_v1 SET source_identity_key='wrong'",
            "missing_target": f"UPDATE {schema}.address_alias_v1 SET target_address_key='00000000-0000-0000-0000-000000000099'",
            "merged_target": f"UPDATE {schema}.address_archive_v2 SET merged_into=address_key WHERE first_line='21 Target Road'",
            "stamped_key": f"UPDATE {schema}.provider_directory_location SET address_key='00000000-0000-0000-0000-000000000099'",
        }
        await database.status(statements_by_corruption[corruption])
        before = await _rows(database, schema, "address_archive_v2")
        revision = await archive._revision(fhir.db, schema, await archive._oid(fhir.db, schema, "address_archive_v2"))
        with pytest.raises(RuntimeError, match="(alias integrity|Stamped canonical)"):
            await _prepare(schema, fence)
        assert await _rows(database, schema, "address_archive_v2") == before
        assert (
            await archive._revision(fhir.db, schema, await archive._oid(fhir.db, schema, "address_archive_v2"))
            == revision
        )
        assert not admission._relations
        assert (
            await database.scalar(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=:schema AND c.relname LIKE 'cms_archive_%'",
                schema=schema,
            )
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["live_write", "view", "seal", "delta_swap"])
async def test_prepared_identity_drift_fails_closed_and_cleanup_preserves_replacements(monkeypatch, corruption):
    async with _fixture(monkeypatch) as (database, schema, fence, admission, _checks):
        prepared, _metrics = await _prepare(schema, fence)
        try:
            with pytest.raises(DBAPIError, match="prepared_read_only"):
                await database.status(f"UPDATE {schema}.{prepared.delta_table} SET first_line=first_line")
            statements_by_corruption = {
                "live_write": f"UPDATE {schema}.address_archive_v2 SET first_line=first_line WHERE false",
                "view": f"CREATE OR REPLACE VIEW {schema}.{prepared.effective_relation} AS SELECT * FROM {schema}.address_archive_v2",
                "seal": f"ALTER TABLE {schema}.{prepared.delta_table} DISABLE TRIGGER {archive._SEAL}",
                "delta_swap": f"ALTER TABLE {schema}.{prepared.delta_table} RENAME TO retired_delta",
            }
            await database.status(statements_by_corruption[corruption])
            if corruption == "delta_swap":
                await database.status(f"CREATE TABLE {schema}.{prepared.delta_table} (value text)")
            with pytest.raises(RuntimeError, match="(preparation_changed|write_seal_changed)"):
                await prepared.assert_ready(fhir)
        finally:
            await prepared.cleanup(fhir)
        if corruption == "delta_swap":
            assert await archive._oid(fhir.db, schema, prepared.delta_table) is not None
            assert (schema, prepared.delta_table) in admission.cleanup_preserved
        else:
            assert not admission._relations


@pytest.mark.asyncio
async def test_actual_archive_dispatch_routes_overlay_and_requires_prepared_completion(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema, fence, _admission_value, _checks):

        async def overlay_builder(**_options):
            effective_ref = fhir._qt(schema, "address_archive_v2")
            assert "cms_archive_" in effective_ref and effective_ref.endswith('_effective"')
            assert await database.scalar(f"SELECT count(*) FROM {effective_ref}") > 0
            return {"rows": 1}

        async def progress(*_args, **_options):
            return None

        monkeypatch.setattr(fhir, "publish_provider_directory_address_overlay", overlay_builder)
        monkeypatch.setattr(fhir, "_mark_provider_directory_progress", progress)
        before = await _rows(database, schema, "address_archive_v2")
        async with fhir._provider_directory_artifact_bundle_scope() as bundle:
            metrics = await fhir._publish_provider_directory_artifacts(
                fhir.ProviderDirectoryArtifactPublishRequest(
                    run_id=None,
                    metrics={},
                    source_ids=list(fence.source_ids),
                    publish_corroboration=False,
                    publish_artifacts_targets={"location_archive", "address_overlay"},
                )
            )
            assert isinstance(bundle.archive_delta, archive.PreparedArchiveDelta)
            assert bundle.relation_overrides == bundle.archive_delta.relation_overrides
            fhir._assert_candidate_artifact_bundle_complete(
                fence, metrics, bundle, publish_corroboration=False, publish_artifacts_targets={"location_archive"}
            )
            with pytest.raises(RuntimeError, match="archive_preparation_missing"):
                fhir._assert_candidate_artifact_bundle_complete(
                    fence,
                    metrics,
                    fhir.ProviderDirectoryArtifactBundle(),
                    publish_corroboration=False,
                    publish_artifacts_targets={"location_archive"},
                )
            with pytest.raises(RuntimeError, match="common_publication_required"):
                await bundle.promote()
            assert await _rows(database, schema, "address_archive_v2") == before


@pytest.mark.asyncio
async def test_unadmitted_cms_bundle_fails_before_any_live_archive_statement(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema, fence, _admission_value, _checks):
        before = await _rows(database, schema, "address_archive_v2")
        oid = await archive._oid(database, schema, "address_archive_v2")
        revision = await archive._revision(database, schema, oid)
        token = preparation._ACTIVE.set(None)
        try:
            async with fhir._provider_directory_artifact_bundle_scope():
                with pytest.raises(RuntimeError, match="nonprofile_admission_required"):
                    await fhir._publish_provider_directory_artifacts(
                        fhir.ProviderDirectoryArtifactPublishRequest(
                            run_id=None,
                            metrics={},
                            source_ids=list(fence.source_ids),
                            publish_corroboration=False,
                            publish_artifacts_targets={"location_archive"},
                        )
                    )
        finally:
            preparation._ACTIVE.reset(token)
        assert await _rows(database, schema, "address_archive_v2") == before
        assert await archive._revision(database, schema, oid) == revision


@pytest.mark.asyncio
async def test_optional_coordinate_heap_presence_and_mutation_are_native_inputs(monkeypatch):
    async with input_proof._fixture(monkeypatch) as (database, schema):
        await database.status(f"DROP TABLE {schema}.openaddresses_geocode")
        absent = await input_proof._capture(database, schema)
        await database.status(f"CREATE TABLE {schema}.openaddresses_geocode (value int)")
        with pytest.raises(RuntimeError, match="revision_guard_unavailable"):
            await input_proof._capture(database, schema)
        async with database.engine.begin() as connection:
            await native_inputs.register_native_address_inputs(connection, schema)
        present = await input_proof._capture(database, schema)
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await input_proof._assert(database, schema, absent)
        await database.status(f"UPDATE {schema}.openaddresses_geocode SET value=1 WHERE false")
        with pytest.raises(RuntimeError, match="native_inputs_changed"):
            await input_proof._assert(database, schema, present)


async def _assert_view_locked(database, schema, name):
    """Prove the reader holds the view definition on its actual backend."""
    async with database.engine.begin() as other:
        await other.execute(text("SET LOCAL lock_timeout='50ms'"))
        with pytest.raises(DBAPIError):
            await other.execute(
                text(f"CREATE OR REPLACE VIEW {schema}.{name} AS SELECT * FROM {schema}.address_archive_v2")
            )


@pytest.mark.asyncio
async def test_native_acquired_backend_guard_and_applied_owner_are_exact(monkeypatch):
    async with _fixture(monkeypatch) as (database, schema, fence, _admission_value, _checks):
        prepared, _metrics = await _prepare(schema, fence)
        try:
            with pytest.raises(RuntimeError, match="read_requires_owner"):
                await prepared.lock_read_backend(database)
            async with database.acquire() as backend:
                await prepared.lock_read_backend(backend)
                assert await backend.scalar(f"SELECT count(*) FROM {schema}.{prepared.effective_relation}") > 0
                await _assert_view_locked(database, schema, prepared.effective_relation)
            async with database.transaction() as session:
                await prepared.before_lock(fhir, session)
                await prepared.apply(fhir, session)
                await prepared.assert_applied_backend(database)
            with pytest.raises(RuntimeError, match="applied_owner_changed"):
                async with database.transaction():
                    await prepared.assert_applied_backend(database)
        finally:
            await prepared.cleanup(fhir)
