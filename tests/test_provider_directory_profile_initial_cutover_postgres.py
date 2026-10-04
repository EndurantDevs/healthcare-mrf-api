# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native initial swap and metadata operation proof with seeded authority ledgers.

This exercises production cutover operations, not Linux runtime admission.
"""

import asyncio
import importlib
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import Database
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as contract
from tests import test_provider_directory_profile_initial_migration as fixture
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input
from tests.test_provider_directory_profile_failed_cleanup_postgres import _server_tablespace

fhir = importlib.import_module("process.provider_directory_fhir")


def _initial_tablespace_identity():
    """Return exact native fixture coordinates before resource creation."""
    return "hc_initial_test_" + uuid4().hex


@pytest.mark.parametrize("object_kind", ["heap", "index"])
def test_native_initial_receipt_refuses_unfunded_tablespace(monkeypatch, object_kind):
    async def exercise():
        async with fixture._database(monkeypatch) as engine:
            await fixture._upgrade(engine)
            database = Database()
            database.engine = engine
            database.session_factory = async_sessionmaker(engine, expire_on_commit=False)
            monkeypatch.setattr(fhir, "db", database)
            before = await initial.receipt_layout(fhir, fixture._SCHEMA)
            funded_oid = await database.scalar("SELECT dattablespace FROM pg_database WHERE datname=current_database()")
            index_name = await database.scalar(
                "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                "WHERE i.indrelid=CAST(:oid AS oid) ORDER BY c.relname LIMIT 1",
                oid=before.relation_oid,
            )
            metadata_ref = (
                f'TABLE "{fixture._SCHEMA}".{contract.RECEIPT_TABLE}'
                if object_kind == "heap"
                else f'INDEX "{fixture._SCHEMA}"."{index_name}"'
            )
            name = _initial_tablespace_identity()
            async with _server_tablespace(database, name, metadata_ref):
                observed = await initial.receipt_layout(fhir, fixture._SCHEMA)
                assert observed.exact_fingerprint != before.exact_fingerprint
                # Even a fresh fingerprint cannot fund storage on an unreserved volume.
                with pytest.raises(RuntimeError, match="initial_receipt_tablespace_unsupported"):
                    initial.geometry_inputs(fhir, None, SimpleNamespace(tablespace_oid=funded_oid), observed)
                assert await database.scalar(f'SELECT count(*) FROM "{fixture._SCHEMA}".{contract.RECEIPT_TABLE}') == 0

    asyncio.run(exercise())


async def _create_cutover_tables(engine, schema, consumed):
    # No import-run write or admission is exercised here; its twelve-guard proof is separate.
    async with engine.begin() as connection:
        await connection.execute(text(f'ALTER TABLE "{schema}".import_run ADD COLUMN status text'))
        await connection.execute(
            text(f"INSERT INTO \"{schema}\".import_run VALUES (:run,'running')"), {"run": consumed["run_id"]}
        )
        for table, evidence in (("initial_evidence_stage", True), ("initial_profile_stage", False)):
            statement = (
                fhir.profile_artifact.profile_evidence_table_sql
                if evidence
                else fhir.profile_artifact.profile_table_sql
            )
            await connection.execute(text(statement(schema, table, logged=True)))
            for sql in fhir.profile_artifact.profile_index_statements(schema, table, evidence=evidence):
                await connection.execute(text(sql))
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from process import provider_directory_cms_receipt_guard as common_guards
    from tests.test_provider_directory_cms_serving_receipt_postgres import _migration

    async with engine.begin() as connection:

        def create_common(sync):
            with Operations.context(MigrationContext.configure(sync)):
                _migration("20260930100000")._create_table('"' + schema + '"')

        await connection.run_sync(create_common)
        for name, mask, body in (
            (
                "cms_serving_profile_transition",
                "AFTER INSERT OR UPDATE OR DELETE",
                common_guards.profile_transition_body('"' + schema + '"'),
            ),
            (
                "cms_serving_no_truncate",
                "BEFORE TRUNCATE",
                f"BEGIN IF EXISTS (SELECT 1 FROM \"{schema}\".provider_directory_cms_serving_receipt) THEN RAISE EXCEPTION 'cms_serving_native_truncate_forbidden'; END IF; RETURN NULL; END",
            ),
        ):
            await connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".{name}() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $body${body}$body$'
                )
            )
            trigger = (
                f'CREATE CONSTRAINT TRIGGER {name} {mask} ON "{schema}".provider_directory_profile_serving_generation DEFERRABLE INITIALLY DEFERRED FOR EACH ROW'
                if name.endswith("transition")
                else f'CREATE TRIGGER {name} {mask} ON "{schema}".provider_directory_profile_serving_generation FOR EACH STATEMENT'
            )
            await connection.execute(text(trigger + f' EXECUTE FUNCTION "{schema}".{name}()'))


async def _bind_fixture_geometry(consumed, preflight, receipt_payload, schema):
    raw = receipt_payload["capacity_geometry"]
    raw.update(
        executable_plan_hash=consumed["executable_plan_hash"],
        selection_proof_id=consumed["selection_proof_id"],
        desired_source_vector_hash=consumed["source_vector_hash"],
        desired_context_vector_hash=consumed["source_context_vector_hash"],
    )
    raw["physical_projection_contract_id"] = capacity.BOUNDED_ADMISSION_CONTRACT_ID
    raw["serving_generation_oid"] = await fhir._provider_directory_relation_oid(
        schema, "provider_directory_profile_serving_generation"
    )
    geometry = capacity.validated_capacity_geometry(raw)
    receipt_layout = await initial.receipt_layout(fhir, schema)
    geometry = replace(geometry, initial_receipt_storage_fingerprint=receipt_layout.exact_fingerprint)
    plan = _control_wal_plan_input(admission_row_lock_count=fhir.PROVIDER_DIRECTORY_PROFILE_ADMISSION_ROW_LOCK_COUNT)
    geometry = replace(geometry, control_wal_plan_input_hash=capacity.profile_control_wal_plan_input_hash(plan))
    projection = capacity.project_profile_control_wal_capacity(geometry, plan)
    geometry = replace(
        geometry,
        control_wal_upper_bound_bytes=projection.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projection.total_control_metadata_data_bytes,
    )
    projection = capacity.project_profile_control_wal_capacity(geometry, plan)
    geometry_hash = capacity.capacity_geometry_hash(geometry)
    raw = capacity.capacity_geometry_payload(geometry)
    consumed["capacity_geometry_hash"] = geometry_hash
    preflight["capacity_geometry_hash"] = geometry_hash
    preflight_payload = json.loads(preflight["receipt_json"])
    preflight_payload["capacity_geometry"] = raw
    preflight["receipt_json"] = json.dumps(preflight_payload)
    envelope = json.loads(consumed["canonical_lease_json"])
    envelope["signing_preflight_guard"]["healthcare_receipt"] = preflight_payload
    consumed["canonical_lease_json"] = json.dumps(envelope)
    return geometry, receipt_layout, projection, geometry_hash, raw


async def _prepare_fixture_stages(
    engine, build, geometry, admission, receipt_layout, extra_by_field, raw, geometry_hash
):
    """Prepare the original stage identities, metadata forecast and deferred cutover bundle."""
    schema = build.schema
    oid_by_target = {
        name: await fhir._provider_directory_relation_oid(schema, stage)
        for name, stage in (
            (fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, build.evidence_stage),
            (fhir.profile_artifact.PROFILE_TABLE, build.profile_stage),
        )
    }
    # A real checkpoint row is retired by the production operation in the owner transaction.
    await _seed_cutover_checkpoint(engine, schema, build, geometry, oid_by_target)
    metadata_projection = await initial._metadata_projection(fhir, admission, receipt_layout, 4)
    forecast_by_field = {
        "contract_id": contract.FORECAST_CONTRACT,
        "build_id": build.build_id,
        "capacity_geometry_hash": geometry_hash,
        "initial_target_state_sha256": geometry.initial_target_state_sha256,
        "dataset_fence_sha256": initial._fence_hash(fhir, fhir.ProviderDirectoryArtifactDatasetFence(())),
        "new_target_oids": oid_by_target,
        "metadata_data_bytes": metadata_projection.data_bytes,
        "metadata_wal_bytes": metadata_projection.wal_bytes,
        "commit_envelope_bytes": metadata_projection.commit_envelope_bytes,
        "ddl_statement_upper_bound": contract.INITIAL_CUTOVER_STATEMENTS,
    }
    cutover_by_field = {
        "build": build,
        "admission": admission,
        "counts": {"evidence_rows": 0, "profile_rows": 0},
        "oids": oid_by_target,
        "projection": metadata_projection,
        "forecast": forecast_by_field,
        "forecast_hash": fhir._identity_hash(forecast_by_field),
        "wal_start": admission.initial_wal_lsn,
        **{key: column_value for key, column_value in extra_by_field.items() if key != "build"},
    }
    token = initial.REQUESTED.set(True)
    try:
        stages = fhir._prepare_profile_full_swap_stages(
            build,
            fhir.ProviderDirectoryArtifactBuildFence(raw["evidence_target_oid"]),
            fhir.ProviderDirectoryArtifactBuildFence(raw["profile_target_oid"]),
        )
    finally:
        initial.REQUESTED.reset(token)
    return cutover_by_field, stages


async def _cutover_fixture(engine, monkeypatch, prepare_authority=None):
    """Prepare seeded native cutover state without claiming runtime admission."""
    await fixture._upgrade(engine)
    database = Database()
    database.engine = engine
    database.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(fhir, "db", database)
    consumed, preflight, serving, receipt_payload = await fixture._publication_values(engine)
    schema = fixture._SCHEMA
    await _create_cutover_tables(engine, schema, consumed)
    geometry, receipt_layout, projection, geometry_hash, raw = await _bind_fixture_geometry(
        consumed, preflight, receipt_payload, schema
    )
    extra_by_field = {}
    if prepare_authority is not None:
        consumed, preflight, geometry, projection, extra_by_field = await prepare_authority(
            database, engine, consumed, preflight, geometry, projection
        )
        geometry_hash = capacity.capacity_geometry_hash(geometry)
        raw = capacity.capacity_geometry_payload(geometry)
    await fixture._seed_authority(engine, consumed, preflight)
    admission = await _fixture_admission(database, geometry, projection, consumed, preflight)
    if extra_by_field:
        admission = replace(
            admission, lease=extra_by_field["verified_lease"], database_identity=extra_by_field["database_identity"]
        )
    build = fhir._ProviderDirectoryProfileBuild(
        schema=schema,
        generation_id=serving["generation_id"],
        source_ids=(),
        retained_source_ids=(),
        dataset_ids=(),
        profile_as_of=consumed["profile_as_of"],
        evidence_stage="initial_evidence_stage",
        profile_stage="initial_profile_stage",
        build_id=consumed["build_id"],
        owner_run_id=consumed["run_id"],
        selection_proof_id=consumed["selection_proof_id"],
        authority_revision=serving["authority_revision"],
        desired_source_vector_hash=consumed["source_vector_hash"],
        desired_source_context_vector_hash=consumed["source_context_vector_hash"],
        capacity_geometry_status="verified",
        capacity_geometry_hash=geometry_hash,
        capacity_geometry_json=capacity.canonical_capacity_geometry_json(geometry),
    )
    if extra_by_field:
        build = replace(build, **extra_by_field["build"])
    cutover_by_field, stages = await _prepare_fixture_stages(
        engine, build, geometry, admission, receipt_layout, extra_by_field, raw, geometry_hash
    )
    return database, cutover_by_field, stages


@pytest.mark.parametrize("failure", [False, True, "cancel"])
def test_native_initial_swap_metadata_and_rollback(monkeypatch, failure):
    """Exercise atomic initial publication and rollback through the real swap helpers."""

    async def exercise():
        """Keep the native fixture and capacity contexts alive through publication assertions."""
        async with fixture._database(monkeypatch) as engine:
            database, cutover, stages = await _cutover_fixture(engine, monkeypatch)
            admission = cutover["admission"]
            execution = SimpleNamespace(
                generation=7,
                attestation=SimpleNamespace(
                    profile_schema_version=1, profile_strategy_version=admission.geometry.profile_strategy_version
                ),
            )
            token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
            execution_token = fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(execution)
            before = await fhir._provider_directory_relation_oid(fixture._SCHEMA, fhir.profile_artifact.PROFILE_TABLE)
            try:
                publish = _initial_publication_case(database, admission, cutover, stages, engine, before, failure)

                if failure:
                    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
                        await publish()
                else:
                    await publish()
                assert await database.scalar(
                    f'SELECT count(*) FROM "{fixture._SCHEMA}".{contract.RECEIPT_TABLE}'
                ) == int(not failure)
                assert await database.scalar(
                    f'SELECT count(*) FROM "{fixture._SCHEMA}".provider_directory_profile_serving_generation'
                ) == int(not failure)
                assert await database.scalar(
                    f'SELECT count(*) FROM "{fixture._SCHEMA}".provider_directory_profile_build_checkpoint'
                ) == int(bool(failure))
                assert (
                    await fhir._provider_directory_relation_oid(fixture._SCHEMA, fhir.profile_artifact.PROFILE_TABLE)
                    == before
                ) is bool(failure)
            finally:
                fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(execution_token)
                fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    asyncio.run(exercise())


@pytest.mark.parametrize("state", ["empty", "legacy", "mixed", "newer_invalid"])
def test_native_initial_target_snapshot_and_evolved_legacy_context(monkeypatch, state):
    """Observe legacy time absence while rejecting mixed targets and newer invalid history."""
    from process import provider_directory_profile_selection as selection
    from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution

    async def exercise():
        """Capture the target snapshot inside the original native transaction."""
        async with fixture._database(monkeypatch) as engine:
            database, _cutover, _stages = await _cutover_fixture(engine, monkeypatch)
            schema = fixture._SCHEMA
            async with database.transaction():
                await database.status(
                    f'ALTER TABLE "{schema}".import_run ADD COLUMN importer text, ADD COLUMN finished_at timestamptz, ADD COLUMN metrics jsonb'
                )
                if state != "empty":
                    legacy_result = await _seed_legacy_snapshot(
                        database, schema, state, selection, synthetic_profile_execution
                    )
                if state in {"mixed", "newer_invalid"}:
                    with pytest.raises((RuntimeError, ValueError), match="initial_"):
                        await initial.capture_targets(fhir, schema)
                else:
                    observed = await initial.capture_targets(fhir, schema)
                    assert observed.payload["resolution"] == ("empty" if state == "empty" else "legacy_as_of_unknown")
                    assert not hasattr(observed, "profile_as_of")
                    if state == "legacy":
                        assert observed.payload["historical_publication"]["result"] == legacy_result
                        assert observed.payload["historical_publication"]["profile_as_of"] is None
                    assert (
                        await database.scalar(
                            f'SELECT count(*) FROM "{schema}".provider_directory_profile_serving_generation'
                        )
                        == 0
                    )

    asyncio.run(exercise())


async def _fixture_admission(database, geometry, projection, consumed, preflight):
    """Construct the original native fixture admission with its observed WAL origin."""
    admission = fhir._ProviderDirectoryProfileCapacityAdmission(
        geometry=geometry,
        control_wal_projection=projection,
        build_id=consumed["build_id"],
        run_id=consumed["run_id"],
        lease=SimpleNamespace(
            attestation_id=consumed["attestation_id"],
            lease_digest=consumed["lease_digest"],
            nonce=preflight["receipt_sha256"],
            max_build_deadline=consumed["max_build_deadline"],
        ),
        database_identity=None,
        initial_wal_lsn=await database.scalar("SELECT pg_current_wal_insert_lsn()::text"),
        wal_tracker=fhir._ProviderDirectoryProfileWalTracker(
            accounted_control_operation_counts={
                "admission_row_lock": fhir.PROVIDER_DIRECTORY_PROFILE_ADMISSION_ROW_LOCK_COUNT,
                "capacity_consumption_insert": 3,
            }
        ),
    )
    return admission


async def _seed_cutover_checkpoint(engine, schema, build, geometry, oid_by_target):
    """Create the exact ready checkpoint retired by the native owner transaction."""
    async with engine.begin() as connection:
        await connection.execute(
            text(f'''INSERT INTO "{schema}".provider_directory_profile_build_checkpoint
            (build_id,strategy_version,schema_version,resume_lineage_hash,source_ids,retained_source_ids,dataset_ids,
             evidence_stage,profile_stage,evidence_stage_oid,profile_stage_oid,state,profile_as_of,has_existing_artifacts,evidence_total_batches,profile_total_batches,
             evidence_next_batch,profile_next_batch) VALUES (:build,:strategy,1,:lineage,'[]','[]','[]',
             :evidence,:profile,:evidence_oid,:profile_oid,'ready',:asof,false,0,0,0,0)'''),
            {
                "build": build.build_id,
                "strategy": geometry.profile_strategy_version,
                "lineage": "12" * 32,
                "evidence": build.evidence_stage,
                "profile": build.profile_stage,
                "asof": build.profile_as_of,
                "evidence_oid": oid_by_target[fhir.profile_artifact.PROFILE_EVIDENCE_TABLE],
                "profile_oid": oid_by_target[fhir.profile_artifact.PROFILE_TABLE],
            },
        )


def _initial_publication_case(database, admission, cutover, stages, engine, before, failure):
    """Return the exact transactional publication case with its observer and failure injection."""

    async def publish():
        async with database.transaction():
            await fhir._apply_provider_directory_profile_capacity_settings(admission)
            async with initial.swap_window(fhir, cutover):
                for stage in stages:
                    await fhir._install_provider_directory_prepared_stage(stage)
                for stage in stages:
                    await fhir._finish_provider_directory_prepared_stage(stage)
            await initial.finish_cutover(fhir, cutover, fhir.ProviderDirectoryArtifactDatasetFence(()))
            async with engine.connect() as observer:
                assert (
                    await observer.scalar(text(f"SELECT '{fixture._SCHEMA}.provider_directory_profile'::regclass::oid"))
                    == before
                )
                assert (
                    await observer.scalar(
                        text(f'SELECT count(*) FROM "{fixture._SCHEMA}".provider_directory_profile_serving_generation')
                    )
                    == 0
                )
            await database.status("SET CONSTRAINTS ALL IMMEDIATE")
            if failure == "cancel":
                raise asyncio.CancelledError()
            if failure:
                raise RuntimeError("synthetic_after_initial_receipt")

    return publish


async def _seed_legacy_snapshot(database, schema, state, selection, synthetic_profile_execution):
    """Seed exact legacy history, changed source context and the selected malformed case."""
    execution = synthetic_profile_execution()
    generation = "pdprofile_" + "a" * 32
    legacy_result = selection.profile_selection_result(
        execution,
        profile_generation_id=generation,
        profile_rows=1,
        profile_source_evidence_rows=1,
        profile_as_of="2026-08-09",
    )
    legacy_result.pop("profile_as_of")
    await database.status(
        f'''INSERT INTO "{schema}".import_run(run_id,status,importer,finished_at,metrics)
                        VALUES (:run,'succeeded','provider-directory-fhir',now(),CAST(:metrics AS jsonb))''',
        run="run_" + "a" * 32,
        metrics=json.dumps({fhir.PROFILE_SELECTION_RESULT_METRIC: legacy_result}),
    )
    await database.status(
        f'''INSERT INTO "{schema}".provider_directory_profile
                        VALUES (1234567890,'{{}}','{{}}','{{synthetic_profile_source}}','{{synthetic-endpoint-1}}',
                        '{{synthetic-dataset-1}}',1,1,1,:generation,now())''',
        generation=generation,
    )
    await database.status(
        f'''INSERT INTO "{schema}".provider_directory_profile_evidence
                        (evidence_key,npi,fact_type,fact_key,value_json,source_id,endpoint_id,dataset_id,resource_type,resource_id)
                        VALUES (:key,1234567890,'name',:key,'{{}}','synthetic_profile_source','synthetic-endpoint-1',
                        'synthetic-dataset-1','Practitioner','synthetic-resource')''',
        key="a" * 32,
    )
    # Current registry context differs from the old attestation; full replacement does not adopt it.
    await database.status(f'''CREATE TABLE "{schema}".provider_directory_source (
                        source_id text,endpoint_id text,canonical_api_base text,org_name text,plan_name text)''')
    await database.status(f'''INSERT INTO "{schema}".provider_directory_source
                        VALUES ('synthetic_profile_source','synthetic-endpoint-1','https://changed.invalid','Changed context',NULL)''')
    if state == "mixed":
        await database.status(
            f'''INSERT INTO "{schema}".provider_directory_profile
                            SELECT 1234567891,profile_json,evidence_json,source_ids,endpoint_ids,dataset_ids,source_count,
                            independent_source_count,fact_count,:generation,published_at FROM "{schema}".provider_directory_profile''',
            generation="pdprofile_" + "b" * 32,
        )
    if state == "newer_invalid":
        await database.status(
            f'''INSERT INTO "{schema}".import_run(run_id,status,importer,finished_at,metrics)
                            VALUES (:run,'succeeded','provider-directory-fhir',now()+interval '1 second',CAST(:metrics AS jsonb))''',
            run="run_" + "b" * 32,
            metrics=json.dumps({fhir.PROFILE_SELECTION_RESULT_METRIC: {**legacy_result, "unexpected": True}}),
        )
    return legacy_result
