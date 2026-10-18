# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared Doctors mapping, exact geometry binding, and native cutover order."""

import asyncio
import importlib
import subprocess
from contextlib import asynccontextmanager, nullcontext
from copy import deepcopy
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import Database
from process import cms_doctors_preparation as doctors_preparation
from process import entity_address_candidate_preparation as preparation
from process import entity_address_prepared_doctors as dependencies
from tests import test_provider_directory_cms_address as factory_contract
from tests.cms_doctors_preparation_postgres_support import (
    GEO_DOCTORS_SOURCE_ROW,
    doctors_database,
    geo_target_expectations,
    seed_geo_native_inputs,
    stage_family,
)
from tests.cms_doctors_preparation_postgres_support import doctors_source_template as doctors_source_template
from tests.provider_directory_geo_profile9_acceptance_support import profile9_acceptance
from tests.ptg2_serving_address_evidence_postgres_support import (
    _create_geo_assurance_state_table,
    _create_geo_reference_tables,
    _create_mrf_address_table,
    _create_npi_address_table,
)
from tests.test_cms_doctors_preparation_seal_postgres import _changed_native_custody
from tests.test_entity_address_snapshot_destination import _seed_geo_dependencies, _seed_source_rows
from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context

native = importlib.import_module("process.entity_address_unified")


def _inputs(doctors):
    """Supply only the native address source override, separate from overlay identity."""
    return preparation.ProviderDirectoryAddressPreparationInput(
        (),
        "prepared_overlay",
        1,
        0,
        relation_overrides=(("doctor_clinician_address", doctors.relation_overrides["doctor_clinician_address"]),),
    )


@asynccontextmanager
async def _geo_fixture(monkeypatch):
    """Use real Doctors models and the existing small spatial projection fixture."""
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        schema = fixture.schema
        database = Database(engine=fixture.engine, session_factory=async_sessionmaker(fixture.engine))
        monkeypatch.setattr(native, "db", fixture.database)
        await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
        assert await database.scalar("SELECT to_regnamespace('tiger')") is None
        try:
            await database.status("CREATE SCHEMA tiger")
            async with database.engine.begin() as connection:
                metadata = MetaData(schema=schema)
                for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS):
                    await connection.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{model.__tablename__}"'))
                    model.__table__.to_metadata(metadata, schema=schema)
                await connection.run_sync(metadata.create_all)
                await _seed_source_rows(connection, schema, 0)
            for create in (
                _create_geo_assurance_state_table,
                _create_npi_address_table,
                _create_mrf_address_table,
                _create_geo_reference_tables,
            ):
                await create(database, schema)
            for name in ("zip_state", "zcta5"):
                await database.status(f'ALTER TABLE "{schema}"."{name}" SET SCHEMA tiger')
            await _seed_geo_dependencies(database, schema)
            await database.status(f'UPDATE "{schema}".doctor_clinician_address SET updated_at=NULL WHERE npi=7003')
            await database.status(
                f'CREATE TABLE "{schema}".address_stage (LIKE "{schema}".entity_address_unified INCLUDING ALL)'
            )
            await database.status(
                f'INSERT INTO "{schema}".address_stage SELECT * FROM "{schema}".entity_address_unified'
            )
            owner = fixture.component.owner
            await database.status(f'GRANT USAGE ON SCHEMA tiger TO "{owner}"')
            for namespace, name in (
                *dependencies.projection._PROJECTION_DEPENDENCIES,
                (schema, "entity_address_unified"),
                (schema, "entity_address_geo_assurance_state"),
                (schema, "address_stage"),
            ):
                await database.status(f'GRANT SELECT ON "{namespace or schema}"."{name}" TO "{owner}"')
            yield fixture
        finally:
            await database.status("DROP SCHEMA IF EXISTS tiger CASCADE")
            assert await database.scalar("SELECT to_regnamespace('tiger')") is None


async def _prepared_family(fixture):
    """Finish mutable row setup before the real three-table preparation seals it."""
    ctx = await stage_family(fixture.database, fixture.schema)
    stage = doctors_preparation._native().make_class(doctors_preparation._models()[0], ctx["import_date"]).__tablename__
    await fixture.database.status(
        f'UPDATE "{fixture.schema}"."{stage}" SET npi=7003, '
        "address_key='00000000-0000-0000-0000-000000000003',updated_at='2026-08-25 12:00:00'"
    )
    return ctx


async def _bindings(fixture, doctors):
    """Observe all six actual heaps, substituting only the prepared Doctors address."""
    bindings_by_name = {}
    for namespace, name in dependencies.projection._PROJECTION_DEPENDENCIES:
        namespace = namespace or fixture.schema
        actual_name = doctors.relation_overrides.get(name, name)
        row = await fixture.database.first(
            "SELECT oid::bigint,pg_relation_filenode(oid)::bigint FROM pg_class WHERE oid=to_regclass(:name)",
            name=f'"{namespace}"."{actual_name}"',
        )
        bindings_by_name[f"{namespace}.{name}"] = {
            "schema_name": namespace,
            "table_name": actual_name,
            "relation_oid": row[0],
            "relfilenode": row[1],
        }
    return bindings_by_name


async def _capture(fixture, doctors, bindings):
    """Use the same trusted dependency validation called by native address preparation."""
    return await dependencies.capture_dependencies(
        fixture.database,
        fixture.schema,
        _inputs(doctors).relation_overrides,
        doctors=doctors,
        dependency_bindings=bindings,
    )


@pytest.fixture
def geo_source_inputs(monkeypatch):
    """Finish raw fixture data before the original bootstrap enrolls its immutable inputs."""
    from process.ext import address_canon
    from tests import cms_registry_complete_publication_postgres_support as complete

    original_seed = complete._seed_native_doctors

    async def seed(database):
        await original_seed(database)
        await seed_geo_native_inputs(database, monkeypatch)

    monkeypatch.setattr(complete, "_seed_native_doctors", seed)
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "cms_doctors,nppes,mrf")
    return GEO_DOCTORS_SOURCE_ROW


@pytest.fixture
def native_source_commit():
    """Use the same read-only checkout provenance as the original complete Native fixture."""
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


@pytest.mark.asyncio
async def test_source_ranges_and_geo_use_prepared_doctors_until_exact_native_apply(
    monkeypatch, tmp_path, geo_source_inputs, native_source_commit
):
    """A complete late rollback preserves prepared Geo, Doctors and the pinned Reader family."""

    async def observe(*, scope, schema, stage, projected_rows):
        async with native.preparation_admission.native_transaction():
            await native._apply_entity_address_transaction_settings()
            await _assert_staged_source_queries(native.db, schema)
            await _assert_geo_targets(native.db, schema, stage, projected_rows)

    await _run_admitted_source_address(
        monkeypatch, tmp_path, native_source_commit, observe, geo_source_inputs, rollback=True
    )
    assert preparation.current() is None and preparation.geo_dependency_options() == {}


async def _assert_geo_targets(database, schema, stage, projected_rows):
    """Inspect only the five raw fixture identities while retaining every other actual output."""
    expected = geo_target_expectations()
    projected = await database.all(
        f"SELECT npi,address_key::text,premise_key::text,location_key,geo_evidence_source_id,"
        f'geo_identity_coherent,geo_point_coherent FROM "{schema}"."{stage}" '
        "WHERE npi=ANY(CAST(:npis AS bigint[]))",
        npis=sorted({entry["npi"] for entry in expected.values()}),
    )
    by_key = {row._mapping["address_key"]: dict(row._mapping) for row in projected}
    assert len(by_key) == 5 and len({row._mapping["location_key"] for row in projected}) == 5
    for name, fields in expected.items():
        assert all(by_key[fields["address_key"]][field] == value for field, value in fields.items()), name
    assert await native._validate_geo_assurance_projection(schema, stage) == 0
    assert projected_rows == await database.scalar(f'SELECT count(*) FROM "{schema}"."{stage}"')
    legacy = dependencies.projection.legacy_evidence_source_id_sql(
        "address_row", schema_name=schema, unified_table_name=stage
    )
    assert (
        await database.scalar(
            f'SELECT {legacy} FROM "{schema}"."{stage}" address_row WHERE address_key::text=:key',
            key=expected["cms"]["address_key"],
        )
        == 0
    )


async def _assert_staged_source_queries(database, schema):
    """Exercise actual normalization, staged NPI range discovery, and range expansion."""
    availability_by_table = {"doctor_clinician_address": True}
    selects = native._current_provider_directory_source_selects(
        schema, availability_by_table, native._source_selects(schema, availability_by_table)
    )
    ranges = await native._npi_table_ranges(schema, "doctor_clinician_address", 2)
    assert ranges == [(1000000020, 1000000021)]
    shards = native._shard_source_selects(schema, selects, doctor_clinician_address_ranges=ranges)
    assert "d.npi >= 1000000020" in shards[0] and "d.npi < 1000000021" in shards[0]
    row = await database.first(shards[0])
    assert (row._mapping["npi"], row._mapping["city_name"]) == (1000000020, "prepared")


def _assert_apply_and_rollback(original, doctors, monkeypatch):
    """Prove one complete apply rolls back without refunding its signed WAL charge."""
    from process import provider_directory_cms_publication as publication
    from process import provider_directory_generic_source_policy as source_policy

    async def commit(fhir, execution, prepared, **kwargs):
        address = kwargs["address"]
        admission = fhir._provider_directory_profile_capacity_admission()
        metadata_before = admission.wal_tracker.accounted_metadata_wal_bytes
        before_apply = publication._apply_prepared_results

        async def fail_after_apply(*args, **options):
            receipt_id, payload = await before_apply(*args, **options)
            await dependencies.assert_applied_dependencies(fhir.db, address.native_dependencies)
            session = fhir.db._transaction_binding().session
            current = await publication.receipts.read_current_receipt(session, fhir._schema())
            assert current["receipt_id"] == receipt_id and current["payload"] == payload
            assert await session.scalar(
                text("SELECT " + dependencies.projection.projection_state_available_sql(fhir._schema()))
            )
            raise RuntimeError("late rollback")

        with monkeypatch.context() as failure:
            failure.setattr(publication, "_apply_prepared_results", fail_after_apply)
            with pytest.raises(RuntimeError, match="^late rollback$") as caught:
                await original(fhir, execution, prepared, **kwargs)
        async with source_policy.source_transaction(fhir.db) as session:
            await publication.receipts.assert_native_dependencies(
                session, fhir._schema(), kwargs["native_dependencies"]
            )
            assert await publication.receipts.read_current_receipt(session, fhir._schema()) == kwargs["predecessor"]
            await dependencies.assert_prepared_dependencies(fhir.db, address.native_dependencies)
            for _target, stage, oid in address.stage_oids:
                assert (
                    await session.scalar(
                        text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"{address.db_schema}.{stage}"}
                    )
                    == oid
                )
        assert not doctors.committed and not address.committed
        _assert_rollback_metadata_charge(fhir, admission, metadata_before)
        raise caught.value

    return commit


def _assert_rollback_metadata_charge(fhir, admission, metadata_before):
    """Keep aborted physical WAL charged and reject a request beyond the signed remainder."""
    from process.provider_directory_profile_capacity_types import METADATA_WAL_UPPER_BOUND_BYTES

    maximum = admission.geometry.metadata_wal_upper_bound_bytes
    charged = admission.wal_tracker.accounted_metadata_wal_bytes
    assert maximum == METADATA_WAL_UPPER_BOUND_BYTES == 64 * 1024 * 1024
    assert metadata_before < charged <= maximum
    with pytest.raises(RuntimeError, match="^provider_directory_profile_capacity_metadata_wal_projected$"):
        fhir._profile_metadata_wal_candidate(admission, maximum - charged + 1)
    assert admission.wal_tracker.accounted_metadata_wal_bytes == charged


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "bare_override",
        "missing_bundle",
        "missing_geo",
        "wrong_oid",
        "wrong_filenode",
        "unproven_other",
        "held_tiger",
        "family",
        "disabled_seal",
    ],
)
async def test_unproven_or_changed_doctors_inputs_fail_before_native_work(monkeypatch, change):
    async with _geo_fixture(monkeypatch) as fixture:
        ctx = await _prepared_family(fixture)
        async with doctors_preparation.prepare_cms_doctors_generation(ctx) as doctors:
            supplied = doctors
            bindings = await _bindings(fixture, doctors)
            address = bindings[f"{fixture.schema}.doctor_clinician_address"]
            custody = nullcontext()
            match change:
                case "bare_override":
                    supplied = None
                case "missing_bundle":
                    supplied = object()
                case "missing_geo":
                    bindings = None
                case "wrong_oid":
                    address["relation_oid"] += 100000
                case "wrong_filenode":
                    owner = fixture.component.owner
                    await fixture.database.status(f'GRANT USAGE ON SCHEMA tiger TO "{owner}"')
                    await fixture.database.status(f'GRANT SELECT ON tiger.zip_state,tiger.zcta5 TO "{owner}"')
                    address["relfilenode"] += 100000
                case "unproven_other":
                    bindings[f"{fixture.schema}.npi_address"]["table_name"] = "address_stage"
                case "held_tiger":
                    await fixture.database.status(f'CREATE TABLE "{fixture.schema}".held_zcta AS TABLE tiger.zcta5')
                    identity = await fixture.database.first(
                        "SELECT oid::bigint,pg_relation_filenode(oid)::bigint FROM pg_class "
                        "WHERE oid=to_regclass(:name)",
                        name=f"{fixture.schema}.held_zcta",
                    )
                    bindings["tiger.zcta5"] = {
                        "schema_name": fixture.schema,
                        "table_name": "held_zcta",
                        "relation_oid": identity[0],
                        "relfilenode": identity[1],
                    }
                case "family":
                    supplied = replace(doctors, stage_oids=doctors.stage_oids[:1])
                case "disabled_seal":
                    custody = _changed_native_custody(fixture, doctors, "update")
            process = AsyncMock()
            monkeypatch.setattr(native, "process_entity_address_unified_data", process)
            refusal = {
                "wrong_filenode": "entity-address geo dependency binding changed",
                "disabled_seal": "ordinary mutation",
            }.get(change)
            async with custody:
                with pytest.raises((RuntimeError, ValueError), match=refusal):
                    await preparation.prepare_provider_directory_entity_address(
                        {}, {}, preparation_input=_inputs(doctors), doctors=supplied, dependency_bindings=bindings
                    )
            process.assert_not_awaited()
            assert preparation.current() is None and preparation.geo_dependency_options() == {}


def _example_bindings():
    """Supply a closed scalar map for the finalizer's forwarding contract only."""
    return {
        f"{schema or 'mrf'}.{name}": {
            "schema_name": schema or "mrf",
            "table_name": name,
            "relation_oid": index,
            "relfilenode": index + 10,
        }
        for index, (schema, name) in enumerate(dependencies.projection._PROJECTION_DEPENDENCIES, 1)
    }


def test_dependency_options_copy_input_maps_and_keep_standalone_default():
    """Runtime callers cannot mutate captured input identity through emitted geo options."""
    assert preparation.geo_dependency_options() == {}
    bindings = _example_bindings()
    captured = dependencies.PreparedAddressDependencies("mrf", None, (), deepcopy(bindings))
    token = preparation._NATIVE_DEPENDENCIES.set(captured)
    try:
        preparation.geo_dependency_options()["dependency_bindings"]["mrf.npi_address"]["relation_oid"] = 99
        assert captured.dependency_bindings == bindings
    finally:
        preparation._NATIVE_DEPENDENCIES.reset(token)


@pytest.mark.asyncio
async def test_real_finalizer_forwards_desired_geo_bindings(monkeypatch):
    """The existing geo publication path must receive the trusted preparation context."""
    _mock_shutdown_dependencies(monkeypatch, [])
    monkeypatch.setattr(native, "enqueue_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    bindings = _example_bindings()
    captured = dependencies.PreparedAddressDependencies("mrf", None, (), bindings)
    ctx = _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL)
    geometry = AsyncMock(return_value=0)
    prepared = object()
    monkeypatch.setattr(native, "_materialize_geo_assurance", geometry)
    monkeypatch.setattr(preparation, "prepare_finalized_generation", AsyncMock(return_value=prepared))
    token = preparation._PREPARATION.set(
        preparation.ProviderDirectoryAddressPreparationInput((), "overlay_stage", 1, 0)
    )
    dependency_token = preparation._NATIVE_DEPENDENCIES.set(captured)
    try:
        assert await native.publish_entity_address_unified_generation(ctx, prepare_only=True) is prepared
        assert geometry.await_args.kwargs["dependency_bindings"] == bindings
    finally:
        preparation._NATIVE_DEPENDENCIES.reset(dependency_token)
        preparation._PREPARATION.reset(token)


@pytest.mark.asyncio
async def test_source_backend_rejects_replacement_and_releases_locks_after_cancellation(
    monkeypatch, tmp_path, geo_source_inputs, native_source_commit
):
    """Actual admitted source queries release cancellation locks and refuse a replaced heap."""

    async def observe(*, scope, schema, stage, projected_rows):
        component = native.db._doctors_source_component
        fixture = SimpleNamespace(database=native.db, schema=schema, engine=component.admin_engine, component=component)
        doctors = preparation._NATIVE_DEPENDENCIES.get().doctors
        await _assert_backend_lock_cleanup(fixture, doctors)
        await _assert_replacement_rejected(fixture, doctors)
        async with native.preparation_admission.native_transaction():
            await native._apply_entity_address_transaction_settings()
            await _assert_geo_targets(native.db, schema, stage, projected_rows)

    await _run_admitted_source_address(monkeypatch, tmp_path, native_source_commit, observe, geo_source_inputs)


async def _assert_backend_lock_cleanup(fixture, doctors):
    """Observe a real competing DDL lock, then prove cancellation releases the input lock."""
    is_locked = asyncio.Event()

    async def hold():
        """Wait inside the existing native backend scope after input identity checks."""
        async with native.preparation_admission.native_transaction():
            is_locked.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(hold())
    relation = f'"{fixture.schema}"."{doctors.stage_oids[0][1]}"'
    try:
        async with asyncio.timeout(5):
            await is_locked.wait()
        with pytest.raises(DBAPIError, match="lock"):
            async with fixture.engine.begin() as connection:
                await connection.execute(text(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with fixture.engine.begin() as connection:
        await connection.execute(text(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))


async def _assert_replacement_rejected(fixture, doctors):
    """A readable same-name foreign heap cannot stand in for the producer's sealed original."""
    database, schema = fixture.database, fixture.schema
    stage = doctors.stage_oids[0][1]
    async with fixture.engine.begin() as connection:
        await connection.execute(text(f'ALTER TABLE "{schema}"."{stage}" RENAME TO held_doctors'))
        await connection.execute(
            text(f'CREATE TABLE "{schema}"."{stage}" (LIKE "{schema}".held_doctors INCLUDING ALL)')
        )
        owner = fixture.component.policy.owner_role.replace('"', '""')
        await connection.execute(text(f'GRANT SELECT ON "{schema}"."{stage}" TO "{owner}"'))
    try:
        with pytest.raises(RuntimeError, match="Doctors identity changed"):
            await native._npi_table_ranges(schema, "doctor_clinician_address", 2)
    finally:
        async with fixture.engine.begin() as connection:
            await connection.execute(text(f'DROP TABLE "{schema}"."{stage}" RESTRICT'))
            await connection.execute(text(f'ALTER TABLE "{schema}".held_doctors RENAME TO "{stage}"'))


async def _run_admitted_source_address(monkeypatch, tmp_path, source_commit, callback, source_row, *, rollback=False):
    """Drive the real paired preflight and Source-created address family for a geometry observer."""
    from uuid import uuid4

    from process import provider_directory_generic_source_policy as source_policy
    from tests import cms_npd_admission_postgres_support as source
    from tests import cms_registry_complete_publication_postgres_support as complete

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "2")
    with pytest.raises(RuntimeError, match="stage row count 0 below minimum 2"):
        native._validate_publish_row_count(stage_rows=0, previous_rows=0, test_mode=False, min_rows_required=2)
    monkeypatch.setattr(source, "_resource_rows", partial(complete.office_resources, source._resource_rows))
    observed_stages = []
    monkeypatch.setattr(native, "enqueue_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        native,
        "_materialize_geo_assurance",
        _admitted_geo_observer(native._materialize_geo_assurance, callback, observed_stages),
    )
    async with complete.source_native_publication_database(monkeypatch, tmp_path, source_commit) as administrator:
        component = administrator._source_component
        directory, acquired = source.retained_release(tmp_path, include_missing_network=False)
        with source.release_probe_client(directory) as client:
            initial = await source.run_protected_admission(
                administrator, monkeypatch, "admitted-geo", directory, acquired, client
            )
        async with source_policy.bind_generic_source_transaction_factory(component.factory):
            assert complete.runtime.build_baked_healthcare_source_commit() == source_commit
            execution = await complete.attested_execution(administrator, initial, monkeypatch)
            run_id = "run_" + uuid4().hex
            async with component.admin_engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics) VALUES(:run,'test-engine','provider-directory-fhir','running','{}','{}')"
                    ),
                    {"run": run_id},
                )
            await _publish_geo_generation(component, execution, run_id, monkeypatch, source_row, rollback)
            assert len(observed_stages) == 1
        assert component.database._transaction_binding() is None
        return initial


async def _publish_geo_generation(component, execution, run_id, monkeypatch, source_row, rollback):
    """Sign and publish the same held Doctors family through the original complete owner."""
    from process import provider_directory_cms_preflight as preflight
    from process import provider_directory_cms_serving as serving
    from tests import cms_npd_admission_postgres_support as source
    from tests.test_network_cms_registry_complete_publication_postgres import _paired_native_execution

    ctx = await _stage_component_doctors(component, monkeypatch, source_row)
    async with doctors_preparation.prepare_cms_doctors_generation(ctx) as doctors:
        original_factory = preflight.cms_address_preparation
        factory = _desired_doctors_factory(original_factory, doctors)
        with monkeypatch.context() as signed_workload:
            signed_workload.setattr(preflight, "cms_address_preparation", factory)
            execution = await _paired_native_execution(monkeypatch, execution)
        async with profile9_acceptance(
            component, monkeypatch, cancel_reader=not rollback, rollback=rollback
        ) as profile_proof:
            with monkeypatch.context() as publication:
                publication.setattr(serving, "cms_address_preparation", factory)
                commit = profile_proof.commit(
                    _commit_with_prepared_doctors(serving.commit_prepared_serving_generation, doctors)
                )
                if rollback:
                    commit = _assert_apply_and_rollback(commit, doctors, monkeypatch)
                publication.setattr(serving, "commit_prepared_serving_generation", commit)
                outcome = pytest.raises(RuntimeError, match="^late rollback$") if rollback else nullcontext()
                with outcome:
                    await serving.publish_current_attested_profile(
                        source.fhir, execution=execution, run_id=run_id, control_run_id=run_id, metrics={}
                    )
        assert doctors.committed is not rollback


async def _stage_component_doctors(component, monkeypatch, source_row):
    """Provision the real fixed family before its retained CSV producer creates stages."""
    from process import cms_doctors_immutable as immutable
    from tests.cms_doctors_component_postgres_support import _provision_component
    from tests.cms_doctors_preparation_postgres_support import _bind_doctors_database

    await _provision_component(component.admin_engine, component.policy)
    async with component.database.transaction() as session:
        component.authority = await immutable.capture_doctors_predecessor(session, component.policy.schema)
        assert component.authority.serving_generation is not None
    component.database._doctors_source_component = component
    _bind_doctors_database(monkeypatch, component.database)
    return await stage_family(component.database, component.policy.schema, source_row=source_row)


def _desired_doctors_factory(original, doctors):
    """Apply the same genuine desired family before signing and before producing admission."""

    def desired(*args, **kwargs):
        fence = kwargs["native_input_fence"]
        bindings = deepcopy(fence["geo_bindings"])
        target, stage, oid = doctors.stage_oids[0]
        bindings[f"{doctors.schema}.{target}"] = {
            "schema_name": doctors.schema,
            "table_name": stage,
            "relation_oid": oid,
            "relfilenode": dict(doctors.sealed_filenodes)[oid],
        }
        return original(*args, **kwargs).with_prepared_doctors(doctors, bindings)

    return desired


def _commit_with_prepared_doctors(original, doctors):
    """Forward the held genuine family through the unchanged original commit API."""

    async def commit(*args, **kwargs):
        return await original(*args, **kwargs, doctors=doctors)

    return commit


def _admitted_geo_observer(original, callback, observed_stages):
    """Forward only within the real same-task building scope and its registered owned stage."""

    async def observe(schema, stage, **options):
        from process import entity_address_source_preparation as source_preparation

        scope = native.preparation_admission._ADMISSION.get()
        issued = source_preparation.require_building(scope)
        assert issued is not None and issued.task is asyncio.current_task()
        source_preparation.require_owned_relation(scope, stage, scope.owned_oids[stage])
        result = await original(schema, stage, **options)
        await callback(scope=scope, schema=schema, stage=stage, projected_rows=result)
        assert source_preparation.require_building(scope) is issued
        observed_stages.append((stage, scope.owned_oids[stage]))
        return result

    return observe


@pytest.mark.asyncio
async def test_admitted_geo_observer_refuses_unissued_stage_before_native_work():
    """An observer cannot turn an ordinary geometry fixture into Source stage authority."""
    original, callback = AsyncMock(), AsyncMock()
    with pytest.raises(AssertionError):
        await _admitted_geo_observer(original, callback, [])("mrf", "ordinary_stage", force=True)
    original.assert_not_awaited()
    callback.assert_not_awaited()


address_build_case = factory_contract.address_build_case
prepared_doctors_case = factory_contract.prepared_doctors_case


def test_desired_fixture_factory_matches_real_prepared_doctors_workload(address_build_case, prepared_doctors_case):
    """The fixture signs exactly the real factory workload without mutating its native fence."""
    doctors, bindings = prepared_doctors_case
    incumbent = deepcopy(address_build_case[4])
    actual = _desired_doctors_factory(factory_contract.address.cms_address_preparation, doctors)(
        *address_build_case[:4],
        native_input_fence=address_build_case[4],
        run_id="run_" + "2" * 32,
        worker_count=2,
        temp_file_limit_bytes_per_backend=1024,
    )
    expected = factory_contract._factory(address_build_case, doctors=doctors, dependency_bindings=bindings)
    assert actual.input_json == expected.input_json
    assert actual.input_hash == expected.input_hash
    assert actual.source_relation_overrides == doctors.relation_overrides
    assert address_build_case[4] == incumbent


def test_desired_fixture_factory_preserves_real_source_provenance_refusal(address_build_case, prepared_doctors_case):
    """A fixture transform cannot admit an unproved prepared family."""
    doctors, _bindings = prepared_doctors_case
    doctors = replace(doctors, source_provenance_json=None)
    with pytest.raises(ValueError, match="cms_doctors_source_provenance_required"):
        _desired_doctors_factory(factory_contract.address.cms_address_preparation, doctors)(
            *address_build_case[:4],
            native_input_fence=address_build_case[4],
            run_id="run_" + "2" * 32,
            worker_count=2,
            temp_file_limit_bytes_per_backend=1024,
        )


@pytest.mark.asyncio
async def test_prepared_doctors_commit_forwarding_preserves_original_publication_objects():
    """The test hook only supplies the held family; every original publication input is unchanged."""
    original = AsyncMock(return_value=object())
    doctors = object()
    arguments = tuple(object() for _ in range(3))
    keyword_by_name = {key: object() for key in ("address", "candidate_proof", "native_dependencies", "predecessor")}
    original_by_name = dict(keyword_by_name)
    result = await _commit_with_prepared_doctors(original, doctors)(*arguments, **keyword_by_name)
    original.assert_awaited_once_with(*arguments, **original_by_name, doctors=doctors)
    assert result is original.return_value
    assert keyword_by_name == original_by_name


def test_geo_raw_inputs_keep_distinct_units_and_valid_source_npis():
    """The genuine three-family CSV parsers accept the replacement synthetic identity."""
    from process.cms_doctors_education import doctor_education_row
    from process.cms_doctors_groups import doctor_group_site_row
    from process.cms_doctors_rows import doctor_address_row
    from process.provider_directory_profile import is_valid_npi

    fields_by_name = {**GEO_DOCTORS_SOURCE_ROW, "Med_sch": "prepared", "Grd_yr": "2001"}
    manifest_by_field = {"downloaded_at": "2026-08-25T12:00:00+00:00", "generation_id": "a" * 64}
    assert doctor_education_row(fields_by_name, 1, manifest_by_field)["npi"] == 1000000020
    assert doctor_group_site_row(fields_by_name, 1, manifest_by_field)["npi"] == 1000000020
    assert doctor_address_row(fields_by_name, None)["city"] == "prepared"
    expected = geo_target_expectations()
    assert all(is_valid_npi(str(entry["npi"])) for entry in expected.values())
    assert expected["cms"]["address_key"] != expected["cms-anchor"]["address_key"]
    assert expected["cms"]["premise_key"] == expected["cms-anchor"]["premise_key"]
    assert len({entry["location_key"] for entry in expected.values()}) == 5


def _rollback_fixture_directory(database):
    """Use the real metadata refusal predicate with an observable host-only charge ledger."""
    directory = importlib.import_module("process.provider_directory_fhir")
    admission = SimpleNamespace(
        geometry=SimpleNamespace(metadata_wal_upper_bound_bytes=64 * 1024 * 1024),
        wal_tracker=SimpleNamespace(accounted_metadata_wal_bytes=7),
    )
    fhir = SimpleNamespace(
        db=database,
        _schema=lambda: "mrf",
        _provider_directory_profile_capacity_admission=lambda: admission,
        _profile_metadata_wal_candidate=directory._profile_metadata_wal_candidate,
    )
    return fhir, admission


@pytest.mark.asyncio
async def test_rollback_observer_preserves_publication_objects_and_wal_charge(monkeypatch):
    """Host protocol check only: the failure hook wraps the original complete apply."""
    from process import provider_directory_cms_publication as publication
    from process import provider_directory_generic_source_policy as source_policy

    session = SimpleNamespace(scalar=AsyncMock(return_value=202))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    address = SimpleNamespace(
        native_dependencies=object(), committed=False, stage_oids=(("live", "stage", 202),), db_schema="mrf"
    )
    doctors = SimpleNamespace(committed=False)
    fhir, admission = _rollback_fixture_directory(database)
    prepared, execution, native_dependencies = object(), object(), object()
    state, calls = SimpleNamespace(has_applied=False), []
    receipt_by_field = {"receipt_id": "receipt", "payload": {"proof": "complete"}}

    @asynccontextmanager
    async def transaction(_database):
        yield session

    async def original(*args, **kwargs):
        calls.append((args, kwargs))
        admission.wal_tracker.accounted_metadata_wal_bytes += 11
        state.has_applied = True
        try:
            await publication._apply_prepared_results(*args, **kwargs)
            doctors.committed = address.committed = True
            return "committed"
        finally:
            state.has_applied = False

    monkeypatch.setattr(source_policy, "source_transaction", transaction)
    monkeypatch.setattr(
        publication,
        "_apply_prepared_results",
        AsyncMock(return_value=(receipt_by_field["receipt_id"], receipt_by_field["payload"])),
    )
    monkeypatch.setattr(
        publication.receipts,
        "read_current_receipt",
        AsyncMock(side_effect=lambda *_args: receipt_by_field if state.has_applied else None),
    )
    for module, name in (
        (dependencies, "assert_applied_dependencies"),
        (dependencies, "assert_prepared_dependencies"),
        (publication.receipts, "assert_native_dependencies"),
    ):
        monkeypatch.setattr(module, name, AsyncMock())
    options_by_name = dict(address=address, native_dependencies=native_dependencies, predecessor=None)
    with pytest.raises(RuntimeError, match="^late rollback$"):
        await _assert_apply_and_rollback(original, doctors, monkeypatch)(fhir, execution, prepared, **options_by_name)
    assert calls == [((fhir, execution, prepared), options_by_name)]
    assert dependencies.assert_applied_dependencies.await_count == 1
    assert dependencies.assert_prepared_dependencies.await_count == 1
    assert admission.wal_tracker.accounted_metadata_wal_bytes == 18
