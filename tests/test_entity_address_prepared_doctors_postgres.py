# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared Doctors mapping, exact geometry binding, and native cutover order."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_preparation as doctors_preparation
from process import entity_address_candidate_preparation as preparation
from process import entity_address_prepared_doctors as dependencies
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family
from tests.ptg2_serving_address_evidence_postgres_support import (
    _create_geo_assurance_state_table,
    _create_geo_reference_tables,
    _create_mrf_address_table,
    _create_npi_address_table,
)
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
        database, schema = fixture.database, fixture.schema
        monkeypatch.setattr(native, "db", database)
        await database.status("CREATE EXTENSION IF NOT EXISTS postgis")
        assert await database.scalar("SELECT to_regnamespace('tiger')") is None
        try:
            await database.status("CREATE SCHEMA tiger")
            async with database.engine.begin() as connection:
                metadata = MetaData(schema=schema)
                for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS):
                    await connection.execute(text(f'DROP TABLE "{schema}"."{model.__tablename__}"'))
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


@pytest.mark.asyncio
async def test_source_ranges_and_geo_use_prepared_doctors_until_exact_native_apply(monkeypatch):
    """New data comes from held tables while activation still requires canonical native publication."""
    async with _geo_fixture(monkeypatch) as fixture:
        database, schema = fixture.database, fixture.schema
        ctx = await _prepared_family(fixture)
        async with doctors_preparation.prepare_cms_doctors_generation(ctx) as doctors:
            bindings = await _bindings(fixture, doctors)
            captured = await _capture(fixture, doctors, bindings)
            token = preparation._PREPARATION.set(_inputs(doctors))
            dependency_token = preparation._NATIVE_DEPENDENCIES.set(captured)
            try:
                await _assert_staged_source_queries(database, schema)
                await native._project_geo_assurance_transaction(schema, "address_stage", force=True)
                assert (
                    await database.scalar(
                        f"SELECT geo_evidence_source_id FROM \"{schema}\".address_stage WHERE location_key='cms'"
                    )
                    == 0
                )
                projected, invalid, stage_oid, forced = await native._project_geo_assurance_transaction(
                    schema, "address_stage", force=False, **preparation.geo_dependency_options()
                )
                assert (projected, invalid, forced) == (5, 0, True)
                assert (
                    await database.scalar(
                        f"SELECT geo_evidence_source_id FROM \"{schema}\".address_stage WHERE location_key='cms'"
                    )
                    == 3
                )
            finally:
                preparation._NATIVE_DEPENDENCIES.reset(dependency_token)
                preparation._PREPARATION.reset(token)
            assert preparation.geo_dependency_options() == {}
            await _assert_apply_and_rollback(fixture, doctors, captured, stage_oid)


async def _assert_staged_source_queries(database, schema):
    """Exercise actual normalization, staged NPI range discovery, and range expansion."""
    availability_by_table = {"doctor_clinician_address": True}
    selects = native._current_provider_directory_source_selects(
        schema, availability_by_table, native._source_selects(schema, availability_by_table)
    )
    ranges = await native._npi_table_ranges(schema, "doctor_clinician_address", 2)
    assert ranges == [(7003, 7004)]
    shards = native._shard_source_selects(schema, selects, doctor_clinician_address_ranges=ranges)
    assert "d.npi >= 7003" in shards[0] and "d.npi < 7004" in shards[0]
    row = await database.first(shards[0])
    assert (row._mapping["npi"], row._mapping["city_name"]) == (7003, "prepared")


async def _assert_apply_and_rollback(fixture, doctors, captured, stage_oid):
    """Late failure restores both candidate names and incumbent authority for exact retry."""
    database, schema = fixture.database, fixture.schema
    with pytest.raises(RuntimeError, match="publication is missing"):
        async with database.transaction():
            await dependencies.assert_applied_dependencies(database, captured)
    for _attempt in range(2):
        with pytest.raises(RuntimeError, match="late rollback"):
            async with database.transaction():
                await database.status(f'ALTER TABLE "{schema}".entity_address_unified RENAME TO previous_address')
                await database.status(f'ALTER TABLE "{schema}".address_stage RENAME TO entity_address_unified')
                assert await database.scalar(native._activate_geo_assurance_candidate_sql(schema)) is None
                await doctors_preparation.apply_prepared_cms_doctors_generation(doctors)
                await dependencies.assert_applied_dependencies(database, captured)
                assert await database.scalar(native._activate_geo_assurance_candidate_sql(schema)) == stage_oid
                raise RuntimeError("late rollback")
        await dependencies.assert_prepared_dependencies(database, captured)


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
                    address["relfilenode"] += 100000
                case "unproven_other":
                    bindings[f"{fixture.schema}.npi_address"]["table_name"] = "address_stage"
                case "family":
                    supplied = replace(doctors, stage_oids=doctors.stage_oids[:1])
                case "disabled_seal":
                    await fixture.database.status(
                        f'ALTER TABLE "{fixture.schema}"."{address["table_name"]}" DISABLE TRIGGER {doctors_preparation._SEAL_TRIGGER}'
                    )
            process = AsyncMock()
            monkeypatch.setattr(native, "process_entity_address_unified_data", process)
            with pytest.raises((RuntimeError, ValueError)):
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
async def test_source_backend_rejects_replacement_and_releases_locks_after_cancellation(monkeypatch):
    """Each actual query holds the proven input; cancellation releases that backend's locks."""
    async with _geo_fixture(monkeypatch) as fixture:
        ctx = await _prepared_family(fixture)
        async with doctors_preparation.prepare_cms_doctors_generation(ctx) as doctors:
            captured = await _capture(fixture, doctors, await _bindings(fixture, doctors))
            token = preparation._PREPARATION.set(_inputs(doctors))
            dependency_token = preparation._NATIVE_DEPENDENCIES.set(captured)
            try:
                await _assert_backend_lock_cleanup(fixture, doctors)
                await _assert_replacement_rejected(fixture, doctors)
            finally:
                preparation._NATIVE_DEPENDENCIES.reset(dependency_token)
                preparation._PREPARATION.reset(token)


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
            async with fixture.database.engine.begin() as connection:
                await connection.execute(text(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    async with fixture.database.engine.begin() as connection:
        await connection.execute(text(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE NOWAIT"))


async def _assert_replacement_rejected(fixture, doctors):
    """A same-name foreign heap cannot be read even if the original is later restored."""
    database, schema = fixture.database, fixture.schema
    stage = doctors.stage_oids[0][1]
    async with database.transaction():
        await database.status(f'ALTER TABLE "{schema}"."{stage}" RENAME TO held_doctors')
        await database.status(f'CREATE TABLE "{schema}"."{stage}" (LIKE "{schema}".held_doctors INCLUDING ALL)')
    try:
        with pytest.raises(RuntimeError, match="Doctors identity changed"):
            await native._npi_table_ranges(schema, "doctor_clinician_address", 2)
    finally:
        async with database.transaction():
            await database.status(f'DROP TABLE "{schema}"."{stage}" RESTRICT')
            await database.status(f'ALTER TABLE "{schema}".held_doctors RENAME TO "{stage}"')
