# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual worker startup and Source lifetime, without simulated publication authority."""

import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from process import network_fhir_source_witness_capture as capture_module
from process import registry_publication_worker as worker_module
from process.network_candidate_custody import NetworkCandidateCustodyError
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_source_witness_runtime import RegistrySourceWitnessConfig
from tests import registry_candidate_custody_split_postgres_support as custody_fixture
from tests.source_witness_capture_postgres_support import capture_arguments, source_witness_case
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_source_witness_capture_postgres import _capture
from tests.test_verified_office_source_witnesses_v2_postgres import _registry_on_source

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _worker_case(monkeypatch):
    """Compose existing owned fixtures without adding roles, grants or authority."""
    async with source_witness_case(monkeypatch) as source, AsyncExitStack() as resources:
        serving = await resources.enter_async_context(_registry_on_source(source.fixture, monkeypatch))
        addresses = await resources.enter_async_context(aclosing(custom_db.__wrapped__(serving)))
        address_fixture = await anext(addresses)
        workers = await resources.enter_async_context(aclosing(custody_fixture.split_db.__wrapped__(address_fixture)))
        fixture = await anext(workers)
        await custody_fixture._assert_handoff(fixture)
        roles = tuple(sorted({*fixture.roles_by_kind.values(), *fixture.readers, source.fixture.component.reader}))
        config = replace(
            fixture.config,
            initial_address_sources=RegistryCompositionAddressSources(address_fixture.base, compact=True),
            source_witness=RegistrySourceWitnessConfig(
                (source.source_profile,), source.fixture.role_by_kind["owner"], roles
            ),
        )
        yield SimpleNamespace(source=source, fixture=fixture, config=config)
        assert await custody_fixture._protected(fixture) == fixture.protected


def _observe_worker(monkeypatch, *, export_case=None):
    """Forward every production call and retain actual pool, identity and runtime observations."""
    original_pool = worker_module.asyncpg.create_pool
    original_install = worker_module.install_source_witness_runtime
    original_identity = worker_module._custody_identity
    original_qualified = worker_module._qualified_worker_runtime
    observed = SimpleNamespace(pools=[], installed=[], identities=[], qualified=[], captures=[])

    async def create_pool(**options):
        pool = await original_pool(**options)
        observed.pools.append((options["user"], pool))
        try:
            async with pool.acquire() as connection:
                assert tuple(await connection.fetchrow("SELECT session_user,current_user")) == (options["user"],) * 2
        except BaseException:
            await pool.close()
            raise
        return pool

    async def install(config, resources, **options):
        installed = await original_install(config, resources, **options)
        from directory_membership_runtime import CatalogSnapshotRuntime, open_membership_source_export

        runtime = installed.initial_address_sources.source_witness_runtime
        assert runtime.source_export.func is open_membership_source_export
        assert type(runtime.source_export.args[0]) is CatalogSnapshotRuntime
        observed.installed.append(installed)
        return installed

    async def identity(connection, schema, role):
        actual = await original_identity(connection, schema, role)
        observed.identities.append(actual)
        return actual

    async def qualified(config, resources, qualification, connection):
        consumer = await original_qualified(config, resources, qualification, connection)
        observed.qualified.append(consumer)
        if export_case is not None:
            runtime = config.initial_address_sources.source_witness_runtime
            case = SimpleNamespace(**{**vars(export_case), "source_export": runtime.source_export})
            arguments_by_name = {**capture_arguments(case), "runtime_roles": runtime.runtime_roles}
            capture = await _capture(case, capture_module, arguments_by_name)
            observed.captures.append(capture)
            async with case.reader.transaction(isolation="repeatable_read", readonly=True):
                assert await capture_module.require_source_fhir_witness_capture(case.reader, capture) == capture
        return consumer

    monkeypatch.setattr(worker_module.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(worker_module, "install_source_witness_runtime", install)
    monkeypatch.setattr(worker_module, "_custody_identity", identity)
    monkeypatch.setattr(worker_module, "_qualified_worker_runtime", qualified)
    return observed


def _assert_drained(observed, expected_roles):
    assert [role for role, _ in observed.pools] == expected_roles
    assert all(pool.is_closing() for _, pool in observed.pools)
    assert len(observed.installed) == 1
    runtime = observed.installed[0].initial_address_sources.source_witness_runtime
    assert runtime.source_export.args[0]._closed is True


async def _record_native_actors(case, record_property):
    """Retain actual role OIDs and the child database identity before fixture teardown."""
    connection = case.fixture.fixture.connection
    roles = await connection.fetch(
        "SELECT rolname,oid::bigint AS oid,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,rolbypassrls "
        "FROM pg_roles WHERE rolname=ANY($1::text[]) ORDER BY rolname",
        list(case.config.source_witness.runtime_roles),
    )
    assert tuple(role["rolname"] for role in roles) == case.config.source_witness.runtime_roles
    database = await connection.fetchrow("SELECT datname,oid::bigint FROM pg_database WHERE datname=current_database()")
    assert database["oid"] == case.source.origin.database_oid
    record_property("native_worker_database", json.dumps(dict(database), sort_keys=True))
    record_property("native_worker_actors", json.dumps([dict(role) for role in roles], sort_keys=True))


async def test_actual_worker_refuses_incomplete_actors_before_custodian_pool(monkeypatch, record_property):
    """Bind refusals to authenticated Q/R/K identities, including a metadata-owner reader alias."""
    async with _worker_case(monkeypatch) as case:
        await _record_native_actors(case, record_property)
        fixture, config = case.fixture, case.config
        for kind in ("queue", "reader", "custodian", "custody"):
            missing = fixture.roles_by_kind[kind]
            settings = replace(
                config.source_witness,
                runtime_roles=tuple(role for role in config.source_witness.runtime_roles if role != missing),
            )
            with monkeypatch.context() as patch:
                observed = _observe_worker(patch)
                with pytest.raises(ValueError, match="^registry_source_witness_runtime_census_differs$"):
                    await worker_module.run_registry_publication_worker(replace(config, source_witness=settings))
            assert observed.qualified == [] and len(observed.identities) == 1
            assert observed.identities[0]["schema_owner_role"] == fixture.roles_by_kind["queue"]
            assert observed.identities[0]["custody_owner_role"] == fixture.roles_by_kind["custody"]
            _assert_drained(observed, [config.consumer.lifecycle_role, config.consumer.execution_role])
        await _refuse_metadata_owner_reader(case, monkeypatch)


async def _refuse_metadata_owner_reader(case, monkeypatch):
    """A genuine metadata owner named as Reader must still fail the native Q-reach check."""
    config, fixture = case.config, case.fixture
    owner = fixture.roles_by_kind["queue"]
    consumer = replace(config.consumer, reader_roles=tuple(sorted({*config.consumer.reader_roles, owner})))
    aliased = replace(config, consumer=consumer)
    assert owner in config.source_witness.runtime_roles
    with monkeypatch.context() as patch:
        observed = _observe_worker(patch)
        with pytest.raises(NetworkCandidateCustodyError):
            await worker_module.run_registry_publication_worker(aliased)
    assert observed.qualified == [] and len(observed.identities) == 1
    assert observed.identities[0]["schema_owner_role"] == owner
    _assert_drained(observed, [consumer.lifecycle_role, consumer.execution_role])


async def test_actual_worker_owns_original_export_and_drains_native_pools(monkeypatch, record_property):
    """Exercise the installed original export after custody qualification; no queued publication is claimed."""
    async with _worker_case(monkeypatch) as case:
        await _record_native_actors(case, record_property)
        with monkeypatch.context() as patch:
            observed = _observe_worker(patch, export_case=case.source)
            assert await worker_module.run_registry_publication_worker(case.config) == {"state": "no_work"}
        assert len(observed.qualified) == len(observed.captures) == 1
        consumer, capture = observed.qualified[0], observed.captures[0]
        assert callable(consumer.seal_candidate) and callable(consumer.prepare_address_sources)
        assert asdict(capture.origin) == asdict(case.source.origin)
        assert capture.runtime_roles == case.config.source_witness.runtime_roles
        assert capture.owner_role == case.config.source_witness.capture_owner_role
        _assert_drained(
            observed,
            [
                case.config.consumer.lifecycle_role,
                case.config.consumer.execution_role,
                case.config.custody_url.username,
            ],
        )
