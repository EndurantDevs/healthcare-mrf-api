# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual named Source publication, original factory lifetime and native worker cleanup."""

import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from process import network_fhir_source_witness_capture as capture_module
from process import registry_publication_worker as worker_module
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_source_witness_runtime import RegistrySourceWitnessConfig
from tests import registry_candidate_custody_split_postgres_support as worker_fixture
from tests.source_witness_capture_postgres_support import (
    assert_independent_derived_rows,
    capture_arguments,
    source_witness_case,
)
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_source_witness_capture_postgres import _capture
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _worker_case(monkeypatch):
    """Reuse genuine enrollment and administrator freezes with the existing application roles."""
    async with source_witness_case(monkeypatch, named=True) as source, AsyncExitStack() as resources:
        monkeypatch.setenv(
            "NETWORK_REGISTRY_TEST_DSN", source.fixture.component.admin_engine.url.render_as_string(hide_password=False)
        )
        serving_scope = await resources.enter_async_context(aclosing(serving_schema.__wrapped__()))
        serving = await anext(serving_scope)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        addresses = await resources.enter_async_context(aclosing(custom_db.__wrapped__(serving)))
        address_fixture = await anext(addresses)
        workers = await resources.enter_async_context(aclosing(worker_fixture.split_db.__wrapped__(address_fixture)))
        fixture = await anext(workers)
        await worker_fixture._install_named_worker_permissions(fixture)
        roles = tuple(sorted({*fixture.config.preparation_roles["application_roles"], *source.application_roles}))
        config = replace(
            fixture.config,
            initial_address_sources=RegistryCompositionAddressSources(address_fixture.base, compact=True),
            source_witness=RegistrySourceWitnessConfig(
                (source.source_profile,), source.fixture.role_by_kind["owner"], roles
            ),
            preparation_roles={**fixture.config.preparation_roles, "application_roles": list(roles)},
        )
        yield SimpleNamespace(source=source, fixture=fixture, config=config)
        assert await worker_fixture._protected(fixture) == fixture.protected


def _observe_worker(monkeypatch, export_case):
    """Forward actual factory, pool and named runtime calls without replacing their data checks."""
    original_pool = worker_module.asyncpg.create_pool
    original_install = worker_module.install_source_witness_runtime
    original_runtime = worker_module._configured_worker_runtime
    observed = SimpleNamespace(pools=[], installed=[], configured=[], captures=[])

    async def create_pool(**options):
        pool = await original_pool(**options)
        observed.pools.append((options["user"], pool))
        return pool

    async def install(config, resources, **options):
        installed = await original_install(config, resources, **options)
        from directory_membership_runtime import CatalogSnapshotRuntime, open_membership_source_export

        runtime = installed.initial_address_sources.source_witness_runtime
        assert runtime.source_export.func is open_membership_source_export
        assert type(runtime.source_export.args[0]) is CatalogSnapshotRuntime
        observed.installed.append(installed)
        return installed

    async def configured(config, resources):
        consumer = await original_runtime(config, resources)
        observed.configured.append(consumer)
        runtime = config.initial_address_sources.source_witness_runtime
        case = SimpleNamespace(**{**vars(export_case), "source_export": runtime.source_export})
        arguments_by_name = {**capture_arguments(case), "runtime_roles": runtime.runtime_roles}
        capture = await _capture(case, capture_module, arguments_by_name)
        observed.captures.append(capture)
        async with case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert await capture_module.require_source_fhir_witness_capture(case.reader, capture) == capture
            assert await assert_independent_derived_rows(case.reader, capture) == sum(
                count for _, count in case.origin.resource_counts
            )
        return consumer

    monkeypatch.setattr(worker_module.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(worker_module, "install_source_witness_runtime", install)
    monkeypatch.setattr(worker_module, "_configured_worker_runtime", configured)
    return observed


def _assert_drained(observed, expected_roles):
    assert [role for role, _ in observed.pools] == expected_roles
    assert all(pool.is_closing() for _, pool in observed.pools)
    assert len(observed.installed) == 1
    runtime = observed.installed[0].initial_address_sources.source_witness_runtime
    assert runtime.source_export.args[0]._closed is True


async def test_actual_worker_owns_original_export_and_drains_native_pools(monkeypatch, record_property):
    """Consume a genuinely published edition through the installed original export before worker shutdown."""
    async with _worker_case(monkeypatch) as case:
        record_property("source_publication", json.dumps(asdict(case.source.origin), sort_keys=True))
        with monkeypatch.context() as patch:
            observed = _observe_worker(patch, case.source)
            assert await worker_module.run_registry_publication_worker(case.config) == {"state": "no_work"}
        assert len(observed.configured) == len(observed.captures) == 1
        consumer, capture = observed.configured[0], observed.captures[0]
        assert callable(consumer.seal_candidate) and consumer.prepare_address_sources is None
        assert consumer.preparation_roles == case.config.preparation_roles
        assert asdict(capture.origin) == asdict(case.source.origin)
        _assert_drained(
            observed,
            [
                case.config.consumer.lifecycle_role,
                case.config.consumer.execution_role,
                case.config.consumer.execution_role,
            ],
        )
