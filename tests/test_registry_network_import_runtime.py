# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An admitted import drains its factories and preserves completed shared captures."""

import sys
from contextlib import AsyncExitStack, asynccontextmanager
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_membership_refresh_runtime as runtime
from process import registry_network_import_runtime as bootstrap


@pytest.mark.asyncio
async def test_failed_publication_keeps_completed_source_captures_for_other_requests():
    resolver = runtime._SourceResolver(object())
    resolver.prepared_recipes = (object(),)
    resolver.created_schemas = ("completed_source",)
    resolver.prepared_command = "a" * 64
    connection = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("retained data must survive")))
    await resolver.discard_sources(connection, {"request_id": object()})
    assert resolver.prepared_recipes == resolver.created_schemas == ()
    assert resolver.prepared_command is None
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_source_factory_lifetime_drains_after_import_success_or_failure(monkeypatch, fails):
    from process import registry_fhir_catalog_worker as catalog_worker
    from process import registry_publication_worker as worker

    events = []

    @asynccontextmanager
    async def factory_runtime():
        events.append("open")
        try:
            yield {"catalog_export": lambda: None, "freeze_preparation": AsyncMock()}
        finally:
            events.append("close")

    source_module = ModuleType("directory_catalog_runtime")
    source_module.network_import_source_runtime = factory_runtime
    monkeypatch.setitem(sys.modules, "directory_catalog_runtime", source_module)
    from process.registry_fhir_catalog_sources import parse_fhir_catalog_source_profile
    from process.registry_imported_source_inventory import ImportedSourceInventoryConfig
    from tests.test_registry_typed_source_recipe import _recipe

    profile = parse_fhir_catalog_source_profile(_recipe().source_pin.catalog_descriptor["native_read_profile"])
    settings = SimpleNamespace(
        inventory_sources=ImportedSourceInventoryConfig("source_aca", "source_ptg", (profile,)),
        app_origin=None,
        control_schema="registry",
        source_reader_url=object(),
    )
    monkeypatch.setattr(runtime, "_settings", lambda configured: settings)
    monkeypatch.setattr(worker, "_worker_sessions", lambda *args: lambda: None)
    catalog = SimpleNamespace(source_endpoint_ids=None, consumer=SimpleNamespace(control_schema="registry"))
    monkeypatch.setattr(catalog_worker, "parse_fhir_catalog_sync_config", lambda *args, **kwargs: catalog)
    try:
        async with AsyncExitStack() as cleanup:
            async with bootstrap.native_network_import_runtime(object(), cleanup) as (prepared, source_runtime):
                assert prepared is catalog and source_runtime.config is settings.inventory_sources
                assert source_runtime.inventory.control_schema == settings.control_schema
                assert events == ["open"]
                if fails:
                    raise RuntimeError("synthetic_import_failure")
    except RuntimeError as error:
        assert fails and str(error) == "synthetic_import_failure"
    assert events == ["open", "close"]
