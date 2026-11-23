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
async def test_catalog_discovery_reuses_first_inventory_until_next_import():
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

    profile = FHIRCatalogSourceReadProfile("source")
    visible_pages = [("first edition",)]
    reads = []

    @asynccontextmanager
    async def factory(*, source_profile):
        assert source_profile == profile

        async def inventory():
            reads.append(source_profile)
            for page in visible_pages:
                yield page

        yield {"inventory": inventory}

    async def read(factory):
        async with factory(source_profile=profile) as export:
            return [page async for page in export["inventory"]()]

    first_run = bootstrap._pin_catalog_inventory(factory)
    initial_pages = await read(first_run)
    visible_pages.append(("later edition",))

    assert await read(first_run) == initial_pages
    assert await read(bootstrap._pin_catalog_inventory(factory)) == visible_pages
    assert reads == [profile, profile]


@pytest.fixture
def configured_source_runtime(monkeypatch):
    from process import registry_fhir_catalog_worker as catalog_worker
    from process import registry_publication_worker as worker
    from process.registry_fhir_catalog_sources import parse_fhir_catalog_source_profile
    from process.registry_imported_source_inventory import ImportedSourceInventoryConfig
    from tests.test_registry_typed_source_recipe import _recipe

    events = []
    capability_by_name = {"catalog_export": lambda: None, "freeze_preparation": AsyncMock()}

    @asynccontextmanager
    async def factory_runtime():
        events.append("open")
        try:
            yield capability_by_name
        finally:
            events.append("close")

    source_module = ModuleType("directory_catalog_runtime")
    source_module.network_import_source_runtime = factory_runtime
    monkeypatch.setitem(sys.modules, "directory_catalog_runtime", source_module)

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
    return SimpleNamespace(events=events, capability_by_name=capability_by_name, settings=settings, catalog=catalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_source_factory_lifetime_drains_after_import_success_or_failure(configured_source_runtime, fails):
    case = configured_source_runtime
    try:
        async with AsyncExitStack() as cleanup:
            async with bootstrap.native_network_import_runtime(object(), cleanup) as (prepared, source_runtime):
                assert prepared is case.catalog and source_runtime.config is case.settings.inventory_sources
                assert source_runtime.inventory.control_schema == case.settings.control_schema
                assert case.events == ["open"]
                if fails:
                    raise RuntimeError("synthetic_import_failure")
    except RuntimeError as error:
        assert fails and str(error) == "synthetic_import_failure"
    assert case.events == ["open", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,error,events",
    [
        ("inventory", "native_inventory_required", []),
        ("publisher", "freeze_capability_invalid", ["open", "close"]),
        ("partial-catalog", "catalog_configuration_invalid", ["open", "close"]),
    ],
)
async def test_invalid_native_import_drains_open_factory(configured_source_runtime, fault, error, events):
    case = configured_source_runtime
    if fault == "inventory":
        case.settings.inventory_sources = None
    elif fault == "publisher":
        del case.capability_by_name["freeze_preparation"]
    else:
        case.catalog.source_endpoint_ids = ("selected-source",)

    with pytest.raises(ValueError, match=error):
        async with AsyncExitStack() as cleanup:
            async with bootstrap.native_network_import_runtime(object(), cleanup):
                pytest.fail("invalid configuration entered source runtime")
    assert case.events == events
