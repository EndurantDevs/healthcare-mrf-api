# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker startup owns the catalog provider context without exposing credentials."""

import asyncio
import builtins
import json
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from types import ModuleType
from unittest.mock import AsyncMock, Mock

import pytest
from click.testing import CliRunner

import process
from process import registry_cms_source_worker as cancellation
from process import registry_fhir_catalog_sync as sync
from process import registry_fhir_catalog_worker as worker
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_publication_worker import configuration

FACTORY_KEY = "registry_fhir_catalog_source_export"


def _configuration(*, source_reader=False):
    existing = configuration()
    return {
        "source_reader_dsn": "postgresql://source_reader@db.example.test/example" if source_reader else None,
        "loader_dsn": "postgresql://catalog_loader@db.example.test/example",
        "sealer_dsn": "postgresql://catalog_sealer@db.example.test/example",
        "lifecycle_dsn": existing["lifecycle_dsn"],
        "execution_dsn": existing["execution_dsn"],
        "consumer": existing["consumer"],
        "standing_grant": standing_grant(control_schema="registry").document(),
        "loader_role": "catalog_loader",
        "reader_roles": ["registry_reader"],
        "operation_seconds": 60,
    }


def _install_source_helper(monkeypatch, callback):
    module = ModuleType("directory_catalog_runtime")
    module.catalog_source_export_runtime = callback
    monkeypatch.setitem(sys.modules, "directory_catalog_runtime", module)


def _source_scope(monkeypatch, context_by_field, *, factory=None, enter_error=None, close_error=None):
    events, producer = [], Mock() if factory is None else factory

    @asynccontextmanager
    async def scope():
        assert worker._SOURCE_RUNTIME_CONTEXT_KEY in context_by_field
        events.append("enter")
        try:
            if enter_error is not None:
                raise enter_error
            yield producer
        except BaseException as error:
            events.append(("error", error))
            raise
        finally:
            events.append("close")
            if close_error is not None:
                raise close_error

    callback = Mock(side_effect=scope)
    _install_source_helper(monkeypatch, callback)
    return producer, callback, events


def test_existing_worker_class_owns_both_lifecycle_hooks():
    settings = process.RegistryFHIRCatalogSync
    assert settings.on_startup is worker.fhir_catalog_sync_startup
    assert settings.on_shutdown is worker.fhir_catalog_sync_shutdown
    assert settings.queue_name == "arq:RegistryFHIRCatalogSync" and settings.max_jobs == settings.max_tries == 1


@pytest.mark.asyncio
async def test_reader_startup_uses_existing_database_and_never_imports_source(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=True)))
    startup = AsyncMock()
    monkeypatch.setattr(worker, "db_startup", startup)
    original_import = builtins.__import__

    def import_without_source(name, *arguments, **options):
        assert name != "directory_catalog_runtime"
        return original_import(name, *arguments, **options)

    monkeypatch.setattr(builtins, "__import__", import_without_source)
    context_by_field = {"unrelated": "retained"}
    await process.RegistryFHIRCatalogSync.on_startup(context_by_field)
    startup.assert_awaited_once_with(context_by_field)
    await process.RegistryFHIRCatalogSync.on_shutdown(context_by_field)
    assert context_by_field == {"unrelated": "retained"}


@pytest.mark.asyncio
async def test_source_startup_installs_factory_and_control_forwards_it(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {"context": {}}
    producer, callback, events = _source_scope(monkeypatch, context_by_field)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    monkeypatch.setattr(sync, "reconcile_fhir_catalog", AsyncMock(return_value={"state": "unchanged"}))

    async def watch(*_arguments):
        await asyncio.Future()

    monkeypatch.setattr(cancellation, "watch_registry_control_cancel", watch)
    await process.RegistryFHIRCatalogSync.on_startup(context_by_field)
    try:
        assert context_by_field[FACTORY_KEY] is producer and events == ["enter"]
        result = await worker.control_start(context_by_field, {"run_id": "synthetic-run"})
        assert result["state"] == "unchanged" and events == ["enter"]
        assert sync.reconcile_fhir_catalog.await_args.kwargs == {"source_export": producer}
        assert sync.reconcile_fhir_catalog.await_args.args[0].source_url is None
    finally:
        await process.RegistryFHIRCatalogSync.on_shutdown(context_by_field)
    assert events == ["enter", "close"]
    assert FACTORY_KEY not in context_by_field and worker._SOURCE_RUNTIME_CONTEXT_KEY not in context_by_field
    callback.assert_called_once_with()
    producer.assert_not_called()


def test_parameter_free_cli_uses_validated_server_configuration_and_serializes_result(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=True)))
    reconcile = AsyncMock(return_value={"state": "unchanged", "accepted_sources": 0})
    monkeypatch.setattr(worker, "run_fhir_catalog_sync", reconcile)
    monkeypatch.setattr(process, "run_import", asyncio.run)
    result = CliRunner().invoke(worker.registry_fhir_catalog_sync_command, [])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {"state": "unchanged", "accepted_sources": 0}
    assert reconcile.await_args.args[0].source_url.username == "source_reader"


@pytest.mark.parametrize("arguments", [["extra"], ["--source", "untrusted"], ["--help", "extra"]])
def test_catalog_cli_refuses_client_arguments_before_server_configuration(monkeypatch, arguments):
    parse = Mock(side_effect=AssertionError("invalid command must not parse server configuration"))
    monkeypatch.setattr(worker, "parse_fhir_catalog_sync_config", parse)
    assert CliRunner().invoke(worker.registry_fhir_catalog_sync_command, arguments).exit_code == 2
    parse.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["shape", "null-loader", "duplicate-role", "database", "grant"])
async def test_invalid_configuration_fails_before_source_or_database_acquisition(monkeypatch, damage):
    configured = _configuration()
    edits_by_name = {
        "shape": (configured, "unexpected", True),
        "null-loader": (configured, "loader_dsn", None),
        "duplicate-role": (configured, "sealer_dsn", configured["loader_dsn"]),
        "database": (configured, "sealer_dsn", configured["sealer_dsn"].replace("/example", "/another")),
        "grant": (configured["standing_grant"], "version", 0),
    }
    target, key, replacement = edits_by_name[damage]
    target[key] = replacement
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configured))
    context_by_field = {}
    _producer, callback, events = _source_scope(monkeypatch, context_by_field)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    with pytest.raises(ValueError, match="configuration_invalid"):
        await worker.fhir_catalog_sync_startup(context_by_field)
    callback.assert_not_called()
    worker.db_startup.assert_not_awaited()
    assert context_by_field == {} and events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("database startup failed"), asyncio.CancelledError("startup canceled")]
)
async def test_database_startup_failure_closes_source_with_original_error(monkeypatch, failure):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, context_by_field)
    monkeypatch.setattr(worker, "db_startup", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as caught:
        await worker.fhir_catalog_sync_startup(context_by_field)
    assert caught.value is failure
    assert events == ["enter", ("error", failure), "close"]
    assert context_by_field == {}
    await worker.fhir_catalog_sync_shutdown(context_by_field)
    assert events.count("close") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [RuntimeError("Source entry failed"), asyncio.CancelledError("Source entry canceled")]
)
async def test_source_acquisition_failure_cleans_up_before_database_start(monkeypatch, failure):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, context_by_field, enter_error=failure)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    with pytest.raises(type(failure)) as caught:
        await worker.fhir_catalog_sync_startup(context_by_field)
    assert caught.value is failure and events == ["enter", ("error", failure), "close"]
    assert context_by_field == {}
    worker.db_startup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_failure_preserves_original_startup_failure(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    failure = RuntimeError("database startup failed")
    _producer, _callback, events = _source_scope(
        monkeypatch, context_by_field, close_error=RuntimeError("cleanup failed")
    )
    monkeypatch.setattr(worker, "db_startup", AsyncMock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        await worker.fhir_catalog_sync_startup(context_by_field)
    assert caught.value is failure and events.count("close") == 1 and context_by_field == {}
    assert failure.__notes__ == ["Catalog Source startup cleanup failed"]


@pytest.mark.asyncio
async def test_noncallable_source_contract_closes_before_database_start(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, context_by_field, factory=False)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    with pytest.raises(ValueError, match="configuration_invalid"):
        await worker.fhir_catalog_sync_startup(context_by_field)
    assert events[0] == "enter" and events[-1] == "close" and context_by_field == {}
    worker.db_startup.assert_not_awaited()


@pytest.mark.asyncio
async def test_shutdown_waits_until_source_runtime_is_drained(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    entered, release, closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    producer = Mock()

    @asynccontextmanager
    async def scope():
        try:
            yield producer
        finally:
            entered.set()
            await release.wait()
            closed.set()

    _install_source_helper(monkeypatch, scope)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    context_by_field = {}
    await worker.fhir_catalog_sync_startup(context_by_field)
    shutdown = asyncio.create_task(worker.fhir_catalog_sync_shutdown(context_by_field))
    try:
        async with asyncio.timeout(1):
            await entered.wait()
            assert not shutdown.done() and not closed.is_set()
            release.set()
            await shutdown
        assert closed.is_set() and context_by_field == {}
    finally:
        release.set()
        await shutdown


@pytest.mark.asyncio
async def test_foreign_stack_refuses_before_source_import_and_shutdown_preserves_it(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    foreign, close_foreign = AsyncExitStack(), AsyncMock()
    foreign.push_async_callback(close_foreign)
    context_by_field = {worker._SOURCE_RUNTIME_CONTEXT_KEY: foreign}
    original_import = builtins.__import__

    def import_without_source(name, *arguments, **options):
        assert name != "directory_catalog_runtime"
        return original_import(name, *arguments, **options)

    monkeypatch.setattr(builtins, "__import__", import_without_source)
    try:
        with pytest.raises(ValueError, match="configuration_invalid"):
            await worker.fhir_catalog_sync_startup(context_by_field)
        await worker.fhir_catalog_sync_shutdown(context_by_field)
        assert context_by_field == {worker._SOURCE_RUNTIME_CONTEXT_KEY: foreign}
        close_foreign.assert_not_awaited()
    finally:
        await foreign.aclose()


@pytest.mark.asyncio
async def test_shutdown_preserves_replaced_factory_and_closes_owned_runtime(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, context_by_field)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    await worker.fhir_catalog_sync_startup(context_by_field)
    replacement = Mock()
    context_by_field[FACTORY_KEY] = replacement
    await worker.fhir_catalog_sync_shutdown(context_by_field)
    assert events == ["enter", "close"]
    assert context_by_field == {FACTORY_KEY: replacement}
    replacement.assert_not_called()


@pytest.mark.asyncio
async def test_startup_failure_closes_local_record_after_runtime_slot_replacement(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, context_by_field)
    foreign, close_foreign = AsyncExitStack(), AsyncMock()
    foreign.push_async_callback(close_foreign)
    failure = RuntimeError("database startup failed")

    async def database_startup(actual_context):
        actual_context[worker._SOURCE_RUNTIME_CONTEXT_KEY] = foreign
        raise failure

    monkeypatch.setattr(worker, "db_startup", database_startup)
    try:
        with pytest.raises(RuntimeError) as caught:
            await worker.fhir_catalog_sync_startup(context_by_field)
        assert caught.value is failure and events == ["enter", ("error", failure), "close"]
        assert context_by_field == {worker._SOURCE_RUNTIME_CONTEXT_KEY: foreign}
        await worker.fhir_catalog_sync_shutdown(context_by_field)
        close_foreign.assert_not_awaited()
    finally:
        await foreign.aclose()


@pytest.mark.asyncio
async def test_shutdown_ignores_owned_record_belonging_to_another_context(monkeypatch):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration()))
    owner_context_by_field = {}
    _producer, _callback, events = _source_scope(monkeypatch, owner_context_by_field)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    await worker.fhir_catalog_sync_startup(owner_context_by_field)
    record = owner_context_by_field[worker._SOURCE_RUNTIME_CONTEXT_KEY]
    foreign_context_by_field = {worker._SOURCE_RUNTIME_CONTEXT_KEY: record}
    try:
        await worker.fhir_catalog_sync_shutdown(foreign_context_by_field)
        assert foreign_context_by_field[worker._SOURCE_RUNTIME_CONTEXT_KEY] is record
        assert owner_context_by_field[worker._SOURCE_RUNTIME_CONTEXT_KEY] is record
        assert events == ["enter"]
    finally:
        await worker.fhir_catalog_sync_shutdown(owner_context_by_field)
    assert events == ["enter", "close"]
