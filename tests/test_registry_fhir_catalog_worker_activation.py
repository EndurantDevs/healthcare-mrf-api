# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker configuration and trusted Source callback wiring."""

import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_cms_source_worker as cancellation
from process import registry_fhir_catalog_reporting as reporting
from process import registry_fhir_catalog_sync as sync
from process import registry_fhir_catalog_worker as worker
from process.control_cancel import ImportCancelledError
from tests.test_registry_fhir_catalog_grant import standing_grant
from tests.test_registry_fhir_catalog_reporting import disposition
from tests.test_registry_publication_worker import configuration

CONTEXT_KEY = "registry_fhir_catalog_source_export"


def _configuration(*, source_reader=True):
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


@pytest.mark.parametrize("with_producer", [False, True])
def test_native_reader_configuration_keeps_source_and_registry_roles(with_producer):
    producer = Mock() if with_producer else None
    parsed = worker.parse_fhir_catalog_sync_config(json.dumps(_configuration()), source_export=producer)
    urls = (parsed.source_url, parsed.loader_url, parsed.sealer_url, parsed.lifecycle_url, parsed.execution_url)
    assert tuple(url.username for url in urls) == (
        "source_reader",
        "catalog_loader",
        "catalog_sealer",
        "worker_lifecycle",
        "worker_execution",
    )
    assert {url.database for url in urls} == {"example"}
    assert parsed.loader_role == "catalog_loader" and parsed.reader_roles == ("registry_reader",)
    assert parsed.consumer.control_schema == parsed.grant.control_schema == "registry"
    assert parsed.operation_seconds == 60
    if producer is not None:
        producer.assert_not_called()


def test_four_role_configuration_requires_a_trusted_callable():
    encoded = json.dumps(_configuration(source_reader=False))
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(encoded)
    producer = Mock()
    parsed = worker.parse_fhir_catalog_sync_config(encoded, source_export=producer)
    assert parsed.source_url is None
    assert (
        parsed.loader_url.username,
        parsed.sealer_url.username,
        parsed.lifecycle_url.username,
        parsed.execution_url.username,
    ) == ("catalog_loader", "catalog_sealer", "worker_lifecycle", "worker_execution")
    producer.assert_not_called()


@pytest.mark.parametrize("source_reader", [False, True])
@pytest.mark.parametrize("producer", [False, "factory", {}])
def test_noncallable_factory_rejects_before_either_connection_route(source_reader, producer):
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(
            json.dumps(_configuration(source_reader=source_reader)), source_export=producer
        )


@pytest.mark.parametrize(
    "damage",
    [
        "duplicate-role",
        "database",
        "loader-role",
        "lifecycle-role",
        "execution-role",
        "grant-schema",
        "grant",
        "null-loader",
    ],
)
def test_four_registry_roles_and_standing_grant_keep_existing_checks(damage):
    configured = _configuration(source_reader=False)
    edits_by_name = {
        "duplicate-role": (configured, "sealer_dsn", configured["loader_dsn"]),
        "database": (configured, "sealer_dsn", configured["sealer_dsn"].replace("/example", "/another")),
        "loader-role": (configured, "loader_role", "different_loader"),
        "lifecycle-role": (configured["consumer"], "lifecycle_role", "different_role"),
        "execution-role": (configured["consumer"], "execution_role", "different_role"),
        "grant-schema": (configured["standing_grant"], "control_schema", "another_registry"),
        "grant": (configured["standing_grant"], "version", 0),
        "null-loader": (configured, "loader_dsn", None),
    }
    target, key, replacement = edits_by_name[damage]
    target[key] = replacement
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())


@pytest.mark.parametrize("damage", ["missing-reader", "injected-factory"])
def test_trusted_factory_does_not_open_configuration_shape(damage):
    configured = _configuration(source_reader=False)
    if damage == "missing-reader":
        del configured["source_reader_dsn"]
    else:
        configured[CONTEXT_KEY] = "factory"
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())


def _watcher(monkeypatch):
    closed = asyncio.Event()

    async def watch(*_arguments):
        try:
            await asyncio.Future()
        finally:
            closed.set()

    monkeypatch.setattr(cancellation, "watch_registry_control_cancel", watch)
    return closed


@pytest.mark.asyncio
@pytest.mark.parametrize("with_producer", [False, True])
async def test_control_forwards_only_top_level_factory_and_keeps_reporting(monkeypatch, with_producer):
    producer = Mock() if with_producer else None
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=not with_producer)))
    parse = Mock(wraps=worker.parse_fhir_catalog_sync_config)
    run = AsyncMock(wraps=worker.run_fhir_catalog_sync)
    monkeypatch.setattr(worker, "parse_fhir_catalog_sync_config", parse)
    monkeypatch.setattr(worker, "run_fhir_catalog_sync", run)
    monkeypatch.setattr(sync, "reconcile_fhir_catalog", AsyncMock(return_value={"state": "unchanged"}))
    closed = _watcher(monkeypatch)
    context_by_field = {"context": {"audit": {"existing": "retained"}}}
    if producer is not None:
        context_by_field[CONTEXT_KEY] = producer
    result = await worker.control_start(context_by_field, {"run_id": "synthetic-run"})
    expected_options = {"source_export": producer} if producer is not None else {}
    assert (
        parse.call_args.kwargs
        == run.await_args.kwargs
        == sync.reconcile_fhir_catalog.await_args.kwargs
        == expected_options
    )
    assert (run.await_args.args[0].source_url is None) is with_producer
    assert result["state"] == "unchanged" and closed.is_set()
    audit = context_by_field["context"]["audit"]
    assert audit["existing"] == "retained" and audit["fhir_catalog_sync"]["run_state"] == "unchanged"
    assert reporting.current_fhir_catalog_report() is None
    if producer is not None:
        producer.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("placement", ["context", "audit", "task"])
async def test_client_or_audit_data_cannot_supply_factory(monkeypatch, placement):
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=False)))
    monkeypatch.setattr(worker, "run_fhir_catalog_sync", AsyncMock())
    context_by_field, task = {"context": {}}, {"run_id": "synthetic-run"}
    if placement == "task":
        task[CONTEXT_KEY] = Mock()
    elif placement == "context":
        context_by_field["context"][CONTEXT_KEY] = Mock()
    else:
        context_by_field["context"]["audit"] = {CONTEXT_KEY: Mock()}
    with pytest.raises(ValueError, match="configuration_invalid|parameters_invalid"):
        await worker.control_start(context_by_field, task)
    worker.run_fhir_catalog_sync.assert_not_awaited()
    with pytest.raises(ValueError, match="parameters_invalid"):
        worker.validate_fhir_catalog_sync_params({CONTEXT_KEY: Mock()})


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("private failure"), asyncio.CancelledError("private cancellation")])
async def test_original_failure_or_cancellation_keeps_partial_report(monkeypatch, error):
    producer = Mock()
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=False)))

    async def reconcile(_config, *, source_export):
        assert source_export is producer
        report = reporting.current_fhir_catalog_report()
        report.add_sources((disposition(),), generations=("a" * 64,))
        report.source_started("a" * 64)
        raise error

    monkeypatch.setattr(sync, "reconcile_fhir_catalog", reconcile)
    closed = _watcher(monkeypatch)
    context_by_field = {CONTEXT_KEY: producer, "context": {"audit": {"existing": "retained"}}}
    with pytest.raises(type(error)) as caught:
        await worker.control_start(context_by_field, {"run_id": "synthetic-run"})
    assert caught.value is error and closed.is_set()
    audit = context_by_field["context"]["audit"]
    assert audit["existing"] == "retained"
    assert audit["fhir_catalog_sync"]["run_state"] == (
        "canceled" if isinstance(error, asyncio.CancelledError) else "failed"
    )
    assert len(audit["fhir_catalog_sync"]["sources"]) == 1 and "private failure" not in str(audit)
    assert reporting.current_fhir_catalog_report() is None


@pytest.mark.asyncio
async def test_control_watcher_cancels_owned_producer_work(monkeypatch):
    producer, started, closed = Mock(), asyncio.Event(), asyncio.Event()
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(_configuration(source_reader=False)))
    error = ImportCancelledError("synthetic control cancellation")

    async def reconcile(_config, *, source_export):
        assert source_export is producer
        started.set()
        try:
            await asyncio.Future()
        finally:
            closed.set()

    async def watch(*_arguments):
        await started.wait()
        raise error

    monkeypatch.setattr(sync, "reconcile_fhir_catalog", reconcile)
    monkeypatch.setattr(cancellation, "watch_registry_control_cancel", watch)
    context_by_field = {CONTEXT_KEY: producer, "context": {}}
    with pytest.raises(ImportCancelledError) as caught:
        await worker.control_start(context_by_field, {"run_id": "synthetic-run"})
    assert caught.value is error and closed.is_set()
    assert context_by_field["context"]["audit"]["fhir_catalog_sync"]["run_state"] == "canceled"
