# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Trusted producer injection leaves parameter-free worker entry points unchanged."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_sync as sync
from process import registry_fhir_catalog_worker as worker


def _config():
    return worker.RegistryFHIRCatalogSyncConfig(None, None, None, None, None, None, None, "example_loader", ())


@pytest.mark.parametrize("with_producer", [False, True])
async def test_worker_forwards_only_explicit_trusted_producer(monkeypatch, with_producer):
    reconcile = AsyncMock(return_value={"state": "unchanged"})
    monkeypatch.setattr(sync, "reconcile_fhir_catalog", reconcile)
    config = _config()
    producer = Mock() if with_producer else None
    result = await worker.run_fhir_catalog_sync(config, source_export=producer)
    if with_producer:
        reconcile.assert_awaited_once_with(config, source_export=producer)
        producer.assert_not_called()
    else:
        reconcile.assert_awaited_once_with(config)
    assert result["state"] == "unchanged" and "audit" in result


@pytest.mark.parametrize("error", [RuntimeError("synthetic failure"), asyncio.CancelledError()])
async def test_producer_failure_and_cancellation_remain_original(monkeypatch, error):
    reconcile = AsyncMock(side_effect=error)
    monkeypatch.setattr(sync, "reconcile_fhir_catalog", reconcile)
    producer = Mock()
    config = _config()
    with pytest.raises(type(error)) as raised:
        await worker.run_fhir_catalog_sync(config, source_export=producer)
    assert raised.value is error
    reconcile.assert_awaited_once_with(config, source_export=producer)


def test_client_parameters_cannot_inject_producer():
    with pytest.raises(ValueError, match="parameters_invalid"):
        worker.validate_fhir_catalog_sync_params({"source_export": "synthetic"})
