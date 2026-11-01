# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A selected retry cannot claim all-source discovery or widen its Source callbacks."""

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_reporting as reporting
from process import registry_fhir_catalog_sync as sync
from process import registry_fhir_catalog_worker as worker
from tests.test_registry_fhir_catalog_worker_activation import _configuration
from tests.test_registry_fhir_catalog_worker_startup import _install_source_helper


def test_optional_server_selection_preserves_default_and_empty_caller_parameters():
    configured = _configuration(source_reader=False)
    parsed = worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())
    assert parsed.source_endpoint_ids is None
    configured["source_endpoint_ids"] = ["a" * 64]
    parsed = worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())
    assert parsed.source_endpoint_ids == ("a" * 64,)
    worker.validate_fhir_catalog_sync_params({})
    with pytest.raises(ValueError, match="parameters_invalid"):
        worker.validate_fhir_catalog_sync_params({"source_endpoint_ids": ["a" * 64]})


@pytest.mark.parametrize(
    "invalid", [[], False, "a" * 64, ["bad"], [False], ["a" * 64] * 2, ["b" * 64, "a" * 64], ["a" * 64] * 1001]
)
def test_invalid_selection_refused_before_runtime(invalid):
    configured = _configuration(source_reader=False)
    configured["source_endpoint_ids"] = invalid
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())


def test_selection_cannot_fall_back_to_native_reader():
    configured = _configuration()
    configured["source_endpoint_ids"] = ["a" * 64]
    with pytest.raises(ValueError, match="configuration_invalid"):
        worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())


@pytest.mark.asyncio
async def test_startup_binds_selection_before_source_runtime_and_reports_narrow_scope(monkeypatch):
    configured = _configuration(source_reader=False)
    configured["source_endpoint_ids"] = ["a" * 64]
    monkeypatch.setenv(worker.CONFIG_ENV, json.dumps(configured))
    opened_options = []

    @asynccontextmanager
    async def source_runtime(**options):
        opened_options.append(options)
        yield Mock()

    _install_source_helper(monkeypatch, source_runtime)
    monkeypatch.setattr(worker, "db_startup", AsyncMock())
    monkeypatch.setattr(sync, "reconcile_fhir_catalog", AsyncMock(return_value={"state": "unchanged"}))
    context_by_field = {}
    try:
        await worker.fhir_catalog_sync_startup(context_by_field)
        assert opened_options == [{"endpoint_ids": ("a" * 64,)}]
        parsed = worker.parse_fhir_catalog_sync_config(json.dumps(configured), source_export=Mock())
        result = await worker.run_fhir_catalog_sync(parsed, source_export=Mock())
        report = result["audit"]["fhir_catalog_sync"]
        assert report["inventory_scope"] == "selected_endpoints_and_retained_bindings"
        assert report["selected_endpoint_ids"] == ["a" * 64]
        assert report["global_source_count"] is report["global_network_count"] is None
    finally:
        await worker.fhir_catalog_sync_shutdown(context_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("returned", [(), ("b" * 64,)])
async def test_selected_discovery_rejects_missing_and_unrequested_endpoints(returned):
    from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile

    selected_profile = FHIRCatalogSourceReadProfile("sample", "sample_owner", ("sample_reader",))

    async def inventory():
        if returned:
            yield tuple(SimpleNamespace(endpoint_id=value) for value in returned)

    with reporting.fhir_catalog_run_reporting():
        with pytest.raises(ValueError, match="source_endpoint_scope_changed|source_endpoint_unavailable"):
            await sync._discover_catalog_sources(
                None,
                SimpleNamespace(source_profiles=(selected_profile,)),
                endpoint_ids=("a" * 64,),
                exports_by_profile={sync.canonical_json(selected_profile.document()): {"inventory": inventory}},
            )
