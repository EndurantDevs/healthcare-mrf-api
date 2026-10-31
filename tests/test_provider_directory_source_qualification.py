# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh isolated source qualification preserves source and transport admission."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_policy as source_policy
from process.provider_directory_source_selection import qualify_stale_isolated_source

directory = importlib.import_module("process.provider_directory_fhir")
CAPABILITY = {"resourceType": "CapabilityStatement", "rest": [{"resource": [{"type": "Practitioner"}]}]}


def _source():
    return {
        "source_id": "synthetic-source",
        "api_base": "https://directory.example.test/fhir",
        "canonical_api_base": "https://directory.example.test/fhir",
        "last_validated_status": "valid_non_fhir",
        "auth_type": "none",
        "metadata_json": {},
    }


def _task(**overrides):
    return {
        "source_ids": ["synthetic-source"],
        "provider_directory_endpoint_scope": "https://directory.example.test/fhir",
        "import_resources": True,
        "full_refresh": True,
        "defer_typed_materialization": True,
        "probe": False,
        "bulk_export": False,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "resources": ["Practitioner"],
        "timeout": 3,
        **overrides,
    }


@pytest.fixture(autouse=True)
def _isolate_transport_and_forbid_probe_writes(monkeypatch):
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: True)
    token = source_policy._FACTORY.set(SimpleNamespace(storage_binding=object()))
    monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _source: None)
    writers = [AsyncMock(side_effect=AssertionError("unexpected catalog probe writer")) for _ in range(2)]
    monkeypatch.setattr(directory, "_run_source_probe_batch", writers[0])
    monkeypatch.setattr(directory, "_upsert_rows", writers[1])
    monkeypatch.setattr(directory, "_fetch_source_json", AsyncMock(side_effect=AssertionError("unexpected network")))
    try:
        yield
    finally:
        source_policy._FACTORY.reset(token)
    for writer in writers:
        writer.assert_not_awaited()


def _mock_transport(monkeypatch, *, resource_status=200, resource_payload=None, metadata_error=None):
    async def fetch(_source_record, url, *, timeout):
        assert timeout == 3
        if "/metadata" in url:
            return (None, None, metadata_error, 0) if metadata_error else (200, CAPABILITY, None, 1)
        return resource_status, resource_payload or {"resourceType": "Bundle", "type": "searchset"}, None, 1

    transport = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(directory, "_fetch_source_json", transport)
    return transport


def _selected(source_rows):
    return directory._select_resource_import_sources(
        source_rows, valid_source_ids=None, open_only=True, include_auth_required=False
    )[0]


@pytest.mark.asyncio
async def test_stale_seed_requires_real_capability_and_resource_access(monkeypatch):
    source_record = _source()
    transport = _mock_transport(monkeypatch)
    assert _selected([source_record]) == []
    qualified = await qualify_stale_isolated_source(directory, [source_record], _task(), timeout=3, run_id="run-test")
    assert qualified == [{**source_record, "last_validated_status": "valid"}]
    assert _selected(qualified) == qualified
    assert source_record["last_validated_status"] == "valid_non_fhir"
    assert transport.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        {"resource_status": 401},
        {"resource_status": 403},
        {"resource_status": 503},
        {"metadata_error": "TimeoutError: upstream unavailable"},
        {"resource_payload": {"resourceType": "OperationOutcome", "issue": [{"severity": "fatal"}]}},
    ],
)
async def test_failed_live_qualification_cannot_admit_stale_seed(monkeypatch, response):
    _mock_transport(monkeypatch, **response)
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["valid", "auth_required", "probe_only", "unreachable"])
async def test_other_seed_statuses_do_not_probe(monkeypatch, status):
    source_rows = [{**_source(), "last_validated_status": status}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [{"provider_directory_manual_only": True}, {"provider_directory_coverage_mode": "probe_only"}],
)
async def test_manual_and_probe_only_sources_stay_dormant(metadata):
    source_rows = [{**_source(), "metadata_json": metadata}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()
    assert _selected(source_rows) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"retry_of_run_id": "run-prior"},
        {"provider_directory_pagination_root_run_id": "run-root"},
        {"probe": True},
        {"defer_typed_materialization": False},
        {"import_resources": False},
        {"full_refresh": False},
        {"bulk_export": True},
        {"publish_artifacts": True},
        {"publish_source_dataset_only": True},
        {"stale_cleanup": True},
        {"seed_only": True},
        {"test_mode": True},
        {"source_ids": ["different-source"]},
    ],
)
async def test_retries_and_other_operations_do_not_change(overrides):
    source_rows = [_source()]
    assert (
        await qualify_stale_isolated_source(directory, source_rows, _task(**overrides), timeout=3, run_id=None)
        is source_rows
    )
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_storage_does_not_qualify(monkeypatch):
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: False)
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_source_factory_does_not_qualify():
    source_policy._FACTORY.get().storage_binding = None
    source_rows = [_source()]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_credentials_still_refuse_before_network():
    source_rows = [{**_source(), "auth_type": "OAuth2/SMART"}]
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_resource_probe", [False, True])
async def test_capability_alone_and_changed_endpoint_do_not_qualify(monkeypatch, has_resource_probe):
    source_rows = [_source()]
    probe_map = {"status": "valid", "http_status": 200, "api_base": source_rows[0]["api_base"]}
    if has_resource_probe:
        probe_map.update(
            api_base="https://other.example.test/fhir", resource_probe={"status": "reachable", "http_status": 200}
        )
    monkeypatch.setattr(directory, "_probe_source", AsyncMock(return_value=(probe_map, CAPABILITY)))
    assert await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
    assert _selected(source_rows) == []


@pytest.mark.asyncio
async def test_endpoint_scope_is_checked_before_transport():
    with pytest.raises(RuntimeError, match="endpoint_scope_mismatch"):
        await qualify_stale_isolated_source(
            directory,
            [_source()],
            _task(provider_directory_endpoint_scope="https://other.example.test/fhir"),
            timeout=3,
            run_id=None,
        )
    directory._fetch_source_json.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError])
async def test_probe_timeout_refuses_and_cancellation_propagates(monkeypatch, failure):
    monkeypatch.setattr(directory, "_probe_source", AsyncMock(side_effect=failure))
    source_rows = [_source()]
    if failure is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None)
    else:
        assert (
            await qualify_stale_isolated_source(directory, source_rows, _task(), timeout=3, run_id=None) is source_rows
        )
        assert _selected(source_rows) == []


@pytest.mark.asyncio
async def test_real_import_entry_qualifies_scoped_seed_before_existing_storage(monkeypatch):
    for name in (
        "ensure_database",
        "_prepare_provider_directory_job_tables",
        "_raise_if_resource_import_cancelled",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(directory, name, AsyncMock())
    seed_by_field = {
        "id": 17,
        "org_name": "Synthetic Health",
        "api_base": _source()["api_base"],
        "auth_type": "none",
        "last_validated_status": "valid_non_fhir",
    }
    source_record = directory._source_row_from_seed(seed_by_field)
    monkeypatch.setattr(directory, "_resolve_seed_db", Mock(return_value=("unused-local-seed", None)))
    monkeypatch.setattr(directory, "_seed_rows_from_sqlite", Mock(return_value=[seed_by_field]))
    monkeypatch.setattr(directory, "_resolve_retest_results", Mock(return_value=(None, None)))
    _mock_transport(monkeypatch)
    source_upsert = AsyncMock(side_effect=RuntimeError("qualified source reached storage"))
    monkeypatch.setattr(directory, "_upsert_provider_directory_source_rows", source_upsert)
    with pytest.raises(RuntimeError, match="qualified source reached storage"):
        await directory.process_provider_directory_fhir_data(
            {"context": {}}, _task(source_ids=[source_record["source_id"]])
        )
    qualified = source_upsert.await_args.args[0]
    assert len(qualified) == 1
    assert qualified[0]["source_id"] == source_record["source_id"]
    assert qualified[0]["api_base"] == source_record["api_base"]
    assert qualified[0]["last_validated_status"] == "valid"
    assert seed_by_field["last_validated_status"] == "valid_non_fhir"
    assert _selected(qualified) == qualified
