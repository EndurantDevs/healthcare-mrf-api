# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Qualification uses prepared proxy transport and retains audit on cancellation."""

import asyncio
import contextlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
from yarl import URL

from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_source_proxy as adapter
from process.ext import source_http_proxy as proxy
from tests.test_provider_directory_source_qualification import CAPABILITY, _task, directory


@pytest.fixture
def source_entry(monkeypatch):
    for name in (proxy.POLICY_ENV, proxy.POOL_ENV, adapter.BINDING_ENV, adapter.COOLDOWNS_ENV):
        monkeypatch.delenv(name, raising=False)
    for name in (
        "ensure_database",
        "_prepare_provider_directory_job_tables",
        "_raise_if_resource_import_cancelled",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(directory, name, AsyncMock())
    for name in ("_run_source_probe_batch", "_upsert_rows"):
        monkeypatch.setattr(directory, name, AsyncMock(side_effect=AssertionError("unexpected catalog probe write")))
    monkeypatch.setattr(directory, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    monkeypatch.setattr(directory, "_fetch_json_sync", Mock(side_effect=AssertionError("unexpected direct fetch")))
    seed_by_field = {
        "id": 17,
        "org_name": "Synthetic Health",
        "api_base": "https://directory.example.test/fhir",
        "auth_type": "none",
        "last_validated_status": "valid_non_fhir",
    }
    source_record = directory._source_row_from_seed(seed_by_field)
    monkeypatch.setattr(directory, "_resolve_seed_db", Mock(return_value=("unused-local-seed", None)))
    monkeypatch.setattr(directory, "_seed_rows_from_sqlite", Mock(return_value=[seed_by_field]))
    monkeypatch.setattr(directory, "_resolve_retest_results", Mock(return_value=(None, None)))
    upsert = AsyncMock(side_effect=RuntimeError("qualified source reached storage"))
    monkeypatch.setattr(directory, "_upsert_provider_directory_source_rows", upsert)
    token = source_policy._FACTORY.set(SimpleNamespace(storage_binding=object()))
    try:
        yield SimpleNamespace(
            context={"context": {}},
            source_record=source_record,
            task=_task(source_ids=[source_record["source_id"]]),
            upsert=upsert,
        )
    finally:
        source_policy._FACTORY.reset(token)


def _configure_proxy(monkeypatch, entry):
    source_record = entry.source_record
    base = source_record["api_base"]
    binding_by_field = {
        "source_id": source_record["source_id"],
        "endpoint_scope": base,
        "policy_revision": "synthetic-policy",
        "pool_revision": "synthetic-pool",
    }
    policy_by_field = {
        "version": 1,
        "revision": "synthetic-policy",
        "sources": [{"source_id": source_record["source_id"], "endpoint_scope": base, "use_proxy": True}],
    }
    pool_by_field = {
        "version": 1,
        "revision": "synthetic-pool",
        "proxies": [
            {"id": f"synthetic-{index}", "url": f"https://synthetic:synthetic%3Dpassword@127.0.0.1:{10000 + index}"}
            for index in (1, 2)
        ],
    }
    cooldowns_by_field = {
        "version": 1,
        "pool_revision": "synthetic-pool",
        "origin": str(URL(base).origin()),
        "cooldowns": [],
    }
    for name, envelope in (
        (proxy.POLICY_ENV, policy_by_field),
        (proxy.POOL_ENV, pool_by_field),
        (adapter.BINDING_ENV, binding_by_field),
        (adapter.COOLDOWNS_ENV, cooldowns_by_field),
    ):
        monkeypatch.setenv(name, json.dumps(envelope))
    return binding_by_field


def _record_transport(monkeypatch, entry, respond):
    observed = SimpleNamespace(requests=[], sessions=[])

    @contextlib.asynccontextmanager
    async def get(session, url, **options):
        scope = adapter.current_run()
        assert scope is not None and scope.bindings[entry.source_record["source_id"]]
        assert directory._SOURCE_HTTP_SESSION.get() is session and not session.closed
        assert entry.context["context"]["audit"]["source_http_proxy"]["binding"] == scope.expected_binding
        entry.upsert.assert_not_awaited()
        observed.requests.append((str(url), options))
        observed.sessions.append(session)
        status, body = await respond(len(observed.requests), str(url))
        yield SimpleNamespace(status=status, payload=body)

    monkeypatch.setattr(aiohttp.ClientSession, "get", get)
    monkeypatch.setattr(
        directory, "_read_source_http_payload", AsyncMock(side_effect=lambda response, **_kwargs: response.payload)
    )
    return observed


@pytest.mark.asyncio
async def test_entry_qualifies_with_prepared_async_proxy(monkeypatch, source_entry):
    binding_by_field = _configure_proxy(monkeypatch, source_entry)
    base = source_entry.source_record["api_base"]

    async def respond(request_number, url):
        assert url == (f"{base}/metadata?_format=json" if request_number == 1 else f"{base}/Practitioner?_count=1")
        return 200, CAPABILITY if request_number == 1 else {"resourceType": "Bundle", "type": "searchset", "entry": []}

    observed = _record_transport(monkeypatch, source_entry, respond)
    with pytest.raises(RuntimeError, match="qualified source reached storage"):
        await directory.process_data(source_entry.context, source_entry.task)
    qualified = source_entry.upsert.await_args.args[0]
    assert qualified[0]["last_validated_status"] == "valid"
    assert source_entry.source_record["last_validated_status"] == "valid_non_fhir"
    assert len(observed.requests) == 2 and observed.sessions[0] is observed.sessions[1]
    for _url, options in observed.requests:
        assert options["proxy"].startswith("https://127.0.0.1:")
        assert options["proxy_auth"].password == "synthetic=password"
        assert options["ssl"].check_hostname and options["allow_redirects"] is False
        assert "Proxy-Authorization" not in options["headers"]
    assert all(session.closed for session in observed.sessions)
    assert adapter.current_run() is None and directory._SOURCE_HTTP_SESSION.get() is None
    assert source_entry.context["context"]["audit"]["source_http_proxy"]["binding"] == binding_by_field
    directory._fetch_json_sync.assert_not_called()


@pytest.mark.asyncio
async def test_quota_cancellation_retains_audit_and_closes_session(monkeypatch, source_entry):
    source_entry.source_record = directory._source_row_from_seed(
        {
            "id": 17,
            "org_name": "Synthetic Health",
            "api_base": directory.MOLINA_PROVIDER_DIRECTORY_BASE,
            "auth_type": "none",
        }
    )
    source_entry.source_record["last_validated_status"] = "valid_non_fhir"
    monkeypatch.setattr(directory, "_source_row_from_seed", Mock(return_value=source_entry.source_record))
    source_entry.task["source_ids"] = [source_entry.source_record["source_id"]]
    source_entry.task["provider_directory_endpoint_scope"] = source_entry.source_record["api_base"]
    binding_by_field = _configure_proxy(monkeypatch, source_entry)
    pending = asyncio.Event()
    cleanup_by_field = {}

    async def respond(request_number, _url):
        if request_number == 1:
            return 403, {"message": "Out of call volume quota.", directory.SOURCE_RETRY_AFTER_FIELD: "300"}
        assert request_number == 2
        pending.set()
        await asyncio.Future()

    async def invoke():
        try:
            await directory.process_data(source_entry.context, source_entry.task)
        finally:
            cleanup_by_field.update(scope=adapter.current_run(), session=directory._SOURCE_HTTP_SESSION.get())

    observed = _record_transport(monkeypatch, source_entry, respond)
    running = asyncio.create_task(invoke())
    try:
        await asyncio.wait_for(pending.wait(), timeout=3)
        before = source_entry.context["context"]["audit"]["source_http_proxy"]
        assert before["binding"] == binding_by_field and len(before["cooldowns"]["cooldowns"]) == 1
    finally:
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    audit_by_field = source_entry.context["context"]["audit"]["source_http_proxy"]
    assert audit_by_field is before and audit_by_field["binding"] == binding_by_field
    assert audit_by_field["cooldowns"]["pool_revision"] == "synthetic-pool"
    assert audit_by_field["cooldowns"]["cooldowns"][0]["retry_not_before"] is not None
    assert observed.requests[0][0] == observed.requests[1][0]
    assert observed.requests[0][1]["proxy"] != observed.requests[1][1]["proxy"]
    assert all(session.closed for session in observed.sessions)
    assert cleanup_by_field == {"scope": None, "session": None}
    assert adapter.current_run() is None and directory._SOURCE_HTTP_SESSION.get() is None
    assert "synthetic=password" not in json.dumps(audit_by_field)
    source_entry.upsert.assert_not_awaited()
    directory._fetch_json_sync.assert_not_called()


@pytest.mark.asyncio
async def test_unconfigured_entry_keeps_direct_qualification(monkeypatch, source_entry):
    session_factory = Mock(side_effect=AssertionError("unexpected qualification session"))
    monkeypatch.setattr(directory, "_source_http_client_session", session_factory)
    replies = [(200, CAPABILITY, None, 1), (200, {"resourceType": "Bundle", "type": "searchset"}, None, 1)]
    directory._fetch_json_sync.side_effect = replies
    with pytest.raises(RuntimeError, match="qualified source reached storage"):
        await directory.process_data(source_entry.context, source_entry.task)
    assert source_entry.upsert.await_args.args[0][0]["last_validated_status"] == "valid"
    assert directory._fetch_json_sync.call_count == 2
    session_factory.assert_not_called()
    assert adapter.current_run() is None and "audit" not in source_entry.context["context"]


@pytest.mark.asyncio
async def test_endpoint_mismatch_refuses_before_qualification(monkeypatch, source_entry):
    _configure_proxy(monkeypatch, source_entry)
    source_entry.task["provider_directory_endpoint_scope"] = "https://other.example.test/fhir"
    session_factory = Mock(side_effect=AssertionError("unexpected qualification session"))
    monkeypatch.setattr(directory, "_source_http_client_session", session_factory)
    group_resolution = Mock(side_effect=AssertionError("unexpected group resolution"))
    credentials = Mock(side_effect=AssertionError("unexpected credential resolution"))
    monkeypatch.setattr(directory, "_resource_import_group_key", group_resolution)
    monkeypatch.setattr(directory, "_credential_spec_for_source", credentials)
    with pytest.raises(RuntimeError, match="endpoint_scope_mismatch"):
        await directory.process_data(source_entry.context, source_entry.task)
    source_entry.upsert.assert_not_awaited()
    directory._fetch_json_sync.assert_not_called()
    session_factory.assert_not_called()
    group_resolution.assert_not_called()
    credentials.assert_not_called()
    assert adapter.current_run() is None
