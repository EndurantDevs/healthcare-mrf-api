# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source opt-in remains authoritative through async proxy fetch and quota resume."""

import asyncio
import contextlib
import datetime
import importlib
import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from process import provider_directory_source_proxy as adapter
from process.ext import source_http_proxy as proxy

importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://api.interop.molinahealthcare.com/providerdirectory"
SOURCE = {"source_id": "synthetic-source", "canonical_api_base": BASE, "api_base": BASE, "auth_type": "none"}
URL = BASE + "/Practitioner?_count=50&cursorMark=synthetic%2Bcursor%3D&format=json"
BUNDLE = {"resourceType": "Bundle", "type": "searchset", "entry": []}


@pytest.fixture(autouse=True)
def clean_configuration(monkeypatch):
    for name in (proxy.POLICY_ENV, proxy.POOL_ENV, adapter.BINDING_ENV, adapter.COOLDOWNS_ENV):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(importer, "_load_credentials_config", lambda: {})


def configure(monkeypatch, *, aliases=False, binding_revision="policy-1"):
    rules = [{"source_id": SOURCE["source_id"], "endpoint_scope": BASE, "use_proxy": True}]
    if aliases:
        rules.append({"source_id": "synthetic-alias", "endpoint_scope": BASE, "use_proxy": False})
    monkeypatch.setenv(proxy.POLICY_ENV, json.dumps({"version": 1, "revision": "policy-1", "sources": rules}))
    monkeypatch.setenv(
        proxy.POOL_ENV,
        json.dumps(
            {
                "version": 1,
                "revision": "pool-1",
                "proxies": [
                    {
                        "id": f"egress-{index}",
                        "url": f"https://synthetic:synthetic%3Dpassword@127.0.0.1:{10000 + index}",
                    }
                    for index in range(1, 4)
                ],
            }
        ),
    )
    monkeypatch.setenv(
        adapter.BINDING_ENV,
        json.dumps(
            {
                "source_id": SOURCE["source_id"],
                "endpoint_scope": BASE,
                "policy_revision": binding_revision,
                "pool_revision": "pool-1",
            }
        ),
    )
    monkeypatch.setenv(
        adapter.COOLDOWNS_ENV,
        json.dumps(
            {
                "version": 1,
                "pool_revision": "pool-1",
                "origin": "https://api.interop.molinahealthcare.com",
                "cooldowns": [],
            }
        ),
    )


class RecordingSession:
    def __init__(self, responses):
        self.closed = False
        self.responses = list(responses)
        self.requests = []
        self.connector = SimpleNamespace(limit=8, limit_per_host=8)

    @contextlib.asynccontextmanager
    async def get(self, url, **options):
        self.requests.append((str(url), options))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        yield SimpleNamespace(status=response[0], payload=response[1])


@contextlib.asynccontextmanager
async def scope(monkeypatch, responses, records=None, *, session=None):
    token, run = adapter.enter_run(importer)
    session = session or RecordingSession(responses)
    session_token = importer._SOURCE_HTTP_SESSION.set(session)
    monkeypatch.setattr(
        importer, "_read_source_http_payload", AsyncMock(side_effect=lambda response, **_kw: response.payload)
    )
    try:
        await run.prepare(records or [SOURCE])
        yield run, session
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)
        adapter.exit_run(token)


@pytest.mark.asyncio
async def test_opted_source_is_async_and_preserves_encoded_cursor_and_cached_tls(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    monkeypatch.setattr(importer, "_fetch_json_sync", Mock(side_effect=AssertionError("direct fallback")))
    async with scope(monkeypatch, [(200, BUNDLE), (200, BUNDLE)]) as (_run, session):
        for _index in range(2):
            assert (await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2))[:3] == (200, BUNDLE, None)
        first_options, second_options = [options for _url, options in session.requests]
        assert session.requests[0][0] == session.requests[1][0] == URL
        assert first_options["proxy"] == second_options["proxy"]
        assert str(first_options["proxy"]) in {f"https://127.0.0.1:{port}" for port in (10001, 10002, 10003)}
        assert first_options["ssl"] is second_options["ssl"] and first_options["ssl"].check_hostname
        assert first_options["proxy_auth"].password == "synthetic=password"
        assert "synthetic=password" not in repr(first_options["proxy_auth"])
        assert first_options["allow_redirects"] is False
        assert "Proxy-Authorization" not in first_options["headers"]
    assert adapter.current_run() is None


@pytest.mark.asyncio
async def test_unconfigured_source_keeps_direct_transport_despite_shared_pool(monkeypatch):
    configure(monkeypatch)
    direct_by_field = {**SOURCE, "source_id": "unconfigured-source"}
    fetch = AsyncMock(return_value=(200, BUNDLE, None, 1))
    monkeypatch.setattr(importer, "_fetch_json", fetch)
    async with scope(monkeypatch, []) as (_run, session):
        assert await importer._fetch_source_json_unpaced(direct_by_field, URL, timeout=2) == (200, BUNDLE, None, 1)
        assert session.requests == []
    fetch.assert_awaited_once_with(URL, timeout=2)


@pytest.mark.asyncio
async def test_configured_source_refuses_missing_run_scope(monkeypatch):
    configure(monkeypatch)
    with pytest.raises(proxy.SourceProxyError, match="scope_required"):
        await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)


@pytest.mark.parametrize("raw", ["{synthetic-password", "[]", '{"unexpected":"synthetic-password"}'])
def test_malformed_worker_binding_is_sanitized_before_scope_installation(monkeypatch, raw):
    configure(monkeypatch)
    monkeypatch.setenv(adapter.BINDING_ENV, raw)
    with pytest.raises(proxy.SourceProxyError, match="binding_invalid") as failure:
        adapter.enter_run(importer)
    assert "synthetic-password" not in str(failure.value)
    assert adapter.current_run() is None


def test_malformed_cooldown_envelope_refuses_before_scope_installation(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv(adapter.COOLDOWNS_ENV, "{synthetic-password")
    with pytest.raises(proxy.SourceProxyError, match="cooldown_invalid") as failure:
        adapter.enter_run(importer)
    assert "synthetic-password" not in str(failure.value)
    assert adapter.current_run() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["policy-removed", "source-unselected"])
async def test_captured_binding_cannot_disappear_before_preparation(monkeypatch, change):
    configure(monkeypatch)
    records = [SOURCE]
    if change == "policy-removed":
        monkeypatch.delenv(proxy.POLICY_ENV)
    else:
        records = [{**SOURCE, "source_id": "unconfigured-source"}]
    with pytest.raises(proxy.SourceProxyError, match="binding_changed"):
        async with scope(monkeypatch, [], records):
            pytest.fail("changed binding must refuse")
    assert adapter.current_run() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("with_policy", [True, False])
async def test_unselected_run_never_opens_pool_or_emits_proxy_audit(monkeypatch, with_policy):
    configure(monkeypatch)
    for name in (adapter.BINDING_ENV, adapter.COOLDOWNS_ENV):
        monkeypatch.delenv(name)
    if not with_policy:
        monkeypatch.delenv(proxy.POLICY_ENV)
    monkeypatch.setenv(proxy.POOL_ENV, "invalid credential pool must remain unread")
    direct_by_field = {**SOURCE, "source_id": "unconfigured-source"}
    fetch = AsyncMock(return_value=(200, BUNDLE, None, 1))
    monkeypatch.setattr(importer, "_fetch_json", fetch)
    async with scope(monkeypatch, [], [direct_by_field]) as (run, session):
        metrics_by_key = {}
        run.attach_audit(metrics_by_key)
        assert metrics_by_key == {} and not run.enabled(direct_by_field)
        importer.source_proxy.require_direct_transport(importer, direct_by_field)
        assert await importer._fetch_source_json_unpaced(direct_by_field, URL, timeout=2) == (200, BUNDLE, None, 1)
        assert session.requests == []
    assert adapter.selected_run(importer, direct_by_field) is None


@pytest.mark.asyncio
async def test_enabled_dispatch_requires_prepared_scope_before_network(monkeypatch):
    configure(monkeypatch)
    token, _run = adapter.enter_run(importer)
    try:
        with pytest.raises(proxy.SourceProxyError, match="scope_unprepared"):
            await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
    finally:
        adapter.exit_run(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_closed", [False, True])
async def test_proxy_fetch_cannot_fall_back_when_async_session_is_unavailable(monkeypatch, is_closed):
    configure(monkeypatch)
    token, run = adapter.enter_run(importer)
    session_token = importer._SOURCE_HTTP_SESSION.set(SimpleNamespace(closed=True) if is_closed else None)
    direct = Mock(side_effect=AssertionError("direct fallback"))
    monkeypatch.setattr(importer, "_fetch_json_sync", direct)
    try:
        await run.prepare([SOURCE])
        with pytest.raises(proxy.SourceProxyError, match="async_session_required"):
            await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
    finally:
        importer._SOURCE_HTTP_SESSION.reset(session_token)
        adapter.exit_run(token)
    direct.assert_not_called()


@pytest.mark.asyncio
async def test_all_manually_blocked_routes_stop_before_origin_credentials_or_network(monkeypatch):
    configure(monkeypatch)
    async with scope(monkeypatch, []) as (run, session):
        binding = run.bindings[SOURCE["source_id"]]
        for index in range(1, 4):
            binding.defer(f"egress-{index}", None)
        credentials = AsyncMock(side_effect=AssertionError("credential I/O"))
        monkeypatch.setattr(run, "_origin_options", credentials)
        assert await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2) == (
            None,
            None,
            "source_proxy_unavailable",
            0,
        )
        credentials.assert_not_called()
        assert session.requests == []
        assert len(binding.cooldown_snapshot()["cooldowns"]) == 3


@pytest.mark.asyncio
async def test_origin_environment_secret_and_spec_stay_frozen_after_prepare(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv("SYNTHETIC_SOURCE_CREDENTIAL", "synthetic-before")
    credential_by_field = {"headers": {"X-Source-Token": importer.SECRET_ENV_PREFIX + "SYNTHETIC_SOURCE_CREDENTIAL"}}
    monkeypatch.setattr(
        importer, "_load_credentials_config", lambda: {"sources": {SOURCE["source_id"]: credential_by_field}}
    )
    async with scope(monkeypatch, [(200, BUNDLE)]) as (_run, session):
        monkeypatch.setenv("SYNTHETIC_SOURCE_CREDENTIAL", "synthetic-after")
        credential_by_field["headers"]["X-Source-Token"] = "synthetic-mutated"
        await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert session.requests[0][1]["headers"]["X-Source-Token"] == "synthetic-before"


@pytest.mark.asyncio
async def test_task_endpoint_scope_is_checked_before_group_or_credential_resolution(monkeypatch):
    configure(monkeypatch)
    token, run = adapter.enter_run(importer)
    grouping = Mock(side_effect=AssertionError("group credential resolution"))
    monkeypatch.setattr(run, "_group_policies", grouping)
    try:
        with pytest.raises(RuntimeError, match="endpoint_scope"):
            await run.prepare([SOURCE], endpoint_scope="https://foreign.example.test/fhir")
    finally:
        adapter.exit_run(token)
    grouping.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["bulk-source", "bulk-output", "graphql"])
async def test_unsupported_transport_cannot_bypass_source_proxy(monkeypatch, transport):
    configure(monkeypatch)
    direct_fetch = Mock(side_effect=AssertionError("unsupported direct request"))
    monkeypatch.setattr(importer, "_post_json_sync", direct_fetch)
    async with scope(monkeypatch, []) as (run, session):
        monkeypatch.setattr(run, "_origin_options", AsyncMock(side_effect=AssertionError("origin credentials")))
        with pytest.raises(proxy.SourceProxyError, match="transport_unsupported"):
            if transport == "bulk-source":
                importer._bulk_export_source_request_options(SOURCE, BASE + "/$export")
            elif transport == "bulk-output":
                importer._bulk_export_output_request_options(SOURCE, BASE + "/output", requires_access_token=False)
            else:
                await importer._probe_alohr_graphql_connector(SOURCE, timeout=2, run_id=None)
        assert session.requests == []
    direct_fetch.assert_not_called()


def official_file_source(monkeypatch, *, enabled=True):
    source_record = importer._uhc_official_file_source_row(
        "synthetic-endpoint", observed_at=datetime.datetime(2035, 1, 1, tzinfo=datetime.UTC)
    )
    assert importer._uses_uhc_official_file_connector(source_record)
    monkeypatch.setenv(
        proxy.POLICY_ENV,
        json.dumps(
            {
                "version": 1,
                "revision": "policy-1",
                "sources": [
                    {
                        "source_id": source_record["source_id"],
                        "endpoint_scope": source_record["canonical_api_base"],
                        "use_proxy": enabled,
                    }
                ],
            }
        ),
    )
    monkeypatch.setenv(proxy.POOL_ENV, "unreadable pool must not be loaded")
    return source_record


def graphql_source(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setenv(proxy.POOL_ENV, "unreadable pool must not be loaded")
    source_by_field = {
        **SOURCE,
        "metadata_json": {"provider_directory_graphql_tenant_id": importer.ALOHR_TENANT_ID},
    }
    assert importer._uses_alohr_graphql_connector(source_by_field)
    return source_by_field


@pytest.mark.asyncio
async def test_graphql_prepare_refuses_before_grouping_pool_or_credentials(monkeypatch):
    source_record = graphql_source(monkeypatch)
    token, run = adapter.enter_run(importer)
    grouping = Mock(side_effect=AssertionError("credential grouping"))
    bind = Mock(side_effect=AssertionError("secret pool"))
    monkeypatch.setattr(run, "_group_policies", grouping)
    monkeypatch.setattr(run.config, "bind", bind)
    try:
        with pytest.raises(proxy.SourceProxyError, match="transport_unsupported"):
            await run.prepare([source_record])
        assert run.bindings == {} and run.credentials == {}
    finally:
        adapter.exit_run(token)
    grouping.assert_not_called()
    bind.assert_not_called()


@pytest.mark.asyncio
async def test_graphql_acquisition_without_probe_refuses_before_persistence_or_network(monkeypatch):
    source_record = graphql_source(monkeypatch)
    for name in (
        "ensure_database",
        "_prepare_provider_directory_job_tables",
        "_clear_resource_rows_seen",
        "_mark_provider_directory_progress",
    ):
        monkeypatch.setattr(importer, name, AsyncMock())
    monkeypatch.setattr(importer, "_source_row_from_seed", lambda _seed: source_record)
    monkeypatch.setattr(importer, "_resolve_retest_results", lambda *_args: (None, None))
    persistence = AsyncMock(side_effect=AssertionError("source persistence"))
    request = Mock(side_effect=AssertionError("direct GraphQL request"))
    monkeypatch.setattr(importer, "_upsert_provider_directory_source_rows", persistence)
    monkeypatch.setattr(importer, "_post_json_sync", request)
    with pytest.raises(proxy.SourceProxyError, match="transport_unsupported"):
        await importer.process_provider_directory_fhir_data(
            {},
            {
                "test_mode": True,
                "source_ids": [source_record["source_id"]],
                "probe": False,
                "import_resources": True,
                "defer_typed_materialization": False,
                "publish_artifacts": False,
                "publish_corroboration": False,
            },
        )
    assert adapter.current_run() is None
    persistence.assert_not_called()
    request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_state", ["absent", "unprepared", "prepared"])
@pytest.mark.parametrize("enabled_index", [0, 1])
async def test_graphql_acquisition_checks_every_source_before_initialization(monkeypatch, scope_state, enabled_index):
    source_record = graphql_source(monkeypatch)
    source_records = [{**source_record, "source_id": "unconfigured-source"}]
    source_records.insert(enabled_index, source_record)
    token, run = adapter.enter_run(importer) if scope_state != "absent" else (None, None)
    if scope_state == "prepared":
        run.bindings[source_record["source_id"]] = object()
    initialize = Mock(side_effect=AssertionError("GraphQL stream initialization"))
    monkeypatch.setattr(importer, "_initialize_alohr_graphql_import_state", initialize)
    errors_by_state = {
        "absent": "scope_required",
        "unprepared": "scope_unprepared",
        "prepared": "transport_unsupported",
    }
    try:
        with pytest.raises(proxy.SourceProxyError, match=errors_by_state[scope_state]):
            await importer._import_alohr_graphql_source_group(source_records, ["Practitioner"], None)
    finally:
        if token is not None:
            adapter.exit_run(token)
    initialize.assert_not_called()


@pytest.mark.asyncio
async def test_unselected_graphql_source_keeps_its_direct_connector(monkeypatch):
    source_by_field = {**graphql_source(monkeypatch), "source_id": "unconfigured-source"}
    initialize = Mock(side_effect=RuntimeError("synthetic direct GraphQL acquisition"))
    monkeypatch.setattr(importer, "_initialize_alohr_graphql_import_state", initialize)
    with pytest.raises(RuntimeError, match="synthetic direct GraphQL acquisition"):
        await importer._import_alohr_graphql_source_group([source_by_field], ["Practitioner"], None)
    initialize.assert_called_once_with([source_by_field], ["Practitioner"], None)


@pytest.mark.asyncio
async def test_official_file_prepare_refuses_before_grouping_pool_credentials_or_network(monkeypatch):
    source_record = official_file_source(monkeypatch)
    token, run = adapter.enter_run(importer)
    grouping = Mock(side_effect=AssertionError("credential grouping"))
    bind = Mock(side_effect=AssertionError("secret pool"))
    credentials = Mock(side_effect=AssertionError("source credentials"))
    catalog = AsyncMock(side_effect=AssertionError("catalog network"))
    acquisition = AsyncMock(side_effect=AssertionError("file network"))
    monkeypatch.setattr(run, "_group_policies", grouping)
    monkeypatch.setattr(run.config, "bind", bind)
    monkeypatch.setattr(importer, "_credential_spec_for_source", credentials)
    monkeypatch.setattr(importer, "refresh_uhc_provider_file_catalog", catalog)
    monkeypatch.setattr(importer, "acquire_complete_uhc_catalog_set", acquisition)
    try:
        with pytest.raises(proxy.SourceProxyError, match="transport_unsupported"):
            await run.prepare([source_record])
        metrics_by_key = {}
        run.attach_audit(metrics_by_key)
        assert metrics_by_key == {} and run.bindings == {} and run.credentials == {}
    finally:
        adapter.exit_run(token)
    for operation in (grouping, bind, credentials, catalog, acquisition):
        operation.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["probe", "import"])
@pytest.mark.parametrize("scope_state", ["absent", "unprepared", "prepared"])
async def test_official_file_direct_entries_cannot_bypass_proxy_policy(monkeypatch, operation, scope_state):
    source_record = official_file_source(monkeypatch)
    token, run = adapter.enter_run(importer) if scope_state != "absent" else (None, None)
    if scope_state == "prepared":
        run.bindings[source_record["source_id"]] = object()
    catalog = AsyncMock(side_effect=AssertionError("catalog network"))
    acquisition = AsyncMock(side_effect=AssertionError("file acquisition"))
    credentials = Mock(side_effect=AssertionError("source credentials"))
    monkeypatch.setattr(importer, "refresh_uhc_provider_file_catalog", catalog)
    monkeypatch.setattr(importer, "_acquire_and_build_uhc_stage", acquisition)
    monkeypatch.setattr(importer, "_credential_spec_for_source", credentials)
    errors_by_state = {
        "absent": "scope_required",
        "unprepared": "scope_unprepared",
        "prepared": "transport_unsupported",
    }
    try:
        with pytest.raises(proxy.SourceProxyError, match=errors_by_state[scope_state]):
            if operation == "probe":
                await importer._probe_uhc_official_file_connector(source_record, timeout=1, run_id=None)
            else:
                await importer._import_uhc_official_file_source_group(
                    [source_record], ["Practitioner"], ctx={}, task={}, run_id="synthetic-run"
                )
    finally:
        if token is not None:
            adapter.exit_run(token)
    for request in (catalog, acquisition, credentials):
        request.assert_not_called()


@pytest.mark.asyncio
async def test_unselected_official_file_source_keeps_its_direct_connector(monkeypatch):
    source_record = official_file_source(monkeypatch, enabled=False)
    catalog = AsyncMock(return_value={"catalog_set_sha256": "a" * 64, "catalog_file_count": 1})
    acquisition = AsyncMock(side_effect=RuntimeError("synthetic direct acquisition"))
    monkeypatch.setattr(importer, "refresh_uhc_provider_file_catalog", catalog)
    monkeypatch.setattr(importer, "_acquire_and_build_uhc_stage", acquisition)
    probe, capability = await importer._probe_uhc_official_file_connector(source_record, timeout=1, run_id=None)
    assert probe["status"] == "valid" and probe["transport"] == "official_provider_files" and capability is None
    catalog.assert_awaited_once_with()
    with pytest.raises(RuntimeError, match="synthetic direct acquisition"):
        await importer._import_uhc_official_file_source_group(
            [source_record], ["Practitioner"], ctx={}, task={}, run_id="synthetic-run"
        )
    acquisition.assert_awaited_once_with({}, {}, "synthetic-run")


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["binding", "cooldowns"])
async def test_missing_or_changed_frozen_binding_refuses_network(monkeypatch, changed):
    configure(monkeypatch, binding_revision="wrong" if changed == "binding" else "policy-1")
    if changed == "cooldowns":
        monkeypatch.delenv(adapter.COOLDOWNS_ENV)
    with pytest.raises(proxy.SourceProxyError, match="binding"):
        async with scope(monkeypatch, []):
            pytest.fail("scope must refuse")


@pytest.mark.asyncio
async def test_alias_disagreement_is_refused_before_fetch(monkeypatch):
    configure(monkeypatch, aliases=True)
    alias_by_field = {**SOURCE, "source_id": "synthetic-alias"}
    with pytest.raises(proxy.SourceProxyError, match="alias_policy_mismatch"):
        async with scope(monkeypatch, [], [SOURCE, alias_by_field]):
            pytest.fail("scope must refuse")


@pytest.mark.asyncio
async def test_signed_census_refuses_proxy_before_credentials_or_network(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(importer, "current_version_census_contract", lambda _source: object())
    credentials = Mock(side_effect=AssertionError("credential resolution"))
    monkeypatch.setattr(importer, "_credential_spec_for_source", credentials)
    with pytest.raises(proxy.SourceProxyError, match="census_contract_forbidden"):
        async with scope(monkeypatch, []):
            pytest.fail("scope must refuse")
    credentials.assert_not_called()


def quota(delay):
    return 403, {"message": "Out of call volume quota.", importer.SOURCE_RETRY_AFTER_FIELD: str(delay)}


@pytest.mark.asyncio
async def test_quota_failover_replays_exact_cursor_and_never_clears_block_on_success(monkeypatch):
    configure(monkeypatch)
    async with scope(monkeypatch, [quota(300), (200, BUNDLE), (200, BUNDLE)]) as (run, session):
        fetch_result = await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert fetch_result[:3] == (200, BUNDLE, None)
        await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert [url for url, _options in session.requests] == [URL] * 3
        routes = [options["proxy"] for _url, options in session.requests]
        assert routes[0] != routes[1] == routes[2]
        assert len(run.bindings[SOURCE["source_id"]].cooldown_snapshot()["cooldowns"]) == 1


@pytest.mark.asyncio
async def test_quota_failover_tries_each_route_once_even_after_cooldowns_expire(monkeypatch):
    configure(monkeypatch)
    clock_values = [datetime.datetime.now(datetime.UTC)]
    monkeypatch.setattr(proxy, "_utcnow", lambda: clock_values[0])

    class SlowQuotaSession(RecordingSession):
        @contextlib.asynccontextmanager
        async def get(self, url, **options):
            self.requests.append((str(url), options))
            assert len(self.requests) <= 3, "a logical fetch revisited an attempted route"
            clock_values[0] += datetime.timedelta(seconds=120)
            status, payload = quota(1)
            yield SimpleNamespace(status=status, payload=payload)

    session = SlowQuotaSession([])
    async with scope(monkeypatch, [], session=session) as (run, _session):
        fetch_result = await asyncio.wait_for(importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2), timeout=1)
        assert fetch_result[0] == 403
        assert len(session.requests) == 3
        assert len({options["proxy"] for _url, options in session.requests}) == 3
        assert len(run.bindings[SOURCE["source_id"]].cooldown_snapshot()["cooldowns"]) == 3


@pytest.mark.asyncio
async def test_proxy_connect_failure_uses_healthy_route_without_global_cooldown(monkeypatch):
    configure(monkeypatch)
    pace = AsyncMock()
    monkeypatch.setattr(importer, "_pace_source_request", pace)
    failure = aiohttp.ClientProxyConnectionError(None, OSError("synthetic connection refused"))
    async with scope(monkeypatch, [failure, (200, BUNDLE)]) as (run, session):
        assert (await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2))[:3] == (200, BUNDLE, None)
        assert [url for url, _options in session.requests] == [URL, URL]
        assert session.requests[0][1]["proxy"] != session.requests[1][1]["proxy"]
        assert run.bindings[SOURCE["source_id"]].cooldown_snapshot()["cooldowns"] == []
    assert pace.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("is_quota", [True, False])
async def test_proxy_failover_accounts_for_every_attempt_duration(monkeypatch, is_quota):
    configure(monkeypatch)
    failure = (*quota(300), None, 13) if is_quota else (None, None, "ClientProxyConnectionError", 13)
    fetch = AsyncMock(side_effect=[failure, (200, BUNDLE, None, 17)])
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", fetch)
    async with scope(monkeypatch, []) as (run, _session):
        assert await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2) == (200, BUNDLE, None, 30)
        if is_quota:
            assert run._last_quota[SOURCE["source_id"]][3] == 0


@pytest.mark.asyncio
async def test_all_proxy_connect_failures_stop_after_one_attempt_per_route(monkeypatch):
    configure(monkeypatch)
    failures = [aiohttp.ClientProxyConnectionError(None, OSError("synthetic connection refused")) for _ in range(3)]
    async with scope(monkeypatch, failures) as (_run, session):
        fetch_result = await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert fetch_result[:3] == (None, None, "ClientProxyConnectionError")
        assert len(session.requests) == 3
        assert len({options["proxy"] for _url, options in session.requests}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("error", ["ClientConnectorCertificateError", "ClientHttpProxyError", "ServerTimeoutError"])
async def test_origin_and_ambiguous_proxy_errors_do_not_rotate(monkeypatch, error):
    configure(monkeypatch)
    fetch = AsyncMock(return_value=(None, None, error, 1))
    monkeypatch.setattr(importer, "_fetch_json_with_source_session", fetch)
    async with scope(monkeypatch, []) as (_run, _session):
        assert await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2) == (None, None, error, 1)
    assert fetch.await_count == 1


@pytest.mark.asyncio
async def test_all_blocked_stops_with_earliest_reset_and_preserves_cooldowns(monkeypatch):
    configure(monkeypatch)
    started = datetime.datetime.now(datetime.UTC)
    async with scope(monkeypatch, [quota(100), quota(20), quota(50)]) as (run, session):
        metrics_by_key = {}
        run.attach_audit(metrics_by_key)
        fetch_result = await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert fetch_result[0] == 403 and len(session.requests) == 3
        retry_at = datetime.datetime.fromisoformat(
            importer._molina_quota_retry_not_before(SOURCE, fetch_result[0], fetch_result[1])
        )
        expected = started.replace(microsecond=0) + datetime.timedelta(
            seconds=20 + importer.MOLINA_QUOTA_RESET_GRACE_SECONDS
        )
        assert 0 <= (retry_at - expected).total_seconds() < 3
        snapshot = run.bindings[SOURCE["source_id"]].cooldown_snapshot()
        earliest = min(datetime.datetime.fromisoformat(item["retry_not_before"]) for item in snapshot["cooldowns"])
        assert 0 <= (retry_at - earliest).total_seconds() <= 1
        assert len(snapshot["cooldowns"]) == 3 and metrics_by_key["source_http_proxy"]["cooldowns"] == snapshot
    monkeypatch.setenv(adapter.COOLDOWNS_ENV, json.dumps(snapshot))
    async with scope(monkeypatch, []) as (_run, session):
        fetch_result = await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert session.requests == [] and fetch_result[0] == 403
        assert (
            importer._source_fetch_result_with_payload_error(SOURCE, URL, fetch_result)[2]
            == importer.SOURCE_QUOTA_EXHAUSTED_ERROR
        )


@pytest.mark.asyncio
async def test_ordinary_forbidden_and_origin_errors_do_not_rotate(monkeypatch):
    configure(monkeypatch)
    forbidden_by_field = {"resourceType": "OperationOutcome", "issue": [{"diagnostics": "Forbidden"}]}
    async with scope(monkeypatch, [(403, forbidden_by_field)]) as (_run, session):
        assert (await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2))[:3] == (
            403,
            forbidden_by_field,
            None,
        )
        assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_query_credentials_preserve_cursor_bytes_and_reject_conflicts(monkeypatch):
    configure(monkeypatch)
    credential_by_field = {"query_params": {"api-key": "synthetic=value+"}}
    monkeypatch.setattr(
        importer, "_load_credentials_config", lambda: {"sources": {SOURCE["source_id"]: credential_by_field}}
    )
    async with scope(monkeypatch, [(200, BUNDLE)]) as (_run, session):
        await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert session.requests[0][0] == URL + "&api-key=synthetic%3Dvalue%2B"
        with pytest.raises(proxy.SourceProxyError, match="credential_conflict"):
            await importer._fetch_source_json_unpaced(SOURCE, URL + "&api-key=other", timeout=2)
        assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_credential_specs_freeze_and_resolution_does_not_block_event_loop(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        lambda: {
            "sources": {
                SOURCE["source_id"]: {
                    "oauth2": {
                        "token_url": "https://issuer.example.test/token",
                        "client_id": "synthetic-client",
                        "client_secret": "synthetic-secret",
                    }
                }
            }
        },
    )
    monkeypatch.setattr(importer, "_fetch_oauth2_client_token_sync", lambda *_args, **_kwargs: "synthetic-original")
    main_thread = threading.get_ident()
    resolve = importer._credential_request_options_for_source
    async with scope(monkeypatch, [(200, BUNDLE)]) as (_run, session):

        def slow_resolution(*args, **kwargs):
            assert threading.get_ident() != main_thread
            time.sleep(0.04)
            return resolve(*args, **kwargs)

        monkeypatch.setattr(importer, "_credential_request_options_for_source", slow_resolution)
        monkeypatch.setattr(importer, "_load_credentials_config", Mock(side_effect=AssertionError("credential reload")))
        fetch = asyncio.create_task(importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2))
        await asyncio.sleep(0.01)
        assert not fetch.done()
        assert (await fetch)[:3] == (200, BUNDLE, None)
        assert session.requests[0][1]["headers"]["Authorization"] == "Bearer synthetic-original"


@pytest.mark.asyncio
async def test_origin_proxy_auth_header_is_refused(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        lambda: {"sources": {SOURCE["source_id"]: {"headers": {"proxy-authorization": "synthetic"}}}},
    )
    async with scope(monkeypatch, []) as (_run, session):
        with pytest.raises(proxy.SourceProxyError, match="origin_header_forbidden"):
            await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        assert session.requests == []


@pytest.mark.asyncio
async def test_matching_query_credential_in_next_url_is_not_appended_again(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(
        importer,
        "_load_credentials_config",
        lambda: {
            "sources": {SOURCE["source_id"]: {"query_params": {"api-key": "synthetic=value+"}}},
        },
    )
    authenticated_url = URL + "&api-key=synthetic%3Dvalue%2B"
    async with scope(monkeypatch, [(200, BUNDLE)]) as (_run, session):
        await importer._fetch_source_json_unpaced(SOURCE, authenticated_url, timeout=2)
        assert session.requests[0][0] == authenticated_url
        with pytest.raises(proxy.SourceProxyError, match="credential_conflict"):
            await importer._fetch_source_json_unpaced(
                SOURCE, authenticated_url + "&api-key=synthetic%3Dvalue%2B", timeout=2
            )


@pytest.mark.asyncio
async def test_target_scope_is_checked_before_origin_credential_resolution(monkeypatch):
    configure(monkeypatch)
    async with scope(monkeypatch, []) as (run, session):
        options = AsyncMock(side_effect=AssertionError("origin credential I/O"))
        monkeypatch.setattr(run, "_origin_options", options)
        with pytest.raises(proxy.SourceProxyError, match="target_invalid"):
            await importer._fetch_source_json_unpaced(
                SOURCE, "https://foreign.example.test/fhir/Practitioner", timeout=2
            )
        options.assert_not_called()
        assert session.requests == []


@pytest.mark.asyncio
async def test_eight_cursors_share_limit_and_pace_quota_failover(monkeypatch):
    configure(monkeypatch)
    pace = AsyncMock()
    monkeypatch.setattr(importer, "_pace_source_request", pace)
    source_by_field = {**SOURCE, "_provider_directory_request_pacing_state": {}}

    class ConcurrentSession(RecordingSession):
        active = maximum = 0

        @contextlib.asynccontextmanager
        async def get(self, url, **options):
            self.requests.append((str(url), options))
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            try:
                await asyncio.sleep(0.01)
                status, payload = quota(300) if options["proxy"].endswith(":10001") else (200, BUNDLE)
                yield SimpleNamespace(status=status, payload=payload)
            finally:
                self.active -= 1

    session = ConcurrentSession([])
    async with scope(monkeypatch, [], [source_by_field], session=session) as (run, _session):
        resources = [
            "InsurancePlan",
            "PractitionerRole",
            "Practitioner",
            "Organization",
            "Location",
            "HealthcareService",
            "OrganizationAffiliation",
            "Endpoint",
        ]
        responses = await asyncio.wait_for(
            asyncio.gather(
                *[
                    importer._fetch_source_json_once(source_by_field, BASE + "/" + resource + "?_count=50", timeout=2)
                    for resource in resources
                ]
            ),
            timeout=5,
        )
        assert all(fetch_result[:3] == (200, BUNDLE, None) for fetch_result in responses)
        assert 2 <= session.maximum <= 8 and session.active == 0
        assert pace.await_count == len(session.requests)
        assert len(run.bindings[SOURCE["source_id"]].cooldown_snapshot()["cooldowns"]) == 1


@pytest.mark.asyncio
async def test_cancellation_releases_request_slot_without_clearing_policy(monkeypatch):
    configure(monkeypatch)
    monkeypatch.setattr(importer, "_pace_source_request", AsyncMock())
    source_by_field = {**SOURCE, "_provider_directory_request_pacing_state": {}}
    entered = asyncio.Event()

    class WaitingSession(RecordingSession):
        active = 0

        @contextlib.asynccontextmanager
        async def get(self, url, **options):
            self.requests.append((str(url), options))
            self.active += 1
            try:
                entered.set()
                await asyncio.Event().wait()
                yield SimpleNamespace(status=200, payload=BUNDLE)
            finally:
                self.active -= 1

    session = WaitingSession([])
    async with scope(monkeypatch, [], [source_by_field], session=session) as (run, _session):
        fetch = asyncio.create_task(importer._fetch_source_json_once(source_by_field, URL, timeout=2))
        await asyncio.wait_for(entered.wait(), timeout=2)
        fetch.cancel()
        with pytest.raises(asyncio.CancelledError):
            await fetch
        assert session.active == 0
        assert source_by_field["_provider_directory_request_pacing_state"]["request_slots"].active == 0
        assert run.expected_binding["policy_revision"] == "policy-1"
    assert adapter.current_run() is None


@pytest.mark.asyncio
async def test_terminal_audit_retains_nonsecret_binding_and_cooldowns(monkeypatch):
    from process.control_lifecycle import _terminal_metrics_from_context

    configure(monkeypatch)
    async with scope(monkeypatch, [quota(100), quota(20), quota(50)]) as (run, _session):
        metrics_by_key = {}
        run.attach_audit(metrics_by_key)
        await importer._fetch_source_json_unpaced(SOURCE, URL, timeout=2)
        terminal = _terminal_metrics_from_context({"audit": metrics_by_key})
        assert terminal["audit"]["source_http_proxy"]["binding"] == run.expected_binding
        assert len(terminal["audit"]["source_http_proxy"]["cooldowns"]["cooldowns"]) == 3
        serialized_terminal = json.dumps(terminal)
        for secret in ("synthetic=password", "synthetic%3Dpassword", "synthetic:synthetic"):
            assert secret not in serialized_terminal
