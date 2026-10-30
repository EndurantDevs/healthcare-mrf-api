# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import contextlib
import dataclasses
import importlib
import json
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.test_process_provider_directory_fhir_unit import (
    _checkpoint_complete_fetch_result,
    _parallel_resource_source,
    _patch_parallel_resource_flow,
    _resource_concurrency_options,
    _RestPageHarness,
    _source_http_test_session,
    build_subset_contract,
    unit_checkpoint_catalog,
    unit_source_factory,
)

importer = importlib.import_module("process.provider_directory_fhir")
source_adapters = importlib.import_module("process.provider_directory_generic_source_adapters")


def _configure_neutral_parallel_source(source_record, cap):
    """Bind the existing complete checkpoint fixture to an ordinary configured source."""
    api_base = "https://directory.example.test/fhir"
    source_record.update(api_base=api_base, canonical_api_base=api_base, metadata_json={})
    source_record["_pagination_checkpoint_context"] = dataclasses.replace(
        source_record["_pagination_checkpoint_context"], canonical_api_base=api_base
    )
    if cap is not None:
        source_record["metadata_json"]["provider_directory_resource_scan_concurrency"] = cap
    return source_record


@pytest.mark.parametrize(
    ("cap", "requested", "is_source_storage", "pool_capacity", "expected"),
    [
        (2, 2, False, 3, 2),
        (None, 1, False, 4, 1),
        (None, 2, False, 4, 2),
        (None, 3, False, 4, 3),
        (1, 3, False, 4, 1),
        (2, 3, False, 4, 2),
        (3, 2, False, 4, 2),
        (3, 3, False, 4, 3),
        (3, 3, False, 3, 1),
        (3, 3, True, 2, 2),
        (2, 2, True, 2, 2),
    ],
)
def test_configured_resource_scans_respect_source_and_connection_caps(
    monkeypatch, cap, requested, is_source_storage, pool_capacity, expected
):
    """Opt in ordinary endpoints without exceeding their explicit or storage-specific cap."""
    source_record = _configure_neutral_parallel_source(_parallel_resource_source(), cap)
    options = _resource_concurrency_options(
        requested_concurrency=requested, resource_types=("Location", "HealthcareService", "Organization")
    )
    monkeypatch.setattr(importer, "is_source_storage_required", lambda: is_source_storage)
    monkeypatch.setattr(importer, "_provider_directory_database_pool_capacity", lambda: pool_capacity)
    with _source_http_test_session():
        assert importer._effective_resource_scan_concurrency(source_record, options) == expected


@pytest.mark.parametrize("cap", [0, -1, 4, True, 2.0, "invalid", [], {}])
def test_source_resource_cap_rejects_invalid_metadata(cap):
    """Malformed source policy must not silently select a parallel default."""
    source_record = _configure_neutral_parallel_source(_parallel_resource_source(), cap)
    with pytest.raises(ValueError, match="provider_directory_resource_scan_concurrency_invalid"):
        importer._effective_resource_scan_concurrency(source_record, _resource_concurrency_options())


def test_resource_concurrency_rejects_decimal_input_exceeding_integer_conversion_limit():
    """Translate Python's numeric parsing refusal into the public configuration error."""
    previous_limit = sys.get_int_max_str_digits()
    try:
        sys.set_int_max_str_digits(640)
        with pytest.raises(ValueError, match="provider_directory_resource_scan_concurrency_invalid") as caught:
            importer._validated_resource_scan_concurrency("1" * 641)
        assert isinstance(caught.value.__cause__, ValueError)
    finally:
        sys.set_int_max_str_digits(previous_limit)


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"request_concurrency": 0}, "request"),
        ({"request_concurrency": 101}, "request"),
        ({"request_concurrency": True}, "request"),
        ({"request_concurrency": "2"}, "request"),
        ({"source_concurrency": 0}, "source"),
        ({"source_concurrency": True}, "source"),
        ({"source_concurrency": "2"}, "source"),
    ],
)
def test_http_concurrency_refuses_invalid_caps_before_allocating_transport(monkeypatch, options, error):
    """Bad budgets must fail before allocating a connector that needs asynchronous cleanup."""
    connector = Mock()
    monkeypatch.setattr(importer.aiohttp, "TCPConnector", connector)
    with pytest.raises(ValueError, match=f"provider_directory_{error}_concurrency_invalid"):
        importer._source_http_client_session(**options)
    connector.assert_not_called()


@pytest.mark.parametrize(("host_cap", "source_cap", "expected"), [(3, 1, 1), (1, 3, 1), (3, 3, 2), (2, None, 2)])
async def test_partition_windows_share_actual_transport_and_source_caps(host_cap, source_cap, expected):
    """An eligible anonymous partition scan obeys the narrower source or host budget."""
    source_record = _configure_neutral_parallel_source(_parallel_resource_source(), source_cap)
    async with importer._source_http_session_scope(force=True, request_concurrency=host_cap):
        session = importer._SOURCE_HTTP_SESSION.get()
        assert (
            importer._partition_window_concurrency(source_record, source_record["api_base"] + "/Location") == expected
        )
    assert session.closed


def test_reviewed_census_remains_serial_despite_source_parallel_opt_in(monkeypatch):
    """An ordinary concurrency setting cannot alter a reviewed traversal contract."""
    source_record = _configure_neutral_parallel_source(_parallel_resource_source(), 3)
    source_record[importer.CURRENT_VERSION_CENSUS_CONTRACT_FIELD] = build_subset_contract(source_id="source_a")
    monkeypatch.setattr(importer, "_provider_directory_database_pool_capacity", lambda: 4)
    with _source_http_test_session():
        assert importer._effective_resource_scan_concurrency(source_record, _resource_concurrency_options()) == 1


async def test_published_resource_estimates_ignore_noninteger_and_unknown_counts(monkeypatch):
    """Only known resources with nonnegative integer counts affect scan priority."""
    read_header = AsyncMock(
        return_value={
            "status": importer.ENDPOINT_DATASET_PUBLISHED,
            "resource_counts": {
                "Location": 12,
                "Organization": 0,
                "PractitionerRole": True,
                "Practitioner": 3.0,
                "HealthcareService": -1,
                "Endpoint": "4",
                "InsurancePlan": [],
                "OrganizationAffiliation": {},
                "Unknown": 100,
            },
        }
    )
    monkeypatch.setattr(source_adapters, "current_endpoint_dataset_record", read_header)
    estimates_by_resource = await source_adapters.resource_scan_estimates(
        importer, {"_endpoint_id": "endpoint-a", "_endpoint_dataset_id": "candidate-a"}
    )
    assert estimates_by_resource == {"Location": 12, "Organization": 0}
    read_header.assert_awaited_once_with(importer, "endpoint-a", exclude_dataset_id="candidate-a")


@pytest.mark.parametrize(
    ("endpoint_id", "status", "counts"),
    [
        (None, "published", {"Location": 12}),
        ("endpoint-a", "validated", {"Location": 12}),
        ("endpoint-a", "published", [{"Location": 12}]),
        ("endpoint-a", "published", True),
        ("endpoint-a", "published", -1),
    ],
)
async def test_missing_or_unpublished_resource_estimates_cannot_prioritize(monkeypatch, endpoint_id, status, counts):
    """Absent incumbents and malformed count containers retain natural scan order."""
    read_header = AsyncMock(return_value={"status": status, "resource_counts": counts})
    monkeypatch.setattr(source_adapters, "current_endpoint_dataset_record", read_header)
    assert await source_adapters.resource_scan_estimates(importer, {"_endpoint_id": endpoint_id}) == {}
    if endpoint_id is None:
        read_header.assert_not_awaited()


async def test_larger_foundation_scans_start_first_and_role_waits_for_every_foundation(monkeypatch):
    """Use size estimates without changing stable ties, unknown order, or the role barrier."""
    requested_resources = [
        "PractitionerRole",
        "Organization",
        "Location",
        "Endpoint",
        "HealthcareService",
        "Practitioner",
    ]
    expected_foundations = ["Location", "HealthcareService", "Practitioner", "Organization", "Endpoint"]
    lifecycle_events = []

    async def fetch(_source, resource_type, **_options):
        if resource_type == "PractitionerRole":
            assert {resource for event, resource in lifecycle_events if event == "finish"} == set(expected_foundations)
        lifecycle_events.append(("start", resource_type))
        await asyncio.sleep(0)
        lifecycle_events.append(("finish", resource_type))
        return _checkpoint_complete_fetch_result(importer.RESOURCE_MODELS_BY_TYPE[resource_type], 0)

    source_record = _configure_neutral_parallel_source(_patch_parallel_resource_flow(monkeypatch, fetch), 2)
    read_header = AsyncMock(
        return_value={
            "status": importer.ENDPOINT_DATASET_PUBLISHED,
            "resource_counts": {"PractitionerRole": 1000, "Location": 20, "HealthcareService": 5, "Practitioner": 5},
        }
    )
    monkeypatch.setattr(source_adapters, "current_endpoint_dataset_record", read_header)
    with _source_http_test_session():
        await importer._import_resources(
            [source_record],
            resources=requested_resources,
            per_resource_limit=0,
            page_limit=0,
            page_count=100,
            timeout=3,
            run_id="run_1",
            stream_batch_size=100,
            source_concurrency=1,
            resource_scan_concurrency=2,
            is_pagination_checkpointing_enabled=True,
            defer_typed_materialization=True,
        )
    assert [resource for event, resource in lifecycle_events if event == "start"] == expected_foundations + [
        "PractitionerRole"
    ]


@pytest.mark.usefixtures("unit_checkpoint_catalog")
@pytest.mark.parametrize(
    ("environment_enabled", "override", "expected"),
    [
        (None, None, False),
        (True, None, True),
        (False, None, False),
        (None, True, True),
        (False, True, False),
        ("invalid", True, False),
        (True, False, False),
        (None, False, False),
    ],
)
async def test_prefetch_override_preserves_default_and_explicit_disable(
    monkeypatch, environment_enabled, override, expected
):
    page_scan = _RestPageHarness(monkeypatch, prefetch=bool(environment_enabled))
    if environment_enabled is None:
        monkeypatch.delenv(importer.SOURCE_HTTP_PAGE_PREFETCH_ENV, raising=False)
    elif isinstance(environment_enabled, str):
        monkeypatch.setenv(importer.SOURCE_HTTP_PAGE_PREFETCH_ENV, environment_enabled)
    token = importer._REST_PAGE_PREFETCH_OVERRIDE.set(override)
    try:
        result = await page_scan.run()
    finally:
        importer._REST_PAGE_PREFETCH_OVERRIDE.reset(token)

    assert result.complete
    assert result.rows_written == 2
    assert result.page_prefetch_eligible is expected
    assert result.page_prefetch_started == int(expected)
    assert result.page_prefetch_consumed == int(expected)
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


@pytest.fixture
def tracked_http_sessions(monkeypatch):
    """Keep the real task-local scope while replacing network session allocation."""
    sessions = []

    @contextlib.asynccontextmanager
    async def client_session(**_kwargs):
        session = SimpleNamespace(closed=False)
        sessions.append(session)
        try:
            yield session
        finally:
            session.closed = True

    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    monkeypatch.setattr(importer, "_source_http_client_session", client_session)
    return sessions


@pytest.mark.parametrize("import_count", [2, 10])
async def test_prefetch_request_settings_are_isolated_between_concurrent_imports(
    monkeypatch, tracked_http_sessions, import_count
):
    """Each overlapping import owns its session and child-task prefetch setting."""
    all_started = asyncio.Event()
    session_by_source = {}

    async def import_resources(sources, **options):
        assert "rest_page_prefetch" not in options
        source_id = sources[0]["source_id"]
        session_by_source[source_id] = importer._SOURCE_HTTP_SESSION.get()
        if len(session_by_source) == import_count:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), 1)

        async def observe_child_context():
            await asyncio.sleep(0)
            assert importer._SOURCE_HTTP_SESSION.get() is session_by_source[source_id]
            assert not session_by_source[source_id].closed
            return importer._REST_PAGE_PREFETCH_OVERRIDE.get()

        return await asyncio.create_task(observe_child_context())

    monkeypatch.setattr(importer, "_import_resources", import_resources)
    prefetch_settings = await asyncio.gather(
        *(
            importer._import_resources_with_source_http_session(
                [{"source_id": f"source-{index}"}], rest_page_prefetch=bool(index % 2)
            )
            for index in range(import_count)
        )
    )

    assert prefetch_settings == [bool(index % 2) for index in range(import_count)]
    assert len({id(session) for session in session_by_source.values()}) == import_count
    assert all(session.closed for session in tracked_http_sessions)
    assert importer._SOURCE_HTTP_SESSION.get() is None
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


async def test_canceling_one_import_does_not_close_a_sibling_session(monkeypatch, tracked_http_sessions):
    """Cancel one import without resetting or closing another import's context."""
    both_started = asyncio.Event()
    release_sibling = asyncio.Event()
    session_by_source = {}

    async def import_resources(sources, **_options):
        source_id = sources[0]["source_id"]
        session_by_source[source_id] = importer._SOURCE_HTTP_SESSION.get()
        if len(session_by_source) == 2:
            both_started.set()
        await release_sibling.wait()
        assert importer._SOURCE_HTTP_SESSION.get() is session_by_source[source_id]
        assert not session_by_source[source_id].closed
        return source_id

    monkeypatch.setattr(importer, "_import_resources", import_resources)
    tasks = [
        asyncio.create_task(importer._import_resources_with_source_http_session([{"source_id": source_id}]))
        for source_id in ("source-a", "source-b")
    ]
    try:
        await asyncio.wait_for(both_started.wait(), 1)
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        assert session_by_source["source-a"].closed
        assert not session_by_source["source-b"].closed
        assert not tasks[1].done()
        release_sibling.set()
        assert await asyncio.wait_for(tasks[1], 1) == "source-b"
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert all(session.closed for session in tracked_http_sessions)
    assert importer._SOURCE_HTTP_SESSION.get() is None
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


async def test_concurrent_import_credentials_remain_in_their_task_scope(monkeypatch, tmp_path, tracked_http_sessions):
    """Do not share credentials between simultaneous imports of the same host."""
    both_started = asyncio.Event()
    started_sources = []
    paths_by_source = {}
    for source_id in ("source-a", "source-b"):
        path = tmp_path / f"{source_id}.json"
        path.write_text(json.dumps({"hosts": {"directory.example.test": {"headers": {"X-Test-Key": source_id}}}}))
        paths_by_source[source_id] = str(path)

    async def fetch_with_options(_url, *, extra_headers, **_options):
        await asyncio.sleep(0)
        return 200, {"source_id": extra_headers["X-Test-Key"]}, None, 1

    async def import_resources(sources, **_options):
        source = sources[0]
        token = importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(paths_by_source[source["source_id"]])
        try:
            started_sources.append(source["source_id"])
            if len(started_sources) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), 1)
            response = await importer._fetch_source_json_once(source, source["api_base"] + "/Location", timeout=1)
            assert response[1] == {"source_id": source["source_id"]}
            return response[0]
        finally:
            importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.reset(token)

    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.setattr(importer, "_fetch_json_with_options", fetch_with_options)
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    response_statuses = await asyncio.gather(
        *(
            importer._import_resources_with_source_http_session(
                [{"source_id": source_id, "api_base": "https://directory.example.test/fhir"}]
            )
            for source_id in paths_by_source
        )
    )
    assert response_statuses == [200, 200]
    assert importer._PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.get() is None
    assert all(session.closed for session in tracked_http_sessions)


@pytest.fixture
async def blocked_http_endpoint():
    """Hold real HTTP responses until the test has observed the connection cap."""
    counts_by_state = {"active": 0, "maximum": 0, "started": 0}
    counts_by_host = {}
    two_started = asyncio.Event()
    release = asyncio.Event()
    handler_tasks = set()

    async def respond(reader, writer):
        handler_task = asyncio.current_task()
        handler_tasks.add(handler_task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            host = next(line for line in headers.split(b"\r\n") if line.lower().startswith(b"host:")).decode()
            host_counts = counts_by_host.setdefault(host, {"active": 0, "maximum": 0})
            host_counts["active"] += 1
            host_counts["maximum"] = max(host_counts["maximum"], host_counts["active"])
            counts_by_state["active"] += 1
            counts_by_state["started"] += 1
            counts_by_state["maximum"] = max(counts_by_state["maximum"], counts_by_state["active"])
            if counts_by_state["active"] == 2:
                two_started.set()
            try:
                await release.wait()
                body = b'{"resourceType":"Bundle","type":"searchset","entry":[]}'
                writer.write(
                    b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode()
                    + body
                )
                await writer.drain()
            finally:
                counts_by_state["active"] -= 1
                host_counts["active"] -= 1
        finally:
            writer.close()
            await writer.wait_closed()
            handler_tasks.discard(handler_task)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}/fhir"
    try:
        yield url, counts_by_state, two_started, release, counts_by_host
    finally:
        release.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handler_tasks, return_exceptions=True)


async def test_http_connection_budget_caps_requests_and_cleans_queued_cancellation(blocked_http_endpoint):
    """Enforce the request budget in the real transport and release canceled waiters."""
    url, counts_by_state, two_started, release, _counts_by_host = blocked_http_endpoint
    source_by_field = {"source_id": "bounded-source", "api_base": url}
    async with importer._source_http_session_scope(force=True, request_concurrency=2):
        session = importer._SOURCE_HTTP_SESSION.get()
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(source_by_field, url + "/Location", timeout=3))
            for _ in range(4)
        ]
        try:
            await asyncio.wait_for(two_started.wait(), 2)
            assert counts_by_state == {"active": 2, "maximum": 2, "started": 2}
            tasks[2].cancel()
            with pytest.raises(asyncio.CancelledError):
                await tasks[2]
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(tasks[0], tasks[1], tasks[3]), 2)
            assert [response[0] for response in responses] == [200, 200, 200]
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert session.closed
    assert all(task.done() for task in tasks)
    assert counts_by_state == {"active": 0, "maximum": 2, "started": 3}
    assert importer._SOURCE_HTTP_SESSION.get() is None


async def test_http_source_budget_overlaps_hosts_without_exceeding_each_host_cap(blocked_http_endpoint):
    """One slow host must not consume another host's independently admitted slot."""
    url, counts_by_state, two_started, release, counts_by_host = blocked_http_endpoint
    urls = [url, url.replace("127.0.0.1", "localhost")]
    async with importer._source_http_session_scope(force=True, request_concurrency=1, source_concurrency=2):
        session = importer._SOURCE_HTTP_SESSION.get()
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once({"api_base": base}, base + "/Location", timeout=3))
            for base in urls
            for _ in range(2)
        ]
        try:
            await asyncio.wait_for(two_started.wait(), 2)
            assert counts_by_state == {"active": 2, "maximum": 2, "started": 2}
            assert len(counts_by_host) == 2
            assert all(host_counts == {"active": 1, "maximum": 1} for host_counts in counts_by_host.values())
            release.set()
            responses = await asyncio.wait_for(asyncio.gather(*tasks), 2)
            assert [response[0] for response in responses] == [200, 200, 200, 200]
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert session.closed
    assert counts_by_state == {"active": 0, "maximum": 2, "started": 4}
    assert all(host_counts == {"active": 0, "maximum": 1} for host_counts in counts_by_host.values())


@pytest.mark.parametrize(
    ("linked_limit", "linked_concurrency", "expected_requests"), [(0, 5, 2), (1, 5, 5), (1, 500, 100)]
)
async def test_session_wrapper_preserves_source_and_linked_concurrency_budgets(
    monkeypatch, linked_limit, linked_concurrency, expected_requests
):
    """Pass both existing concurrency dimensions through the shared session wrapper."""
    session_options = []

    @contextlib.asynccontextmanager
    async def session_scope(**options):
        session_options.append(options)
        yield

    source_records = [{"source_id": "source-a"}, {"source_id": "source-b"}]
    import_resources = AsyncMock(return_value={})
    monkeypatch.setattr(importer, "_source_http_session_scope", session_scope)
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    await importer._import_resources_with_source_http_session(
        source_records,
        resource_scan_concurrency=2,
        source_concurrency=10,
        linked_resource_limit=linked_limit,
        linked_resource_concurrency=linked_concurrency,
    )
    assert session_options == [
        {
            "force": False,
            "require_verified_tls": False,
            "request_concurrency": expected_requests,
            "source_concurrency": 2,
        }
    ]
    import_resources.assert_awaited_once_with(
        source_records,
        resource_scan_concurrency=2,
        source_concurrency=10,
        linked_resource_limit=linked_limit,
        linked_resource_concurrency=linked_concurrency,
    )


@pytest.fixture
async def throttled_http_endpoint():
    """Return one delayed throttle response and record actual sibling request arrival."""
    first_started = asyncio.Event()
    release = asyncio.Event()
    starts_by_path = {}
    handler_tasks = set()
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()

    async def respond(reader, writer):
        handler_task = asyncio.current_task()
        handler_tasks.add(handler_task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            path = headers.split(b" ", 2)[1].decode()
            starts_by_path[path] = asyncio.get_running_loop().time()
            if path == "/blocker":
                blocker_started.set()
                await release_blocker.wait()
            is_throttled = path == "/limited"
            if is_throttled:
                first_started.set()
                await release.wait()
            body = b"{}" if is_throttled else b'{"resourceType":"Bundle","type":"searchset","entry":[]}'
            status = b"429 Too Many Requests" if is_throttled else b"200 OK"
            retry_header = b"Retry-After: 1\r\n" if is_throttled else b""
            writer.write(
                b"HTTP/1.1 "
                + status
                + b"\r\nConnection: close\r\nContent-Type: application/json\r\n"
                + retry_header
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            handler_tasks.discard(handler_task)

    server = await asyncio.start_server(respond, "127.0.0.1", 0)
    url = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    try:
        yield url, first_started, release, starts_by_path, blocker_started, release_blocker
    finally:
        release.set()
        release_blocker.set()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handler_tasks, return_exceptions=True)


async def test_already_queued_sibling_honors_new_retry_after_deadline(throttled_http_endpoint):
    """Recheck source cooldown after waiting behind a request that returns 429."""
    url, first_started, release, starts_by_path, _blocker_started, _release_blocker = throttled_http_endpoint
    source_by_field = {"api_base": url, "_provider_directory_request_pacing_state": {}}
    sibling_attempted = asyncio.Event()

    async def fetch_sibling():
        sibling_attempted.set()
        return await importer._fetch_source_json_once(dict(source_by_field), url + "/sibling", timeout=3)

    async with importer._source_http_session_scope(force=True, request_concurrency=1):
        tasks = [
            asyncio.create_task(
                importer._fetch_source_json_attempt(
                    dict(source_by_field), url + "/limited", timeout=3, empty_collection_retry_delays=()
                )
            )
        ]
        try:
            await asyncio.wait_for(first_started.wait(), 2)
            tasks.append(asyncio.create_task(fetch_sibling()))
            await asyncio.wait_for(sibling_attempted.wait(), 2)
            release.set()
            first, sibling = await asyncio.wait_for(asyncio.gather(*tasks), 3)
        finally:
            release.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    assert first[0][0] == 429
    assert sibling[0] == 200
    assert (
        starts_by_path["/sibling"]
        >= source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"]
    )


async def test_sources_sharing_host_recheck_cooldown_after_shared_slot_wait(throttled_http_endpoint):
    """A second source cannot push a throttled sibling behind the connector gate."""
    url, first_started, release, starts_by_path, blocker_started, release_blocker = throttled_http_endpoint
    source_by_field = {"api_base": url + "/source_by_field", "_provider_directory_request_pacing_state": {}}
    other_source_by_field = {"api_base": url + "/other_source_by_field", "_provider_directory_request_pacing_state": {}}
    async with importer._source_http_session_scope(force=True, request_concurrency=2, source_concurrency=2):
        tasks = [
            asyncio.create_task(importer._fetch_source_json_once(other_source_by_field, url + "/blocker", timeout=3))
        ]
        try:
            await asyncio.wait_for(blocker_started.wait(), 2)
            tasks.append(
                asyncio.create_task(importer._fetch_source_json_once(source_by_field, url + "/limited", timeout=3))
            )
            await asyncio.wait_for(first_started.wait(), 2)
            tasks.append(
                asyncio.create_task(
                    importer._fetch_source_json_once(dict(source_by_field), url + "/sibling", timeout=3)
                )
            )
            await asyncio.sleep(0)
            release.set()
            assert (await asyncio.wait_for(tasks[1], 2))[0] == 429
            assert (await asyncio.wait_for(tasks[2], 2))[0] == 200
            assert (
                starts_by_path["/sibling"]
                >= source_by_field["_provider_directory_request_pacing_state"]["retry_after_monotonic"]
            )
        finally:
            release.set()
            release_blocker.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def source_request_clock(monkeypatch):
    """Advance pacing delays deterministically without altering the event-loop clock."""
    clock = SimpleNamespace(now=10.0, delays=[])
    original_sleep = asyncio.sleep

    async def sleep(delay):
        clock.delays.append(delay)
        clock.now += delay
        await original_sleep(0)

    monkeypatch.setattr(importer, "time", SimpleNamespace(monotonic=lambda: clock.now))
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    return clock


async def test_resource_copies_share_one_source_request_interval(monkeypatch, source_request_clock):
    """Parallel resource cursors must not each spend the same source pacing slot."""
    source_by_field = {
        "api_base": "https://directory.example.test/fhir",
        "metadata_json": {"provider_directory_request_interval_seconds": 0.25},
        "_provider_directory_request_pacing_state": {},
    }
    observed_starts = []

    async def fetch_json(_url, **_options):
        observed_starts.append(source_request_clock.now)
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 0

    monkeypatch.setattr(importer, "_fetch_json", fetch_json)
    await asyncio.gather(
        *(
            importer._fetch_source_json_once(
                dict(source_by_field), source_by_field["api_base"] + "/" + resource_type, timeout=1
            )
            for resource_type in ("Location", "Organization", "HealthcareService")
        )
    )

    assert observed_starts == [10.0, 10.25, 10.5]
    assert source_request_clock.delays == [0.25, 0.25]


async def test_retry_after_from_one_resource_delays_its_sibling_only(monkeypatch, source_request_clock):
    """A source's throttle response must delay siblings without stalling another source."""
    source_by_field = {
        "api_base": "https://directory.example.test/fhir",
        "_provider_directory_request_pacing_state": {},
    }

    async def fetch_json(url, **_options):
        if url.endswith("/Location"):
            return 429, {importer.SOURCE_RETRY_AFTER_FIELD: "2"}, None, 0
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 0

    monkeypatch.setattr(importer, "_fetch_json", fetch_json)
    await importer._fetch_source_json_attempt(
        dict(source_by_field), source_by_field["api_base"] + "/Location", timeout=1, empty_collection_retry_delays=()
    )
    independent_source_by_field = {
        "api_base": "https://independent.example.test/fhir",
        "_provider_directory_request_pacing_state": {},
    }
    await importer._fetch_source_json_once(
        independent_source_by_field, independent_source_by_field["api_base"] + "/Organization", timeout=1
    )
    assert source_request_clock.now == 10.0
    await importer._fetch_source_json_once(
        dict(source_by_field), source_by_field["api_base"] + "/Organization", timeout=1
    )

    assert source_request_clock.now == 12.0
    assert source_request_clock.delays == [2.0]


@pytest.mark.parametrize("retry_after", [None, "invalid-date"])
async def test_throttle_without_valid_retry_after_preserves_existing_cooldown(
    monkeypatch, source_request_clock, retry_after
):
    """A headerless or malformed throttle cannot invent or erase a sibling's pacing deadline."""
    pacing_by_field = {"retry_after_monotonic": 9.0}
    source_by_field = {
        "api_base": "https://directory.example.test/fhir",
        "_provider_directory_request_pacing_state": pacing_by_field,
    }
    payload_by_field = {} if retry_after is None else {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}
    fetch_json = AsyncMock(return_value=(429, payload_by_field, None, 0))
    monkeypatch.setattr(importer, "_fetch_json", fetch_json)
    async with importer._source_http_session_scope(force=True, request_concurrency=1):
        response = await importer._fetch_source_json_once(
            source_by_field, source_by_field["api_base"] + "/Location", timeout=1
        )
    assert response == (429, payload_by_field, None, 0)
    assert pacing_by_field["retry_after_monotonic"] == 9.0
    assert source_request_clock.delays == []
    fetch_json.assert_awaited_once()


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_prefetch_override_restores_outer_context_on_failure(monkeypatch, failure):
    @contextlib.asynccontextmanager
    async def session_scope(**_kwargs):
        yield

    async def import_resources(_sources, **_kwargs):
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is True
        raise failure()

    monkeypatch.setattr(importer, "_source_http_session_scope", session_scope)
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    token = importer._REST_PAGE_PREFETCH_OVERRIDE.set(False)
    try:
        with pytest.raises(failure):
            await importer._import_resources_with_source_http_session(
                [{"source_id": "source_a"}], rest_page_prefetch=True
            )
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is False
    finally:
        importer._REST_PAGE_PREFETCH_OVERRIDE.reset(token)


async def test_prefetch_override_does_not_force_disabled_transport(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "false")

    async def import_resources(_sources, **_kwargs):
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is True
        assert importer._SOURCE_HTTP_SESSION.get() is None
        assert (
            importer._rest_page_prefetch_session(
                {"api_base": "https://example.test/fhir"}, "https://example.test/fhir/Practitioner"
            )
            is None
        )
        return {}

    monkeypatch.setattr(importer, "_import_resources", import_resources)
    assert (
        await importer._import_resources_with_source_http_session([{"source_id": "source_a"}], rest_page_prefetch=True)
        == {}
    )
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


@pytest.mark.parametrize("setting", ["true", 1, 0, {}, []])
async def test_prefetch_setting_rejects_non_boolean_values_before_database_work(monkeypatch, setting):
    database_setup = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", database_setup)
    with pytest.raises(ValueError, match="provider_directory_rest_page_prefetch_invalid"):
        await importer.process_data({"context": {}}, {"rest_page_prefetch": setting, "probe": False})
    database_setup.assert_not_awaited()
