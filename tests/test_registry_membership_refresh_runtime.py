# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed server configuration and bounded dedicated enrollment transport."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from process import registry_membership_refresh_runtime as runtime
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable
from process.registry_refresh_anchor_contract import encoded_document


def configuration():
    return {
        "enabled": True,
        "app_origin": "https://authority.example",
        "service_token": "example-dedicated-token",
        "control_schema": "registry",
        "fence_dsn": "postgresql://fence_reader:{password}@db.example/example",
        "source_reader_dsn": "postgresql://source_reader:{password}@db.example/example",
        "address_reader_dsn": "postgresql://address_reader:{password}@db.example/example",
        "address_schema": "mrf",
        "runtime_roles": ["network_reader"],
        "source_profiles": {},
    }


def scope(number):
    return {
        "binding_id": f"00000000-0000-0000-0000-{number:012x}",
        "binding_revision": 1,
        "network_id": 7,
        "continuity_policy": "exact_capture",
        "source_scope": {
            "source_system": "ptg",
            "source_id": "example",
            "dataset_schema": "source_schema",
            "producer_id": "producer",
            "source_key": "network",
            "approved_binding_sha256": "a" * 64,
            "capture_generation": "b" * 64,
        },
    }


@pytest.mark.parametrize("configured", [None, {}, {"enabled": False}, {"enabled": 1}, "null", "{", "NaN"])
def test_default_missing_disabled_or_malformed_configuration_is_unavailable(configured):
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


def test_configuration_uses_only_dedicated_server_values_and_native_profiles():
    parsed = runtime._settings(configuration())
    assert parsed.app_origin == "https://authority.example"
    assert parsed.fence_url.username == "fence_reader"
    assert parsed.address_profile.runtime_roles == ("network_reader",)
    assert "password" not in repr(parsed) and "dedicated-token" not in repr(parsed)


@pytest.mark.parametrize(
    "change",
    [
        {"extra": True},
        {"npi_source": {}},
        {"resolve_sources": "not-a-capability"},
        {"service_token": "bad\nvalue"},
        {"source_reader_dsn": "postgresql://reader:{password}@other.example/example"},
        {"address_reader_dsn": "postgresql://reader:{password}@db.example/other"},
        {"address_reader_dsn": "postgresql://reader:{password}@db.example/example?options=unsafe"},
        {"runtime_roles": ["z", "a"]},
        {"runtime_roles": []},
        {"address_schema": "bad;schema"},
        {"source_profiles": {"unknown": {}}},
        {"source_profiles": {"ptg": {"recipe": {}}}},
        {"read_budget": {"maximum_bytes": True, "maximum_pages": 1, "maximum_coordinates": 1}},
    ],
)
def test_closed_configuration_refuses_scope_credentials_and_resource_overrides(change):
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings({**configuration(), **change})


def test_native_socket_urls_match_exact_socket_database_and_port_without_tcp():
    configured = configuration()
    for name in ("fence_dsn", "source_reader_dsn", "address_reader_dsn"):
        configured[name] = "postgresql://reader:{password}@/example?host=/tmp/example-socket&port=55484"
    parsed = runtime._settings(configured)
    assert runtime._target(parsed.fence_url) == runtime._target(parsed.address_reader_url)
    configured["address_reader_dsn"] += "0"
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


class _Body(httpx.AsyncByteStream):
    def __init__(self, encoded):
        self.encoded = encoded

    async def __aiter__(self):
        yield self.encoded


def _reply(document, *, headers=None, status=200):
    encoded = encoded_document(document)
    return httpx.Response(
        status,
        stream=_Body(encoded),
        headers=headers
        or {
            "content-type": "application/json",
            "cache-control": "private, no-store",
            "content-length": str(len(encoded)),
        },
    )


@pytest.mark.asyncio
async def test_enrollment_pages_use_dedicated_token_and_consistent_exact_cursor():
    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            return _reply(
                {
                    "policy_revision": 9,
                    "enrolled_scopes": [scope(number) for number in range(1, 101)],
                    "next_binding_id": scope(100)["binding_id"],
                }
            )
        return _reply({"policy_revision": 9, "enrolled_scopes": [scope(101)], "next_binding_id": None})

    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example", "dedicated-only", enabled=True, transport=httpx.MockTransport(respond)
    ) as authority:
        authority._client.cookies.set("foreign", "cookie")
        selected = await runtime._enrollments(authority)
        assert len(selected) == 101 and selected[-1] == scope(101)
        assert [request.url.path for request in requests] == [runtime._ENROLLMENT_PATH] * 2
        assert requests[1].url.params["after_binding_id"] == scope(100)["binding_id"]
        assert all(request.headers["authorization"] == "Bearer dedicated-only" for request in requests)
        assert all("cookie" not in request.headers for request in requests)
        assert not authority._client.cookies


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["policy", "duplicate", "cursor", "extra"])
async def test_inconsistent_or_partial_enrollment_paging_never_succeeds(damage):
    pages = [
        {
            "policy_revision": 1,
            "enrolled_scopes": [scope(number) for number in range(1, 101)],
            "next_binding_id": scope(100)["binding_id"],
        },
        {"policy_revision": 1, "enrolled_scopes": [scope(101)], "next_binding_id": None},
    ]
    if damage == "policy":
        pages[1]["policy_revision"] = 2
    elif damage == "duplicate":
        pages[1]["enrolled_scopes"] = [scope(100)]
    elif damage == "cursor":
        pages[0]["next_binding_id"] = scope(99)["binding_id"]
    else:
        pages[0]["extra"] = True
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example",
        "dedicated-only",
        enabled=True,
        transport=httpx.MockTransport(lambda _request: _reply(pages.pop(0))),
    ) as authority:
        with pytest.raises((ValueError, RegistryMembershipRefreshUnavailable)):
            await runtime._enrollments(authority)


@pytest.mark.asyncio
async def test_more_than_one_thousand_enrollments_refuses_without_partial_vector():
    calls = []

    def respond(request):
        start = len(calls) * 100 + 1
        calls.append(request)
        return _reply(
            {
                "policy_revision": 1,
                "enrolled_scopes": [scope(number) for number in range(start, start + 100)],
                "next_binding_id": scope(start + 99)["binding_id"],
            }
        )

    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example", "dedicated-only", enabled=True, transport=httpx.MockTransport(respond)
    ) as authority:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime._enrollments(authority)
        assert len(calls) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers,status",
    [
        ({"location": "https://foreign.example"}, 302),
        ({"content-type": "application/json"}, 200),
        ({"content-type": "application/json", "cache-control": "no-store", "set-cookie": "x=y"}, 200),
        ({"content-type": "application/json", "cache-control": "no-store", "content-length": "1048577"}, 200),
    ],
)
async def test_unproven_enrollment_http_response_is_closed_and_refused(headers, status):
    response = _reply({}, headers=headers, status=status)
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example",
        "dedicated-only",
        enabled=True,
        transport=httpx.MockTransport(lambda _request: response),
    ) as authority:
        with pytest.raises((ValueError, RegistryMembershipRefreshUnavailable)):
            await runtime._enrollments(authority)
        assert response.is_closed


@pytest.mark.asyncio
async def test_api_default_disabled_is_none_and_never_allocates_resources(monkeypatch):
    from contextlib import AsyncExitStack
    from unittest.mock import AsyncMock

    allocate = AsyncMock(side_effect=AssertionError("unexpected native resources"))
    monkeypatch.setattr(runtime, "_build_native_bundle", allocate)
    async with AsyncExitStack() as cleanup:
        assert await runtime.build_registry_membership_refresh_runtime(None, cleanup) is None
        assert await runtime.build_registry_membership_refresh_runtime({"enabled": False}, cleanup) is None
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime.build_registry_membership_refresh_runtime({"enabled": False, "unknown": True}, cleanup)
    allocate.assert_not_awaited()


@pytest.mark.asyncio
async def test_api_namespace_and_actual_database_are_bound_before_installation(monkeypatch):
    from contextlib import AsyncExitStack
    from unittest.mock import AsyncMock

    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "other_registry")
    allocate = AsyncMock(side_effect=AssertionError("unexpected native resources"))
    monkeypatch.setattr(runtime, "_build_native_bundle", allocate)
    async with AsyncExitStack() as cleanup:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime.build_registry_membership_refresh_runtime(configuration(), cleanup)
        allocate.assert_not_awaited()
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry")
        allocate.side_effect = None
        allocate.return_value = SimpleNamespace(
            inputs_service=SimpleNamespace(resources=SimpleNamespace(identity=("expected",)))
        )
        monkeypatch.setattr(runtime, "_api_identity", AsyncMock(return_value=("foreign",)))
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime.build_registry_membership_refresh_runtime(configuration(), cleanup)


@pytest.mark.asyncio
async def test_one_successful_readiness_key_avoids_replaying_provider_facts_on_normal_ticks(monkeypatch):
    from dataclasses import replace

    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    first = await case.service._observe(object(), [scope(1)])
    assert first["ready_address_generation"] == case.pin.generation_id
    assert case.events == ["observe", "address_metadata", "full_source_pin", "address_fenced", "source_release"]
    case.events.clear()
    case.head.update(generation_id=5, manifest_sha256="e" * 64)
    second = await case.service._observe(object(), [scope(1)])
    assert second["ready_address_generation"] == first["ready_address_generation"]
    assert case.events == ["observe", "address_metadata"]
    assert case.prepare.await_count == 1
    case.approved = replace(case.approved, generation_id="f" * 64)
    await case.service._observe(object(), [scope(1)])
    assert case.prepare.await_count == 2


@pytest.mark.asyncio
async def test_deterministic_missing_offices_cache_only_pending_readiness(monkeypatch):
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    case.prepare.return_value = False
    first = await case.service._observe(object(), [scope(1)])
    second = await case.service._observe(object(), [scope(1)])
    assert first["ready_address_generation"] is second["ready_address_generation"] is None
    assert case.service._readiness_key is not None and case.prepare.await_count == 1


@pytest.mark.asyncio
async def test_semantic_key_changes_for_address_enrollment_and_capture_but_not_head(monkeypatch):
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    await case.service._observe(object(), [scope(1)])
    original = case.service._readiness_key
    case.descriptor["descriptor_sha256"] = "f" * 64
    await case.service._observe(object(), [scope(1)])
    assert case.service._readiness_key != original
    changed = case.service._readiness_key
    await case.service._observe(object(), [scope(2)])
    assert case.service._readiness_key != changed and case.prepare.await_count == 3


@pytest.mark.asyncio
async def test_recovery_preserves_original_head_and_rejects_a_mislabeled_semantic_key(monkeypatch):
    from unittest.mock import AsyncMock
    from uuid import UUID

    from process.registry_membership_refresh_contract import (
        accepted_membership_source_sha256,
        membership_refresh_command_sha256,
    )
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    case.head.update(generation_id=8, manifest_sha256="e" * 64)
    accepted_sources = list(case.selected.accepted_sources)
    command_by_field = {
        "operation": "refresh_memberships",
        "expected_approved_revision": 7,
        "expected_serving_generation": 0,
        "expected_serving_manifest_sha256": None,
        "address_artifact_sha256": case.descriptor["descriptor_sha256"],
        "accepted_sources": accepted_sources,
        "accepted_source_vector_sha256": accepted_membership_source_sha256(accepted_sources),
        "enrolled_scopes": [scope(1)],
        "reason": "Refresh accepted_sources enrolled network memberships",
        "idempotency_key": "synthetic",
    }
    request_by_field = {"request_id": UUID("d69a5cb2-4333-43eb-bba8-e03b6108599b"), "state": "queued"}
    monkeypatch.setattr(runtime, "_recovery", AsyncMock(return_value=(request_by_field, command_by_field)))
    observation = await case.service._observe(object(), [scope(1)])
    assert observation["expected_serving_generation"] == 0 and observation["expected_serving_manifest_sha256"] is None
    assert observation["refresh_request"]["command_sha256"] == membership_refresh_command_sha256(command_by_field)
    command_by_field["enrolled_scopes"] = [scope(2)]
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await case.service._observe(object(), [scope(1)])


@pytest.mark.asyncio
async def test_transient_or_unclassified_correspondence_failure_never_caches_readiness(monkeypatch):
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    case.prepare.side_effect = TimeoutError("transient")
    for _attempt in range(2):
        with pytest.raises(TimeoutError):
            await case.service._observe(object(), [scope(1)])
    assert case.service._readiness_key is None and case.prepare.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [TimeoutError, ValueError, PermissionError])
async def test_only_exact_typed_missing_office_is_a_cacheable_wait(monkeypatch, error_type):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from process import registry_initial_source_composition
    from process.registry_source_office_errors import SourceOfficeNotReady
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)

    @asynccontextmanager
    async def nested():
        yield

    session = SimpleNamespace(begin_nested=nested)
    proof = AsyncMock(side_effect=SourceOfficeNotReady("missing-office"))
    monkeypatch.setattr(registry_initial_source_composition, "prepare_source_offices", proof)
    original = runtime.RegistryMembershipRefreshInputsService._has_office_correspondence
    assert await original(case.service, session, case.approved, case.selected, case.pin) is False
    proof.side_effect = error_type("not-a-missing-office-proof")
    with pytest.raises(error_type):
        await original(case.service, session, case.approved, case.selected, case.pin)


@pytest.mark.parametrize(
    "dsn",
    [
        None,
        "postgresql://reader:{password}@db.example/example\n",
        "postgresql://reader:{password}@db.example/example?ssl=disable",
        "postgresql://reader:{password}@db.example/example?port=5432",
        "postgresql://reader:{password}@/example?host=/tmp/../socket",
        "postgresql://reader:{password}@/example?host=/tmp/socket&port=0",
        "postgresql://reader:{password}@db.example/example?host=/tmp/socket",
    ],
)
def test_native_pool_configuration_rejects_ambiguous_socket_and_transport_identity(dsn):
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings({**configuration(), "fence_dsn": dsn})


def test_source_profiles_bind_each_fixed_reader_and_disjoint_head_fence():
    configured = configuration()
    configured["source_profiles"] = {
        "cms": {
            "control_schema": "registry",
            "owner_role": "cms_owner",
            "publisher_role": "cms_publisher",
            "reader_roles": ["source_reader"],
        },
        "aca": {
            "control_schema": "registry",
            "publisher_role": "aca_publisher",
            "reader_roles": ["source_reader"],
            "source_schema": "mrf",
            "head_fence_dsn": "postgresql://aca_fence:{password}@db.example/example",
        },
        "ptg": {"control_schema": "registry", "owner_role": "ptg_owner", "approval_role": "ptg_approval"},
        "generic": {"head_fence_dsn": "postgresql://generic_fence:{password}@db.example/example"},
    }
    settings = runtime._settings(configured)
    assert set(settings.source_profiles) == {"cms", "aca", "ptg"}
    assert settings.aca_head_fence_url.username == "aca_fence"
    assert settings.generic_head_fence_url.username == "generic_fence"
    configured["source_profiles"]["cms"]["reader_roles"] = ["other_reader"]
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)
    configured["source_profiles"]["cms"]["reader_roles"] = ["source_reader"]
    configured["source_profiles"]["aca"]["reader_roles"] = ["other_reader"]
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(configured)


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"", b"{}", b"x" * 1048577])
async def test_streamed_enrollment_body_must_fit_its_declared_size(content):
    response = httpx.Response(
        200,
        stream=_Body(content),
        headers={"content-type": "application/json", "cache-control": "no-store", "content-length": "1"},
    )
    try:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime._page_body(response)
    finally:
        await response.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [PermissionError, TimeoutError, asyncio.CancelledError])
async def test_service_failure_invalidates_cached_readiness_and_preserves_cancellation(monkeypatch, error_type):
    authorize = object()
    enrollments = AsyncMock(side_effect=error_type("observation failed"))
    monkeypatch.setattr(runtime, "_enrollments", enrollments)
    service = runtime.RegistryMembershipRefreshInputsService(object(), authorize)
    service._readiness_key = "prior-success"
    expected_error = (
        error_type if error_type in {PermissionError, asyncio.CancelledError} else RegistryMembershipRefreshUnavailable
    )
    with pytest.raises(expected_error):
        await service()
    assert service._readiness_key is None
    enrollments.assert_awaited_once_with(authorize)


@pytest.mark.asyncio
async def test_service_returns_only_the_complete_fenced_observation(monkeypatch):
    events, session, selected = [], object(), [scope(1)]

    @asynccontextmanager
    async def fenced(sessions, *, control_schema):
        assert sessions == "fence-pool" and control_schema == "registry"
        events.append("enter")
        try:
            yield session
        finally:
            events.append("exit")

    monkeypatch.setattr(runtime, "registry_company_approval_transaction", fenced)
    monkeypatch.setattr(runtime, "_enrollments", AsyncMock(return_value=selected))
    resources = SimpleNamespace(settings=SimpleNamespace(control_schema="registry"), fence_sessions="fence-pool")
    service = runtime.RegistryMembershipRefreshInputsService(resources, object())
    observation_by_name = {"ready_address_generation": "complete"}
    observe = AsyncMock(return_value=observation_by_name)
    monkeypatch.setattr(service, "_observe", observe)
    assert await service() is observation_by_name and events == ["enter", "exit"]
    observe.assert_awaited_once_with(session, selected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "head",
    [
        None,
        {"generation_id": 1, "eligible": False},
        {"approved_revision": 0, "generation_id": 0, "manifest_sha256": "a" * 64},
    ],
)
async def test_native_head_cannot_expose_unsealed_or_inconsistent_generation(head):
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=head))
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await runtime._heads(driver, "registry")


@pytest.mark.asyncio
async def test_api_cancellation_is_not_rewritten_to_unavailability(monkeypatch):
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "registry")
    monkeypatch.setattr(runtime, "_build_native_bundle", AsyncMock(side_effect=asyncio.CancelledError()))
    async with AsyncExitStack() as cleanup:
        with pytest.raises(asyncio.CancelledError):
            await runtime.build_registry_membership_refresh_runtime(configuration(), cleanup)


@pytest.mark.parametrize("encoded", [False, True])
def test_runtime_configuration_cap_counts_encoded_bytes(encoded):
    configured_by_field = {**configuration(), "service_token": "é" * (runtime._MAX_CONFIG_BYTES // 2)}
    selected = json.dumps(configured_by_field, ensure_ascii=False) if encoded else configured_by_field
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings(selected)


@pytest.mark.parametrize(
    "change",
    [
        {"runtime_roles": ("network_reader",)},
        {
            "source_profiles": {
                "aca": {"control_schema": "registry", "publisher_role": "publisher", "reader_roles": ["source_reader"]}
            }
        },
    ],
)
def test_runtime_profile_cannot_coerce_roles_or_omit_the_source_namespace(change):
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        runtime._settings({**configuration(), **change})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "document,disabled", [(' {"enabled":false} ', True), ("", False), (" " * (runtime._MAX_CONFIG_BYTES + 1), False)]
)
async def test_api_json_configuration_is_bounded_before_any_native_allocation(monkeypatch, document, disabled):
    allocate = AsyncMock()
    monkeypatch.setattr(runtime, "_build_native_bundle", allocate)
    async with AsyncExitStack() as cleanup:
        if disabled:
            assert await runtime.build_registry_membership_refresh_runtime(document, cleanup) is None
        else:
            with pytest.raises(RegistryMembershipRefreshUnavailable):
                await runtime.build_registry_membership_refresh_runtime(document, cleanup)
    allocate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("selected", [{}, [scope(number) for number in range(1, 102)]])
async def test_enrollment_response_requires_a_bounded_scope_array(selected):
    response = _reply({"policy_revision": 1, "enrolled_scopes": selected, "next_binding_id": None})
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example",
        "dedicated-only",
        enabled=True,
        transport=httpx.MockTransport(lambda _request: response),
    ) as authority:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await runtime._enrollments(authority)
    assert response.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [None, "", "a" * 257])
async def test_completed_request_cannot_publish_an_untyped_or_unbounded_address_generation(generation):
    from tests.test_registry_membership_refresh_observations import published_rows

    _, request, candidate, manifest = published_rows()
    candidate["address_generation"] = manifest.source_generations["unified_address"] = generation
    driver = SimpleNamespace(fetch=AsyncMock(return_value=[request]), fetchrow=AsyncMock(return_value=candidate))
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await runtime._published_request(driver, "registry", manifest)


@pytest.mark.asyncio
async def test_observed_head_must_match_the_freshly_resolved_manifest(monkeypatch):
    from tests.test_registry_membership_refresh_observations import published_rows

    *_, manifest = published_rows()
    resolve = AsyncMock(return_value=manifest)
    monkeypatch.setattr(runtime, "resolve_network_serving_manifest", resolve)
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await runtime._manifest(
            object(), "registry", {"generation_id": manifest.generation_id, "manifest_sha256": "a" * 64}
        )
    assert resolve.await_args.kwargs["generation_id"] == manifest.generation_id


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["database", "login"])
async def test_source_pins_refuse_changed_execution_identity_before_acquiring_fences(monkeypatch, changed):
    from tests.test_registry_membership_refresh_contract import envelope

    resources = SimpleNamespace(settings=runtime._settings(configuration()), identity=("original", 1))
    monkeypatch.setattr(
        runtime,
        "_native_identity",
        AsyncMock(return_value=("other", 2) if changed == "database" else resources.identity),
    )
    fence = AsyncMock()
    monkeypatch.setattr(runtime, "registry_company_approval_transaction", fence)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value="other_login"))
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        async with runtime._SourceResolver(resources, "publisher").pin_sources(connection, envelope(), None):
            pytest.fail("changed execution identity yielded source capabilities")
    fence.assert_not_called()
    assert connection.fetchval.await_count == int(changed == "login")


@pytest.mark.asyncio
@pytest.mark.parametrize("untyped_readiness", [None, 0, 1])
async def test_readiness_requires_an_explicit_boolean_and_releases_source_fences(monkeypatch, untyped_readiness):
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    case = ObservationCase(monkeypatch)
    case.prepare.return_value = untyped_readiness
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await case.service._observe(object(), [scope(1)])
    assert case.service._readiness_key is None and case.service._is_ready is False
    assert not case.has_source_fence and case.events[-1] == "source_release"


@pytest.mark.asyncio
async def test_specialized_office_failure_is_not_cached_as_a_known_missing_office(monkeypatch):
    from process import registry_initial_source_composition as offices
    from process.registry_source_office_errors import SourceOfficeNotReady
    from tests.registry_membership_refresh_runtime_support import ObservationCase

    class UpstreamOfficeUnavailable(SourceOfficeNotReady):
        pass

    @asynccontextmanager
    async def nested():
        yield

    case = ObservationCase(monkeypatch)
    error = UpstreamOfficeUnavailable("source unavailable")
    monkeypatch.setattr(offices, "prepare_source_offices", AsyncMock(side_effect=error))
    with pytest.raises(UpstreamOfficeUnavailable) as rejected:
        await runtime.RegistryMembershipRefreshInputsService._has_office_correspondence(
            case.service, SimpleNamespace(begin_nested=nested), case.approved, case.selected, case.pin
        )
    assert rejected.value is error and case.service._readiness_key is None


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [("one",), ("two",)]])
async def test_api_database_identity_requires_exactly_one_native_row(monkeypatch, rows):
    from db.models import db

    read = AsyncMock(return_value=rows)
    monkeypatch.setattr(db, "all", read)
    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await runtime._api_identity("registry")
    assert read.await_args.kwargs == {"schema": "registry"}


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["ready", "timeout", "cancel", "looser"])
async def test_worker_applies_its_exact_profile_and_stricter_limits_before_installation(monkeypatch, outcome):
    from process.registry_publication_worker import parse_registry_publication_worker_config
    from tests.test_registry_publication_worker import configuration as worker_configuration

    worker = parse_registry_publication_worker_config(json.dumps(worker_configuration()))
    configured = configuration()
    configured["runtime_roles"] = sorted(set(worker.consumer.loader_roles + worker.consumer.reader_roles))
    for name, role in (
        ("fence_dsn", "fence_reader"),
        ("source_reader_dsn", "source_reader"),
        ("address_reader_dsn", "address_reader"),
    ):
        configured[name] = worker.execution_url.set(drivername="postgresql", username=role).render_as_string(
            hide_password=False
        )
    limits = replace(worker.sql_limits, work_mem_bytes=worker.sql_limits.work_mem_bytes // 2)
    configured["sql_limits"] = asdict(limits)
    if outcome == "looser":
        configured["sql_limits"]["work_mem_bytes"] = worker.sql_limits.work_mem_bytes * 2
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", worker.consumer.control_schema)
    monkeypatch.setenv(runtime.CONFIG_ENV, json.dumps(configured))
    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example",
        "dedicated-only",
        enabled=True,
        transport=httpx.MockTransport(lambda _request: pytest.fail("installation attempted HTTP")),
    ) as authority:
        installed = runtime.RegistryMembershipRefreshRuntime(
            authority, runtime._settings(configured).address_profile, AsyncMock(), AsyncMock()
        )
        error = asyncio.CancelledError() if outcome == "cancel" else TimeoutError() if outcome == "timeout" else None
        build = AsyncMock(return_value=SimpleNamespace(runtime=installed), side_effect=error)
        monkeypatch.setattr(runtime, "_build_native_bundle", build)
        async with AsyncExitStack() as cleanup:
            if outcome == "ready":
                assert await runtime.membership_refresh_worker_runtime(worker, cleanup) is installed
            else:
                expected = asyncio.CancelledError if outcome == "cancel" else RegistryMembershipRefreshUnavailable
                with pytest.raises(expected):
                    await runtime.membership_refresh_worker_runtime(worker, cleanup)
        if outcome == "looser":
            build.assert_not_awaited()
        else:
            assert build.await_args.args[0].sql_limits == limits
            assert build.await_args.args[2] == worker.consumer.execution_role
