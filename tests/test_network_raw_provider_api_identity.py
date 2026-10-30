# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""RAW provider guard wiring; native resolver and custody authority remain separate proof."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from api import network_provider_only_read as provider_only
from api import network_provider_read as readers
from api.network_address_scope import NetworkAddressReadScope
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_read_identity import NetworkReadIdentityError
from process.network_serving_read import PinnedNetworkServingManifest

pytestmark = pytest.mark.asyncio
_PID = 77


def _manifest(*, compact=False, artifacts=False):
    identity = str(UUID(int=1))
    schema = "network_candidate_" + UUID(identity).hex
    manifest = PinnedNetworkServingManifest(7, identity, schema, 1, {}, 3, "a" * 64, 42)
    object.__setattr__(
        manifest,
        "_candidate_relation_pins",
        tuple(
            CompactAddressRelation(schema, name, oid)
            for name, oid in (
                ("network_membership", 101),
                ("provider_location_binding", 102),
                ("network_source_contribution", 999),
            )
        ),
    )
    if compact:
        artifact_id = str(UUID(int=2))
        layout = CompactNetworkLayout(
            CompactAddressRelation(schema, "network_slice_ref", 103),
            CompactAddressRelation("synthetic_addresses", "entity_address_unified", 201),
            (CompactEdgeArtifact(artifact_id, "network_candidate_" + UUID(artifact_id).hex, 301),) if artifacts else (),
            CompactAddressRelation("synthetic_custom", "entity_address_unified", 202),
        )
        object.__setattr__(manifest, "_compact_layout", layout)
    return manifest


def _office_spies(monkeypatch, manifest, *, guard_error=None):
    events = []
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        get_server_pid=Mock(side_effect=lambda: events.append("pid") or _PID),
        acquire=Mock(side_effect=AssertionError("RAW read acquired a new backend")),
    )

    async def data(query, *arguments):
        assert events == ["resolve", "pid"]
        events.append("data")
        return {
            "valid": True,
            "bounded": True,
            "result": json.dumps({"generation_id": manifest.generation_id, "total_count": 0, "providers": []}),
        }

    async def resolve(actual, **keywords):
        assert actual is connection and keywords["api_read"] is True
        events.append("resolve")
        return manifest

    async def guard(actual, relations, *, expected_backend_pid):
        assert actual is connection and events[-1] == "data" and expected_backend_pid == _PID
        events.append("guard")
        if guard_error is not None:
            raise guard_error

    native_result = readers._result

    def decode(receipt):
        assert events[-1] == "guard"
        events.append("decode")
        return native_result(receipt)

    connection.fetchrow = AsyncMock(side_effect=data)
    resolver, verifier = AsyncMock(side_effect=resolve), AsyncMock(side_effect=guard)
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(readers, "verify_consumed_network_relations", verifier)
    monkeypatch.setattr(readers, "_result", Mock(side_effect=decode))
    return connection, verifier, events


@pytest.mark.parametrize("compact,artifacts", [(False, False), (True, False), (True, True)])
async def test_office_page_guards_only_rendered_relations(monkeypatch, compact, artifacts):
    manifest = _manifest(compact=compact, artifacts=artifacts)
    connection, verifier, events = _office_spies(monkeypatch, manifest)
    caller_manifest = replace(manifest)
    assert caller_manifest.candidate_relation_pins == ()
    result = await readers.read_network_provider_page(connection, NetworkAddressReadScope(caller_manifest, (11,), None))
    assert result["providers"] == [] and events == ["resolve", "pid", "data", "guard", "decode"]
    relations = verifier.await_args.args[1]
    expected = {101, 102, 201, 202} if compact else {101, 102, 42}
    if artifacts:
        expected.update((103, 301))
    assert {relation.table_oid for relation in relations} == expected
    assert 999 not in expected
    connection.acquire.assert_not_called()


@pytest.mark.parametrize("missing", ["network_membership", "provider_location_binding", "all"])
async def test_missing_candidate_pins_refuse_before_data(monkeypatch, missing):
    manifest = _manifest()
    pins = (
        tuple(pin for pin in manifest.candidate_relation_pins if pin.table_name != missing) if missing != "all" else ()
    )
    object.__setattr__(manifest, "_candidate_relation_pins", pins)
    connection, verifier, _events = _office_spies(monkeypatch, manifest)
    with pytest.raises(readers.NetworkProviderReadUnavailable):
        await readers.read_network_provider_page(connection, NetworkAddressReadScope(manifest, (11,), None))
    connection.fetchrow.assert_not_awaited()
    connection.get_server_pid.assert_not_called()
    verifier.assert_not_awaited()


async def test_guard_refusal_precedes_empty_result_and_cache(monkeypatch):
    manifest = _manifest()
    connection, verifier, events = _office_spies(
        monkeypatch, manifest, guard_error=NetworkReadIdentityError("synthetic mismatch")
    )
    cached_documents = []
    with pytest.raises(readers.NetworkProviderReadUnavailable):
        cached_documents.append(
            await readers.read_network_provider_page(connection, NetworkAddressReadScope(manifest, (11,), None))
        )
    assert cached_documents == [] and events == ["resolve", "pid", "data", "guard"]
    readers._result.assert_not_called()
    verifier.assert_awaited_once()


async def test_empty_detail_is_guarded_before_not_found(monkeypatch):
    manifest = _manifest()
    connection, verifier, events = _office_spies(monkeypatch, manifest)
    with pytest.raises(readers.NetworkProviderNotFound):
        await readers.read_network_provider_detail(
            connection,
            NetworkAddressReadScope(manifest, (11,), None),
            provider_system="npi",
            provider_id="1000000491",
        )
    assert events[-2:] == ["guard", "decode"]
    verifier.assert_awaited_once()


async def test_cancellation_does_not_decode_result(monkeypatch):
    manifest = _manifest()
    connection, _, _ = _office_spies(monkeypatch, manifest, guard_error=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await readers.read_network_provider_page(connection, NetworkAddressReadScope(manifest, (11,), None))
    readers._result.assert_not_called()


@pytest.mark.parametrize("providers", [False, True])
async def test_provider_only_projection_guards_real_tables(monkeypatch, providers):
    from process import network_candidate_custody as custody
    from tests.test_registry_source_inventory_provider_read import APPROVED, CONTROL, _fixture, _result

    fixture = _fixture()
    manifest = fixture.manifest
    object.__setattr__(manifest, "_custody_control", {"candidate_table_oid": 11})
    object.__setattr__(
        manifest,
        "_candidate_relation_pins",
        (
            CompactAddressRelation(manifest.schema_name, "network_provider_only_evidence", 21),
            CompactAddressRelation(manifest.schema_name, "network_source_inventory", 22),
        ),
    )
    header = CompactAddressRelation(CONTROL, "network_membership_candidate", 11)
    approved_pin = CompactAddressRelation(CONTROL, "registry_approved_record", 13)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        get_server_pid=lambda: _PID,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(side_effect=[fixture.candidate, _result(fixture, providers=providers)]),
    )
    resolver = AsyncMock(return_value=manifest)
    guard = AsyncMock()
    monkeypatch.setattr(provider_only, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(custody, "read_network_control_relation_pins", AsyncMock(return_value=(header,)))
    monkeypatch.setattr(
        provider_only, "pin_api_retained_approved_membership_source", AsyncMock(return_value=(APPROVED, approved_pin))
    )
    monkeypatch.setattr(provider_only, "verify_consumed_network_relations", guard)
    legacy = AsyncMock(side_effect=AssertionError("projection selected legacy Source hydration"))
    monkeypatch.setattr(provider_only, "_source_bindings", legacy)
    native_decode = provider_only._result

    def decode(receipt, *arguments):
        assert guard.await_count == 2
        return native_decode(receipt, *arguments)

    monkeypatch.setattr(provider_only, "_result", Mock(side_effect=decode))
    document = await provider_only.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert document["total_count"] == int(providers)
    assert guard.await_args_list[0].args[1] == (header,)
    assert {pin.table_oid for pin in guard.await_args_list[1].args[1]} == {13, 21, 22}
    assert resolver.await_args.kwargs["api_read"] is True
    legacy.assert_not_awaited()


@pytest.mark.parametrize("failure_at", [1, 2])
async def test_provider_only_refusal_precedes_projection_decode(monkeypatch, failure_at):
    from process import network_candidate_custody as custody
    from tests.test_registry_source_inventory_provider_read import APPROVED, CONTROL, _fixture, _result

    fixture = _fixture()
    object.__setattr__(fixture.manifest, "_custody_control", {"candidate_table_oid": 11})
    object.__setattr__(
        fixture.manifest,
        "_candidate_relation_pins",
        (
            CompactAddressRelation(fixture.manifest.schema_name, "network_provider_only_evidence", 21),
            CompactAddressRelation(fixture.manifest.schema_name, "network_source_inventory", 22),
        ),
    )
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        get_server_pid=lambda: _PID,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(side_effect=[fixture.candidate, _result(fixture, providers=False)]),
    )
    monkeypatch.setattr(provider_only, "resolve_network_serving_manifest", AsyncMock(return_value=fixture.manifest))
    monkeypatch.setattr(
        custody,
        "read_network_control_relation_pins",
        AsyncMock(return_value=(CompactAddressRelation(CONTROL, "network_membership_candidate", 11),)),
    )
    monkeypatch.setattr(
        provider_only,
        "pin_api_retained_approved_membership_source",
        AsyncMock(return_value=(APPROVED, CompactAddressRelation(CONTROL, "registry_approved_record", 13))),
    )
    guard = AsyncMock(side_effect=[None] * (failure_at - 1) + [NetworkReadIdentityError("synthetic mismatch")])
    monkeypatch.setattr(provider_only, "verify_consumed_network_relations", guard)
    decoder = Mock(side_effect=AssertionError("unguarded projection decoded"))
    monkeypatch.setattr(provider_only, "_result", decoder)
    with pytest.raises(readers.NetworkProviderReadUnavailable):
        await provider_only.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    decoder.assert_not_called()


async def test_canceled_provider_projection_never_decodes_or_exposes_partial_page(monkeypatch):
    from tests.test_registry_source_inventory_provider_read import CONTROL, _fixture, _spies

    fixture = _fixture()
    connection, _, _, events = _spies(monkeypatch, fixture)
    original = connection.fetchrow.side_effect
    entered = asyncio.Event()

    async def fetch(query, *arguments):
        if len(arguments) == 2:
            return await original(query, *arguments)
        entered.set()
        await asyncio.Event().wait()

    connection.fetchrow.side_effect = fetch
    decoder = Mock(side_effect=AssertionError("canceled page was decoded"))
    monkeypatch.setattr(provider_only, "_result", decoder)
    task = asyncio.create_task(
        provider_only.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    decoder.assert_not_called()
    assert events == ["resolve", "candidate", "candidate_guard"]
