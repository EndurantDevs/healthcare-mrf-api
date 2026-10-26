# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent address preparation precedes a fresh execution snapshot and authority."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from process import network_candidate_custody as custody
from process import registry_candidate_composition as composition
from process import registry_publication_consumer as consumer
from process import registry_publication_execution as execution
from process import registry_publication_worker as worker
from process.network_address_projection import PinnedAddressSource
from process.network_custom_address_source import CustomAddressSourceReceipt
from process.registry_approval_store import RegistryApprovalConflict
from tests.test_registry_publication_worker import configuration


def _settings():
    return worker.parse_registry_publication_worker_config(json.dumps(configuration()))


def _sources():
    return composition.RegistryCompositionAddressSources(
        PinnedAddressSource("retained_base", "entity_address_unified", "a" * 64)
    )


def test_address_callback_is_private_callable_configuration():
    settings = _settings()
    callback = AsyncMock()
    assert replace(settings.consumer, prepare_address_sources=callback).prepare_address_sources is callback
    assert "AsyncMock" not in repr(replace(settings.consumer, prepare_address_sources=callback))
    with pytest.raises(ValueError, match="consumer_config_invalid"):
        replace(settings.consumer, prepare_address_sources={})
    configured = configuration()
    configured["consumer"]["prepare_address_sources"] = {}
    with pytest.raises(ValueError, match="worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(configured))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "prepare_failure", "lease", "revoked"])
async def test_address_preparation_rechecks_lease_and_authority_before_composition(monkeypatch, outcome):
    events = []
    address_sources, prepared = _sources(), _sources()
    source_pin = object()
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 7},
    }

    async def prepare(**options):
        assert options == dict(
            request_id=request_by_name["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=address_sources,
        )
        events.append("prepare")
        if outcome == "prepare_failure":
            raise ValueError("preparation_refused")
        return prepared

    async def request(*_args):
        events.append("lease")
        if outcome == "lease" and events.count("lease") == 2:
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return request_by_name

    async def authorize(*_args):
        events.append("authority")
        if outcome == "revoked" and events.count("authority") == 2:
            raise PermissionError("authority_denied")

    monkeypatch.setattr(consumer, "_request", request)
    monkeypatch.setattr(consumer, "_fresh_authority", authorize)
    config = replace(_settings().consumer, prepare_address_sources=None if outcome == "legacy" else prepare)
    operation = consumer._prepared_address_sources(
        object(), config, request_by_name, 4, source_pin, address_sources, object()
    )
    if outcome in {"legacy", "complete"}:
        assert await operation is (address_sources if outcome == "legacy" else prepared)
        assert events == ([] if outcome == "legacy" else ["lease", "authority", "prepare", "lease", "authority"])
    else:
        with pytest.raises(PermissionError if outcome == "revoked" else ValueError):
            await operation
        assert (
            events
            == {
                "prepare_failure": ["lease", "authority", "prepare"],
                "lease": ["lease", "authority", "prepare", "lease"],
                "revoked": ["lease", "authority", "prepare", "lease", "authority"],
            }[outcome]
        )


class _DriverUUID(UUID):
    pass


class _PreparationConnection:
    def __init__(self):
        self.active = False
        self.committed = False

    @asynccontextmanager
    async def transaction(self, **options):
        assert options == {"isolation": "repeatable_read"} and not self.active
        self.active = True
        try:
            yield self
        except BaseException:
            raise
        else:
            self.committed = True
        finally:
            self.active = False


def _receipt(address_sources, roles):
    identity = uuid4()
    return CustomAddressSourceReceipt(
        composition_id=str(identity),
        approved_revision=4,
        control_schema="registry",
        base_source=address_sources.base_source,
        npi_source=None,
        source_oids=(1, 2, 3, "a" * 64),
        npi_oids=None,
        owner_role="registry_custody_owner",
        runtime_roles=roles,
        schema_oid=4,
        address_table_oid=5,
        binding_table_oid=6,
        office_lookup_index_oid=7,
        owner_oid=8,
        custom_pair_count=0,
        approved_records_sha256="b" * 64,
        generation_sha256="c" * 64,
        address_source=PinnedAddressSource("network_composition_" + identity.hex, "entity_address_unified", "c" * 64),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["materialize", "retained", "helper_failure", "identity", "roles", "cancel"])
async def test_custodian_callback_commits_only_verified_original_actor_and_complete_census(monkeypatch, outcome):
    settings = _settings()
    connection = _PreparationConnection()
    identity_by_name = {
        "schema_owner_role": settings.consumer.queue_owner_role,
        "custody_owner_role": "registry_custody_owner",
    }
    roles = worker._custody_runtime_roles(settings, identity_by_name, settings.consumer)
    address_sources = _sources()
    artifact = _receipt(address_sources, roles)

    @asynccontextmanager
    async def acquire():
        yield connection

    async def prepare(actual, **options):
        assert actual is connection and connection.active and not connection.committed
        assert type(options["request_id"]) is UUID
        assert options["custody"] == composition.RegistryAddressArtifactCustody(
            "registry_custody_owner", roles, "registry"
        )
        assert {settings.consumer.execution_role, settings.consumer.physical_owner_role} <= set(roles)
        if outcome == "helper_failure":
            raise ValueError("preparation_refused")
        if outcome == "cancel":
            raise asyncio.CancelledError
        return None if outcome == "retained" else artifact

    identity = AsyncMock(side_effect=[identity_by_name, {} if outcome == "identity" else identity_by_name])
    reach = AsyncMock(side_effect=[None, ValueError("role_reach_changed") if outcome == "roles" else None])
    monkeypatch.setattr(worker, "_custody_identity", identity)
    monkeypatch.setattr(custody, "_q_reach_closed", reach)
    monkeypatch.setattr(composition, "prepare_registry_composition_address_artifact", prepare)
    callback = worker._address_preparer(
        settings, settings.consumer, SimpleNamespace(acquire=acquire), identity_by_name, "custodian_login"
    )
    operation = callback(
        request_id=_DriverUUID(str(uuid4())),
        approved_revision=4,
        source_manifest=None,
        expected_head=0,
        address_sources=address_sources,
    )
    if outcome in {"materialize", "retained"}:
        prepared_sources = await operation
        assert connection.committed and not connection.active
        assert (
            prepared_sources is address_sources
            if outcome == "retained"
            else prepared_sources.prepared_address_artifact is artifact
        )
        assert identity.await_count == reach.await_count == 2
    else:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else ValueError):
            await operation
        assert not connection.committed and not connection.active


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "prepare_failure", "revoked", "lease", "digest"])
async def test_retained_operation_preparation_fences_exact_request_and_purpose(monkeypatch, outcome):
    events = []
    address_sources, prepared = _sources(), _sources()
    source_pin = object()
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 7, "expected_approved_revision": 4},
    }

    async def request(*_args):
        events.append("lease")
        if outcome == "lease" and events.count("lease") == 2:
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return {**request_by_name, "request_sha256": "b" * 64} if outcome == "digest" else request_by_name

    async def authority(_authorize, actual, operation):
        assert actual == request_by_name and operation == "prepare_source_rollback"
        events.append("authority")
        if outcome == "revoked" and events.count("lease") == 2:
            raise PermissionError("authority_denied")

    async def prepare(**options):
        assert options == dict(
            request_id=request_by_name["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=address_sources,
        )
        events.append("prepare")
        if outcome == "prepare_failure":
            raise ValueError("preparation_refused")
        return prepared

    monkeypatch.setattr(execution, "_request", request)
    monkeypatch.setattr(execution, "_fresh_operation_authority", authority)
    operation = execution._prepared_operation_addresses(
        object(),
        request_by_name,
        source_pin,
        address_sources,
        None if outcome == "legacy" else prepare,
        object(),
        "registry",
        "prepare_source_rollback",
    )
    if outcome in {"legacy", "complete"}:
        assert await operation is (address_sources if outcome == "legacy" else prepared)
        assert events == ([] if outcome == "legacy" else ["lease", "authority", "prepare", "lease", "authority"])
    else:
        with pytest.raises(PermissionError if outcome == "revoked" else ValueError):
            await operation
        assert ("prepare" in events) == (outcome != "digest")


@pytest.mark.asyncio
@pytest.mark.parametrize("callbacks", [{"unknown": None}, {"prepare_address_sources": {}}])
async def test_rollback_preparation_rejects_unknown_or_noncallable_callbacks(callbacks):
    with pytest.raises(ValueError, match="registry_publication_address_preparation_invalid"):
        await execution.complete_queued_registry_source_rollback_preview(
            SimpleNamespace(is_in_transaction=lambda: False),
            uuid4(),
            uuid4(),
            authorize=object(),
            writer_roles={},
            **callbacks,
        )
