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
from process.network_custom_address_source import CustomAddressSourceReceipt, _arguments
from process.registry_approval_store import RegistryApprovalConflict
from tests.test_registry_publication_worker import configuration


def _settings(reader_count=None):
    configured = configuration()
    if reader_count is not None:
        configured["consumer"]["reader_roles"] = [f"registry_reader_{index:03d}" for index in range(reader_count)]
    return worker.parse_registry_publication_worker_config(json.dumps(configured))


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
        self.input_readable = True

    async def fetchval(self, query, role, schema, table):
        assert "has_schema_privilege" in query and "has_table_privilege" in query
        assert role == _settings().consumer.execution_role
        assert schema == "retained_base" and table == '"retained_base"."entity_address_unified"'
        return self.input_readable

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
@pytest.mark.parametrize("reader_count", [1, 29])
@pytest.mark.parametrize(
    "outcome", ["materialize", "retained", "helper_failure", "identity", "roles", "input_reader", "cancel"]
)
async def test_custodian_callback_preserves_complete_census(monkeypatch, outcome, reader_count):
    """Keep every accepted actor through commit, failure, cancellation and retained-only preparation."""
    settings = _settings(reader_count)
    connection = _PreparationConnection()
    connection.input_readable = outcome != "input_reader"
    identity_by_name = {
        "schema_owner_role": settings.consumer.queue_owner_role,
        "custody_owner_role": "registry_custody_owner",
    }
    roles = worker._custody_runtime_roles(settings, identity_by_name, settings.consumer)
    assert len(roles) == reader_count + 4
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
    from process import registry_composition_resume as resume

    monkeypatch.setattr(resume, "resolve_registered_compact_format", AsyncMock(return_value=None))
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
    await _assert_preparation_outcome(operation, connection, address_sources, artifact, identity, reach, outcome)


@pytest.mark.parametrize("role_count", [0, 33, 128, 129])
def test_address_artifact_census_has_the_same_bound_as_candidate_custody(role_count):
    roles = tuple(f"runtime_actor_{index:03d}" for index in range(role_count))
    identity = uuid4()
    if role_count in {0, 129}:
        with pytest.raises(composition.RegistryCompositionError, match="artifact_roles_differ"):
            composition.RegistryAddressArtifactCustody("custody_owner", roles, "registry")
        with pytest.raises(ValueError, match="sorted unique bounded tuple"):
            _arguments(_sources().base_source, str(identity), 4, "custody_owner", roles, None)
        return
    artifact = composition.RegistryAddressArtifactCustody("custody_owner", roles, "registry")
    assert artifact.runtime_roles == roles
    assert custody._final_roles(artifact.owner_role, roles)["reader_roles"] == list(roles)
    assert _arguments(_sources().base_source, str(identity), 4, artifact.owner_role, roles, None) == (
        "network_composition_" + identity.hex
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError], ids=["held", "lost", "cancelled"])
async def test_ptg_preparation_retains_same_fenced_session_driver_store_and_budget(monkeypatch, failure):
    from process import registry_company_approval_fence as fence

    driver, store, budget, sessions = object(), object(), object(), object()
    raw = SimpleNamespace(driver_connection=driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=raw))
    session = SimpleNamespace(connection=AsyncMock(return_value=connection))
    config = SimpleNamespace(
        control_schema="registry",
        composition_runtime=SimpleNamespace(store=store, read_budget=budget),
    )
    events = []

    @asynccontextmanager
    async def transaction(factory, **options):
        assert factory is sessions and options == {"control_schema": "registry"}
        events.append("begin_fenced_snapshot")
        yield session
        events.append("commit_and_release")

    async def require(actual, schema):
        assert actual is session and schema == "registry"
        assert not session.connection.await_count
        events.append("verify_native_fence")
        if failure is not None:
            raise failure

    monkeypatch.setattr(fence, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(fence, "require_registry_company_approval_fence", require)
    if failure is not None:
        with pytest.raises(failure):
            async with worker._address_preparation_snapshot(None, sessions, config):
                pytest.fail("Unverified preparation reached the raw driver")
        session.connection.assert_not_awaited()
        assert events == ["begin_fenced_snapshot", "verify_native_fence"]
        return
    async with worker._address_preparation_snapshot(None, sessions, config) as (actual, authority):
        assert actual is driver and authority == (session, store, budget)
        assert events == ["begin_fenced_snapshot", "verify_native_fence"]
    assert events == ["begin_fenced_snapshot", "verify_native_fence", "commit_and_release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["none", "generation", "schema_oid", "ordinary_archive", "missing", "refused"])
async def test_cms_dependency_uses_only_authenticated_existing_pair_readers(monkeypatch, drift):
    from process import network_cms_registry_source_pair as pairs
    from process import network_custom_address_source as addresses

    pin = PinnedAddressSource("entity_address_archive_" + uuid4().hex, "entity_address_unified", "a" * 64)
    retained = SimpleNamespace(
        address_source=replace(pin, generation_id="b" * 64) if drift == "generation" else pin,
        address_ownership=SimpleNamespace(schema_oid=42),
        runtime_roles=("source_input_reader",),
    )
    comment = {"ordinary_archive": "ordinary-archive-v1:", "missing": None}.get(drift, pairs._COMMENT)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=comment))
    read = AsyncMock(return_value=(retained, 43 if drift == "schema_oid" else 42))
    verify = AsyncMock(side_effect=ValueError("pair_custody_changed") if drift == "refused" else None)
    monkeypatch.setattr(pairs, "_read_committed_pair", read)
    monkeypatch.setattr(pairs, "require_registry_cms_source_pair_connection", verify)
    if drift in {"generation", "schema_oid", "refused"}:
        with pytest.raises(ValueError):
            await addresses._retained_dependency_readers(connection, pin)
    else:
        readers = await addresses._retained_dependency_readers(connection, pin)
        assert readers == (retained.runtime_roles if drift == "none" else None)
    assert read.await_count == (0 if drift in {"ordinary_archive", "missing"} else 1)
    assert verify.await_count == (1 if drift in {"none", "refused"} else 0)
    if verify.await_count:
        verify.assert_awaited_once_with(connection, retained, verify_content=False)


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


async def _assert_preparation_outcome(operation, connection, address_sources, artifact, identity, reach, outcome):
    """Check every success, retained-only, failure and cancellation outcome after the callback."""
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
