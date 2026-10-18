# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual direct-LOGIN consumer, HTTP authority and native publication fences."""

import asyncio
import hashlib
import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

from process import network_approved_membership_source as approved_source
from process import network_custom_address_source as custom_addresses
from process import network_em_provider_inputs as provider_inputs
from process import network_fhir_membership_source as fhir_sources
from process import network_fhir_source_custody as fhir_custody
from process import network_fhir_source_epoch as fhir_epochs
from process import network_membership_candidate_lifecycle as lifecycle
from process import network_serving_read as serving_read
from process import registry_candidate_composition as composition
from process import registry_publication_consumer as consumer
from process import registry_source_recipe_composition as source_recipes
from process import registry_source_retention as retention
from process import registry_source_selection_receipt as selection_receipts
from process.network_address_projection import PinnedAddressSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_publication_authority import RegistryPublicationAuthorityClient
from process.registry_publication_consumer import RegistryPublicationConsumerConfig, consume_registry_publication_once
from process.registry_publication_execution import _source_rollback_address_sources
from process.registry_publication_queue import (
    RegistryReactivationCommand,
    enqueue_registry_publication,
    enqueue_registry_reactivation,
    enqueue_registry_source_rollback_preview,
    enqueue_registry_source_rollback_publication,
    read_registry_publication_request,
)
from process.registry_record_store import RegistryActor
from process.registry_source_recipe_composition import RegistrySourceRecipeStage
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests import cms_registry_recipe_postgres_support as cms_recipes
from tests.test_network_custom_address_source_postgres import _draft
from tests.test_network_fhir_membership_source_postgres import fhir_source as fhir_source
from tests.test_network_fhir_membership_source_postgres import reviewed_fhir_source as reviewed_fhir_source
from tests.test_registry_approval_store_postgres import _command, _create
from tests.test_registry_publication_authority import _reply
from tests.test_registry_publication_worker_permissions_postgres import (
    _fixture_dsn,
    _install,
    _login,
    _options,
)
from tests.test_registry_publication_worker_permissions_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_publication_worker_permissions_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_publication_worker_permissions_postgres import (
    worker_permissions as worker_permissions,
)
from tests.test_registry_source_recipe_composition_postgres import _publish_raw_recipe
from tests.test_registry_source_retention import candidate_receipt

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _consumer(fixture, *, authority_hook=None):
    await _install(fixture)
    config = RegistryPublicationConsumerConfig(**_options(fixture), reader_roles=(fixture.fixture.roles["reader"],))
    dsn = _fixture_dsn()
    pools = []
    calls = []

    async def authority(request):
        payload = json.loads(request.content)
        calls.append(payload)
        if authority_hook is not None:
            status = await authority_hook(payload, len(calls))
            if isinstance(status, httpx.Response):
                return status
            if status is not None:
                return httpx.Response(status, json={"error": "refused"})
        name = "selection" if "selection" in payload else "command"
        digest = hashlib.sha256(json.dumps(payload[name], sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        return _reply(
            {
                "authorized": True,
                "actor": payload["actor"],
                name + "_sha256": digest,
                "policy_revision": len(calls),
            },
        )

    try:
        for kind in ("lifecycle", "execution"):
            pools.append(
                await asyncpg.create_pool(
                    dsn, user=fixture.roles_by_kind[kind], password=fixture.role_password, min_size=1, max_size=2
                )
            )
        async with RegistryPublicationAuthorityClient(
            "https://authority.example.test", "synthetic-service-token", transport=httpx.MockTransport(authority)
        ) as authorize:
            yield (
                dict(
                    lifecycle_pool=pools[0],
                    execution_pool=pools[1],
                    authorize=authorize,
                    config=config,
                    initial_address_sources=RegistryCompositionAddressSources(fixture.fixture.base),
                ),
                calls,
            )
    finally:
        for pool in pools:
            await pool.close()


async def _enqueue_ordinary(fixture, head=0):
    draft_record = await _draft(fixture.fixture, _create(), fixture.seed.actor)
    command = await _command(fixture.fixture.connection, fixture.fixture.control_schema, draft_record)
    async with _login(fixture, "api") as connection, connection.transaction():
        return await enqueue_registry_publication(
            connection,
            command,
            fixture.seed.actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=head,
            control_schema=fixture.fixture.control_schema,
        )


async def _status(fixture, queued):
    return await read_registry_publication_request(
        fixture.fixture.connection,
        UUID(queued["request_id"]),
        control_schema=fixture.fixture.control_schema,
    )


async def _head(fixture):
    return await fixture.fixture.connection.fetchval(
        f'SELECT generation_id FROM "{fixture.fixture.control_schema}".network_serving_control WHERE id=1'
    )


async def _publish_same_approval(fixture, config):
    connection = fixture.fixture.connection
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        source_pin = await resolve_network_serving_manifest(connection, control_schema=config.control_schema)
    address_sources = await _source_rollback_address_sources(connection, source_pin, config.control_schema)
    copy_target, address_source = await compose_registry_membership_candidate(
        connection,
        request_id=uuid4(),
        approved_revision=source_pin.approved_custom_revision,
        expected_head=source_pin.generation_id,
        source_manifest=source_pin,
        address_sources=address_sources,
        writer_roles=config.writer_roles,
        control_schema=config.control_schema,
    )
    await prepare_and_publish_network_candidate(
        connection, copy_target, address_source, **config.writer_roles, control_schema=config.control_schema
    )
    return await _head(fixture)


async def _complete_reviewed_source_pair(fixture, arguments, actor, revision, first_head, second_head):
    command_by_name = dict(
        operation="prepare_source_rollback",
        target_generation=second_head,
        expected_serving_generation=first_head,
        expected_approved_revision=revision,
        reason="Review source edition",
        idempotency_key=uuid4().hex,
    )
    async with _login(fixture, "api") as connection, connection.transaction():
        preview_queue = await enqueue_registry_source_rollback_preview(
            connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.fixture.control_schema,
        )
    preview = await consume_registry_publication_once(**arguments)
    assert preview["result"]["operation"] == "prepare_source_rollback" and await _head(fixture) == first_head
    command_by_name = dict(
        operation="publish_source_rollback",
        preview_request_id=preview_queue["request_id"],
        candidate_sha256=preview["result"]["candidate_sha256"],
        expected_serving_generation=first_head,
        expected_approved_revision=revision,
        reason="Publish reviewed source",
        idempotency_key=uuid4().hex,
    )
    async with _login(fixture, "api") as connection, connection.transaction():
        await enqueue_registry_source_rollback_publication(
            connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.fixture.control_schema,
        )
    result_by_name = await consume_registry_publication_once(**arguments)
    assert result_by_name["result"]["operation"] == "publish_source_rollback"
    assert await _head(fixture) == result_by_name["result"]["generation_id"]


async def test_missing_initial_source_refuses_before_approval(worker_permissions):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    schema = fixture.fixture.control_schema
    approval_sql = (
        f'SELECT approved_revision, (SELECT count(*) FROM "{schema}".registry_approval_history) AS approvals '
        f'FROM "{schema}".registry_revision_control WHERE id=1'
    )
    async with _consumer(fixture) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture)
        before = await connection.fetchrow(approval_sql)
        arguments["initial_address_sources"] = None
        with pytest.raises(ValueError, match="registry_publication_request_refused"):
            await consume_registry_publication_once(**arguments)
        assert await connection.fetchrow(approval_sql) == before
        assert await _head(fixture) is None
        assert (await _status(fixture, queued))["state"] == "rejected"
        assert not calls


async def test_actual_consumer_runs_all_four_operations_and_empty_queue(worker_permissions):
    fixture = worker_permissions
    async with _consumer(fixture) as (arguments, calls):
        assert await consume_registry_publication_once(**arguments) is None
        queued = await _enqueue_ordinary(fixture)
        first = await consume_registry_publication_once(**arguments)
        assert first["request_id"] == queued["request_id"] and (await _status(fixture, queued))["state"] == "completed"
        first_head = await _head(fixture)
        arguments["initial_address_sources"] = None
        queued = await _enqueue_ordinary(fixture, first_head)
        await consume_registry_publication_once(**arguments)
        first_head = await _head(fixture)
        second_head = await _publish_same_approval(fixture, arguments["config"])
        revision = await fixture.fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{fixture.fixture.control_schema}".registry_revision_control WHERE id=1'
        )
        # Retained operations require the existing administrator actor contract.
        actor = RegistryActor("platform_admin", uuid4(), "system")
        async with _login(fixture, "api") as connection, connection.transaction():
            await enqueue_registry_reactivation(
                connection,
                RegistryReactivationCommand(first_head, second_head, revision, "Restore retained edition", uuid4().hex),
                actor,
                session_token_sha256="a" * 64,
                control_schema=fixture.fixture.control_schema,
            )
        result_by_name = await consume_registry_publication_once(**arguments)
        assert result_by_name["result"]["operation"] == "reactivate_generation" and await _head(fixture) == first_head
        await _complete_reviewed_source_pair(fixture, arguments, actor, revision, first_head, second_head)
        assert len(calls) >= 20


@pytest.mark.parametrize("status", [403, 408, 429, 503])
async def test_authority_denial_or_unavailable_never_publishes(worker_permissions, status):
    fixture = worker_permissions

    async def refuse(_payload, _count):
        return status

    async with _consumer(fixture, authority_hook=refuse) as (arguments, _):
        queued = await _enqueue_ordinary(fixture)
        with pytest.raises(ValueError if status == 403 else RuntimeError):
            await consume_registry_publication_once(**arguments)
        assert await _head(fixture) is None
        assert (await _status(fixture, queued))["state"] == ("rejected" if status == 403 else "running")


@pytest.mark.parametrize("operation", ["reactivate_generation", "prepare_source_rollback"])
async def test_retained_operation_rejection_remains_readable(worker_permissions, operation):
    """Denied retained requests keep a terminal result accepted by the status reader."""
    fixture = worker_permissions
    denial_by_name = {"enabled": False}

    async def refuse(_payload, _count):
        return 403 if denial_by_name["enabled"] else None

    async with _consumer(fixture, authority_hook=refuse) as (arguments, _):
        await _enqueue_ordinary(fixture)
        await consume_registry_publication_once(**arguments)
        first_head = await _head(fixture)
        current_head = await _publish_same_approval(fixture, arguments["config"])
        revision = await fixture.fixture.connection.fetchval(
            f'SELECT approved_revision FROM "{fixture.fixture.control_schema}".registry_revision_control WHERE id=1'
        )
        actor = RegistryActor("platform_admin", uuid4(), "system")
        fields_by_name = dict(
            target_generation=first_head,
            expected_serving_generation=current_head,
            expected_approved_revision=revision,
            reason="Review retained edition",
            idempotency_key=uuid4().hex,
        )
        async with _login(fixture, "api") as connection, connection.transaction():
            if operation == "reactivate_generation":
                queued = await enqueue_registry_reactivation(
                    connection,
                    RegistryReactivationCommand(**fields_by_name),
                    actor,
                    session_token_sha256="a" * 64,
                    control_schema=fixture.fixture.control_schema,
                )
            else:
                queued = await enqueue_registry_source_rollback_preview(
                    connection,
                    {"operation": operation, **fields_by_name},
                    actor,
                    session_token_sha256="a" * 64,
                    control_schema=fixture.fixture.control_schema,
                )
        denial_by_name["enabled"] = True
        with pytest.raises(ValueError, match="registry_publication_request_refused"):
            await consume_registry_publication_once(**arguments)
        status = await _status(fixture, queued)
        assert status["state"] == "rejected"
        assert status["result"] == {"error": {"code": "registry_publication_rejected"}}
        assert await _head(fixture) == current_head


async def test_expired_lease_restarts_exact_request_after_transient_authority_failure(worker_permissions):
    fixture = worker_permissions
    is_unavailable = True

    async def unavailable(_payload, _count):
        return 503 if is_unavailable else None

    async with _consumer(fixture, authority_hook=unavailable) as (arguments, _):
        queued = await _enqueue_ordinary(fixture)
        with pytest.raises(RuntimeError):
            await consume_registry_publication_once(**arguments)
        await fixture.fixture.connection.execute(
            f'UPDATE "{fixture.fixture.control_schema}".registry_publication_request '
            "SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        is_unavailable = False
        completed = await consume_registry_publication_once(**arguments)
        assert (
            completed["request_id"] == queued["request_id"] and (await _status(fixture, queued))["state"] == "completed"
        )


@asynccontextmanager
async def _worker_provider_inputs(fixture):
    """Retain the existing native nine-heap fixture under the worker's exact roles."""
    from tests.managed_em_read_postgres_support import _close_schema, _held_tables

    connection = fixture.fixture.connection
    schema = "em_inputs_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        inputs = await _held_tables(connection, schema)
        await _close_schema(connection, schema, fixture.roles_by_kind["physical"], fixture.fixture.roles["reader"])
        if await connection.fetchval("SELECT to_regnamespace('tiger') IS NOT NULL"):
            await connection.execute(f'GRANT USAGE ON SCHEMA tiger TO "{fixture.roles_by_kind["execution"]}"')
        yield inputs
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


class _CustodyTimeoutConnection:
    """Inject a real query timeout at the custody catalog-read boundary."""

    def __init__(self, connection, observations):
        self.connection, self.observations = connection, observations

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetch(self, query, *parameters):
        assert query == provider_inputs._CATALOG_SQL
        self.observations.append(await self.connection.fetchrow("SELECT current_user=session_user AS direct"))
        await self.connection.execute("SET LOCAL statement_timeout='1ms'")
        return await self.connection.fetch("SELECT pg_sleep(1)")


async def test_provider_custody_timeout_retains_lease_and_resumes_exact_request(worker_permissions, monkeypatch):
    fixture = worker_permissions
    namespace = f'"{fixture.fixture.control_schema}"'
    original = provider_inputs.verify_candidate_em_provider_inputs
    observations = []

    async def timeout(connection, candidate, *, runtime_roles):
        return await original(
            _CustodyTimeoutConnection(connection, observations), candidate, runtime_roles=runtime_roles
        )

    async with _worker_provider_inputs(fixture) as inputs, _consumer(fixture) as (arguments, calls):
        arguments["initial_address_sources"] = replace(arguments["initial_address_sources"], em_provider_inputs=inputs)
        queued = await _enqueue_ordinary(fixture)
        request_id = UUID(queued["request_id"])
        monkeypatch.setattr(provider_inputs, "verify_candidate_em_provider_inputs", timeout)
        with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        pending = await _status(fixture, queued)
        assert pending["state"] == "running" and pending["result"] is None and await _head(fixture) is None
        assert len(observations) == 1 and observations[0]["direct"] is True
        retained = await fixture.fixture.connection.fetchrow(
            f"SELECT lease_id,request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
            request_id,
        )
        assert retained["lease_id"] is not None
        candidate = await fixture.fixture.connection.fetchrow(
            f"SELECT state,validation_json FROM {namespace}.network_membership_candidate"
        )
        assert candidate["state"] == "ready" and "em_provider_inputs" in json.loads(candidate["validation_json"])
        monkeypatch.setattr(provider_inputs, "verify_candidate_em_provider_inputs", original)
        await fixture.fixture.connection.execute(
            f"UPDATE {namespace}.registry_publication_request SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            request_id,
        )
        before_calls = len(calls)
        completed = await consume_registry_publication_once(**arguments)
        assert completed["request_id"] == queued["request_id"] and len(calls) > before_calls
        assert (await _status(fixture, queued))["state"] == "completed"
        assert await _head(fixture) == completed["result"]["serving_generation"] > 0
        resumed = await fixture.fixture.connection.fetchrow(
            f"SELECT request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
            request_id,
        )
        assert dict(resumed) == {key: retained[key] for key in ("request_sha256", "command_json")}
        assert await consume_registry_publication_once(**arguments) is None


class _SealTimeoutConnection:
    """Inject one real native query timeout inside the actual retention verifier."""

    def __init__(self, connection):
        self.connection = connection

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetchval(self, *arguments, **options):
        await self.connection.execute("SET LOCAL statement_timeout='1ms'")
        return await self.connection.fetchval("SELECT pg_sleep(1)")


def _cancel_database_read(fixture, queued, monkeypatch, boundary, failure_by_field):
    module, name = (
        (serving_read, "_read_manifest")
        if boundary == "manifest"
        else (lifecycle, "verify_registry_source_retention_rows")
    )
    original = getattr(module, name)

    async def cancel(connection, *parameters, **options):
        namespace = f'"{fixture.fixture.control_schema}"'
        failure_by_field["lease_id"] = await fixture.fixture.connection.fetchval(
            f"SELECT lease_id FROM {namespace}.registry_publication_request WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        if boundary == "seal":
            return await original(_SealTimeoutConnection(connection), *parameters, **options)
        await connection.execute("SET LOCAL statement_timeout='1ms'")
        return await connection.fetchrow("SELECT pg_sleep(1)")

    monkeypatch.setattr(module, name, cancel)
    return lambda: monkeypatch.setattr(module, name, original)


@pytest.mark.parametrize("boundary", ["manifest", "seal"])
async def test_database_failure_after_approval_resumes_exact_request(worker_permissions, monkeypatch, boundary):
    fixture = worker_permissions
    async with _consumer(fixture) as (arguments, calls):
        first = await _enqueue_ordinary(fixture)
        await consume_registry_publication_once(**arguments)
        assert (await _status(fixture, first))["state"] == "completed"
        first_head = await _head(fixture)
        queued = await _enqueue_ordinary(fixture, first_head)
        namespace = f'"{fixture.fixture.control_schema}"'
        revision_query = f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        before_revision = await fixture.fixture.connection.fetchval(revision_query)
        failure_by_field = {}
        restore = _cancel_database_read(fixture, queued, monkeypatch, boundary, failure_by_field)
        with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        committed_revision = await fixture.fixture.connection.fetchval(revision_query)
        assert committed_revision > before_revision
        pending = await _status(fixture, queued)
        assert pending["state"] == "running" and pending["result"] is None
        assert await _head(fixture) == first_head
        retained = await fixture.fixture.connection.fetchrow(
            f"SELECT lease_id,request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        assert retained["lease_id"] == failure_by_field["lease_id"] and retained["lease_id"] is not None
        restore()
        await fixture.fixture.connection.execute(
            f"UPDATE {namespace}.registry_publication_request SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        authority_calls = len(calls)
        completed = await consume_registry_publication_once(**arguments)
        assert completed["request_id"] == queued["request_id"] and len(calls) > authority_calls
        assert (await _status(fixture, queued))["state"] == "completed"
        assert await _head(fixture) == completed["result"]["serving_generation"] > first_head
        assert await fixture.fixture.connection.fetchval(revision_query) == committed_revision
        resumed = await fixture.fixture.connection.fetchrow(
            f"SELECT request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        assert dict(resumed) == {key: retained[key] for key in ("request_sha256", "command_json")}
        assert await consume_registry_publication_once(**arguments) is None


def _retention_failure_operation(connection, monkeypatch, reader):
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    candidate_by_field = {
        **candidate_receipt(),
        **{
            field: getattr(copy_target, field)
            for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        },
        "state": "sealed" if reader == "verify" else "open",
    }
    if reader == "verify":
        return retention.verify_registry_source_retention_rows(connection, copy_target, candidate_by_field)

    async def locked(*arguments):
        return candidate_by_field

    monkeypatch.setattr(lifecycle, "_locked_candidate", locked)
    stage = RegistrySourceRecipeStage("registry_source_page_" + uuid4().hex, 2, 0, "a" * 64)
    return retention.retain_registry_source_memberships(connection, copy_target, stage)


@pytest.mark.parametrize("reader", ["scope", "document", "page", "retain", "verify"])
async def test_database_wrappers_preserve_transient_cause(monkeypatch, reader):
    failure = asyncpg.CannotConnectNowError("temporary database outage")

    class UnavailableConnection:
        def is_in_transaction(self):
            return True

        @asynccontextmanager
        async def transaction(self):
            yield

        async def fetchrow(self, *arguments):
            raise failure

        async def execute(self, *arguments):
            raise failure

    connection = UnavailableConnection()
    membership_source = approved_source.ApprovedMembershipSource(1, "a" * 64)
    if reader == "scope":
        operation = approved_source.pin_approved_membership_source(connection, approved_revision=1)
    elif reader == "document":
        operation = approved_source.read_approved_membership_commands(connection, membership_source)
    elif reader == "page":

        async def pinned(*arguments, **options):
            return membership_source

        monkeypatch.setattr(approved_source, "_scope", pinned)
        operation = approved_source.read_approved_membership_batch(connection, membership_source)
    else:
        operation = _retention_failure_operation(connection, monkeypatch, reader)
    with pytest.raises(
        (approved_source.ApprovedMembershipSourceError, retention.RegistrySourceRetentionError)
    ) as caught:
        await operation
    assert caught.value.__cause__ is failure
    outer = ValueError("validated source could not be read")
    outer.__cause__ = caught.value
    assert consumer._has_database_cause(outer)


async def test_permanent_validation_does_not_inherit_suppressed_database_context():
    try:
        raise asyncpg.CannotConnectNowError("temporary database outage")
    except asyncpg.PostgresError:
        try:
            raise ValueError("independent validation refusal") from None
        except ValueError as error:
            assert not consumer._has_database_cause(error)
    first, second = ValueError("first"), ValueError("second")
    first.__cause__, second.__cause__ = second, first
    assert not consumer._has_database_cause(first)


class _StorageFailureConnection:
    def __init__(self, failure):
        self.failure = failure

    def is_in_transaction(self):
        return True

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchval(self, query, *parameters):
        if query == "SHOW transaction_isolation":
            return "repeatable read"
        raise self.failure

    async def fetchrow(self, *arguments):
        raise self.failure

    async def fetch(self, *arguments):
        raise self.failure

    async def execute(self, *arguments):
        raise self.failure


def _cms_storage_failure_pin(*, is_epoch):
    membership_pin = fhir_sources.PinnedFHIRMembershipSource(
        "source_example", "cms-npd", "endpoint", "dataset", "a" * 64, "release", 100, "cms-npd", "2026-01-01"
    )
    if not is_epoch:
        return replace(
            membership_pin,
            custody_owner_role="protected_owner",
            custody_runtime_roles=("runtime_reader",),
            custody_proof_sha256="b" * 64,
            custody_catalog_sha256="c" * 64,
        )
    epoch_id = uuid4()
    epoch = fhir_epochs.RetainedCMSFHIRSourceEpoch(
        fhir_epochs._origin(membership_pin),
        epoch_id,
        "registry_cms_epoch_" + epoch_id.hex,
        100,
        tuple((name, index + 100) for index, name in enumerate(sorted(fhir_epochs._TABLES))),
        "protected_owner",
        ("runtime_reader",),
        *("a" * 64 for _ in range(4)),
    )
    return replace(membership_pin, retained_epoch=epoch)


@pytest.mark.parametrize("is_database_failure", [True, False])
@pytest.mark.parametrize("stage", ["address", "custody", "epoch", "recipe_custody", "recipe_epoch", "fhir_epoch"])
async def test_reachable_preparation_storage_cause_remains_distinct(stage, is_database_failure):
    failure = (
        asyncpg.CannotConnectNowError("temporary database outage")
        if is_database_failure
        else ValueError("semantic source refusal")
    )
    connection = _StorageFailureConnection(failure)
    membership_pin = _cms_storage_failure_pin(is_epoch=stage in {"epoch", "recipe_epoch", "fhir_epoch"})
    if stage == "address":
        operation = custom_addresses.prepare_custom_address_source(
            connection,
            PinnedAddressSource("source_example", "entity_address_unified", "a" * 64),
            composition_id=str(uuid4()),
            approved_revision=1,
            owner_role="protected_owner",
            runtime_roles=("runtime_reader",),
        )
    elif stage == "custody":
        operation = fhir_custody.capture_retained_cms_fhir_source_custody(
            connection, membership_pin, owner_role="protected_owner", runtime_roles=("runtime_reader",)
        )
    elif stage == "epoch":
        operation = fhir_epochs.require_retained_cms_fhir_source_epoch(
            connection, membership_pin, membership_pin.retained_epoch
        )
    elif stage.startswith("recipe_"):
        operation = source_recipes._require_recipe_custody(connection, SimpleNamespace(source_pin=membership_pin))
    else:
        operation = fhir_sources._require_pinned_source(connection, membership_pin)
    with pytest.raises(ValueError) as caught:
        await operation
    assert consumer._has_database_cause(caught.value) is is_database_failure


async def test_final_authority_revocation_refuses_prepared_candidate(worker_permissions):
    fixture = worker_permissions

    async def revoke(_payload, count):
        return 403 if count >= 5 else None

    async with _consumer(fixture, authority_hook=revoke) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture)
        with pytest.raises(ValueError):
            await consume_registry_publication_once(**arguments)
        assert len(calls) == 5 and await _head(fixture) is None
        assert (await _status(fixture, queued))["state"] == "rejected"
        assert (
            await fixture.fixture.connection.fetchval(
                f"SELECT count(*) FROM \"{fixture.fixture.control_schema}\".network_membership_candidate WHERE state='ready'"
            )
            == 1
        )


@pytest.mark.parametrize("fault", ["lease", "expiry", "cancel", "renew"])
async def test_independent_heartbeat_or_cancel_fences_execution(worker_permissions, monkeypatch, fault):
    fixture = worker_permissions
    entered = asyncio.Event()
    hold = asyncio.Event()

    async def wait_for_revocation(_payload, _count):
        entered.set()
        await hold.wait()

    monkeypatch.setattr(consumer, "_HEARTBEAT_SECONDS", 0.05)
    async with _consumer(fixture, authority_hook=wait_for_revocation) as (arguments, _):
        queued = await _enqueue_ordinary(fixture)
        pending = asyncio.create_task(consume_registry_publication_once(**arguments))
        await asyncio.wait_for(entered.wait(), 5)
        if fault == "cancel":
            pending.cancel()
        if fault == "lease":
            await fixture.fixture.connection.execute(
                f'UPDATE "{fixture.fixture.control_schema}".registry_publication_request '
                "SET lease_id=$1 WHERE request_id=$2",
                uuid4(),
                UUID(queued["request_id"]),
            )
        if fault == "expiry":
            await fixture.fixture.connection.execute(
                f'UPDATE "{fixture.fixture.control_schema}".registry_publication_request '
                "SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
                UUID(queued["request_id"]),
            )
        if fault == "renew":
            query = f'SELECT lease_expires_at FROM "{fixture.fixture.control_schema}".registry_publication_request WHERE request_id=$1'
            previous_expiry = await fixture.fixture.connection.fetchval(query, UUID(queued["request_id"]))
            await asyncio.sleep(0.15)
            renewed_expiry = await fixture.fixture.connection.fetchval(query, UUID(queued["request_id"]))
            assert renewed_expiry > previous_expiry
            hold.set()
            assert (await asyncio.wait_for(pending, 15))["state"] == "completed"
            assert await _head(fixture) is not None
            return
        with pytest.raises(asyncio.CancelledError if fault == "cancel" else ValueError):
            await asyncio.wait_for(pending, 5)
        assert await _head(fixture) is None
        assert (await _status(fixture, queued))["state"] == "running"


def _pause_heartbeat_verification(monkeypatch, boundary):
    entered = asyncio.Event()
    checkpoint = asyncio.Event()
    release = asyncio.Event()
    hold = asyncio.Event()
    sql_denied = asyncio.Event()
    original_verify, original_renew = consumer._verify_lifecycle, consumer.renew_registry_publication_lease
    checks = []

    async def wait_for_heartbeat(_payload, _count):
        entered.set()
        await hold.wait()

    async def verify_at_boundary(connection, config):
        checks.append(None)
        if len(checks) == 2:
            await entered.wait()
            if boundary == "after_check":
                await original_verify(connection, config)
            checkpoint.set()
            await release.wait()
            if boundary == "after_check":
                return
        await original_verify(connection, config)

    async def record_sql_refusal(*arguments, **options):
        try:
            return await original_renew(*arguments, **options)
        except asyncpg.InsufficientPrivilegeError:
            sql_denied.set()
            raise

    monkeypatch.setattr(consumer, "_verify_lifecycle", verify_at_boundary)
    monkeypatch.setattr(consumer, "renew_registry_publication_lease", record_sql_refusal)
    monkeypatch.setattr(consumer, "_HEARTBEAT_SECONDS", 0.01)
    return SimpleNamespace(
        checkpoint=checkpoint, release=release, hold=hold, sql_denied=sql_denied, authorize=wait_for_heartbeat
    )


@pytest.mark.parametrize("boundary", ["before_check", "after_check"])
async def test_heartbeat_revocation_retains_lease(worker_permissions, monkeypatch, boundary):
    fixture = worker_permissions
    paused = _pause_heartbeat_verification(monkeypatch, boundary)
    async with _consumer(fixture, authority_hook=paused.authorize) as (arguments, _):
        queued = await _enqueue_ordinary(fixture)
        pending = asyncio.create_task(consume_registry_publication_once(**arguments))
        try:
            await asyncio.wait_for(paused.checkpoint.wait(), 5)
            table = f'"{fixture.fixture.control_schema}".registry_publication_request'
            lease_query = f"SELECT request_id,lease_id,lease_expires_at,state FROM {table} WHERE request_id=$1"
            claimed = await fixture.fixture.connection.fetchrow(lease_query, UUID(queued["request_id"]))
            assert claimed["state"] == "running" and claimed["lease_id"] is not None
            role = f'"{fixture.roles_by_kind["lifecycle"]}"'
            await fixture.fixture.connection.execute(f"REVOKE UPDATE(lease_expires_at) ON {table} FROM {role}")
            paused.release.set()
            error = RuntimeError if boundary == "after_check" else ValueError
            code = (
                "registry_publication_consumer_unavailable"
                if boundary == "after_check"
                else "registry_publication_queue_permissions_unavailable"
            )
            with pytest.raises(error, match="^" + code + "$"):
                await asyncio.wait_for(pending, 5)
            assert paused.sql_denied.is_set() == (boundary == "after_check")
            retained = await fixture.fixture.connection.fetchrow(lease_query, UUID(queued["request_id"]))
            assert retained["state"] == "running" and await _head(fixture) is None
            for key in ("request_id", "lease_id", "lease_expires_at"):
                assert retained[key] == claimed[key]
            await fixture.fixture.connection.execute(f"GRANT UPDATE(lease_expires_at) ON {table} TO {role}")
            await fixture.fixture.connection.execute(
                f"UPDATE {table} SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
                UUID(queued["request_id"]),
            )
            paused.hold.set()
            completed = await consume_registry_publication_once(**arguments)
            assert completed["request_id"] == queued["request_id"]
            assert (await _status(fixture, queued))["state"] == "completed" and await _head(fixture) is not None
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


@pytest.mark.parametrize("tamper", ["actor", "digest"])
async def test_nonmatching_authority_receipt_is_rejected_by_actual_execution(worker_permissions, tamper):
    fixture = worker_permissions

    async def invalid(payload, _count):
        actor_by_name = dict(payload["actor"])
        digest = hashlib.sha256(
            json.dumps(payload["selection"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if tamper == "actor":
            actor_by_name["user_id"] = str(uuid4())
        else:
            digest = "0" * 64
        return _reply(
            {"authorized": True, "actor": actor_by_name, "selection_sha256": digest, "policy_revision": 1},
        )

    async with _consumer(fixture, authority_hook=invalid) as (arguments, _):
        queued = await _enqueue_ordinary(fixture)
        with pytest.raises(ValueError, match="registry_publication_request_refused"):
            await consume_registry_publication_once(**arguments)
        assert await _head(fixture) is None and (await _status(fixture, queued))["state"] == "rejected"


@pytest.mark.parametrize("identity", ["claim", "execution"])
async def test_wrong_direct_login_pool_refuses_before_authorized_work(worker_permissions, identity):
    fixture = worker_permissions
    async with _consumer(fixture) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture)
        wrong_kind = "execution" if identity == "claim" else "lifecycle"
        wrong_pool = await asyncpg.create_pool(
            _fixture_dsn(),
            user=fixture.roles_by_kind[wrong_kind],
            password=fixture.role_password,
            min_size=1,
            max_size=1,
        )
        try:
            arguments["lifecycle_pool" if identity == "claim" else "execution_pool"] = wrong_pool
            with pytest.raises(ValueError):
                await consume_registry_publication_once(**arguments)
            assert not calls and await _head(fixture) is None
            assert (await _status(fixture, queued))["state"] == "queued"
        finally:
            await wrong_pool.close()


async def test_execution_permission_drift_retains_request_for_exact_resume(worker_permissions, monkeypatch):
    fixture = worker_permissions
    original_claim = consumer._claim
    revocation_by_name = {"pending": True}
    schema = '"' + fixture.fixture.control_schema + '"'
    role = '"' + fixture.roles_by_kind["execution"] + '"'

    async def revoke_after_claim(pool, config):
        claimed = await original_claim(pool, config)
        if revocation_by_name["pending"]:
            await fixture.fixture.connection.execute(f"REVOKE USAGE ON SCHEMA {schema} FROM {role}")
            revocation_by_name["pending"] = False
        return claimed

    monkeypatch.setattr(consumer, "_claim", revoke_after_claim)
    async with _consumer(fixture) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture)
        initial_approvals = await fixture.fixture.connection.fetchval(
            f"SELECT count(*) FROM {schema}.registry_approval_history"
        )
        with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        assert not calls and await _head(fixture) is None
        assert (await _status(fixture, queued))["state"] == "running"
        assert (
            await fixture.fixture.connection.fetchval(f"SELECT count(*) FROM {schema}.registry_approval_history")
            == initial_approvals
        )
        await fixture.fixture.connection.execute(f"GRANT USAGE ON SCHEMA {schema} TO {role}")
        await fixture.fixture.connection.execute(
            f"UPDATE {schema}.registry_publication_request "
            "SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            UUID(queued["request_id"]),
        )
        completed = await consume_registry_publication_once(**arguments)
        assert completed["request_id"] == queued["request_id"]
        assert (await _status(fixture, queued))["state"] == "completed" and await _head(fixture) is not None
        assert (
            await fixture.fixture.connection.fetchval(f"SELECT count(*) FROM {schema}.registry_approval_history")
            == initial_approvals + 1
        )


class _SourceTimeoutConnection:
    """Keep the real source wrappers and fail one actual native SQL boundary."""

    def __init__(self, connection, boundary):
        self.connection, self.boundary = connection, boundary

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def _timeout(self):
        await self.connection.execute("SET LOCAL statement_timeout='1ms'")
        return await self.connection.fetchrow("SELECT pg_sleep(1)")

    async def fetchval(self, query, *arguments, **options):
        if self.boundary == "recipe-isolation":
            return await self._timeout()
        return await self.connection.fetchval(query, *arguments, **options)

    async def execute(self, query, *arguments, **options):
        if self.boundary == "custody-timeout" and query.startswith("LOCK"):
            return await self._timeout()
        return await self.connection.execute(query, *arguments, **options)

    async def fetchrow(self, query, *arguments, **options):
        if self.boundary in {"recipe-manifest", "selection-approved"} or (
            self.boundary == "selection-candidate" and "network_membership_candidate" in query
        ):
            return await self._timeout()
        return await self.connection.fetchrow(query, *arguments, **options)


def _cancel_source_read(monkeypatch, boundary, failure_by_field):
    if boundary == "custody-timeout":
        module, name = source_recipes, "require_retained_cms_fhir_source_epoch"
    elif boundary.startswith("recipe-"):
        module, name = composition, "resolve_retained_registry_source_recipes"
    else:
        module, name = selection_receipts, "record_registry_source_selection"
    original = getattr(module, name)

    async def cancel(connection, *parameters, **options):
        failure_by_field["calls"] = failure_by_field.get("calls", 0) + 1
        return await original(_SourceTimeoutConnection(connection, boundary), *parameters, **options)

    monkeypatch.setattr(module, name, cancel)
    return lambda: monkeypatch.setattr(module, name, original)


# Only source-resume cases request the existing native admission template.
cms_recipe_template = pytest_asyncio.fixture(scope="module", loop_scope="module")(
    cms_recipes.cms_recipe_template.__wrapped__
)
cms_recipe_database = cms_recipes.cms_recipe_database


@pytest.fixture
async def retained_recipe_worker(cms_recipe_template, cms_recipe_database, monkeypatch, tmp_path):
    """Reuse actual CMS admission, base fixtures and direct worker identities together."""
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN",
        cms_recipe_database.engine.url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False),
    )
    async with AsyncExitStack() as stack:
        schemas = await stack.enter_async_context(aclosing(serving_schema.__wrapped__()))
        registry_schema = await anext(schemas)
        custom = await stack.enter_async_context(aclosing(custom_db.__wrapped__(registry_schema)))
        custom_fixture = await anext(custom)
        workers = await stack.enter_async_context(aclosing(worker_permissions.__wrapped__(custom_fixture)))
        fixture = await anext(workers)
        sources = await stack.enter_async_context(
            aclosing(
                cms_recipes.reviewed_cms_source.__wrapped__(cms_recipe_database, registry_schema, monkeypatch, tmp_path)
            )
        )
        reviewed_source = await anext(sources)
        source_pin = reviewed_source[0][2]
        reader = source_pin.retained_epoch.runtime_roles[0]
        await fixture.fixture.connection.execute(
            f'GRANT "{reader}" TO "{fixture.roles_by_kind["execution"]}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
        yield fixture, reviewed_source


async def _retained_recipe_head(fixture, reviewed_source):
    copy_targets = []
    manifest = await _publish_raw_recipe(fixture.fixture, reviewed_source, copy_targets)
    assert "registry_source_recipes" in manifest.source_generations
    return manifest.generation_id


async def _resume_source_failure(fixture, arguments, queued, head, revision_query, committed_revision):
    namespace = f'"{fixture.fixture.control_schema}"'
    pending = await _status(fixture, queued)
    assert pending["state"] == "running" and pending["result"] is None
    assert await _head(fixture) == head
    retained = await fixture.fixture.connection.fetchrow(
        f"SELECT lease_id,request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
        UUID(queued["request_id"]),
    )
    assert retained["lease_id"] is not None
    await fixture.fixture.connection.execute(
        f"UPDATE {namespace}.registry_publication_request SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
        UUID(queued["request_id"]),
    )
    completed = await consume_registry_publication_once(**arguments)
    assert completed["request_id"] == queued["request_id"]
    assert (await _status(fixture, queued))["state"] == "completed"
    assert await _head(fixture) == completed["result"]["serving_generation"] > head
    assert await fixture.fixture.connection.fetchval(revision_query) == committed_revision
    resumed = await fixture.fixture.connection.fetchrow(
        f"SELECT request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1",
        UUID(queued["request_id"]),
    )
    assert dict(resumed) == {key: retained[key] for key in ("request_sha256", "command_json")}
    assert await consume_registry_publication_once(**arguments) is None


@pytest.mark.parametrize(
    "boundary", ["recipe-isolation", "recipe-manifest", "selection-approved", "selection-candidate", "custody-timeout"]
)
async def test_retained_recipe_database_failure_resumes_exact_request(retained_recipe_worker, monkeypatch, boundary):
    fixture, reviewed_source = retained_recipe_worker
    head = await _retained_recipe_head(fixture, reviewed_source)
    async with _consumer(fixture) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture, head)
        namespace = f'"{fixture.fixture.control_schema}"'
        revision_query = f"SELECT approved_revision FROM {namespace}.registry_revision_control WHERE id=1"
        before_revision = await fixture.fixture.connection.fetchval(revision_query)
        failure_by_field = {}
        restore = _cancel_source_read(monkeypatch, boundary, failure_by_field)
        with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        assert failure_by_field["calls"] == 1 and calls
        committed_revision = await fixture.fixture.connection.fetchval(revision_query)
        assert committed_revision > before_revision
        restore()
        await _resume_source_failure(fixture, arguments, queued, head, revision_query, committed_revision)


@pytest.mark.parametrize("stage", ["custody", "epoch", "recipe_custody", "recipe_epoch", "fhir_epoch"])
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.InterfaceError("connection is closed"),
        TimeoutError("storage timeout"),
        ConnectionError("storage disconnected"),
    ],
)
async def test_closed_storage_cause_remains_retryable(stage, failure):
    connection = _StorageFailureConnection(failure)
    pin = _cms_storage_failure_pin(is_epoch=stage in {"epoch", "recipe_epoch", "fhir_epoch"})
    if stage == "custody":
        operation = fhir_custody.capture_retained_cms_fhir_source_custody(
            connection, pin, owner_role="protected_owner", runtime_roles=("runtime_reader",)
        )
    elif stage == "epoch":
        operation = fhir_epochs.require_retained_cms_fhir_source_epoch(connection, pin, pin.retained_epoch)
    elif stage.startswith("recipe_"):
        operation = source_recipes._require_recipe_custody(connection, SimpleNamespace(source_pin=pin))
    else:
        operation = fhir_sources._require_pinned_source(connection, pin)
    with pytest.raises(ValueError) as caught:
        await operation
    assert consumer._has_database_cause(caught.value)


@pytest.mark.parametrize(
    "failure",
    [ValueError("semantic refusal"), FileNotFoundError("missing source file"), OSError("source file unavailable")],
)
async def test_nonstorage_refusal_never_becomes_retryable(failure):
    outer = ValueError("source refused")
    outer.__cause__ = failure
    assert not consumer._has_database_cause(outer)


class _SleepingComposition:
    def __init__(self, original):
        self.original = original
        self.prepared = None
        self.cancelled = False
        self.drained = False
        self.has_statement_timeout = False

    async def __call__(self, connection, **arguments):
        self.prepared = await self.original(connection, **arguments)
        try:
            await connection.execute("SELECT pg_sleep(30)")
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        except asyncpg.QueryCanceledError:
            self.has_statement_timeout = True
            raise
        finally:
            self.drained = True
        return self.prepared


def _deadline_worker_config(fixture, arguments):
    from process import registry_publication_worker as worker

    url = worker._database_url(_fixture_dsn())
    return worker.RegistryPublicationWorkerConfig(
        url.set(username=fixture.roles_by_kind["lifecycle"], password=fixture.role_password),
        url.set(username=fixture.roles_by_kind["execution"], password=fixture.role_password),
        "https://authority.example.test",
        "synthetic-service-token",
        arguments["config"],
        arguments["initial_address_sources"],
        operation_seconds=3,
    )


@pytest.mark.parametrize("failure", ["operation", "statement"])
async def test_worker_operation_deadline_drains_native_query_and_resumes_exact_request(
    bounded_worker_permissions, monkeypatch, failure
):
    from process import registry_publication_worker as worker

    fixture = bounded_worker_permissions
    namespace = '"' + fixture.fixture.control_schema + '"'
    async with _consumer(fixture) as (arguments, calls):
        first = await _enqueue_ordinary(fixture)
        await consume_registry_publication_once(**arguments)
        assert (await _status(fixture, first))["state"] == "completed"
        head = await _head(fixture)
        queued = await _enqueue_ordinary(fixture, head)
        request_id = UUID(queued["request_id"])
        identity_sql = (
            f"SELECT request_sha256,command_json FROM {namespace}.registry_publication_request WHERE request_id=$1"
        )
        immutable = await fixture.fixture.connection.fetchrow(identity_sql, request_id)
        head_sql = f"SELECT * FROM {namespace}.network_serving_manifest WHERE generation_id=$1"
        original_head = await fixture.fixture.connection.fetchrow(head_sql, head)

        @asynccontextmanager
        async def authority_client(_origin, _token):
            yield arguments["authorize"]

        monkeypatch.setattr(worker, "RegistryPublicationAuthorityClient", authority_client)
        pause = _SleepingComposition(consumer.compose_registry_membership_candidate)
        monkeypatch.setattr(consumer, "compose_registry_membership_candidate", pause)
        config = _timeout_worker_config(fixture, arguments, failure)
        with (
            pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable")
            if failure == "statement"
            else pytest.raises(TimeoutError)
        ):
            await worker.run_registry_publication_worker(config)
        assert pause.prepared is not None and pause.drained
        assert pause.cancelled is (failure == "operation")
        assert pause.has_statement_timeout is (failure == "statement")
        candidate_id, state_sql = await _preserved_worker_timeout_candidate(
            fixture,
            queued,
            pause,
            head=head,
            identity_sql=identity_sql,
            immutable=immutable,
            head_sql=head_sql,
            original_head=original_head,
        )
        monkeypatch.setattr(consumer, "compose_registry_membership_candidate", pause.original)
        await fixture.fixture.connection.execute(
            f"UPDATE {namespace}.registry_publication_request SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            request_id,
        )
        before_calls = len(calls)
        completed = await worker.run_registry_publication_worker(_deadline_worker_config(fixture, arguments))
        assert completed == {"request_id": queued["request_id"], "state": "completed"} and len(calls) > before_calls
        assert await fixture.fixture.connection.fetchrow(identity_sql, request_id) == immutable
        assert await fixture.fixture.connection.fetchval(state_sql, candidate_id) == "published"
        assert (await _status(fixture, queued))["state"] == "completed" and await _head(fixture) > head


@pytest_asyncio.fixture
async def bounded_worker_permissions(worker_permissions):
    """The fixture operator configures a role default without granting parameter SET."""
    fixture = worker_permissions
    database = await fixture.fixture.connection.fetchval("SELECT current_database()")
    role = fixture.roles_by_kind["execution"]
    await fixture.fixture.connection.execute(
        f'''ALTER ROLE "{role}" IN DATABASE "{database}" SET temp_file_limit='256MB' '''
    )
    return fixture


async def test_worker_unlimited_temp_refuses_before_claim_or_authority(worker_permissions, monkeypatch):
    from process import registry_publication_worker as worker

    fixture = worker_permissions
    async with _consumer(fixture) as (arguments, calls):
        queued = await _enqueue_ordinary(fixture)

        @asynccontextmanager
        async def authority_client(_origin, _token):
            yield arguments["authorize"]

        monkeypatch.setattr(worker, "RegistryPublicationAuthorityClient", authority_client)
        with pytest.raises(RuntimeError, match="registry_publication_worker_sql_limits_unavailable"):
            await worker.run_registry_publication_worker(_deadline_worker_config(fixture, arguments))
        assert (await _status(fixture, queued))["state"] == "queued" and calls == []
        assert await _head(fixture) is None


async def test_worker_effective_sql_limits_on_raw_and_genuine_ptg_backend(bounded_worker_permissions, monkeypatch):
    from process import registry_publication_worker as worker

    fixture = bounded_worker_permissions
    async with _consumer(fixture) as (arguments, _calls):
        config = _deadline_worker_config(fixture, arguments)
        config = replace(
            config,
            ptg=worker._PTGWorkerConfig(
                worker.RegistryPTGProducerScopeStore(
                    fixture.roles_by_kind["physical"], fixture.roles_by_kind["queue"], fixture.fixture.control_schema
                ),
                worker.RegistryPTGGraphReadBudget(1024 * 1024),
            ),
        )
        backend_ids = []

        async def inspect(connection):
            await config.sql_limits.verify(connection)
            assert await connection.fetchval(
                "SELECT current_user=session_user AND NOT rolsuper FROM pg_roles WHERE rolname=current_user"
            )
            assert not await connection.fetchval("SELECT has_parameter_privilege(current_user,'temp_file_limit','SET')")
            assert await connection.fetchval("SELECT pg_size_bytes(current_setting('work_mem'))") == 16 * 1024 * 1024
            backend_ids.append(await connection.fetchval("SELECT pg_backend_pid()"))

        async def consume(**options):
            async with options["execution_pool"].acquire() as connection:
                await inspect(connection)
            runtime = options["config"].composition_runtime
            async with runtime.sessions.kw["bind"].connect() as connection:
                raw = await connection.get_raw_connection()
                await inspect(raw.driver_connection)
                await raw.driver_connection.execute("SET work_mem='32MB'")
            with pytest.raises(RuntimeError, match="registry_publication_worker_sql_limits_unavailable"):
                async with runtime.sessions.kw["bind"].connect():
                    raise AssertionError("unsafe reborrow must refuse before application work")
            return None

        monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
        assert await worker.run_registry_publication_worker(config) == dict(state="no_work")
        assert len(backend_ids) == len(set(backend_ids)) == 2


async def _preserved_worker_timeout_candidate(
    fixture, queued, pause, *, head, identity_sql, immutable, head_sql, original_head
):
    """Real SQL timeout retains exact incumbent, command and sealed candidate for retry."""
    request_id = UUID(queued["request_id"])
    namespace = '"' + fixture.fixture.control_schema + '"'
    pending = await _status(fixture, queued)
    assert pending["state"] == "running" and pending["result"] is None and await _head(fixture) == head
    assert await fixture.fixture.connection.fetchrow(identity_sql, request_id) == immutable
    assert await fixture.fixture.connection.fetchrow(head_sql, head) == original_head
    lease = await fixture.fixture.connection.fetchval(
        f"SELECT lease_id FROM {namespace}.registry_publication_request WHERE request_id=$1", request_id
    )
    assert lease is not None
    candidate_id = UUID(pause.prepared[0].candidate_id)
    state_sql = f"SELECT state FROM {namespace}.network_membership_candidate WHERE candidate_id=$1"
    assert await fixture.fixture.connection.fetchval(state_sql, candidate_id) == "sealed"
    assert (
        await fixture.fixture.connection.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND state='active' AND query='SELECT pg_sleep(30)'",
            fixture.roles_by_kind["execution"],
        )
        == 0
    )
    return candidate_id, state_sql


def _timeout_worker_config(fixture, arguments, failure):
    """Exercise native statement expiry separately from the existing operation deadline."""
    config = _deadline_worker_config(fixture, arguments)
    return (
        replace(config, operation_seconds=30, sql_limits=replace(config.sql_limits, statement_timeout_ms=1000))
        if failure == "statement"
        else config
    )


def _ordinary_recipe_sources(*, phase=None):
    pin = _cms_storage_failure_pin(is_epoch=False)
    recipe = RegistrySourceMembershipRecipe(
        pin,
        RegistryNetworkSourceCoordinates("fhir", pin.source_id, pin.schema_name, pin.dataset_id, "producer", "edition"),
    )
    return RegistryCompositionAddressSources(
        PinnedAddressSource("retained_example", "entity_address_unified", "a" * 64),
        PinnedAddressSource("retained_npi", "npi", "b" * 64),
        **({phase: (recipe,)} if phase else {}),
    )


def _ordinary_publication_request(head):
    """Build the closed request used by initial and replacement recipe cases."""
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "idempotency_key": "example-publication",
        "session_token_sha256": "a" * 64,
        "actor_json": {
            "kind": "client_owner",
            "user_id": str(uuid4()),
            "client_id": "example-client",
            "impersonator_id": None,
        },
        "command_json": {
            "expected_draft_revision": 1,
            "expected_approved_revision": 0,
            "expected_serving_generation": head,
            "selection": [{"record_kind": "network", "record_id": 1, "revision": 1}],
            "reason": "Example publication",
        },
    }
    return request_by_name


def _ordinary_recipe_execution(monkeypatch, *, head, address_sources, failure=None):
    request_by_name = _ordinary_publication_request(head)
    config = RegistryPublicationConsumerConfig(
        "registry", "lifecycle", "execution", "api", "queue_owner", "physical_owner"
    )
    source_pin = object()
    transactions = []

    @asynccontextmanager
    async def transaction(**options):
        transactions.append(options)
        yield

    connection = SimpleNamespace(transaction=transaction)
    approval = AsyncMock(return_value={"approved_revision": 1})
    resolve = AsyncMock(return_value=source_pin)
    retained = AsyncMock(
        return_value=replace(address_sources, initial_source_recipes=(), replacement_source_recipes=())
    )
    compose = AsyncMock(return_value=(object(), object()), side_effect=failure)
    complete = AsyncMock(return_value={"state": "completed"})

    async def authorize(**arguments):
        digest = hashlib.sha256(
            json.dumps(arguments["selection"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {"authorized": True, "actor": arguments["actor"], "selection_sha256": digest, "policy_revision": 1}

    for name, implementation in (
        ("approve_queued_registry_publication", approval),
        ("resolve_network_serving_manifest", resolve),
        ("_source_rollback_address_sources", retained),
        ("compose_registry_membership_candidate", compose),
        ("complete_queued_registry_publication", complete),
    ):
        monkeypatch.setattr(consumer, name, implementation)
    return SimpleNamespace(
        request=request_by_name,
        config=config,
        connection=connection,
        authorize=AsyncMock(side_effect=authorize),
        approval=approval,
        resolve=resolve,
        retained=retained,
        compose=compose,
        complete=complete,
        source_pin=source_pin,
        transactions=transactions,
    )


@pytest.mark.parametrize(
    "phase,head", [("initial_source_recipes", 0), ("replacement_source_recipes", 7), (None, 7), (None, 0)]
)
async def test_ordinary_recipe_dispatch_preserves_retained_custody_and_fresh_authority(monkeypatch, phase, head):
    sources = _ordinary_recipe_sources(phase=phase)
    execution = _ordinary_recipe_execution(monkeypatch, head=head, address_sources=sources)
    result = await consumer._ordinary_publication(
        execution.connection, execution.request, execution.authorize, execution.config, sources
    )
    assert result == {"state": "completed"}
    execution.approval.assert_awaited_once()
    assert execution.approval.await_args.kwargs["authorize"] is execution.authorize
    execution.authorize.assert_awaited_once()
    composed = execution.compose.await_args.kwargs
    assert composed["address_sources"] == sources
    assert composed["approved_revision"] == 1 and composed["expected_head"] == head
    if head:
        execution.resolve.assert_awaited_once_with(execution.connection, generation_id=head, control_schema="registry")
        execution.retained.assert_awaited_once_with(execution.connection, execution.source_pin, "registry")
        assert execution.transactions == [{"isolation": "repeatable_read", "readonly": True}]
        assert composed["source_manifest"] is execution.source_pin
        if phase is None:
            assert composed["address_sources"] is execution.retained.return_value
    else:
        execution.retained.assert_not_awaited()
        assert composed["source_manifest"] is None
    execution.complete.assert_awaited_once()


async def test_ordinary_nonzero_head_without_configured_sources_keeps_retained_selection(monkeypatch):
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=_ordinary_recipe_sources())
    await consumer._ordinary_publication(
        execution.connection, execution.request, execution.authorize, execution.config, None
    )
    assert execution.compose.await_args.kwargs["address_sources"] is execution.retained.return_value
    execution.retained.assert_awaited_once()


@pytest.mark.parametrize("phase,head", [("initial_source_recipes", 7), ("replacement_source_recipes", 0)])
async def test_ordinary_recipe_wrong_head_refuses_before_approval(monkeypatch, phase, head):
    sources = _ordinary_recipe_sources(phase=phase)
    execution = _ordinary_recipe_execution(monkeypatch, head=head, address_sources=sources)
    with pytest.raises(ValueError, match="^registry_publication_source_recipe_phase_invalid$"):
        await consumer._ordinary_publication(
            execution.connection, execution.request, execution.authorize, execution.config, sources
        )
    execution.approval.assert_not_awaited()
    execution.compose.assert_not_awaited()


@pytest.mark.parametrize("damage", ["base_generation", "npi", "office_store", "office_budget", "provider_inputs"])
async def test_ordinary_replacement_cannot_change_retained_address_authority(monkeypatch, damage):
    address_sources = _ordinary_recipe_sources(phase="replacement_source_recipes")
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=address_sources)
    changes_by_name = {
        "base_generation": {"base_source": replace(address_sources.base_source, generation_id="c" * 64)},
        "npi": {"npi_source": None},
        "office_store": {"office_scope_store": object()},
        "office_budget": {"office_read_budget": object()},
        "provider_inputs": {
            "em_provider_inputs": provider_inputs.EMProviderInputs(
                tuple(
                    provider_inputs.EMProviderInputRelation(
                        role, "provider_example", "relation_" + str(index), "edition", index + 1, index + 1
                    )
                    for index, role in enumerate(provider_inputs._ROLES)
                )
            )
        },
    }
    with pytest.raises(ValueError, match="^registry_publication_replacement_source_mismatch$"):
        await consumer._ordinary_publication(
            execution.connection,
            execution.request,
            execution.authorize,
            execution.config,
            replace(address_sources, **changes_by_name[damage]),
        )
    execution.retained.assert_awaited_once()
    execution.compose.assert_not_awaited()
    execution.complete.assert_not_awaited()


@pytest.mark.parametrize("failure", ["authority", "selection", "custody"])
async def test_ordinary_replacement_preserves_authority_and_source_refusals(monkeypatch, failure):
    sources = _ordinary_recipe_sources(phase="replacement_source_recipes")
    error = ValueError("registry_office_selection_unselected" if failure == "selection" else "source_custody_changed")
    execution = _ordinary_recipe_execution(monkeypatch, head=7, address_sources=sources, failure=error)
    if failure == "authority":
        execution.authorize = AsyncMock(return_value={"authorized": False})
    with pytest.raises(ValueError) as caught:
        await consumer._ordinary_publication(
            execution.connection, execution.request, execution.authorize, execution.config, sources
        )
    if failure == "authority":
        execution.compose.assert_not_awaited()
    else:
        execution.compose.assert_awaited_once()
        assert caught.value is error
    execution.complete.assert_not_awaited()
