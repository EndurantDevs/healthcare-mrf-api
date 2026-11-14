# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real bootstrap, direct-login queue consumer and named administrative preparation."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_bootstrap_sources as bootstrap
from process import registry_publication_execution as execution
from process.network_membership_publication import publish_network_candidate
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_network_catalog_read import (
    RegistryNetworkCatalogError,
    RegistryNetworkCatalogQuery,
    read_registry_network_catalog,
)
from process.registry_publication_consumer import consume_registry_publication_once
from process.registry_publication_queue import (
    enqueue_registry_initial_inventory,
    validated_registry_initial_inventory_command,
)
from process.registry_record_store import RegistryActor
from tests.test_network_custom_address_source_postgres import _bootstrap_prepare, _bootstrap_specification, _draft
from tests.test_network_custom_address_source_postgres import bootstrap_db as bootstrap_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _create
from tests.test_registry_publication_consumer_postgres import (
    _consumer,
    _head,
    _retained_recipe_custodian,
    _status,
)
from tests.test_registry_publication_worker_permissions_postgres import _login, _worker_roles

pytestmark = pytest.mark.asyncio


def _command(receipt=None, **changes):
    return {
        "operation": "publish_initial_inventory",
        "bootstrap_id": receipt.specification.bootstrap_id if receipt else "22222222-2222-4222-8222-222222222222",
        "bootstrap_generation_sha256": receipt.generation_sha256 if receipt else "b" * 64,
        "expected_serving_generation": 0,
        "expected_approved_revision": 0,
        "reason": "Publish initial inventory",
        "idempotency_key": uuid4().hex,
        **changes,
    }


@pytest.fixture
async def initial_worker(bootstrap_db, serving_schema, request):
    fixture = bootstrap_db
    fixture.control_schema = serving_schema[1]
    fixture.base = None
    fixture.composition_ids = []
    actor = RegistryActor("platform_admin", uuid4(), "system")
    await _draft(fixture, _create(), actor)
    async with _worker_roles(fixture, SimpleNamespace(actor=actor)) as worker:
        runtime_roles = (
            (fixture.roles["reader"],)
            if getattr(request, "param", None) == "reader_only"
            else tuple(sorted((fixture.roles["loader"], fixture.roles["reader"])))
        )
        receipt = await _bootstrap_prepare(
            fixture, await _bootstrap_specification(fixture), runtime_roles=runtime_roles
        )
        fixture.base = receipt.address_source
        worker.bootstrap = receipt
        worker.bootstrap_reader_roles = runtime_roles
        async with _retained_recipe_custodian(worker, runtime_roles) as operator:
            worker.preparation_operator = operator
            yield worker


async def _enqueue(worker, command):
    async with _login(worker, "api") as connection, connection.transaction():
        return await enqueue_registry_initial_inventory(
            connection,
            command,
            worker.seed.actor,
            session_token_sha256="a" * 64,
            control_schema=worker.fixture.control_schema,
        )


async def _snapshot(worker):
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    relations = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname=$1 AND "
        "(tablename LIKE 'registry_%' OR tablename LIKE 'network_registry_%' OR tablename LIKE 'manual_%') "
        "AND tablename<>'registry_publication_request' ORDER BY tablename",
        schema,
    )
    return {
        row["tablename"]: await connection.fetchval(
            f"SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text "
            f'FROM "{schema}"."{row["tablename"]}" t'
        )
        for row in relations
    }


@asynccontextmanager
async def _custodian(worker, arguments):
    """Reuse the configured application callback and genuine separate administrator freeze."""
    operator = worker.preparation_operator

    async def validate(target, *, pre_enrolled_only=False):
        return await operator.seal_candidate(target)

    config = replace(
        arguments["config"],
        seal_candidate=validate,
        preparation_roles=worker.fixture.preparation_roles,
    )
    yield {**arguments, "config": config}, operator.owner_role


def _sources(worker):
    return RegistryCompositionAddressSources(worker.bootstrap.address_source, worker.bootstrap.npi_source)


async def test_initial_inventory_consumes_genuine_bootstrap_and_custody_without_approval(initial_worker, monkeypatch):
    """Publish real empty inventory while preserving every unrelated draft and history row."""
    worker = initial_worker
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    command = _command(worker.bootstrap)
    before = await _snapshot(worker)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        with pytest.raises(NetworkServingReadUnavailable):
            await resolve_network_serving_manifest(connection, control_schema=schema)
        with pytest.raises(RegistryNetworkCatalogError, match="unavailable"):
            await read_registry_network_catalog(
                connection, RegistryNetworkCatalogQuery(), excluded_network_ids=(), control_schema=schema
            )
    queued = await _enqueue(worker, command)
    assert (await _status(worker, queued))["result"] is None
    scans = []
    full_verify = bootstrap.verify_network_bootstrap_sources

    async def verify(connection, receipt):
        assert await connection.fetchval("SHOW transaction_read_only") == "on"
        assert await connection.fetchval("SHOW transaction_isolation") == "repeatable read"
        scans.append(receipt)
        return await full_verify(connection, receipt)

    monkeypatch.setattr(bootstrap, "verify_network_bootstrap_sources", verify)
    async with _consumer(worker) as (arguments, calls), _custodian(worker, arguments) as (arguments, custodian):
        assert set(worker.bootstrap_reader_roles) > set(arguments["config"].reader_roles)
        arguments["initial_address_sources"] = _sources(worker)
        completed = await consume_registry_publication_once(**arguments)
        result_by_name = completed["result"]
        assert completed["state"] == "completed" and scans == [worker.bootstrap]
        assert (
            result_by_name["previous_generation_id"]
            == result_by_name["approved_custom_revision"]
            == result_by_name["membership_rows"]
            == 0
        )
        assert result_by_name["generation_id"] == await _head(worker) and result_by_name["policy_revision"] == len(
            calls
        )
        assert all(call["command"] == command for call in calls)
        status = await _status(worker, queued)
        assert (
            status["result"] == result_by_name
            and status["command_sha256"]
            == hashlib.sha256(json.dumps(command, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        )
        assert await _enqueue(worker, command) == {**queued, "state": "completed", "replayed": True}
        with pytest.raises(RegistryApprovalConflict, match="idempotency_conflict"):
            await _enqueue(worker, {**command, "reason": "Changed reason"})
        with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
            await _enqueue(worker, {**command, "idempotency_key": uuid4().hex})
        assert await consume_registry_publication_once(**arguments) is None
        await _assert_initial_manifest(worker, result_by_name, custodian)
    assert await _snapshot(worker) == before


async def test_initial_inventory_accepts_legacy_sequence_owner_as_reader(initial_worker):
    """Publish ordinary inventory when an actual reader also owns the mutable parent's sequence."""
    worker = initial_worker
    connection, loader = worker.fixture.connection, worker.fixture.roles["loader"]
    sequence = await connection.fetchval(
        "SELECT pg_get_serial_sequence($1,'npi')", worker.fixture.source_schema + ".npi"
    )
    sequence_before = await connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")
    inventory_before = await _snapshot(worker)
    await _enqueue(worker, _command(worker.bootstrap))
    async with _consumer(worker) as (arguments, _calls):
        arguments["config"] = replace(
            arguments["config"], reader_roles=tuple(sorted((*arguments["config"].reader_roles, loader)))
        )
        async with _custodian(worker, arguments) as (arguments, custodian):
            arguments["initial_address_sources"] = _sources(worker)
            completed = await consume_registry_publication_once(**arguments)
            assert completed["state"] == "completed"
            assert completed["result"]["generation_id"] == await _head(worker)
            await _assert_initial_manifest(worker, completed["result"], custodian)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        assert await bootstrap.verify_network_bootstrap_sources(connection, worker.bootstrap) == worker.bootstrap
    assert await connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == sequence_before
    assert await _snapshot(worker) == inventory_before


async def _assert_initial_manifest(worker, result_by_name, custodian):
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        manifest = await resolve_network_serving_manifest(connection, control_schema=schema)
        candidate_by_name = await connection.fetchrow(
            f'SELECT * FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(result_by_name["candidate_id"]),
        )
        validation = json.loads(candidate_by_name["validation_json"])["final_validation"]
        assert candidate_by_name["state"] == "published" and validation["complete"] is True
        assert validation["candidate_id"] == result_by_name["candidate_id"]
        assert validation["schema_name"] == manifest.schema_name
        catalog_by_name = await read_registry_network_catalog(
            connection,
            RegistryNetworkCatalogQuery(),
            excluded_network_ids=(),
            control_schema=schema,
        )
        assert catalog_by_name["items"] == () and catalog_by_name["total"] == 0
        assert catalog_by_name["generation"] == str(manifest.generation_id)
        assert catalog_by_name["approved_custom_revision"] == "0"
    assert manifest.manifest_sha256 == result_by_name["manifest_sha256"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{manifest.schema_name}".network_membership') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{manifest.schema_name}".entity_address_unified') == 1
    await _assert_initial_reader_permissions(worker, manifest.schema_name)


async def _assert_initial_reader_permissions(worker, schema):
    """Exercise native SELECT and write refusal against the published qualified relations."""
    connection = worker.fixture.connection
    async with connection.transaction(readonly=True):
        await connection.execute(f'SET LOCAL ROLE "{worker.fixture.roles["reader"]}"')
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".entity_address_unified') == 1
    for role in sorted(set(worker.roles_by_kind.values()) | set(worker.fixture.roles.values())):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{role}"')
                await connection.execute(f'DELETE FROM "{schema}".entity_address_unified')
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction(readonly=True):
            await connection.execute(f'SET LOCAL ROLE "{worker.fixture.roles["publisher"]}"')
            await connection.fetchval(f'SELECT count(*) FROM "{schema}".entity_address_unified')


@pytest.mark.parametrize(
    "change",
    [
        {"expected_serving_generation": True},
        {"expected_approved_revision": True},
        {"expected_serving_generation": 1},
        {"expected_approved_revision": 1},
        {"bootstrap_id": str(UUID(int=0))},
        {"bootstrap_id": "22222222222242228222222222222222"},
        {"bootstrap_generation_sha256": "B" * 64},
        {"bootstrap_generation_sha256": True},
        {"selection": []},
        {"reason": " padded "},
        {"reason": "é" * 257},
        {"reason": "a\nb"},
    ],
)
async def test_initial_command_rejects_noncanonical_or_approval_fields(change):
    with pytest.raises(ValueError, match="initial_inventory_command_invalid"):
        validated_registry_initial_inventory_command(_command(**change))


async def _inject_final_refusal(worker, copy_target, refusal, seal, competing_heads, injected, *, pre_enrolled_only):
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    injected.append(refusal)
    assert pre_enrolled_only is True
    if refusal == "custody_absent":
        return
    await seal(copy_target, pre_enrolled_only=pre_enrolled_only)
    if refusal == "lease":
        await connection.execute(
            f"UPDATE \"{schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second'"
        )
    if refusal == "revision":
        await connection.execute(f'UPDATE "{schema}".registry_revision_control SET approved_revision=1')
    if refusal == "map":
        # Model damaged metadata; the ordinary CHECK already forbids revision-zero rows.
        await connection.execute(
            f'ALTER TABLE "{schema}".registry_approved_record DROP CONSTRAINT registry_approved_revision'
        )
        await connection.execute(
            f'INSERT INTO "{schema}".registry_approved_record '
            "(approved_revision,record_kind,record_key,record_revision,custom_revision,record_json) "
            "VALUES(0,'company','999',1,1,'{}'::jsonb)"
        )
    if refusal == "head":
        async with connection.transaction(isolation="repeatable_read"):
            competing_heads.append(
                await publish_network_candidate(
                    connection, copy_target, control_schema=schema, preparation_roles=worker.fixture.preparation_roles
                )
            )
    if refusal == "candidate_rows":
        await connection.execute(
            f'UPDATE "{schema}".network_membership_candidate SET expected_rows=1,accepted_rows=1 WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
    if refusal == "candidate_recipe":
        await connection.execute(
            f'UPDATE "{schema}".network_membership_candidate SET source_recipes_json=$1::jsonb WHERE candidate_id=$2',
            "[{}]",
            UUID(copy_target.candidate_id),
        )
    if refusal == "command":
        await connection.execute(
            f'UPDATE "{schema}".registry_publication_request SET command_json=command_json||\'{{"reason":"Changed"}}\'::jsonb'
        )


@pytest.mark.parametrize(
    "refusal",
    [
        "custody_missing",
        "custody_absent",
        "source",
        "ancillary",
        "revoked",
        "lease",
        "revision",
        "map",
        "head",
        "bootstrap_digest",
        "bootstrap_content",
        "candidate_rows",
        "candidate_recipe",
        "command",
    ],
)
async def test_initial_inventory_refusal_preserves_inventory(initial_worker, refusal):
    """Refuse changed authority, sources or final evidence without changing serving inventory."""
    worker = initial_worker
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    command_by_name = _command(worker.bootstrap)
    if refusal == "bootstrap_digest":
        command_by_name["bootstrap_generation_sha256"] = "0" * 64
    queued = await _enqueue(worker, command_by_name)
    before = await _snapshot(worker)
    competing_heads, injected = [], []

    async def authority_hook(payload, call):
        return 403 if refusal == "revoked" and injected else None

    async with (
        _consumer(worker, authority_hook=authority_hook) as (arguments, calls),
        _custodian(worker, arguments) as (arguments, _role),
    ):
        arguments["initial_address_sources"] = _sources(worker)
        if refusal == "custody_missing":
            arguments["config"] = replace(arguments["config"], seal_candidate=None)
        if refusal == "source":
            arguments["initial_address_sources"] = RegistryCompositionAddressSources(worker.bootstrap.address_source)
        if refusal == "ancillary":
            arguments["initial_address_sources"] = replace(_sources(worker), office_read_budget=object())
        if refusal == "bootstrap_content":
            await connection.execute(
                f"UPDATE \"{worker.bootstrap.address_source.schema_name}\".entity_address_unified SET first_line='Changed'"
            )
        if refusal not in {"custody_missing", "source", "ancillary", "bootstrap_content"}:
            seal = arguments["config"].seal_candidate
            arguments["config"] = replace(
                arguments["config"],
                seal_candidate=lambda target, **options: _inject_final_refusal(
                    worker,
                    target,
                    refusal,
                    seal,
                    competing_heads,
                    injected,
                    **options,
                ),
            )
        error = RegistryApprovalConflict if refusal == "lease" else ValueError
        with pytest.raises(error, match="lease_conflict" if refusal == "lease" else "request_refused"):
            await consume_registry_publication_once(**arguments)
        assert await _head(worker) == (competing_heads[0]["generation_id"] if competing_heads else None)
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_serving_manifest') == len(
            competing_heads
        )
        if refusal == "head":
            assert len(competing_heads) == 1
        await _assert_initial_refusal_state(worker, refusal, before, queued, injected)


async def _assert_initial_refusal_state(worker, refusal, before, queued, injected):
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    if refusal in {
        "custody_absent",
        "revoked",
        "lease",
        "revision",
        "map",
        "head",
        "candidate_rows",
        "candidate_recipe",
        "command",
    }:
        assert injected == [refusal]
    if refusal == "revision":
        await connection.execute(f'UPDATE "{schema}".registry_revision_control SET approved_revision=0')
    elif refusal == "map":
        await connection.execute(f'DELETE FROM "{schema}".registry_approved_record WHERE approved_revision=0')
    assert await _snapshot(worker) == before
    if refusal != "command":
        assert (await _status(worker, queued))["result"] in (None, {"error": {"code": "registry_publication_rejected"}})


@pytest.mark.parametrize("boundary", ["authority", "completion"])
async def test_initial_inventory_resumes_sealed_candidate_after_transient_failure(
    initial_worker, monkeypatch, boundary
):
    worker = initial_worker
    connection, schema = worker.fixture.connection, worker.fixture.control_schema
    queued = await _enqueue(worker, _command(worker.bootstrap))
    before = await _snapshot(worker)
    completion_calls = []
    finish = execution.finish_registry_publication_request

    async def fail_completion(*arguments, **options):
        await finish(*arguments, **options)
        completion_calls.append(options["state"])
        if boundary == "completion" and len(completion_calls) == 1:
            raise ConnectionError("Completion transaction interrupted")

    monkeypatch.setattr(execution, "finish_registry_publication_request", fail_completion)
    authority_failures = []

    async def authority_hook(payload, call):
        if boundary == "authority" and not authority_failures:
            sealed = await connection.fetchval(
                f'SELECT EXISTS(SELECT FROM "{schema}".network_membership_candidate '
                "WHERE validation_json->'final_validation' IS NOT NULL)"
            )
            if sealed:
                authority_failures.append(call)
                return 503

    async with (
        _consumer(worker, authority_hook=authority_hook) as (arguments, calls),
        _custodian(worker, arguments) as (arguments, custodian),
    ):
        arguments["initial_address_sources"] = _sources(worker)
        with pytest.raises(RuntimeError, match="consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        assert (await _status(worker, queued))["state"] == "running" and await _head(worker) is None
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_serving_manifest') == 0
        candidate_id = await connection.fetchval(
            f'SELECT candidate_id FROM "{schema}".network_membership_candidate '
            "WHERE validation_json->'final_validation' IS NOT NULL"
        )
        assert candidate_id is not None
        await connection.execute(
            f"UPDATE \"{schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second'"
        )
        completed = await consume_registry_publication_once(**arguments)
        assert completed["result"]["candidate_id"] == str(candidate_id)
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_membership_candidate') == 1
        assert (
            await connection.fetchval(
                f'SELECT count(*) FROM "{schema}".network_membership_candidate '
                "WHERE validation_json->'final_validation' IS NOT NULL"
            )
            == 1
        )
        await _assert_initial_manifest(worker, completed["result"], custodian)
    assert await _snapshot(worker) == before


async def test_initial_inventory_queue_refuses_nonadministrator_provenance(initial_worker):
    worker = initial_worker
    before = await _snapshot(worker)
    for actor in (
        RegistryActor("client_owner", uuid4(), "system"),
        RegistryActor("platform_admin", uuid4(), "client_example"),
        RegistryActor("platform_admin", uuid4(), "system", uuid4()),
    ):
        async with _login(worker, "api") as connection, connection.transaction():
            with pytest.raises(ValueError, match="actor_invalid"):
                await enqueue_registry_initial_inventory(
                    connection,
                    _command(worker.bootstrap),
                    actor,
                    session_token_sha256="a" * 64,
                    control_schema=worker.fixture.control_schema,
                )
    assert await _snapshot(worker) == before


async def test_initial_inventory_lost_completion_response_replays_durable_result(initial_worker, monkeypatch):
    worker = initial_worker
    command_by_name = _command(worker.bootstrap)
    queued = await _enqueue(worker, command_by_name)
    before = await _snapshot(worker)
    finish = execution._finish_initial_inventory

    async def lose_response(*arguments, **options):
        await finish(*arguments, **options)
        raise ConnectionError("Completion response lost")

    monkeypatch.setattr(execution, "_finish_initial_inventory", lose_response)
    async with _consumer(worker) as (arguments, calls), _custodian(worker, arguments) as (arguments, custodian):
        arguments["initial_address_sources"] = _sources(worker)
        with pytest.raises(RuntimeError, match="consumer_unavailable"):
            await consume_registry_publication_once(**arguments)
        status_by_name = await _status(worker, queued)
        assert status_by_name["state"] == "completed"
        assert await _enqueue(worker, command_by_name) == {**queued, "state": "completed", "replayed": True}
        assert await consume_registry_publication_once(**arguments) is None
        await _assert_initial_manifest(worker, status_by_name["result"], custodian)
    assert await _snapshot(worker) == before


@pytest.mark.parametrize("initial_worker", ["reader_only"], indirect=True)
async def test_initial_inventory_uses_private_bootstrap_access(initial_worker):
    from process import network_bootstrap_read_access as access

    worker = initial_worker
    connection, executor = worker.fixture.connection, worker.roles_by_kind["execution"]
    await connection.execute(f'REVOKE "{worker.fixture.roles["reader"]}" FROM "{executor}"')
    await _enqueue(worker, _command(worker.bootstrap))
    async with _consumer(worker) as (arguments, _calls), _custodian(worker, arguments) as (arguments, _custody):
        async with connection.transaction(isolation="repeatable_read"):
            await access.install_bootstrap_read_access(connection, worker.bootstrap, reader_role=executor)
        arguments["initial_address_sources"] = _sources(worker)
        async with arguments["execution_pool"].acquire() as executing:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with executing.transaction(readonly=True):
                    await executing.fetchval(f'SELECT count(*) FROM "{worker.fixture.source_schema}".npi')
        completed = await consume_registry_publication_once(**arguments)
        assert completed["state"] == "completed"
        assert completed["result"]["membership_rows"] == 0
