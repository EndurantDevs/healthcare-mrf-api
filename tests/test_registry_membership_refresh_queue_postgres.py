# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine address authority and native queue CAS; App HTTP doubles prove protocol only."""

import importlib.util
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.ext.asyncio import create_async_engine

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_authority import RegistryMembershipRefreshAuthorityClient
from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable
from process.registry_membership_refresh_queue import (
    RegistryMembershipRefreshAddressProfile,
    enqueue_registry_membership_refresh,
    validated_membership_refresh_request_record,
    validated_membership_refresh_request_status,
)
from process.registry_publication_queue import (
    claim_registry_publication_request,
)
from tests.test_network_address_artifact_postgres import address_db as address_db
from tests.test_network_serving_schema_postgres import _SERVING_MIGRATIONS
from tests.test_registry_membership_refresh_authority import receipt
from tests.test_registry_membership_refresh_contract import command, envelope
from tests.test_registry_publication_authority import _reply

pytestmark = pytest.mark.asyncio
_MIGRATIONS = _SERVING_MIGRATIONS


@pytest.fixture
async def refresh_queue_db(address_db, monkeypatch):
    from pathlib import Path

    address_fixture = address_db
    schema = "refresh_queue_" + uuid4().hex
    for name in ("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "HLTHPRT_DB_SCHEMA", "DB_SCHEMA"):
        monkeypatch.setenv(name, schema)
    await address_fixture.admin.execute(f'CREATE SCHEMA "{schema}"')
    engine = create_async_engine(address_fixture.url.set(drivername="postgresql+asyncpg"), pool_size=1, max_overflow=0)
    try:
        async with engine.begin() as connection:
            for filename in _MIGRATIONS:
                path = Path(__file__).parents[1] / "alembic/versions" / filename
                spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
                migration = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(migration)

                def migrate(sync):
                    with Operations.context(MigrationContext.configure(sync)):
                        migration.upgrade()

                await connection.run_sync(migrate)
        await address_fixture.admin.execute(
            f'UPDATE "{schema}".registry_revision_control SET draft_revision=9,approved_revision=5'
        )
        await address_fixture.admin.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{address_fixture.roles["reader"]}"')
        await address_fixture.admin.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{address_fixture.roles["reader"]}"'
        )
        await address_fixture.admin.execute(
            f'GRANT INSERT ON "{schema}".registry_publication_request TO "{address_fixture.roles["reader"]}"'
        )
        yield SimpleNamespace(
            source=address_fixture,
            connection=address_fixture.reader,
            admin=address_fixture.admin,
            schema=schema,
            profile=RegistryMembershipRefreshAddressProfile(address_fixture.schema, (address_fixture.roles["reader"],)),
            engine=engine,
            command=command(
                expected_serving_generation=0,
                expected_serving_manifest_sha256=None,
                address_artifact_sha256=address_fixture.descriptor["descriptor_sha256"],
            ),
        )
    finally:
        await engine.dispose()
        await address_fixture.admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await address_fixture.admin.fetchval("SELECT to_regnamespace($1)", schema) is None


def authorize(handler=None):
    async def granted(request):
        return _reply(receipt(json.loads(request.content)))

    return RegistryMembershipRefreshAuthorityClient(
        "https://authority.example.invalid",
        "synthetic-refresh-only",
        enabled=True,
        transport=httpx.MockTransport(handler or granted),
    )


async def enqueue(fixture, client, **changes):
    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await enqueue_registry_membership_refresh(
            fixture.connection,
            envelope(command={**fixture.command, **changes}),
            authorize=client,
            address_profile=fixture.profile,
            control_schema=fixture.schema,
        )


async def stored(fixture):
    return await fixture.admin.fetchrow(f'SELECT * FROM "{fixture.schema}".registry_publication_request')


async def test_native_first_head_queue_replay_keeps_manual_revisions_and_exact_identity(refresh_queue_db):
    fixture = refresh_queue_db
    calls = []

    async def authority(request):
        calls.append(json.loads(request.content))
        return _reply(receipt(calls[-1]))

    async with authorize(authority) as client:
        first = await enqueue(fixture, client)
        replay = await enqueue(fixture, client)
    assert replay == {**first, "replayed": True} and len(calls) == 2
    request_by_field = await stored(fixture)
    assert request_by_field["session_token_sha256"] is None
    assert json.loads(request_by_field["actor_json"]) == envelope()["actor"]
    reconstructed = validated_membership_refresh_request_record(request_by_field)
    assert reconstructed["command"] == fixture.command
    assert validated_membership_refresh_request_status(request_by_field) == fixture.command
    assert tuple(
        await fixture.admin.fetchrow(
            f'SELECT draft_revision,approved_revision FROM "{fixture.schema}".registry_revision_control'
        )
    ) == (9, 5)
    assert await fixture.admin.fetchval(f'SELECT generation_id FROM "{fixture.schema}".network_serving_control') is None
    async with fixture.admin.transaction():
        assert (
            await claim_registry_publication_request(
                fixture.admin, operation_family="publication", control_schema=fixture.schema
            )
            is None
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"expected_approved_revision": 4},
        {"expected_serving_generation": 1, "expected_serving_manifest_sha256": "b" * 64},
        {"address_artifact_sha256": "f" * 64},
    ],
)
async def test_stale_native_heads_and_address_artifact_refuse_without_queue_mutation(refresh_queue_db, changes):
    fixture = refresh_queue_db
    async with authorize() as client:
        with pytest.raises(RegistryApprovalConflict):
            await enqueue(fixture, client, **changes)
    assert await stored(fixture) is None


async def test_fresh_authority_denial_blocks_replay_and_stored_receipt_is_not_permission(refresh_queue_db):
    fixture = refresh_queue_db
    calls = []

    async def authority(request):
        calls.append(request)
        return _reply(receipt(json.loads(request.content)), status=200 if len(calls) == 1 else 403)

    async with authorize(authority) as client:
        first = await enqueue(fixture, client)
        with pytest.raises(PermissionError):
            await enqueue(fixture, client)
    assert str((await stored(fixture))["request_id"]) == first["request_id"] and len(calls) == 2


async def test_idempotency_conflict_rollback_and_stored_digest_validation(refresh_queue_db):
    fixture = refresh_queue_db
    async with authorize() as client:
        transaction = fixture.connection.transaction(isolation="repeatable_read")
        await transaction.start()
        await enqueue_registry_membership_refresh(
            fixture.connection,
            envelope(command=fixture.command),
            authorize=client,
            address_profile=fixture.profile,
            control_schema=fixture.schema,
        )
        await transaction.rollback()
        assert await stored(fixture) is None
        await enqueue(fixture, client)
        with pytest.raises(RegistryApprovalConflict):
            await enqueue(fixture, client, reason="Other exact request")
    request_by_field = dict(await stored(fixture))
    for changes in ({"session_token_sha256": "a" * 64}, {"request_sha256": "b" * 64}):
        with pytest.raises(ValueError):
            validated_membership_refresh_request_record({**request_by_field, **changes})
    with pytest.raises(ValueError, match="refresh_result_invalid"):
        validated_membership_refresh_request_status({**request_by_field, "state": "completed", "result_json": {}})


async def test_native_null_session_exception_is_only_exact_service_operation(refresh_queue_db):
    import asyncpg

    fixture = refresh_queue_db
    async with authorize() as client:
        await enqueue(fixture, client)
    request_by_field = dict(await stored(fixture))
    original_actor = json.loads(request_by_field["actor_json"])
    human_actor_by_field = {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "system"}
    trials = [
        (original_actor, None, {"operation": "other"}),
        (human_actor_by_field, None, json.loads(request_by_field["command_json"])),
        (human_actor_by_field, None, {}),
        ({**original_actor, "user_id": str(uuid4())}, None, json.loads(request_by_field["command_json"])),
        (original_actor, "a" * 64, json.loads(request_by_field["command_json"])),
        (original_actor, "a" * 64, {"operation": "other"}),
    ]
    for actor, session_hash, document in trials:
        with pytest.raises(asyncpg.CheckViolationError):
            await fixture.admin.execute(
                f'INSERT INTO "{fixture.schema}".registry_publication_request(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7)',
                uuid4(),
                "a" * 64,
                session_hash,
                json.dumps(actor),
                json.dumps(document),
                uuid4().hex,
                "b" * 64,
            )
    await fixture.admin.execute(
        f'INSERT INTO "{fixture.schema}".registry_publication_request(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256) VALUES($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7)',
        uuid4(),
        "a" * 64,
        "c" * 64,
        json.dumps(human_actor_by_field),
        "{}",
        uuid4().hex,
        "b" * 64,
    )


async def test_missing_profile_and_unsafe_native_address_subject_fail_closed(refresh_queue_db):
    fixture = refresh_queue_db
    async with authorize() as client:
        async with fixture.connection.transaction(isolation="repeatable_read"):
            with pytest.raises(RegistryMembershipRefreshUnavailable):
                await enqueue_registry_membership_refresh(
                    fixture.connection,
                    envelope(command=fixture.command),
                    authorize=client,
                    address_profile=None,
                    control_schema=fixture.schema,
                )
        await fixture.admin.execute(
            f'GRANT UPDATE ON "{fixture.source.schema}".entity_address_unified TO "{fixture.source.roles["reader"]}"'
        )
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await enqueue(fixture, client)
    assert await stored(fixture) is None


async def test_opted_in_worker_claims_only_its_exact_refresh_request(refresh_queue_db):
    fixture = refresh_queue_db
    async with authorize() as client:
        first = await enqueue(fixture, client)
        second = await enqueue(fixture, client, idempotency_key="second-refresh")
    async with fixture.admin.transaction():
        assert (
            await claim_registry_publication_request(
                fixture.admin,
                operation_family="publication",
                control_schema=fixture.schema,
            )
            is None
        )
        assert (
            await claim_registry_publication_request(
                fixture.admin,
                operation_family="publication",
                control_schema=fixture.schema,
                membership_refresh_only=True,
                request_id=uuid4(),
            )
            is None
        )
        claimed = await claim_registry_publication_request(
            fixture.admin,
            operation_family="publication",
            control_schema=fixture.schema,
            membership_refresh_only=True,
            request_id=UUID(second["request_id"]),
        )
    assert str(claimed["request_id"]) == second["request_id"]
    assert (
        await fixture.admin.fetchval(
            f'SELECT state FROM "{fixture.schema}".registry_publication_request WHERE request_id=$1',
            UUID(first["request_id"]),
        )
        == "queued"
    )


async def control_head(fixture, origin):
    """Install control metadata only; this fixture does not assert a serving custody seal."""
    candidate_id = uuid4()
    await fixture.admin.execute(
        f"INSERT INTO \"{fixture.schema}\".network_membership_candidate(candidate_id,dataset_id,schema_id,producer_id,schema_name,source_generations,approved_custom_revision,expected_head,expected_rows,state,validation_json,index_ready) VALUES($1,$2,$3,$4,$5,$6::jsonb,5,0,0,'published','{{}}'::jsonb,true)",
        candidate_id,
        uuid4(),
        uuid4(),
        uuid4(),
        "network_candidate_" + candidate_id.hex,
        json.dumps({"network_address_origin": origin}),
    )
    generation_id = await fixture.admin.fetchval(
        f'INSERT INTO "{fixture.schema}".network_serving_manifest(candidate_id,schema_revision,source_generations,approved_custom_revision,manifest_sha256) VALUES($1,1,$2::jsonb,5,$3) RETURNING generation_id',
        candidate_id,
        json.dumps({"network_address_origin": origin}),
        "e" * 64,
    )
    await fixture.admin.execute(
        f'UPDATE "{fixture.schema}".network_serving_control SET generation_id=$1', generation_id
    )
    return {"expected_serving_generation": generation_id, "expected_serving_manifest_sha256": "e" * 64}


async def test_new_current_address_can_differ_from_old_serving_origin(refresh_queue_db):
    fixture = refresh_queue_db
    head = await control_head(fixture, "d" * 64)
    async with authorize() as client:
        assert (await enqueue(fixture, client, **head))["state"] == "queued"


async def test_retained_origin_locator_requires_actual_protected_serving_seal(refresh_queue_db):
    fixture = refresh_queue_db
    head = await control_head(fixture, fixture.command["address_artifact_sha256"])
    async with authorize() as client:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await enqueue(fixture, client, **head)
    assert await stored(fixture) is None
