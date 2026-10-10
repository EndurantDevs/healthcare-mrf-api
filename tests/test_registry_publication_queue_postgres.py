# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Queue retries and controller leases use actual native constraints and transactions."""

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_publication_queue import (
    RegistryReactivationCommand,
    claim_registry_publication_request,
    enqueue_registry_publication,
    enqueue_registry_reactivation,
    enqueue_registry_source_rollback_preview,
    finish_registry_publication_request,
    read_registry_publication_request,
    renew_registry_publication_lease,
    validated_registry_source_rollback_preview_command,
    validated_registry_source_rollback_publication_command,
)
from process.registry_record_store import RegistryActor
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_membership_candidate_indexes_postgres import indexed_db as indexed_db
from tests.test_network_membership_publication_postgres import _published_pair
from tests.test_network_membership_publication_postgres import publication_db as publication_db
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command, _create, _draft

pytestmark = pytest.mark.asyncio


async def _enqueue(connection, schema, command, actor, **changes):
    async with connection.transaction():
        return await enqueue_registry_publication(
            connection,
            command,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=0,
            control_schema=schema,
            **changes,
        )


async def test_enqueue_exact_retry_keeps_original_request_after_draft_changes(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, record)
    first = await _enqueue(connection, schema, command, actor)
    assert first["state"] == "queued" and not first["replayed"]
    await _draft(engine, schema, _create("company"), actor)
    assert await _enqueue(connection, schema, command, actor) == {**first, "replayed": True}
    with pytest.raises(RegistryApprovalConflict, match="idempotency_conflict"):
        await _enqueue(connection, schema, replace(command, reason="Changed reason"), actor)
    with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
        await _enqueue(connection, schema, replace(command, idempotency_key=uuid4().hex), actor)
    detail = await read_registry_publication_request(connection, UUID(first["request_id"]), control_schema=schema)
    assert detail["selection"] == list(command.selection) and detail["result"] is None
    assert "session_token_sha256" not in detail and "command_json" not in detail
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_exact_lease_renewal_expiry_reclaim_and_terminal_fence(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    draft_record = await _draft(engine, schema, _create(), actor)
    first = await _enqueue(connection, schema, await _command(connection, schema, draft_record), actor)
    async with connection.transaction():
        lease = await claim_registry_publication_request(connection, control_schema=schema)
    assert str(lease["request_id"]) == first["request_id"]
    async with connection.transaction():
        assert await claim_registry_publication_request(connection, control_schema=schema) is None
        assert not await renew_registry_publication_lease(
            connection, lease["request_id"], uuid4(), control_schema=schema
        )
        assert await renew_registry_publication_lease(
            connection, lease["request_id"], lease["lease_id"], control_schema=schema
        )
    await connection.execute(
        f"UPDATE \"{schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second'"
    )
    async with connection.transaction():
        assert not await renew_registry_publication_lease(
            connection, lease["request_id"], lease["lease_id"], control_schema=schema
        )
        reclaimed = await claim_registry_publication_request(connection, control_schema=schema)
    assert reclaimed["lease_id"] != lease["lease_id"]
    rejected_by_name = {"error": {"code": "registry_publication_rejected"}}
    async with connection.transaction():
        with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
            async with connection.transaction():
                await finish_registry_publication_request(
                    connection,
                    lease["request_id"],
                    lease["lease_id"],
                    state="rejected",
                    result=rejected_by_name,
                    control_schema=schema,
                )
        await finish_registry_publication_request(
            connection,
            reclaimed["request_id"],
            reclaimed["lease_id"],
            state="rejected",
            result=rejected_by_name,
            control_schema=schema,
        )
    detail = await read_registry_publication_request(connection, lease["request_id"], control_schema=schema)
    assert detail["state"] == "rejected" and detail["result"] == rejected_by_name
    async with connection.transaction():
        assert await claim_registry_publication_request(connection, control_schema=schema) is None


async def test_caller_rollback_removes_only_its_queue_request(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, record)
    transaction = connection.transaction()
    await transaction.start()
    first = await enqueue_registry_publication(
        connection, command, actor, session_token_sha256="a" * 64, expected_serving_generation=0, control_schema=schema
    )
    await transaction.rollback()
    assert await read_registry_publication_request(connection, UUID(first["request_id"]), control_schema=schema) is None
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1


async def _enqueue_reactivation(fixture, command, actor):
    async with fixture.connection.transaction():
        return await enqueue_registry_reactivation(
            fixture.connection, command, actor, session_token_sha256="a" * 64, control_schema=fixture.control_schema
        )


async def test_reactivation_queue_union_has_exact_audit_and_conflicting_retry(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    command = RegistryReactivationCommand(
        original["generation_id"], current["generation_id"], 0, "Reviewed rollback", uuid4().hex
    )
    actor = RegistryActor("platform_admin", uuid4(), "system")
    first = await _enqueue_reactivation(fixture, command, actor)
    assert await _enqueue_reactivation(fixture, command, actor) == {**first, "replayed": True}
    detail = await read_registry_publication_request(
        fixture.connection, UUID(first["request_id"]), control_schema=fixture.control_schema
    )
    assert set(detail) == {
        "request_id",
        "state",
        "actor",
        "result",
        "created_at",
        "updated_at",
        "operation",
        "command_sha256",
    }
    assert detail["operation"] == "reactivate_generation" and len(detail["command_sha256"]) == 64
    for field, replacement in (("target_generation", current["generation_id"]), ("reason", "Different reason")):
        with pytest.raises(RegistryApprovalConflict, match="idempotency_conflict"):
            await _enqueue_reactivation(fixture, replace(command, **{field: replacement}), actor)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_revision_control SET draft_revision=1,approved_revision=1'
    )
    assert await _enqueue_reactivation(fixture, command, actor) == {**first, "replayed": True}
    with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
        await _enqueue_reactivation(fixture, replace(command, idempotency_key=uuid4().hex), actor)


@pytest.mark.parametrize(
    "change",
    ["owner", "client", "impersonator", "generation_bool", "zero", "overflow", "reason_control", "reason_bytes"],
)
async def test_reactivation_queue_rejects_wrong_actor_and_malformed_command(publication_db, change):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    command = RegistryReactivationCommand(
        original["generation_id"], current["generation_id"], 0, "Reviewed rollback", uuid4().hex
    )
    actor = RegistryActor("platform_admin", uuid4(), "system")
    actor_changes_by_case = {
        "owner": {"kind": "client_owner"},
        "client": {"client_id": "client"},
        "impersonator": {"impersonator_id": uuid4()},
    }
    command_changes_by_case = {
        "generation_bool": {"target_generation": True},
        "zero": {"expected_serving_generation": 0},
        "overflow": {"expected_approved_revision": 2**63},
        "reason_control": {"reason": "Review\x01reason"},
        "reason_bytes": {"reason": "é" * 257},
    }
    if change in actor_changes_by_case:
        actor = replace(actor, **actor_changes_by_case[change])
    else:
        command = replace(command, **command_changes_by_case[change])
    with pytest.raises(ValueError):
        await _enqueue_reactivation(fixture, command, actor)
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.control_schema}".registry_publication_request'
        )
        == 0
    )


@pytest.mark.parametrize("change", ["extra", "operation", "bool", "zero", "overflow", "padded", "bytes", "key"])
async def test_source_rollback_command_is_exact_and_never_normalizes(change):
    command_by_name = {
        "operation": "prepare_source_rollback",
        "target_generation": 1,
        "expected_serving_generation": 2,
        "expected_approved_revision": 0,
        "reason": "Review source edition",
        "idempotency_key": "retry-key",
    }
    fresh = validated_registry_source_rollback_preview_command(command_by_name)
    assert fresh == command_by_name and fresh is not command_by_name
    changes_by_case = {
        "extra": {"actor": {}},
        "operation": {"operation": "reactivate_generation"},
        "bool": {"target_generation": True},
        "zero": {"expected_serving_generation": 0},
        "overflow": {"expected_approved_revision": 2**63},
        "padded": {"reason": " padded "},
        "bytes": {"reason": "é" * 257},
        "key": {"idempotency_key": "bad\x00key"},
    }
    with pytest.raises(ValueError):
        validated_registry_source_rollback_preview_command({**command_by_name, **changes_by_case[change]})


async def test_source_rollback_queue_distinct_status_and_cross_operation_key_conflict(publication_db):
    fixture = publication_db
    original, current = await _published_pair(fixture)
    actor = RegistryActor("platform_admin", uuid4(), "system")
    command_by_name = {
        "operation": "prepare_source_rollback",
        "target_generation": original["generation_id"],
        "expected_serving_generation": current["generation_id"],
        "expected_approved_revision": 0,
        "reason": "Review source edition",
        "idempotency_key": uuid4().hex,
    }
    async with fixture.connection.transaction():
        queued = await enqueue_registry_source_rollback_preview(
            fixture.connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.control_schema,
        )
    status = await read_registry_publication_request(
        fixture.connection, UUID(queued["request_id"]), control_schema=fixture.control_schema
    )
    assert status["operation"] == "prepare_source_rollback" and "selection" not in status
    assert set(status) == {
        "request_id",
        "state",
        "actor",
        "result",
        "created_at",
        "updated_at",
        "operation",
        "command_sha256",
    }
    reactivation = RegistryReactivationCommand(
        **{key: field_value for key, field_value in command_by_name.items() if key != "operation"}
    )
    with pytest.raises(RegistryApprovalConflict, match="idempotency_conflict"):
        async with fixture.connection.transaction():
            await enqueue_registry_reactivation(
                fixture.connection,
                reactivation,
                actor,
                session_token_sha256="a" * 64,
                control_schema=fixture.control_schema,
            )


@pytest.mark.parametrize("change", ["extra", "operation", "uuid", "digest", "bool", "zero", "padded", "key"])
async def test_source_publication_command_is_exact_and_bound_to_review(change):
    command_by_name = {
        "operation": "publish_source_rollback",
        "preview_request_id": str(uuid4()),
        "candidate_sha256": "a" * 64,
        "expected_serving_generation": 2,
        "expected_approved_revision": 0,
        "reason": "Reviewed source publication",
        "idempotency_key": "exact-retry",
    }
    fresh = validated_registry_source_rollback_publication_command(command_by_name)
    assert fresh == command_by_name and fresh is not command_by_name
    changes_by_case = {
        "extra": {"candidate_id": str(uuid4())},
        "operation": {"operation": "prepare_source_rollback"},
        "uuid": {"preview_request_id": "00000000-0000-0000-0000-000000000000"},
        "digest": {"candidate_sha256": "A" * 64},
        "bool": {"expected_approved_revision": True},
        "zero": {"expected_serving_generation": 0},
        "padded": {"reason": " padded "},
        "key": {"idempotency_key": "bad\x00key"},
    }
    with pytest.raises(ValueError, match="source_rollback_publication_command_invalid"):
        validated_registry_source_rollback_publication_command({**command_by_name, **changes_by_case[change]})
