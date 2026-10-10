# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real approval, closed preparation and atomic lease-fenced publication."""

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest

from process import registry_publication_execution as execution
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_custom_address_source import (
    NetworkCustomAddressSourceError,
    verify_custom_address_source,
    verify_retained_custom_address_source,
)
from process.network_custom_address_source import (
    _from_document as _custom_address_receipt,
)
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_company_approval_fence import _fence_key
from process.registry_publication_queue import (
    RegistryReactivationCommand,
    claim_registry_publication_request,
    enqueue_registry_publication,
    enqueue_registry_reactivation,
    enqueue_registry_source_rollback_preview,
    enqueue_registry_source_rollback_publication,
    read_registry_publication_request,
)
from process.registry_record_store import RegistryActor
from tests.test_network_address_projection_postgres import projection_db as projection_db
from tests.test_network_custom_address_source_postgres import _draft as _custom_draft
from tests.test_network_membership_candidate_indexes_postgres import indexed_db as indexed_db
from tests.test_network_membership_pipeline_postgres import _arguments
from tests.test_network_membership_pipeline_postgres import pipeline_db as pipeline_db
from tests.test_network_membership_publication_postgres import _published_pair
from tests.test_network_membership_publication_postgres import publication_db as publication_db
from tests.test_network_membership_validation_postgres import validation_db as validation_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command, _create, _draft
from tests.test_registry_approval_store_postgres import _approve as _approve_records
from tests.test_registry_candidate_composition_postgres import (
    _approve_rollback_manual_office,
    _assert_rollback_source_and_manual,
    _initial_arguments,
    _publish_composition_target,
    _remove_candidates,
    _roles,
    _rollback_members,
    _rollback_reader,
)
from tests.test_registry_candidate_composition_postgres import initial_composition as initial_composition
from tests.test_registry_candidate_composition_postgres import office_db as office_db
from tests.test_registry_candidate_composition_postgres import replacement_aca_source as replacement_aca_source
from tests.test_registry_candidate_composition_postgres import rollback_composition as rollback_composition

pytestmark = pytest.mark.asyncio


def _authority(calls, *, denied=False, policy_revision=5):
    async def authorize(**request_by_name):
        calls.append(request_by_name)
        if denied:
            raise PermissionError("registry_publication_denied")
        digest = hashlib.sha256(
            json.dumps(request_by_name["selection"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "authorized": True,
            "actor": request_by_name["actor"],
            "selection_sha256": digest,
            "policy_revision": policy_revision,
        }

    return authorize


async def _claim(fixture, engine):
    actor = _actor()
    draft_record = await _draft(engine, fixture.control_schema, _create(), actor)
    command = await _command(fixture.connection, fixture.control_schema, draft_record)
    async with fixture.connection.transaction():
        queued = await enqueue_registry_publication(
            fixture.connection,
            command,
            actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=0,
            control_schema=fixture.control_schema,
        )
        lease = await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema)
    assert str(lease["request_id"]) == queued["request_id"]
    return lease


async def _approve(fixture, lease, calls):
    approved = await execution.approve_queued_registry_publication(
        fixture.connection,
        lease["request_id"],
        lease["lease_id"],
        authorize=_authority(calls),
        control_schema=fixture.control_schema,
    )
    # This sealed fixture represents a candidate created after the explicit approval.
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_membership_candidate SET approved_custom_revision=$1',
        approved["approved_revision"],
    )
    return approved


async def _complete(fixture, lease, authorize):
    writer_roles = _arguments(fixture)
    writer_roles.pop("control_schema")
    await fixture.connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        return await execution.complete_queued_registry_publication(
            fixture.connection,
            lease["request_id"],
            lease["lease_id"],
            fixture.copy_target,
            fixture.address_source,
            authorize=authorize,
            writer_roles=writer_roles,
            control_schema=fixture.control_schema,
        )
    finally:
        await fixture.connection.execute("RESET ROLE")


async def _heads(fixture):
    return await fixture.observer.fetchrow(
        f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
    )


async def test_fresh_authority_and_atomic_manifest_status_receipt(pipeline_db, serving_schema):
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    calls = []
    approved = await _approve(fixture, lease, calls)
    assert (await _heads(fixture))["generation_id"] is None
    policy_revisions = iter((6, 7, 8))

    async def changed_policy_revision(**request_by_name):
        """Return the fresh policy revision from each completion authority read."""
        return await _authority(calls, policy_revision=next(policy_revisions))(**request_by_name)

    receipt = await _complete(fixture, lease, changed_policy_revision)
    assert len(calls) == 6 and all(request_by_name == calls[0] for request_by_name in calls)
    assert receipt == {
        field: approved[field] for field in ("approved_revision", "previous_approved_revision", "selected_count")
    } | {"serving_generation": (await _heads(fixture))["generation_id"], "policy_revision": 8}
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "completed" and status["result"] == receipt


async def _wait_for_request_lock(fixture, pending):
    """Prove the worker is blocked by the second native connection's row lock."""
    async with asyncio.timeout(5):
        while True:
            blockers = await fixture.observer.fetchval(
                "SELECT pg_blocking_pids($1)", fixture.connection.get_server_pid()
            )
            if fixture.observer.get_server_pid() in blockers:
                return
            if pending.done():
                await pending
                raise AssertionError("worker did not wait for the request lock")


async def _assert_revoked_execution_unchanged(fixture, lease, before, history_count):
    """Denial leaves approval, serving, and the durable request result untouched."""
    assert (
        await fixture.connection.fetchrow(
            f'SELECT draft_revision,approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
        )
        == before
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history')
        == history_count
    )
    assert (await _heads(fixture))["generation_id"] is None
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 0
    )
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None


async def _revoke_while_request_locked(fixture, lease, phase, table_name="registry_publication_request"):
    """Release the actual native row fence only after trusted authority is revoked."""
    checked = asyncio.Event()
    calls = []
    is_revoked = False

    async def authorize(**request_by_name):
        """Model trusted live authority changing while PostgreSQL fences the request."""
        checked.set()
        if is_revoked and table_name == "registry_publication_request":
            assert (
                await fixture.observer.fetchval(
                    "SELECT count(*) FROM pg_locks WHERE pid=$1 AND granted AND mode<>'AccessShareLock' "
                    "AND relation IN (to_regclass($2),to_regclass($3))",
                    fixture.connection.get_server_pid(),
                    f'"{fixture.control_schema}".registry_revision_control',
                    f'"{fixture.control_schema}".network_serving_control',
                )
                == 0
            )
        return await _authority(calls, denied=is_revoked)(**request_by_name)

    blocker = fixture.observer.transaction()
    await blocker.start()
    pending = None
    try:
        if table_name == "registry_publication_request":
            await fixture.observer.fetchrow(
                f'SELECT request_id FROM "{fixture.control_schema}".registry_publication_request WHERE request_id=$1 FOR UPDATE',
                lease["request_id"],
            )
        else:
            await fixture.observer.fetchrow(
                f'SELECT id FROM "{fixture.control_schema}".{table_name} WHERE id=1 FOR UPDATE'
            )
        pending = asyncio.create_task(
            _complete(fixture, lease, authorize)
            if phase == "publication"
            else execution.approve_queued_registry_publication(
                fixture.connection,
                lease["request_id"],
                lease["lease_id"],
                authorize=authorize,
                control_schema=fixture.control_schema,
            )
        )
        await asyncio.wait_for(checked.wait(), 5)
        await _wait_for_request_lock(fixture, pending)
        is_revoked = True
        await blocker.rollback()
        with pytest.raises(PermissionError, match="registry_publication_denied"):
            await asyncio.wait_for(pending, 5)
    finally:
        if fixture.observer.is_in_transaction():
            await blocker.rollback()
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    assert len(calls) == (2 if table_name == "registry_publication_request" else 3)
    assert all(call == calls[0] for call in calls)


@pytest.mark.parametrize("phase", ["approval", "publication"])
async def test_revocation_during_request_lock_preserves_state(pipeline_db, serving_schema, phase):
    """Revoked live authority after a real lock wait prevents both durable writes."""
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    if phase == "publication":
        await _approve(fixture, lease, [])
    before = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
    )
    history_count = await fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history'
    )
    await _revoke_while_request_locked(fixture, lease, phase)
    await _assert_revoked_execution_unchanged(fixture, lease, before, history_count)


@pytest.mark.parametrize(
    ("phase", "table_name"),
    [
        ("approval", "registry_revision_control"),
        ("publication", "registry_revision_control"),
        ("publication", "network_serving_control"),
    ],
)
async def test_revocation_during_control_lock_preserves_state(pipeline_db, serving_schema, phase, table_name):
    """Fresh authority after each control wait prevents approval and publication."""
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    if phase == "publication":
        await _approve(fixture, lease, [])
    before = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
    )
    history_count = await fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history'
    )
    await _revoke_while_request_locked(fixture, lease, phase, table_name)
    await _assert_revoked_execution_unchanged(fixture, lease, before, history_count)


async def test_revocation_after_preparation_preserves_ready_candidate_and_serving_head(pipeline_db, serving_schema):
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    await _approve(fixture, lease, [])
    denied_calls = []
    with pytest.raises(PermissionError):
        await _complete(fixture, lease, _authority(denied_calls, denied=True))
    assert len(denied_calls) == 1 and (await _heads(fixture))["generation_id"] is None
    assert (
        await fixture.connection.fetchval(f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate')
        == "ready"
    )
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None


async def test_expired_lease_during_final_status_rolls_back_manifest(pipeline_db, serving_schema, monkeypatch):
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    await _approve(fixture, lease, [])
    original_finish = execution.finish_registry_publication_request

    async def expire_before_finish(connection, *args, **kwargs):
        await connection.execute(
            f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            lease["request_id"],
        )
        await original_finish(connection, *args, **kwargs)

    monkeypatch.setattr(execution, "finish_registry_publication_request", expire_before_finish)
    with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
        await _complete(fixture, lease, _authority([]))
    assert (await _heads(fixture))["generation_id"] is None
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 0
    )
    assert (
        await fixture.connection.fetchval(f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate')
        == "ready"
    )


@pytest.mark.parametrize("policy_revision", [True, -1, 2**63])
async def test_invalid_authority_receipt_cannot_approve(pipeline_db, serving_schema, policy_revision):
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    with pytest.raises(ValueError, match="authority_invalid"):
        await execution.approve_queued_registry_publication(
            fixture.connection,
            UUID(str(lease["request_id"])),
            lease["lease_id"],
            authorize=_authority([], policy_revision=policy_revision),
            control_schema=fixture.control_schema,
        )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history')
        == 0
    )


async def _reactivation_claim(fixture):
    original, current = await _published_pair(fixture)
    command = RegistryReactivationCommand(
        original["generation_id"],
        current["generation_id"],
        0,
        "Reviewed rollback",
        "reactivation-" + original["manifest_sha256"][:16],
    )
    actor = RegistryActor("platform_admin", UUID(fixture.copy_target.candidate_id), "system")
    async with fixture.connection.transaction():
        queued = await enqueue_registry_reactivation(
            fixture.connection, command, actor, session_token_sha256="a" * 64, control_schema=fixture.control_schema
        )
        lease = await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema)
    return command, actor, queued, lease


def _reactivation_authority(calls, *, revoke=False, tamper=False, mutate_actor=False):
    async def authorize(**request_by_name):
        calls.append(request_by_name)
        if mutate_actor:
            request_by_name["actor"]["kind"] = "client_owner"
        if revoke and len(calls) == 2:
            raise PermissionError("administrator access revoked")
        digest = hashlib.sha256(
            json.dumps(request_by_name["command"], sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "authorized": True,
            "actor": request_by_name["actor"],
            "command_sha256": "0" * 64 if tamper else digest,
            "policy_revision": 5,
        }

    return authorize


async def _complete_reactivation(fixture, lease, authorize):
    await fixture.connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        return await execution.complete_queued_registry_reactivation(
            fixture.connection,
            lease["request_id"],
            lease["lease_id"],
            authorize=authorize,
            control_schema=fixture.control_schema,
        )
    finally:
        await fixture.connection.execute("RESET ROLE")


async def test_reactivation_atomic_audit_and_completed_retry(publication_db):
    fixture = publication_db
    command, actor, queued, lease = await _reactivation_claim(fixture)
    calls = []
    result_by_name = await _complete_reactivation(fixture, lease, _reactivation_authority(calls))
    assert len(calls) == 3 and all(call == calls[0] for call in calls)
    assert calls[0]["command"]["idempotency_key"] == command.idempotency_key
    assert set(result_by_name) == {
        "operation",
        "previous_generation_id",
        "generation_id",
        "approved_custom_revision",
        "manifest_sha256",
        "changed",
    }
    assert result_by_name["generation_id"] == command.target_generation < result_by_name["previous_generation_id"]
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "completed" and status["result"] == result_by_name
    async with fixture.connection.transaction():
        assert await enqueue_registry_reactivation(
            fixture.connection, command, actor, session_token_sha256="a" * 64, control_schema=fixture.control_schema
        ) == {**queued, "state": "completed", "replayed": True}
        assert (
            await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema) is None
        )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history')
        == 0
    )


@pytest.mark.parametrize("failure", ["revoked", "digest", "lease", "command_changed", "actor_mutated"])
async def test_reactivation_authority_and_lease_failures_preserve_head_and_audit(publication_db, monkeypatch, failure):
    fixture = publication_db
    command, _actor, _queued, lease = await _reactivation_claim(fixture)
    if failure == "command_changed":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET command_json=jsonb_set(command_json,'{{reason}}','\"Different reason\"'::jsonb)"
        )
    if failure == "lease":
        original_finish = execution.finish_registry_publication_request

        async def expire_then_finish(connection, *arguments, **keywords):
            await connection.execute(
                f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second'"
            )
            await original_finish(connection, *arguments, **keywords)

        monkeypatch.setattr(execution, "finish_registry_publication_request", expire_then_finish)
    with pytest.raises((PermissionError, ValueError)):
        await _complete_reactivation(
            fixture,
            lease,
            _reactivation_authority(
                [], revoke=failure == "revoked", tamper=failure == "digest", mutate_actor=failure == "actor_mutated"
            ),
        )
    assert (await _heads(fixture))["generation_id"] == command.expected_serving_generation
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None


@pytest.mark.parametrize("damage", ["monotonic", "target", "approval", "changed", "digest", "extra"])
async def test_reactivation_status_rejects_mismatched_durable_result(publication_db, damage):
    fixture = publication_db
    _command, _actor, _queued, lease = await _reactivation_claim(fixture)
    result_by_name = await _complete_reactivation(fixture, lease, _reactivation_authority([]))
    changes_by_damage = {
        "target": {"generation_id": result_by_name["previous_generation_id"]},
        "approval": {"approved_custom_revision": 1},
        "changed": {"changed": 1},
        "digest": {"manifest_sha256": "invalid"},
        "extra": {"serving_generation": result_by_name["generation_id"]},
    }
    if damage == "monotonic":
        result_by_name = {"serving_generation": result_by_name["generation_id"], "approved_revision": 1}
    else:
        result_by_name.update(changes_by_damage[damage])
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_publication_request SET result_json=$1::jsonb',
        json.dumps(result_by_name),
    )
    with pytest.raises(ValueError):
        await read_registry_publication_request(
            fixture.connection, lease["request_id"], control_schema=fixture.control_schema
        )


@pytest.fixture
async def source_preview(rollback_composition):
    history = rollback_composition
    fixture = history.fixture
    command_by_name = {
        "operation": "prepare_source_rollback",
        "target_generation": history.first.generation_id,
        "expected_serving_generation": history.second.generation_id,
        "expected_approved_revision": history.revision,
        "reason": "Review retained source edition",
        "idempotency_key": uuid4().hex,
    }
    actor = RegistryActor("platform_admin", uuid4(), "system")
    async with fixture.connection.transaction():
        queued = await enqueue_registry_source_rollback_preview(
            fixture.connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.control_schema,
        )
        lease = await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=history.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(lease["request_id"], "address:" + approved.generation_id))
    try:
        yield history, command_by_name, actor, queued, lease
    finally:
        await _remove_candidates(fixture, [], lease["request_id"])


async def _complete_source_preview(source_preview, authorize):
    history, _command, _actor, _queued, lease = source_preview
    return await execution.complete_queued_registry_source_rollback_preview(
        history.fixture.connection,
        lease["request_id"],
        lease["lease_id"],
        authorize=authorize,
        writer_roles=_roles(history.fixture),
        control_schema=history.fixture.control_schema,
    )


async def _distinct_npi_locations(fixture, seed):
    """Correct custom offices before approval without duplicating ACA source sites."""
    for field, unit in (("first", "Suite 90"), ("second", "Suite 91")):
        location = getattr(seed, field)
        command = replace(
            seed.location_command,
            record_id=UUID(location["record_id"]),
            operation="correct",
            expected_revision=location["revision"],
            fields={
                **seed.location_command.fields,
                "address_json": {**seed.location_command.fields["address_json"], "second_line": unit},
            },
            idempotency_key=uuid4().hex,
        )
        setattr(seed, field, await _custom_draft(fixture, command, seed.actor))


async def _npi_initial_arguments(fixture):
    """Approve a real known NPI office before admitting the actual ACA recipe."""
    seed, arguments_by_name = await _initial_arguments(fixture)
    await _distinct_npi_locations(fixture, seed)
    member = seed.membership_command.fields["memberships_json"][0]
    command = replace(
        seed.membership_command,
        operation="correct",
        expected_revision=seed.membership["revision"],
        fields={"memberships_json": [{**member, "provider_system": "npi", "provider_id": "1000000491"}]},
        idempotency_key=uuid4().hex,
    )
    seed.membership = await _custom_draft(fixture, command, seed.actor)
    seed.membership_command = command
    approved = await _approve_records(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, seed.first, seed.second, seed.membership),
        seed.actor,
    )
    seed.revision = approved["approved_revision"]
    arguments_by_name["approved_revision"] = seed.revision
    arguments_by_name["address_sources"] = RegistryCompositionAddressSources(
        fixture.base,
        PinnedAddressSource(fixture.source_schema, "npi", fixture.base.generation_id),
        initial_source_recipes=fixture.recipes,
    )
    return seed, arguments_by_name


async def _custom_npi_history(fixture, copy_targets, request_ids):
    """Publish two recipe-backed compositions with one protected NPI pin."""
    seed, arguments_by_name = await _npi_initial_arguments(fixture)
    npi_pin = arguments_by_name["address_sources"].npi_source
    manifests = []
    for _ in range(2):
        request_id = arguments_by_name["request_id"]
        request_ids.append(request_id)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                fixture.connection, approved_revision=seed.revision, control_schema=fixture.control_schema
            )
        fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
        copy_target, addresses = await compose_registry_membership_candidate(fixture.connection, **arguments_by_name)
        copy_targets.append(copy_target)
        await _publish_composition_target(fixture, copy_target, addresses)
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            previous = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        manifests.append(previous)
        arguments_by_name = dict(
            request_id=uuid4(),
            approved_revision=seed.revision,
            expected_head=previous.generation_id,
            source_manifest=previous,
            address_sources=RegistryCompositionAddressSources(
                PinnedAddressSource(previous.schema_name, "entity_address_unified", previous.manifest_sha256), npi_pin
            ),
            writer_roles=_roles(fixture),
            control_schema=fixture.control_schema,
        )
    revision = await _approve_rollback_manual_office(fixture, seed)
    return SimpleNamespace(fixture=fixture, first=manifests[0], second=manifests[1], revision=revision, seed=seed)


@pytest.fixture
async def custom_npi_preview(initial_composition):
    """Register exact candidate and address identities before the real builders."""
    fixture = initial_composition
    copy_targets, request_ids = [], []
    try:
        history = await _custom_npi_history(fixture, copy_targets, request_ids)
        command_by_name = {
            "operation": "prepare_source_rollback",
            "target_generation": history.first.generation_id,
            "expected_serving_generation": history.second.generation_id,
            "expected_approved_revision": history.revision,
            "reason": "Review retained NPI office",
            "idempotency_key": uuid4().hex,
        }
        actor = RegistryActor("platform_admin", uuid4(), "system")
        async with fixture.connection.transaction():
            queued = await enqueue_registry_source_rollback_preview(
                fixture.connection,
                command_by_name,
                actor,
                session_token_sha256="a" * 64,
                control_schema=fixture.control_schema,
            )
            lease = await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema)
        request_ids.append(lease["request_id"])
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            approved = await pin_approved_membership_source(
                fixture.connection, approved_revision=history.revision, control_schema=fixture.control_schema
            )
        fixture.composition_ids.append(uuid5(lease["request_id"], "address:" + approved.generation_id))
        yield history, command_by_name, actor, queued, lease
    finally:
        for request_id in request_ids:
            await _remove_candidates(fixture, copy_targets, request_id)


async def test_source_preview_preserves_verified_retained_custom_npi_office(custom_npi_preview):
    """A retained serving heap needs no invented local NPI table for rollback."""
    history, _command, _actor, _queued, _lease = custom_npi_preview
    fixture = history.fixture
    assert await fixture.connection.fetchval("SELECT to_regclass($1)", history.first.schema_name + ".npi") is None
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        report = json.loads(
            await fixture.connection.fetchval(
                f'SELECT validation_json::text FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(history.first.candidate_id),
            )
        )
        document = await fixture.connection.fetchval(
            "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1",
            report["address_source"]["schema_name"],
        )
        retained = _custom_address_receipt(document)
        assert retained.npi_source.schema_name == fixture.source_schema
        with pytest.raises(NetworkCustomAddressSourceError, match="Approved revision"):
            await verify_custom_address_source(fixture.connection, retained)
        assert await verify_retained_custom_address_source(fixture.connection, retained) == retained
    preview_result = await _complete_source_preview(custom_npi_preview, _reactivation_authority([]))
    candidate = await fixture.connection.fetchrow(
        f'SELECT state,schema_name FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(preview_result["candidate_id"]),
    )
    assert candidate["state"] == "ready"
    assert preview_result["source_selection"] == {"mapped_rows": 2, "omitted_rows": 0}
    offices = await fixture.connection.fetch(
        f'SELECT second_line FROM "{candidate["schema_name"]}".entity_address_unified '
        "WHERE entity_type='npi' AND entity_id='1000000491' ORDER BY second_line"
    )
    assert [office["second_line"] for office in offices] == ["Suite 2", "Suite 3", "Suite 91"]
    office = await fixture.connection.fetchrow(
        f'SELECT provider_id,location_id FROM "{candidate["schema_name"]}".provider_location_binding '
        "WHERE provider_system='npi' AND provider_id='1000000491' AND location_id=$1",
        UUID(history.seed.second["record_id"]),
    )
    assert office is not None
    assert office["location_id"] == UUID(history.seed.second["record_id"])
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == history.second.generation_id
    )


async def test_source_preview_rejects_changed_recorded_npi_oid(custom_npi_preview):
    """Equal names, columns and NPI rows cannot replace the recorded native OID."""
    history, _command, _actor, _queued, lease = custom_npi_preview
    fixture = history.fixture
    schema = '"' + fixture.source_schema + '"'
    original_oid = await fixture.connection.fetchval("SELECT to_regclass($1)::oid", fixture.source_schema + ".npi")
    await fixture.connection.execute(f"ALTER TABLE {schema}.npi RENAME TO npi_previous")
    await fixture.connection.execute(f"CREATE TABLE {schema}.npi (LIKE {schema}.npi_previous INCLUDING ALL)")
    await fixture.connection.execute(f"INSERT INTO {schema}.npi SELECT * FROM {schema}.npi_previous")
    await fixture.connection.execute(f'ALTER TABLE {schema}.npi OWNER TO "{fixture.roles["owner"]}"')
    await fixture.connection.execute(f'GRANT SELECT ON {schema}.npi TO "{fixture.roles["reader"]}"')
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)::oid", fixture.source_schema + ".npi") != original_oid
    )
    assert await fixture.connection.fetchval(
        f"SELECT (SELECT array_agg(to_jsonb(n) ORDER BY npi) FROM {schema}.npi n) = "
        f"(SELECT array_agg(to_jsonb(n) ORDER BY npi) FROM {schema}.npi_previous n)"
    )
    with pytest.raises(NetworkCustomAddressSourceError, match="physical identity"):
        await _complete_source_preview(custom_npi_preview, _reactivation_authority([]))
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None


async def _revoke_retained_control_wait(fixture, lease, table_name, operation):
    """A second native backend holds the control until trusted authority changes."""
    observer = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    blocker = observer.transaction()
    checked, calls = asyncio.Event(), []
    is_revoked, pending = False, None
    head = await fixture.connection.fetchval(
        f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
    )

    async def authorize(**request):
        checked.set()
        calls.append(request)
        if is_revoked:
            raise PermissionError("registry_publication_denied")
        return await _reactivation_authority([])(**request)

    try:
        await blocker.start()
        await observer.fetchrow(f'SELECT id FROM "{fixture.control_schema}".{table_name} WHERE id=1 FOR UPDATE')
        pending = asyncio.create_task(operation(authorize))
        await asyncio.wait_for(checked.wait(), 5)
        await _wait_for_request_lock(SimpleNamespace(connection=fixture.connection, observer=observer), pending)
        is_revoked = True
        await blocker.rollback()
        with pytest.raises(PermissionError, match="registry_publication_denied"):
            await asyncio.wait_for(pending, 5)
    finally:
        if observer.is_in_transaction():
            await blocker.rollback()
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        await observer.close()
    assert len(calls) == 3 and all(call == calls[0] for call in calls)
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control WHERE id=1'
        )
        == head
    )
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None


@pytest.mark.parametrize("table_name", ["registry_revision_control", "network_serving_control"])
async def test_reactivation_revocation_after_control_wait(publication_db, table_name):
    """Both retained-head control waits require a fresh authorization receipt."""
    fixture = publication_db
    _command, _actor, _queued, lease = await _reactivation_claim(fixture)
    await _revoke_retained_control_wait(
        fixture, lease, table_name, lambda authorize: _complete_reactivation(fixture, lease, authorize)
    )


@pytest.mark.parametrize("table_name", ["registry_revision_control", "network_serving_control"])
async def test_source_preview_revocation_after_control_wait(source_preview, table_name):
    """A prepared candidate cannot become an authorized preview after revocation."""
    history, _command, _actor, _queued, lease = source_preview
    await _revoke_retained_control_wait(
        history.fixture, lease, table_name, lambda authorize: _complete_source_preview(source_preview, authorize)
    )


@pytest.mark.parametrize("table_name", ["registry_revision_control", "network_serving_control"])
async def test_source_publication_revocation_after_control_wait(source_publication, table_name):
    """Retained rollback publication cannot cross either control with old authority."""
    history, _command, _actor, _queued, lease, _preview = source_publication
    await _revoke_retained_control_wait(
        history.fixture,
        lease,
        table_name,
        lambda authorize: _complete_source_publication(source_publication, authorize),
    )


async def test_source_preview_actual_aca_latest_manual_diff_ready_without_publication(source_preview):
    history, command, actor, queued, lease = source_preview
    fixture = history.fixture
    before = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision,generation_id FROM "{fixture.control_schema}".registry_revision_control '
        f'CROSS JOIN "{fixture.control_schema}".network_serving_control'
    )
    calls = []
    result_by_name = await _complete_source_preview(source_preview, _reactivation_authority(calls))
    assert len(calls) == 3 and all(call["command"] == command for call in calls)
    assert set(result_by_name) == {
        "operation",
        "candidate_id",
        "source_generation_id",
        "expected_serving_generation",
        "approved_custom_revision",
        "candidate_sha256",
        "additions",
        "removals",
        "unresolved_rows",
        "source_selection",
    }
    assert (result_by_name["additions"], result_by_name["removals"], result_by_name["unresolved_rows"]) == (2, 1, 0)
    assert result_by_name["source_selection"] == {"mapped_rows": 2, "omitted_rows": 0}
    candidate = await fixture.connection.fetchrow(
        f"SELECT state,validation_json,source_recipes_json,expected_head,approved_custom_revision "
        f'FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(result_by_name["candidate_id"]),
    )
    assert candidate["state"] == "ready" and candidate["approved_custom_revision"] == history.revision
    after = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision,generation_id FROM "{fixture.control_schema}".registry_revision_control '
        f'CROSS JOIN "{fixture.control_schema}".network_serving_control'
    )
    assert after == before
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "completed" and status["result"] == result_by_name
    async with fixture.connection.transaction():
        replay = await enqueue_registry_source_rollback_preview(
            fixture.connection,
            command,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.control_schema,
        )
    assert replay == {**queued, "state": "completed", "replayed": True}
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_serving_manifest WHERE candidate_id=$1)',
        UUID(result_by_name["candidate_id"]),
    )


@pytest.mark.parametrize("fault", ["cancel", "revoke", "lease", "approval"])
async def test_source_preview_fault_keeps_closed_candidate_and_head_then_resumes(source_preview, monkeypatch, fault):
    history, _command, _actor, _queued, lease = source_preview
    fixture = history.fixture
    prepare = execution.prepare_network_candidate
    finish = execution.finish_registry_publication_request
    calls = []

    async def interrupted_prepare(*arguments, **options):
        candidate = await prepare(*arguments, **options)
        if fault == "cancel":
            raise asyncio.CancelledError
        if fault == "approval":
            draft = await _draft(fixture.engine, fixture.control_schema, _create(), history.seed.actor)
            await _approve_for_preview(fixture, draft, history.seed.actor)
        return candidate

    async def expired_finish(connection, *arguments, **options):
        await connection.execute(
            f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second'"
        )
        return await finish(connection, *arguments, **options)

    monkeypatch.setattr(execution, "prepare_network_candidate", interrupted_prepare)
    if fault == "lease":
        monkeypatch.setattr(execution, "finish_registry_publication_request", expired_finish)
    expected = {
        "cancel": asyncio.CancelledError,
        "revoke": PermissionError,
        "lease": RegistryApprovalConflict,
        "approval": RegistryApprovalConflict,
    }[fault]
    with pytest.raises(expected):
        await _complete_source_preview(source_preview, _reactivation_authority(calls, revoke=fault == "revoke"))
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == history.second.generation_id
    )
    state = await fixture.connection.fetchval(
        f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1',
        uuid5(lease["request_id"], "dataset"),
    )
    assert state == "ready"
    monkeypatch.setattr(execution, "prepare_network_candidate", prepare)
    monkeypatch.setattr(execution, "finish_registry_publication_request", finish)
    if fault != "approval":
        result_by_name = await _complete_source_preview(source_preview, _reactivation_authority([]))
        assert result_by_name["unresolved_rows"] == 0


async def _approve_for_preview(fixture, draft, actor):
    from tests.test_registry_approval_store_postgres import _approve

    await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, draft),
        actor,
    )


@pytest.mark.parametrize("damage", ["bool", "overflow", "head", "extra", "monotonic"])
async def test_source_preview_status_rejects_malformed_completed_audit(source_preview, damage):
    history, _command, _actor, _queued, lease = source_preview
    fixture = history.fixture
    result_by_name = await _complete_source_preview(source_preview, _reactivation_authority([]))
    changes_by_case = {
        "bool": {"additions": True},
        "overflow": {"source_selection": {"mapped_rows": 2**53, "omitted_rows": 0}},
        "head": {"expected_serving_generation": history.first.generation_id},
        "extra": {"reason": "private"},
        "monotonic": {"selected_count": 1, "previous_approved_revision": 0},
    }
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".registry_publication_request SET result_json=$1::jsonb WHERE request_id=$2',
        json.dumps({**result_by_name, **changes_by_case[damage]}),
        lease["request_id"],
    )
    with pytest.raises(ValueError, match="source_rollback_result_invalid"):
        await read_registry_publication_request(
            fixture.connection, lease["request_id"], control_schema=fixture.control_schema
        )


async def test_source_preview_diff_holds_only_isolated_candidate_locks(source_preview, monkeypatch):
    history, _command, _actor, _queued, _lease = source_preview
    fixture = history.fixture
    difference = execution._source_rollback_difference
    observations = []

    async def observed_difference(connection, *arguments):
        controls_locked = await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
            "AND relation=ANY($1::regclass[]) AND mode<>'AccessShareLock')",
            [
                f'"{fixture.control_schema}".registry_revision_control',
                f'"{fixture.control_schema}".network_serving_control',
            ],
        )
        observations.append(controls_locked)
        return await difference(connection, *arguments)

    monkeypatch.setattr(execution, "_source_rollback_difference", observed_difference)
    receipt = await _complete_source_preview(source_preview, _reactivation_authority([]))
    assert observations == [False] and receipt["additions"] == 2


async def test_source_preview_ineligible_retained_source_rejects_before_preparation(source_preview):
    history, _command, _actor, _queued, lease = source_preview
    fixture = history.fixture
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
        history.first.generation_id,
    )
    with pytest.raises(ValueError, match="registry_source_rollback_source_unavailable"):
        await _complete_source_preview(source_preview, _reactivation_authority([]))
    assert not await fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT 1 FROM "{fixture.control_schema}".network_membership_candidate WHERE dataset_id=$1)',
        uuid5(lease["request_id"], "dataset"),
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == history.second.generation_id
    )


async def test_source_preview_manifest_storage_error_remains_transient(monkeypatch):
    from process.network_serving_read import NetworkServingReadUnavailable

    async def unavailable(*arguments, **options):
        try:
            raise asyncpg.CannotConnectNowError("synthetic storage outage")
        except asyncpg.PostgresError as error:
            raise NetworkServingReadUnavailable("Network serving manifest verification failed") from error

    import asyncpg

    monkeypatch.setattr(execution, "resolve_network_serving_manifest", unavailable)
    with pytest.raises(asyncpg.CannotConnectNowError):
        await execution._source_rollback_manifest(None, generation_id=1)


@pytest.fixture
async def source_publication(source_preview):
    history, _preview_command, actor, _preview_queue, preview_lease = source_preview
    preview = await _complete_source_preview(source_preview, _reactivation_authority([]))
    command_by_name = {
        "operation": "publish_source_rollback",
        "preview_request_id": str(preview_lease["request_id"]),
        "candidate_sha256": preview["candidate_sha256"],
        "expected_serving_generation": history.second.generation_id,
        "expected_approved_revision": history.revision,
        "reason": "Publish reviewed retained source",
        "idempotency_key": uuid4().hex,
    }
    fixture = history.fixture
    async with fixture.connection.transaction():
        queued = await enqueue_registry_source_rollback_publication(
            fixture.connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=fixture.control_schema,
        )
        lease = await claim_registry_publication_request(fixture.connection, control_schema=fixture.control_schema)
    return history, command_by_name, actor, queued, lease, preview


async def _complete_source_publication(source_publication, authorize):
    history, _command, _actor, _queued, lease, _preview = source_publication
    return await execution.complete_queued_registry_source_rollback_publication(
        history.fixture.connection,
        lease["request_id"],
        lease["lease_id"],
        authorize=authorize,
        writer_roles=_roles(history.fixture),
        control_schema=history.fixture.control_schema,
    )


async def test_source_publication_explicit_review_commits_head_audit_and_preserves_readers(source_publication):
    from process.network_serving_read import resolve_network_serving_manifest

    history, command, actor, queued, lease, preview = source_publication
    fixture = history.fixture
    reader = await _rollback_reader(fixture)
    try:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await reader.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
            pin = await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema)
            before = await _rollback_members(reader, pin)
            calls = []
            result_by_name = await _complete_source_publication(source_publication, _reactivation_authority(calls))
            assert len(calls) == 3 and all(call["command"] == command for call in calls)
            assert result_by_name["candidate_id"] == preview["candidate_id"]
            assert result_by_name["candidate_sha256"] == preview["candidate_sha256"]
            assert result_by_name["previous_generation_id"] == history.second.generation_id
            assert await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema) == pin
            assert await _rollback_members(reader, pin) == before
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            current = await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
            assert current.generation_id == result_by_name["generation_id"] > history.second.generation_id
            await _assert_rollback_source_and_manual(history, current)
        status = await read_registry_publication_request(
            fixture.connection, lease["request_id"], control_schema=fixture.control_schema
        )
        assert status["state"] == "completed" and status["result"] == result_by_name
        assert set(result_by_name) == {
            "operation",
            "preview_request_id",
            "candidate_id",
            "candidate_sha256",
            "previous_generation_id",
            "generation_id",
            "approved_custom_revision",
            "manifest_sha256",
        }
        async with fixture.connection.transaction():
            replay = await enqueue_registry_source_rollback_publication(
                fixture.connection,
                command,
                actor,
                session_token_sha256="a" * 64,
                control_schema=fixture.control_schema,
            )
        assert replay == {**queued, "state": "completed", "replayed": True}
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest'
            )
            == 3
        )
    finally:
        await reader.close()


@pytest.mark.parametrize("fault", ["revoke", "cancel", "timeout", "lease"])
async def test_source_publication_final_fault_rolls_back_head_and_audit_exact_retry(
    source_publication, monkeypatch, fault
):
    history, _command, _actor, _queued, lease, preview = source_publication
    fixture = history.fixture
    finish = execution.finish_registry_publication_request

    async def fail_after_manifest(connection, *arguments, **options):
        if fault == "lease":
            await connection.execute(
                f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET lease_expires_at=now()-interval '1 second' WHERE request_id=$1",
                lease["request_id"],
            )
            return await finish(connection, *arguments, **options)
        raise asyncio.CancelledError if fault == "cancel" else TimeoutError("synthetic audit timeout")

    if fault != "revoke":
        monkeypatch.setattr(execution, "finish_registry_publication_request", fail_after_manifest)
    expected = {
        "revoke": PermissionError,
        "cancel": asyncio.CancelledError,
        "timeout": TimeoutError,
        "lease": RegistryApprovalConflict,
    }[fault]
    with pytest.raises(expected):
        await _complete_source_publication(source_publication, _reactivation_authority([], revoke=fault == "revoke"))
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == history.second.generation_id
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(preview["candidate_id"]),
        )
        == "ready"
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 2
    )
    status = await read_registry_publication_request(
        fixture.connection, lease["request_id"], control_schema=fixture.control_schema
    )
    assert status["state"] == "running" and status["result"] is None
    monkeypatch.setattr(execution, "finish_registry_publication_request", finish)
    result_by_name = await _complete_source_publication(source_publication, _reactivation_authority([]))
    assert result_by_name["candidate_id"] == preview["candidate_id"]
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.control_schema}".network_serving_manifest')
        == 3
    )


@pytest.mark.parametrize("damage", ["preview", "digest", "closure", "source", "approval"])
async def test_source_publication_refuses_changed_review_pins_and_closure(source_publication, damage):
    history, command_by_name, _actor, _queued, _lease, preview = source_publication
    fixture = history.fixture
    candidate_schema = await fixture.connection.fetchval(
        f'SELECT schema_name FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
        UUID(preview["candidate_id"]),
    )
    if damage == "preview":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".registry_publication_request SET result_json=jsonb_set(result_json,'{{candidate_sha256}}',to_jsonb($1::text)) WHERE request_id=$2",
            "0" * 64,
            UUID(command_by_name["preview_request_id"]),
        )
    elif damage == "digest":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=validation_json||\'{{"changed":true}}\'::jsonb WHERE candidate_id=$1',
            UUID(preview["candidate_id"]),
        )
    elif damage == "closure":
        await fixture.connection.execute(
            f'GRANT INSERT ON "{candidate_schema}".network_membership TO "{fixture.roles["reader"]}"'
        )
    elif damage == "source":
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
            history.first.generation_id,
        )
    else:
        draft = await _draft(fixture.engine, fixture.control_schema, _create(), history.seed.actor)
        await _approve_for_preview(fixture, draft, history.seed.actor)
    with pytest.raises(ValueError):
        await _complete_source_publication(source_publication, _reactivation_authority([]))
    assert (
        await fixture.connection.fetchval(
            f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
        )
        == history.second.generation_id
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(preview["candidate_id"]),
        )
        == "ready"
    )


async def test_source_publication_competing_native_head_preserves_reviewed_ready_candidate(source_publication):
    history, _command, _actor, _queued, _lease, preview = source_publication
    fixture = history.fixture
    request_id = uuid4()
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=history.revision, control_schema=fixture.control_schema
        )
    fixture.composition_ids.append(uuid5(request_id, "address:" + approved.generation_id))
    competing_target, addresses = await execution.compose_registry_membership_candidate(
        fixture.connection, **{**history.arguments_by_name, "request_id": request_id}
    )
    try:
        await execution.prepare_network_candidate(
            fixture.connection, competing_target, addresses, **_roles(fixture), control_schema=fixture.control_schema
        )
        async with fixture.connection.transaction():
            competing_manifest = await execution.publish_network_candidate(
                fixture.connection, competing_target, control_schema=fixture.control_schema
            )
        with pytest.raises(RegistryApprovalConflict, match="registry_source_rollback_revision_conflict"):
            await _complete_source_publication(source_publication, _reactivation_authority([]))
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
            )
            == competing_manifest["generation_id"]
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
                UUID(preview["candidate_id"]),
            )
            == "ready"
        )
    finally:
        await _remove_candidates(fixture, [competing_target], request_id)


async def test_revocation_during_company_fence_preserves_approval(pipeline_db, serving_schema):
    """Live authority is checked again after a real shared advisory fence wait."""
    fixture = pipeline_db
    lease = await _claim(fixture, serving_schema[2])
    before = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{fixture.control_schema}".registry_revision_control WHERE id=1'
    )
    history_count = await fixture.connection.fetchval(
        f'SELECT count(*) FROM "{fixture.control_schema}".registry_approval_history'
    )
    key = _fence_key(fixture.control_schema)
    checked = asyncio.Event()
    calls = []
    is_revoked = False

    async def authorize(**request_by_name):
        checked.set()
        return await _authority(calls, denied=is_revoked)(**request_by_name)

    pending = None
    is_held = False
    try:
        await fixture.observer.execute("SELECT pg_advisory_lock_shared($1)", key)
        is_held = True
        pending = asyncio.create_task(
            execution.approve_queued_registry_publication(
                fixture.connection,
                lease["request_id"],
                lease["lease_id"],
                authorize=authorize,
                control_schema=fixture.control_schema,
            )
        )
        await asyncio.wait_for(checked.wait(), 5)
        await _wait_for_request_lock(fixture, pending)
        is_revoked = True
        assert await fixture.observer.fetchval("SELECT pg_advisory_unlock_shared($1)", key) is True
        is_held = False
        with pytest.raises(PermissionError, match="registry_publication_denied"):
            await asyncio.wait_for(pending, 5)
    finally:
        if is_held:
            assert await fixture.observer.fetchval("SELECT pg_advisory_unlock_shared($1)", key) is True
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    assert len(calls) == 3 and all(call == calls[0] for call in calls)
    await _assert_revoked_execution_unchanged(fixture, lease, before, history_count)
