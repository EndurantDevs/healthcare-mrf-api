# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Completion and exact request claims cannot borrow ordinary queue authority."""

import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import registry_membership_refresh_queue as queue
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_contract import (
    RegistryMembershipRefreshUnavailable,
    membership_refresh_command_sha256,
)
from process.registry_membership_refresh_queue import _validated_completion
from process.registry_publication_queue import claim_registry_publication_request
from tests.test_registry_membership_refresh_contract import command


def result_for(selected):
    return {
        "operation": "refresh_memberships",
        "previous_generation_id": selected["expected_serving_generation"],
        "generation_id": selected["expected_serving_generation"] + 1,
        "candidate_id": str(uuid4()),
        "manifest_sha256": "f" * 64,
        "approved_revision": selected["expected_approved_revision"],
        "command_sha256": membership_refresh_command_sha256(selected),
        "policy_revision": 3,
        "orphan_bindings": 0,
    }


@pytest.mark.parametrize("head", [0, 9])
def test_completion_retains_exact_command_identity(head):
    selected = command(
        expected_serving_generation=head, expected_serving_manifest_sha256=None if head == 0 else "a" * 64
    )
    _validated_completion(result_for(selected), selected)


@pytest.mark.parametrize(
    "changes",
    [
        {"operation": "publish_fhir_catalog"},
        {"candidate_id": str(uuid4()).upper()},
        {"generation_id": 9},
        {"generation_id": True},
        {"approved_revision": 4},
        {"previous_generation_id": 8},
        {"command_sha256": "a" * 64},
        {"policy_revision": -1},
        {"orphan_bindings": True},
        {"orphan_bindings": -1},
        {"manifest_sha256": "F" * 64},
        {"extra": 1},
    ],
)
def test_invalid_completion_never_becomes_recovery_evidence(changes):
    selected = command()
    with pytest.raises(ValueError, match="refresh_result_invalid"):
        _validated_completion({**result_for(selected), **changes}, selected)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"operation_family": "office", "request_id": uuid4()},
        {"operation_family": "publication"},
        {
            "operation_family": "publication",
            "request_id": uuid4(),
            "metadata_only": True,
            "allow_metadata": True,
        },
    ],
)
async def test_refresh_claim_cannot_drain_an_unscoped_or_other_worker_queue(options):
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    with pytest.raises(ValueError):
        await claim_registry_publication_request(connection, membership_refresh_only=True, **options)


@pytest.mark.parametrize(
    "state,result",
    [("queued", {}), ("running", {"generation_id": 1}), ("rejected", {"error": "private detail"}), ("unknown", None)],
)
def test_refresh_status_cannot_reuse_completion_or_unvalidated_failure_metadata(state, result):
    from tests.test_registry_membership_refresh_observations import published_rows

    _, record, _, _ = published_rows()
    record.update(state=state, result_json=json.dumps(result))
    with pytest.raises(ValueError, match="refresh_result_invalid|refresh_state_invalid"):
        queue.validated_membership_refresh_request_status(record)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["transaction", "authority", "isolation"])
async def test_refresh_enqueue_requires_a_live_typed_repeatable_snapshot(fault):
    from tests.test_registry_membership_refresh_authority import client
    from tests.test_registry_membership_refresh_contract import envelope

    connection = SimpleNamespace(
        is_in_transaction=lambda: fault != "transaction",
        fetchval=AsyncMock(return_value="read committed"),
        fetchrow=AsyncMock(),
    )
    async with client(lambda _request: pytest.fail("invalid context contacted authority")) as authorize:
        with pytest.raises(
            (ValueError, RegistryMembershipRefreshUnavailable),
            match="requires_caller_transaction|refresh_unconfigured|snapshot_required",
        ):
            await queue.enqueue_registry_membership_refresh(
                connection,
                envelope(),
                authorize={} if fault == "authority" else authorize,
                address_profile=queue.RegistryMembershipRefreshAddressProfile("addresses", ("reader",)),
            )
    connection.fetchrow.assert_not_awaited()


def _queue_race_connection(winner):
    """Model a concurrent insert between the first lookup and replay lookup."""
    state = SimpleNamespace(lookups=0, stored_by_field={}, events=[])

    @asynccontextmanager
    async def transaction():
        state.events.append("enter")
        try:
            yield
        finally:
            state.events.append("exit")

    async def fetchrow(sql, *arguments):
        if "WHERE actor_key=$1 AND idempotency_key=$2" in sql:
            state.lookups += 1
            if state.lookups == 1 or winner == "missing":
                return None
            return {
                **state.stored_by_field,
                "request_sha256": "0" * 64 if winner == "changed" else state.stored_by_field["request_sha256"],
            }
        assert "INSERT INTO" in sql
        state.stored_by_field.update(
            request_id=uuid4(),
            state="queued",
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            request_sha256=arguments[-1],
        )
        return None

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value="repeatable read"),
        fetchrow=AsyncMock(side_effect=fetchrow),
        transaction=transaction,
    )
    return connection, state


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["same", "changed", "missing"])
async def test_refresh_insert_race_rechecks_the_complete_immutable_request(monkeypatch, winner):
    from tests.test_registry_membership_refresh_authority import client, receipt
    from tests.test_registry_membership_refresh_contract import envelope
    from tests.test_registry_publication_authority import _reply

    document = envelope()
    calls = []
    connection, state = _queue_race_connection(winner)

    async def authority(request):
        actual = json.loads(request.content)
        calls.append(actual)
        return _reply(receipt(actual))

    current = AsyncMock(return_value={})
    address = AsyncMock()
    monkeypatch.setattr(queue, "_current_heads", current)
    monkeypatch.setattr(queue, "_require_address", address)
    async with client(authority) as authorize:
        options_by_name = dict(
            authorize=authorize,
            address_profile=queue.RegistryMembershipRefreshAddressProfile("addresses", ("reader",)),
            control_schema="registry",
        )
        if winner == "same":
            queued = await queue.enqueue_registry_membership_refresh(connection, document, **options_by_name)
            assert queued["replayed"] is True and queued["request_id"] == str(state.stored_by_field["request_id"])
        else:
            with pytest.raises(RegistryApprovalConflict, match="queue_idempotency_conflict"):
                await queue.enqueue_registry_membership_refresh(connection, document, **options_by_name)
    assert calls == [document] and state.events == ["enter", "exit"] and state.lookups == 2
    current.assert_awaited_once()
    address.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["layout", "generation", "manifest", "address"])
async def test_retained_address_reuse_requires_the_exact_current_source_epoch(monkeypatch, changed):
    selected = command()
    pinned = SimpleNamespace(
        compact_layout=object(),
        generation_id=selected["expected_serving_generation"],
        manifest_sha256=selected["expected_serving_manifest_sha256"],
        source_generations={"network_address_origin": selected["address_artifact_sha256"]},
    )
    if changed == "layout":
        pinned.compact_layout = None
    elif changed == "generation":
        pinned.generation_id += 1
    elif changed == "manifest":
        pinned.manifest_sha256 = "f" * 64
    else:
        pinned.source_generations["network_address_origin"] = "f" * 64
    monkeypatch.setattr(queue, "resolve_network_serving_manifest", AsyncMock(return_value=pinned))
    with pytest.raises(RegistryMembershipRefreshUnavailable, match="retained_address_unproved"):
        await queue._has_retained_address(
            object(),
            selected,
            {
                "address_origin": selected["address_artifact_sha256"],
                "generation_id": selected["expected_serving_generation"],
            },
            "registry",
        )
