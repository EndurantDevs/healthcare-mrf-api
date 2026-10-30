# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit queue families fail closed before a protected claim can run."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import registry_publication_queue as queue
from process.registry_approval_store import RegistryApprovalCommand, RegistryApprovalConflict
from process.registry_publication_queue import claim_registry_publication_request
from process.registry_record_store import RegistryActor


class _Connection:
    def __init__(self, *, in_transaction=True):
        self.in_transaction = in_transaction
        self.fetchrow = AsyncMock(return_value=None)
        self.fetchval = AsyncMock(return_value=None)

    def is_in_transaction(self):
        return self.in_transaction


@pytest.mark.asyncio
@pytest.mark.parametrize("family", [None, "", "all", "publication ", "Publication", "office_prepare", True, 0, [], {}])
async def test_unknown_or_malformed_family_refuses_before_claim(family):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_operation_family_invalid"):
        await claim_registry_publication_request(connection, operation_family=family)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_omitted_family_cannot_fall_back_to_unfiltered_claim():
    connection = _Connection()
    with pytest.raises(TypeError, match="operation_family"):
        await claim_registry_publication_request(connection)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("request_by_name", [{}, {"request_id": UUID(int=1)}])
@pytest.mark.parametrize(
    "family,ordinary,operations",
    [
        (
            "publication",
            True,
            (
                "reactivate_generation",
                "prepare_source_rollback",
                "publish_source_rollback",
                "publish_initial_inventory",
            ),
        ),
        ("office", False, ("prepare_ptg_office_capture", "approve_ptg_office_capture")),
    ],
)
async def test_family_claim_has_closed_parameters_for_queued_and_expired_rows(
    family, ordinary, operations, request_by_name
):
    connection = _Connection()
    assert (
        await claim_registry_publication_request(
            connection, operation_family=family, control_schema="registry", **request_by_name
        )
        is None
    )
    connection.fetchrow.assert_awaited_once()
    query, lease_id, seconds, accepts_absent_operation, accepted_operations, selected_request_id = (
        connection.fetchrow.await_args.args
    )
    assert isinstance(lease_id, UUID) and lease_id.int and seconds == 300
    assert accepts_absent_operation is ordinary and accepted_operations == operations
    assert selected_request_id == request_by_name.get("request_id")
    assert "WHERE (state='queued' OR state='running' AND lease_expires_at<=clock_timestamp())" in query
    assert "AND ($5::uuid IS NULL OR request_id=$5)" in query
    assert "jsonb_typeof(command_json->'operation')='string'" in query
    assert "NOT (command_json ? 'operation')" in query
    assert "ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id", [UUID(int=0), "00000000-0000-0000-0000-000000000001", 1, True])
async def test_malformed_request_identity_refuses_before_claim(request_id):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_request_identity_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication", request_id=request_id)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [True, 0, 29, 901, "300"])
async def test_partition_preserves_lease_refusal_before_sql(seconds):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication", lease_seconds=seconds)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_still_requires_caller_owned_transaction():
    connection = _Connection(in_transaction=False)
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        await claim_registry_publication_request(connection, operation_family="publication")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"allow_metadata": 1},
        {"metadata_only": 1},
        {"metadata_only": True},
        {"metadata_only": True, "allow_metadata": True, "operation_family": "office"},
        {"membership_refresh_only": 1},
        {"membership_refresh_only": True},
        {"membership_refresh_only": True, "request_id": UUID(int=1), "operation_family": "office"},
        {"membership_refresh_only": True, "request_id": UUID(int=1), "metadata_only": True, "allow_metadata": True},
    ],
)
async def test_refresh_claim_cannot_expand_family_or_omit_the_exact_request(options):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_operation_family_invalid"):
        await claim_registry_publication_request(connection, **{"operation_family": "publication", **options})
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [30, 900])
async def test_exact_refresh_claim_keeps_request_and_lease_bounds(seconds):
    connection = _Connection()
    request_id = UUID(int=1)
    connection.fetchrow.return_value = {"request_id": request_id, "state": "running"}
    assert await claim_registry_publication_request(
        connection,
        operation_family="publication",
        membership_refresh_only=True,
        request_id=request_id,
        lease_seconds=seconds,
        control_schema="registry",
    ) == {"request_id": request_id, "state": "running"}
    _, lease, duration, ordinary, operations, selected = connection.fetchrow.await_args.args
    assert lease.int and duration == seconds
    assert ordinary is False and operations == ("refresh_memberships",) and selected == request_id


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [True, 29, 901])
async def test_lease_renewal_refuses_invalid_duration_before_sql(seconds):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        await queue.renew_registry_publication_lease(connection, UUID(int=1), UUID(int=2), lease_seconds=seconds)
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["renew", "finish"])
@pytest.mark.parametrize("fault", ["request", "lease", "transaction"])
async def test_lease_mutations_require_typed_identity_and_caller_transaction(operation, fault):
    connection = _Connection(in_transaction=fault != "transaction")
    request = "request" if fault == "request" else UUID(int=1)
    lease = "lease" if fault == "lease" else UUID(int=2)
    with pytest.raises(ValueError, match="registry_queue_lease_invalid"):
        if operation == "renew":
            await queue.renew_registry_publication_lease(connection, request, lease)
        else:
            await queue.finish_registry_publication_request(connection, request, lease, state="completed", result={})
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("state,result", [("running", {}), ("completed", []), ("rejected", {"reason": "x" * 65536})])
async def test_terminal_result_is_closed_and_bounded_before_clearing_a_lease(state, result):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_queue_result_invalid"):
        await queue.finish_registry_publication_request(
            connection, UUID(int=1), UUID(int=2), state=state, result=result
        )
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("request_id", [None, "00000000-0000-0000-0000-000000000001", UUID(int=0)])
async def test_status_refuses_untyped_or_zero_request_identity_before_reading(request_id):
    connection = _Connection()
    with pytest.raises(ValueError, match="registry_request_identity_invalid"):
        await queue.read_registry_publication_request(connection, request_id)
    connection.fetchrow.assert_not_awaited()


def _enqueue_command(kind):
    if kind == "reactivation":
        return queue.RegistryReactivationCommand(1, 4, 2, "Restore reviewed generation", "reviewed-operation")
    return RegistryApprovalCommand(
        3,
        2,
        ({"record_kind": "membership", "record_id": 1, "revision": 1},),
        "Publish reviewed membership",
        "reviewed-operation",
    )


async def _enqueue(connection, kind, command, session_hash="a" * 64):
    actor = RegistryActor("platform_admin", UUID(int=1), "system")
    if kind == "reactivation":
        return await queue.enqueue_registry_reactivation(
            connection, command, actor, session_token_sha256=session_hash, control_schema="registry"
        )
    return await queue.enqueue_registry_publication(
        connection,
        command,
        actor,
        session_token_sha256=session_hash,
        expected_serving_generation=4,
        control_schema="registry",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["approval", "reactivation"])
@pytest.mark.parametrize("fault", ["transaction", "session"])
async def test_enqueue_requires_transaction_and_exact_session_commitment_before_any_sql(kind, fault):
    connection = _Connection(in_transaction=fault != "transaction")
    with pytest.raises(ValueError, match="requires_caller_transaction|session_hash_invalid"):
        await _enqueue(connection, kind, _enqueue_command(kind), None if fault == "session" else "a" * 64)
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_publication_selection_is_bounded_before_canonical_sql_or_queue_write():
    command = replace(
        _enqueue_command("approval"),
        selection=tuple({"record_kind": "membership", "record_id": index + 1, "revision": 1} for index in range(101)),
    )
    connection = _Connection()
    with pytest.raises(ValueError, match="queue_selection_invalid"):
        await _enqueue(connection, "approval", command)
    connection.fetchrow.assert_not_awaited()


class _EnqueueRace:
    """Model the same-key insertion winner and transaction cleanup for each outcome."""

    def __init__(self, kind, outcome):
        self.kind = kind
        self.outcome = outcome
        self.events = []
        self.lookups = 0
        self.inserted_by_field = None
        self.failure = {
            "sql_failure": asyncpg.SerializationError("synthetic concurrent write"),
            "cancelled": asyncio.CancelledError(),
            "timeout": TimeoutError("synthetic database timeout"),
        }.get(outcome)

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")

    async def fetchrow(self, sql, *arguments):
        if "jsonb_array_elements" in sql:
            return {"valid": True, "unique_records": True, "selection_json": arguments[0]}
        if "WHERE actor_key=$1 AND idempotency_key=$2" in sql:
            self.lookups += 1
            if self.lookups == 1 or self.outcome == "missing_winner":
                return None
            return {
                **self.inserted_by_field,
                "request_sha256": (
                    "b" * 64 if self.outcome == "different_request" else self.inserted_by_field["request_sha256"]
                ),
            }
        if "registry_revision_control" in sql:
            head = 5 if self.outcome == "head_changed" else 4
            return (2, head) if self.kind == "reactivation" else (3, 2, head)
        assert "INSERT INTO" in sql
        if self.failure is not None:
            raise self.failure
        self.inserted_by_field = {
            "request_id": UUID(int=99),
            "state": "queued",
            "created_at": datetime(2026, 1, 1, tzinfo=UTC),
            "request_sha256": arguments[-1],
        }
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["approval", "reactivation"])
@pytest.mark.parametrize(
    "outcome",
    ["same_request", "different_request", "missing_winner", "head_changed", "sql_failure", "cancelled", "timeout"],
)
async def test_enqueue_race_preserves_exact_replay_head_check_and_transaction_failure(kind, outcome):
    command = _enqueue_command(kind)
    before = asdict(command)
    race = _EnqueueRace(kind, outcome)
    failure = race.failure
    connection = _Connection()
    connection.transaction = race.transaction
    connection.fetchrow.side_effect = race.fetchrow
    if outcome == "same_request":
        receipt = await _enqueue(connection, kind, command)
        assert receipt == {
            "request_id": str(UUID(int=99)),
            "state": "queued",
            "created_at": "2026-01-01T00:00:00+00:00",
            "replayed": True,
        }
        assert race.events == ["begin", "commit"] and race.lookups == 2
    else:
        expected = type(failure) if failure is not None else RegistryApprovalConflict
        with pytest.raises(expected) as rejected:
            await _enqueue(connection, kind, command)
        if failure is not None:
            assert rejected.value is failure
        else:
            assert str(rejected.value) == (
                "registry_queue_revision_conflict"
                if outcome == "head_changed"
                else "registry_queue_idempotency_conflict"
            )
        assert race.events == ["begin", "rollback"]
        assert race.lookups == (2 if outcome in {"different_request", "missing_winner"} else 1)
    assert asdict(command) == before
