# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit queue families fail closed before a protected claim can run."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import registry_publication_queue as queue
from process import registry_source_binding_publication as source_batch
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
                "publish_source_binding_batch",
                "approve_source_binding_batch",
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


def _source_batch_command(**changes):
    return {
        "operation": source_batch.OPERATION,
        "batch_references": [{"idempotency_key": "draft-one", "request_sha256": "b" * 64, "custom_revision": 3}],
        "source_coordinates": {
            "source_system": "fhir",
            "source_id": "sample-source",
            "dataset_schema": "sample_data",
            "dataset_id": "dataset-one",
            "producer_id": "registry-source-fhir-witness-v1",
            "edition_id": str(UUID(int=12)),
        },
        "metadata_generation": "a" * 64,
        "selection_count": None,
        "selection_sha256": None,
        "expected_draft_revision": 3,
        "expected_approved_revision": 0,
        "expected_serving_generation": 1,
        "reason": "Review the complete source",
        "idempotency_key": "publish-source",
        **changes,
    }


def _source_batch_request(count=2602, *, operation=source_batch.OPERATION):
    selected, digest = source_batch.selection_identity(
        [{"record_kind": "network", "record_id": number + 1, "revision": 1} for number in range(count)]
    )
    compact = _source_batch_command(operation=operation, selection_count=count, selection_sha256=digest)
    stored = {key: value for key, value in compact.items() if key != "idempotency_key"} | {"selection": selected}
    actor_by_name = {
        "kind": "platform_admin",
        "user_id": str(UUID(int=22)),
        "client_id": "system",
        "impersonator_id": None,
    }
    metadata = queue.publication_request_metadata(stored, actor_by_name, "c" * 64, compact["idempotency_key"])
    return {**metadata, "idempotency_key": compact["idempotency_key"], "session_token_sha256": "c" * 64}


def _source_approval_result(compact, command):
    return {
        "operation": source_batch.APPROVAL_OPERATION,
        "approved_revision": command.expected_draft_revision + 1,
        "previous_approved_revision": command.expected_approved_revision,
        "selected_count": len(command.selection),
        "selection_sha256": compact["selection_sha256"],
        "serving_generation": compact["expected_serving_generation"],
        "policy_revision": 7,
    }


@pytest.mark.parametrize("damage", [None, "count", "digest", "head", "revision", "policy", "operation", "extra"])
def test_source_approval_result_is_closed_and_preserves_the_serving_head(damage):
    compact, command = source_batch.stored_source_batch(
        _source_batch_request(101, operation=source_batch.APPROVAL_OPERATION)
    )
    result_by_field = _source_approval_result(compact, command)
    changes_by_case = {
        "count": {"selected_count": 100},
        "digest": {"selection_sha256": "0" * 64},
        "head": {"serving_generation": 2},
        "revision": {"approved_revision": 5},
        "policy": {"policy_revision": True},
        "operation": {"operation": source_batch.OPERATION},
        "extra": {"source_selection": {"mapped_rows": 1, "omitted_rows": 0}},
    }
    result_by_field.update(changes_by_case.get(damage, {}))
    if damage is None:
        queue._source_batch_result(result_by_field, compact, command, "completed")
        queue._source_batch_result(None, compact, command, "queued")
    else:
        with pytest.raises(ValueError):
            queue._source_batch_result(result_by_field, compact, command, "completed")


@pytest.mark.asyncio
async def test_source_approval_uses_existing_complete_authority_pages():
    from process.registry_publication_execution import _fresh_authority

    request = _source_batch_request(201, operation=source_batch.APPROVAL_OPERATION)
    calls = []

    async def authorize(**arguments):
        calls.extend(arguments["selection"])
        return {
            "authorized": True,
            "actor": arguments["actor"],
            "policy_revision": 7,
            "selection_sha256": hashlib.sha256(
                json.dumps(arguments["selection"], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }

    assert await _fresh_authority(authorize, request) == 7
    compact, command = source_batch.stored_source_batch(request)
    assert calls == list(command.selection)
    assert source_batch.source_batch_page(compact, command, 0)["operation"] == source_batch.APPROVAL_OPERATION


@pytest.mark.parametrize(
    "change",
    [
        {"selection_count": 5001, "selection_sha256": "d" * 64},
        {"selection_count": True, "selection_sha256": "d" * 64},
        {"selection_count": 1},
        {"operation": "other"},
        {"extra": 1},
        {"batch_references": []},
        {"batch_references": [{"idempotency_key": "draft", "request_sha256": "b" * 64, "custom_revision": 4}]},
    ],
)
def test_source_batch_reference_refuses_invalid_or_unbounded_commands(change):
    with pytest.raises(ValueError):
        source_batch.validated_source_batch_command(_source_batch_command(**change), allow_unbound=True)


def test_source_batch_reference_keeps_draft_chunks_ordered_and_old_selection_cap():
    compact = _source_batch_command()
    first = compact["batch_references"][0]
    compact["batch_references"] = [
        dict(first, custom_revision=2),
        dict(first, idempotency_key="draft-two", request_sha256="c" * 64),
    ]
    assert source_batch.validated_source_batch_command(compact, allow_unbound=True) == compact
    for references in (list(reversed(compact["batch_references"])), [first, first]):
        with pytest.raises(ValueError):
            source_batch.validated_source_batch_command(dict(compact, batch_references=references), allow_unbound=True)


@pytest.mark.asyncio
async def test_source_batch_authorizes_complete_vector_in_existing_hundred_record_pages():
    request = _source_batch_request()
    calls = []

    async def authority(**arguments):
        page = arguments["selection"]
        assert len(page) <= 100 and arguments["session_token_sha256"] == "c" * 64
        calls.extend(page)
        return {
            "authorized": True,
            "actor": arguments["actor"],
            "policy_revision": 7,
            "selection_sha256": hashlib.sha256(
                json.dumps(page, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }

    assert await source_batch.authorize_source_batch(authority, request) == 7
    compact, command = source_batch.stored_source_batch(request)
    assert calls == list(command.selection) and len(calls) == compact["selection_count"] == 2602
    pages = [source_batch.source_batch_page(compact, command, offset) for offset in range(0, 2602, 100)]
    assert len(pages) == 27 and len(pages[-1]["selection"]) == 2 and pages[-1]["next_offset"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["denied", "digest", "policy", "timeout"])
@pytest.mark.parametrize("operation", source_batch.OPERATIONS)
async def test_source_batch_late_authority_failure_never_returns_partial_acceptance(damage, operation):
    calls = []

    async def authority(**arguments):
        calls.append(arguments["selection"])
        page = arguments["selection"]
        if len(calls) == 2 and damage == "timeout":
            raise TimeoutError
        return {
            "authorized": len(calls) != 2 or damage != "denied",
            "actor": arguments["actor"],
            "policy_revision": 8 if len(calls) == 2 and damage == "policy" else 7,
            "selection_sha256": "0" * 64
            if len(calls) == 2 and damage == "digest"
            else hashlib.sha256(json.dumps(page, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        }

    with pytest.raises((ValueError, TimeoutError)):
        await source_batch.authorize_source_batch(authority, _source_batch_request(201, operation=operation))
    assert len(calls) == 2


def test_source_batch_stored_reference_and_selection_are_both_request_bound():
    request = _source_batch_request(101)
    compact, command = source_batch.stored_source_batch(request)
    assert len(command.selection) == compact["selection_count"] == 101
    for field in ("request_sha256", "actor_key", "session_token_sha256"):
        with pytest.raises(ValueError):
            source_batch.stored_source_batch(dict(request, **{field: "0" * 64}))
    stored = json.loads(request["command_json"])
    stored["selection"][0]["revision"] = 2
    with pytest.raises(ValueError):
        source_batch.stored_source_batch(dict(request, command_json=json.dumps(stored)))


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


def test_source_batch_references_require_distinct_request_hashes():
    compact = _source_batch_command()
    first = compact["batch_references"][0]
    compact["batch_references"] = [
        dict(first, custom_revision=2),
        dict(first, idempotency_key="draft-two"),
    ]
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        source_batch.validated_source_batch_command(compact, allow_unbound=True)


@pytest.mark.parametrize(
    "change",
    [
        {"expected_draft_revision": True},
        {"expected_approved_revision": 4},
        {"expected_serving_generation": -1},
        {"selection_count": 0, "selection_sha256": "a" * 64},
        {"selection_count": 1, "selection_sha256": "A" * 64},
        {"selection_count": None, "selection_sha256": "a" * 64},
        {"metadata_generation": "not-a-digest"},
        {"reason": " trailing "},
        {"batch_references": None},
    ],
)
def test_source_batch_scalar_guards_refuse_malformed_compact_references(change):
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        source_batch.validated_source_batch_command(_source_batch_command(**change), allow_unbound=True)


@pytest.mark.parametrize(
    "selected",
    [
        [],
        [{"record_kind": "network", "record_id": number + 1, "revision": 1} for number in range(5001)],
        [{"record_kind": "network", "record_id": True, "revision": 1}],
        [{"record_kind": "network", "record_id": 1, "revision": True}],
        [{"record_kind": "provider", "record_id": str(UUID(int=1)), "revision": 1}],
        [{"record_kind": "network_binding", "record_id": str(UUID(int=0)), "revision": 1}],
        [{"record_kind": "network", "record_id": 1, "revision": 1}] * 2,
    ],
)
def test_source_batch_selection_rejects_invalid_keys_duplicates_and_count(selected):
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        source_batch.selection_identity(selected)


@pytest.mark.parametrize("offset", [None, True, -1, 1, 99, 101, 5000])
def test_source_batch_page_rejects_noncanonical_or_outside_offsets(offset):
    compact, command = source_batch.stored_source_batch(_source_batch_request(101))
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        source_batch.source_batch_page(compact, command, offset)


@pytest.mark.parametrize(
    "field,value",
    [
        ("selection_count", 100),
        ("selection_sha256", "a" * 64),
        ("metadata_generation", "d" * 64),
        ("expected_draft_revision", 4),
    ],
)
def test_source_batch_stored_scalar_changes_cannot_reuse_request_hash(field, value):
    request = _source_batch_request(101)
    stored = json.loads(request["command_json"])
    stored[field] = value
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        source_batch.stored_source_batch(dict(request, command_json=json.dumps(stored)))


@pytest.mark.asyncio
async def test_source_batch_authority_pages_share_one_real_deadline(monkeypatch):
    original_timeout = asyncio.timeout
    requested_budgets, page_lengths = [], []
    cancelled_pages = []

    def short_timeout(seconds):
        requested_budgets.append(seconds)
        return original_timeout(0.05)

    async def authority(**arguments):
        page = arguments["selection"]
        page_lengths.append(len(page))
        if len(page_lengths) == 2:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled_pages.append(len(page_lengths))
        return {
            "authorized": True,
            "actor": arguments["actor"],
            "policy_revision": 7,
            "selection_sha256": hashlib.sha256(
                json.dumps(page, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }

    monkeypatch.setattr(source_batch.asyncio, "timeout", short_timeout)
    with pytest.raises(TimeoutError):
        await source_batch.authorize_source_batch(authority, _source_batch_request(201))
    assert requested_budgets == [15] and page_lengths == [100, 100] and cancelled_pages == [2]


@pytest.mark.parametrize(
    "case",
    [
        "preview",
        "selection",
        "request-id",
        "queue",
        "actor",
        "hash",
        "query",
        "queue-offset",
        "request-extra",
        "unknown-field",
        "queue-unbound",
    ],
)
def test_source_batch_http_parser_keeps_command_actor_and_status_modes_closed(case):
    from api.endpoint.registry_publication import _source_batch_body

    body_by_field = _source_batch_command()
    actor_by_field = {"kind": "platform_admin", "user_id": str(UUID(int=22)), "client_id": "system"}
    body_by_field.update(actor=actor_by_field, session_token_sha256="c" * 64)
    operation = "preview"
    if case in {"selection", "request-id", "request-extra"}:
        operation = "selection"
        body_by_field["offset"] = 100
    if case in {"request-id", "request-extra"}:
        body_by_field = {
            "request_id": str(UUID(int=23)),
            "offset": 0,
            "actor": actor_by_field,
            "session_token_sha256": "c" * 64,
        }
    if case in {"queue", "queue-offset", "queue-unbound"}:
        operation = "queue"
        body_by_field.update(selection_count=1, selection_sha256="d" * 64)
    changes_by_case = {
        "actor": {"actor": {**actor_by_field, "kind": "service"}},
        "hash": {"session_token_sha256": "C" * 64},
        "queue-offset": {"offset": 0},
        "request-extra": {"extra": 1},
        "unknown-field": {"selection": []},
        "queue-unbound": {"selection_count": None, "selection_sha256": None},
    }
    body_by_field.update(changes_by_case.get(case, {}))
    request = SimpleNamespace(
        body=json.dumps(body_by_field).encode(), args={}, query_string="unexpected=1" if case == "query" else ""
    )
    if case not in {"preview", "selection", "request-id", "queue"}:
        with pytest.raises(ValueError):
            _source_batch_body(request, operation)
        return
    compact, actor, session_hash, offset = _source_batch_body(request, operation)
    assert actor.kind == "platform_admin" and actor.user_id == UUID(int=22) and session_hash == "c" * 64
    assert offset == (100 if case == "selection" else 0)
    if case == "request-id":
        assert compact == {"request_id": str(UUID(int=23))}
    else:
        assert compact["operation"] == source_batch.OPERATION and "actor" not in compact


@pytest.mark.parametrize(
    "case",
    [
        "completed",
        "queued",
        "rejected",
        "count",
        "previous",
        "generation",
        "extra",
        "counter-negative",
        "counter-total",
        "pending-result",
        "policy-type",
    ],
)
def test_source_batch_status_result_keeps_completion_heads_and_counters_bound(case):
    compact, command = source_batch.stored_source_batch(_source_batch_request(101))
    result_by_field = {
        "approved_revision": 4,
        "previous_approved_revision": 0,
        "selected_count": 101,
        "serving_generation": 2,
        "policy_revision": 7,
        "source_selection": {"mapped_rows": 1, "omitted_rows": 0},
    }
    changes_by_case = {
        "count": {"selected_count": 100},
        "previous": {"previous_approved_revision": 1},
        "generation": {"serving_generation": 0},
        "extra": {"selection": []},
        "counter-negative": {"source_selection": {"mapped_rows": -1, "omitted_rows": 0}},
        "counter-total": {"source_selection": {"mapped_rows": 2**53 - 1, "omitted_rows": 1}},
        "policy-type": {"policy_revision": True},
    }
    result_by_field.update(changes_by_case.get(case, {}))
    state = "completed"
    if case == "queued":
        state, result_by_field = "queued", None
    if case == "rejected":
        state, result_by_field = "rejected", {"error": {"code": "registry_publication_rejected"}}
    if case == "pending-result":
        state = "running"
    if case in {"completed", "queued", "rejected"}:
        queue._source_batch_result(result_by_field, compact, command, state)
    else:
        with pytest.raises(ValueError):
            queue._source_batch_result(result_by_field, compact, command, state)


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", [0, 1])
async def test_source_batch_receipt_budget_refuses_before_history_fetch(remaining):
    compact = _source_batch_command()
    reference = compact["batch_references"][0]
    records = [
        {"record_kind": "network_binding", "record_id": str(UUID(int=number)), "revision": 1} for number in (1, 2)
    ]
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "request_sha256": reference["request_sha256"],
                "receipt": json.dumps({"custom_revision": 3, "records": records}),
            }
        ),
        fetch=AsyncMock(),
    )
    with pytest.raises(ValueError, match="registry_source_batch_invalid"):
        await source_batch._one_batch_history(
            connection,
            '"registry"',
            compact,
            RegistryActor("platform_admin", UUID(int=22), "system"),
            reference,
            remaining_records=remaining,
        )
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate", [False, True])
async def test_source_batch_history_applies_remaining_budget_to_each_reference(monkeypatch, duplicate):
    compact = _source_batch_command(
        batch_references=[
            {"idempotency_key": "first", "request_sha256": "a" * 64, "custom_revision": 2},
            {"idempotency_key": "second", "request_sha256": "b" * 64, "custom_revision": 3},
        ]
    )
    first_records = [{"binding_id": str(UUID(int=number))} for number in (1, 2)]
    second_records = [{"binding_id": str(UUID(int=1 if duplicate else 3))}]
    load = AsyncMock(side_effect=[first_records, second_records])
    monkeypatch.setattr(source_batch, "_one_batch_history", load)
    arguments = (object(), '"registry"', compact, RegistryActor("platform_admin", UUID(int=22), "system"))
    if duplicate:
        with pytest.raises(ValueError, match="registry_source_batch_invalid"):
            await source_batch._binding_history(*arguments)
    else:
        assert await source_batch._binding_history(*arguments) == first_records + second_records
    assert [call.kwargs["remaining_records"] for call in load.await_args_list] == [5000, 4998]


class _BatchHttpSession:
    def __init__(self, driver):
        self.events = []
        self.active = False
        self.execute = AsyncMock()
        self.connection = AsyncMock(
            return_value=SimpleNamespace(
                get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver))
            )
        )

    @asynccontextmanager
    async def begin(self):
        assert not self.active
        self.active = True
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.active = False


@pytest.fixture
def source_batch_http(monkeypatch):
    from api.endpoint import registry_publication as publication

    monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", "synthetic-registry-token")
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    compact, command = source_batch.stored_source_batch(_source_batch_request(101))
    driver = _Connection()
    driver.fetchval.return_value = compact["expected_serving_generation"]
    session = _BatchHttpSession(driver)
    actor = RegistryActor("platform_admin", UUID(int=22), "system")
    body_by_field = {
        **compact,
        "actor": {**asdict(actor), "user_id": str(actor.user_id)},
        "session_token_sha256": "c" * 64,
    }
    request = SimpleNamespace(
        body=json.dumps(body_by_field).encode(),
        args={},
        query_string="",
        headers={"Authorization": "Bearer synthetic-registry-token"},
        ctx=SimpleNamespace(sa_session=session),
        app=SimpleNamespace(ctx=SimpleNamespace()),
    )
    resolve = AsyncMock(return_value=(compact, command))
    enqueue = AsyncMock(return_value={"request_id": str(UUID(int=23)), "state": "queued", "replayed": False})
    monkeypatch.setattr(source_batch, "resolve_source_batch", resolve)
    monkeypatch.setattr(source_batch, "enqueue_source_batch", enqueue)
    return SimpleNamespace(
        endpoint=publication,
        request=request,
        body_by_field=body_by_field,
        compact=compact,
        command=command,
        driver=driver,
        session=session,
        actor=actor,
        resolve=resolve,
        enqueue=enqueue,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("stored", [False, True])
async def test_source_batch_endpoint_selection_reads_only_the_bound_page(source_batch_http, stored):
    fixture = source_batch_http
    body_by_field = {**fixture.body_by_field, "offset": 100}
    if stored:
        body_by_field = {key: body_by_field[key] for key in ("actor", "session_token_sha256", "offset")}
        body_by_field["request_id"] = str(UUID(int=23))
        fixture.driver.fetchrow.return_value = _source_batch_request(101)
    fixture.request.body = json.dumps(body_by_field).encode()
    response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "selection")
    assert response_message.status == 200 and response_message.headers["Cache-Control"] == "private, no-store"
    assert json.loads(response_message.body) == source_batch.source_batch_page(fixture.compact, fixture.command, 100)
    assert fixture.session.events == ["begin", "commit"] and not fixture.session.active
    assert [str(call.args[0]) for call in fixture.session.execute.await_args_list] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET LOCAL lock_timeout='1s'",
    ]
    fixture.enqueue.assert_not_awaited()
    if stored:
        fixture.resolve.assert_not_awaited()
        assert fixture.driver.fetchrow.await_args.args[1:] == (UUID(int=23),)
    else:
        fixture.resolve.assert_awaited_once_with(fixture.driver, fixture.compact, fixture.actor, allow_unbound=True)
        fixture.driver.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", source_batch.OPERATIONS)
async def test_source_batch_endpoint_queue_owns_one_transaction_and_exact_session(source_batch_http, operation):
    fixture = source_batch_http
    fixture.compact["operation"] = fixture.body_by_field["operation"] = operation
    fixture.request.body = json.dumps(fixture.body_by_field).encode()

    async def enqueue(driver, document, actor, session_hash):
        assert fixture.session.active and driver is fixture.driver
        assert document == fixture.compact and actor == fixture.actor and session_hash == "c" * 64
        return {"request_id": str(UUID(int=23)), "state": "queued", "replayed": False}

    fixture.enqueue.side_effect = enqueue
    response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "queue")
    assert response_message.status == 200 and response_message.headers["Cache-Control"] == "private, no-store"
    assert json.loads(response_message.body) == {"request_id": str(UUID(int=23)), "state": "queued", "replayed": False}
    assert fixture.session.events == ["begin", "commit"] and not fixture.session.active
    assert [str(call.args[0]) for call in fixture.session.execute.await_args_list] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET LOCAL lock_timeout='1s'",
    ]
    fixture.enqueue.assert_awaited_once()
    fixture.resolve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["selection", "preview", "queue"])
@pytest.mark.parametrize("damage", ["auth", "operation", "body", "actor", "duplicate", "query", "unbound-queue"])
async def test_source_batch_endpoint_rejects_before_database_or_queue(source_batch_http, operation, damage):
    from sanic.exceptions import Forbidden

    fixture = source_batch_http
    body_by_case = {
        "body": b"[]",
        "actor": json.dumps({**fixture.body_by_field, "actor": {"kind": "platform_admin"}}).encode(),
        "duplicate": fixture.request.body[:-1] + b',"actor":{}}',
        "unbound-queue": json.dumps(
            {**fixture.body_by_field, "selection_count": None, "selection_sha256": None}
        ).encode(),
    }
    fixture.request.body = body_by_case.get(damage, fixture.request.body)
    fixture.request.query_string = "extra=1" if damage == "query" else ""
    fixture.request.headers = {} if damage == "auth" else fixture.request.headers
    operation = {"operation": "unknown", "unbound-queue": "queue"}.get(damage, operation)
    if damage == "auth":
        with pytest.raises(Forbidden):
            await fixture.endpoint.request_source_batch_publication(fixture.request, operation)
    else:
        response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, operation)
        assert response_message.status == 400 and json.loads(response_message.body) == {
            "error": {"code": "registry_request_invalid"}
        }
        assert response_message.headers["Cache-Control"] == "private, no-store"
    assert fixture.session.events == []
    fixture.session.execute.assert_not_awaited()
    fixture.resolve.assert_not_awaited()
    fixture.enqueue.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,status",
    [
        (RegistryApprovalConflict("synthetic private conflict"), 409),
        (ValueError("synthetic private value"), 400),
        (TypeError("synthetic private type"), 400),
        (KeyError("synthetic private key"), 400),
        (asyncpg.PostgresError("synthetic private database"), 503),
    ],
)
async def test_source_batch_endpoint_queue_failure_rolls_back_and_sanitizes(source_batch_http, error, status):
    fixture = source_batch_http
    fixture.enqueue.side_effect = error
    response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "queue")
    assert response_message.status == status and response_message.headers["Cache-Control"] == "private, no-store"
    assert b"synthetic private" not in response_message.body
    assert fixture.session.events == ["begin", "rollback"] and not fixture.session.active
    fixture.enqueue.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_batch_endpoint_cancelled_queue_rolls_back_without_receipt(source_batch_http):
    fixture = source_batch_http
    fixture.enqueue.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await fixture.endpoint.request_source_batch_publication(fixture.request, "queue")
    assert fixture.session.events == ["begin", "rollback"] and not fixture.session.active
    fixture.enqueue.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("current_head", [1, 2, None])
async def test_source_batch_endpoint_fallback_preview_is_summary_only_and_head_bound(
    monkeypatch, source_batch_http, current_head
):
    fixture = source_batch_http
    fixture.driver.fetchval.return_value = current_head
    preview = AsyncMock(return_value={"selected_count": 101, "records": []})
    monkeypatch.setattr(fixture.endpoint, "preview_registry_approval", preview)
    response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "preview")
    if current_head == 1:
        assert response_message.status == 200 and json.loads(response_message.body) == {
            "selected_count": 101,
            "records": [],
            "operation": source_batch.OPERATION,
            "selection_count": 101,
            "selection_sha256": fixture.compact["selection_sha256"],
        }
        preview.assert_awaited_once_with(
            fixture.driver, fixture.command, fixture.actor, office_authority=None, summary_only=True
        )
        assert fixture.session.events == ["begin", "commit", "begin", "commit"]
    else:
        assert response_message.status == 409
        preview.assert_not_awaited()
        assert fixture.session.events == ["begin", "commit", "begin", "rollback"]
    assert response_message.headers["Cache-Control"] == "private, no-store"
    fixture.enqueue.assert_not_awaited()
    fixture.resolve.assert_awaited_once_with(fixture.driver, fixture.compact, fixture.actor, allow_unbound=True)
    assert not fixture.session.active


@pytest.mark.asyncio
async def test_source_batch_endpoint_configured_summary_uses_fenced_source_session(monkeypatch, source_batch_http):
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from process.registry_ptg_scope_engine import RegistryPTGScopeEngineService
    from tests.test_registry_composition_session_bridge import _bridge_owner

    fixture = source_batch_http
    sessions, driver, events = _bridge_owner(monkeypatch)
    identity_by_field = {
        "database_name": "synthetic",
        "database_oid": 1,
        "control_oid": 2,
        "current_role": "api",
        "session_role": "api",
    }
    driver.fetchrow = AsyncMock(
        side_effect=[identity_by_field, {**identity_by_field, "current_role": "synthetic", "session_role": "synthetic"}]
    )
    driver.fetchval = AsyncMock(return_value=1)
    service = object.__new__(RegistryPTGScopeEngineService)
    object.__setattr__(service, "reader_sessions", sessions)
    object.__setattr__(service, "store", RegistryPTGProducerScopeStore("owner", "approver", "synthetic_control"))
    fixture.request.app.ctx.registry_ptg_scope_engine = service
    monkeypatch.setattr(fixture.endpoint, "_namespace", lambda _: '"synthetic_control"')
    monkeypatch.setattr(fixture.endpoint, "_driver", AsyncMock(return_value=driver))

    async def preview(connection, command, actor, *, office_authority, summary_only):
        assert connection is driver and command is fixture.command and actor == fixture.actor
        assert summary_only is True and type(office_authority[0]) is AsyncSession
        assert office_authority[0].in_transaction() and office_authority[1] is service.store
        assert events.index("lock") < events.index("native_fence")
        return {"selected_count": 101, "records": []}

    observed = AsyncMock(side_effect=preview)
    monkeypatch.setattr(fixture.endpoint, "preview_registry_approval", observed)
    response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "preview")
    assert response_message.status == 200 and json.loads(response_message.body)["selection_count"] == 101
    assert json.loads(response_message.body)["records"] == []
    observed.assert_awaited_once()
    fixture.enqueue.assert_not_awaited()
    assert fixture.session.events == ["begin", "commit", "begin", "commit"]
    assert events[-1] == "close" and events.count("checkout") == events.count("unlock") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unavailable", "sqlalchemy", "timeout", "value", "type", "conflict", "cancel"])
async def test_source_batch_endpoint_preview_failure_is_sanitized_or_cancelled(monkeypatch, source_batch_http, failure):
    from sqlalchemy.exc import SQLAlchemyError

    fixture = source_batch_http
    errors_by_case = {
        "unavailable": fixture.endpoint.RegistryImportedSelectionUnavailable("synthetic private failure"),
        "sqlalchemy": SQLAlchemyError("synthetic private failure"),
        "timeout": TimeoutError("synthetic private failure"),
        "value": ValueError("synthetic private failure"),
        "type": TypeError("synthetic private failure"),
        "conflict": RegistryApprovalConflict("synthetic private failure"),
        "cancel": asyncio.CancelledError(),
    }
    fixture.request.app.ctx.registry_ptg_scope_engine = object()
    preview = AsyncMock(side_effect=errors_by_case[failure])
    monkeypatch.setattr(fixture.endpoint, "_configured_preview", preview)
    if failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await fixture.endpoint.request_source_batch_publication(fixture.request, "preview")
    else:
        response_message = await fixture.endpoint.request_source_batch_publication(fixture.request, "preview")
        assert response_message.status == (409 if failure == "conflict" else 503)
        assert b"synthetic private" not in response_message.body
        assert response_message.headers["Cache-Control"] == "private, no-store"
    fixture.enqueue.assert_not_awaited()
    assert fixture.session.events == ["begin", "commit"] and not fixture.session.active
