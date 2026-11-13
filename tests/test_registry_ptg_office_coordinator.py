# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable reservation, exact lease ordering and lost acknowledgments without sockets."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_office_coordinator as coordinator
from process.registry_ptg_office_intent import _digest
from process.registry_ptg_scope_engine import RegistryPTGScopeDeadlineExpired
from tests.test_registry_ptg_office_approval import _command as office_command
from tests.test_registry_ptg_office_intent import Connection, selector, source_command

IDENTITY = (42, "123456789")


def envelope():
    return {
        "actor": {
            "kind": "platform_admin",
            "user_id": "11111111-1111-4111-8111-111111111111",
            "client_id": "system",
        },
        "session_token_sha256": "b" * 64,
        "source_command": source_command(),
        "selector": selector(),
        "read_limits": {
            "maximum_bytes": 1048576,
            "maximum_pages": 32,
            "maximum_coordinates": 100,
        },
    }


class QueueConnection:
    def __init__(self, storage, events):
        self.storage, self.events = storage, events
        self.expired = False
        self.finish_error = False
        self.claim_sql = None
        self.commits = 0
        self.lose_commit_on = None

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.events.append("queue_begin")
        yield
        self.events.append("queue_commit")
        self.commits += 1
        if self.commits == self.lose_commit_on:
            raise TimeoutError

    def is_in_transaction(self):
        return True

    async def execute(self, sql):
        assert sql == "SET LOCAL search_path TO pg_catalog,pg_temp"
        self.events.append("canonical_path")

    async def fetchrow(self, sql, request_id):
        record = next(
            (item for item in self.storage.requests_by_key.values() if item["request_id"] == request_id),
            None,
        )
        return None if record is None else {"result_json": None, **copy.deepcopy(record)}

    async def fetchval(self, sql, *args):
        request_record = next(
            stored_request
            for stored_request in self.storage.requests_by_key.values()
            if stored_request["request_id"] == args[0]
        )
        if "SET lease_expires_at=clock_timestamp()" in sql:
            assert request_record["state"] == "running" and request_record["lease_id"] == args[1]
            assert "WHERE request_id=$1 AND lease_id=$2 AND state='running'" in sql
            self.expired = True
            self.events.append("pending_lease_yield")
            return args[1]
        if "SET state='running'" in sql:
            self.claim_sql = sql
            self.events.append("exact_claim")
            assert args[2] == request_record["request_sha256"] and json.loads(args[3]) == json.loads(
                request_record["command_json"]
            )
            assert args[4:7] == (
                coordinator.OPERATION,
                str(coordinator.VERSION),
                json.loads(request_record["command_json"])["semantic_sha256"],
            )
            if request_record["state"] == "running" and not self.expired:
                return None
            request_record.update(state="running", lease_id=args[1])
            self.expired = False
            return args[1]
        self.events.append("finish")
        if self.finish_error:
            raise TimeoutError
        assert request_record["state"] == "running" and request_record["lease_id"] == args[1] and not self.expired
        request_record.update(state=args[2], result_json=json.loads(args[3]), lease_id=None)
        return args[0]


class QueuePool:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


class Session:
    def __init__(self, storage, events):
        self.storage, self.events = storage, events
        self.lose_commit = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    @asynccontextmanager
    async def begin(self):
        before = copy.deepcopy(self.storage.requests_by_key)
        try:
            yield
        except BaseException:
            self.storage.requests_by_key = before
            self.events.append("management_rollback")
            raise
        else:
            self.events.append("management_commit")
            if self.lose_commit:
                self.lose_commit = False
                raise TimeoutError

    async def execute(self, statement):
        self.events.append(str(statement))


def _physical_service(events, prepared_by_capture):
    preparation = SimpleNamespace(
        scope_service=SimpleNamespace(store=SimpleNamespace(control_schema="synthetic_control"))
    )

    async def physical(document, **kwargs):
        assert "management_commit" in events and events[-1] == "queue_commit"
        events.append("physical_prepare")
        selected = document["selector"]
        command = office_command()
        command.update(
            {
                name: selected[name]
                for name in (
                    "capture_id",
                    "scope_id",
                    "client_id",
                    "scope_approval_sha256",
                    "idempotency_key",
                    "reason",
                    "retained_generation_id",
                )
            }
        )
        command["source"]["source_scope"].update(
            scope_id=selected["scope_id"],
            approval_sha256=selected["scope_approval_sha256"],
        )
        prepared_by_field = {
            "capture_id": selected["capture_id"],
            "state": "prepared",
            "command": command,
            "command_sha256": _digest(command),
            "manifest_sha256": "d" * 64,
        }
        prepared_by_capture[selected["capture_id"]] = prepared_by_field
        return prepared_by_field

    async def read(document, **kwargs):
        events.append("genuine_reader_read")
        assert set(document["selector"]) == {
            "capture_id",
            "scope_id",
            "client_id",
            "scope_approval_sha256",
            "idempotency_key",
        }
        return copy.deepcopy(prepared_by_capture[document["selector"]["capture_id"]])

    preparation.prepare, preparation.read = (
        AsyncMock(side_effect=physical),
        AsyncMock(side_effect=read),
    )
    return preparation


def boundaries(monkeypatch):
    events, storage = [], Connection()
    session = Session(storage, events)
    queue = QueueConnection(storage, events)
    prepared_by_capture = {}
    preparation = _physical_service(events, prepared_by_capture)
    service = object.__new__(coordinator.RegistryPTGOfficeIntentCoordinator)
    for name, configured_component in dict(
        preparation=preparation,
        management_sessions=lambda: session,
        queue_pool=QueuePool(queue),
        management_role="synthetic_management",
        queue_role="synthetic_queue",
        owner_role="synthetic_owner",
    ).items():
        object.__setattr__(service, name, configured_component)

    async def source(*args):
        events.append("fresh_source_authority")
        return IDENTITY

    monkeypatch.setattr(coordinator, "_approved_source", AsyncMock(side_effect=source))
    monkeypatch.setattr(coordinator, "native_driver", AsyncMock(return_value=storage))
    return SimpleNamespace(
        service=service,
        events=events,
        storage=storage,
        session=session,
        queue=queue,
        preparation=preparation,
        prepared_by_capture=prepared_by_capture,
    )


@pytest.mark.asyncio
async def test_reserve_commit_and_exact_lease_commit_precede_physical_prepare(
    monkeypatch,
):
    fixture = boundaries(monkeypatch)
    reply = await fixture.service.prepare(envelope())
    assert reply["intent"]["state"] == "completed" and reply["preparation"]["state"] == "prepared"
    assert fixture.events.index("fresh_source_authority") < fixture.events.index("management_commit")
    assert (
        fixture.events.index("management_commit")
        < fixture.events.index("exact_claim")
        < fixture.events.index("physical_prepare")
        < fixture.events.index("finish")
    )
    assert any("READ COMMITTED" in event for event in fixture.events)
    assert "WHERE request_id=$1 AND request_sha256=$3 AND command_json=$4::jsonb" in fixture.queue.claim_sql
    assert "semantic_sha256" in fixture.queue.claim_sql and "version" in fixture.queue.claim_sql
    assert "SKIP LOCKED" not in fixture.queue.claim_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_call,is_unknown",
    [(1, False), (2, False), (3, False), (4, True), (5, True)],
)
async def test_fresh_authority_failure_rolls_back_or_preserves_original_intent(monkeypatch, failure_call, is_unknown):
    fixture = boundaries(monkeypatch)
    results = [IDENTITY] * 5
    results[failure_call - 1] = PermissionError("denied")
    monkeypatch.setattr(coordinator, "_approved_source", AsyncMock(side_effect=results))
    with pytest.raises(RegistryPTGScopeDeadlineExpired if is_unknown else PermissionError) as error:
        await fixture.service.prepare(envelope())
    if is_unknown:
        assert error.value.outcome_unknown and len(fixture.storage.requests_by_key) == 1
    else:
        assert not fixture.storage.requests_by_key
    assert "finish" not in fixture.events


@pytest.mark.asyncio
async def test_lost_reserve_ack_and_renewed_session_keep_original_audit_and_handles(
    monkeypatch,
):
    fixture = boundaries(monkeypatch)
    fixture.session.lose_commit = True
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as error:
        await fixture.service.prepare(envelope())
    assert error.value.outcome_unknown and not fixture.preparation.prepare.await_count
    original = copy.deepcopy(next(iter(fixture.storage.requests_by_key.values())))
    current = envelope()
    current["session_token_sha256"] = "c" * 64
    reply = await fixture.service.prepare(current)
    stored = next(iter(fixture.storage.requests_by_key.values()))
    assert len(fixture.storage.requests_by_key) == 1 and str(stored["request_id"]) == reply["intent"]["request_id"]
    for name in (
        "actor_json",
        "session_token_sha256",
        "command_json",
        "request_sha256",
        "created_at",
    ):
        assert stored[name] == original[name]
    assert fixture.preparation.prepare.await_args.args[0]["session_token_sha256"] == "c" * 64


@pytest.mark.asyncio
async def test_lost_physical_ack_pending_lease_then_same_capture_reclaim(monkeypatch):
    fixture = boundaries(monkeypatch)
    physical = fixture.preparation.prepare.side_effect

    async def lose_ack(document, **kwargs):
        await physical(document, **kwargs)
        raise TimeoutError

    fixture.preparation.prepare.side_effect = lose_ack
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as error:
        await fixture.service.prepare(envelope())
    assert error.value.outcome_unknown
    original = copy.deepcopy(next(iter(fixture.storage.requests_by_key.values())))
    pending = await fixture.service.prepare(envelope())
    assert pending["preparation"] is None and pending["intent"]["request_id"] == str(original["request_id"])
    assert fixture.preparation.prepare.await_count == 1
    fixture.queue.expired = True
    fixture.preparation.prepare.side_effect = physical
    reply = await fixture.service.prepare(envelope())
    assert reply["preparation"]["capture_id"] == json.loads(original["command_json"])["selector"]["capture_id"]
    assert len(fixture.storage.requests_by_key) == 1


@pytest.mark.asyncio
async def test_completed_replay_reads_native_capture_and_never_prepares_again(
    monkeypatch,
):
    fixture = boundaries(monkeypatch)
    first = await fixture.service.prepare(envelope())
    renewed = envelope()
    renewed["session_token_sha256"] = "c" * 64
    replay = await fixture.service.prepare(renewed)
    assert replay["intent"]["request_id"] == first["intent"]["request_id"]
    assert replay["preparation"] == first["preparation"]
    assert fixture.preparation.read.await_count == 1 and fixture.preparation.prepare.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing", "manifest", "malformed_result", "audit"])
async def test_completed_replay_refuses_missing_capture_or_stored_drift(monkeypatch, drift):
    fixture = boundaries(monkeypatch)
    first = await fixture.service.prepare(envelope())
    record = next(iter(fixture.storage.requests_by_key.values()))
    if drift == "missing":
        fixture.prepared_by_capture.clear()
    elif drift == "manifest":
        fixture.prepared_by_capture[first["preparation"]["capture_id"]]["manifest_sha256"] = "e" * 64
    elif drift == "malformed_result":
        record["result_json"] = None
    else:
        record["request_sha256"] = "e" * 64
    with pytest.raises(ValueError if drift == "audit" else RegistryPTGScopeDeadlineExpired):
        await fixture.service.prepare(envelope())
    assert fixture.preparation.prepare.await_count == 1


@pytest.mark.asyncio
async def test_lost_finish_ack_never_rejects_or_allocates_replacement(monkeypatch):
    fixture = boundaries(monkeypatch)
    fixture.queue.finish_error = True
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as error:
        await fixture.service.prepare(envelope())
    assert error.value.outcome_unknown
    record = next(iter(fixture.storage.requests_by_key.values()))
    assert record["state"] == "running" and len(fixture.storage.requests_by_key) == 1
    assert all("rejected" not in str(call) for call in fixture.storage.calls)


@pytest.mark.parametrize(
    "change",
    [
        {"capture_id": "11111111-1111-4111-8111-111111111111"},
        {"idempotency_key": "browser"},
        {"recipes": []},
    ],
)
def test_reservation_selector_never_accepts_browser_identity_or_recipes(change):
    document = envelope()
    document["selector"].update(change)
    with pytest.raises(ValueError):
        coordinator._envelope(document)


@pytest.mark.asyncio
async def test_lost_claim_commit_ack_keeps_one_pending_intent_without_physical_handoff(
    monkeypatch,
):
    fixture = boundaries(monkeypatch)
    fixture.queue.lose_commit_on = 1
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as error:
        await fixture.service.prepare(envelope())
    assert error.value.outcome_unknown and not fixture.preparation.prepare.await_count
    record = next(iter(fixture.storage.requests_by_key.values()))
    original_request = record["request_id"]
    reply = await fixture.service.prepare(envelope())
    assert reply["intent"]["request_id"] == str(original_request) and reply["preparation"] is None
    assert len(fixture.storage.requests_by_key) == 1 and not fixture.preparation.prepare.await_count


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [{"command": {}}, {"approval_sha256": "c" * 64}, {}])
async def test_reader_reconstructs_actual_approval_and_exact_source_before_authority(monkeypatch, change):
    document = envelope()
    authority = SimpleNamespace(authorize=AsyncMock())
    session = SimpleNamespace(execute=AsyncMock())
    scope = SimpleNamespace(reader_sessions=object(), store=object(), authority=authority)
    service = SimpleNamespace(preparation=SimpleNamespace(scope_service=scope), schema="synthetic_control")

    @asynccontextmanager
    async def reader(factory, *, control_schema):
        assert factory is scope.reader_sessions and control_schema == service.schema
        yield session

    stored_by_field = {
        "command": document["source_command"],
        "approval_sha256": document["selector"]["scope_approval_sha256"],
        **change,
    }
    read = AsyncMock(return_value=stored_by_field)
    monkeypatch.setattr(coordinator, "registry_company_approval_transaction", reader)
    monkeypatch.setattr(coordinator, "read_registry_ptg_published_plan_scope", read)
    monkeypatch.setattr(coordinator, "native_driver", AsyncMock(return_value=object()))
    if change:
        with pytest.raises(ValueError, match="source_changed"):
            await coordinator._approved_source(service, document, coordinator._deadline(None))
        assert not authority.authorize.await_count
    else:
        assert await coordinator._approved_source(service, document, coordinator._deadline(None)) == stored_by_field
        assert authority.authorize.await_args.args[0] == {
            "actor": document["actor"],
            "session_token_sha256": document["session_token_sha256"],
            "command": document["source_command"],
        }
    assert read.await_args.kwargs == {
        "scope_id": document["selector"]["scope_id"],
        "client_id": document["selector"]["client_id"],
        "approval_sha256": document["selector"]["scope_approval_sha256"],
        "store": scope.store,
    }


@pytest.mark.asyncio
async def test_unicode_reason_uses_physical_command_digest_on_prepare_and_replay(monkeypatch):
    from process.registry_ptg_office_capture import _digest as physical_digest

    fixture = boundaries(monkeypatch)
    physical = fixture.preparation.prepare.side_effect

    async def actual_wire_hash(document, **kwargs):
        reply = await physical(document, **kwargs)
        reply["command_sha256"] = physical_digest(reply["command"])
        return reply

    fixture.preparation.prepare.side_effect = actual_wire_hash
    request = envelope()
    request["selector"]["reason"] = "Reviewed Montréal office 🏥"
    first = await fixture.service.prepare(request)
    assert first["preparation"]["command_sha256"] == physical_digest(first["preparation"]["command"])
    assert first["preparation"]["command_sha256"] != _digest(first["preparation"]["command"])
    replay = await fixture.service.prepare(request)
    assert replay == first


@pytest.mark.asyncio
async def test_legacy_completion_replay_preserves_opaque_versions_and_original_lease(monkeypatch):
    fixture = boundaries(monkeypatch)
    original_prepare = fixture.preparation.prepare.side_effect

    async def prepare_legacy(document, **arguments):
        prepared = await original_prepare(document, **arguments)
        prepared["command"]["retained_serving"].pop("address_relation")
        prepared["command"]["retained_serving"]["address_table_oid"] = 17
        prepared["command_sha256"] = "9" * 64
        return prepared

    fixture.preparation.prepare.side_effect = prepare_legacy
    first = await fixture.service.prepare(envelope())
    replay = await fixture.service.prepare(envelope())
    assert first["intent"]["request_id"] == replay["intent"]["request_id"]
    assert first["preparation"]["command_sha256"] == replay["preparation"]["command_sha256"] == "9" * 64
    assert fixture.preparation.prepare.await_count == fixture.preparation.read.await_count == 1
    assert len(fixture.storage.requests_by_key) == 1


@pytest.mark.asyncio
async def test_pending_admin_freeze_yields_same_request_lease_without_completion(monkeypatch):
    fixture = boundaries(monkeypatch)
    prepare = fixture.preparation.prepare.side_effect

    async def pending(document, **kwargs):
        reply = await prepare(document, **kwargs)
        reply["state"] = "handed_off"
        return reply

    fixture.preparation.prepare.side_effect = pending
    first = await fixture.service.prepare(envelope())
    assert first["intent"]["state"] == "running" and first["preparation"]["state"] == "handed_off"
    assert fixture.queue.expired and "finish" not in fixture.events
    assert fixture.events.index("physical_prepare") < fixture.events.index("pending_lease_yield")
    request_id = first["intent"]["request_id"]
    fixture.preparation.prepare.side_effect = prepare
    completed = await fixture.service.prepare(envelope())
    assert completed["intent"]["state"] == "completed"
    assert completed["intent"]["request_id"] == request_id
    assert completed["preparation"]["capture_id"] == first["preparation"]["capture_id"]
    assert len(fixture.storage.requests_by_key) == 1
