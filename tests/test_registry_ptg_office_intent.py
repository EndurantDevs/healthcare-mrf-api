# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable semantic office reservation without sockets or ambient authority."""

import copy
import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

import pytest

from process import registry_ptg_office_intent as intent
from process.registry_record_store import RegistryActor
from tests.test_registry_ptg_office_preparation import selector as prepare_selector
from tests.test_registry_ptg_published_plan import identity
from tests.test_registry_ptg_published_plan import intent as source_intent
from tests.test_registry_ptg_scope_engine import _ownership


def source_command():
    published = identity()
    return {
        **source_intent(),
        "operation": "approve_ptg_published_plan_scope",
        "published_identity": published,
        "ownership": _ownership(),
        "coordinates": {
            "source_system": "ptg",
            "source_id": published["source_key"],
            "dataset_schema": "synthetic_ptg",
            "dataset_id": published["snapshot_id"],
            "producer_id": "ptg_published_result_source_authority.v1",
            "edition_id": published["snapshot_manifest_sha256"],
        },
        "source": {
            "binding_source_key": published["source_key"],
            "snapshot_id": published["snapshot_id"],
            "ptg_schema_name": "synthetic_ptg",
        },
    }


def selector():
    selection_by_field = {
        name: value for name, value in prepare_selector().items() if name not in {"capture_id", "idempotency_key"}
    }
    selection_by_field.update({name: source_command()[name] for name in ("scope_id", "client_id")})
    return selection_by_field


def actor():
    return RegistryActor("platform_admin", UUID(prepare_selector()["scope_id"]), "system")


class Connection:
    def __init__(self):
        self.requests_by_key = {}
        self.calls = []
        self.active = True
        self.isolation = "read committed"
        self.hide_conflict = False

    def is_in_transaction(self):
        return self.active

    async def fetchval(self, sql):
        self.calls.append(sql)
        return self.isolation

    @asynccontextmanager
    async def transaction(self):
        previous = copy.deepcopy(self.requests_by_key)
        try:
            yield
        except BaseException:
            self.requests_by_key = previous
            raise

    async def fetchrow(self, sql, *args):
        self.calls.append(sql)
        if sql.startswith("INSERT"):
            key = args[1], args[5]
            if key in self.requests_by_key:
                return None
            request_by_field = dict(
                zip(
                    (
                        "request_id",
                        "actor_key",
                        "session_token_sha256",
                        "actor_json",
                        "command_json",
                        "idempotency_key",
                        "request_sha256",
                    ),
                    args,
                    strict=True,
                )
            )
            request_by_field.update(state="queued", created_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
            self.requests_by_key[key] = request_by_field
            return request_by_field
        assert sql.startswith("SELECT")
        return None if self.hide_conflict else self.requests_by_key.get(tuple(args))


async def reserve(connection, *, source=None, selection=None, original_actor=None, session="a" * 64):
    return await intent.reserve_registry_ptg_office_prepare_intent(
        connection,
        source or source_command(),
        selection or selector(),
        original_actor or actor(),
        session_token_sha256=session,
        control_schema="synthetic_registry",
    )


@pytest.mark.asyncio
async def test_lost_ack_and_renewed_session_return_original_command_handles_and_audit():
    connection = Connection()
    original = await reserve(connection)
    audit = copy.deepcopy(next(iter(connection.requests_by_key.values())))
    recovered = await reserve(connection, session="b" * 64)
    assert recovered == {**original, "replayed": True}
    assert next(iter(connection.requests_by_key.values())) == audit
    assert audit["session_token_sha256"] == "a" * 64
    assert json.loads(audit["actor_json"])["impersonator_id"] is None
    assert recovered["command"]["selector"]["capture_id"] == original["command"]["selector"]["capture_id"]
    assert recovered["command"]["selector"]["idempotency_key"] == original["command"]["selector"]["idempotency_key"]
    assert len(connection.requests_by_key) == 1
    assert all(not sql.startswith(("UPDATE", "DELETE")) for sql in connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["reason", "scope_approval_sha256", "retained_generation_id", "selections", "source", "actor"]
)
async def test_semantic_changes_are_separate_intents(field):
    connection = Connection()
    original = await reserve(connection)
    selection, source, original_actor = selector(), source_command(), actor()
    match field:
        case "reason":
            selection[field] = "A different reviewed reason"
        case "scope_approval_sha256":
            selection[field] = "c" * 64
        case "retained_generation_id":
            selection[field] += 1
        case "selections":
            selection[field][0]["source_record_ordinal"] += 1
        case "source":
            source["reason"] = "A different source review"
        case "actor":
            original_actor = RegistryActor("platform_admin", UUID("33333333-3333-4333-8333-333333333333"), "system")
    changed = await reserve(connection, source=source, selection=selection, original_actor=original_actor)
    assert changed["request_id"] != original["request_id"]
    assert changed["semantic_sha256"] != original["semantic_sha256"]
    assert len(connection.requests_by_key) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_snapshot_isolation_refuses_before_any_insert(isolation):
    connection = Connection()
    connection.isolation = isolation
    with pytest.raises(ValueError, match="requires_read_committed"):
        await reserve(connection)
    assert not any(sql.startswith("INSERT") for sql in connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda request_by_field: request_by_field.update(request_sha256="0" * 64),
        lambda request_by_field: request_by_field.update(actor_key="0" * 64),
        lambda request_by_field: request_by_field.update(session_token_sha256="bad"),
        lambda request_by_field: request_by_field.update(actor_json="{}"),
        lambda request_by_field: request_by_field.update(command_json="{}"),
        lambda request_by_field: request_by_field.update(state="unknown"),
        lambda request_by_field: request_by_field.update(request_id=UUID(int=0)),
        lambda request_by_field: request_by_field.update(created_at=datetime(2026, 1, 1)),
    ],
)
async def test_malformed_original_audit_never_returns_replacement_handles(mutation):
    connection = Connection()
    await reserve(connection)
    mutation(next(iter(connection.requests_by_key.values())))
    with pytest.raises(ValueError, match="stored_invalid"):
        await reserve(connection, session="b" * 64)
    assert len(connection.requests_by_key) == 1


@pytest.mark.asyncio
async def test_missing_conflict_row_remains_same_intent_retry_not_a_new_identity():
    connection = Connection()
    await reserve(connection)
    connection.hide_conflict = True
    with pytest.raises(ValueError, match="retry_same_semantic_key"):
        await reserve(connection)
    assert len(connection.requests_by_key) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        lambda selection: selection.update(capture_id="11111111-1111-4111-8111-111111111111"),
        lambda selection: selection.update(idempotency_key="browser-key"),
        lambda selection: selection.update(client_id="other_client"),
        lambda selection: selection.update(retained_generation_id=True),
        lambda selection: selection["selections"].append(copy.deepcopy(selection["selections"][0])),
    ],
)
async def test_invalid_or_browser_owned_intent_handles_refuse_before_sql(mutation):
    connection = Connection()
    selection_by_field = selector()
    mutation(selection_by_field)
    with pytest.raises((ValueError, TypeError)):
        await reserve(connection, selection=selection_by_field)
    assert not connection.calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed_actor",
    [
        RegistryActor("client_owner", actor().user_id, "client_example"),
        RegistryActor("platform_admin", actor().user_id, "client_example"),
        RegistryActor("platform_admin", actor().user_id, "system", actor().user_id),
    ],
)
async def test_nonoriginal_administrator_refuses_before_sql(changed_actor):
    connection = Connection()
    with pytest.raises(ValueError):
        await reserve(connection, original_actor=changed_actor)
    assert not connection.calls


@pytest.mark.asyncio
async def test_caller_transaction_is_required_without_opening_or_committing_one():
    connection = Connection()
    connection.active = False
    with pytest.raises(ValueError, match="requires_caller_transaction"):
        await reserve(connection)
    assert not connection.calls and not connection.requests_by_key


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("operation", "approve_ptg_office_capture"),
        ("version", True),
        ("semantic_sha256", "0" * 64),
        ("selector", {**prepare_selector(), "capture_id": "33333333-3333-4333-8333-333333333333"}),
    ],
)
async def test_changed_stored_command_cannot_return_unverified_handles(field, value):
    connection = Connection()
    await reserve(connection)
    request_by_field = next(iter(connection.requests_by_key.values()))
    command = json.loads(request_by_field["command_json"])
    command[field] = value
    request_by_field["command_json"] = json.dumps(command)
    with pytest.raises(ValueError, match="stored_invalid"):
        await reserve(connection, session="b" * 64)
    assert len(connection.requests_by_key) == 1
