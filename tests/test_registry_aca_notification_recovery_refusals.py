# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable assertion recovery and notification refusal with declared database doubles."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_aca_source_consumer as consumer
from process import registry_aca_source_notifications as notifications
from tests.test_registry_aca_source_consumer import config
from tests.test_registry_aca_source_notifications import _Connection, _store
from tests.test_registry_aca_source_worker import event


class AssertionConnection:
    """Model candidate assertion CAS and a reply lost after its transaction commits."""

    def __init__(self, *, saved=None, changed="UPDATE 1", lose_reply=False):
        self.saved = deepcopy(saved)
        self.changed = changed
        self.lose_reply = lose_reply
        self.active = False
        self.transactions = []
        self.writes = []

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active and options == {}
        self.active = True
        before = deepcopy(self.saved)
        try:
            yield
        except BaseException:
            self.saved = before
            self.transactions.append("rollback")
            raise
        else:
            self.transactions.append("commit")
            if self.lose_reply:
                self.lose_reply = False
                raise ConnectionError("assertion commit reply lost")
        finally:
            self.active = False

    async def fetchval(self, statement, *parameters):
        assert self.active and "SELECT validation_json" in statement
        assert parameters == (consumer.SOURCE_EVENT_ASSERTION, UUID(int=6))
        return None if self.saved is None else json.dumps(self.saved)

    async def execute(self, statement, *parameters):
        assert self.active and statement.startswith('UPDATE "registry_control".network_membership_candidate')
        assert parameters[0] == consumer.SOURCE_EVENT_ASSERTION and parameters[2] == UUID(int=6)
        self.writes.append(parameters)
        if self.changed == "UPDATE 1":
            assert self.saved is None
            self.saved = json.loads(parameters[1])
        return self.changed


def _checkpoint_case():
    accepted = consumer._event(event(), {"schema_oid": 13, "owner_oid": 12})
    attempt = UUID(int=7)
    assertion_by_name = {"revision": 1, "kind": "aca", "checkpoint": {**deepcopy(accepted), "attempt_id": str(attempt)}}
    return accepted, attempt, assertion_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["receipt", "attempt", "cas"])
async def test_foreign_or_lost_candidate_assertion_cannot_reach_sealing(monkeypatch, conflict):
    accepted, attempt, assertion_by_name = _checkpoint_case()
    if conflict == "receipt":
        assertion_by_name["checkpoint"]["header"]["notification_sha256"] = "f" * 64
    elif conflict == "attempt":
        assertion_by_name["checkpoint"]["attempt_id"] = str(UUID(int=8))
    connection = AssertionConnection(saved=None if conflict == "cas" else assertion_by_name, changed="UPDATE 0")
    before = deepcopy(connection.saved)
    seal = AsyncMock()
    publication = AsyncMock(wraps=consumer.publish_network_candidate)
    monkeypatch.setattr(consumer, "publish_network_candidate", publication)

    with pytest.raises(ValueError, match="registry_aca_source_consumer_invalid"):
        await consumer._checkpoint(
            connection, SimpleNamespace(candidate_id=str(UUID(int=6))), accepted, attempt, config(seal_candidate=seal)
        )

    assert connection.saved == before and connection.transactions == ["rollback"]
    assert not connection.active and len(connection.writes) == int(conflict == "cas")
    seal.assert_not_awaited()
    publication.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("seal_failure", [RuntimeError("sealing deferred"), asyncio.CancelledError()])
async def test_lost_assertion_ack_resumes_exact_committed_receipt_without_rewriting(monkeypatch, seal_failure):
    accepted, attempt, assertion_by_name = _checkpoint_case()
    connection = AssertionConnection(lose_reply=True)
    target = SimpleNamespace(candidate_id=str(UUID(int=6)))
    seal = AsyncMock(side_effect=seal_failure)
    settings = config(seal_candidate=seal)
    publication = AsyncMock(wraps=consumer.publish_network_candidate)
    monkeypatch.setattr(consumer, "publish_network_candidate", publication)

    with pytest.raises(ConnectionError, match="assertion commit reply lost"):
        await consumer._checkpoint(connection, target, accepted, attempt, settings)
    assert connection.saved == assertion_by_name and connection.transactions == ["commit"]
    seal.assert_not_awaited()

    with pytest.raises(type(seal_failure)) as observed:
        await consumer._checkpoint(connection, target, accepted, attempt, settings)
    assert observed.value is seal_failure
    assert connection.saved == assertion_by_name and connection.transactions == ["commit", "commit"]
    assert len(connection.writes) == 1 and not connection.active
    seal.assert_awaited_once_with(target)
    publication.assert_not_awaited()


class NotificationReader(_Connection):
    """Expose native catalog/receipt transport while retaining actual store qualification."""

    def __init__(self):
        super().__init__(writing=False)
        del self.transaction
        self.active = False
        self.receipt_reads = []
        self.exits = []

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active and options == dict(isolation="repeatable_read", readonly=True)
        self.active = True
        try:
            yield
        except BaseException:
            self.exits.append("rollback")
            raise
        finally:
            self.active = False

    async def fetchrow(self, statement, *parameters):
        if statement != notifications._IDENTITY_SQL:
            self.receipt_reads.append(parameters[0])
        return await super().fetchrow(statement, *parameters)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", ["rebound_inbox", "reader_grants", "missing_receipt"])
async def test_unqualified_notification_read_closes_snapshot_without_mutation(refusal):
    connection = NotificationReader()
    identity_by_field = {
        name: connection.identity[name]
        for name in ("schema_oid", "owner_oid", "inbox_oid", "head_oid", "database_name", "database_oid")
    }
    if refusal == "rebound_inbox":
        connection.identity["inbox_oid"] += 1
    elif refusal == "reader_grants":
        connection.runtime_closed = False
    release_events = []

    @asynccontextmanager
    async def acquire():
        try:
            yield connection
        finally:
            release_events.append(True)

    with pytest.raises(ValueError, match="registry_aca_source_(consumer|notification)_invalid"):
        await consumer._notice(SimpleNamespace(acquire=acquire), _store(), identity_by_field, event())

    assert connection.receipt_reads == ([UUID(event()["capture_id"])] if refusal == "missing_receipt" else [])
    assert connection.rows == {} and connection.statements == []
    assert connection.exits == ["rollback"] and not connection.active and release_events == [True]
