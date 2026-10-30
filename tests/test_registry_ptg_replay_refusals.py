# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable replay refusals preserve publication and finish transactions before retry."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid5

import asyncpg
import pytest

from process import registry_ptg_source_consumer as consumer
from process.network_address_projection import PinnedAddressSource
from process.registry_candidate_composition import _target
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    registry_source_recipes_sha256,
)
from tests.test_registry_ptg_source_consumer import (
    IDENTITY,
    STORE,
    Connection,
    config,
    header,
    native_event,
    recipe,
)


async def _durable_replay():
    selected = await recipe()
    event_by_name = dict(event_id=str(UUID(int=7)), header=header(native_event()), reader_identity_sha256="f" * 64)
    encoded = canonical_registry_source_recipes((selected,))
    generations_by_name = dict(retained_network_source="a" * 64, custom_membership="b" * 64, unified_address="c" * 64)
    attempt = uuid5(UUID(event_by_name["event_id"]), consumer._json(("a" * 64, "b" * 64, encoded)))
    copy_target = _target(attempt, 1, 1, generations_by_name)
    candidate_by_name = {
        name: getattr(copy_target, name)
        for name in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    }
    candidate_by_name.update(
        approved_custom_revision=1,
        expected_head=1,
        source_generations={**generations_by_name, RECIPE_DIGEST_KEY: registry_source_recipes_sha256(encoded)},
        source_recipes_json=json.loads(encoded),
        validation_json={
            consumer._CHECKPOINT: {**deepcopy(event_by_name), "attempt_id": str(attempt)},
            "address_source": asdict(PinnedAddressSource("addresses", "entity_address_unified", "c" * 64)),
        },
    )
    return selected, event_by_name, candidate_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["duplicate", "event_identity", "recipe", "attempt", "candidate"])
async def test_durable_ptg_replay_refuses_changed_identity_without_publication(monkeypatch, fault):
    selected, event_by_name, candidate_by_name = await _durable_replay()
    candidates = [candidate_by_name]
    match fault:
        case "duplicate":
            other_candidate = deepcopy(candidate_by_name)
            other_candidate["candidate_id"] = str(UUID(int=99))
            candidates.append(other_candidate)
        case "event_identity":
            candidate_by_name["validation_json"][consumer._CHECKPOINT]["reader_identity_sha256"] = "e" * 64
        case "recipe":
            other = replace(selected, source_pin=replace(selected.source_pin, capture_id=str(UUID(int=99))))
            candidate_by_name["source_recipes_json"] = json.loads(canonical_registry_source_recipes((other,)))
        case "attempt":
            candidate_by_name["validation_json"][consumer._CHECKPOINT]["attempt_id"] = str(UUID(int=99))
        case "candidate":
            foreign_target = _target(
                UUID(int=99),
                1,
                1,
                {
                    key: generation
                    for key, generation in candidate_by_name["source_generations"].items()
                    if key != RECIPE_DIGEST_KEY
                },
            )
            candidate_by_name.update(asdict(foreign_target))
    before_candidates, before_event = deepcopy(candidates), deepcopy(event_by_name)
    connection = SimpleNamespace(fetch=AsyncMock(return_value=candidates), execute=AsyncMock())
    publication = AsyncMock(wraps=consumer.prepare_and_publish_network_candidate)
    monkeypatch.setattr(consumer, "prepare_and_publish_network_candidate", publication)

    refusal = (
        "registry_source_inventory_metadata_invalid" if fault == "recipe" else "registry_ptg_source_consumer_invalid"
    )
    with pytest.raises(ValueError, match=refusal):
        await consumer._replay(connection, event_by_name, selected, config())

    publication.assert_not_awaited()
    connection.execute.assert_not_awaited()
    connection.fetch.assert_awaited_once()
    assert candidates == before_candidates and event_by_name == before_event


@pytest.mark.asyncio
async def test_absent_durable_checkpoint_does_not_invent_a_replay(monkeypatch):
    selected, event_by_name, _candidate_by_name = await _durable_replay()
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[]), execute=AsyncMock())
    publication = AsyncMock(wraps=consumer.prepare_and_publish_network_candidate)
    monkeypatch.setattr(consumer, "prepare_and_publish_network_candidate", publication)
    assert await consumer._replay(connection, event_by_name, selected, config()) is None
    publication.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("gate", ["wrong_role", "busy"])
async def test_unadmitted_ptg_worker_cannot_read_notifications_or_unlock_another_owner(gate):
    connection = Connection()
    connection.fetchval = AsyncMock(side_effect=[False] if gate == "wrong_role" else [True, False])
    connection.execute = AsyncMock(side_effect=AssertionError("Unadmitted worker reached session setup"))
    pool = SimpleNamespace(acquire=Mock(side_effect=AssertionError("Unadmitted worker read a notification")))
    arguments_by_name = dict(
        connection=connection,
        notification_pool=pool,
        store=STORE,
        reader_role="office_reader",
        identity=IDENTITY,
        event=header(native_event()),
        config=config(),
    )
    if gate == "wrong_role":
        with pytest.raises(ValueError, match="registry_ptg_source_consumer_invalid"):
            await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name)
    else:
        assert await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name) == {"state": "busy"}
    pool.acquire.assert_not_called()
    connection.execute.assert_not_awaited()
    assert not any("pg_advisory_unlock" in call.args[0] for call in connection.fetchval.await_args_list)


class RefusingSelectionConnection(Connection):
    """Fail the actual selection query; transactions retain ordinary rollback semantics."""

    def __init__(self, failure):
        super().__init__()
        self.failure = failure
        self.active = False
        self.transactions = []
        self.execute = AsyncMock()

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active
        assert options == dict(isolation="repeatable_read", readonly=True)
        self.active = True
        self.transactions.append("begin")
        try:
            yield self
        except BaseException:
            self.transactions.append("rollback")
            raise
        finally:
            self.active = False

    async def fetchval(self, statement, *parameters):
        assert self.active and "registry_revision_control" in statement
        self.calls.append((statement, parameters))
        raise self.failure


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [asyncpg.InsufficientPrivilegeError("reader denied"), asyncpg.DeadlockDetectedError()]
)
async def test_permanent_or_nonretryable_database_failure_preserves_cause_without_retry(monkeypatch, failure):
    selected = await recipe()
    connection = RefusingSelectionConnection(failure)
    publication = AsyncMock(wraps=consumer.publish_network_candidate)
    monkeypatch.setattr(consumer, "publish_network_candidate", publication)
    with pytest.raises(type(failure)) as observed:
        await consumer._retry_source_attempt(connection, None, STORE, "office_reader", IDENTITY, {}, selected, config())
    assert observed.value is failure
    assert connection.transactions == ["begin", "rollback"]
    assert not connection.active and len(connection.calls) == 1
    connection.execute.assert_not_awaited()
    publication.assert_not_awaited()


@pytest.mark.asyncio
async def test_serialization_conflict_retries_only_after_rollback_and_stops_at_configured_bound(monkeypatch):
    selected = await recipe()
    connection = RefusingSelectionConnection(asyncpg.SerializationError("snapshot changed"))
    configuration = replace(config(), max_attempts=3)
    publication = AsyncMock(wraps=consumer.publish_network_candidate)
    monkeypatch.setattr(consumer, "publish_network_candidate", publication)
    outcome = await consumer._retry_source_attempt(
        connection, None, STORE, "office_reader", IDENTITY, {}, selected, configuration
    )
    assert outcome == {"state": "conflict"}
    assert connection.transactions == ["begin", "rollback"] * configuration.max_attempts
    assert not connection.active and len(connection.calls) == configuration.max_attempts
    connection.execute.assert_not_awaited()
    publication.assert_not_awaited()
