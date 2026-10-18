# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Selected-family, native custody and final source-fence protocol regressions."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid5

import pytest

from process import registry_aca_source_consumer as consumer
from process import registry_aca_source_recipe as producer
from process.network_membership_publication import NetworkPublicationError
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from tests.test_registry_aca_source_recipe import _ROLES, _prepared
from tests.test_registry_aca_source_worker import event


def config(**kwargs):
    return consumer.RegistryACASourceConsumerConfig(
        "registry_control", "executor", "loading_owner", "source-example", "producer-example", 7, 2026, **kwargs
    )


@asynccontextmanager
async def transaction(**options):
    yield


async def recipe(monkeypatch):
    monkeypatch.setattr(consumer, "_require_archive", AsyncMock())
    prepared, validation = await _prepared(monkeypatch)
    return producer._recipe(prepared, validation, _ROLES)


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted", ["source_owner", "source_publisher"])
async def test_native_seal_omitting_source_authority_is_terminal(monkeypatch, omitted):
    roles = ("source_owner", "source_publisher", "source_reader")
    verify = AsyncMock(return_value={"receipt": {"runtime_roles": [role for role in roles if role != omitted]}})
    monkeypatch.setattr(consumer, "verify_network_candidate_custody", verify)
    with pytest.raises(ValueError):
        await consumer._verify_source_custody(object(), object(), config(source_authority_roles=roles))
    assert verify.await_args.kwargs["required"] is True


@pytest.mark.asyncio
async def test_nonselected_or_unqualified_initial_family_requires_review(monkeypatch):
    source = await recipe(monkeypatch)
    connection = SimpleNamespace(transaction=transaction, fetchval=AsyncMock(side_effect=[1, 0, False]))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock())
    bindings = AsyncMock()
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", bindings)
    assert await consumer._selection(connection, source, config()) == {"state": "review_required"}
    bindings.assert_not_awaited()
    connection.fetchval.side_effect = [1, 0, True]
    assert await consumer._selection(connection, source, config()) == {"state": "review_required"}
    bindings.assert_awaited_once()


@pytest.mark.asyncio
async def test_replacement_preserves_other_retained_sources_and_requires_single_slot(monkeypatch):
    source = await recipe(monkeypatch)
    from tests import test_registry_aca_source_recipe as fixtures

    metadata = fixtures._metadata()
    metadata["network_membership"]["release_id"] = "prior-release"
    monkeypatch.setattr(fixtures, "_metadata", lambda: metadata)
    old = await recipe(monkeypatch)
    metadata["network_membership"]["source_id"] = "other-source"
    metadata["network_bindings"]["source_id"] = "other-source"
    other = await recipe(monkeypatch)
    pin = SimpleNamespace(generation_id=3)
    connection = SimpleNamespace(transaction=transaction, fetchval=AsyncMock(side_effect=[1, 3, True]))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock())
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock())
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    retained = AsyncMock(return_value=(other, old))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", retained)
    selected = await consumer._selection(connection, source, config())
    assert selected[2] is pin and selected[3][0] == other
    assert selected[3][1].binding_coordinates == old.binding_coordinates
    assert selected[3][1].source_pin.prepared == old.source_pin.prepared
    assert selected[3][1].source_pin.retained_successor == source.source_pin
    connection.fetchval.side_effect = [1, 3, True]
    retained.return_value = (old, old)
    assert await consumer._selection(connection, source, config()) == {"state": "review_required"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("seal failed"), asyncio.CancelledError()])
async def test_seal_failure_or_cancel_prevents_source_fence_and_publish(monkeypatch, failure):
    accepted_recipe = await recipe(monkeypatch)
    pinned = SimpleNamespace(generation_id=2, manifest_sha256="a" * 64, schema_name="old_candidate")
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    copy_target = SimpleNamespace(candidate_id=str(UUID(int=6)))
    compose = AsyncMock(return_value=(copy_target, object()))
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", compose)
    monkeypatch.setattr(
        consumer,
        "_selection",
        AsyncMock(return_value=(1, SimpleNamespace(generation_id="approved"), pinned, (accepted_recipe,))),
    )
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())
    seal = AsyncMock(side_effect=failure)
    connection = SimpleNamespace(
        transaction=transaction, fetchval=AsyncMock(return_value=None), execute=AsyncMock(return_value="UPDATE 1")
    )
    publish = AsyncMock()
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    with pytest.raises(type(failure)):
        await consumer._attempt(
            connection,
            None,
            None,
            {},
            consumer._event(event(), {}),
            accepted_recipe,
            config(composition_runtime=runtime, seal_candidate=seal),
        )
    assert connection.execute.await_count == 1
    seal.assert_awaited_once_with(copy_target)
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_source_proof_remains_held_until_final_publication_commit(monkeypatch):
    accepted_recipe = await recipe(monkeypatch)
    pinned = SimpleNamespace(generation_id=2, manifest_sha256="a" * 64, schema_name="old_candidate")
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    copy_target = object()
    events = []

    @asynccontextmanager
    async def fence(*args):
        events.append("source_locked")
        try:
            yield
        finally:
            events.append("source_released")

    @asynccontextmanager
    async def final_transaction(**options):
        events.append("cas_started")
        yield
        events.append("cas_committed")

    connection = SimpleNamespace(transaction=final_transaction, execute=AsyncMock(return_value="UPDATE 1"))
    monkeypatch.setattr(
        consumer,
        "_selection",
        AsyncMock(return_value=(1, SimpleNamespace(generation_id="approved"), pinned, (accepted_recipe,))),
    )
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", AsyncMock(return_value=(copy_target, object())))
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())
    monkeypatch.setattr(consumer, "_checkpoint", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=accepted_recipe))
    monkeypatch.setattr(consumer, "current_aca_source_fence", fence)
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock())
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock())

    async def publish(*args, **kwargs):
        events.append("published")

    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    # Target is only used as native candidate identity by the final control update.
    copy_target = SimpleNamespace(candidate_id=str(UUID(int=6)))
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", AsyncMock(return_value=(copy_target, object())))
    assert await consumer._attempt(
        connection,
        None,
        None,
        {},
        consumer._event(event(), {}),
        accepted_recipe,
        config(composition_runtime=runtime, seal_candidate=AsyncMock()),
    ) == {"state": "completed"}
    assert events == ["source_locked", "cas_started", "published", "cas_committed", "source_released"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [NetworkPublicationError("native custody changed"), ValueError("invalid admission")]
)
async def test_immutable_failure_is_not_hidden_as_retry_conflict(monkeypatch, failure):
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))
    attempt = AsyncMock(side_effect=failure)
    monkeypatch.setattr(consumer, "_attempt", attempt)
    with pytest.raises(type(failure), match=str(failure)):
        await consumer._retry_source_attempt(None, None, None, None, None, None, config())
    assert attempt.await_count == 1


@pytest.mark.asyncio
async def test_published_replay_precedes_another_build_after_conflict(monkeypatch):
    replay = AsyncMock(side_effect=[None, {"state": "replayed"}])
    attempt = AsyncMock(side_effect=NetworkPublicationError("Expected serving head changed before publication"))
    monkeypatch.setattr(consumer, "_replay", replay)
    monkeypatch.setattr(consumer, "_attempt", attempt)
    assert await consumer._retry_source_attempt(None, None, None, None, None, None, config()) == {"state": "replayed"}
    assert attempt.await_count == 1 and replay.await_count == 2


@pytest.mark.asyncio
async def test_stale_source_stops_without_rebuilding_or_claiming_publication(monkeypatch):
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))
    attempt = AsyncMock(side_effect=consumer.StaleACASourceError())
    monkeypatch.setattr(consumer, "_attempt", attempt)
    assert await consumer._retry_source_attempt(None, None, None, None, None, None, config()) == {
        "state": "stale_source"
    }
    assert attempt.await_count == 1


async def _publication_case(monkeypatch):
    """Keep orchestration real while replacing native qualification and publication I/O."""
    accepted_recipe = await recipe(monkeypatch)
    identity_by_field = {"owner_oid": 12, "schema_oid": 13}
    accepted = consumer._event(event(), identity_by_field)
    encoded = canonical_registry_source_recipes((accepted_recipe,))
    generations_by_source = {
        "retained_network_source": "a" * 64,
        "custom_membership": "b" * 64,
        "unified_address": "c" * 64,
    }
    attempt = uuid5(UUID(accepted["event_id"]), consumer._json(("a" * 64, "b" * 64, encoded)))
    copy_target = consumer._target(attempt, 1, 3, generations_by_source)
    candidate_by_name = {
        **asdict(copy_target),
        "approved_custom_revision": 1,
        "expected_head": 3,
        "source_generations": {
            **generations_by_source,
            consumer.RECIPE_DIGEST_KEY: registry_source_recipes_sha256(encoded),
        },
        "source_recipes_json": json.loads(encoded),
        "validation_json": {consumer._CHECKPOINT: {**accepted, "attempt_id": str(attempt)}},
    }
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    compose = AsyncMock(return_value=(copy_target, object()))
    monkeypatch.setattr(RegistryPublicationPTGRuntime, "compose", compose)
    monkeypatch.setattr(consumer, "_canonical_session", AsyncMock())
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=accepted_recipe))
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())
    approved = SimpleNamespace(generation_id="b" * 64, approved_revision=1)
    pinned = SimpleNamespace(generation_id=3, manifest_sha256="a" * 64, schema_name="old_candidate")
    monkeypatch.setattr(consumer, "_selection", AsyncMock(return_value=(1, approved, pinned, (accepted_recipe,))))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock())
    custody = AsyncMock(return_value={"receipt": {"runtime_roles": ["source_owner", "source_publisher"]}})
    monkeypatch.setattr(consumer, "verify_network_candidate_custody", custody)
    settings = config(
        composition_runtime=runtime,
        seal_candidate=AsyncMock(),
        source_authority_roles=("source_owner", "source_publisher"),
        max_attempts=1,
    )
    store = consumer.RegistryACASourceNotificationStore("registry_control", "retained_publisher", ("source_reader",))
    return SimpleNamespace(
        recipe=accepted_recipe,
        identity=identity_by_field,
        accepted=accepted,
        target=copy_target,
        candidate=candidate_by_name,
        config=settings,
        store=store,
        compose=compose,
        custody=custody,
    )


def _publication_connection(monkeypatch, *, failure=None):
    state = SimpleNamespace(
        active=False,
        fenced=False,
        head="previous",
        pending=None,
        lose_reply=failure == "lost_reply",
        commit_fences=[],
        rollback_fences=[],
    )
    transitions = []

    @asynccontextmanager
    async def control_transaction(**options):
        assert not state.active
        state.active = True
        try:
            yield
        except BaseException:
            state.pending = None
            state.rollback_fences.append(state.fenced)
            transitions.append("rollback")
            raise
        else:
            if state.pending is not None:
                state.commit_fences.append(state.fenced)
                state.head, state.pending = state.pending, None
                transitions.append("commit")
                if state.lose_reply:
                    state.lose_reply = False
                    raise ConnectionError("publication reply lost after commit")
        finally:
            state.active = False

    @asynccontextmanager
    async def source_fence(*args):
        if failure == "stale_source":
            raise consumer.StaleACASourceError("source superseded")
        state.fenced = True
        try:
            yield
        finally:
            state.fenced = False

    connection = SimpleNamespace(
        transaction=control_transaction,
        is_in_transaction=lambda: state.active,
        is_closed=lambda: False,
        fetch=AsyncMock(return_value=[]),
        fetchval=AsyncMock(side_effect=lambda sql, *args: None if "SELECT validation_json" in sql else True),
        execute=AsyncMock(side_effect=["UPDATE 1", "UPDATE 0"] if failure == "checkpoint_conflict" else None),
    )
    if failure != "checkpoint_conflict":
        connection.execute.return_value = "UPDATE 1"
    monkeypatch.setattr(consumer, "current_aca_source_fence", source_fence)
    return connection, state, transitions


async def _consume_case(case, connection):
    return await consumer.consume_registry_aca_source_notification_once(
        connection=connection,
        notification_pool=None,
        store=case.store,
        identity=case.identity,
        event=event(),
        config=case.config,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_completed_or_lost_reply_replays_exact_sealed_candidate_without_rebuilding(monkeypatch, lost_reply):
    case = await _publication_case(monkeypatch)
    connection, state, transitions = _publication_connection(monkeypatch, failure="lost_reply" if lost_reply else None)

    async def seal(target):
        assert target == case.target and not state.active and state.head == "previous"
        provisional = json.loads(connection.execute.await_args.args[2])
        assert provisional["kind"] == "aca" and provisional["revision"] == 1
        assert provisional["checkpoint"] == case.candidate["validation_json"][consumer._CHECKPOINT]

    case.config.seal_candidate.side_effect = seal

    async def publish(driver, target, **options):
        assert driver is connection and target == case.target and state.active and state.fenced
        assert options == {"control_schema": "registry_control"}
        state.pending = target

    publish_call = AsyncMock(side_effect=publish)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish_call)
    if lost_reply:
        with pytest.raises(ConnectionError, match="reply lost after commit"):
            await _consume_case(case, connection)
    else:
        assert await _consume_case(case, connection) == {"state": "completed"}
    assert state.head == case.target and not state.active and not state.fenced
    assert "pg_advisory_unlock" in connection.fetchval.await_args.args[0]
    checkpoint_args = connection.execute.await_args.args
    assert json.loads(checkpoint_args[2]) == case.candidate["validation_json"][consumer._CHECKPOINT]
    assert checkpoint_args[3] == UUID(case.target.candidate_id)
    assert checkpoint_args[4] == consumer.SOURCE_EVENT_ASSERTION
    assert json.loads(checkpoint_args[5])["checkpoint"] == json.loads(checkpoint_args[2])
    if lost_reply:
        for field in ("validation_json", "source_generations"):
            case.candidate[field] = json.dumps(case.candidate[field])
    connection.fetch.return_value = [case.candidate]
    assert await _consume_case(case, connection) == {"state": "replayed"}
    assert state.head == case.target and transitions == ["commit", "commit"]
    assert state.commit_fences == [True, True]
    case.compose.assert_awaited_once()
    assert case.compose.await_args.kwargs["request_id"] == UUID(json.loads(checkpoint_args[2])["attempt_id"])
    case.config.seal_candidate.assert_awaited_once_with(case.target)
    assert case.custody.await_count == 2
    assert all(
        call.args == (connection, case.target) and call.kwargs["required"] for call in case.custody.await_args_list
    )
    assert publish_call.await_count == 2 and connection.execute.await_count == 2
    assert sum("pg_advisory_unlock" in call.args[0] for call in connection.fetchval.await_args_list) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stale_source", "checkpoint_conflict", "cancel"])
async def test_final_source_or_cas_failure_preserves_previous_head_and_releases_lock(monkeypatch, failure):
    case = await _publication_case(monkeypatch)
    connection, state, transitions = _publication_connection(monkeypatch, failure=failure)
    publish = AsyncMock(side_effect=asyncio.CancelledError() if failure == "cancel" else None)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    if failure == "stale_source":
        assert await _consume_case(case, connection) == {"state": "stale_source"}
    else:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
            await _consume_case(case, connection)
    assert state.head == "previous" and not state.active and not state.fenced
    assert "pg_advisory_unlock" in connection.fetchval.await_args.args[0]
    case.compose.assert_awaited_once()
    case.config.seal_candidate.assert_awaited_once_with(case.target)
    assert publish.await_count == (1 if failure == "cancel" else 0)
    assert transitions == ([] if failure == "stale_source" else ["rollback"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [asyncio.CancelledError(), NetworkPublicationError("Serving head changed before publication")]
)
async def test_replay_keeps_source_fence_until_failed_publication_rolls_back(monkeypatch, failure):
    case = await _publication_case(monkeypatch)
    connection, state, transitions = _publication_connection(monkeypatch)
    connection.fetch.return_value = [case.candidate]
    publish = AsyncMock(side_effect=failure)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    if isinstance(failure, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await _consume_case(case, connection)
    else:
        assert await _consume_case(case, connection) == {"state": "conflict"}
    assert state.head == "previous" and not state.active and not state.fenced
    assert transitions == ["rollback"] and state.rollback_fences == [True]
    assert "pg_advisory_unlock" in connection.fetchval.await_args.args[0]
    case.compose.assert_not_awaited()
    case.config.seal_candidate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["event", "attempt", "target", "generations", "duplicate"])
async def test_replay_rejects_foreign_checkpoint_or_candidate_before_publication(monkeypatch, damage):
    case = await _publication_case(monkeypatch)
    candidate = case.candidate
    if damage == "event":
        candidate["validation_json"][consumer._CHECKPOINT]["reader_identity_sha256"] = "f" * 64
    elif damage == "attempt":
        candidate["validation_json"][consumer._CHECKPOINT]["attempt_id"] = str(UUID(int=99))
    elif damage == "target":
        candidate["dataset_id"] = str(UUID(int=99))
    elif damage == "generations":
        candidate["source_generations"]["custom_membership"] = "f" * 64
    connection = SimpleNamespace(
        transaction=transaction,
        fetch=AsyncMock(return_value=[candidate] * (2 if damage == "duplicate" else 1)),
        fetchval=AsyncMock(return_value=True),
    )
    publish = AsyncMock()
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    with pytest.raises(ValueError):
        await consumer._replay(connection, case.accepted, case.recipe, case.config)
    publish.assert_not_awaited()
    case.custody.assert_not_awaited()
