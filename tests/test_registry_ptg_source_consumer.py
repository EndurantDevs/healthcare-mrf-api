"""Exact native event proof and approved-only refresh control, without a database."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_source_consumer as consumer
from process import registry_ptg_source_notifications as notices
from process.network_membership_publication import NetworkPublicationError
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from tests.test_registry_ptg_source_notifications import IDENTITY, STORE, Reader
from tests.test_registry_ptg_source_notifications import event as native_event


class ExactReader(Reader):
    async def fetchrow(self, sql, *args):
        if "WHERE a.capture_id=$1::uuid" in sql:
            self.calls.append((sql, args))
            return next((row for row in self.rows if UUID(row["capture_id"]) == args[0]), None)
        return await super().fetchrow(sql, *args)


def header(row):
    return {key: row[key] for key in ("capture_id", "approval_sha256", "hold_sha256")}


async def recipe():
    row = native_event()
    return await notices.read_registry_ptg_source_notification(
        ExactReader([row]), STORE, reader_role="office_reader", identity=IDENTITY, event=header(row)
    )


@pytest.mark.asyncio
async def test_exact_reader_proves_pair_without_history_scan():
    row = native_event()
    reader = ExactReader([row, native_event(2)])
    actual = await notices.read_registry_ptg_source_notification(
        reader, STORE, reader_role="office_reader", identity=IDENTITY, event=header(row)
    )
    assert actual.source_pin.capture_id == row["capture_id"]
    assert actual.source_pin.approval_sha256 == row["approval_sha256"]
    assert reader.identity_reads == 2
    queries = [sql for sql, _ in reader.calls if "WHERE a.capture_id=$1::uuid" in sql]
    assert len(queries) == 1 and "ORDER BY" not in queries[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "digest", "identity", "hold"])
async def test_exact_reader_refuses_missing_changed_or_wrong_custody(damage):
    row = native_event()
    requested = header(row)
    reader = ExactReader([] if damage == "missing" else [row])
    if damage == "digest":
        requested["approval_sha256"] = "f" * 64
    if damage == "identity":
        reader.drift = True
    if damage == "hold":
        row["hold_json"] = "{}"
    with pytest.raises(ValueError):
        await notices.read_registry_ptg_source_notification(
            reader, STORE, reader_role="office_reader", identity=IDENTITY, event=requested
        )


@pytest.mark.asyncio
async def test_replacement_uses_exact_old_approved_binding_and_preserves_others():
    new = await recipe()
    old = replace(new, source_pin=replace(new.source_pin, capture_id=str(UUID(int=3))))
    other = replace(new, source_pin=replace(new.source_pin, capture_id=str(UUID(int=4))))
    selector_by_name = {
        "capture_id": old.source_pin.capture_id,
        "recipe_sha256": registry_source_recipes_sha256(canonical_registry_source_recipes((old,))),
    }
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(selector_by_name)))
    pin = SimpleNamespace(approved_custom_revision=7)
    assert await consumer._replacement(connection, '"registry"', pin, (other, old), "binding-id", new) == (other, new)
    assert connection.fetchval.await_args.args[1:] == (7, "binding-id")
    assert await consumer._replacement(connection, '"registry"', pin, (old, old), "binding-id", new) is None
    connection.fetchval.reset_mock()
    assert await consumer._replacement(connection, '"registry"', pin, (other, new), "binding-id", new) == (other, new)
    connection.fetchval.assert_not_awaited()


class Connection:
    def __init__(self):
        self.calls = []
        self.head = 1

    def is_in_transaction(self):
        return False

    def is_closed(self):
        return False

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        if "current_user" in sql or "advisory" in sql:
            return True
        if "approved_revision" in sql:
            return 1
        if "generation_id" in sql:
            return self.head
        raise AssertionError(sql)

    @asynccontextmanager
    async def transaction(self, **kwargs):
        yield self


def config():
    runtime = object.__new__(RegistryPublicationPTGRuntime)
    return consumer.RegistryPTGSourceConsumerConfig(
        "office_control", "executor", "physical_owner", composition_runtime=runtime
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("selected,head", [(None, 1), ("binding", 0)])
async def test_unselected_or_initial_head_never_composes(monkeypatch, selected, head):
    connection = Connection()
    connection.head = head
    monkeypatch.setattr(
        consumer, "pin_approved_membership_source", AsyncMock(return_value=SimpleNamespace(approved_revision=1))
    )
    monkeypatch.setattr(consumer, "_selected", AsyncMock(return_value=selected))
    resolve = AsyncMock(side_effect=AssertionError("unexpected serving read/compose"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", resolve)
    result = await consumer._attempt(connection, None, STORE, "office_reader", IDENTITY, {}, await recipe(), config())
    assert result == {"state": "review_required"}
    resolve.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_head_conflict_retries_and_unlocks(monkeypatch):
    actual = await recipe()
    monkeypatch.setattr(consumer, "_canonical_session", AsyncMock())
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=actual))
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))
    attempt = AsyncMock(
        side_effect=[NetworkPublicationError("Serving head changed before publication"), {"state": "completed"}]
    )
    monkeypatch.setattr(consumer, "_attempt", attempt)
    connection = Connection()
    assert await consumer.consume_registry_ptg_source_notification_once(
        connection=connection,
        notification_pool=None,
        store=STORE,
        reader_role="office_reader",
        identity=IDENTITY,
        event=header(native_event()),
        config=config(),
    ) == {"state": "completed"}
    assert attempt.await_count == 2
    assert "pg_advisory_unlock" in connection.calls[-1][0]


@pytest.mark.asyncio
async def test_permanent_failure_does_not_retry_and_releases_lock(monkeypatch):
    monkeypatch.setattr(consumer, "_canonical_session", AsyncMock())
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=await recipe()))
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))
    attempt = AsyncMock(side_effect=ValueError("changed custody"))
    monkeypatch.setattr(consumer, "_attempt", attempt)
    connection = Connection()
    with pytest.raises(ValueError, match="changed custody"):
        await consumer.consume_registry_ptg_source_notification_once(
            connection=connection,
            notification_pool=None,
            store=STORE,
            reader_role="office_reader",
            identity=IDENTITY,
            event=header(native_event()),
            config=config(),
        )
    assert attempt.await_count == 1 and "pg_advisory_unlock" in connection.calls[-1][0]


@pytest.mark.asyncio
async def test_cancellation_drains_before_advisory_unlock(monkeypatch):
    started, drained = asyncio.Event(), asyncio.Event()
    monkeypatch.setattr(consumer, "_canonical_session", AsyncMock())
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=await recipe()))
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))

    async def attempt(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    monkeypatch.setattr(consumer, "_attempt", attempt)
    connection = Connection()
    task = asyncio.create_task(
        consumer.consume_registry_ptg_source_notification_once(
            connection=connection,
            notification_pool=None,
            store=STORE,
            reader_role="office_reader",
            identity=IDENTITY,
            event=header(native_event()),
            config=config(),
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert drained.is_set() and "pg_advisory_unlock" in connection.calls[-1][0]


@pytest.mark.asyncio
async def test_lost_ack_replay_checks_exact_checkpoint_and_never_reactivates_another_head(monkeypatch):
    from dataclasses import asdict
    from uuid import uuid5

    from process.network_address_projection import PinnedAddressSource
    from process.registry_candidate_composition import _target
    from process.registry_source_recipe_store import RECIPE_DIGEST_KEY

    selected = await recipe()
    document_by_name = {
        "event_id": str(UUID(int=7)),
        "header": header(native_event()),
        "reader_identity_sha256": "f" * 64,
    }
    encoded = canonical_registry_source_recipes((selected,))
    generations_by_name = {
        "retained_network_source": "a" * 64,
        "custom_membership": "b" * 64,
        "unified_address": "c" * 64,
    }
    attempt = uuid5(UUID(document_by_name["event_id"]), consumer._json(("a" * 64, "b" * 64, encoded)))
    copy_target = _target(attempt, 1, 1, generations_by_name)
    address = PinnedAddressSource("addresses", "entity_address_unified", "c" * 64)
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
            consumer._CHECKPOINT: {**document_by_name, "attempt_id": str(attempt)},
            "address_source": asdict(address),
        },
    )
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[candidate_by_name]))
    publish = AsyncMock()
    monkeypatch.setattr(consumer, "prepare_and_publish_network_candidate", publish)
    assert await consumer._replay(connection, document_by_name, selected, config()) == {"state": "replayed"}
    publish.assert_awaited_once_with(
        connection, copy_target, address, **config().writer_roles, control_schema="office_control"
    )
    candidate_by_name["validation_json"][consumer._CHECKPOINT]["header"] = {
        **header(native_event()),
        "hold_sha256": "e" * 64,
    }
    with pytest.raises(ValueError):
        await consumer._replay(connection, document_by_name, selected, config())
    assert publish.await_count == 1


def _install_ptg_seal(cfg, phase_state, connection, copy_target, sealing):
    """Retain exact seal-time assertions and configured failure behavior."""

    async def seal(actual):
        assert (
            actual == copy_target
            and phase_state.saved is not None
            and phase_state.completed is None
            and not connection.active
        )
        assert phase_state.saved["kind"] == "ptg" and phase_state.saved["revision"] == 1
        if sealing == "failure":
            raise ValueError("custody_changed")
        if sealing == "cancel":
            raise asyncio.CancelledError

    if sealing != "legacy":
        cfg.seal_candidate = AsyncMock(side_effect=seal)


def _ptg_attempt_checkpoint(monkeypatch, copy_target, approved, sealing, cfg):
    """Model committed provisional assertion and final checkpoint rollback without globals."""
    connection = Connection()
    connection.active = False
    phase_state = SimpleNamespace(saved=None, completed=None)
    statements = []
    writes = []

    @asynccontextmanager
    async def transaction(**kwargs):
        assert not connection.active
        before = phase_state.completed
        statements.clear()
        connection.active = True
        try:
            yield connection
        except BaseException:
            phase_state.completed = before
            raise
        finally:
            connection.active = False

    async def checkpoint(sql, *args):
        assert connection.active
        if "jsonb_set" in sql:
            assert args[0] == consumer.SOURCE_EVENT_ASSERTION
            phase_state.saved = json.loads(args[1])
            writes.append(phase_state.saved)
        else:
            if sealing != "legacy":
                assert statements == []
                assert json.loads(args[4]) == phase_state.saved
                if sealing == "zero":
                    return "UPDATE 0"
            phase_state.completed = json.loads(args[1])
        statements.append("write")
        return "UPDATE 1"

    original_fetch = connection.fetchval

    async def fetch(sql, *args):
        statements.append("read")
        if "validation_json" in sql:
            return phase_state.saved
        return await original_fetch(sql, *args)

    async def pin_source(*args, **kwargs):
        statements.append("pin")
        return approved

    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(side_effect=pin_source))
    connection.transaction = transaction
    connection.fetchval = fetch
    connection.execute = AsyncMock(side_effect=checkpoint)
    _install_ptg_seal(cfg, phase_state, connection, copy_target, sealing)
    if sealing == "mismatch":
        phase_state.saved = {"wrong": True}
    return connection, phase_state, writes


def _ptg_publish_outcome(phase_state, sealing):
    """Keep final publication failure and cancellation after the checkpoint assertion."""

    async def publish_result(*args, **kwargs):
        assert phase_state.completed is not None
        if sealing == "publish_failure":
            raise NetworkPublicationError("Expected serving head changed before publication")
        if sealing == "publish_cancel":
            raise asyncio.CancelledError

    return publish_result


async def _prepared_ptg_attempt(monkeypatch, changed, sealing):
    """Prepare the selected source, candidate and transactional fake for one retry scenario."""
    from process.network_address_projection import PinnedAddressSource
    from process.network_membership_copy import MembershipCopyTarget
    from process.network_serving_read import PinnedNetworkServingManifest

    selected = await recipe()
    old = replace(selected, source_pin=replace(selected.source_pin, capture_id=str(UUID(int=3))))
    pin = PinnedNetworkServingManifest(1, str(UUID(int=2)), "retained", 1, {}, 1, "a" * 64, 5)
    approved = SimpleNamespace(approved_revision=1, generation_id="b" * 64)
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "_selected", AsyncMock(return_value="binding"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(old,)))
    monkeypatch.setattr(consumer, "_replacement", AsyncMock(return_value=(selected,)))
    guarded = AsyncMock()
    monkeypatch.setattr(consumer, "require_selected_ptg_office_recipes", guarded)
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=i)) for i in (1, 2, 3, 4)), "network_candidate_" + UUID(int=4).hex
    )
    address = PinnedAddressSource("addresses", "entity_address_unified", "c" * 64)
    compose = AsyncMock(return_value=(copy_target, address))
    runtime = SimpleNamespace(compose=compose)
    cfg = SimpleNamespace(
        control_schema="office_control", composition_runtime=runtime, writer_roles={}, seal_candidate=None
    )
    prepare, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(consumer, "prepare_network_candidate", prepare)
    monkeypatch.setattr(consumer, "publish_network_candidate", publish)
    monkeypatch.setattr(consumer, "_notice", AsyncMock(return_value=old if changed else selected))
    connection, phase_state, writes = _ptg_attempt_checkpoint(monkeypatch, copy_target, approved, sealing, cfg)
    arguments = (
        connection,
        None,
        STORE,
        "office_reader",
        IDENTITY,
        {"event_id": str(UUID(int=9)), "header": header(native_event())},
        selected,
        cfg,
    )

    publish.side_effect = _ptg_publish_outcome(phase_state, sealing)
    return SimpleNamespace(
        arguments=arguments,
        publish=publish,
        guarded=guarded,
        prepare=prepare,
        compose=compose,
        connection=connection,
        phase_state=phase_state,
        writes=writes,
        cfg=cfg,
        copy_target=copy_target,
        selected=selected,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
@pytest.mark.parametrize(
    "sealing",
    ["legacy", "complete", "resume", "mismatch", "failure", "cancel", "publish_failure", "publish_cancel", "zero"],
)
async def test_positive_attempt_uses_existing_pipeline_and_rechecks_source_before_atomic_publish(
    monkeypatch, changed, sealing
):
    from process.network_address_projection import PinnedAddressSource
    from process.network_membership_copy import MembershipCopyTarget
    from process.network_serving_read import PinnedNetworkServingManifest

    scenario = await _prepared_ptg_attempt(monkeypatch, changed, sealing)
    arguments, publish, guarded, prepare, compose = (
        scenario.arguments,
        scenario.publish,
        scenario.guarded,
        scenario.prepare,
        scenario.compose,
    )
    connection, phase_state, writes, cfg = scenario.connection, scenario.phase_state, scenario.writes, scenario.cfg
    copy_target, selected = scenario.copy_target, scenario.selected
    if changed or sealing in {"mismatch", "failure", "cancel", "publish_failure", "publish_cancel", "zero"}:
        with pytest.raises(
            asyncio.CancelledError
            if sealing == "cancel" or (sealing == "publish_cancel" and not changed)
            else ValueError
        ):
            await consumer._attempt(*arguments)
        assert publish.await_count == int(sealing in {"publish_failure", "publish_cancel"} and not changed)
        assert phase_state.completed is None
        assert len(writes) == int(sealing not in {"legacy", "mismatch"})
    else:
        assert await consumer._attempt(*arguments) == {"state": "completed"}
        publish.assert_awaited_once_with(connection, copy_target, control_schema="office_control")
        assert guarded.await_count == 2
        assert "network_membership_candidate" in connection.execute.await_args.args[0]
        if sealing == "resume":
            phase_state.completed = None
            assert await consumer._attempt(*arguments) == {"state": "completed"}
            assert len(writes) == 1 and cfg.seal_candidate.await_count == 2
    address_sources = compose.await_args.kwargs["address_sources"]
    assert address_sources.replacement_source_recipes == (selected,) and address_sources.initial_source_recipes == ()
    assert prepare.await_count == (2 if sealing == "resume" and not changed else 1)


def test_ptg_custody_callback_is_runtime_only_and_hidden():
    callback = AsyncMock()
    assert replace(config(), seal_candidate=callback).seal_candidate is callback
    assert "AsyncMock" not in repr(replace(config(), seal_candidate=callback))
    with pytest.raises(ValueError):
        replace(config(), seal_candidate={})


def test_ptg_worker_json_cannot_select_custody_callback():
    from process.registry_ptg_source_worker import parse_registry_ptg_source_worker_config
    from tests.test_registry_ptg_source_worker import configuration

    value = configuration()
    value["consumer"]["seal_candidate"] = {}
    with pytest.raises(ValueError):
        parse_registry_ptg_source_worker_config(json.dumps(value))
