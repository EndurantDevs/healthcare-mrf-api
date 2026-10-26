# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed trusted consumer configuration and deterministic per-source lock identity."""

from dataclasses import replace

import pytest

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig, _lock_key


def _config():
    current = RegistryNetworkSourceCoordinates(
        "fhir", "cms-npd", "source_schema", "new-dataset", "producer", "new-edition"
    )
    previous = replace(current, dataset_id="old-dataset", edition_id="old-edition")
    return RegistryCMSSourceConsumerConfig(
        "registry_control", "registry_execution", "snapshot_owner", current, previous
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_attempts", 0),
        ("max_attempts", 4),
        ("max_attempts", True),
        ("operation_seconds", 0),
        ("operation_seconds", 3601),
        ("operation_seconds", False),
        ("loader_roles", ["loader"]),
        ("reader_roles", ("reader", "reader")),
        ("execution_role", "role;bad"),
        ("coordinates", {}),
        ("prepare_address_sources", {}),
    ],
)
def test_consumer_configuration_is_closed(field, value):
    with pytest.raises(ValueError):
        replace(_config(), **{field: value})


def test_source_lock_serializes_editions_but_isolates_source_producer_and_registry():
    config = _config()
    advanced = replace(config, coordinates=replace(config.coordinates, dataset_id="successor", edition_id="next"))
    assert _lock_key(config) == _lock_key(advanced)
    assert _lock_key(config) != _lock_key(replace(config, control_schema="other_registry"))
    other = replace(config.coordinates, producer_id="other-producer")
    with pytest.raises(ValueError):
        replace(config, coordinates=other)
    assert _lock_key(config) != _lock_key(replace(config, coordinates=other, replaced_coordinates=other))


import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid5

from process import registry_cms_source_consumer as consumer
from process.network_address_projection import PinnedAddressSource
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe, canonical_registry_source_recipes


@pytest.mark.asyncio
@pytest.mark.parametrize("initial", [False, True])
@pytest.mark.parametrize("outcome", ["complete", "refused", "cancel"])
async def test_source_attempt_commits_independent_address_preparation_before_composition(monkeypatch, initial, outcome):
    import asyncio

    notice, attempt = _notice(), UUID(int=9)
    pin = None if initial else SimpleNamespace(generation_id=7)
    observed, events = {}, []

    async def prepare(**options):
        events.append("prepare")
        observed.update(options)
        assert options["request_id"] == attempt and options["approved_revision"] == 4
        assert options["source_manifest"] is pin and options["expected_head"] == (0 if initial else 7)
        sources = options["address_sources"]
        assert sources.base_source == notice.pair.address_source
        assert sources.introduced_source_recipes == (notice.pair.recipe,)
        if outcome != "complete":
            raise asyncio.CancelledError if outcome == "cancel" else ValueError("address_refused")
        return sources

    async def compose(_connection, **options):
        events.append("compose")
        assert options["address_sources"] is observed["address_sources"]
        assert options["source_manifest"] is pin and options["request_id"] == attempt
        return "target", "addresses"

    composed = AsyncMock(side_effect=compose)
    ready = AsyncMock()
    monkeypatch.setattr(consumer, "compose_registry_membership_candidate", composed)
    monkeypatch.setattr(consumer, "prepare_network_candidate", ready)
    config = replace(_config(), prepare_address_sources=prepare)
    operation = consumer._prepare_source_candidate(object(), notice, config, attempt, 4, pin, (notice.pair.recipe,))
    if outcome == "complete":
        assert await operation == "target"
        assert events == ["prepare", "compose"]
        ready.assert_awaited_once()
    else:
        with pytest.raises(asyncio.CancelledError if outcome == "cancel" else ValueError):
            await operation
        assert events == ["prepare"]
        composed.assert_not_awaited()
        ready.assert_not_awaited()


def _recipe(dataset, edition, *, endpoint="endpoint-one", producer="producer"):
    pin = PinnedFHIRMembershipSource(
        "source_schema",
        "cms-npd",
        endpoint,
        dataset,
        "a" * 64,
        "release",
        100,
        "scope",
        "2026-01-01",
        custody_owner_role="source_owner",
        custody_runtime_roles=("source_reader",),
        custody_proof_sha256="f" * 64,
        custody_catalog_sha256="e" * 64,
    )
    coordinates = RegistryNetworkSourceCoordinates("fhir", "cms-npd", "source_schema", dataset, producer, edition)
    return RegistrySourceMembershipRecipe(pin, coordinates)


def _notice():
    return SimpleNamespace(
        sha256="b" * 64,
        pair=SimpleNamespace(
            recipe=_recipe("future-dataset", "future-edition"),
            runtime_roles=(),
            pair_sha256="c" * 64,
            address_source=PinnedAddressSource("retained_addresses", "entity_address_unified", "edition"),
        ),
    )


class _Connection:
    def __init__(self, rows=(), head=1):
        self.rows, self.head, self.calls = rows, head, []

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        assert "LIMIT 2" in query and "notification_sha256" in query
        return self.rows

    async def fetchval(self, query):
        self.calls.append((query, ()))
        return self.head

    @asynccontextmanager
    async def transaction(self, **kwargs):
        assert kwargs == dict(isolation="repeatable_read", readonly=True)
        yield self


def _published_checkpoint(notice, config):
    event = consumer._event(notice, config)
    recipes = canonical_registry_source_recipes((notice.pair.recipe,))
    generations_by_source = dict(retained_network_source="d" * 64, custom_membership="e" * 64)
    attempt = uuid5(
        UUID(event["event_id"]),
        consumer._json(
            (generations_by_source["retained_network_source"], generations_by_source["custom_membership"], recipes)
        ),
    )
    target = consumer._target(attempt, 5, 7, generations_by_source)
    return dict(
        dataset_id=target.dataset_id,
        schema_id=target.schema_id,
        producer_id=target.producer_id,
        candidate_id=target.candidate_id,
        schema_name=target.schema_name,
        approved_custom_revision=5,
        expected_head=7,
        source_generations=generations_by_source,
        source_recipes_json=recipes,
        validation_json={
            consumer._CHECKPOINT: event | {"attempt_id": str(attempt)},
            "address_source": asdict(PinnedAddressSource("retained_address", "entity_address_unified", "edition")),
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("slots", [0, 1, 2])
async def test_periodic_future_edition_uses_actual_unique_retained_slot(monkeypatch, slots):
    notice = _notice()
    config = _config()
    old = _recipe("actual-old", "actual-old-edition")
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(old,) * slots))
    resolved, replay = await consumer._periodic_config(_Connection(), notice, config, "endpoint-one")
    if slots != 1:
        assert resolved is replay is None
    else:
        assert resolved.coordinates == notice.pair.recipe.binding_coordinates
        assert resolved.replaced_coordinates == old.binding_coordinates
        assert resolved.replaced_coordinates != config.replaced_coordinates and replay == []


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["family", "endpoint", "foreign_old_endpoint"])
async def test_periodic_scope_requires_review(monkeypatch, damage):
    notice = _notice()
    config = _config()
    old = _recipe("old", "old-edition")
    if damage == "family":
        config = replace(config, coordinates=replace(config.coordinates, dataset_schema="other_source"))
    if damage == "foreign_old_endpoint":
        old = _recipe("old", "old-edition", endpoint="other-endpoint")
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(old,)))
    connection = _Connection(head=None if damage == "empty_head" else 1)
    assert await consumer._periodic_config(
        connection, notice, config, "foreign" if damage == "endpoint" else "endpoint-one"
    ) == (None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "pair", "old_family", "old_extra", "recipe", "duplicate"])
async def test_periodic_checkpoint_recovers_exact_old_before_current_head(monkeypatch, damage):
    notice = _notice()
    original = replace(
        _config(),
        coordinates=notice.pair.recipe.binding_coordinates,
        replaced_coordinates=_recipe("saved-old", "saved-old-edition").binding_coordinates,
    )
    row = _published_checkpoint(notice, original)
    checkpoint = row["validation_json"][consumer._CHECKPOINT]
    if damage == "pair":
        checkpoint["pair_sha256"] = "f" * 64
    elif damage == "old_family":
        checkpoint["replaced_coordinates"]["producer_id"] = "foreign"
    elif damage == "old_extra":
        checkpoint["replaced_coordinates"]["unknown"] = "value"
    elif damage == "recipe":
        row["source_recipes_json"] = canonical_registry_source_recipes((_recipe("other", "other-edition"),))
    connection = _Connection(rows=[row, row] if damage == "duplicate" else [row], head=None)
    head = AsyncMock(side_effect=AssertionError("Replay must precede any current-head lookup"))
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", head)
    if damage:
        with pytest.raises(ValueError):
            await consumer._periodic_config(connection, notice, _config(), "endpoint-one")
    else:
        resolved, rows = await consumer._periodic_config(connection, notice, _config(), "endpoint-one")
        assert resolved == original and rows == [row] and len(connection.calls) == 1
    head.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["stale", "missing_mapping", "replay"])
async def test_periodic_consume_preserves_replay_staleness_and_review(monkeypatch, outcome):
    notice = _notice()
    config = _config()
    monkeypatch.setattr(consumer, "_notification", AsyncMock(return_value=notice))
    monkeypatch.setattr(
        consumer,
        "_periodic_config",
        AsyncMock(return_value=(replace(config, coordinates=notice.pair.recipe.binding_coordinates), [])),
    )
    monkeypatch.setattr(
        consumer, "_replay", AsyncMock(return_value={"state": "replayed"} if outcome == "replay" else None)
    )
    current = AsyncMock(side_effect=ValueError() if outcome == "stale" else None)
    attempt = AsyncMock(return_value={"state": "review_required"})
    monkeypatch.setattr(consumer, "_current_source", current)
    monkeypatch.setattr(consumer, "_attempt", attempt)
    result = await consumer._consume(
        object(), object(), object(), "source_schema", "a" * 64, "b" * 64, config, "endpoint-one"
    )
    assert (
        result["state"]
        == {"stale": "stale_source", "missing_mapping": "review_required", "replay": "replayed"}[outcome]
    )
    if outcome == "replay":
        current.assert_not_awaited()
    if outcome != "missing_mapping":
        attempt.assert_not_awaited()


@pytest.mark.asyncio
async def test_future_event_requires_new_exact_approved_mapping_before_composition(monkeypatch):
    notice = _notice()
    config = _config()
    connection = _Connection()
    resolved = replace(config, coordinates=notice.pair.recipe.binding_coordinates)
    monkeypatch.setattr(consumer, "_notification", AsyncMock(return_value=notice))
    monkeypatch.setattr(consumer, "_periodic_config", AsyncMock(return_value=(resolved, [])))
    monkeypatch.setattr(consumer, "_current_source", AsyncMock())
    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=object()))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=object()))
    approved = AsyncMock(return_value=0)
    compose = AsyncMock(side_effect=AssertionError("Missing new-edition mapping must not compose"))
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", approved)
    monkeypatch.setattr(consumer, "compose_registry_membership_candidate", compose)
    result = await consumer._consume(
        connection, object(), object(), "source_schema", "a" * 64, notice.sha256, config, "endpoint-one"
    )
    assert result["state"] == "review_required"
    assert approved.await_args.args[2] == notice.pair.recipe.binding_coordinates
    compose.assert_not_awaited()


@pytest.mark.asyncio
async def test_recovered_checkpoint_reuses_exact_physical_replay_target(monkeypatch):
    notice = _notice()
    config = replace(
        _config(),
        coordinates=notice.pair.recipe.binding_coordinates,
        replaced_coordinates=_recipe("saved-old", "saved-edition").binding_coordinates,
    )
    row = _published_checkpoint(notice, config)
    publisher = AsyncMock(return_value={"retained": "exact"})
    monkeypatch.setattr(consumer, "prepare_and_publish_network_candidate", publisher)
    connection = _Connection([row])
    result = await consumer._replay(connection, consumer._event(notice, config), config, [row])
    assert result["state"] == "replayed" and connection.calls == []
    assert publisher.await_args.args[1].candidate_id == row["candidate_id"]


class _PhaseConnection(_Connection):
    def __init__(self):
        super().__init__()
        self.active = False
        self.phases = []
        self.checkpoints = []

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **kwargs):
        self.active = True
        try:
            yield self
        except BaseException:
            self.phases.append("rollback")
            raise
        finally:
            self.active = False

    async def execute(self, query, *args):
        self.checkpoints.append(json.loads(args[1]))


@pytest.mark.asyncio
async def test_periodic_attempt_uses_competitor_slot_and_latest_map_from_same_snapshot(monkeypatch):
    notice = _notice()
    old_a = _recipe("old-a", "edition-a")
    old_b = _recipe("old-b", "edition-b")
    other = _recipe("other", "other-edition", endpoint="other-endpoint")
    config = replace(
        _config(), coordinates=notice.pair.recipe.binding_coordinates, replaced_coordinates=old_a.binding_coordinates
    )
    connection = _PhaseConnection()
    pin = SimpleNamespace(generation_id=9, manifest_sha256="d" * 64)
    approved = SimpleNamespace(generation_id="e" * 64)

    async def recipes(*args, **kwargs):
        assert connection.active
        return (old_b, other)

    monkeypatch.setattr(consumer, "resolve_network_serving_manifest", AsyncMock(return_value=pin))
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", AsyncMock(return_value=1))
    monkeypatch.setattr(consumer, "resolve_retained_registry_source_recipes", recipes)

    async def compose(*args, **kwargs):
        assert not connection.active
        return consumer._target(kwargs["request_id"], 1, pin.generation_id, {}), PinnedAddressSource(
            "addresses", "entity_address_unified", "edition"
        )

    composer = AsyncMock(side_effect=compose)
    monkeypatch.setattr(consumer, "compose_registry_membership_candidate", composer)
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())
    monkeypatch.setattr(consumer, "publish_network_candidate", AsyncMock(return_value={"published": "C"}))
    outcome = await consumer._attempt(connection, notice, consumer._event(notice, config), config, "endpoint-one")
    saved = connection.checkpoints[0]
    assert outcome["state"] == "completed" and saved["replaced_coordinates"] == asdict(old_b.binding_coordinates)
    supplied = composer.await_args.kwargs
    assert supplied["expected_head"] == 9
    assert supplied["address_sources"].replacement_source_recipes == (notice.pair.recipe, other)
    expected = consumer._event(notice, replace(config, replaced_coordinates=old_b.binding_coordinates))
    assert saved["event_id"] == expected["event_id"]
    expected_attempt = uuid5(
        UUID(expected["event_id"]),
        consumer._json(
            (
                pin.manifest_sha256,
                approved.generation_id,
                canonical_registry_source_recipes((notice.pair.recipe, other)),
            )
        ),
    )
    assert supplied["request_id"] == expected_attempt and supplied["approved_revision"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["cas", "approval", "serialization", "privilege", "invalid", "exhausted", "open_transaction"]
)
async def test_periodic_retry_rechecks_checkpoint_after_exact_rolled_back_conflict(monkeypatch, failure):
    notice = _notice()
    config = replace(_config(), coordinates=notice.pair.recipe.binding_coordinates)
    connection = _PhaseConnection()
    monkeypatch.setattr(consumer, "_notification", AsyncMock(return_value=notice))
    discover = AsyncMock(return_value=(config, []))
    monkeypatch.setattr(consumer, "_periodic_config", discover)
    monkeypatch.setattr(consumer, "_replay", AsyncMock(return_value=None))
    monkeypatch.setattr(consumer, "_current_source", AsyncMock())
    errors_by_case = {
        "cas": consumer.NetworkPublicationError("Expected serving head changed before publication"),
        "approval": consumer.ApprovedMembershipSourceError(
            "Approved source revision no longer matches the control pin"
        ),
        "serialization": consumer.asyncpg.SerializationError("synthetic"),
        "privilege": consumer.asyncpg.InsufficientPrivilegeError("synthetic"),
        "invalid": ValueError("synthetic"),
        "exhausted": consumer.asyncpg.SerializationError("synthetic"),
        "open_transaction": consumer.asyncpg.SerializationError("synthetic"),
    }
    calls = []

    async def attempt(*args):
        calls.append(args)
        if len(calls) > 1 and failure != "exhausted":
            return {"state": "completed"}
        if failure == "open_transaction":
            connection.active = True
            raise errors_by_case[failure]
        async with connection.transaction(isolation="repeatable_read"):
            raise errors_by_case[failure]

    monkeypatch.setattr(consumer, "_attempt", attempt)
    if failure in {"privilege", "invalid", "open_transaction"}:
        with pytest.raises(type(errors_by_case[failure])):
            await consumer._consume(
                connection, object(), object(), "source_schema", "a" * 64, notice.sha256, config, "endpoint-one"
            )
        assert discover.await_count == len(calls) == 1
    else:
        outcome = await consumer._consume(
            connection, object(), object(), "source_schema", "a" * 64, notice.sha256, config, "endpoint-one"
        )
        assert outcome["state"] == ("conflict" if failure == "exhausted" else "completed")
        assert discover.await_count == len(calls) == config.max_attempts
        assert len(connection.phases) == (2 if failure == "exhausted" else 1)


class _CMSCheckpointConnection(_PhaseConnection):
    def __init__(self, expected_by_name, outcome, copy_target, phases):
        super().__init__()
        self.expected_by_name, self.outcome, self.copy_target, self.event_phases = (
            expected_by_name,
            outcome,
            copy_target,
            phases,
        )
        self.row = (
            {consumer.SOURCE_EVENT_ASSERTION: expected_by_name}
            if outcome == "resume"
            else ({consumer.SOURCE_EVENT_ASSERTION: {"wrong": True}} if outcome == "mismatch" else {})
        )

    @asynccontextmanager
    async def transaction(self, **options):
        import copy

        before = copy.deepcopy(self.row)
        self.active, self.statements = True, []
        try:
            yield self
        except BaseException:
            self.row = before
            raise
        finally:
            self.active = False

    async def fetchval(self, sql, *args):
        self.statements.append("read")
        if "validation_json" in sql:
            return self.row.get(consumer.SOURCE_EVENT_ASSERTION)
        return await super().fetchval(sql)

    async def execute(self, sql, *args):
        assert self.active
        if "jsonb_set" in sql:
            assert args[0] == consumer.SOURCE_EVENT_ASSERTION
            self.row[args[0]] = json.loads(args[1])
            self.event_phases.append("assertion")
        else:
            assert self.statements == []  # The original race barrier must precede every RR read.
            assert args[0] == consumer._CHECKPOINT and args[2] == UUID(self.copy_target.candidate_id)
            assert json.loads(args[4]) == self.row[args[3]] == self.expected_by_name
            self.event_phases.append("checkpoint")
            if self.outcome == "zero":
                return "UPDATE 0"
            self.row[args[0]] = json.loads(args[1])
        self.statements.append("write")
        return "UPDATE 1"


def _cms_checkpoint_callbacks(monkeypatch, connection, expected_by_name, outcome, copy_target, phases):
    """Assert seal, source and final publish ordering for every CMS refusal phase."""
    import asyncio

    async def publish(*args, **kwargs):
        assert connection.row[consumer._CHECKPOINT] == expected_by_name["checkpoint"]
        if outcome == "cas":
            raise consumer.NetworkPublicationError("Expected serving head changed before publication")
        if outcome == "serialization":
            raise consumer.asyncpg.SerializationError("synthetic conflict")
        if outcome == "publish_cancel":
            raise asyncio.CancelledError
        return {"generation_id": 10}

    publisher = AsyncMock(side_effect=publish)
    monkeypatch.setattr(consumer, "publish_network_candidate", publisher)

    async def seal(actual):
        assert actual == copy_target and not connection.active
        assert connection.row == {consumer.SOURCE_EVENT_ASSERTION: expected_by_name}
        phases.append("seal")
        if outcome == "failure":
            raise ValueError("custody_changed")
        if outcome == "cancel":
            raise asyncio.CancelledError

    async def source():
        assert not connection.active and phases[-1] == "seal"
        phases.append("source")
        if outcome == "source":
            raise ValueError("source_changed")

    return publisher, seal, source


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    ["complete", "resume", "mismatch", "failure", "cancel", "source", "cas", "serialization", "publish_cancel", "zero"],
)
async def test_sealed_attempt_binds_provisional_assertion_and_commits_checkpoint_only_with_publication(
    monkeypatch, outcome
):
    import asyncio

    notice = _notice()
    cfg = replace(_config(), coordinates=notice.pair.recipe.binding_coordinates)
    event = consumer._event(notice, cfg)
    pin = SimpleNamespace(generation_id=9, manifest_sha256="d" * 64)
    approved = SimpleNamespace(generation_id="e" * 64)
    selected = (pin, (notice.pair.recipe,), event)
    attempt = uuid5(
        UUID(event["event_id"]),
        consumer._json((pin.manifest_sha256, approved.generation_id, canonical_registry_source_recipes(selected[1]))),
    )
    expected_by_name = {"revision": 1, "kind": "cms", "checkpoint": {**event, "attempt_id": str(attempt)}}
    phases = []

    copy_target = consumer._target(attempt, 1, 9, {})
    connection = _CMSCheckpointConnection(expected_by_name, outcome, copy_target, phases)
    monkeypatch.setattr(consumer, "pin_approved_membership_source", AsyncMock(return_value=approved))
    bindings = AsyncMock(return_value=1)
    monkeypatch.setattr(consumer, "require_approved_network_source_bindings", bindings)
    selection = AsyncMock(return_value=selected)
    monkeypatch.setattr(consumer, "_attempt_sources", selection)
    monkeypatch.setattr(
        consumer,
        "compose_registry_membership_candidate",
        AsyncMock(return_value=(copy_target, notice.pair.address_source)),
    )
    monkeypatch.setattr(consumer, "prepare_network_candidate", AsyncMock())

    publisher, seal, source_recheck = _cms_checkpoint_callbacks(
        monkeypatch, connection, expected_by_name, outcome, copy_target, phases
    )
    cfg = replace(cfg, seal_candidate=seal)
    if outcome in {"complete", "resume"}:
        assert (await consumer._attempt(connection, notice, event, cfg, recheck_source=source_recheck))[
            "state"
        ] == "completed"
        assert phases == (["assertion"] if outcome == "complete" else []) + ["seal", "source", "checkpoint"]
        assert bindings.await_count == selection.await_count == 1
    else:
        error = (
            asyncio.CancelledError
            if outcome in {"cancel", "publish_cancel"}
            else consumer.NetworkPublicationError
            if outcome == "cas"
            else consumer.asyncpg.SerializationError
            if outcome == "serialization"
            else ValueError
        )
        with pytest.raises(error):
            await consumer._attempt(connection, notice, event, cfg, recheck_source=source_recheck)
        assert consumer._CHECKPOINT not in connection.row
        assert publisher.await_count == int(outcome in {"cas", "serialization", "publish_cancel"})
    if outcome == "mismatch":
        assert phases == []


def test_cms_custody_callback_is_runtime_only_and_hidden():
    callback = AsyncMock()
    assert replace(_config(), seal_candidate=callback).seal_candidate is callback
    assert "AsyncMock" not in repr(replace(_config(), seal_candidate=callback))
    with pytest.raises(ValueError):
        replace(_config(), seal_candidate={})


def test_cms_worker_json_cannot_select_custody_callback():
    from process.registry_cms_source_worker import parse_registry_cms_source_worker_config
    from tests.test_registry_cms_source_worker import configuration

    value = configuration()
    value["consumer"]["seal_candidate"] = {}
    with pytest.raises(ValueError):
        parse_registry_cms_source_worker_config(json.dumps(value))
