# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real checkpoint races through retained CMS composition and protected publication."""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_cms_source_consumer as consumer
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_composition import (
    RegistryCompositionAddressSources,
    compose_registry_membership_candidate,
)
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes
from tests import test_registry_cms_source_consumer_postgres as support
from tests.test_provider_directory_cms_resource_batch_postgres import cms_source_template as cms_source_template

pytestmark = pytest.mark.usefixtures(cms_source_template.__name__)


async def _publisher_sessions(database, _pair, store, stack):
    # Existing publisher custody isolates publication concurrency, not ordinary reader grants.
    engine = create_async_engine(
        database.engine.url,
        isolation_level="REPEATABLE READ",
        connect_args={
            "server_settings": {"role": store.publisher_role, "default_transaction_read_only": "on"},
        },
        pool_size=1,
        max_overflow=0,
    )
    stack.push_async_callback(engine.dispose)
    return store, async_sessionmaker(engine)


class _CheckpointBarrier:
    def __init__(self, connection, phase):
        self.connection = connection
        self.phase = phase
        self.arrived = asyncio.Event()
        self.release = asyncio.Event()
        self.candidates = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, statement, *arguments):
        if "SET validation_json=validation_json||jsonb_build_object" not in statement:
            return await self.connection.execute(statement, *arguments)
        assert arguments[0] == "cms_source_notification"
        self.candidates.append(str(arguments[2]))
        if len(self.candidates) != 1:
            return await self.connection.execute(statement, *arguments)
        if self.phase == "serialization":
            result = await self.connection.execute(statement, *arguments)
        self.arrived.set()
        await self.release.wait()
        if self.phase == "cas":
            result = await self.connection.execute(statement, *arguments)
        return result


async def _competing_publication(fixture, pair, incumbent, revision):
    target, addresses = await compose_registry_membership_candidate(
        fixture.execution_connection,
        request_id=uuid4(),
        approved_revision=revision,
        expected_head=incumbent.generation_id,
        source_manifest=incumbent,
        address_sources=RegistryCompositionAddressSources(
            pair.address_source, replacement_source_recipes=(pair.recipe,)
        ),
        writer_roles={**fixture.writer_roles, "reader_roles": pair.runtime_roles},
        control_schema=fixture.control_schema,
    )
    return await prepare_and_publish_network_candidate(
        fixture.execution_connection,
        target,
        addresses,
        **{**fixture.writer_roles, "reader_roles": pair.runtime_roles},
        control_schema=fixture.control_schema,
        seal_candidate=fixture.seal_candidate,
    )


def _arguments(fixture, pair, notice, config, store, sessions, barrier):
    return dict(
        connection=barrier,
        notification_sessions=sessions,
        notification_store=store,
        source_schema="mrf",
        receipt_id=notice.pair.proof.serving_receipt_id,
        notification_sha256=notice.sha256,
        config=config,
        source_endpoint_id=pair.recipe.source_pin.endpoint_id,
    )


async def _run_competitor(database, fixture, pair, incumbent, manual, barrier, arguments_by_name):
    task = asyncio.create_task(consumer.consume_registry_cms_source_notification_once(**arguments_by_name))
    other = None
    try:
        await asyncio.wait_for(barrier.arrived.wait(), 30)
        other = await asyncpg.connect(
            database.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
        )
        await consumer._canonical_session(other)
        competitor = SimpleNamespace(**{**vars(fixture), "connection": other, "execution_connection": other})
        if barrier.phase == "serialization":
            manual = await support.custom._seed(competitor)
        else:
            competing = await _competing_publication(competitor, pair, incumbent, manual.revision)
            assert competing["generation_id"] > incumbent.generation_id
        barrier.release.set()
        completed = await asyncio.wait_for(task, 60)
        return completed, manual
    finally:
        barrier.release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if other is not None:
            await other.close()


async def _assert_retention(fixture, pair, incumbent, manual, barrier):
    current = await support._assert_members(fixture, incumbent, manual)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        retained = await resolve_retained_registry_source_recipes(
            fixture.connection, current, control_schema=fixture.control_schema
        )
        assert retained == (pair.recipe,)
        report = await fixture.connection.fetchval(
            f'SELECT validation_json FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            barrier.candidates[0],
        )
        assert "cms_source_notification" not in json.loads(report)
        assert (
            await resolve_network_serving_manifest(fixture.connection, control_schema=fixture.control_schema)
        ) == current
    return current


def _assert_failures(race, barrier):
    assert len(barrier.candidates) == 2 and len(set(barrier.candidates)) == 2
    assert len(race.failures) == 1
    if race.phase == "serialization":
        assert race.failures[0][:2] == ("SerializationError", "40001")
    else:
        assert race.failures[0][0] == "NetworkPublicationError"
        assert race.failures[0][2] == "Expected serving head changed before publication"


def _write_evidence(race, barrier, manual):
    evidence = os.environ.get("REGISTRY_RACE_EVIDENCE")
    if evidence:
        Path(evidence + "-" + race.phase + ".json").write_text(
            json.dumps(
                {
                    "phase": race.phase,
                    "real_failures": race.failures,
                    "checkpoint_attempts": len(barrier.candidates),
                    "rolled_back_checkpoint_absent": True,
                    "latest_custom_revision": manual.revision,
                    "old_head_retained": True,
                    "published_recipe_exact": True,
                    "replayed": True,
                    "scope": "safe original Registry executor / independent custody / existing Source publisher",
                },
                indent=2,
            )
            + "\n"
        )


async def _run_race(database, pair, notice, old, fixture, store, sessions, race):
    config = await support.custody.candidate_custody_consumer(fixture, pair, old, store)
    incumbent = await support._initial_registry(fixture, old)
    await support.recipes._reviewed_cms_binding(
        fixture.connection,
        fixture.control_schema,
        fixture.engine,
        pair.recipe.source_pin,
        pair.recipe.binding_coordinates,
    )
    manual = await support.custom._seed(fixture) if race.phase == "cas" else None
    barrier = _CheckpointBarrier(fixture.execution_connection, race.phase)
    arguments_by_name = _arguments(fixture, pair, notice, config, store, sessions, barrier)
    completed, manual = await _run_competitor(database, fixture, pair, incumbent, manual, barrier, arguments_by_name)
    assert completed["state"] == "completed"
    _assert_failures(race, barrier)
    current = await _assert_retention(fixture, pair, incumbent, manual, barrier)
    replay = await consumer.consume_registry_cms_source_notification_once(**arguments_by_name)
    assert replay["state"] == "replayed" and replay["manifest"]["generation_id"] == current.generation_id
    assert len(barrier.candidates) == 2
    _write_evidence(race, barrier, manual)
    return dict(arguments_by_name=arguments_by_name, generation=current.generation_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["serialization", "cas"])
async def test_checkpoint_race_retries_real_publication_with_latest_custom(monkeypatch, tmp_path, phase):
    """Pause real SQL; concurrent approval/publication commits without the source lock."""
    original_attempt = consumer._attempt
    race = SimpleNamespace(phase=phase, failures=[])

    async def observed_attempt(connection, *arguments, **keywords):
        try:
            return await original_attempt(connection, *arguments, **keywords)
        except (asyncpg.SerializationError, consumer.NetworkPublicationError) as error:
            race.failures.append((type(error).__name__, getattr(error, "sqlstate", None), str(error)))
            assert not connection.is_in_transaction()
            raise

    async def consume_pair(database, pair, notice, old, fixture, store, sessions):
        return await _run_race(database, pair, notice, old, fixture, store, sessions, race)

    monkeypatch.setattr(consumer, "_attempt", observed_attempt)
    monkeypatch.setattr(support, "_direct_reader", _publisher_sessions)
    monkeypatch.setattr(support, "_consume_pair", consume_pair)
    await support.test_committed_notification_refresh_replay_review_and_owned_lock(monkeypatch, tmp_path)
