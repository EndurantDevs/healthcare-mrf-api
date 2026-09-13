# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest


source = importlib.import_module("process.entity_address_snapshot_source")


def _session(snapshot: str | None = None):
    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield

    session.begin = transaction
    if snapshot is not None:
        session.execute.side_effect = [None] * 8 + [SimpleNamespace(scalar_one=lambda: snapshot)]
    return session


def _session_factory(*sessions):
    remaining = iter(sessions)

    @asynccontextmanager
    async def scope():
        yield next(remaining)

    return scope


def _recording_session_factory(*sessions, exits: list[tuple[object, BaseException | None]]):
    remaining = iter(sessions)

    @asynccontextmanager
    async def scope():
        session = next(remaining)
        failure = None
        try:
            yield session
        except BaseException as exception:
            failure = exception
            raise
        finally:
            exits.append((session, failure))

    return scope


def test_stage_schema_is_derived_only_from_a_uuid_dataset_owner():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")

    assert source.entity_address_archive_stage_schema(dataset_id) == (
        "entity_address_archive_550e8400e29b41d4a716446655440000"
    )
    with pytest.raises(ValueError, match="UUID"):
        source.entity_address_archive_stage_schema(str(dataset_id))


@pytest.mark.asyncio
async def test_stage_export_clones_pinned_family():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    stage_schema = source.entity_address_archive_stage_schema(dataset_id)
    live, clone, stage = _session("00000003-0000001B-1"), _session(), _session("00000004-0000001C-1")
    copied = AsyncMock()

    manifest = await source.export_entity_address_archive_stage(
        _session_factory(live, clone, stage),
        schema_name="mrf",
        dataset_id=dataset_id,
        archive_copy=copied,
    )

    copied.assert_awaited_once()
    capture = copied.await_args.args[0]
    assert capture.schema_name == stage_schema
    assert capture.dataset_id == dataset_id
    assert manifest.schema_name == stage_schema
    assert manifest.relations == source.entity_address_archive_relations()
    clone_statements = [str(call.args[0]) for call in clone.execute.await_args_list]
    assert clone_statements[:3] == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'",
        f'CREATE SCHEMA "{stage_schema}"',
    ]
    assert len(clone_statements) == 17
    assert all(
        f'CREATE TABLE "{stage_schema}".' in statement or f'INSERT INTO "{stage_schema}".' in statement
        for statement in clone_statements[3:]
    )
    stage_statements = [str(call.args[0]) for call in stage.execute.await_args_list]
    assert all(f'LOCK TABLE "{stage_schema}".' in statement for statement in stage_statements[1:-1])


@pytest.mark.asyncio
async def test_stage_export_rejects_non_uuid_owner_before_opening_any_session():
    factory = AsyncMock()

    with pytest.raises(ValueError, match="UUID"):
        await source.export_entity_address_archive_stage(
            factory,
            schema_name="mrf",
            dataset_id="550e8400-e29b-41d4-a716-446655440000",
            archive_copy=AsyncMock(),
        )

    factory.assert_not_called()


@pytest.mark.asyncio
async def test_stage_callback_failure_releases_pins():
    dataset_id = UUID("550e8400-e29b-41d4-a716-446655440000")
    live, clone, stage = _session("00000003-0000001B-1"), _session(), _session("00000004-0000001C-1")
    exits: list[tuple[object, BaseException | None]] = []
    failure = RuntimeError("native dump failed")

    async def fail_archive_copy(_capture):
        raise failure

    with pytest.raises(RuntimeError, match="native dump failed"):
        await source.export_entity_address_archive_stage(
            _recording_session_factory(live, clone, stage, exits=exits),
            schema_name="mrf",
            dataset_id=dataset_id,
            archive_copy=fail_archive_copy,
        )

    assert exits == [(clone, None), (live, None), (stage, failure)]
    assert not any("DROP SCHEMA" in str(call.args[0]) for call in clone.execute.await_args_list)
