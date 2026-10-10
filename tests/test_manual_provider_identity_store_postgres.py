# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable manual identities, source reuse and atomic edits on migrated native tables."""

import asyncio
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_record_store as store
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def provider_db(serving_schema):
    connection, schema, engine = serving_schema
    await connection.execute(f'CREATE TABLE "{schema}".npi (npi bigint PRIMARY KEY)')
    yield connection, schema, async_sessionmaker(engine), engine


def _actor():
    return RegistryActor("platform_admin", uuid4(), "client_example")


def _create(**changes):
    command = RegistryRecordCommand(
        "provider",
        uuid4(),
        "create",
        0,
        {
            "display_name": " Example Provider ",
            "provider_kind": "individual",
            "aliases": [" B ", "A", "A"],
            "npi": None,
        },
        "Explicit provider identity",
        uuid4().hex,
    )
    return replace(command, **changes)


async def _apply(sessions, schema, command, actor):
    async with sessions() as session, session.begin():
        return await apply_registry_record_command(session, command, actor, schema=schema, source_schema=schema)


async def test_source_npi_is_separate_from_protected_registry_namespace(provider_db):
    connection, schema, sessions, _ = provider_db
    source_schema = "manual_source_test_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{source_schema}"')
        await connection.execute(f'CREATE TABLE "{source_schema}".npi (npi bigint PRIMARY KEY)')
        await connection.execute(f'INSERT INTO "{source_schema}".npi VALUES (1000000004)')
        command = _create(fields={**_create().fields, "npi": "1000000004"})
        async with sessions() as session, session.begin():
            with pytest.raises(RegistryRecordConflict, match="reuse_required"):
                await apply_registry_record_command(
                    session, command, _actor(), schema=schema, source_schema=source_schema
                )
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry') == 0
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".npi') == 0
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", source_schema) is None


async def test_missing_npi_distinct_same_name_and_bounded_reads(provider_db):
    _, schema, sessions, _ = provider_db
    actor, command = _actor(), _create()
    first = await _apply(sessions, schema, command, actor)
    second = await _apply(sessions, schema, _create(), actor)
    assert first["record_id"] != second["record_id"]
    assert first["record"]["display_name"] == second["record"]["display_name"] == "Example Provider"
    assert first["record"] == {
        "provider_id": str(command.record_id),
        "display_name": "Example Provider",
        "provider_kind": "individual",
        "aliases": ["A", "B"],
        "npi": None,
        "archived": False,
        "revision": 1,
        "created_at": first["record"]["created_at"],
    }
    async with sessions() as session:
        assert await get_registry_record(session, "provider", command.record_id, schema=schema) == first["record"]
        assert await get_registry_record(session, "provider", uuid4(), schema=schema) is None
        assert len(await list_registry_records(session, "provider", limit=1, schema=schema)) == 1
        assert len(await list_registry_records(session, "provider", offset=1, schema=schema)) == 1
        assert await list_registry_records(session, "provider", record_ids=[command.record_id], schema=schema) == [
            first["record"]
        ]
        for parameters in [{"limit": 101}, {"offset": -1}, {"offset": 1000001}, {"record_ids": [uuid4()] * 101}]:
            with pytest.raises(ValueError):
                await list_registry_records(session, "provider", schema=schema, **parameters)
        with pytest.raises(ValueError, match="duplicate"):
            await list_registry_records(session, "provider", record_ids=[command.record_id] * 2, schema=schema)


async def test_provider_lifecycle_immutable_replay_and_stale_revision(provider_db):
    connection, schema, sessions, _ = provider_db
    actor = _actor()
    command = _create(fields={**_create().fields, "npi": "1000000004"})
    first = await _apply(sessions, schema, command, actor)
    corrected = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**command.fields, "display_name": "Corrected Provider", "provider_kind": "organization", "npi": None},
    )
    correction = await _apply(sessions, schema, corrected, actor)
    assert correction["revision"] == correction["custom_revision"] == 2
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES (1000000004)')
    assert await _apply(sessions, schema, command, actor) == first
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(sessions, schema, replace(corrected, idempotency_key=uuid4().hex), actor)
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(sessions, schema, replace(command, reason="Changed retry"), actor)
    for operation, expected_revision, archived in [("archive", 2, True), ("restore", 3, False)]:
        change = replace(
            command, operation=operation, expected_revision=expected_revision, fields={}, idempotency_key=uuid4().hex
        )
        retained = await _apply(sessions, schema, change, actor)
        assert retained["record"]["archived"] is archived
        assert retained["record"]["provider_id"] == str(command.record_id)
        assert retained["record"]["provider_kind"] == "organization"
        assert retained["record"]["display_name"] == "Corrected Provider"
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 4
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (4, 0)


@pytest.mark.parametrize(
    "changed_fields",
    [
        {"display_name": "x" * 257},
        {"display_name": " "},
        {"provider_kind": "unknown"},
        {"provider_kind": []},
        {"aliases": ["x"] * 101},
        {"aliases": ["x" * 513]},
        {"aliases": [None]},
        {"npi": "1000000005"},
        {"npi": "3000000002"},
        {"npi": 1000000004},
        {"npi": " 1000000004"},
        {"npi": "１００００００００４"},
        {"npi": ""},
        {"extra": True},
    ],
)
async def test_invalid_provider_fields_never_persist(provider_db, changed_fields):
    connection, schema, sessions, _ = provider_db
    command = _create()
    with pytest.raises(ValueError):
        await _apply(sessions, schema, replace(command, fields={**command.fields, **changed_fields}), _actor())
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


async def test_source_manual_claims_use_locked_set_query(provider_db):
    connection, schema, sessions, engine = provider_db
    actor, command = _actor(), _create()
    first = await _apply(sessions, schema, command, actor)
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES (1000000004)')
    correction = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**command.fields, "npi": "1000000004"},
    )
    statements = []

    def observe_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", observe_statement)
    try:
        with pytest.raises(RegistryRecordConflict, match="npi_reuse_required"):
            await _apply(sessions, schema, correction, actor)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", observe_statement)
    claim_queries = [
        statement for statement in statements if ".npi " in statement and ".manual_provider_registry " in statement
    ]
    assert len(claim_queries) == 1 and claim_queries[0].count("EXISTS") == 2
    assert next(index for index, statement in enumerate(statements) if "FOR UPDATE" in statement) < statements.index(
        claim_queries[0]
    )
    with pytest.raises(RegistryRecordConflict, match="npi_reuse_required"):
        await _apply(sessions, schema, _create(fields=correction.fields), actor)
    claimant = _create(fields={**command.fields, "npi": "1000000012"})
    claimed = await _apply(sessions, schema, claimant, actor)
    with pytest.raises(RegistryRecordConflict, match="npi_claimed"):
        await _apply(sessions, schema, _create(fields=claimant.fields), actor)
    await _apply(
        sessions,
        schema,
        replace(claimant, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
        actor,
    )
    with pytest.raises(RegistryRecordConflict, match="npi_claimed"):
        await _apply(sessions, schema, _create(fields=claimant.fields), actor)
    async with sessions() as session:
        assert await get_registry_record(session, "provider", command.record_id, schema=schema) == first["record"]
        assert (await get_registry_record(session, "provider", claimant.record_id, schema=schema))["npi"] == claimed[
            "record"
        ]["npi"]


async def test_restore_rechecks_source_identity_and_exact_self_claim_is_allowed(provider_db):
    connection, schema, sessions, _ = provider_db
    actor = _actor()
    command = _create(fields={**_create().fields, "npi": "1000000004"})
    await _apply(sessions, schema, command, actor)
    correction = replace(command, operation="correct", expected_revision=1, idempotency_key=uuid4().hex)
    assert (await _apply(sessions, schema, correction, actor))["record"]["npi"] == "1000000004"
    archive = replace(command, operation="archive", expected_revision=2, fields={}, idempotency_key=uuid4().hex)
    await _apply(sessions, schema, archive, actor)
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES (1000000004)')
    with pytest.raises(RegistryRecordConflict, match="npi_reuse_required"):
        await _apply(
            sessions,
            schema,
            replace(archive, operation="restore", expected_revision=3, idempotency_key=uuid4().hex),
            actor,
        )
    assert await connection.fetchval(f'SELECT archived FROM "{schema}".manual_provider_registry') is True
    assert await connection.fetchval(f'SELECT revision FROM "{schema}".manual_provider_registry') == 3


async def test_concurrent_npi_claims_have_one_identity_and_history(provider_db):
    connection, schema, sessions, _ = provider_db
    actor, fields = _actor(), {**_create().fields, "npi": "1000000004"}
    outcomes = await asyncio.gather(
        *[_apply(sessions, schema, _create(fields=fields), actor) for _ in range(2)], return_exceptions=True
    )
    assert sum(isinstance(outcome, dict) for outcome in outcomes) == 1
    assert (
        sum(isinstance(outcome, RegistryRecordConflict) and "npi_claimed" in str(outcome) for outcome in outcomes) == 1
    )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 1


async def test_native_unique_race_is_a_conflict_and_savepoint_recovers(provider_db, monkeypatch):
    connection, schema, sessions, _ = provider_db
    original_validation = store._validate_provider_npi_claim

    async def race_after_validation(session, command, fields, selected_schema, source_schema):
        await original_validation(session, command, fields, selected_schema, source_schema)
        await connection.execute(
            f'INSERT INTO "{schema}".manual_provider_registry(provider_id,display_name,provider_kind,npi) VALUES($1,$2,$3,$4)',
            uuid4(),
            "Concurrent Claim",
            "individual",
            fields["npi"],
        )

    monkeypatch.setattr(store, "_validate_provider_npi_claim", race_after_validation)
    command = _create(fields={**_create().fields, "npi": "1000000004"})
    async with sessions() as session, session.begin():
        with pytest.raises(RegistryRecordConflict, match="npi_claimed"):
            await apply_registry_record_command(session, command, _actor(), schema=schema, source_schema=schema)
        assert await session.scalar(text("SELECT 1")) == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 0
    assert await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control') == 0


async def test_provider_caller_rollback_restores_head_history_and_control(provider_db):
    connection, schema, sessions, _ = provider_db
    actor, command = _actor(), _create()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, command, actor, schema=schema, source_schema=schema)
            raise RuntimeError("caller rollback")
    first = await _apply(sessions, schema, command, actor)
    correction = replace(
        command,
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**command.fields, "display_name": "Rolled Back", "npi": "1000000004"},
    )
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with sessions() as session, session.begin():
            await apply_registry_record_command(session, correction, actor, schema=schema, source_schema=schema)
            raise RuntimeError("caller rollback")
    async with sessions() as session:
        assert await get_registry_record(session, "provider", command.record_id, schema=schema) == first["record"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_record_history') == 1
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (1, 0)
