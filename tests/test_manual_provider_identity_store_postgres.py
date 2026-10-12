# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable manual identities, source reuse and atomic edits on migrated native tables."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_provider_batch as provider_batch
from process import registry_record_store as store
from process.registry_provider_batch import apply_registry_provider_batch
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

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


async def _batch(connection, schema, commands, references, actor):
    async with connection.transaction(isolation="repeatable_read"):
        return await apply_registry_provider_batch(
            connection, commands, references, actor, control_schema=schema, source_schema=schema
        )


async def _provider_state(connection, schema):
    return tuple(
        await connection.fetchrow(
            f'SELECT (SELECT count(*) FROM "{schema}".manual_provider_registry),'
            f'(SELECT count(*) FROM "{schema}".registry_record_history),draft_revision,approved_revision '
            f'FROM "{schema}".registry_revision_control WHERE id=1'
        )
    )


async def test_provider_batch_preserves_order_no_npi_references_and_one_draft_step(provider_db):
    connection, schema, sessions, _ = provider_db
    actor = _actor()
    known = _create()
    await _apply(sessions, schema, known, actor)
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
    commands = [_create(), _create()]
    references = [
        {"provider_system": "npi", "provider_id": "1000000004"},
        {"provider_system": "manual", "provider_id": str(known.record_id)},
        {"provider_system": "npi", "provider_id": "1000000004"},
    ]
    receipt = await _batch(connection, schema, commands, references, actor)
    assert receipt["references"] == references
    assert [entry["record_id"] for entry in receipt["records"]] == [str(command.record_id) for command in commands]
    assert {entry["custom_revision"] for entry in receipt["records"]} == {2}
    assert all(entry["record"]["npi"] is None for entry in receipt["records"])
    assert receipt["records"][0]["record"]["display_name"] == receipt["records"][1]["record"]["display_name"]
    assert await _provider_state(connection, schema) == (3, 3, 2, 0)
    before = await _provider_state(connection, schema)
    assert await _batch(connection, schema, [], references, actor) == {"records": [], "references": references}
    assert await _provider_state(connection, schema) == before
    assert await _batch(connection, schema, commands, references, actor) == receipt
    assert await _provider_state(connection, schema) == before


async def test_provider_batch_replays_existing_receipts_after_source_and_head_changes(provider_db):
    connection, schema, sessions, _ = provider_db
    actor = _actor()
    command = _create(fields={**_create().fields, "npi": "1000000004"})
    original = await _apply(sessions, schema, command, actor)
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
    assert (await _batch(connection, schema, [command], [], actor))["records"] == [original]
    correction = replace(
        command,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "npi": None},
        idempotency_key=uuid4().hex,
    )
    await _apply(sessions, schema, correction, actor)
    new_command = _create()
    mixed = await _batch(connection, schema, [command, new_command], [], actor)
    assert mixed["records"][0] == original and mixed["records"][1]["custom_revision"] == 3
    assert await _apply(sessions, schema, new_command, actor) == mixed["records"][1]
    before = await _provider_state(connection, schema)
    for changed, changed_actor in [
        (replace(command, reason="Changed retry"), actor),
        (command, replace(actor, user_id=uuid4())),
    ]:
        with pytest.raises(RegistryRecordConflict, match="idempotency"):
            await _batch(connection, schema, [new_command, changed], [], changed_actor)
    assert await _provider_state(connection, schema) == before


@pytest.mark.parametrize(
    "failure",
    [
        "stale",
        "source_npi",
        "duplicate_npi",
        "transfer_npi",
        "unknown_manual",
        "new_manual_reference",
        "archived_manual",
        "unknown_npi",
    ],
)
async def test_provider_batch_last_conflict_rolls_back_complete_set(provider_db, failure):
    connection, schema, sessions, _ = provider_db
    actor, first, last = _actor(), _create(), _create()
    references = []
    if failure == "stale":
        last = replace(last, operation="correct", expected_revision=1)
    if failure in {"source_npi", "duplicate_npi", "transfer_npi"}:
        last = replace(last, fields={**last.fields, "npi": "1000000004"})
    if failure == "source_npi":
        await connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
    if failure == "duplicate_npi":
        first = replace(first, fields=last.fields)
    if failure == "transfer_npi":
        known = _create(fields=last.fields)
        await _apply(sessions, schema, known, actor)
        first = replace(known, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
    if failure in {"unknown_manual", "new_manual_reference", "archived_manual"}:
        identity = first.record_id if failure == "new_manual_reference" else uuid4()
        if failure == "archived_manual":
            known = _create(record_id=identity)
            await _apply(sessions, schema, known, actor)
            await _apply(
                sessions,
                schema,
                replace(known, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
                actor,
            )
        references = [{"provider_system": "manual", "provider_id": str(identity)}]
    if failure == "unknown_npi":
        references = [{"provider_system": "npi", "provider_id": "1000000004"}]
    before = await _provider_state(connection, schema)
    with pytest.raises(RegistryRecordConflict):
        await _batch(connection, schema, [first, last], references, actor)
    assert await _provider_state(connection, schema) == before


async def test_provider_batch_all_operations_keep_exact_metadata_and_revisions(provider_db):
    connection, schema, _, _ = provider_db
    actor, creation = _actor(), _create()
    await _batch(connection, schema, [creation], [], actor)
    command = creation
    for operation, revision in [("correct", 1), ("archive", 2), ("restore", 3)]:
        command = replace(
            command,
            operation=operation,
            expected_revision=revision,
            fields=creation.fields if operation == "correct" else {},
            idempotency_key=uuid4().hex,
        )
        receipt = await _batch(connection, schema, [command], [], actor)
        assert receipt["records"][0]["revision"] == revision + 1
    events = await connection.fetch(
        f'SELECT event_json::text FROM "{schema}".registry_record_history ORDER BY revision'
    )
    assert [json.loads(entry["event_json"]) for entry in events] == [
        {"operation": operation, "before_revision": revision, "operation_context": {}}
        for revision, operation in enumerate(["create", "correct", "archive", "restore"])
    ]
    assert await _provider_state(connection, schema) == (1, 4, 4, 0)


class _CountingProviderConnection:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def __getattr__(self, name):
        original = getattr(self.connection, name)
        if name not in {"fetchrow", "fetchval", "execute", "copy_records_to_table"}:
            return original

        async def counted(*arguments, **kwargs):
            self.calls.append((name, arguments[0]))
            return await original(*arguments, **kwargs)

        return counted


async def test_provider_batch_has_constant_roundtrips_and_one_copy(provider_db):
    connection, schema, _, _ = provider_db
    actor = _actor()
    signatures = []
    for count in (1, 100):
        tracked = _CountingProviderConnection(connection)
        receipt = await _batch(tracked, schema, [_create() for _ in range(count)], [], actor)
        assert len(receipt["records"]) == count
        signatures.append([name for name, _ in tracked.calls])
        assert sum(name == "copy_records_to_table" for name, _ in tracked.calls) == 1
        assert sum("FOR UPDATE" in sql for name, sql in tracked.calls if name == "fetchrow") == 1
    assert (
        signatures
        == [["fetchrow", "execute", "copy_records_to_table", "fetchrow", "fetchval", "execute", "fetchval", "execute"]]
        * 2
    )
    assert await _provider_state(connection, schema) == (101, 101, 2, 0)


async def test_provider_batch_late_history_refusal_preserves_earlier_caller_work(provider_db):
    connection, schema, _, _ = provider_db
    first, last = _create(), _create()
    await connection.execute(
        f'ALTER TABLE "{schema}".registry_record_history ADD CONSTRAINT refuse_last_provider '
        f"CHECK(record_key<>'{last.record_id}')"
    )
    previous = uuid4()
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute(
            f'INSERT INTO "{schema}".company_group_registry '
            "(group_id,group_kind,display_name) VALUES($1,'corporate_parent','Earlier work')",
            previous,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await apply_registry_provider_batch(
                connection, [first, last], [], _actor(), control_schema=schema, source_schema=schema
            )
        assert await connection.fetchval(f'SELECT group_id FROM "{schema}".company_group_registry') == previous
        assert await _provider_state(connection, schema) == (0, 0, 0, 0)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_group_registry') == 1


async def test_provider_batch_invalid_bounds_and_canonical_references_do_no_database_work(provider_db):
    connection, schema, _, _ = provider_db
    command = _create()
    rejected_batches = [
        ([], []),
        ([command] * 2, []),
        ([_create() for _ in range(101)], []),
        ([replace(command, record_kind="company")], []),
        ([command], [{"provider_system": "npi", "provider_id": "100000000４"}]),
        ([command], [{"provider_system": "manual", "provider_id": str(uuid4()).upper()}]),
        ([command], [{"provider_system": "npi", "provider_id": 1000000004}]),
        ([command], [{"provider_system": "npi", "provider_id": "1000000004", "extra": True}]),
    ]
    huge_fields_by_name = {**command.fields, "aliases": ["x" * 512] * 100}
    rejected_batches.append(([_create(fields=huge_fields_by_name) for _ in range(30)], []))
    tracked = _CountingProviderConnection(connection)
    for commands, references in rejected_batches:
        with pytest.raises(ValueError):
            await _batch(tracked, schema, commands, references, _actor())
    with pytest.raises(ValueError, match="requires_caller_transaction"):
        await apply_registry_provider_batch(
            tracked, [command], [], _actor(), control_schema=schema, source_schema=schema
        )
    assert tracked.calls == [] and await _provider_state(connection, schema) == (0, 0, 0, 0)


@pytest.mark.parametrize("change", ["archive", "restore", "correct"])
async def test_provider_batch_references_require_preexisting_and_final_active_identity(provider_db, change):
    connection, schema, sessions, _ = provider_db
    actor, known = _actor(), _create()
    await _apply(sessions, schema, known, actor)
    expected_revision = 1
    if change == "restore":
        await _apply(
            sessions,
            schema,
            replace(known, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex),
            actor,
        )
        expected_revision = 2
    changed = replace(
        known,
        operation=change,
        expected_revision=expected_revision,
        fields=known.fields if change == "correct" else {},
        idempotency_key=uuid4().hex,
    )
    reference_by_field = {"provider_system": "manual", "provider_id": str(known.record_id)}
    before = await _provider_state(connection, schema)
    first = _create()
    if change == "correct":
        receipt = await _batch(connection, schema, [first, changed], [reference_by_field], actor)
        assert receipt["references"] == [reference_by_field] and len(receipt["records"]) == 2
    else:
        with pytest.raises(RegistryRecordConflict, match="reference_unavailable"):
            await _batch(connection, schema, [first, changed], [reference_by_field], actor)
        assert await _provider_state(connection, schema) == before


async def test_provider_batch_freezes_commands_and_references_before_await(provider_db, monkeypatch):
    connection, schema, _, _ = provider_db
    await connection.execute(f'INSERT INTO "{schema}".npi VALUES(1000000004)')
    commands = [_create()]
    references = [{"provider_system": "npi", "provider_id": "1000000004"}]
    identity = str(commands[0].record_id)
    original_stage = provider_batch._stage

    async def mutate_before_copy(*arguments):
        commands[0].fields["display_name"] = "Changed after preparation"
        commands.clear()
        references[0]["provider_id"] = "1000000012"
        return await original_stage(*arguments)

    monkeypatch.setattr(provider_batch, "_stage", mutate_before_copy)
    receipt = await _batch(connection, schema, commands, references, _actor())
    assert receipt["records"][0]["record_id"] == identity
    assert receipt["records"][0]["record"]["display_name"] == "Example Provider"
    assert receipt["references"] == [{"provider_system": "npi", "provider_id": "1000000004"}]


async def test_provider_batch_small_archive_commands_refuse_expanded_receipts_atomically(provider_db):
    connection, schema, sessions, _ = provider_db
    actor = _actor()
    aliases = [str(index).zfill(3) + "x" * 509 for index in range(100)]
    commands = []
    for _ in range(25):
        creation = _create(fields={**_create().fields, "aliases": aliases})
        await _apply(sessions, schema, creation, actor)
        commands.append(
            replace(creation, operation="archive", expected_revision=1, fields={}, idempotency_key=uuid4().hex)
        )
    before = await _provider_state(connection, schema)
    with pytest.raises(ValueError, match="output_limit"):
        await _batch(connection, schema, commands, [], actor)
    assert await _provider_state(connection, schema) == before
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".manual_provider_registry WHERE archived') == 0


async def test_provider_batch_uses_explicit_source_namespace_for_reference_and_claim(provider_db):
    connection, schema, _, _ = provider_db
    source = "provider_reference_source_" + uuid4().hex
    try:
        await connection.execute(f'CREATE SCHEMA "{source}"')
        await connection.execute(f'CREATE TABLE "{source}".npi(npi bigint PRIMARY KEY)')
        await connection.execute(f'INSERT INTO "{source}".npi VALUES(1000000004)')
        reference_by_field = {"provider_system": "npi", "provider_id": "1000000004"}
        async with connection.transaction(isolation="repeatable_read"):
            receipt = await apply_registry_provider_batch(
                connection, [], [reference_by_field], _actor(), control_schema=schema, source_schema=source
            )
        assert receipt == {"records": [], "references": [reference_by_field]}
        async with connection.transaction(isolation="repeatable_read"):
            with pytest.raises(RegistryRecordConflict, match="reuse_required"):
                await apply_registry_provider_batch(
                    connection,
                    [_create(fields={**_create().fields, "npi": "1000000004"})],
                    [],
                    _actor(),
                    control_schema=schema,
                    source_schema=source,
                )
        assert await _provider_state(connection, schema) == (0, 0, 0, 0)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", source) is None


@pytest.mark.parametrize("identity", ["not-a-uuid", "00000000-0000-0000-0000-000000000000"])
async def test_provider_batch_refuses_malformed_or_nil_manual_references(identity):
    with pytest.raises(ValueError, match="reference_invalid"):
        provider_batch._reference_pair({"provider_system": "manual", "provider_id": identity})


async def test_provider_batch_copy_count_mismatch_stops_before_validation():
    connection = SimpleNamespace(execute=AsyncMock(), copy_records_to_table=AsyncMock(return_value="COPY 0"))
    rows, _ = provider_batch._prepared_rows([_create()], [], _actor())
    with pytest.raises(ValueError, match="copy_count_invalid"):
        await provider_batch._stage(connection, "example_batch", rows)
    connection.copy_records_to_table.assert_awaited_once()
    assert connection.execute.await_count == 1


async def test_provider_batch_missing_revision_control_stops_before_copy():
    @asynccontextmanager
    async def savepoint():
        yield

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=savepoint,
        fetchrow=AsyncMock(return_value=None),
        execute=AsyncMock(),
        copy_records_to_table=AsyncMock(),
    )
    with pytest.raises(RegistryRecordConflict, match="control_unavailable"):
        await apply_registry_provider_batch(
            connection, [_create()], [], _actor(), control_schema="example_registry", source_schema="example_source"
        )
    connection.fetchrow.assert_awaited_once()
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.parametrize("failure", ["revision_exhausted", "claimed_npi", "other_unique", "write_count"])
async def test_provider_batch_failed_write_never_advances_global_revision(failure):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=0), execute=AsyncMock())
    revision = 9223372036854775807 if failure == "revision_exhausted" else 0
    expected_error, message = RegistryRecordConflict, "revision_conflict"
    if failure == "revision_exhausted":
        message = "revision_exhausted"
    elif failure in {"claimed_npi", "other_unique"}:
        error = asyncpg.UniqueViolationError("synthetic uniqueness conflict")
        error.constraint_name = "manual_provider_npi_unique" if failure == "claimed_npi" else "another_unique"
        connection.fetchval.side_effect = error
        expected_error = RegistryRecordConflict if failure == "claimed_npi" else asyncpg.UniqueViolationError
        message = "npi_claimed" if failure == "claimed_npi" else "synthetic uniqueness conflict"
    with pytest.raises(expected_error, match=message):
        await provider_batch._write_batch(
            connection, '"example_registry"', 'pg_temp."example_batch"', {"mutation_count": 1}, revision, "{}"
        )
    connection.execute.assert_not_awaited()
    if failure == "revision_exhausted":
        connection.fetchval.assert_not_awaited()


async def test_provider_batch_combined_byte_bound_does_not_repeat_actor_overhead(provider_db):
    connection, schema, _, _ = provider_db
    fields_by_name = {**_create().fields, "aliases": ["x" * 512] * 100}
    commands = [_create(fields=fields_by_name) for _ in range(20)]
    commands.append(_create(fields={**fields_by_name, "aliases": ["x" * 512] * 20}))
    submitted = json.dumps(
        {"commands": [asdict(command) for command in commands], "references": []},
        default=str,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert 1040000 < len(submitted) <= 1048576
    receipt = await _batch(connection, schema, commands, [], _actor())
    assert len(receipt["records"]) == 21
    assert await _provider_state(connection, schema) == (21, 21, 1, 0)
