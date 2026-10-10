"""Replay, scope and concurrency checks against an isolated native registry."""

import asyncio
import importlib
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, text

from process.network_legacy_alias_adoption import (
    MAX_ADOPTION_ROWS,
    LegacyNetworkAdoptionError,
    LegacyNetworkAdoptionRow,
    adopt_legacy_network_aliases,
)
from process.network_registry_identity import allocate_network_ids
from tests.test_network_registry_postgres import registry_db


def _adoption_row(**changes):
    return replace(LegacyNetworkAdoptionRow("fhir", "source-one", "medical/2026", "proof-one", str(uuid4())), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"legacy_uuid": "00000000-0000-0000-0000-000000000000"},
        {"legacy_uuid": "not-a-uuid"},
        {"source_system": " "},
        {"source_system": "x" * 65},
        {"source_id": "x" * 129},
        {"scope_key": "x" * 513},
        {"evidence_id": "x\0y"},
        {"reviewed_network_id": True},
        {"reviewed_network_id": 0},
        {"reviewed_network_id": 2147483648},
        {"reviewed_network_id": "42"},
    ],
)
def test_adoption_rejects_invalid_identity_scope_and_review(changes):
    with pytest.raises(LegacyNetworkAdoptionError):
        _adoption_row(**changes)


async def test_adoption_requires_transaction_and_bounded_validated_rows():
    absent_transaction = SimpleNamespace(in_transaction=lambda: False)
    with pytest.raises(LegacyNetworkAdoptionError, match="caller-owned transaction"):
        await adopt_legacy_network_aliases(absent_transaction, [_adoption_row()], schema="registry_test")
    with pytest.raises(LegacyNetworkAdoptionError, match="row limit"):
        await adopt_legacy_network_aliases(
            object(), [_adoption_row()] * (MAX_ADOPTION_ROWS + 1), schema="registry_test"
        )
    with pytest.raises(LegacyNetworkAdoptionError, match="validated rows"):
        await adopt_legacy_network_aliases(object(), [{"legacy_uuid": str(uuid4())}], schema="registry_test")
    with pytest.raises(TypeError):
        LegacyNetworkAdoptionRow("fhir", "source", "medical", "proof", str(uuid4()), network_label="Same Label")


async def test_replay_duplicate_aliases_and_same_label_distinct_networks(registry_db):
    connection, schema, sessions = registry_db
    first_alias, second_alias = _adoption_row(), _adoption_row()
    async with sessions() as session, session.begin():
        first = await adopt_legacy_network_aliases(session, [first_alias, first_alias, second_alias], schema=schema)
    assert first[0] == first[1]
    assert first[0].network_id != first[2].network_id
    async with sessions() as session, session.begin():
        replay = await adopt_legacy_network_aliases(session, [second_alias, first_alias], schema=schema)
    assert replay == (first[2], first[0])
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) VALUES ($1,$3),($2,$3)',
        first[0].network_id,
        first[2].network_id,
        "Synthetic Repeated Label",
    )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 2
    aliases = await connection.fetch(f'SELECT alias_type,alias_value FROM "{schema}".network_registry_alias')
    assert {tuple(alias) for alias in aliases} == {
        ("legacy_fhir_uuid", first_alias.legacy_uuid),
        ("legacy_fhir_uuid", second_alias.legacy_uuid),
    }


async def test_reviewed_many_aliases_and_exact_source_scopes(registry_db):
    connection, schema, sessions = registry_db
    async with sessions() as session, session.begin():
        canonical_ids = list((await allocate_network_ids(session, [uuid4(), uuid4()], schema=schema)).values())
        same_uuid = str(uuid4())
        aliases = [
            _adoption_row(legacy_uuid=same_uuid, reviewed_network_id=canonical_ids[0]),
            _adoption_row(legacy_uuid=same_uuid, scope_key="dental/2026", reviewed_network_id=canonical_ids[1]),
            _adoption_row(legacy_uuid=same_uuid, source_id="source-two", reviewed_network_id=canonical_ids[1]),
            _adoption_row(reviewed_network_id=canonical_ids[1]),
        ]
        adopted = await adopt_legacy_network_aliases(session, aliases, schema=schema)
    assert [alias.network_id for alias in adopted] == [
        canonical_ids[0],
        canonical_ids[1],
        canonical_ids[1],
        canonical_ids[1],
    ]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 2
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 4
    async with sessions() as session, session.begin():
        assert await adopt_legacy_network_aliases(
            session, [replace(aliases[0], reviewed_network_id=None)], schema=schema
        ) == (adopted[0],)


async def test_missing_or_conflicting_review_preserves_other_caller_writes(registry_db):
    connection, schema, sessions = registry_db
    async with sessions() as session, session.begin():
        canonical_ids = list((await allocate_network_ids(session, [uuid4(), uuid4()], schema=schema)).values())
        await session.execute(
            text(
                f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) VALUES (:network_id,:display_name)'
            ),
            {"network_id": canonical_ids[0], "display_name": "Synthetic Manual Network"},
        )
        adoption_row = _adoption_row(reviewed_network_id=canonical_ids[0])
        with pytest.raises(LegacyNetworkAdoptionError, match="conflicts"):
            await adopt_legacy_network_aliases(
                session, [adoption_row, replace(adoption_row, reviewed_network_id=canonical_ids[1])], schema=schema
            )
        with pytest.raises(LegacyNetworkAdoptionError, match="unknown network"):
            await adopt_legacy_network_aliases(session, [_adoption_row(reviewed_network_id=2147483647)], schema=schema)
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".network_registry_alias')) == 0
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".network_registry_record')) == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_record') == 1


async def test_existing_alias_conflict_rejects_entire_batch_and_retains_evidence(registry_db):
    connection, schema, sessions = registry_db
    adopted_row = _adoption_row()
    async with sessions() as session, session.begin():
        original = await adopt_legacy_network_aliases(session, [adopted_row], schema=schema)
        other_id = next(iter((await allocate_network_ids(session, [uuid4()], schema=schema)).values()))
    async with sessions() as session, session.begin():
        with pytest.raises(LegacyNetworkAdoptionError, match="conflicts"):
            await adopt_legacy_network_aliases(
                session, [_adoption_row(), replace(adopted_row, reviewed_network_id=other_id)], schema=schema
            )
        unchanged = await adopt_legacy_network_aliases(
            session, [replace(adopted_row, evidence_id="proof-later")], schema=schema
        )
        assert unchanged == original
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 1
    assert await connection.fetchval(f'SELECT evidence_id FROM "{schema}".network_registry_alias') == "proof-one"


async def test_concurrent_unreviewed_adoption_cannot_fork_alias(registry_db):
    connection, schema, sessions = registry_db
    adoption_row = _adoption_row()

    async def adopt_once():
        async with sessions() as session, session.begin():
            return await adopt_legacy_network_aliases(session, [adoption_row], schema=schema)

    adopted_batches = await asyncio.gather(adopt_once(), adopt_once(), adopt_once())
    assert adopted_batches[0] == adopted_batches[1] == adopted_batches[2]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 1


async def test_concurrent_conflicting_reviews_have_one_binding_and_one_rejection(registry_db, monkeypatch):
    connection, schema, sessions = registry_db
    async with sessions() as session, session.begin():
        canonical_ids = list((await allocate_network_ids(session, [uuid4(), uuid4()], schema=schema)).values())
    module = importlib.import_module("process.network_legacy_alias_adoption")
    check_reviewed_aliases = module._check_reviewed_aliases
    barrier = asyncio.Barrier(2)

    async def synchronized_review(*args):
        await check_reviewed_aliases(*args)
        await barrier.wait()

    monkeypatch.setattr(module, "_check_reviewed_aliases", synchronized_review)
    adoption_row = _adoption_row()

    async def adopt_reviewed(network_id):
        async with sessions() as session, session.begin():
            return await adopt_legacy_network_aliases(
                session, [replace(adoption_row, reviewed_network_id=network_id)], schema=schema
            )

    outcomes = await asyncio.gather(
        *(adopt_reviewed(network_id) for network_id in canonical_ids), return_exceptions=True
    )
    assert sum(isinstance(outcome, LegacyNetworkAdoptionError) for outcome in outcomes) == 1
    assert sum(isinstance(outcome, tuple) for outcome in outcomes) == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 1
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 2


async def test_allocation_and_aliases_follow_outer_rollback(registry_db):
    connection, schema, sessions = registry_db
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with sessions() as session, session.begin():
            await adopt_legacy_network_aliases(session, [_adoption_row()], schema=schema)
            raise RuntimeError("outer rollback")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_alias') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".network_registry_identity') == 0


async def test_maximum_batch_uses_the_same_number_of_database_statements(registry_db):
    _, schema, sessions = registry_db
    observed_statements = []

    def observe_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
        observed_statements.append(statement)

    native_engine = sessions.kw["bind"].sync_engine
    event.listen(native_engine, "before_cursor_execute", observe_statement)
    try:
        async with sessions() as session, session.begin():
            await adopt_legacy_network_aliases(session, [_adoption_row()], schema=schema)
        one_row_statement_count = len(observed_statements)
        observed_statements.clear()
        adoption_rows = [_adoption_row() for _ in range(MAX_ADOPTION_ROWS)]
        async with sessions() as session, session.begin():
            adopted_aliases = await adopt_legacy_network_aliases(session, adoption_rows, schema=schema)
        assert len(adopted_aliases) == MAX_ADOPTION_ROWS
        assert len({alias.network_id for alias in adopted_aliases}) == MAX_ADOPTION_ROWS
        assert len(observed_statements) == one_row_statement_count
        assert one_row_statement_count <= 16
    finally:
        event.remove(native_engine, "before_cursor_execute", observe_statement)
