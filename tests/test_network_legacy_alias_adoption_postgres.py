"""Replay, scope and concurrency checks against an isolated native registry."""

import asyncio
import importlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, text

from process import network_catalog_import as catalog_import
from process.network_catalog_store import (
    MAX_CATALOG_BYTES,
    NetworkCatalogDraftRow,
    NetworkCatalogSeedError,
    NetworkCatalogSourceContext,
    seed_network_catalog_drafts,
)
from process.network_legacy_alias_adoption import (
    MAX_ADOPTION_ROWS,
    LegacyNetworkAdoptionError,
    LegacyNetworkAdoptionRow,
    adopt_legacy_network_aliases,
)
from process.network_registry_identity import allocate_network_ids
from process.registry_approval_store import approve_registry_records
from process.registry_management_permissions import RegistryManagementPermissionError
from process.registry_manual_undo import RegistryManualUndoCommand, prepare_registry_manual_undo
from process.registry_record_store import (
    RegistryRecordCommand,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
)
from tests.test_network_registry_postgres import registry_db as registry_db
from tests.test_registry_approval_store_postgres import _command as approval_command
from tests.test_registry_management_permissions_postgres import _arguments, _draft, _install
from tests.test_registry_management_permissions_postgres import permissions_db as permissions_db
from tests.test_registry_management_permissions_postgres import provider_db as provider_db
from tests.test_registry_management_permissions_postgres import serving_schema as serving_schema
from tests.test_registry_record_store_postgres import _actor


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


def _row(**changes):
    return replace(
        NetworkCatalogDraftRow(
            LegacyNetworkAdoptionRow("fhir", "source-v1", "medical", "synthetic-evidence", str(uuid4())),
            1,
            "Synthetic Network",
            ("Reviewed alias",),
        ),
        **changes,
    )


def _context(draft_rows, **changes):
    alias = draft_rows[0].alias
    return NetworkCatalogSourceContext(
        **{
            "source_system": alias.source_system,
            "source_id": alias.source_id,
            "alias_scope": alias.scope_key,
            "reason": "Reviewed source names",
            "idempotency_key": "source-v1-catalog",
            **changes,
        }
    )


async def _ready(fixture):
    await _install(fixture)
    await fixture.connection.execute(
        f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{fixture.roles_by_kind["owner"]}"'
    )


async def _adopt(fixture, draft_rows):
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["owner"]}"'))
        bindings = await adopt_legacy_network_aliases(
            session, [draft_row.alias for draft_row in draft_rows], schema=fixture.schema
        )
    return [
        replace(draft_row, network_id=binding.network_id)
        for draft_row, binding in zip(draft_rows, bindings, strict=True)
    ]


async def _seed(fixture, draft_rows, actor, **changes):
    caller = await fixture.connection.fetchval("SELECT current_user")
    async with fixture.connection.transaction():
        receipt = await seed_network_catalog_drafts(
            fixture.connection, draft_rows, actor, **_arguments(fixture), source_context=_context(draft_rows, **changes)
        )
        assert await fixture.connection.fetchval("SELECT current_user") == caller
    return receipt


async def _state(fixture):
    namespace = f'"{fixture.schema}"'
    tables = ("network_registry_record", "registry_record_history", "network_registry_alias")
    retained_documents = [
        await fixture.connection.fetchval(
            f"SELECT coalesce(jsonb_agg(to_jsonb(row) ORDER BY to_jsonb(row)::text),'[]'::jsonb)::text "
            f"FROM {namespace}.{table} row"
        )
        for table in tables
    ]
    return retained_documents + [
        tuple(await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.registry_revision_control")),
        tuple(await fixture.connection.fetchrow(f"SELECT * FROM {namespace}.network_serving_control")),
    ]


async def test_batch_seeds_distinct_named_heads_exact_history_and_explicit_approval(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _adopt(fixture, [_row(display_name="Same Name"), _row(display_name="Same Name")])
    assert draft_rows[0].network_id != draft_rows[1].network_id
    sequence = f'"{fixture.schema}".network_registry_identity_network_id_seq'
    sequence_values = tuple(await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}"))
    receipt = await _seed(fixture, draft_rows, actor)
    assert (receipt["created_records"], receipt["draft_revision"], receipt["approved_revision"]) == (2, 1, 0)
    assert tuple(await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")) == sequence_values
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        heads = await list_registry_records(session, "network", schema=fixture.schema)
        assert len(heads) == 2 and {head["display_name"] for head in heads} == {"Same Name"}
        assert await get_registry_record(session, "network", draft_rows[0].network_id, schema=fixture.schema) in heads
    history = await fixture.connection.fetch(
        f"SELECT history.custom_revision,history.actor_json::text,history.request_sha256,history.event_json::text,"
        f'history.record_json=to_jsonb(head) AS exact FROM "{fixture.schema}".registry_record_history history '
        f'JOIN "{fixture.schema}".network_registry_record head ON head.network_id=history.record_key::int'
    )
    assert len(history) == 2 and all(draft_row["exact"] and draft_row["custom_revision"] == 1 for draft_row in history)
    assert {draft_row["request_sha256"] for draft_row in history} == {receipt["request_sha256"]}
    assert all(
        json.loads(history_row["event_json"])
        == {"operation": "catalog_seed", "before_revision": 0, "operation_context": {}}
        for history_row in history
    )
    retained = await _state(fixture)
    replay = await _seed(fixture, list(reversed(draft_rows)), actor)
    assert replay["created_records"] == 0 and replay["preserved_records"] == 2
    assert replay["request_sha256"] == receipt["request_sha256"] and await _state(fixture) == retained
    selected_records = [
        {"record_kind": "network", "record_id": draft_row.network_id, "revision": 1} for draft_row in draft_rows
    ]
    command = await approval_command(fixture.connection, fixture.schema, *selected_records)
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["publisher"]}"')
        approved = await approve_registry_records(fixture.connection, command, actor, control_schema=fixture.schema)
    assert approved["approved_revision"] == 2
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_approved_record') == 2


async def test_first_correction_undo_and_recurring_import_preserve_manual_and_archived_heads(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _adopt(fixture, [_row(), _row(display_name="Archived Network")])
    await _seed(fixture, draft_rows, actor)
    correction = RegistryRecordCommand(
        "network",
        draft_rows[0].network_id,
        "correct",
        1,
        {"display_name": "Manual Name", "aliases": ["Manual alias"]},
        "Reviewed correction",
        uuid4().hex,
    )
    await _draft(fixture, correction, actor)
    undo = RegistryManualUndoCommand("network", draft_rows[0].network_id, 2, 1, "Undo initial correction", uuid4().hex)
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        prepared = await prepare_registry_manual_undo(session, undo, actor, schema=fixture.schema)
        undone = await apply_registry_record_command(
            session, prepared.command, actor, schema=fixture.schema, undo_provenance=prepared.provenance
        )
    assert undone["revision"] == 3 and undone["record"]["display_name"] == draft_rows[0].display_name
    await _draft(fixture, replace(correction, expected_revision=3, idempotency_key=uuid4().hex), actor)
    await _draft(
        fixture,
        RegistryRecordCommand("network", draft_rows[1].network_id, "archive", 1, {}, "Retire network", uuid4().hex),
        actor,
    )
    v2 = await _adopt(
        fixture,
        [
            replace(draft_row, alias=replace(draft_row.alias, source_id="source-v2"), display_name="Later Source Name")
            for draft_row in draft_rows
        ],
    )
    assert [draft_row.network_id for draft_row in v2] == [draft_row.network_id for draft_row in draft_rows]
    retained = await _state(fixture)
    for edition_rows, key in ((v2, "source-v2-catalog"), (draft_rows, "source-v1-catalog")):
        receipt = await _seed(fixture, edition_rows, actor, idempotency_key=key)
        assert receipt["created_records"] == 0 and receipt["preserved_records"] == 2
        assert receipt["approved_revision"] == 0 and await _state(fixture) == retained


@pytest.mark.parametrize("conflict", ["unknown-alias", "integer", "evidence", "ambiguous-name", "retained-key"])
async def test_binding_conflicts_refuse_whole_batch_and_restore_caller_role(permissions_db, conflict):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _adopt(fixture, [_row(), _row()])
    if conflict == "retained-key":
        await _seed(fixture, draft_rows[:1], actor)
        draft_rows[0] = replace(draft_rows[0], display_name="Changed Name")
    retained = await _state(fixture)
    if conflict == "unknown-alias":
        draft_rows[1] = replace(draft_rows[1], alias=replace(draft_rows[1].alias, legacy_uuid=str(uuid4())))
    elif conflict == "integer":
        draft_rows[1] = replace(draft_rows[1], network_id=draft_rows[0].network_id)
    elif conflict == "evidence":
        draft_rows[1] = replace(draft_rows[1], alias=replace(draft_rows[1].alias, evidence_id="Different evidence"))
    elif conflict == "ambiguous-name":
        draft_rows.append(replace(draft_rows[0], display_name="Conflicting Name"))
    caller = await fixture.connection.fetchval("SELECT current_user")
    async with fixture.connection.transaction():
        with pytest.raises(NetworkCatalogSeedError, match="conflicts"):
            await seed_network_catalog_drafts(
                fixture.connection, draft_rows, actor, **_arguments(fixture), source_context=_context(draft_rows)
            )
        assert await fixture.connection.fetchval("SELECT current_user") == caller
        assert await _state(fixture) == retained
        assert await fixture.connection.fetchval("SELECT 17") == 17
    assert await _state(fixture) == retained


async def test_outer_rollback_and_api_authority_preserve_immutable_inputs(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _adopt(fixture, [_row()])
    retained = await _state(fixture)
    transaction = fixture.connection.transaction()
    await transaction.start()
    try:
        assert (
            await seed_network_catalog_drafts(
                fixture.connection, draft_rows, actor, **_arguments(fixture), source_context=_context(draft_rows)
            )
        )["created_records"] == 1
    finally:
        await transaction.rollback()
    assert await _state(fixture) == retained
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        with pytest.raises(RegistryManagementPermissionError, match="cannot assume"):
            await seed_network_catalog_drafts(
                fixture.connection, draft_rows, actor, **_arguments(fixture), source_context=_context(draft_rows)
            )
        assert await fixture.connection.fetchval("SELECT current_user") == fixture.roles_by_kind["api"]
    assert await _state(fixture) == retained


class _CountedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.calls = []

    def __getattr__(self, name):
        method = getattr(self.connection, name)
        if name not in {"execute", "fetch", "fetchval", "fetchrow", "copy_records_to_table"}:
            return method

        async def counted(*args, **kwargs):
            self.calls.append(name)
            return await method(*args, **kwargs)

        return counted


async def test_native_copy_and_statement_count_do_not_grow_per_network(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _adopt(fixture, [_row() for _ in range(101)])
    counts = []
    for subset, key in ((draft_rows[:1], "one-row"), (draft_rows[1:], "one-hundred-rows")):
        connection = _CountedConnection(fixture.connection)
        async with fixture.connection.transaction():
            receipt = await seed_network_catalog_drafts(
                connection, subset, actor, **_arguments(fixture), source_context=_context(subset, idempotency_key=key)
            )
        assert receipt["created_records"] == len(subset)
        assert connection.calls.count("copy_records_to_table") == 1
        counts.append(connection.calls)
    assert counts[0] == counts[1]
    assert (
        await fixture.connection.fetchval(f'SELECT draft_revision FROM "{fixture.schema}".registry_revision_control')
        == 2
    )


async def test_preflight_limits_actor_fields_and_exact_explicit_source_context():
    draft_row, actor = _row(), _actor()
    invalid_cases = [
        ([], "row limit"),
        ([draft_row] * (MAX_ADOPTION_ROWS + 1), "row limit"),
        ([{}], "validated alias"),
        ([replace(draft_row, network_id=True)], "network_id_invalid"),
        ([replace(draft_row, display_name="bad\nname")], "display_name_invalid"),
        ([replace(draft_row, aliases=("x",) * 101)], "aliases_invalid"),
        ([replace(draft_row, alias=replace(draft_row.alias, reviewed_network_id=2))], "reviewed integer"),
    ]
    arguments_by_name = {
        "api_role": "api",
        "owner_role": "owner",
        "control_schema": "registry",
        "source_context": _context([draft_row]),
    }
    for draft_rows, refusal in invalid_cases:
        with pytest.raises(ValueError, match=refusal):
            await seed_network_catalog_drafts(object(), draft_rows, actor, **arguments_by_name)
    with pytest.raises(NetworkCatalogSeedError, match="source context"):
        await seed_network_catalog_drafts(
            object(),
            [draft_row],
            actor,
            **{
                **arguments_by_name,
                "source_context": replace(arguments_by_name["source_context"], source_id="other-source"),
            },
        )
    with pytest.raises(ValueError, match="actor_invalid"):
        await seed_network_catalog_drafts(object(), [draft_row], object(), **arguments_by_name)
    with pytest.raises(NetworkCatalogSeedError, match="platform administrator"):
        await seed_network_catalog_drafts(
            object(), [draft_row], replace(actor, kind="client_owner"), **arguments_by_name
        )
    with pytest.raises(NetworkCatalogSeedError, match="caller-owned"):
        await seed_network_catalog_drafts(
            SimpleNamespace(is_in_transaction=lambda: False), [draft_row], actor, **arguments_by_name
        )
    large = replace(draft_row, aliases=tuple(f"{index:03d}" + "x" * 509 for index in range(100)))
    assert MAX_CATALOG_BYTES < 5000 * 100 * 512
    with pytest.raises(NetworkCatalogSeedError, match="byte limit"):
        await seed_network_catalog_drafts(object(), [large] * 200, actor, **arguments_by_name)


def _operator_rows(draft_rows):
    return [
        {**asdict(draft_row.alias), "display_name": draft_row.display_name, "aliases": list(draft_row.aliases)}
        for draft_row in draft_rows
    ]


async def _invoke_catalog(fixture, draft_rows, actor, source_context):
    async with fixture.sessions() as session, session.begin():
        caller = await session.scalar(text("SELECT current_user"))
        receipt = await catalog_import.import_network_catalog(
            session, _operator_rows(draft_rows), actor, source_context, **_arguments(fixture)
        )
        assert await session.scalar(text("SELECT current_user")) == caller
    return receipt


async def _coordinated_catalog(fixture, draft_rows, actor, context, ready, backend_ids):
    async with fixture.sessions() as session, session.begin():
        backend_ids.append(await session.scalar(text("SELECT pg_backend_pid()")))
        await ready.wait()
        caller = await session.scalar(text("SELECT current_user"))
        receipt = await catalog_import.import_network_catalog(
            session, _operator_rows(draft_rows), actor, context, **_arguments(fixture)
        )
        assert await session.scalar(text("SELECT current_user")) == caller
    return receipt


async def _wait_catalog_lockers(connection, backend_ids, tasks):
    async with asyncio.timeout(10):
        while True:
            await connection.execute("SELECT pg_stat_clear_snapshot()")
            blocked = await connection.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE pid=ANY($1::int[]) "
                "AND wait_event_type='Lock' AND query LIKE '%registry_revision_control%FOR UPDATE%'",
                backend_ids,
            )
            if blocked == 2:
                return
            for task in tasks:
                if task.done():
                    task.result()
            await asyncio.sleep(0.01)


async def test_concurrent_catalog_imports_lock_control_before_adopting_disjoint_aliases(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    batches = [[_row()], [_row()]]
    contexts = [
        _context(draft_rows, idempotency_key=f"concurrent-catalog-{index}") for index, draft_rows in enumerate(batches)
    ]
    backend_ids = []
    ready = asyncio.Barrier(3)

    tasks = []
    try:
        async with fixture.connection.transaction():
            await fixture.connection.fetchrow(
                f'SELECT id FROM "{fixture.schema}".registry_revision_control WHERE id=1 FOR UPDATE'
            )
            tasks = [
                asyncio.create_task(_coordinated_catalog(fixture, draft_rows, actor, context, ready, backend_ids))
                for draft_rows, context in zip(batches, contexts)
            ]
            async with asyncio.timeout(10):
                await ready.wait()
            # Both callers reach the contested control lock before it is released.
            await _wait_catalog_lockers(fixture.connection, backend_ids, tasks)
        async with asyncio.timeout(10):
            receipts = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert [receipt["created_records"] for receipt in receipts] == [1, 1]
    assert {receipt["draft_revision"] for receipt in receipts} == {1, 2}
    assert all(receipt["approved_revision"] == 0 for receipt in receipts)
    namespace = f'"{fixture.schema}"'
    for table in (
        "network_registry_identity",
        "network_registry_alias",
        "network_registry_record",
        "registry_record_history",
    ):
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.{table}") == 2
    retained = await _state(fixture)
    sequence = f"{namespace}.network_registry_identity_network_id_seq"
    sequence_values = tuple(await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}"))
    replays = await asyncio.gather(
        *(_invoke_catalog(fixture, draft_rows, actor, context) for draft_rows, context in zip(batches, contexts))
    )
    assert all(receipt["created_records"] == 0 and receipt["preserved_records"] == 1 for receipt in replays)
    assert [receipt["request_sha256"] for receipt in replays] == [receipt["request_sha256"] for receipt in receipts]
    assert await _state(fixture) == retained
    assert tuple(await fixture.connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")) == sequence_values


async def test_catalog_operator_cli_seeds_and_replays_stable_ids(permissions_db, monkeypatch, tmp_path, capsys):
    from tests.test_network_catalog_import import _arguments as cli_arguments
    from tests.test_registry_source_import_postgres import _configured_pool

    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    databases = _configured_pool(monkeypatch, fixture.sessions.kw["bind"], fixture.schema)
    from process import registry_source_import

    monkeypatch.setattr(catalog_import, "Database", registry_source_import.Database)
    draft_rows = [_row(display_name="Same Name"), _row(display_name="Same Name")]
    input_bytes = json.dumps(_operator_rows(draft_rows)).encode()
    input_file = tmp_path / "catalog.json"
    input_file.write_bytes(input_bytes)
    arguments = cli_arguments(input_file, input_bytes)
    for field, field_value in {
        "actor-user-id": str(actor.user_id),
        "actor-client-id": actor.client_id,
        "api-role": fixture.roles_by_kind["api"],
        "owner-role": fixture.roles_by_kind["owner"],
        "control-schema": fixture.schema,
    }.items():
        arguments[arguments.index("--" + field) + 1] = field_value
    for expected_created in (2, 0):
        assert await asyncio.to_thread(catalog_import.run_command, arguments) == 0
        captured = capsys.readouterr()
        assert captured.err == "" and captured.out.count("\n") == 1
        receipt = json.loads(captured.out)
        assert receipt["created_records"] == expected_created and receipt["draft_revision"] == 1
        assert receipt["approved_revision"] == 0 and receipt["requested_records"] == 2
        if expected_created:
            retained = await _state(fixture)
        else:
            assert await _state(fixture) == retained
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 2
    assert len(databases) == 2 and all(database.engine is None for database in databases)


async def test_catalog_import_savepoint_rolls_back_adoption_when_seed_conflicts(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_row = _row()
    retained = await _state(fixture)
    async with fixture.sessions() as session, session.begin():
        caller = await session.scalar(text("SELECT current_user"))
        with pytest.raises(NetworkCatalogSeedError, match="conflicts"):
            await catalog_import.import_network_catalog(
                session,
                _operator_rows([draft_row, replace(draft_row, display_name="Conflicting Name")]),
                actor,
                _context([draft_row]),
                **_arguments(fixture),
            )
        assert await session.scalar(text("SELECT current_user")) == caller
        assert await session.scalar(text("SELECT 17")) == 17
    assert await _state(fixture) == retained
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 0


async def test_operator_refuses_untrusted_credentials(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = [_row()]
    retained = await _state(fixture)
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        with pytest.raises(RegistryManagementPermissionError, match="cannot assume"):
            await catalog_import.import_network_catalog(
                session, _operator_rows(draft_rows), actor, _context(draft_rows), **_arguments(fixture)
            )
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["api"]
    assert await _state(fixture) == retained
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 0


async def test_catalog_import_outer_rollback_and_refresh_preserve_manual_archive(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = [_row(), _row(display_name="Archived Network")]
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with fixture.sessions() as session, session.begin():
            await catalog_import.import_network_catalog(
                session, _operator_rows(draft_rows), actor, _context(draft_rows), **_arguments(fixture)
            )
            raise RuntimeError("outer rollback")
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 0
    await _invoke_catalog(fixture, draft_rows, actor, _context(draft_rows))
    bound_rows = await _adopt(fixture, draft_rows)
    await _draft(
        fixture,
        RegistryRecordCommand(
            "network",
            bound_rows[0].network_id,
            "correct",
            1,
            {"display_name": "Manual Name", "aliases": ["Manual alias"]},
            "Reviewed correction",
            uuid4().hex,
        ),
        actor,
    )
    await _draft(
        fixture,
        RegistryRecordCommand("network", bound_rows[1].network_id, "archive", 1, {}, "Retire network", uuid4().hex),
        actor,
    )
    later_rows = [
        replace(draft_row, alias=replace(draft_row.alias, source_id="source-v2"), display_name="Later Source Name")
        for draft_row in draft_rows
    ]
    before_refresh = (await _state(fixture))[:2]
    receipt = await _invoke_catalog(
        fixture, later_rows, actor, _context(later_rows, idempotency_key="source-v2-catalog")
    )
    assert receipt["created_records"] == 0 and receipt["preserved_records"] == 2
    assert (await _state(fixture))[:2] == before_refresh
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity') == 2
