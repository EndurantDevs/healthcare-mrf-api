# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit approval selections on actual native migrations and manual drafts."""

import asyncio
import hashlib
import json
import os
from dataclasses import replace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.registry_approval_store import (
    RegistryApprovalCommand,
    RegistryApprovalConflict,
    approve_registry_records,
)
from process.registry_record_store import RegistryActor, RegistryRecordCommand, apply_registry_record_command
from tests.test_network_serving_schema_postgres import serving_schema

pytestmark = pytest.mark.asyncio


def _actor(client="client_example"):
    return RegistryActor("client_owner", uuid4(), client)


def _create(kind="group"):
    fields_by_name = {"display_name": "Example Record", "aliases": []}
    if kind == "group":
        fields_by_name["group_kind"] = "corporate_parent"
    if kind == "company":
        fields_by_name["roles"] = ["employer"]
    return RegistryRecordCommand(
        kind,
        None if kind == "network" else uuid4(),
        "create",
        0,
        fields_by_name,
        "Example creation",
        uuid4().hex,
        uuid4() if kind == "network" else None,
    )


async def _draft(engine, schema, command, actor):
    async with async_sessionmaker(engine)() as session, session.begin():
        return await apply_registry_record_command(session, command, actor, schema=schema)


def _selection(*records):
    return tuple({key: record[key] for key in ("record_kind", "record_id", "revision")} for record in records)


async def _command(connection, schema, *records, **changes):
    heads = await connection.fetchrow(f'SELECT * FROM "{schema}".registry_revision_control WHERE id=1')
    return replace(
        RegistryApprovalCommand(
            heads["draft_revision"], heads["approved_revision"], _selection(*records), "Example approval", uuid4().hex
        ),
        **changes,
    )


async def _approve(connection, schema, command, actor):
    async with connection.transaction():
        return await approve_registry_records(connection, command, actor, control_schema=schema)


async def _approved(connection, schema, revision):
    return await connection.fetch(
        f'SELECT record_kind,record_key,record_revision,record_json FROM "{schema}".registry_approved_record '
        "WHERE approved_revision=$1 ORDER BY record_kind,record_key",
        revision,
    )


async def test_selective_clients_preserve_pending_and_prior_maps(serving_schema):
    connection, schema, engine = serving_schema
    first_actor, second_actor = _actor("client_first"), _actor("client_second")
    first_command, second_command = _create(), _create("company")
    first = await _draft(engine, schema, first_command, first_actor)
    second = await _draft(engine, schema, second_command, second_actor)
    first_approval = await _approve(connection, schema, await _command(connection, schema, first), first_actor)
    first_map = await _approved(connection, schema, first_approval["approved_revision"])
    assert [(approved_record["record_kind"], approved_record["record_key"]) for approved_record in first_map] == [
        ("group", first["record_id"])
    ]
    correction = replace(
        first_command,
        operation="correct",
        expected_revision=1,
        fields={**first_command.fields, "display_name": "Pending correction"},
        idempotency_key=uuid4().hex,
    )
    pending = await _draft(engine, schema, correction, first_actor)
    second_approval = await _approve(connection, schema, await _command(connection, schema, second), second_actor)
    second_map = await _approved(connection, schema, second_approval["approved_revision"])
    assert len(second_map) == 2
    assert (
        next(approved_record for approved_record in second_map if approved_record["record_kind"] == "group")[
            "record_revision"
        ]
        == 1
    )
    assert all("Pending correction" not in approved_record["record_json"] for approved_record in second_map)
    assert await _approved(connection, schema, first_approval["approved_revision"]) == first_map
    assert pending["custom_revision"] < second_approval["approved_revision"]
    final_approval = await _approve(connection, schema, await _command(connection, schema, pending), first_actor)
    final_map = await _approved(connection, schema, final_approval["approved_revision"])
    assert (
        next(approved_record for approved_record in final_map if approved_record["record_kind"] == "group")[
            "record_revision"
        ]
        == 2
    )
    assert await _approved(connection, schema, second_approval["approved_revision"]) == second_map
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (final_approval["approved_revision"], final_approval["approved_revision"])


async def test_exact_replay_precedes_cas_and_canonicalizes_selection(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    records = [await _draft(engine, schema, _create(kind), actor) for kind in ("network", "group")]
    command = await _command(connection, schema, *records)
    receipt = await _approve(connection, schema, command, actor)
    await _draft(engine, schema, _create("company"), actor)
    replay = await _approve(connection, schema, replace(command, selection=tuple(reversed(command.selection))), actor)
    assert replay == {**receipt, "replayed": True}
    for changed in (
        replace(command, reason="Changed"),
        replace(command, expected_draft_revision=9),
        replace(command, expected_approved_revision=1),
        replace(command, selection=command.selection[:1]),
    ):
        with pytest.raises(RegistryApprovalConflict, match="idempotency_conflict"):
            await _approve(connection, schema, changed, actor)
    audit = await connection.fetchrow(f'SELECT * FROM "{schema}".registry_approval_history')
    assert (
        audit["actor_key"]
        == hashlib.sha256(
            json.dumps(json.loads(audit["actor_json"]), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    )
    assert audit["expected_draft_revision"] == command.expected_draft_revision
    assert audit["previous_approved_revision"] == 0
    assert len(json.loads(audit["selection_json"])) == 2


async def test_full_actor_identity_scopes_replay(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, record)
    await _approve(connection, schema, command, actor)
    for other in (
        replace(actor, client_id="client_other"),
        replace(actor, kind="platform_admin"),
        replace(actor, impersonator_id=uuid4()),
        replace(actor, user_id=uuid4()),
    ):
        with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
            await _approve(connection, schema, command, other)
    new_command = await _command(connection, schema, record, idempotency_key=command.idempotency_key)
    assert not (await _approve(connection, schema, new_command, replace(actor, client_id="client_other")))["replayed"]
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 2


@pytest.mark.parametrize("change", [{"expected_draft_revision": 0}, {"expected_approved_revision": 1}])
async def test_both_revision_cas_are_required(serving_schema, change):
    connection, schema, engine = serving_schema
    actor = _actor()
    record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, record, **change)
    with pytest.raises(RegistryApprovalConflict, match="revision_conflict"):
        await _approve(connection, schema, command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_stale_head_missing_or_changed_history_are_rejected(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create()
    first = await _draft(engine, schema, creation, actor)
    second = await _draft(
        engine, schema, replace(creation, operation="correct", expected_revision=1, idempotency_key=uuid4().hex), actor
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(connection, schema, await _command(connection, schema, first), actor)
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history old SET record_json=current.record_json '
        f'FROM "{schema}".registry_record_history current WHERE old.revision=1 AND current.revision=2'
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(connection, schema, await _command(connection, schema, first), actor)
    await connection.execute(
        f'UPDATE "{schema}".registry_record_history '
        "SET record_json=jsonb_set(record_json,'{display_name}','\"Different\"') WHERE revision=2"
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(connection, schema, await _command(connection, schema, second), actor)
    await connection.execute(f'DELETE FROM "{schema}".registry_record_history WHERE revision=2')
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(connection, schema, await _command(connection, schema, second), actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0


@pytest.mark.parametrize("timestamp_json", ["null", '"invalid"'])
async def test_malformed_history_cannot_hide_in_valid_selection(serving_schema, timestamp_json):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected_versions = [await _draft(engine, schema, _create(kind), actor) for kind in ("group", "company")]
    await connection.execute(
        f"UPDATE \"{schema}\".registry_record_history SET record_json=jsonb_set(record_json,'{{created_at}}',$1::jsonb) "
        "WHERE record_kind='group'",
        timestamp_json,
    )
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(connection, schema, await _command(connection, schema, *selected_versions), actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_record_kinds_keep_equal_uuid_ids_distinct(serving_schema):
    connection, schema, engine = serving_schema
    actor, group_command = _actor(), _create()
    group = await _draft(engine, schema, group_command, actor)
    with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
        await _approve(
            connection, schema, await _command(connection, schema, {**group, "record_kind": "company"}), actor
        )
    company = await _draft(engine, schema, replace(_create("company"), record_id=group_command.record_id), actor)
    receipt = await _approve(connection, schema, await _command(connection, schema, group, company), actor)
    approved_rows = await _approved(connection, schema, receipt["approved_revision"])
    assert [(row["record_kind"], row["record_key"]) for row in approved_rows] == [
        ("company", group["record_id"]),
        ("group", group["record_id"]),
    ]


async def test_selected_archive_copies_exact_manual_history(serving_schema):
    connection, schema, engine = serving_schema
    actor, creation = _actor(), _create("network")
    created = await _draft(engine, schema, creation, actor)
    archived = await _draft(
        engine,
        schema,
        replace(
            creation,
            record_id=created["record_id"],
            allocation_key=None,
            operation="archive",
            expected_revision=1,
            fields={},
            idempotency_key=uuid4().hex,
        ),
        actor,
    )
    receipt = await _approve(connection, schema, await _command(connection, schema, archived), actor)
    approved_rows = await _approved(connection, schema, receipt["approved_revision"])
    assert len(approved_rows) == 1 and approved_rows[0]["record_revision"] == 2
    assert json.loads(approved_rows[0]["record_json"]) == archived["record"]
    assert archived["record"]["archived"] is True


async def test_source_only_heads_and_missing_identities_are_rejected(serving_schema):
    connection, schema, _ = serving_schema
    identity = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".company_registry(company_id,display_name,roles) '
        "VALUES($1,'Source Example',ARRAY['employer'])",
        identity,
    )
    for record_id in (str(identity), str(uuid4())):
        record_by_field = {"record_kind": "company", "record_id": record_id, "revision": 1}
        with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
            await _approve(connection, schema, await _command(connection, schema, record_by_field), _actor())


@pytest.mark.parametrize(
    "selection",
    [
        (),
        ({"record_kind": "network", "record_id": value, "revision": 1} for value in (1,)),
        ({"record_kind": "network", "record_id": "1", "revision": 1},),
        ({"record_kind": "network", "record_id": True, "revision": 1},),
        ({"record_kind": "network", "record_id": 1.0, "revision": 1},),
        ({"record_kind": "network", "record_id": 2147483648, "revision": 1},),
        ({"record_kind": "group", "record_id": "00000000-0000-0000-0000-000000000000", "revision": 1},),
        ({"record_kind": "group", "record_id": "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA", "revision": 1},),
        ({"record_kind": "group", "record_id": "example", "revision": 1},),
        ({"record_kind": "group", "record_id": "\0", "revision": 1},),
        ({"record_kind": "group", "record_id": "\ud800", "revision": 1},),
        ({"record_kind": "network", "record_id": 1, "revision": True},),
        ({"record_kind": "network", "record_id": 1, "revision": "1"},),
        ({"record_kind": "network", "record_id": 1, "revision": 1.5},),
        ({"record_kind": "network", "record_id": 1, "revision": 1, "extra": 0},),
        ({"record_kind": "other", "record_id": 1, "revision": 1},),
        (None,),
        ({"record_kind": "network", "record_id": 1, "revision": 1},) * 2,
        ({"record_kind": "network", "record_id": 1, "revision": 1},) * 5001,
        ({"record_kind": "network", "record_id": "x" * 1048576, "revision": 1},),
    ],
)
async def test_selection_shape_and_native_ids_are_bounded(serving_schema, selection):
    connection, schema, _ = serving_schema
    command = RegistryApprovalCommand(0, 0, selection, "Example approval", uuid4().hex)
    with pytest.raises(ValueError, match="selection_invalid"):
        await _approve(connection, schema, command, _actor())
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_actor_namespace_and_caller_transaction_are_required(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    record = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, record)
    with pytest.raises(ValueError, match="caller_transaction"):
        await approve_registry_records(connection, command, actor, control_schema=schema)
    for invalid_actor in (replace(actor, user_id=UUID(int=0)), replace(actor, client_id=" bad "), {}):
        with pytest.raises(ValueError):
            await _approve(connection, schema, command, invalid_actor)
    with pytest.raises(ValueError):
        await _approve(connection, 'invalid";DROP SCHEMA public;', command, actor)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0


async def test_outer_rollback_and_savepoint_failure_are_atomic(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected_version = await _draft(engine, schema, _create(), actor)
    command = await _command(connection, schema, selected_version)
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await approve_registry_records(connection, command, actor, control_schema=schema)
            raise RuntimeError("caller rollback")
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approved_record') == 0
    await connection.execute(
        f'ALTER TABLE "{schema}".registry_approved_record '
        "ADD CONSTRAINT test_reject_approval CHECK (approved_revision<2)"
    )
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE caller_work(value int) ON COMMIT DROP")
        await connection.execute("INSERT INTO caller_work VALUES(7)")
        with pytest.raises(asyncpg.CheckViolationError):
            await approve_registry_records(connection, command, actor, control_schema=schema)
        assert await connection.fetchval("SELECT value FROM caller_work") == 7
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 0
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_approval_%'"
            )
            == 0
        )
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (1, 0)


async def test_competing_approvals_and_exact_concurrent_retry_serialize(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    selected_versions = [await _draft(engine, schema, _create(kind), actor) for kind in ("group", "company")]
    commands = [await _command(connection, schema, selected_version) for selected_version in selected_versions]
    other = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    )
    try:
        outcomes = await asyncio.gather(
            _approve(connection, schema, commands[0], actor),
            _approve(other, schema, commands[1], actor),
            return_exceptions=True,
        )
        assert sum(isinstance(outcome, RegistryApprovalConflict) for outcome in outcomes) == 1
        winning_command = commands[next(index for index, outcome in enumerate(outcomes) if isinstance(outcome, dict))]
        replay_results = await asyncio.gather(
            _approve(connection, schema, winning_command, actor), _approve(other, schema, winning_command, actor)
        )
        assert replay_results[0] == replay_results[1]
        assert replay_results[0]["replayed"]
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 1
        fresh_command = await _command(connection, schema, *selected_versions)
        fresh_results = await asyncio.gather(
            _approve(connection, schema, fresh_command, actor), _approve(other, schema, fresh_command, actor)
        )
        assert sorted(receipt["replayed"] for receipt in fresh_results) == [False, True]
        assert fresh_results[0]["approved_revision"] == fresh_results[1]["approved_revision"]
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_approval_history') == 2
    finally:
        await other.close()


class _CountedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.statements = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def execute(self, *arguments):
        self.statements += 1
        return await self.connection.execute(*arguments)

    async def fetchrow(self, *arguments):
        self.statements += 1
        return await self.connection.fetchrow(*arguments)

    async def fetchval(self, *arguments):
        self.statements += 1
        return await self.connection.fetchval(*arguments)


async def _bulk_manual_versions(connection, schema, seed):
    await connection.execute(
        f"""INSERT INTO "{schema}".company_group_registry
        SELECT md5($1::text||number::text)::uuid,head.group_kind,head.display_name,head.aliases,
               head.archived,head.revision,head.created_at
        FROM "{schema}".company_group_registry head CROSS JOIN generate_series(1,4999) number WHERE head.group_id=$2""",
        uuid4().hex,
        UUID(seed["record_id"]),
    )
    await connection.execute(
        f"""INSERT INTO "{schema}".registry_record_history
        (record_kind,record_key,revision,custom_revision,record_json,actor_json,reason,idempotency_key,request_sha256)
        SELECT 'group',head.group_id::text,1,row_number() OVER(ORDER BY head.group_id)+1,
               jsonb_set(history.record_json,'{{group_id}}',to_jsonb(head.group_id::text)),
               history.actor_json,history.reason,head.group_id::text,history.request_sha256
        FROM "{schema}".company_group_registry head CROSS JOIN "{schema}".registry_record_history history
        WHERE head.group_id<>$1 AND history.record_key=$1::text""",
        UUID(seed["record_id"]),
    )
    await connection.execute(f'UPDATE "{schema}".registry_revision_control SET draft_revision=5000')


async def test_statement_count_is_constant_for_one_and_five_thousand(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    seed = await _draft(engine, schema, _create(), actor)
    await _bulk_manual_versions(connection, schema, seed)
    counted = _CountedConnection(connection)
    first = await _approve(counted, schema, await _command(connection, schema, seed), actor)
    one_count = counted.statements
    selected_versions = tuple(
        dict(row)
        for row in await connection.fetch(
            f'SELECT record_kind,record_key AS record_id,revision FROM "{schema}".registry_record_history'
        )
    )
    counted.statements = 0
    command = await _command(connection, schema, selection=selected_versions)
    bulk = await _approve(counted, schema, command, actor)
    # One company writer fence and one prospective membership anti-join per batch.
    assert counted.statements == one_count == 11
    assert bulk["selected_count"] == 5000
    assert len(await _approved(connection, schema, bulk["approved_revision"])) == 5000
    assert len(await _approved(connection, schema, first["approved_revision"])) == 1
