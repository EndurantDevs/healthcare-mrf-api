# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native set-based batch drafts, exact retry scope and whole-transaction failure."""

import asyncio
import json
import os
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import company_network_link_store as store
from process.company_network_link_store import (
    CompanyLinkBatchCommand,
    CompanyLinkBatchTarget,
    apply_company_network_link_batch,
)
from process.registry_record_store import RegistryAddressUnavailable, RegistryRecordConflict
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import (
    _actor,
    _approve,
    _approved,
    _command,
    _CountedConnection,
    _create,
    _draft,
)

pytestmark = pytest.mark.asyncio


async def _seed(connection, schema, *, companies=2, networks=2):
    """Seed actual source reference heads; the tested store writes all links."""
    company_ids = tuple(uuid4() for _ in range(companies))
    group_id = uuid4()
    await connection.execute(
        f'INSERT INTO "{schema}".company_registry(company_id,display_name,roles) '
        "SELECT company_id,'Example Company',ARRAY['employer'] FROM unnest($1::uuid[]) company(company_id)",
        company_ids,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".company_group_registry(group_id,group_kind,display_name) '
        "VALUES($1,'corporate_parent','Example Group')",
        group_id,
    )
    network_ids = tuple(
        await connection.fetchval(
            f'WITH allocated AS (INSERT INTO "{schema}".network_registry_identity(allocation_key) '
            "SELECT md5($1::text||number::text)::uuid FROM generate_series(1,$2::integer) number RETURNING network_id) "
            "SELECT array_agg(network_id ORDER BY network_id) FROM allocated",
            uuid4().hex,
            networks,
        )
    )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) '
        "SELECT network_id,'Example Network' FROM unnest($1::integer[]) network(network_id)",
        network_ids,
    )
    return company_ids, group_id, network_ids


def _batch(company_ids, network_ids=(), *, expected=0, group=None, selection_group=None):
    return CompanyLinkBatchCommand(
        tuple(CompanyLinkBatchTarget(company_id, expected, network_ids, group) for company_id in company_ids),
        "Reviewed explicit selected company relationships",
        uuid4().hex,
        selection_group,
    )


def _assertion(company_id, network_id, **changes):
    return {
        "company_id": str(company_id),
        "network_id": network_id,
        "relationship_role": "uses",
        "benefit_domain": None,
        "applicability": "national",
        "states": [],
        "valid_from": None,
        "valid_to": None,
        "evidence_text": "Reviewed explicit relationship",
        **changes,
    }


async def _apply(connection, schema, command, actor):
    async with connection.transaction():
        return await apply_company_network_link_batch(connection, command, actor, control_schema=schema)


async def _state(connection, schema):
    return tuple(
        await connection.fetchrow(
            f"SELECT draft_revision,approved_revision,"
            f'(SELECT count(*) FROM "{schema}".company_registry_links),'
            f"(SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'),"
            f'(SELECT count(*) FROM "{schema}".registry_company_link_batch) FROM "{schema}".registry_revision_control'
        )
    )


async def test_full_drafts_preserve_explicit_periods_scope_and_approved_map(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    companies, group, networks = await _seed(connection, schema)
    approved_group = await _draft(engine, schema, _create(), actor)
    approved = await _approve(connection, schema, await _command(connection, schema, approved_group), actor)
    before_map = await _approved(connection, schema, approved["approved_revision"])
    first = _assertion(
        companies[0], networks[0], benefit_domain="medical", valid_from="2024-01-01", valid_to="2025-01-01"
    )
    next_period_by_field = {
        **first,
        "valid_from": "2025-01-01",
        "valid_to": None,
        "evidence_text": "Next explicit edition period",
    }
    operates = _assertion(
        companies[0], networks[1], relationship_role="operates", applicability="states", states=["CA", "NY"]
    )
    command = _batch(companies, networks, group=group, selection_group=group)
    command = replace(
        command,
        targets=(
            replace(command.targets[0], network_assertions=[next_period_by_field, first, operates]),
            command.targets[1],
        ),
    )
    receipt = await _apply(connection, schema, command, actor)
    assert set(receipt) == {"batch_id", "custom_revision", "records"}
    assert UUID(receipt["batch_id"]).int and len(receipt["records"]) == 2
    assert receipt["custom_revision"] == approved["approved_revision"] + 1
    for link_receipt in receipt["records"]:
        assert set(link_receipt) == {"record_kind", "record_id", "revision", "custom_revision", "record"}
        assert link_receipt["record_kind"] == "company_links" and link_receipt["revision"] == 1
        assert link_receipt["custom_revision"] == receipt["custom_revision"]
    documents_by_company = {link_receipt["record_id"]: link_receipt["record"] for link_receipt in receipt["records"]}
    assertions = documents_by_company[str(companies[0])]["network_assertions"]
    assert assertions == [first, next_period_by_field, operates]
    assert assertions[-1]["benefit_domain"] is None and assertions[-1]["states"] == ["CA", "NY"]
    assert documents_by_company[str(companies[1])]["network_assertions"] == []
    assert await _approved(connection, schema, approved["approved_revision"]) == before_map
    assert (await _state(connection, schema))[:2] == (receipt["custom_revision"], approved["approved_revision"])
    histories = await connection.fetch(
        f"SELECT actor_json,reason,idempotency_key,record_json,custom_revision FROM \"{schema}\".registry_record_history WHERE record_kind='company_links'"
    )
    assert len(histories) == 2
    for history in histories:
        assert history["idempotency_key"] == "batch:" + receipt["batch_id"]
        assert json.loads(history["actor_json"])["user_id"] == str(actor.user_id)
        assert history["reason"] == command.reason and history["custom_revision"] == receipt["custom_revision"]
        assert (
            json.loads(history["record_json"]) == documents_by_company[json.loads(history["record_json"])["company_id"]]
        )


async def test_replay_binds_the_whole_selected_set_fields_actor_and_group_context(serving_schema, monkeypatch):
    connection, schema, _ = serving_schema
    actor = _actor()
    companies, group, networks = await _seed(connection, schema, companies=3)
    command = _batch(companies[:2], networks, selection_group=group)
    first = await _apply(connection, schema, command, actor)
    clear = replace(
        command,
        targets=tuple(
            replace(batch_target, expected_revision=1, network_ids=(), group_id=None)
            for batch_target in command.targets
        ),
        idempotency_key=uuid4().hex,
    )
    cleared = await _apply(connection, schema, clear, actor)
    assert all(
        link_receipt["record"]["network_ids"] == []
        and link_receipt["record"]["network_assertions"] == []
        and link_receipt["record"]["group_id"] is None
        for link_receipt in cleared["records"]
    )
    assert await _apply(connection, schema, replace(command, targets=tuple(reversed(command.targets))), actor) == first
    conflicts = [
        replace(command, reason="Changed retry"),
        replace(command, targets=command.targets[:1]),
        replace(command, targets=(_batch(companies[2:], networks).targets[0],)),
        replace(command, targets=(replace(command.targets[0], network_ids=()), command.targets[1])),
        replace(command, selection_group_id=None),
        replace(command, targets=tuple(replace(batch_target, expected_revision=2) for batch_target in command.targets)),
    ]
    for conflict in conflicts:
        with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
            await _apply(connection, schema, conflict, actor)
    with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
        await _apply(connection, schema, command, replace(actor, impersonator_id=uuid4()))
    assert await _state(connection, schema) == (2, 0, 2, 4, 2)
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    assert await _apply(connection, schema, command, actor) == first
    with pytest.raises(RegistryAddressUnavailable, match="native_unavailable"):
        await _apply(
            connection,
            schema,
            replace(
                clear,
                targets=tuple(replace(batch_target, expected_revision=2) for batch_target in clear.targets),
                idempotency_key=uuid4().hex,
            ),
            actor,
        )


@pytest.mark.parametrize(
    "failure",
    [
        "stale",
        "unknown_company",
        "archived_company",
        "unknown_group",
        "archived_group",
        "unknown_network",
        "unallocated_network",
        "archived_network",
        "archived_links",
        "wrong_assertion_company",
        "wrong_assertion_network",
    ],
)
async def test_one_invalid_or_stale_last_target_rejects_the_whole_set(serving_schema, failure):
    connection, schema, _ = serving_schema
    actor = _actor()
    companies, group, networks = await _seed(connection, schema)
    command = _batch(companies, networks)
    last = command.targets[-1]
    expected_error = ValueError
    if failure == "stale":
        last = replace(last, expected_revision=1)
        expected_error = RegistryRecordConflict
    if failure == "unknown_company":
        last = replace(last, company_id=uuid4())
    if failure == "archived_company":
        await connection.execute(
            f'UPDATE "{schema}".company_registry SET archived=true WHERE company_id=$1', last.company_id
        )
    if failure == "unknown_group":
        last = replace(last, group_id=uuid4())
    if failure == "archived_group":
        await connection.execute(f'UPDATE "{schema}".company_group_registry SET archived=true WHERE group_id=$1', group)
        last = replace(last, group_id=group)
    if failure == "unknown_network":
        last = replace(last, network_ids=(2147483647,))
    if failure == "unallocated_network":
        await connection.execute(
            f"INSERT INTO \"{schema}\".network_registry_record(network_id,display_name) VALUES(2147483646,'Unallocated example')"
        )
        last = replace(last, network_ids=(2147483646,))
    if failure == "archived_network":
        await connection.execute(
            f'UPDATE "{schema}".network_registry_record SET archived=true WHERE network_id=$1', networks[-1]
        )
    if failure == "archived_links":
        await _apply(connection, schema, _batch(companies[-1:], networks), actor)
        await connection.execute(
            f'UPDATE "{schema}".company_registry_links SET archived=true WHERE company_id=$1', last.company_id
        )
        last = replace(last, expected_revision=1)
        expected_error = RegistryRecordConflict
    if failure == "wrong_assertion_company":
        last = replace(last, network_assertions=[_assertion(companies[0], networks[0])])
    if failure == "wrong_assertion_network":
        last = replace(last, network_ids=networks[:1], network_assertions=[_assertion(last.company_id, networks[-1])])
    before = await _state(connection, schema)
    with pytest.raises(expected_error):
        await _apply(connection, schema, replace(command, targets=(command.targets[0], last)), actor)
    assert await _state(connection, schema) == before
    assert (
        await connection.fetchval(
            f'SELECT count(*) FROM "{schema}".company_registry_links WHERE company_id=$1', companies[0]
        )
        == 0
    )


async def test_group_selection_is_context_only_and_never_expands_members(serving_schema):
    connection, schema, _ = serving_schema
    actor = _actor()
    companies, group, networks = await _seed(connection, schema, companies=3)
    independent = await _apply(connection, schema, _batch(companies[1:], group=group), actor)
    selected = await _apply(connection, schema, _batch(companies[:1], networks, selection_group=group), actor)
    assert len(selected["records"]) == 1 and selected["records"][0]["record"]["group_id"] is None
    for link_receipt in independent["records"]:
        head = json.loads(
            await connection.fetchval(
                f'SELECT to_jsonb(head)::text FROM "{schema}".company_registry_links head WHERE company_id=$1',
                UUID(link_receipt["record_id"]),
            )
        )
        assert head == link_receipt["record"] and head["network_ids"] == []


async def test_native_validation_and_fixed_roundtrips(serving_schema, monkeypatch):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema, companies=100)
    actor = _actor()
    native = store._fast_module()
    assert callable(native.validate_company_network_assertions)
    native_calls = []

    def validate(input_bytes):
        """Count real PyO3 calls without replacing its native validation."""
        native_calls.append(len(json.loads(input_bytes)))
        return native.validate_company_network_assertions(input_bytes)

    monkeypatch.setattr(store, "_fast_module", lambda: SimpleNamespace(validate_company_network_assertions=validate))
    counts = []
    for company_set in (companies[:1], companies):
        counted = _CountedConnection(connection)
        command = _batch(company_set, networks)
        command = replace(
            command,
            targets=tuple(
                replace(
                    batch_target,
                    expected_revision=int(index == 0 and len(company_set) > 1),
                    network_assertions=[_assertion(batch_target.company_id, networks[0])],
                )
                for index, batch_target in enumerate(command.targets)
            ),
        )
        receipt = await _apply(counted, schema, command, actor)
        assert len(receipt["records"]) == len(company_set)
        counts.append(counted.statements)
    assert counts == [8, 8] and native_calls == [1, 100]
    assert await _state(connection, schema) == (2, 0, 100, 101, 2)


async def test_overlapping_evidence_requires_review_and_never_creates_a_partial_draft(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command = _batch(companies, networks)
    first = _assertion(companies[-1], networks[0], valid_from="2024-01-01", valid_to="2025-01-01")
    overlap_by_field = {**first, "valid_from": "2024-06-01", "evidence_text": "Different retained evidence"}
    command = replace(
        command, targets=(command.targets[0], replace(command.targets[1], network_assertions=[first, overlap_by_field]))
    )
    with pytest.raises(RegistryRecordConflict, match="review_required"):
        await _apply(connection, schema, command, _actor())
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_outer_rollback_and_native_history_failure_preserve_caller_work(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command, actor = _batch(companies, networks), _actor()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await apply_company_network_link_batch(connection, command, actor, control_schema=schema)
            raise RuntimeError("caller rollback")
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)
    await connection.execute(
        f"ALTER TABLE \"{schema}\".registry_record_history ADD CONSTRAINT test_reject_last_company CHECK(record_key<>'{companies[-1]}')"
    )
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE batch_caller_work(value integer) ON COMMIT DROP")
        await connection.execute("INSERT INTO batch_caller_work VALUES(7)")
        with pytest.raises(asyncpg.CheckViolationError):
            await apply_company_network_link_batch(connection, command, actor, control_schema=schema)
        assert await connection.fetchval("SELECT value FROM batch_caller_work") == 7
        assert await _state(connection, schema) == (0, 0, 0, 0, 0)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_company_links_%'"
            )
            == 0
        )


async def test_receipt_overflow_rolls_back_every_head_history_and_control_write(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema, companies=100, networks=5000)
    with pytest.raises(ValueError, match="receipt_limit"):
        await _apply(connection, schema, _batch(companies, networks), _actor())
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_actor_schema_transaction_and_exact_command_bounds_fail_before_mutations(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command, actor = _batch(companies, networks), _actor()
    with pytest.raises(ValueError, match="caller_transaction"):
        await apply_company_network_link_batch(connection, command, actor, control_schema=schema)
    invalid_commands = [
        replace(command, targets=()),
        replace(command, targets=command.targets * 51),
        replace(command, targets=(command.targets[0], command.targets[0])),
        replace(command, reason=" "),
        replace(command, idempotency_key=" bad "),
        replace(command, idempotency_key="a" * 129),
        replace(command, selection_group_id=UUID(int=0)),
    ]
    for batch_target in [
        replace(command.targets[0], company_id=UUID(int=0)),
        replace(command.targets[0], expected_revision=True),
        replace(command.targets[0], expected_revision=9223372036854775806),
        replace(command.targets[0], network_ids=(True,)),
        replace(command.targets[0], network_ids=tuple(reversed(networks))),
        replace(command.targets[0], network_ids=(networks[0], networks[0])),
        replace(command.targets[0], group_id=str(uuid4())),
        replace(command.targets[0], network_assertions=[{}] * 5001),
        replace(command.targets[0], network_assertions=[{"value": float("nan")}]),
    ]:
        invalid_commands.append(replace(command, targets=(batch_target,)))
    for rejected in invalid_commands:
        with pytest.raises(ValueError):
            await _apply(connection, schema, rejected, actor)
    for invalid_actor in [replace(actor, user_id=UUID(int=0)), replace(actor, client_id=" bad "), {}]:
        with pytest.raises(ValueError):
            await _apply(connection, schema, command, invalid_actor)
    with pytest.raises(ValueError):
        await _apply(connection, 'invalid";DROP SCHEMA public;', command, actor)
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_concurrent_identical_commands_return_one_immutable_batch(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command, actor = _batch(companies, networks), _actor()
    other = await asyncpg.connect(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    try:
        first, second = await asyncio.gather(
            _apply(connection, schema, command, actor), _apply(other, schema, command, actor)
        )
        assert first == second and await _state(connection, schema) == (1, 0, 2, 2, 1)
    finally:
        await other.close()


async def test_nested_command_fields_are_sealed_before_control_lock_wait(serving_schema):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    entered, release = asyncio.Event(), asyncio.Event()

    class PausedConnection(_CountedConnection):
        """Delay the real control read; every query still executes on PostgreSQL."""

        async def fetchrow(self, query, *arguments):
            if "FOR UPDATE" in query:
                entered.set()
                await release.wait()
            return await super().fetchrow(query, *arguments)

    assertion = _assertion(companies[0], networks[0])
    command = _batch(companies[:1], networks)
    command = replace(command, targets=(replace(command.targets[0], network_assertions=[assertion]),))
    actor = _actor()
    original_assertion_by_field = {**assertion}
    running = asyncio.create_task(_apply(PausedConnection(connection), schema, command, actor))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assertion["evidence_text"] = "Changed after invocation"
        release.set()
        receipt = await running
    finally:
        release.set()
        if not running.done():
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
    assert receipt["records"][0]["record"]["network_assertions"] == [original_assertion_by_field]
    immutable_retry = replace(
        command,
        targets=(replace(command.targets[0], network_assertions=[original_assertion_by_field]),),
    )
    assert await _apply(connection, schema, immutable_retry, actor) == receipt
    with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
        await _apply(connection, schema, command, actor)
