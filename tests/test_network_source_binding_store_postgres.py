# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native COPY, scoped source CAS and atomic durable batch acceptance."""

import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import network_source_binding_store as store
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_record_store import RegistryAddressUnavailable, RegistryRecordConflict
from tests.test_network_source_binding_schema_postgres import network_binding_db
from tests.test_registry_approval_store_postgres import _CountedConnection
from tests.test_registry_record_store_postgres import _actor, record_db

pytestmark = pytest.mark.asyncio


async def _seed(connection, schema, count=2):
    """Create canonical native identity and active management heads set-wise."""
    networks = await connection.fetchval(
        f'WITH allocated AS (INSERT INTO "{schema}".network_registry_identity(allocation_key) '
        "SELECT md5($1::text||number::text)::uuid FROM generate_series(1,$2::integer) number RETURNING network_id) "
        "SELECT array_agg(network_id ORDER BY network_id) FROM allocated",
        uuid4().hex,
        count,
    )
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_record(network_id,display_name) '
        "SELECT network_id,'Example Network' FROM unnest($1::integer[]) network(network_id)",
        networks,
    )
    return networks


def _row(network_id, **changes):
    return {
        "binding_id": str(uuid4()),
        "source_system": "ptg",
        "source_id": "synthetic-source",
        "dataset_schema": "synthetic_dataset",
        "dataset_id": "dataset-one",
        "producer_id": "producer-one",
        "edition_id": "edition-one",
        "source_key": "opaque-network-key",
        "source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
        "network_id": network_id,
        "evidence_id": "synthetic-review",
        "evidence_sha256": "a" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
        **changes,
    }


def _command(rows):
    return NetworkSourceBindingBatchCommand(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode(),
        "Reviewed exact source scope",
        uuid4().hex,
    )


async def _apply(connection, schema, command, actor):
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


async def _state(connection, schema):
    return tuple(
        await connection.fetchrow(
            f'SELECT draft_revision,approved_revision,(SELECT count(*) FROM "{schema}".registry_network_binding),'
            f"(SELECT count(*) FROM \"{schema}\".registry_record_history WHERE record_kind='network_binding'),"
            f'(SELECT count(*) FROM "{schema}".registry_network_binding_batch) FROM "{schema}".registry_revision_control'
        )
    )


async def test_bind_read_and_exact_replay_preserve_approval(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    input_row = _row(networks[0])
    command = _command([input_row])
    receipt = await _apply(connection, schema, command, actor)
    assert set(receipt) == {"custom_revision", "records"}
    assert receipt == {
        "custom_revision": 1,
        "records": [
            {
                "record_kind": "network_binding",
                "record_id": input_row["binding_id"],
                "revision": 1,
                "network_id": networks[0],
                "archived": False,
            }
        ],
    }
    heads = await store.list_network_source_bindings(connection, networks[0], control_schema=schema)
    assert len(heads) == 1 and heads[0]["binding_id"] == input_row["binding_id"]
    assert heads[0]["source_scope_json"] == input_row["source_scope_json"]
    assert len(heads[0]["binding_key"]) == 64 and heads[0]["revision"] == 1
    assert await store.list_network_source_bindings(connection, networks[1], control_schema=schema) == []
    assert (
        await store.list_network_source_bindings(
            connection,
            networks[0],
            source_system="ptg",
            binding_key=heads[0]["binding_key"],
            control_schema=schema,
        )
        == heads
    )
    monkeypatch.setattr(store, "_fast_module", lambda: None)
    assert await _apply(connection, schema, command, actor) == receipt
    assert await _state(connection, schema) == (1, 0, 1, 1, 1)


async def test_rebind_close_reopen_retain_immutable_full_history(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    original = _row(networks[0])
    await _apply(connection, schema, _command([original]), actor)
    for operation, old_revision, previous, target_network_id, archived in [
        ("rebind", 1, networks[0], networks[1], False),
        ("close", 2, networks[1], networks[1], True),
        ("rebind", 3, networks[1], networks[0], False),
    ]:
        changed_by_field = {
            **original,
            "operation": operation,
            "expected_revision": old_revision,
            "expected_network_id": previous,
            "network_id": target_network_id,
            "evidence_id": f"review-{old_revision}",
        }
        receipt = await _apply(connection, schema, _command([changed_by_field]), actor)
        assert receipt["records"][0]["revision"] == old_revision + 1
        assert receipt["records"][0]["archived"] is archived
    history = await connection.fetch(
        f'SELECT revision,record_json,actor_json,custom_revision,idempotency_key FROM "{schema}".registry_record_history '
        "WHERE record_kind='network_binding' ORDER BY revision"
    )
    assert [entry["revision"] for entry in history] == [1, 2, 3, 4]
    documents = [json.loads(entry["record_json"]) for entry in history]
    assert [document["network_id"] for document in documents] == [networks[0], networks[1], networks[1], networks[0]]
    assert [document["archived"] for document in documents] == [False, False, True, False]
    assert len({document["binding_key"] for document in documents}) == 1
    assert all(document["source_scope_json"] == original["source_scope_json"] for document in documents)
    assert len({entry["idempotency_key"] for entry in history}) == 4
    assert all(json.loads(entry["actor_json"])["user_id"] == str(actor.user_id) for entry in history)
    assert await _state(connection, schema) == (4, 0, 1, 4, 4)


async def test_scope_edition_and_key_conflicts_are_explicit(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    first = _row(networks[0])
    await _apply(connection, schema, _command([first]), actor)
    second_by_field = {**first, "binding_id": str(uuid4()), "edition_id": "edition-two"}
    await _apply(connection, schema, _command([second_by_field]), actor)
    assert await _state(connection, schema) == (2, 0, 2, 2, 2)
    for changed in [
        {**first, "binding_id": str(uuid4())},
        {
            **first,
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": networks[0],
            "edition_id": "wrong-edition",
        },
        {
            **first,
            "operation": "rebind",
            "expected_revision": 1,
            "expected_network_id": networks[0],
            "binding_id": str(uuid4()),
        },
        {**first, "operation": "rebind", "expected_revision": 1, "expected_network_id": networks[1]},
    ]:
        with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
            await _apply(connection, schema, _command([changed]), actor)
        assert await _state(connection, schema) == (2, 0, 2, 2, 2)


async def test_stale_member_rejects_whole_selected_batch(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    first = _row(networks[0])
    await _apply(connection, schema, _command([first]), actor)
    valid_new = _row(networks[1], source_key="second-key")
    stale_by_field = {**first, "operation": "rebind", "expected_revision": 2, "expected_network_id": networks[0]}
    with pytest.raises(RegistryRecordConflict):
        await _apply(connection, schema, _command([valid_new, stale_by_field]), actor)
    assert await _state(connection, schema) == (1, 0, 1, 1, 1)
    heads = await store.list_network_source_bindings(connection, networks[1], control_schema=schema)
    assert heads == []


async def test_replay_binds_exact_bytes_reason_and_complete_actor(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    receipt = await _apply(connection, schema, command, actor)
    for changed in [
        replace(command, input_bytes=command.input_bytes + b" "),
        replace(command, reason="Different review"),
    ]:
        with pytest.raises(RegistryRecordConflict, match="idempotency_conflict"):
            await _apply(connection, schema, changed, actor)
    for changed_actor in [
        replace(actor, user_id=uuid4()),
        replace(actor, client_id="another_client"),
        replace(actor, impersonator_id=uuid4()),
    ]:
        with pytest.raises(RegistryRecordConflict, match="revision_conflict"):
            await _apply(connection, schema, command, changed_actor)
    other = replace(_command([_row(networks[1], source_key="other-key")]), idempotency_key=command.idempotency_key)
    await _apply(connection, schema, other, replace(actor, user_id=uuid4()))
    assert await _apply(connection, schema, command, actor) == receipt
    assert await _state(connection, schema) == (2, 0, 2, 2, 2)


async def test_archived_network_allows_closure_but_not_bind_or_reopen(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    original = _row(networks[0])
    await _apply(connection, schema, _command([original]), actor)
    await connection.execute(
        f'UPDATE "{schema}".network_registry_record SET archived=true WHERE network_id=$1', networks[0]
    )
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([_row(networks[0], source_key="other")]), actor)
    close_by_field = {**original, "operation": "close", "expected_revision": 1, "expected_network_id": networks[0]}
    receipt = await _apply(connection, schema, _command([close_by_field]), actor)
    assert receipt["records"][0]["archived"] is True
    reopen_by_field = {**close_by_field, "operation": "rebind", "expected_revision": 2}
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([reopen_by_field]), actor)
    with pytest.raises(RegistryRecordConflict):
        await _apply(connection, schema, _command([{**close_by_field, "expected_revision": 2}]), actor)
    assert await _state(connection, schema) == (2, 0, 1, 2, 2)


async def test_rebind_preserves_observational_fhir_alias(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    legacy = str(uuid4())
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias(source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id) '
        "VALUES('fhir','synthetic-source','legacy_fhir_uuid',$1,'alias-one',$2,'review-alias')",
        legacy,
        networks[0],
    )
    original = _row(
        networks[0],
        source_system="fhir",
        source_scope_json={
            "organization_id": "Organization-1",
            "legacy_uuid": legacy,
            "alias_scope": "alias-one",
        },
    )
    await _apply(connection, schema, _command([original]), actor)
    conflicting_by_field = {
        **original,
        "network_id": networks[1],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": networks[0],
    }
    corrected = await _apply(connection, schema, _command([conflicting_by_field]), actor)
    assert corrected["records"][0]["network_id"] == networks[1]
    separate_by_field = {
        **original,
        "binding_id": str(uuid4()),
        "network_id": networks[1],
        "source_scope_json": {**original["source_scope_json"], "alias_scope": "alias-two"},
    }
    await _apply(connection, schema, _command([separate_by_field]), actor)
    assert await connection.fetchval(f'SELECT network_id FROM "{schema}".network_registry_alias') == networks[0]
    assert await _state(connection, schema) == (3, 0, 2, 3, 3)


class _CountedCopyConnection(_CountedConnection):
    def __init__(self, connection):
        super().__init__(connection)
        self.copies = 0

    async def copy_to_table(self, *arguments, **keywords):
        self.copies += 1
        return await self.connection.copy_to_table(*arguments, **keywords)


async def test_one_and_hundred_rows_use_fixed_statements_and_one_copy(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    counts = []
    for count in [1, 100]:
        wrapped = _CountedCopyConnection(connection)
        rows = [_row(networks[0], source_key=f"key-{count}-{index}") for index in range(count)]
        receipt = await _apply(wrapped, schema, _command(rows), actor)
        assert len(receipt["records"]) == count
        assert receipt["records"] == sorted(receipt["records"], key=lambda record: record["record_id"])
        assert all(record["revision"] == 1 for record in receipt["records"])
        assert wrapped.copies == 1
        counts.append(wrapped.statements)
    assert counts == [8, 8]
    assert await _state(connection, schema) == (2, 0, 101, 101, 2)


async def test_invalid_native_last_row_and_unavailable_leave_prior_work(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    invalid = _row(True, source_key="bad-key")
    async with connection.transaction():
        await connection.execute(f"UPDATE \"{schema}\".network_registry_record SET display_name='Retained prior work'")
        with pytest.raises(ValueError, match="input_invalid"):
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0]), invalid]), actor, control_schema=schema
            )
        assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0
        monkeypatch.setattr(store, "_fast_module", lambda: None)
        with pytest.raises(RegistryAddressUnavailable):
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0])]), actor, control_schema=schema
            )
    assert (
        await connection.fetchval(f'SELECT min(display_name) FROM "{schema}".network_registry_record')
        == "Retained prior work"
    )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


@pytest.mark.parametrize("mode", ["driver_failure", "wrong_count", "unconsumed"])
async def test_copy_failure_rolls_back_stage_and_batch(network_binding_db, monkeypatch, mode):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    wrapped = _CountedCopyConnection(connection)

    async def faulty_copy(*arguments, **keywords):
        status = await connection.copy_to_table(*arguments, **keywords)
        if mode == "driver_failure":
            raise RuntimeError("synthetic driver failure")
        if mode == "unconsumed":
            keywords["source"].seek(0)
        return "COPY 0" if mode == "wrong_count" else status

    monkeypatch.setattr(wrapped, "copy_to_table", faulty_copy)
    async with connection.transaction():
        with pytest.raises(RegistryAddressUnavailable):
            await apply_network_source_binding_batch(
                wrapped, _command([_row(networks[0])]), actor, control_schema=schema
            )
        assert await connection.fetchval("SELECT 1") == 1
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_source_binding_%'"
            )
            == 0
        )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_transaction_missing_references_and_read_bounds_are_strict(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    with pytest.raises(ValueError, match="requires_caller_transaction"):
        await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)
    with pytest.raises(ValueError, match="target_invalid"):
        await _apply(connection, schema, _command([_row(2147483647)]), actor)
    for arguments in [
        {"network_id": True},
        {"network_id": 0},
        {"network_id": 2147483648},
        {"source_system": "label"},
        {"binding_key": "A" * 64},
        {"limit": True},
        {"limit": 101},
        {"offset": -1},
    ]:
        with pytest.raises(ValueError):
            await store.list_network_source_bindings(
                connection, **{"network_id": networks[0], **arguments}, control_schema=schema
            )
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_caller_rollback_retains_no_batch_heads_or_history(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await apply_network_source_binding_batch(
                connection, _command([_row(networks[0])]), actor, control_schema=schema
            )
            raise RuntimeError("caller rollback")
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_corrupt_native_receipts_fail_closed_before_copy(network_binding_db, monkeypatch):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    copy_bytes, row_count = store._encode(command.input_bytes)
    for encoded in [
        [copy_bytes, row_count],
        (copy_bytes,),
        (copy_bytes, True),
        (copy_bytes, 5001),
        ("not-bytes", row_count),
        (b"invalid", row_count),
        (copy_bytes + b"x", row_count),
        (store._COPY_HEADER + b"x" * store._MAX_COPY_BYTES + b"\xff\xff", row_count),
    ]:
        monkeypatch.setattr(
            store, "_fast_module", lambda: SimpleNamespace(encode_network_source_binding_batch=lambda _: encoded)
        )
        with pytest.raises(RegistryAddressUnavailable, match="native_unavailable"):
            await _apply(connection, schema, command, actor)
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)


async def test_empty_and_malformed_control_inputs_are_rejected(network_binding_db):
    connection, schema, _ = network_binding_db
    networks, actor = await _seed(connection, schema), _actor()
    command = _command([_row(networks[0])])
    for changed in [
        replace(command, input_bytes=b"[]"),
        replace(command, input_bytes=b""),
        replace(command, input_bytes=b"x" * (store._MAX_INPUT_BYTES + 1)),
        replace(command, input_bytes=bytearray(command.input_bytes)),
        replace(command, reason=" "),
        replace(command, idempotency_key=" padded"),
        replace(command, idempotency_key="a\nb"),
    ]:
        with pytest.raises(ValueError):
            await _apply(connection, schema, changed, actor)
    with pytest.raises(ValueError, match="uuid_invalid"):
        await _apply(connection, schema, command, replace(actor, user_id=str(actor.user_id)))
    assert await _state(connection, schema) == (0, 0, 0, 0, 0)
