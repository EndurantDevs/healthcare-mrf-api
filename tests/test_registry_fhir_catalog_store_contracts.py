# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure orchestration contracts for isolated bounded catalog storage."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import registry_fhir_catalog_store as store
from process.registry_fhir_catalog_custody import FHIRCatalogTarget


def _candidate():
    candidate_id = uuid4()
    return FHIRCatalogTarget(*(str(uuid4()) for _ in range(3)), str(candidate_id), "fhir_catalog_" + candidate_id.hex)


def _connection(*, active=True):
    events, copied_batches = [], []

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def copy_rows(_table, *, records, columns, schema_name):
        copied_batches.append((tuple(records), columns, schema_name))

    async def execute(statement, *_parameters):
        if statement.startswith("INSERT INTO"):
            return "INSERT 0 " + str(len(copied_batches[-1][0]))
        return "OK"

    return SimpleNamespace(
        is_in_transaction=Mock(return_value=active),
        transaction=transaction,
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(return_value=False),
        fetchrow=AsyncMock(
            return_value={
                "nspowner": 101,
                "loader_oid": 101,
                "heaps": sorted(store.PHYSICAL_TABLES),
                "unsupported": False,
            }
        ),
        copy_records_to_table=AsyncMock(side_effect=copy_rows),
        events=events,
        copied_batches=copied_batches,
    )


def _network_row(network_id):
    return ("a" * 64, network_id, "Example Network", "[]", False, 1, None, "example network")


async def _write(connection, *, target=None, table="registry_fhir_catalog_network", rows=None, **options):
    return await store.write_fhir_catalog_rows(
        connection,
        _candidate() if target is None else target,
        table,
        [_network_row(1)] if rows is None else rows,
        loader_role="catalog_loader",
        control_schema="catalog_control",
        **options,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options,reason",
    [
        ({"max_rows": True}, "bounds_invalid"),
        ({"max_rows": 0}, "bounds_invalid"),
        ({"max_bytes": store.MAX_BYTES + 1}, "bounds_invalid"),
        ({"unexpected_option": 1}, "options_invalid"),
        ({"columns": ("network_id",)}, "columns_invalid"),
        ({"mode": "unknown"}, "mode_invalid"),
    ],
)
async def test_write_refuses_bad_options_before_candidate_locks_or_copy(options, reason):
    connection = _connection()
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_" + reason):
        await _write(connection, **options)
    assert connection.events == []
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["target", "table", "transaction"])
async def test_write_requires_registered_target_type_known_table_and_outer_transaction(change):
    connection = _connection(active=change != "transaction")
    options_by_name = {"target": object()} if change == "target" else {}
    if change == "table":
        options_by_name["table"] = "registry_fhir_catalog_snapshot"
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_"):
        await _write(connection, **options_by_name)
    assert connection.events == []
    connection.execute.assert_not_awaited()
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_creation_requires_an_existing_outer_transaction():
    connection = _connection(active=False)
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_transaction_required"):
        await store.create_fhir_catalog_candidate(
            connection, _candidate(), loader_role="catalog_loader", control_schema="catalog_control"
        )
    connection.execute.assert_not_awaited()
    assert connection.events == []


@pytest.mark.asyncio
async def test_native_mutability_check_refuses_outside_transaction_before_taking_locks():
    connection = _connection(active=False)
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_transaction_required"):
        await store._mutable(connection, _candidate(), "catalog_loader", "catalog_control")
    connection.execute.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_copy_stages_batches_and_returns_aggregate_accounting():
    connection = _connection()
    rows = [_network_row(index) for index in range(1, 4)]
    result = await _write(connection, rows=rows, max_rows=2, mode="preserve")
    assert [len(batch[0]) for batch in connection.copied_batches] == [2, 1]
    assert [row for batch, _, _ in connection.copied_batches for row in batch] == rows
    assert all(
        columns == store.TABLE_COLUMNS["registry_fhir_catalog_network"] for _, columns, _ in connection.copied_batches
    )
    assert all(schema == "pg_temp" for _, _, schema in connection.copied_batches)
    assert result["input_rows"] == result["affected_rows"] == 3
    assert result["batches"] == 2 and result["payload_bytes"] > 0
    inserts = [call.args[0] for call in connection.execute.await_args_list if call.args[0].startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert all("SELECT DISTINCT ON" in statement and "DO NOTHING" in statement for statement in inserts)
    assert connection.events == ["begin", "commit"]


@pytest.mark.asyncio
async def test_empty_candidate_write_returns_zero_accounting_without_copying_rows():
    connection = _connection()
    assert await _write(connection, rows=[]) == {
        "input_rows": 0,
        "affected_rows": 0,
        "batches": 0,
        "payload_bytes": 0,
    }
    connection.copy_records_to_table.assert_not_awaited()
    assert connection.events == ["begin", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows,reason",
    [([1], "row_shape_invalid"), ([("short",)], "row_shape_invalid"), ([(object(),) * 8], "row_value_invalid")],
)
async def test_invalid_copy_rows_rollback_without_sending_a_native_copy(rows, reason):
    connection = _connection()
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_" + reason):
        await _write(connection, rows=rows)
    assert connection.events == ["begin", "rollback"]
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
async def test_utf8_row_budget_refuses_oversized_record_before_copy():
    connection = _connection()
    row_values = list(_network_row(1))
    row_values[2] = "界" * 64
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_row_too_large"):
        await _write(connection, rows=[row_values], max_bytes=128)
    assert connection.events == ["begin", "rollback"]
    connection.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_copy_failure_or_task_cancellation_rolls_back_original_exception(failure):
    connection = _connection()
    error = failure("synthetic copy refusal")
    connection.copy_records_to_table.side_effect = error
    with pytest.raises(failure) as caught:
        await _write(connection)
    assert caught.value is error
    assert connection.events == ["begin", "rollback"]
    assert not any(call.args[0].startswith("INSERT INTO") for call in connection.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("generation,repeat", [("A" * 64, 1), ("a" * 64, 10001)])
async def test_relationship_replacement_rejects_invalid_or_unbounded_source_selection(generation, repeat):
    connection = _connection()
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_"):
        await store.replace_fhir_catalog_relationships(
            connection,
            _candidate(),
            [generation] * repeat,
            loader_role="catalog_loader",
            control_schema="catalog_control",
        )
    connection.execute.assert_not_awaited()
    assert connection.events == []


@pytest.mark.asyncio
async def test_carry_forward_cannot_target_its_sealed_source_schema(monkeypatch):
    connection, target = _connection(), _candidate()
    verify = AsyncMock()
    monkeypatch.setattr(store, "verify_fhir_catalog_snapshot", verify)
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_snapshot_immutable"):
        await store.copy_fhir_catalog_snapshot(
            connection,
            {"schema_name": target.schema_name},
            target,
            "a" * 64,
            loader_role="catalog_loader",
            control_schema="catalog_control",
        )
    verify.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_carry_forward_oversized_first_row_rolls_back_without_advancing_cursor(monkeypatch):
    connection = _connection()
    verify = AsyncMock()
    monkeypatch.setattr(store, "verify_fhir_catalog_snapshot", verify)
    page_by_field = {"seen": 1, "first_bytes": 128, "copied": 0}
    connection.fetchrow.side_effect = [connection.fetchrow.return_value, page_by_field]
    previous_by_field = {"schema_name": "previous_catalog"}
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_row_too_large"):
        await store.copy_fhir_catalog_snapshot(
            connection,
            previous_by_field,
            _candidate(),
            "a" * 64,
            loader_role="catalog_loader",
            control_schema="catalog_control",
            max_bytes=128,
        )
    verify.assert_awaited_once_with(connection, previous_by_field)
    assert connection.events == ["begin", "rollback"]
    assert connection.fetchrow.await_count == 2
