# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_rows_native as rows
from process import entity_address_rows_preparation as preparation
from tests.entity_address_rows_preparation_support import (
    apply_source_locale,
    assert_no_row_scratch,
    preparation_admission,
    preparation_database,
    source_select_sql,
)


def test_copy_export_refuses_overflow_without_retaining_partial_chunk(monkeypatch):
    monkeypatch.setattr(rows, "MAX_INPUT_BYTES", 8)
    buffer = preparation._BoundedCopyBuffer()
    assert buffer.write(b"abcd") == 4
    assert buffer.write(b"efgh") == 4
    with pytest.raises(RuntimeError, match="measured byte budget"):
        buffer.write(b"i")
    assert buffer.getvalue() == b"abcdefgh"


@pytest.mark.parametrize("include_version", [False, True])
def test_prepared_schema_keeps_the_callers_address_version_intent(include_version):
    sql = preparation.admission._native()
    statement = preparation._prepared_schema_select(sql, '"registry"."input_rows"', include_version)
    if include_version:
        assert "NULLIF(TRIM(b.base_address_version), '')" in statement
    else:
        assert "(NULL::text)::text AS base_address_version" in statement
        assert "b.base_address_version" not in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [None, False, 0, "12", 12])
async def test_unadmitted_scratch_requires_an_actual_positive_relation_oid(monkeypatch, oid):
    monkeypatch.setattr(preparation.admission, "create_stage_sql", AsyncMock())
    read = AsyncMock(return_value=oid)
    monkeypatch.setattr(preparation.admission, "read_scalar", read)
    token = preparation.admission._ADMISSION.set(None)
    owned_by_name = {}
    database = object()
    try:
        operation = preparation._create_scratch(
            SimpleNamespace(db=database), "registry", "input_rows", "CREATE TABLE input_rows()", owned_by_name, None
        )
        if type(oid) is int and oid > 0:
            await operation
            assert owned_by_name == {"input_rows": oid}
        else:
            with pytest.raises(RuntimeError, match="scratch identity is missing"):
                await operation
            assert not owned_by_name
    finally:
        preparation.admission._ADMISSION.reset(token)
    read.assert_awaited_once_with(
        database, "SELECT to_regclass(:relation)::oid::bigint", relation="registry.input_rows"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_oid", [False, True])
async def test_scratch_cleanup_locks_and_rechecks_identity_before_drop(monkeypatch, changed_oid):
    entered, dropped = [], []

    @asynccontextmanager
    async def transaction():
        entered.append(True)
        yield

    async def drop(schema, name):
        dropped.append((schema, name))

    monkeypatch.setattr(preparation.admission, "native_transaction", transaction)
    monkeypatch.setattr(preparation.admission, "drop_stage", drop)
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=99 if changed_oid else 12))
    token = preparation.admission._ADMISSION.set(None)
    try:
        operation = preparation._drop_scratch(
            SimpleNamespace(db=database), "registry", ("never_created", "input_rows"), {"input_rows": 12}
        )
        if changed_oid:
            with pytest.raises(RuntimeError, match="scratch identity changed"):
                await operation
            assert dropped == []
        else:
            await operation
            assert dropped == [("registry", "input_rows")]
    finally:
        preparation.admission._ADMISSION.reset(token)
    assert entered == [True]
    assert [call.args[0] for call in database.status.await_args_list] == [
        "SET LOCAL lock_timeout='500ms'",
        'LOCK TABLE "registry"."input_rows" IN ACCESS EXCLUSIVE MODE NOWAIT',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("native_count,copy_result", [(2, "COPY 2"), (1, "COPY 2"), (2, "COPY 1")])
async def test_copy_batch_requires_both_native_and_database_row_accounting(monkeypatch, native_count, copy_result):
    async def export(_query, *bounds, output, **copy_options_by_name):
        assert bounds == (1, 2) and copy_options_by_name == {"format": "binary"}
        output.write(b"input")

    driver = SimpleNamespace(copy_from_query=export, copy_to_table=AsyncMock(return_value=copy_result))
    encode = AsyncMock(return_value=(b"prepared", native_count))
    monkeypatch.setattr(preparation, "_prepare_bytes", encode)
    operation = preparation._copy_batch(driver, "registry", ("input_rows", "prepared_rows"), 1, 2, False)
    if native_count == 2 and copy_result == "COPY 2":
        await operation
    else:
        with pytest.raises(RuntimeError, match="COPY accounting differs"):
            await operation
    encode.assert_awaited_once_with(b"input")
    invocation = driver.copy_to_table.await_args
    assert invocation.args == ("prepared_rows",)
    assert invocation.kwargs["schema_name"] == "registry"
    assert invocation.kwargs["source"].getvalue() == b"prepared"


@pytest.mark.asyncio
@pytest.mark.parametrize("census_differs", [False, True])
async def test_oversized_row_fallback_keeps_remaining_rows_in_one_native_batch(monkeypatch, census_differs):
    driver = SimpleNamespace(fetchval=AsyncMock(side_effect=[3, census_differs]), execute=AsyncMock())
    bounds = AsyncMock(side_effect=[None, 3])
    copy = AsyncMock()
    monkeypatch.setattr(preparation, "_batch_end", bounds)
    monkeypatch.setattr(preparation, "_copy_batch", copy)
    monkeypatch.setattr(preparation.admission, "check_owned_storage", AsyncMock())
    operation = preparation._fill_prepared_rows(
        driver, "registry", ("input_rows", "prepared_rows"), "SELECT * FROM source_rows s", False
    )
    if census_differs:
        with pytest.raises(RuntimeError, match="candidate census differs"):
            await operation
    else:
        await operation
    assert [call.args[2] for call in bounds.await_args_list] == [1, 2]
    copy.assert_awaited_once_with(driver, "registry", ("input_rows", "prepared_rows"), 2, 3, False)
    assert driver.execute.await_args_list[0].args == (
        'INSERT INTO "registry"."prepared_rows" SELECT * FROM source_rows s WHERE s.row_id=$1',
        1,
    )
    assert driver.execute.await_args_list[-1].args == ('LOCK TABLE "registry"."prepared_rows" IN SHARE MODE NOWAIT',)


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted", [False, True])
async def test_missing_native_encoder_never_falls_back_from_an_admitted_build(monkeypatch, admitted):
    monkeypatch.setattr(rows, "native_preparation_module", lambda: None)
    consume = AsyncMock(return_value="consumed")
    token = preparation.admission._ADMISSION.set(object() if admitted else None)
    try:
        operation = preparation.run_prepared_source(None, "registry", "SELECT * FROM source_rows", consume)
        if admitted:
            with pytest.raises(RuntimeError, match="native preparation is unavailable"):
                await operation
            consume.assert_not_awaited()
        else:
            assert await operation == "consumed"
            consume.assert_awaited_once_with("SELECT * FROM source_rows", native_prepared=False)
    finally:
        preparation.admission._ADMISSION.reset(token)


@pytest.mark.asyncio
async def test_candidate_copy_refuses_a_driver_outside_the_executing_transaction(monkeypatch):
    session = object()
    exit_events = []

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"row_scratch": None}
        try:
            yield session
        finally:
            exit_events.append(True)

    monkeypatch.setattr(preparation.admission, "native_transaction", transaction)
    driver = SimpleNamespace(is_in_transaction=lambda: False, execute=AsyncMock())
    borrow = AsyncMock(return_value=driver)
    monkeypatch.setattr(preparation, "_copy_driver", borrow)
    sql = SimpleNamespace(
        db=SimpleNamespace(status=AsyncMock()), _apply_entity_address_transaction_settings=AsyncMock()
    )
    consume = AsyncMock()
    with pytest.raises(RuntimeError, match="requires its executing transaction"):
        await preparation._consume_candidate(
            sql,
            "registry",
            "SELECT * FROM source_rows",
            ("input_rows", "prepared_rows"),
            "SELECT 1",
            False,
            consume,
            None,
        )
    borrow.assert_awaited_once_with(session)
    driver.execute.assert_not_awaited()
    consume.assert_not_awaited()
    assert exit_events == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["scope", "task", "owner_oid", "scratch_oid", "missing_registration"])
async def test_row_scratch_cannot_cross_tasks_scopes_or_relation_identities(drift):
    admitted = preparation.admission
    scope = admitted._AdmittedPreparation(object(), "0" * 64, asyncio.Semaphore(1))
    capability = admitted._RowScratchCapability()
    state = admitted._RowScratchState(scope, asyncio.current_task(), {"input_rows": 12})
    scope.owned_oids["input_rows"] = 12
    scope.row_scratch_owners["input_rows"] = (12, capability)
    admitted._ROW_SCRATCH_STATES[capability] = state
    token = admitted._ADMISSION.set(scope)
    try:
        assert admitted._row_scratch_state(capability) is state
        if drift == "scope":
            admitted._ADMISSION.set(None)
        if drift == "task":
            state.task = None
        if drift == "owner_oid":
            scope.owned_oids["input_rows"] = 13
        if drift == "scratch_oid":
            scope.row_scratch_owners["input_rows"] = (13, capability)
        if drift == "missing_registration":
            del scope.row_scratch_owners["input_rows"]
        with pytest.raises(RuntimeError, match="scratch (capability is invalid|identity changed)"):
            admitted._row_scratch_state(capability)
    finally:
        admitted._ADMISSION.reset(token)
        admitted._ROW_SCRATCH_STATES.pop(capability, None)


@pytest.mark.asyncio
async def test_row_scratch_retirement_requires_the_complete_owned_relation_identity():
    admitted = preparation.admission
    scope = admitted._AdmittedPreparation(object(), "0" * 64, asyncio.Semaphore(1))
    capability = admitted._RowScratchCapability()
    state = admitted._RowScratchState(scope, asyncio.current_task())
    admitted._ROW_SCRATCH_STATES[capability] = state
    token = admitted._ADMISSION.set(scope)
    try:
        scope.owned_oids["input_rows"] = 12
        admitted._register_row_scratch(scope, "input_rows", 12, capability, state)
        with pytest.raises(RuntimeError, match="remains owned"):
            admitted.finish_row_scratch(capability)
        with pytest.raises(RuntimeError, match="registration changed"):
            admitted._register_row_scratch(scope, "input_rows", 12, capability, state)
        with pytest.raises(RuntimeError, match="retirement changed"):
            admitted._retire_row_scratch(scope, "input_rows", 13)
        assert state.owned_oids == {"input_rows": 12}
        admitted._retire_row_scratch(scope, "input_rows", 12)
        assert not state.owned_oids and not scope.row_scratch_owners
        admitted.finish_row_scratch(capability)
        with pytest.raises(RuntimeError, match="capability is invalid"):
            admitted._row_scratch_state(capability)
    finally:
        admitted._ADMISSION.reset(token)
        admitted._ROW_SCRATCH_STATES.pop(capability, None)


@pytest.mark.asyncio
async def test_row_scratch_inventory_excludes_other_runners_and_rejects_their_drift():
    admitted = preparation.admission
    scope = admitted._AdmittedPreparation(object(), "0" * 64, asyncio.Semaphore(1))
    capabilities = [admitted._RowScratchCapability(), admitted._RowScratchCapability()]
    scope.owned_oids.update(common=10, first=11, second=12)
    for capability, name in zip(capabilities, ("first", "second"), strict=True):
        state = admitted._RowScratchState(scope, asyncio.current_task(), {name: scope.owned_oids[name]})
        admitted._ROW_SCRATCH_STATES[capability] = state
        scope.row_scratch_owners[name] = (scope.owned_oids[name], capability)
    scope_token = admitted._ADMISSION.set(scope)
    row_token = admitted._ROW_SCRATCH.set(capabilities[0])
    try:
        assert admitted._locked_stage_names(scope) == ["common", "first"]
        scope.owned_oids["second"] = 99
        with pytest.raises(RuntimeError, match="inventory changed"):
            admitted._locked_stage_names(scope)
    finally:
        admitted._ROW_SCRATCH.reset(row_token)
        admitted._ADMISSION.reset(scope_token)
        for capability in capabilities:
            admitted._ROW_SCRATCH_STATES.pop(capability, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("native_failure", [False, True])
async def test_repeated_cancel_keeps_native_worker_admitted_until_drained(monkeypatch, native_failure):
    started = asyncio.Event()
    release = threading.Event()
    finished = threading.Event()
    loop = asyncio.get_running_loop()

    def prepare(_payload):
        loop.call_soon_threadsafe(started.set)
        try:
            if not release.wait(5):
                raise AssertionError("synthetic native worker was not released")
            if native_failure:
                raise ValueError("synthetic native failure")
            return b"prepared", 1
        finally:
            finished.set()

    monkeypatch.setattr(rows, "prepare_entity_address_rows", prepare)
    task = asyncio.create_task(preparation._prepare_bytes(b"input"))
    try:
        await asyncio.wait_for(started.wait(), 5)
        for _ in range(2):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not finished.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["raw", "direct"])
@pytest.mark.parametrize("locale", ["C", "turkish"])
async def test_connected_builders_preserve_alias_origin_arrays_and_contacts(monkeypatch, mode, locale):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with preparation_database(monkeypatch) as (sql, database, schema):
        await apply_source_locale(database, schema, locale)
        source_query = source_select_sql(schema)
        if mode == "raw":
            builder = lambda select, **options: sql._insert_raw_from_source_sql(schema, "raw_actual", select, **options)
            expected = sql._insert_raw_from_source_sql(schema, "raw_expected", source_query)
        else:
            builder = lambda select, **options: sql._materialize_sql(schema, "direct_actual", [select], **options)
            expected = sql._materialize_sql(schema, "direct_expected", [source_query])
        await database.status(expected)
        await sql._run_prepared_source_phase(
            schema,
            source_select_sql(schema, count_reads=True),
            builder,
            {"context": {}, "phase": "preparing synthetic rows"},
            include_base_address_version=mode == "raw",
        )
        assert await database.scalar(f"SELECT last_value FROM {schema}.source_reads") == 4
        expected_rows = await database.all(
            f"SELECT * FROM {schema}.{mode}_expected ORDER BY entity_id,source_record_id"
            if mode == "raw"
            else f"SELECT * FROM {schema}.{mode}_expected ORDER BY location_key"
        )
        actual_rows = await database.all(
            f"SELECT * FROM {schema}.{mode}_actual ORDER BY entity_id,source_record_id"
            if mode == "raw"
            else f"SELECT * FROM {schema}.{mode}_actual ORDER BY location_key"
        )
        assert [dict(result_row._mapping) for result_row in actual_rows] == [
            dict(result_row._mapping) for result_row in expected_rows
        ]
        assert len(actual_rows) == (4 if mode == "raw" else 3)
        assert sum(
            str(result_row._mapping["address_key"]) == "00000000-0000-4000-8000-000000000002"
            for result_row in actual_rows
        ) == (2 if mode == "raw" else 1)
        await assert_no_row_scratch(database, schema)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["native", "consumer", "cancel"])
async def test_source_failure_rolls_back_insert_and_removes_owned_scratch(monkeypatch, failure):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with preparation_database(monkeypatch) as (sql, database, schema):

        async def consume(select_sql, *, native_prepared):
            """Exercise rollback after the target has received prepared rows."""
            await database.status(
                sql._insert_raw_from_source_sql(schema, "raw_actual", select_sql, native_prepared=native_prepared)
            )
            if failure == "cancel":
                raise asyncio.CancelledError
            raise RuntimeError("synthetic consumer failure")

        if failure == "native":

            def fail_native(_payload):
                raise ValueError("synthetic native failure")

            monkeypatch.setattr(rows, "prepare_entity_address_rows", fail_native)
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else (ValueError, RuntimeError)):
            await preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume)
        assert await database.scalar(f"SELECT count(*) FROM {schema}.raw_actual") == 0
        await assert_no_row_scratch(database, schema)


@pytest.mark.asyncio
async def test_oversized_source_row_keeps_sql_semantics_and_other_rows_use_native(monkeypatch):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with preparation_database(monkeypatch) as (sql, database, schema):
        await database.status(
            f"UPDATE {schema}.source_rows SET place_id=:name WHERE entity_id='office-3'",
            name="A" * (rows.MAX_FIELD_BYTES + 1),
        )
        await database.status(sql._insert_raw_from_source_sql(schema, "raw_expected", source_select_sql(schema)))
        copied_counts = []
        native = rows.prepare_entity_address_rows

        def observe(payload):
            result = native(payload)
            copied_counts.append(result[1])
            return result

        monkeypatch.setattr(rows, "prepare_entity_address_rows", observe)

        async def consume(select_sql, *, native_prepared):
            """Use the actual raw builder for native and oversized SQL rows together."""
            return await database.status(
                sql._insert_raw_from_source_sql(schema, "raw_actual", select_sql, native_prepared=native_prepared)
            )

        await preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume)
        assert sum(copied_counts) == 3
        assert not await database.scalar(
            f"SELECT EXISTS((SELECT * FROM {schema}.raw_actual EXCEPT ALL SELECT * FROM {schema}.raw_expected) UNION ALL (SELECT * FROM {schema}.raw_expected EXCEPT ALL SELECT * FROM {schema}.raw_actual))"
        )
        await assert_no_row_scratch(database, schema)


@pytest.mark.asyncio
async def test_admitted_runner_accounts_both_heaps_on_its_single_worker(monkeypatch):
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    async with preparation_database(monkeypatch) as (sql, database, schema):
        inventories = []
        await database.status(f"DROP TABLE {schema}.raw_actual")
        async with preparation_admission(sql, database, schema, monkeypatch, inventories) as (authority, scope):
            await preparation.admission.create_stage_sql(
                schema, "raw_actual", sql._prepare_raw_stage_sql(schema, "raw_actual")
            )

            async def consume(select_sql, *, native_prepared):
                """Keep both intermediate OIDs and target under the current real reservation."""
                assert len(scope.owned_oids) == 3
                return await sql._run_sql_phase(
                    sql._insert_raw_from_source_sql(schema, "raw_actual", select_sql, native_prepared=native_prepared),
                    context={},
                    phase="admitted synthetic rows",
                )

            async with asyncio.timeout(20):
                await preparation.run_prepared_source(sql, schema, source_select_sql(schema), consume)
            assert set(scope.owned_oids) == {"raw_actual"}
            assert authority._external_relations == {(schema, "raw_actual")}
            assert any(len(inventory) == 3 for inventory in inventories)
            assert await database.scalar(f"SELECT count(*) FROM {schema}.raw_actual") == 4
            await assert_no_row_scratch(database, schema)
