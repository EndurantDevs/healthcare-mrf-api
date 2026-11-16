# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded native staging preserves fallback, source semantics and rollback."""

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from process.ext import address_batch_stage as stage


class _Database:
    def __init__(self, create_error=None):
        self.create_error = create_error
        self.statements = []
        self.transactions = []
        self.all = AsyncMock(return_value=[])

    @asynccontextmanager
    async def transaction(self):
        try:
            yield
        except BaseException:
            self.transactions.append("rollback")
            raise
        else:
            self.transactions.append("commit")

    async def status(self, statement, **parameters):
        self.statements.append((statement, parameters))
        if self.create_error is not None and statement.startswith("CREATE TEMP TABLE"):
            raise self.create_error
        return 1


def _skip_population(monkeypatch):
    monkeypatch.setattr(stage.address_batch, "is_native_available", lambda: True)
    monkeypatch.setattr(stage, "_has_supported_case", AsyncMock(return_value=True))
    monkeypatch.setattr(stage, "_insert_inputs", AsyncMock())
    monkeypatch.setattr(stage, "_fill_native_pages", AsyncMock())


def test_projection_and_key_preserve_all_six_fields():
    expressions = ["b." + field for field in stage.ADDRESS_FIELDS]
    projection = stage.input_projection_sql(expressions)
    lookup = stage.lookup_key_sql(expressions)
    assert all(f"(b.{field})::text AS {field}" in projection for field in stage.ADDRESS_FIELDS)
    assert lookup == "jsonb_build_array(" + ",".join(f"({field})::text" for field in expressions) + ")::text"
    with pytest.raises(ValueError, match="six input"):
        stage.lookup_key_sql(expressions[:-1])
    with pytest.raises(ValueError, match="six input"):
        stage.input_projection_sql(expressions[:-1])


def test_native_null_remains_distinct_from_sql_fallback():
    sql = stage.field_sql("native", "address_key", "original_key()")
    assert sql == (
        'CASE WHEN "native".native_fields IS NOT NULL THEN "native".native_fields->>\'address_key\' '
        "ELSE original_key() END"
    )
    with pytest.raises(ValueError, match="field differs"):
        stage.field_sql("native", "unknown", "original_key()")
    with pytest.raises(ValueError, match="identifier_invalid"):
        stage.field_sql("unsafe alias", "address_key", "original_key()")


@pytest.mark.asyncio
@pytest.mark.parametrize("result,expected", [([], True), ([(True,) * 12], True), ([(True,) * 11 + (False,)], False)])
async def test_actual_case_probe_compares_bytes(result, expected):
    database = _Database()
    database.all.return_value = result
    assert await stage._has_supported_case(database, "SELECT inputs", {"selected": 1}) is expected
    invocation = database.all.await_args
    assert invocation.args[0].count("convert_to(lower(") == 6
    assert invocation.args[0].count("convert_to(upper(") == 6
    assert "lc_ctype" not in invocation.args[0]
    assert invocation.kwargs == {"selected": 1}


@pytest.mark.asyncio
async def test_missing_native_or_case_support_creates_no_stage(monkeypatch):
    database = _Database()
    monkeypatch.setattr(stage.address_batch, "is_native_available", lambda: False)
    async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs") as table:
        assert table is None
    monkeypatch.setattr(stage.address_batch, "is_native_available", lambda: True)
    monkeypatch.setattr(stage, "_has_supported_case", AsyncMock(return_value=False))
    async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs") as table:
        assert table is None
    assert database.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize("temporary", [False, True])
async def test_success_retires_exact_stage(monkeypatch, temporary):
    _skip_population(monkeypatch)
    database = _Database()
    schema = "pg_temp" if temporary else "data"
    async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs;", temporary=temporary) as table:
        assert table == f'"{schema}"."stage"'
    statements = [sql for sql, _parameters in database.statements]
    assert statements[0].startswith("CREATE TEMP TABLE" if temporary else "CREATE UNLOGGED TABLE")
    assert ("ON COMMIT DROP" in statements[0]) is temporary
    assert f'CREATE INDEX ON "{schema}"."stage" USING hash(lookup_key)' in statements
    assert statements[-1] == f'DROP TABLE "{schema}"."stage"'
    assert database.transactions == ["commit", "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_committed_stage_is_visible_before_consumer_and_cleanup(monkeypatch, cancelled):
    _skip_population(monkeypatch)
    database = _Database()

    async def consume():
        async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs", committed=True) as table:
            assert table == '"data"."stage"'
            assert database.transactions == ["commit", "commit"]
            if cancelled:
                raise asyncio.CancelledError()

    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await consume()
    else:
        await consume()
    assert database.statements[-1][0] == 'DROP TABLE IF EXISTS "data"."stage"'
    assert database.transactions == ["commit", "commit", "commit"]


@pytest.mark.asyncio
async def test_committed_stage_refuses_temporary_or_nested_publication(monkeypatch):
    _skip_population(monkeypatch)
    database = _Database()
    with pytest.raises(ValueError, match="cannot be temporary"):
        async with stage.staged_address_fields(
            database, "data", "stage", "SELECT inputs", temporary=True, committed=True
        ):
            pytest.fail("temporary committed stage yielded")
    database._transaction_binding = lambda: object()
    async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs", committed=True) as table:
        assert table is None
    database._transaction_binding = lambda: None
    database._reader_binding = lambda: object()
    async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs", committed=True) as table:
        assert table is None
    assert database.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_consumer_failure_uses_transaction_rollback(monkeypatch, failure):
    _skip_population(monkeypatch)
    database = _Database()
    with pytest.raises(failure):
        async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs"):
            raise failure()
    assert database.transactions == ["commit", "rollback"]
    assert not any(sql.startswith("DROP TABLE") for sql, _parameters in database.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize("state,expected_fallback", [("42501", True), ("53200", False)])
async def test_temp_permission_denial_is_the_only_ddl_fallback(monkeypatch, state, expected_fallback):
    _skip_population(monkeypatch)
    failure = RuntimeError("create failed")
    failure.sqlstate = state
    database = _Database(failure)
    if expected_fallback:
        async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs", temporary=True) as table:
            assert table is None
        assert database.transactions == ["rollback", "commit"]
    else:
        with pytest.raises(RuntimeError, match="create failed"):
            async with stage.staged_address_fields(database, "data", "stage", "SELECT inputs", temporary=True):
                pytest.fail("failed stage was yielded")


def test_nested_permission_error_is_recognized():
    failure = RuntimeError("driver failure")
    failure.pgcode = "42501"
    wrapper = RuntimeError("wrapped")
    wrapper.orig = failure
    assert stage._is_permission_denied(wrapper)
    assert not stage._is_permission_denied(RuntimeError("other failure"))


def test_page_budget_stops_before_oversized_values(monkeypatch):
    monkeypatch.setattr(stage.address_batch, "MAX_INPUT_BYTES", 8)
    assert stage._page_end([(1, 4, True), (2, 4, True), (3, 1, True)]) == 2
    assert stage._page_end([(1, 4, True), (2, 4, False)]) == 1
    assert stage._page_end([(1, 4, False)]) is None


@pytest.mark.asyncio
async def test_oversized_tuple_is_not_fetched_and_later_pages_continue(monkeypatch):
    database = _Database()
    database.all.side_effect = [[(1, 100, False)], [(2, 4, True), (3, 4, True)], []]
    write = AsyncMock()
    cancel = AsyncMock()
    monkeypatch.setattr(stage, "_write_native_page", write)
    await stage._fill_native_pages(database, '"data"."stage"', cancel)
    write.assert_awaited_once_with(database, '"data"."stage"', 1, 3)
    assert [call.kwargs["after"] for call in database.all.await_args_list] == [0, 1, 3]
    assert cancel.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [None, [None, None], [{"address_key": None}, None]])
async def test_only_accepted_native_results_are_stored(monkeypatch, prepared):
    database = _Database()
    database.all.return_value = [
        (1, "10 Main", None, "City", "MA", "02101", "US"),
        (2, None, None, None, None, None, None),
    ]
    monkeypatch.setattr(stage.address_batch, "prepare_address_batch", lambda rows: prepared)
    await stage._write_native_page(database, '"data"."stage"', 0, 2)
    if prepared is None or prepared == [None, None]:
        assert database.statements == []
    else:
        assert database.statements[0][1]["row_ids"] == [1]
        assert json.loads(database.statements[0][1]["native_fields"][0]) == {"address_key": None}
