# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional queries recover through real savepoints without physical read inventories."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InterfaceError

from api import network_read_execution as execution
from api import provider_profile_snapshot as snapshot
from api import ptg2_serving as serving
from db.connection import ConnectionProxy, Database, SelectAdapter


class _Nested:
    nested = True

    def __init__(self, session):
        self.session, self.parent = session, session.nested
        self.is_active, self.exited = True, False

    async def __aenter__(self):
        self.session.events.append("enter")
        self.session.nested = self
        return self

    async def rollback(self):
        self.session.events.append("rollback")
        if self.session.rollback_error is not None:
            raise self.session.rollback_error
        self.is_active = self.session.aborted = False
        self.session.nested = self.parent
        self.session.last_rollback = self
        if self.session.after_rollback:
            self.session.after_rollback()

    async def __aexit__(self, kind, value, traceback):
        if self.is_active and kind is not None:
            await self.rollback()
        self.is_active, self.exited = False, True
        self.session.nested = self.parent
        self.session.events.append("exit")
        if self.session.exit_error is not None:
            raise self.session.exit_error


class _Driver:
    def __init__(self, session):
        self.session = session

    async def fetchrow(self, sql, *parameters):
        session = self.session
        session.events.append("native query")
        if session.error is not None:
            error, session.error, session.aborted = session.error, None, True
            raise error
        if session.after_query:
            session.after_query()
        return {"count": 0, "bytes": 0, "rows_json": None}


class _Session:
    def __init__(self, error=None):
        self.info, self.transaction, self.nested = {}, SimpleNamespace(is_active=True), None
        self.events = []
        self.executions = []
        self.error, self.aborted, self.last_rollback = error, False, None
        self.rollback_error = self.exit_error = self.after_rollback = self.after_query = None
        self.driver = _Driver(self)
        self.root_rollback, self.root_commit = Mock(), Mock()
        self.result = SimpleNamespace(
            all=Mock(return_value=[{"id": 7}]), first=Mock(return_value={"id": 7}), scalar=Mock(return_value=7)
        )

    def get_transaction(self):
        return self.transaction

    def get_nested_transaction(self):
        return self.nested

    def in_transaction(self):
        return self.transaction.is_active

    def begin_nested(self):
        return _Nested(self)

    async def connection(self):
        async def raw():
            return SimpleNamespace(driver_connection=self.driver)

        return SimpleNamespace(get_raw_connection=raw)

    async def execute(self, statement, parameters):
        self.events.append("query")
        self.executions.append((statement, parameters))
        if self.error is not None:
            error, self.error, self.aborted = self.error, None, True
            raise error
        if self.after_query:
            self.after_query()
        return self.result


async def _query(session, adapter="buffered"):
    if adapter == "native":
        return await execution.bounded_network_read_rows(session, 'SELECT id FROM ONLY "held"."parent" WHERE id=$1', 7)
    return await session.execute(text('SELECT id FROM ONLY "held"."parent" WHERE id=:selected_id'), {"selected_id": 7})


@pytest.mark.parametrize("adapter", ["buffered", "native"])
@pytest.mark.parametrize("wrapped", [False, True])
async def test_mandatory_sql_errors_remain_original(adapter, wrapped):
    error = asyncpg.UndefinedTableError("missing")
    if wrapped:
        error = DBAPIError("private SQL", {}, error)
    session = _Session(error)
    with pytest.raises(type(error)) as caught:
        await _query(session, adapter)
    assert caught.value is error
    assert session.events == ["native query" if adapter == "native" else "query"]


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.DivisionByZeroError("division"),
        asyncpg.QueryCanceledError("timeout"),
        DBAPIError(None, None, asyncpg.DivisionByZeroError("division")),
    ],
)
@pytest.mark.parametrize("adapter", ["buffered", "native"])
async def test_savepoint_preserves_error_and_read(error, adapter):
    session = _Session(error)
    original_cause = error.__cause__
    with pytest.raises(type(error)) as caught:
        async with snapshot.provider_read_savepoint(session):
            await _query(session, adapter)
    assert caught.value is error and error.__cause__ is original_cause
    assert session.events == ["enter", "native query" if adapter == "native" else "query", "rollback", "exit"]
    assert session.get_transaction().is_active and session.nested is None and not session.aborted
    assert (await _query(session)).all() == [{"id": 7}]
    session.root_rollback.assert_not_called()
    session.root_commit.assert_not_called()


@pytest.mark.parametrize("adapter", ["buffered", "native"])
async def test_optional_fallback_follows_savepoint_exit(adapter):
    error = asyncpg.UndefinedTableError("optional table missing")
    session, logger = _Session(error), SimpleNamespace(debug=Mock())
    async with snapshot.provider_optional_read(session, logger):
        await _query(session, adapter)
    assert session.events == ["enter", "native query" if adapter == "native" else "query", "rollback", "exit"]
    logger.debug.assert_called_once_with("Optional provider read failed: %s", error)
    assert (await _query(session)).all() == [{"id": 7}]


@pytest.mark.parametrize("adapter", ["database", "statement", "proxy"])
@pytest.mark.parametrize("method", ["all", "first", "scalar"])
async def test_adapters_preserve_parameters_and_results(adapter, method):
    session, database = _Session(), Database()

    @asynccontextmanager
    async def configured_session():
        yield session

    database._execution_session = configured_session
    statement = text('SELECT id FROM ONLY "held"."parent" WHERE id=:selected_id')
    reader = {
        "database": database,
        "statement": SelectAdapter(database, statement),
        "proxy": ConnectionProxy(database, session, None),
    }[adapter]
    arguments = () if adapter == "statement" else (statement,)
    async with snapshot.provider_read_savepoint(session):
        result = await getattr(reader, method)(*arguments, selected_id=7)
    assert result == {"all": [{"id": 7}], "first": {"id": 7}, "scalar": 7}[method]
    assert session.executions == [(statement, {"selected_id": 7})]
    assert session.events == ["enter", "query", "exit"]


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.InterfaceError("interface"),
        ConnectionError("connection lost"),
        asyncpg.ConnectionDoesNotExistError("disconnect"),
        InterfaceError(None, None, asyncpg.InterfaceError("interface")),
        DBAPIError(None, None, asyncpg.InterfaceError("wrapped interface")),
        DBAPIError(None, None, asyncpg.DivisionByZeroError("invalidated"), connection_invalidated=True),
        ServiceUnavailable("private refusal"),
    ],
)
async def test_nonrecoverable_errors_prevent_optional_fallback(error):
    session, logger = _Session(error), SimpleNamespace(debug=Mock())
    with pytest.raises(ServiceUnavailable) as caught:
        async with snapshot.provider_optional_read(session, logger):
            await _query(session)
    assert caught.value is error if isinstance(error, ServiceUnavailable) else caught.value.__cause__ is error
    logger.debug.assert_not_called()
    assert session.events == ["enter", "query", "rollback", "exit"]


@pytest.mark.parametrize("failure", ["rollback", "context exit"])
async def test_cleanup_errors_prevent_optional_fallback(failure):
    session, logger = _Session(asyncpg.DivisionByZeroError("division")), SimpleNamespace(debug=Mock())
    cleanup_error = RuntimeError("cleanup failed")
    setattr(session, "rollback_error" if failure == "rollback" else "exit_error", cleanup_error)
    with pytest.raises(ServiceUnavailable) as caught:
        async with snapshot.provider_optional_read(session, logger):
            await _query(session)
    assert caught.value.__cause__ is cleanup_error
    logger.debug.assert_not_called()


@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_cancellation_survives_cleanup(cleanup_fails):
    cancellation = asyncio.CancelledError("cancelled")
    session = _Session(cancellation)
    if cleanup_fails:
        session.rollback_error = RuntimeError("cleanup failed")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with snapshot.provider_optional_read(session, SimpleNamespace(debug=Mock())):
            await _query(session)
    assert caught.value is cancellation
    if cleanup_fails:
        assert "Optional savepoint cleanup failed: RuntimeError" in cancellation.__notes__


@pytest.mark.parametrize("consumer", ["columns", "directory", "procedure"])
@pytest.mark.parametrize("private", [False, True])
async def test_ptg_callers_preserve_error_modes(consumer, private):
    error = ServiceUnavailable("private refusal") if private else asyncpg.UndefinedTableError("optional missing")
    session = _Session(error)

    async def query():
        if consumer == "columns":
            return await serving._ptg2_table_columns(session, "mrf.npi")
        if consumer == "directory":
            return await serving._provider_directory_corroboration_by_key(
                session, "mrf.directory", [(1000000001, "synthetic")], plan_id=None, snapshot_id=None, source_key=None
            )
        return await serving._procedure_details_for_rows(
            session, [{"reported_code_system": "CPT", "reported_code": "00100"}]
        )

    if private:
        with pytest.raises(ServiceUnavailable) as caught:
            await query()
        assert caught.value is error
    else:
        assert await query() == {"columns": frozenset(), "directory": None, "procedure": {}}[consumer]
    assert session.events == ["enter", "query", "rollback", "exit"]


async def test_ordinary_errors_keep_optional_fallback():
    error = RuntimeError("optional")
    session = _Session(error)
    with pytest.raises(RuntimeError) as caught:
        await _query(session)
    assert caught.value is error
    session.error, session.aborted = error, False
    logger = SimpleNamespace(debug=Mock())
    async with snapshot.provider_optional_read(session, logger):
        await _query(session)
    logger.debug.assert_called_once_with("Optional provider read failed: %s", error)


@pytest.mark.parametrize("verified_reader", [False, True])
async def test_procedure_catalog_keeps_bound_columns(monkeypatch, verified_reader):
    from tests.test_pricing_api import FakeResult

    session = _Session()
    session.info["api_reader_verified"] = verified_reader
    session.result = FakeResult(
        rows=[{"code_system": "CPT", "code": "00100", "display_name": "Synthetic", "short_description": "Detail"}]
    )
    monkeypatch.setattr(serving, "PTG2_SCHEMA", "mrf")
    assert await serving._procedure_details_for_rows(
        session, [{"reported_code_system": "CPT", "reported_code": "00100"}]
    ) == {("CPT", "00100"): {"procedure_name": "Synthetic", "procedure_description": "Detail"}}
    statement, parameters = session.executions[-1]
    assert ('FROM "mrf"."code_catalog"' if verified_reader else "FROM mrf.code_catalog") in str(statement)
    assert parameters == {"code_system_0": "CPT", "code_0": "00100"}
