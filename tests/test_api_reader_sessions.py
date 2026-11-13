# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit login and task ownership boundaries for API read sessions."""

import asyncio
import os
import re
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database, current_session, gather_reader_calls, run_as_writer
from tests.test_db_connection import _FakeResult, _FakeSession


def _profile_fixture_database(monkeypatch, *, password_failure=False, role_exists=False):
    """Record only non-secret lifecycle evidence for the native profile fixture."""
    state_by_field = {"role_exists": role_exists, "calls": []}
    reader = SimpleNamespace(_reader_login=None, disconnect=AsyncMock())

    async def scalar(statement, params):
        if "to_regclass" in str(statement):
            return int(params["table"].endswith('."cms_doctor_education"'))
        return state_by_field["role_exists"]

    async def execute(statement, _params=None):
        sql = str(statement)
        if "PASSWORD" in sql:
            if password_failure:
                raise ValueError("synthetic provisioning failure")
            return None
        state_by_field["calls"].append(sql)
        if sql.startswith("CREATE ROLE"):
            assert state_by_field["role_exists"] is False
            state_by_field["role_exists"] = True
        elif sql.startswith("DROP ROLE"):
            state_by_field["role_exists"] = False
        return SimpleNamespace(scalars=lambda: iter(["renamed_education"]))

    @asynccontextmanager
    async def begin():
        yield SimpleNamespace(scalar=scalar, execute=execute)

    engine = SimpleNamespace(begin=begin, url=SimpleNamespace(host="localhost", port=5432, database="synthetic_test"))
    database = Database(engine=engine)

    async def connect_reader():
        reader._reader_login = (os.getenv("HLTHPRT_DB_READER_USER"), "")
        database._reader_database = reader
        return reader

    monkeypatch.setattr(database, "_connect_reader", connect_reader)
    return database, reader, state_by_field


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_profile_native_fixture_closes_reader_and_retains_writer(monkeypatch, failure):
    from tests.provider_profile_snapshot_postgres_support import profile_reader

    database, reader, state_by_field = _profile_fixture_database(monkeypatch)
    writer_engine = database.engine

    async def use_fixture():
        async with profile_reader(database, "synthetic", monkeypatch):
            assert database.engine is writer_engine and database._reader_database is reader
            if failure:
                raise failure("synthetic consumer failure")

    if failure:
        with pytest.raises(failure, match="synthetic consumer failure"):
            await use_fixture()
    else:
        await use_fixture()
    reader.disconnect.assert_awaited_once()
    assert database.engine is writer_engine and database._reader_database is None
    assert state_by_field["role_exists"] is False
    assert (
        sum(sql.startswith('GRANT SELECT ON "synthetic"."cms_doctor_education"') for sql in state_by_field["calls"])
        == 1
    )
    assert any(sql.startswith('REVOKE SELECT ON "synthetic"."renamed_education"') for sql in state_by_field["calls"])
    assert not any("DEFAULT PRIVILEGES" in sql or "ALL TABLES" in sql for sql in state_by_field["calls"])


async def test_profile_native_fixture_sanitizes_password_setup_failure(monkeypatch):
    from tests.provider_profile_snapshot_postgres_support import profile_reader

    database, reader, state_by_field = _profile_fixture_database(monkeypatch, password_failure=True)
    with pytest.raises(RuntimeError, match="^Reader fixture login provisioning failed$") as caught:
        async with profile_reader(database, "synthetic", monkeypatch):
            pytest.fail("Failed provisioning cannot reach a Reader")
    assert caught.value.__suppress_context__ is True
    assert not state_by_field["role_exists"] and database._reader_database is None
    reader.disconnect.assert_not_awaited()


async def test_profile_native_fixture_preserves_existing_role(monkeypatch):
    from tests.provider_profile_snapshot_postgres_support import profile_reader

    database, reader, state_by_field = _profile_fixture_database(monkeypatch, role_exists=True)
    with pytest.raises(AssertionError):
        async with profile_reader(database, "synthetic", monkeypatch):
            pytest.fail("An existing role cannot be adopted")
    assert state_by_field["role_exists"] and not state_by_field["calls"]
    reader.disconnect.assert_not_awaited()


def test_profile_reader_grants_cover_receipt_function_dependencies(monkeypatch):
    """Cover the snapshot reader closure without granting publisher-only ledger access."""
    from tests.provider_profile_snapshot_postgres_support import READ_TABLES
    from tests.test_provider_directory_cms_serving_receipt_postgres import _migration

    migration = _migration("20260930100000")
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    for render in (migration._create_native_readers, migration._create_snapshot_check):
        render('"synthetic"')
    required_tables = set(re.findall(r'\b(?:FROM|JOIN)\s+"synthetic"\.(\w+)', "\n".join(statements)))
    assert "address_alias_state_v1" in required_tables
    assert required_tables <= set(READ_TABLES)
    assert "cms_native_input_revision" not in READ_TABLES
    statements.clear()
    migration._create_archive_check('"synthetic"')
    assert 'FROM "synthetic".cms_native_input_revision' in "\n".join(statements)


class ReaderSession(_FakeSession):
    """Model configured transaction modes without a PostgreSQL connection."""

    def __init__(self):
        super().__init__()
        self.info = {}
        self.modes = ("repeatable read", "on")

    async def execute(self, statement, params=None):
        self._in_tx = True
        if (
            str(statement)
            == "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')"
        ):
            self.executed.append((statement, params))
            return _FakeResult(self.modes)
        return await super().execute(statement, params)

    async def connection(self):
        self._in_tx = True
        return self

    async def commit(self):
        await super().commit()
        self._in_tx = False

    async def rollback(self):
        await super().rollback()
        self._in_tx = False


def reader_database(monkeypatch, sessions):
    """Keep Reader and Writer factories distinct in every routing assertion."""
    writer = Database(session_factory=AsyncMock(side_effect=AssertionError("Writer used")))
    reader = Database(session_factory=lambda: sessions.pop(0))
    reader._reader_login = ("reader_test", "")
    monkeypatch.setattr(writer, "_connect_reader", AsyncMock(return_value=reader))
    return writer


@pytest.mark.parametrize(
    ("driver", "connection_options"),
    [
        ("asyncpg", {"server_settings": {"jit": "off"}}),
        ("postgresql+asyncpg", {"server_settings": {"jit": "off"}}),
        ("psycopg", {"options": "-c jit=off"}),
        ("postgresql+psycopg", {"options": "-c jit=off"}),
    ],
)
async def test_reader_pool_uses_separate_login_and_read_only_transactions(monkeypatch, driver, connection_options):
    engine_by_field = {}
    monkeypatch.delenv("PGOPTIONS", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", driver)
    monkeypatch.setenv("HLTHPRT_DB_USER", "writer_test")
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "writer_value")
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", "reader_test")
    monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", "reader_value")
    monkeypatch.setenv("HLTHPRT_DB_READER_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("HLTHPRT_DB_READER_POOL_MAX_SIZE", "3")

    def engine(url, **options):
        engine_by_field.update(url=url, options=options)
        return SimpleNamespace(dispose=AsyncMock())

    monkeypatch.setattr("db.connection.create_async_engine", engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_args, **_kwargs: object())
    writer = Database()
    reader = await writer._connect_reader()
    assert reader is not writer and writer.engine is None
    assert engine_by_field["url"].username == "reader_test"
    assert engine_by_field["url"].password == "reader_value"
    assert engine_by_field["options"]["isolation_level"] == "REPEATABLE READ"
    assert engine_by_field["options"]["execution_options"] == {"postgresql_readonly": True}
    assert engine_by_field["options"]["connect_args"] == connection_options
    assert engine_by_field["options"]["pool_size"] == 2
    assert engine_by_field["options"]["max_overflow"] == 1
    await writer.disconnect()
    assert reader.engine is None
    assert writer._reader_database is None


async def test_psycopg_reader_preserves_existing_server_options(monkeypatch):
    engine_by_option = {}
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "psycopg")
    monkeypatch.setenv("PGOPTIONS", "-c statement_timeout=3000 -c timezone=UTC")

    def engine(_url, **options):
        engine_by_option.update(options)
        return SimpleNamespace(dispose=AsyncMock())

    monkeypatch.setattr("db.connection.create_async_engine", engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", lambda *_args, **_kwargs: object())
    reader = Database()
    reader._reader_login = ("reader_test", "")
    await reader.connect()
    assert engine_by_option["connect_args"] == {"options": "-c statement_timeout=3000 -c timezone=UTC -c jit=off"}
    await reader.disconnect()


@pytest.mark.parametrize("user", ["", "writer_test"])
async def test_reader_configuration_never_falls_back_to_writer(monkeypatch, user):
    monkeypatch.setenv("HLTHPRT_DB_USER", "writer_test")
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", user)
    writer = Database()
    with pytest.raises(RuntimeError, match="distinct Reader"):
        await writer._connect_reader()
    assert writer.engine is None and writer.session_factory is None


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/npi",
        "/api/v1/npi/",
        "/api/v1/npi/id/batch",
        "/api/v1/codes",
        "/control/v1/plan-pricing/code-projection",
        "/control/v1/plan-pricing/code-projection/",
    ],
)
def test_reader_path_includes_root_and_post_read_routes(path):
    assert Database._is_pinned_read_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "/control/imports",
        "/control/v1/imports",
        "/control/v1/plan-pricing/code-projection-extra",
        "/control/v1/plan-pricing/code-projection/nested",
        "/api/v1/npi-extra",
        "/account",
        "/api/v1/geo/",
        "/api/v1/npi/id/1234567890",
        "/api/v1/npi/id/1234567890/",
    ],
)
def test_reader_path_preserves_other_routes(path):
    assert not Database._is_pinned_read_path(path)


async def test_reader_helpers_reuse_exact_session_and_preserve_owner(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    async with database.reader_session() as acquired:
        assert acquired is session and current_session() is session
        async with database.session() as same:
            assert same is session
        async with database.acquire() as connection:
            assert connection._connection is session
            await connection.scalar("SELECT 1")
        await database.scalar("SELECT 2")
        async with database.transaction() as nested:
            assert nested is session
        async with database.reader_session() as same:
            assert same is session
        assert not session.closed and not session.committed
    assert session.committed and session.closed
    assert session.nested_transactions == 1
    assert not database.has_reader_session()


async def test_native_pin_failure_closes_reader_without_writer(monkeypatch):
    from sanic.exceptions import ServiceUnavailable

    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
    pin = AsyncMock(side_effect=RuntimeError("catalog changed"))
    monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", pin)
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    callbacks_by_kind = {}

    class App:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, name):
            def register(callback):
                callbacks_by_kind[name] = callback
                return callback

            return register

    database.init_app(App())
    request = SimpleNamespace(path="/api/v1/npi/id/batch", ctx=SimpleNamespace())
    with pytest.raises(ServiceUnavailable, match="temporarily unavailable"):
        await callbacks_by_kind["request"](request)
    assert pin.await_args.args == (session,)
    assert session.closed and session.rolled_back and not session.committed
    assert request.ctx.session is None and not database.has_reader_session()


@pytest.mark.parametrize("path", ["/control/imports", "/api/v1/geo/"])
async def test_reader_rollout_does_not_replace_other_writer_routes(monkeypatch, path):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
    writer = _FakeSession()
    database = Database(session_factory=lambda: writer)
    connect_reader = AsyncMock(side_effect=AssertionError("Reader used"))
    monkeypatch.setattr(database, "_connect_reader", connect_reader)
    callbacks_by_kind = {}

    class App:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, name):
            def register(callback):
                callbacks_by_kind[name] = callback
                return callback

            return register

    database.init_app(App())
    request = SimpleNamespace(path=path, ctx=SimpleNamespace())
    await callbacks_by_kind["request"](request)
    assert request.ctx.session is writer and not database.has_reader_session()
    await callbacks_by_kind["response"](request, SimpleNamespace(status=200))
    connect_reader.assert_not_awaited()
    assert writer.closed


async def test_npi_count_caller_keeps_both_queries_on_reader_owner(monkeypatch):
    from api.endpoint import npi

    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(npi, "db", database)
    async with database.reader_session():
        assert await npi._compute_npi_counts() == [42, 42]
    assert len(session.executed) == 2


async def test_ptg_network_caller_opens_independent_genuine_reader(monkeypatch):
    from api import ptg2_serving

    parent, child = ReaderSession(), ReaderSession()
    database = reader_database(monkeypatch, [parent, child])
    monkeypatch.setattr(ptg2_serving, "sa_db", database)

    async def search(session, *_args, **_kwargs):
        assert session is child and current_session() is child
        await database.scalar("SELECT 1")
        return {"items": []}

    monkeypatch.setattr(ptg2_serving, "_search_ptg2_provider_procedures_snapshot", search)
    async with database.reader_session():
        result = await asyncio.create_task(
            ptg2_serving._search_provider_procedures_network("synthetic", "snapshot", 1234567890, {}, None)
        )
        assert result == ("synthetic", "snapshot", {"items": []})
        assert current_session() is parent and not parent.closed
    assert parent.closed and child.closed
    with pytest.raises(RuntimeError, match="No SQLAlchemy"):
        current_session()


async def test_child_readers_are_separate_and_inherited_helpers_refuse(monkeypatch):
    parent, child = ReaderSession(), ReaderSession()
    database = reader_database(monkeypatch, [parent, child])

    async def child_read():
        with pytest.raises(RuntimeError, match="separate child Reader"):
            current_session()
        with pytest.raises(RuntimeError, match="separate child Reader"):
            await database.scalar("SELECT 1")
        async with database.reader_session(independent=True) as session:
            assert session is child and current_session() is child
            await database.scalar("SELECT 2")

    async with database.reader_session():
        await asyncio.create_task(child_read())
        assert current_session() is parent and not parent.closed
    assert parent.closed and child.closed


@pytest.mark.parametrize("failure", [RuntimeError("failed"), asyncio.CancelledError()])
async def test_reader_failure_and_cancellation_close_without_writer(monkeypatch, failure):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    with pytest.raises(type(failure)):
        async with database.reader_session():
            raise failure
    assert session.closed and session.rolled_back and not session.committed
    assert not database.has_reader_session()


async def test_reader_starts_configured_transaction_without_setup_queries(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    async with database.reader_session() as acquired:
        assert acquired is session and session.in_transaction()
        assert session.info == {"api_reader_verified": True}
        assert not session.executed
    assert session.closed and session.committed and not session.info


@pytest.mark.parametrize("failure", [RuntimeError("connection failed"), asyncio.CancelledError()])
async def test_reader_connection_failure_closes_and_clears_binding(monkeypatch, failure):
    session = ReaderSession()
    session.connection = AsyncMock(side_effect=failure)
    database = reader_database(monkeypatch, [session])
    with pytest.raises(type(failure)) as caught:
        async with database.reader_session():
            pytest.fail("failed connection yielded a Reader")
    assert caught.value is failure
    assert not session.executed and session.closed and not session.info
    assert not database.has_reader_session()


async def test_explicit_background_writer_preserves_parent_reader(monkeypatch):
    parent, writer = ReaderSession(), _FakeSession()
    database = reader_database(monkeypatch, [parent])
    database.session_factory = lambda: writer

    async def update():
        assert not database.has_reader_session()
        with pytest.raises(RuntimeError, match="No SQLAlchemy"):
            current_session()
        await database.status("UPDATE example SET value=1")

    async with database.reader_session():
        await asyncio.create_task(run_as_writer(update))
        assert current_session() is parent and not parent.closed
    assert writer.closed and len(writer.executed) == 1


def test_unstarted_background_task_does_not_create_inner_coroutine():
    operation = AsyncMock()
    task = run_as_writer(operation)
    task.close()
    operation.assert_not_called()


async def test_background_writer_preserves_arguments_bound_at_dispatch():
    operation = AsyncMock(return_value="updated")
    assert await run_as_writer(operation, "address", 1, source="synthetic") == "updated"
    operation.assert_awaited_once_with("address", 1, source="synthetic")


async def test_pinned_reader_fanout_closes_unstarted_reads_on_failure(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    later_read = AsyncMock()

    async def first_read():
        raise RuntimeError("original failure")

    unopened = later_read()
    async with database.reader_session():
        with pytest.raises(RuntimeError, match="original failure"):
            await gather_reader_calls(database, first_read(), unopened)
    later_read.assert_not_awaited()
    assert unopened.cr_frame is None


async def test_reader_refuses_writer_driver_and_ddl(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    async with database.reader_session():
        with pytest.raises(RuntimeError, match="Writer driver"):
            async with database.acquire_driver():
                pytest.fail("Reader acquired Writer driver")
        with pytest.raises(RuntimeError, match="DDL"):
            await database.execute_ddl("CREATE TABLE example(value int)")
        with pytest.raises(RuntimeError, match="create tables"):
            await database.create_table(None)


async def test_pricing_statistics_queries_stay_on_reader_owner(monkeypatch):
    from api.endpoint import pricing

    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    monkeypatch.setattr(pricing, "db", database)
    owner = asyncio.current_task()
    execute = session.execute

    async def same_task_execute(statement, params=None):
        assert asyncio.current_task() is owner
        return await execute(statement, params)

    monkeypatch.setattr(session, "execute", same_task_execute)
    async with database.reader_session():
        request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))
        response = await pricing.pricing_statistics(request)
        assert response.status == 200
        assert len(session.executed) == 4


def _optional_count_read(session, events, failure):
    """Emulate count failures while checking savepoint and page ordering."""
    from sqlalchemy.exc import DBAPIError, SQLAlchemyError

    from api.endpoint import npi

    async def read(sql, **_params):
        assert current_session() is session
        if "page_npis AS" not in str(sql) and "count(" in str(sql).lower():
            assert events == ["savepoint"]
            session.count_reads += 1
            if failure == "cancellation":
                raise asyncio.CancelledError
            if failure == "unavailable":
                from sanic.exceptions import ServiceUnavailable

                raise ServiceUnavailable("synthetic reader unavailable")
            if failure == "statement_timeout":
                error = RuntimeError("query cancelled")
                error.sqlstate = "57014"
                raise DBAPIError(None, None, error)
            if failure is None:
                return [(12,)]
            raise SQLAlchemyError("optional count failed")
        if failure is None:
            assert events in (["savepoint"], ["savepoint", "nested rollback"])
        else:
            assert events == ["savepoint", "nested rollback"]
        assert npi._NPI_COUNT_READ_SCOPE.get() is None
        assert session.in_transaction() and not session.rolled_back
        events.append("page")
        return []

    return read


def _bind_optional_count_queries(monkeypatch, session, connection):
    execute = session.execute

    async def execute_query(statement, parameters=None):
        if str(statement).startswith(("SELECT session_user, current_user", "SELECT current_setting(")):
            return await execute(statement, parameters)
        query_rows = await connection.all(statement, **(parameters or {}))
        return SimpleNamespace(all=lambda: query_rows)

    monkeypatch.setattr(session, "execute", execute_query)


def _optional_count_reader(monkeypatch, failure, *, unfiltered=False):
    """Bind the real provider caller to a Reader and record its savepoint boundary."""
    from api import provider_list_sql
    from api.endpoint import npi
    from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection

    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    connection = RecordingConnection()
    events = []
    monkeypatch.setattr(npi, "db", database)
    monkeypatch.setattr(database, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(npi, "_NPI_ALL_TOTAL_TIMEOUT_SECONDS", 0.1)

    @asynccontextmanager
    async def native_timeout(owner, *, timeout_ms):
        assert owner is session and 0 < timeout_ms <= 100
        yield

    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", native_timeout)
    session.count_reads = 0
    if unfiltered:
        monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.NPIAddress))
        monkeypatch.setattr(npi, "_npi_count_cache_identity", AsyncMock(return_value="synthetic-publication"))
        monkeypatch.setattr(npi, "_NPI_PRIMARY_TOTAL_CACHE_STATE", {"entry": None})
        monkeypatch.setattr(npi, "ENABLE_NPI_SCHEMA_CACHE", True)
        monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=AssertionError("unbounded count")))

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield session
        except BaseException:
            events.append("nested rollback")
            raise

    monkeypatch.setattr(session, "begin_nested", savepoint)
    monkeypatch.setattr(connection, "all", _optional_count_read(session, events, failure))
    _bind_optional_count_queries(monkeypatch, session, connection)
    return database, session, events


@pytest.mark.parametrize("failure", ["sql", "statement_timeout", "cancellation"])
async def test_npi_optional_count_recovers_savepoint_before_reader_page(monkeypatch, failure):
    """SQL failure degrades after rollback; external cancellation never reaches the page."""
    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, failure)
    request = SimpleNamespace(
        args={"classification": "Pharmacy", "city": "Synthetic", "include_total": "1"},
        ctx=SimpleNamespace(sa_session=session),
    )
    if failure == "cancellation":
        with pytest.raises(asyncio.CancelledError):
            async with database.reader_session():
                await npi.get_all(request)
        assert events == ["savepoint", "nested rollback"]
        assert session.closed and session.rolled_back and not database.has_reader_session()
    else:
        async with database.reader_session():
            response = await npi.get_all(request)
            assert response.status == 200 and events == ["savepoint", "nested rollback", "page"]
            assert current_session() is session and not session.rolled_back


@pytest.mark.parametrize("explicit_total", [None, "1"])
@pytest.mark.parametrize("failure", [None, "sql", "statement_timeout", "cancellation", "unavailable"])
async def test_npi_unfiltered_count_uses_reader_budget_and_recovers_page(monkeypatch, failure, explicit_total):
    import json

    from sanic.exceptions import ServiceUnavailable

    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, failure, unfiltered=True)
    query_by_name = {"limit": "1"}
    if explicit_total is not None:
        query_by_name["include_total"] = explicit_total
    request = SimpleNamespace(args=query_by_name, ctx=SimpleNamespace(sa_session=session))
    error = asyncio.CancelledError if failure == "cancellation" else ServiceUnavailable
    if failure in {"cancellation", "unavailable"}:
        with pytest.raises(error):
            async with database.reader_session():
                await npi.get_all(request)
        assert events == ["savepoint", "nested rollback"]
    else:
        async with database.reader_session():
            response = await npi.get_all(request)
            body = json.loads(response.body)
            assert response.status == 200
            assert body["total_source"] == ("computed" if failure is None else "estimated_timeout_floor")
            assert body["total"] == (12 if failure is None else 0)
            assert events[-1] == "page" and not session.rolled_back
    assert session.count_reads == 1
    database.scalar.assert_not_awaited()
    assert (npi._NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] is not None) is (failure is None)
    assert npi._NPI_COUNT_READ_SCOPE.get() is None


@pytest.mark.parametrize("failure", [None, "statement_timeout", "cancellation", "unavailable"])
@pytest.mark.parametrize("filtered", [False, True])
async def test_npi_count_only_is_exact_or_bounded(monkeypatch, failure, filtered):
    import json

    from sanic.exceptions import ServiceUnavailable

    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, failure, unfiltered=not filtered)
    query_by_name = {"count_only": "1"}
    if filtered:
        query_by_name.update(classification="Pharmacy", city="Synthetic")
    request = SimpleNamespace(args=query_by_name, ctx=SimpleNamespace(sa_session=session))
    if failure in {"cancellation", "unavailable"}:
        error = asyncio.CancelledError if failure == "cancellation" else ServiceUnavailable
        with pytest.raises(error):
            async with database.reader_session():
                await npi.get_all(request)
    else:
        async with database.reader_session():
            response = await npi.get_all(request)
            body = json.loads(response.body)
            if failure is None:
                assert response.status == 200 and body == {"rows": 12}
            else:
                assert response.status == 503 and body["error"] == "count_timeout" and "rows" not in body
            assert not session.rolled_back
    assert session.count_reads == 1 and "page" not in events
    if not filtered:
        database.scalar.assert_not_awaited()
    assert npi._NPI_COUNT_READ_SCOPE.get() is None


async def test_npi_unfiltered_timeout_restores_native_ceiling_before_page(monkeypatch):
    from api import provider_list_sql
    from api.endpoint import npi
    from process.custom_import.read_core import _local_statement_timeout

    database, session, events = _optional_count_reader(monkeypatch, "statement_timeout", unfiltered=True)
    execute = session.execute
    settings = SimpleNamespace(timeout_text="7000", timeout_milliseconds="7000")
    timeout_values = []

    async def execute_with_settings(statement, params=None):
        if "AS timeout_text" in str(statement):
            return SimpleNamespace(one_or_none=lambda: settings)
        if "set_config(" in str(statement):
            setting, timeout_value, local = statement.compile().params.values()
            assert setting == "statement_timeout" and local is True
            settings.timeout_text = settings.timeout_milliseconds = timeout_value
            timeout_values.append(int(timeout_value))
            return None
        return await execute(statement, params)

    monkeypatch.setattr(session, "execute", execute_with_settings)
    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", _local_statement_timeout)
    request = SimpleNamespace(args={"limit": "1"}, ctx=SimpleNamespace(sa_session=session))
    async with database.reader_session():
        response = await npi.get_all(request)
        assert response.status == 200 and not session.rolled_back
        assert settings.timeout_text == "7000"
    assert len(timeout_values) == 2 and 0 < timeout_values[0] <= 100 and timeout_values[-1] == 7000
    assert events == ["savepoint", "nested rollback", "page"] and session.count_reads == 1


@pytest.mark.parametrize("metadata_seconds", [0.08, 0.11])
async def test_npi_unfiltered_cache_lookup_and_count_share_deadline(monkeypatch, metadata_seconds):
    import json

    from api import provider_list_sql
    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, None, unfiltered=True)
    clock = SimpleNamespace(value=1.0)
    clock.monotonic = lambda: clock.value
    monkeypatch.setattr(npi, "time", clock)
    monkeypatch.setattr(provider_list_sql, "time", clock)

    async def cache_identity(_model, **_kwargs):
        clock.value += metadata_seconds
        return "synthetic-publication"

    timeout_values = []

    @asynccontextmanager
    async def native_timeout(owner, *, timeout_ms):
        assert owner is session
        timeout_values.append(timeout_ms)
        yield

    connection = database.acquire()._conn
    read = connection.all

    async def count_crosses_deadline(statement, **params):
        response = await read(statement, **params)
        if "page_npis AS" not in str(statement):
            clock.value = 1.11
        return response

    monkeypatch.setattr(npi, "_npi_count_cache_identity", cache_identity)
    monkeypatch.setattr(connection, "all", count_crosses_deadline)
    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", native_timeout)
    # Both expiry points must recover the Reader before fetching the page.
    request = SimpleNamespace(args={"limit": "1"}, ctx=SimpleNamespace(sa_session=session))
    async with database.reader_session():
        response = await npi.get_all(request)
        assert response.status == 200 and json.loads(response.body)["total_source"] == "estimated_timeout_floor"
    assert session.count_reads == int(metadata_seconds < 0.1)
    if metadata_seconds < 0.1:
        assert len(timeout_values) == 1 and 20 <= timeout_values[0] <= 21
    else:
        assert timeout_values == []
    assert npi._NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] is None
    assert events == ["savepoint", "nested rollback", "page"]
    assert npi._NPI_COUNT_READ_SCOPE.get() is None


async def test_npi_count_metadata_statements_consume_one_phase_budget(monkeypatch):
    import json

    from sqlalchemy import text
    from sqlalchemy.exc import DBAPIError

    from api import provider_list_sql
    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, None, unfiltered=True)
    clock = SimpleNamespace(value=1.0)
    clock.monotonic = lambda: clock.value
    monkeypatch.setattr(npi, "time", clock)
    monkeypatch.setattr(provider_list_sql, "time", clock)
    budget = SimpleNamespace(milliseconds=None)
    ceilings = []

    @asynccontextmanager
    async def native_timeout(owner, *, timeout_ms):
        assert owner is session
        budget.milliseconds = timeout_ms
        ceilings.append(timeout_ms)
        yield

    execute = session.execute

    async def metadata_execute(statement, parameters=None):
        if str(statement) not in {"SELECT first_metadata", "SELECT second_metadata"}:
            return await execute(statement, parameters)
        allowed = budget.milliseconds / 1000
        clock.value += min(0.08, allowed)
        if allowed < 0.08:
            error = RuntimeError("synthetic statement timeout")
            error.sqlstate = "57014"
            raise DBAPIError(None, None, error)
        return None

    async def cache_identity(_model, *, session):
        await npi._execute_stmt(text("SELECT first_metadata"), session=session)
        await npi._execute_stmt(text("SELECT second_metadata"), session=session)
        return "synthetic-publication"

    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", native_timeout)
    monkeypatch.setattr(npi, "_npi_count_cache_identity", cache_identity)
    request = SimpleNamespace(args={"limit": "1"}, ctx=SimpleNamespace(sa_session=session))
    async with database.reader_session():
        monkeypatch.setattr(session, "execute", metadata_execute)
        response = await npi.get_all(request)
        assert response.status == 200 and json.loads(response.body)["total_source"] == "estimated_timeout_floor"
    assert len(ceilings) == 2 and 100 <= ceilings[0] <= 101 and 20 <= ceilings[1] <= 21
    assert clock.value <= 1.102 and session.count_reads == 0
    assert events == ["savepoint", "nested rollback", "page"]
    assert npi._NPI_COUNT_READ_SCOPE.get() is None


def _native_cache_timeout_fixture(session):
    """Emulate native cancellation and restoration on the original request session."""
    from sqlalchemy.exc import DBAPIError

    state = SimpleNamespace(aborted=False, native_reads=0, rollbacks=0, depth=0, failed_restores=0)
    settings = SimpleNamespace(timeout_text="7", timeout_milliseconds="7")
    execute = session.execute

    @asynccontextmanager
    async def nested():
        state.depth += 1
        try:
            yield session
        except BaseException:
            state.aborted = False
            state.rollbacks += 1
            raise
        finally:
            state.depth -= 1

    async def execute_cache(statement, parameters=None):
        sql = str(statement)
        if state.aborted:
            assert "set_config(" in sql
            state.failed_restores += 1
            error = RuntimeError("synthetic aborted transaction")
            error.sqlstate = "25P02"
            raise DBAPIError(None, None, error)
        if "AS timeout_text" in sql:
            return SimpleNamespace(one_or_none=lambda: settings)
        if "set_config(" in sql:
            setting, timeout_value, local = statement.compile().params.values()
            assert setting == "statement_timeout" and local is True and 0 < int(timeout_value) <= 7
            settings.timeout_text = settings.timeout_milliseconds = timeout_value
            return None
        if "npi_canonical_publication_receipt" in sql:
            assert state.depth == 1
            return SimpleNamespace(all=lambda: [])
        if "npi_result_generation" in sql:
            assert state.depth == 2
            state.native_reads += 1
            state.aborted = True
            error = RuntimeError("synthetic native identity statement timeout")
            error.sqlstate = "57014"
            raise DBAPIError(None, None, error)
        return await execute(statement, parameters)

    return state, settings, nested, execute_cache


async def test_npi_count_native_cache_timeout_recovers_original_request_session(monkeypatch):
    """Recover both nested scopes after native identity timeout without replacing the Reader."""
    import json

    from api import provider_list_sql
    from api.endpoint import npi
    from process.custom_import.read_core import _local_statement_timeout

    cache_identity = npi._npi_count_cache_identity
    database, session, _events = _optional_count_reader(monkeypatch, None, unfiltered=True)
    monkeypatch.setattr(npi, "_npi_count_cache_identity", cache_identity)
    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", _local_statement_timeout)
    state, settings, nested, execute_cache = _native_cache_timeout_fixture(session)

    monkeypatch.setattr(session, "begin_nested", nested)
    request = SimpleNamespace(args={"count_only": "1"}, ctx=SimpleNamespace(sa_session=session))
    async with database.reader_session():
        monkeypatch.setattr(session, "execute", execute_cache)
        response = await npi.get_all(request)
        assert response.status == 503 and json.loads(response.body)["error"] == "count_timeout"
        assert not state.aborted and not session.rolled_back
        assert state.native_reads == 1 and state.rollbacks == 2 and state.failed_restores == 1
        assert settings.timeout_text == "7"
    assert session.count_reads == 0 and npi._NPI_COUNT_READ_SCOPE.get() is None


@pytest.mark.parametrize("count_format", ["all", "full_taxonomy", "classification"])
async def test_npi_formatted_counts_share_reader_deadline(monkeypatch, count_format):
    import json

    from api.endpoint import npi

    database, session, events = _optional_count_reader(monkeypatch, "statement_timeout")
    request = SimpleNamespace(
        args={"count_only": "1", "classification": "Pharmacy", "city": "Synthetic", "format": count_format},
        ctx=SimpleNamespace(sa_session=session),
    )
    async with database.reader_session():
        result = await npi.get_all(request)
        assert result.status == 503 and json.loads(result.body)["error"] == "count_timeout"
        assert not session.rolled_back
    assert session.count_reads == 1 and events == ["savepoint", "nested rollback"]
    assert npi._NPI_COUNT_READ_SCOPE.get() is None


async def test_npi_count_deadline_context_does_not_reach_another_task(monkeypatch):
    from api import provider_list_sql
    from api.endpoint import npi

    started, other_finished = asyncio.Event(), asyncio.Event()
    owner = object()
    native_owners = []

    @asynccontextmanager
    async def native_timeout(session, *, timeout_ms):
        native_owners.append(session)
        yield

    async def counted_read():
        started.set()
        await other_finished.wait()
        return 1

    async def count_task():
        token = npi._NPI_COUNT_READ_SCOPE.set((owner, npi.time.monotonic() + 1))
        try:
            assert await npi._npi_count_read(counted_read) == 1
        finally:
            npi._NPI_COUNT_READ_SCOPE.reset(token)
        assert npi._NPI_COUNT_READ_SCOPE.get() is None

    async def unrelated_task():
        await started.wait()
        assert npi._NPI_COUNT_READ_SCOPE.get() is None
        assert await npi._npi_count_read(AsyncMock(return_value=42)) == 42
        other_finished.set()

    monkeypatch.setattr(provider_list_sql, "_local_statement_timeout", native_timeout)
    await asyncio.gather(count_task(), unrelated_task())
    assert native_owners == [owner] and npi._NPI_COUNT_READ_SCOPE.get() is None


@pytest.mark.parametrize("status", [200, 503])
@pytest.mark.parametrize(
    "path, enabled",
    [
        ("/api/v1/npi/", "true"),
        ("/control/v1/plan-pricing/code-projection", "true"),
        ("/control/v1/plan-pricing/code-projection", "false"),
    ],
)
async def test_public_request_routes_helpers_and_releases_reader(monkeypatch, status, path, enabled):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", enabled)
    monkeypatch.setattr("api.reference_family_reads.pin_claims_reader", AsyncMock())
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    middleware_by_kind = {}

    class App:
        def listener(self, _name):
            return lambda callback: callback

        def middleware(self, name):
            def register(callback):
                middleware_by_kind[name] = callback
                return callback

            return register

    database.init_app(App())
    request = SimpleNamespace(path=path, ctx=SimpleNamespace())
    await middleware_by_kind["request"](request)
    assert request.ctx.session is session
    if path.startswith("/control/"):
        from api import plan_release_read_boundary as boundary

        monkeypatch.setattr(boundary, "db", database)
        request.headers = SimpleNamespace(getall=lambda _name, _default: ["internal"])
        request.get_args = lambda **_kwargs: {"plan_release_id": "hprelease_01J00000000000000000000000"}
        assert await boundary.require_internal_plan_release_read(request) is None
    await database.scalar("SELECT 1")
    await middleware_by_kind["response"](request, SimpleNamespace(status=status))
    assert session.closed and request.ctx.session is None
    assert session.committed if status == 200 else session.rolled_back
    assert not database.has_reader_session()


@pytest.mark.parametrize("inherited_ms", [0, 7])
async def test_optional_count_uses_remaining_native_budget_and_restores_ceiling(monkeypatch, inherited_ms):
    from api import provider_list_sql

    clocks = iter([1.0, 1.01])
    monkeypatch.setattr(provider_list_sql, "time", SimpleNamespace(monotonic=lambda: next(clocks)))
    settings = SimpleNamespace(timeout_text=str(inherited_ms), timeout_milliseconds=str(inherited_ms))
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: settings)))
    connection = SimpleNamespace(all=AsyncMock(return_value=[(12,)]))
    assert (
        await provider_list_sql._provider_list_count(connection, "count query", {"value": 1}, session, deadline=1.025)
        == 12
    )
    connection.all.assert_awaited_once_with("count query", value=1)
    calls = session.execute.await_args_list
    assert len(calls) == 3
    assert list(calls[1].args[0].compile().params.values()) == ["statement_timeout", str(inherited_ms or 25), True]
    assert list(calls[2].args[0].compile().params.values()) == ["statement_timeout", str(inherited_ms), True]


@pytest.mark.parametrize("has_started", [False, True])
async def test_optional_count_expired_cumulative_budget_never_returns_total(monkeypatch, has_started):
    from api import provider_list_sql

    clocks = iter([1.0, 2.0] if has_started else [2.0])
    monkeypatch.setattr(provider_list_sql, "time", SimpleNamespace(monotonic=lambda: next(clocks)))
    connection = SimpleNamespace(all=AsyncMock(return_value=[(12,)]))
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(
                one_or_none=lambda: SimpleNamespace(timeout_text="0", timeout_milliseconds="0")
            )
        )
    )
    with pytest.raises(TimeoutError, match="deadline expired"):
        await provider_list_sql._provider_list_count(connection, "count query", {}, session, deadline=1.5)
    assert connection.all.await_count == int(has_started)
    assert session.execute.await_count == (3 if has_started else 0)


async def test_reader_temporary_setup_precedes_read_only_verification(monkeypatch):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])

    async def setup(fresh):
        assert fresh is session and not session.info and not database.has_reader_session()
        assert [str(statement) for statement, _ in session.executed] == ["SET TRANSACTION READ WRITE"]
        await fresh.execute("CREATE TEMP TABLE prepared(value int) ON COMMIT DROP")

    async with database.reader_session(temporary_setup=setup) as acquired:
        assert acquired is session and session.info["api_reader_verified"]
        assert current_session() is session
        assert [str(statement) for statement, _ in session.executed] == [
            "SET TRANSACTION READ WRITE",
            "CREATE TEMP TABLE prepared(value int) ON COMMIT DROP",
            "SET TRANSACTION READ ONLY",
            "SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only')",
        ]
    assert session.committed and session.closed and not session.info and not database.has_reader_session()


async def test_reader_temporary_setup_refuses_reuse_and_allows_explicit_independent(monkeypatch):
    parent, child = ReaderSession(), ReaderSession()
    database = reader_database(monkeypatch, [parent, child])
    setup = AsyncMock()
    async with database.reader_session():
        with pytest.raises(RuntimeError, match="fresh Reader"):
            async with database.reader_session(temporary_setup=setup):
                pytest.fail("reused Reader accepted setup")
        setup.assert_not_awaited()
        assert not parent.executed
        async with database.reader_session(independent=True, temporary_setup=setup) as acquired:
            assert acquired is child and current_session() is child
            setup.assert_awaited_once_with(child)
        assert current_session() is parent and not parent.closed
    assert parent.closed and child.closed and not database.has_reader_session()


@pytest.mark.parametrize("failure", [RuntimeError("setup failed"), asyncio.CancelledError()])
async def test_reader_temporary_setup_failure_rolls_back_before_verification(monkeypatch, failure):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    with pytest.raises(type(failure)) as caught:
        async with database.reader_session(temporary_setup=AsyncMock(side_effect=failure)):
            pytest.fail("failed setup yielded a Reader")
    assert caught.value is failure
    assert [str(statement) for statement, _ in session.executed] == ["SET TRANSACTION READ WRITE"]
    assert session.rolled_back and session.closed and not session.committed and not session.info
    assert not database.has_reader_session()


@pytest.mark.parametrize("modes", [("read committed", "on"), ("repeatable read", "off")])
async def test_prepared_reader_refuses_unverified_transaction_modes(monkeypatch, modes):
    session = ReaderSession()
    session.modes = modes
    database = reader_database(monkeypatch, [session])
    with pytest.raises(RuntimeError, match="repeatable read and read only"):
        async with database.reader_session(temporary_setup=AsyncMock()):
            pytest.fail("wrong transaction mode yielded a verified Reader")
    assert session.rolled_back and session.closed and not session.info and not database.has_reader_session()


@pytest.mark.parametrize("stage", ["setup", "commit"])
async def test_prepared_reader_preserves_primary_error_when_rollback_and_close_fail(monkeypatch, stage):
    failure = RuntimeError("original failure")
    session = ReaderSession()
    session.rollback = AsyncMock(side_effect=RuntimeError("rollback failure"))
    session.close = AsyncMock(side_effect=RuntimeError("close failure"))
    if stage == "commit":
        session.commit = AsyncMock(side_effect=failure)
    database = reader_database(monkeypatch, [session])
    with pytest.raises(RuntimeError, match="No SQLAlchemy session bound"):
        current_session()
    with pytest.raises(RuntimeError) as caught:
        async with database.reader_session(
            temporary_setup=AsyncMock(side_effect=failure if stage == "setup" else None)
        ):
            assert stage == "commit"
    assert caught.value is failure and str(failure) == "original failure"
    assert not session.info and not database.has_reader_session()
    with pytest.raises(RuntimeError, match="No SQLAlchemy session bound"):
        current_session()
    session.close.assert_awaited_once()


@pytest.mark.parametrize("failure", [RuntimeError("body failed"), asyncio.CancelledError()])
async def test_prepared_reader_body_failure_preserves_error_and_cleans_context(monkeypatch, failure):
    session = ReaderSession()
    database = reader_database(monkeypatch, [session])
    with pytest.raises(type(failure)) as caught:
        async with database.reader_session(temporary_setup=AsyncMock()):
            raise failure
    assert caught.value is failure and session.closed and session.rolled_back and not session.committed
    assert not session.info and not database.has_reader_session()


async def test_reader_rejects_invalid_temporary_setup_before_connect(monkeypatch):
    database = reader_database(monkeypatch, [])
    with pytest.raises(TypeError, match="async callable"):
        async with database.reader_session(temporary_setup=object()):
            pytest.fail("invalid setup yielded a Reader")
    database._connect_reader.assert_not_awaited()
