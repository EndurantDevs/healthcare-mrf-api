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
    """Render the installed receipt readers, including their conditional archive dependency."""
    from tests.provider_profile_snapshot_postgres_support import READ_TABLES
    from tests.test_provider_directory_cms_serving_receipt_postgres import _migration

    migration = _migration("20260930100000")
    statements = []
    monkeypatch.setattr(migration.op, "execute", statements.append)
    for render in (
        migration._create_native_readers,
        migration._create_snapshot_check,
        migration._create_current_check,
        migration._create_result_check,
        migration._create_archive_check,
    ):
        render('"synthetic"')
    required_tables = set(re.findall(r'\b(?:FROM|JOIN)\s+"synthetic"\.(\w+)', "\n".join(statements)))
    assert "address_alias_state_v1" in required_tables
    assert "cms_native_input_revision" in required_tables
    assert required_tables <= set(READ_TABLES)


class ReaderSession(_FakeSession):
    """Supply the genuine-login query result without a PostgreSQL connection."""

    def __init__(self, identity=("reader_test", "reader_test")):
        super().__init__()
        self.identity = identity
        self.info = {}

    async def execute(self, statement, params=None):
        self._in_tx = True
        if str(statement) == "SELECT session_user, current_user":
            self.executed.append((statement, params))
            return _FakeResult(self.identity)
        return await super().execute(statement, params)

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


async def test_reader_pool_uses_separate_login_and_read_only_transactions(monkeypatch):
    engine_by_field = {}
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
    assert engine_by_field["options"]["pool_size"] == 2
    assert engine_by_field["options"]["max_overflow"] == 1
    await writer.disconnect()
    assert reader.engine is None
    assert writer._reader_database is None


@pytest.mark.parametrize("user", ["", "writer_test"])
async def test_reader_configuration_never_falls_back_to_writer(monkeypatch, user):
    monkeypatch.setenv("HLTHPRT_DB_USER", "writer_test")
    monkeypatch.setenv("HLTHPRT_DB_READER_USER", user)
    writer = Database()
    with pytest.raises(RuntimeError, match="distinct Reader"):
        await writer._connect_reader()
    assert writer.engine is None and writer.session_factory is None


@pytest.mark.parametrize("path", ["/api/v1/npi", "/api/v1/npi/", "/api/v1/npi/id/batch", "/api/v1/codes"])
def test_reader_path_includes_root_and_post_read_routes(path):
    assert Database._is_pinned_read_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "/control/imports",
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
    assert len(session.executed) == 3


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


async def test_wrong_login_refuses_before_payload_and_closes(monkeypatch):
    session = ReaderSession(("reader_test", "writer_test"))
    database = reader_database(monkeypatch, [session])
    with pytest.raises(RuntimeError, match="login identity"):
        async with database.reader_session():
            pytest.fail("unverified Reader reached payload")
    assert len(session.executed) == 1 and session.closed and session.rolled_back


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
        assert len(session.executed) == 5


def _optional_count_reader(monkeypatch, failure):
    """Bind the real provider caller to a Reader and record its savepoint boundary."""
    from sqlalchemy.exc import DBAPIError, SQLAlchemyError

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

    @asynccontextmanager
    async def savepoint():
        events.append("savepoint")
        try:
            yield session
        except BaseException:
            events.append("nested rollback")
            raise

    async def read(sql, **_params):
        assert current_session() is session
        if "SELECT COUNT(DISTINCT" in str(sql):
            assert events == ["savepoint"]
            if failure == "cancellation":
                raise asyncio.CancelledError
            if failure == "statement_timeout":
                error = RuntimeError("query cancelled")
                error.sqlstate = "57014"
                raise DBAPIError(None, None, error)
            raise SQLAlchemyError("optional count failed")
        assert events == ["savepoint", "nested rollback"]
        assert session.in_transaction() and not session.rolled_back
        events.append("page")
        return []

    monkeypatch.setattr(session, "begin_nested", savepoint)
    monkeypatch.setattr(connection, "all", read)
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


@pytest.mark.parametrize("status", [200, 503])
async def test_public_request_routes_helpers_and_releases_reader(monkeypatch, status):
    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", "true")
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
    request = SimpleNamespace(path="/api/v1/npi/", ctx=SimpleNamespace())
    await middleware_by_kind["request"](request)
    assert request.ctx.session is session
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
