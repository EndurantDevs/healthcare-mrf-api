# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, event, select, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import (
    ConnectionProxy,
    Database,
    DeleteAdapter,
    InsertAdapter,
    SelectAdapter,
)


@pytest.mark.asyncio
async def test_database_connect_initializes_engine(monkeypatch):
    created_by_field = {}

    def fake_create_engine(url, **kwargs):  # pragma: no cover - executed in test
        created_by_field["url"] = url
        created_by_field["kwargs"] = kwargs
        return SimpleNamespace(connect=AsyncMock())

    def fake_sessionmaker(engine, **kwargs):
        created_by_field["session_kwargs"] = kwargs
        return lambda: "session"

    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "psycopg")
    monkeypatch.setenv("HLTHPRT_DB_USER", "user")
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", "pass")
    monkeypatch.setenv("HLTHPRT_DB_HOST", "host")
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5433")
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "db")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "4")

    monkeypatch.setattr("db.connection.create_async_engine", fake_create_engine)
    monkeypatch.setattr("db.connection.async_sessionmaker", fake_sessionmaker)

    db = Database()
    await db.connect()

    assert created_by_field["url"].drivername == "postgresql+psycopg"
    assert created_by_field["kwargs"]["pool_size"] == 2
    assert created_by_field["kwargs"]["max_overflow"] == 2
    assert created_by_field["kwargs"]["hide_parameters"] is True
    assert db.engine is not None
    assert db.session_factory is not None


class _FakeResult:
    def __init__(self, value=None, rowcount=None):
        self._value = value
        self.rowcount = rowcount

    def all(self):
        return [self._value]

    def first(self):
        return self._value

    def scalar(self):
        return self._value


class _FakeSession:
    def __init__(self):
        self._in_tx = False
        self.executed = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.nested_transactions = 0

    async def execute(self, stmt, params=None):
        self.executed.append((stmt, params))
        return _FakeResult(value=42, rowcount=1)

    def in_transaction(self):
        return self._in_tx

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True

    async def close(self):
        self.closed = True

    @asynccontextmanager
    async def begin(self):
        self._in_tx = True
        try:
            yield self
        except Exception:
            self.rolled_back = True
            raise
        else:
            self.committed = True
        finally:
            self._in_tx = False

    @asynccontextmanager
    async def begin_nested(self):
        self.nested_transactions += 1
        try:
            yield self
        except Exception:
            self.rolled_back = True
            raise


@pytest.mark.asyncio
async def test_session_manager_commits_and_rolls_back():
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.session() as acquired:
        assert acquired is session
        session._in_tx = True

    assert session.committed
    assert session.closed

    session2 = _FakeSession()
    db.session_factory = lambda: session2
    with pytest.raises(RuntimeError):
        async with db.session():
            session2._in_tx = True
            raise RuntimeError

    assert session2.rolled_back


@pytest.mark.asyncio
async def test_transaction_context(monkeypatch):
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.transaction() as tx_session:
        assert tx_session is session

    assert not session._in_tx


@pytest.mark.asyncio
async def test_transaction_helpers_reuse_one_bound_session():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async with db.transaction() as transaction_session:
        assert await db.status("UPDATE test_table SET value = 1") == 1
        assert await db.scalar("SELECT 42") == 42
        assert await db.all("SELECT 42") == [42]
        assert await db.select(1).scalar() == 42

    assert sessions == [transaction_session]
    assert len(transaction_session.executed) == 4
    assert transaction_session.committed
    assert transaction_session.closed


@pytest.mark.asyncio
async def test_session_and_native_acquire_reuse_bound_session_without_ending_it():
    """Only an explicitly borrowed publication session joins the caller's transaction."""
    db = Database()
    engine = create_async_engine("postgresql+asyncpg://tester@localhost/synthetic")
    db._database_override = "synthetic"
    try:
        async with AsyncSession(bind=engine) as session, session.begin():
            async with db.bind_existing_session(session):
                assert db._transaction_binding().borrowed
                async with db.session() as nested_session:
                    assert nested_session is session
                async with db.acquire() as native_connection:
                    assert native_connection._connection is session
                async with db.transaction() as nested_session:
                    assert nested_session is session
                    assert session.in_nested_transaction()
                assert session.in_transaction() and not session.in_nested_transaction()
            assert session.in_transaction()
    finally:
        await engine.dispose()
    assert db.engine is None and db.session_factory is None


@pytest.mark.asyncio
@pytest.mark.parametrize("connection_bound", [False, True])
@pytest.mark.parametrize("database_matches", [False, True])
async def test_session_metadata_checks_engine_and_connection_database_identity(connection_bound, database_matches):
    engine = create_async_engine("postgresql+asyncpg://tester@localhost/synthetic")
    session = SimpleNamespace(bind=engine.connect() if connection_bound else engine)
    db = Database()
    db._database_override = "synthetic" if database_matches else "other"
    try:
        assert db._session_database_name(session) == "synthetic"
        if database_matches:
            db._validate_existing_session_database(session)
        else:
            with pytest.raises(RuntimeError, match="database identity does not match"):
                db._validate_existing_session_database(session)
        assert engine.pool.checkedout() == 0
    finally:
        await engine.dispose()
    assert db.engine is None and db.session_factory is None


@pytest.mark.asyncio
@pytest.mark.parametrize("child_task", [False, True])
async def test_owned_transaction_preserves_independent_session_and_acquire(child_task):
    """Independent durable work is not rolled back with an ordinary outer transaction."""
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=object()))

    @asynccontextmanager
    async def acquire_connection():
        yield connection

    db = Database(engine=SimpleNamespace(begin=acquire_connection), session_factory=session_factory)
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with db.transaction() as outer_session:
            assert not db._transaction_binding().borrowed

            async def independent_work():
                async with db.session() as independent_session, independent_session.begin():
                    assert independent_session is not outer_session
                async with db.acquire() as proxy:
                    assert proxy._connection is connection

            if child_task:
                await asyncio.create_task(independent_work())
            else:
                await independent_work()
            assert db._transaction_binding().session is outer_session
            assert not outer_session.committed and not outer_session.closed
            raise RuntimeError("outer rollback")
    assert len(sessions) == 2
    assert sessions[0].rolled_back and not sessions[0].committed
    assert sessions[1].committed and not sessions[1].rolled_back
    assert all(session.closed for session in sessions)


@pytest.mark.asyncio
async def test_independent_session_can_begin_inside_real_session_transaction():
    """SQLAlchemy's real session protocol permits the separate claim transaction."""
    db = Database(session_factory=AsyncSession)
    async with db.transaction() as outer_session:
        async with db.session() as independent_session, independent_session.begin():
            assert independent_session is not outer_session
            assert independent_session.in_transaction()
        assert outer_session.in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["session", "acquire", "transaction"])
async def test_borrowed_session_refuses_child_task_participation(operation):
    """Publication's caller-owned transaction never leaks to a child task."""
    db = Database()

    async def child_work():
        async with getattr(db, operation)():
            pytest.fail("borrowed transaction entered by a child task")

    engine = create_async_engine("postgresql+asyncpg://tester@localhost/synthetic")
    db._database_override = "synthetic"
    try:
        async with AsyncSession(bind=engine) as session, session.begin(), db.bind_existing_session(session):
            with pytest.raises(RuntimeError, match="child asyncio task"):
                await asyncio.create_task(child_work())
            assert session.in_transaction()
    finally:
        await engine.dispose()
    assert db.engine is None and db.session_factory is None


@pytest.mark.asyncio
async def test_helpers_keep_independent_sessions_outside_transaction():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    await db.status("UPDATE test_table SET value = 1")
    await db.scalar("SELECT 42")
    await db.all("SELECT 42")

    assert len(sessions) == 3
    assert all(session.closed for session in sessions)


@pytest.mark.asyncio
async def test_nested_transaction_uses_savepoint_on_bound_session():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async with db.transaction() as outer_session:
        async with db.transaction() as nested_session:
            assert nested_session is outer_session
            assert await db.scalar("SELECT 42") == 42

    assert sessions == [outer_session]
    assert outer_session.nested_transactions == 1


@pytest.mark.asyncio
async def test_nested_database_transactions_keep_each_binding():
    first_db = Database()
    second_db = Database()
    first_session = _FakeSession()
    second_session = _FakeSession()
    first_db.session_factory = lambda: first_session
    second_db.session_factory = lambda: second_session

    async with first_db.transaction():
        async with second_db.transaction():
            await first_db.scalar("SELECT 1")
            await second_db.scalar("SELECT 2")

    assert len(first_session.executed) == 1
    assert len(second_session.executed) == 1


@pytest.mark.asyncio
async def test_transaction_session_is_not_shared_with_child_task():
    db = Database()
    session = _FakeSession()
    db.session_factory = lambda: session

    async with db.transaction():
        child_call = asyncio.create_task(db.scalar("SELECT 42"))
        with pytest.raises(RuntimeError, match="child asyncio task"):
            await child_call
        assert await db.scalar("SELECT 42") == 42


@pytest.mark.asyncio
async def test_parallel_top_level_transactions_use_distinct_sessions():
    db = Database()
    sessions = []

    def session_factory():
        session = _FakeSession()
        sessions.append(session)
        return session

    db.session_factory = session_factory

    async def run_transaction():
        async with db.transaction() as session:
            await asyncio.sleep(0)
            await db.scalar("SELECT 42")
            return session

    first_session, second_session = await asyncio.gather(
        run_transaction(),
        run_transaction(),
    )

    assert first_session is not second_session
    assert sessions == [first_session, second_session]


@pytest.mark.asyncio
async def test_transaction_helpers_share_real_postgres_transaction():
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", "").lower():
        pytest.skip("real transaction test requires a disposable test database")

    database = Database()
    marker = "healthporta-db-transaction-test"
    await database.connect()
    try:
        async with database.transaction():
            await database.status(
                "SELECT set_config('application_name', :marker, true);",
                marker=marker,
            )
            backend_pid = await database.scalar("SELECT pg_backend_pid();")
            rows = await database.all("SELECT pg_backend_pid(), current_setting('application_name');")

            assert rows[0][0] == backend_pid
            assert rows[0][1] == marker

        assert await database.scalar("SELECT current_setting('application_name');") != marker
    finally:
        await database.disconnect()


async def _seed_native_reader(connection, schema, role, password):
    """Give one distinct login only its two exact current/future read heaps."""
    await connection.execute(
        text(f'CREATE ROLE "{role}" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
    )
    try:
        await connection.execute(text(f"ALTER ROLE \"{role}\" PASSWORD '{password}'"))
    except Exception:
        raise RuntimeError("Reader test login password provisioning failed") from None
    await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    for name, marker in (("page", "old"), ("page_next", "new")):
        await connection.execute(text(f'CREATE TABLE "{schema}".{name}(id integer PRIMARY KEY,marker text NOT NULL)'))
        await connection.execute(text(f'INSERT INTO "{schema}".{name} VALUES (1,:marker)'), {"marker": marker})
        await connection.execute(text(f'GRANT SELECT ON "{schema}".{name} TO "{role}"'))
    await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))


async def _cleanup_native_reader(database, engine, schema, role):
    """Close both pools before removing only registered UUID-owned resources."""
    try:
        await database.disconnect()
    finally:
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                await connection.execute(text(f'DROP ROLE IF EXISTS "{role}"'))
                assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
                )
        finally:
            await engine.dispose()


@asynccontextmanager
async def _native_reader_database(monkeypatch, tmp_path):
    """Reuse the existing dedicated local or CI database guard, never ambient defaults."""
    from tests.test_code_catalog_snapshot_postgres import _dsn

    url = make_url(_dsn())
    schema, role = "api_read_" + uuid4().hex, "api_reader_" + uuid4().hex
    password = uuid4().hex
    # Cleanup intent is durable before either role or schema can be created.
    (tmp_path / "reader-resources.json").write_text(json.dumps({"schema": schema, "role": role}) + "\n")
    engine = create_async_engine(url, hide_parameters=True, echo=False)
    database, statements = Database(), []
    has_cleanup_authority = False
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is None
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )
            has_cleanup_authority = True
            await _seed_native_reader(connection, schema, role, password)
        settings_by_name = {
            "DRIVER": "asyncpg",
            "HOST": url.host,
            "PORT": str(url.port or 5432),
            "USER": url.username,
            "PASSWORD": url.password or "",
            "DATABASE": url.database,
            "READER_USER": role,
            "READER_PASSWORD": password,
            "READER_POOL_MIN_SIZE": "1",
            "READER_POOL_MAX_SIZE": "2",
            "ECHO": "False",
        }
        for name, setting_value in settings_by_name.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, setting_value)
        reader = await database._connect_reader()

        def record_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement)

        event.listen(reader.engine.sync_engine, "before_cursor_execute", record_statement)
        yield database, engine, schema, role, statements
    finally:
        if has_cleanup_authority:
            await _cleanup_native_reader(database, engine, schema, role)
        else:
            await engine.dispose()


@pytest.mark.parametrize(
    "dsn,is_admitted",
    [
        ("postgresql://postgres@localhost:5432/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@postgres/ptg2_v3_lifecycle_test_ci_runner", True),
        ("postgresql://postgres@127.0.0.1:5440/hc_code_catalog_archive_0123456789abcdef0123456789abcdef", True),
        ("postgresql://postgres@remote.example.test:5432/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@localhost:5432/another_database", False),
        ("postgresql://another_user@localhost:5432/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@localhost:5441/ptg2_v3_lifecycle_test_ci_runner", False),
        ("postgresql://postgres@localhost:5440/ptg2_v3_lifecycle_test_ci_runner", False),
    ],
)
async def test_reader_native_fixture_uses_existing_dedicated_database_guard(monkeypatch, tmp_path, dsn, is_admitted):
    """Refused targets cannot register resources or reach the engine creation boundary."""
    monkeypatch.setenv("HLTHPRT_CODE_CATALOG_ARCHIVE_TEST_DSN", dsn)

    def engine_boundary(_url, **options):
        assert options == {"hide_parameters": True, "echo": False}
        raise RuntimeError("engine creation boundary")

    monkeypatch.setattr("tests.test_db_connection.create_async_engine", engine_boundary)
    with pytest.raises(RuntimeError if is_admitted else pytest.fail.Exception):
        async with _native_reader_database(monkeypatch, tmp_path):
            pytest.fail("unexpected database connection")
    assert (tmp_path / "reader-resources.json").exists() is is_admitted


@pytest.mark.parametrize("has_authentication_failed", [False, True])
async def test_reader_native_fixture_binds_distinct_password_and_cleans_authentication_failure(
    monkeypatch, tmp_path, has_authentication_failed
):
    """Provision and connect with one transient Reader secret, never the admin credential."""
    monkeypatch.setenv(
        "HLTHPRT_CODE_CATALOG_ARCHIVE_TEST_DSN",
        "postgresql://postgres@postgres/ptg2_v3_lifecycle_test_ci_runner",
    )
    connection = SimpleNamespace(scalar=AsyncMock(side_effect=[None, False]))

    @asynccontextmanager
    async def admin_transaction():
        yield connection

    engine = SimpleNamespace(begin=admin_transaction)
    monkeypatch.setattr("tests.test_db_connection.create_async_engine", lambda *_args, **_kwargs: engine)
    seed, cleanup = AsyncMock(), AsyncMock()
    monkeypatch.setattr("tests.test_db_connection._seed_native_reader", seed)
    monkeypatch.setattr("tests.test_db_connection._cleanup_native_reader", cleanup)
    monkeypatch.setattr("tests.test_db_connection.event.listen", lambda *_args: None)

    async def connect_reader(database):
        password = seed.await_args.args[3]
        is_hex_password = len(password) == 32 and all(character in "0123456789abcdef" for character in password)
        is_password_bound = os.environ["HLTHPRT_DB_READER_PASSWORD"] == password
        assert is_hex_password and is_password_bound
        assert os.environ["HLTHPRT_DB_PASSWORD"] == ""
        assert os.environ["HLTHPRT_DB_PORT"] == "5432"
        assert os.environ["HLTHPRT_DB_READER_USER"] == seed.await_args.args[2]
        assert "password" not in (tmp_path / "reader-resources.json").read_text()
        if has_authentication_failed:
            raise RuntimeError("login refused")
        return SimpleNamespace(engine=SimpleNamespace(sync_engine=object()))

    monkeypatch.setattr(Database, "_connect_reader", connect_reader)
    if has_authentication_failed:
        with pytest.raises(RuntimeError, match="^login refused$"):
            async with _native_reader_database(monkeypatch, tmp_path):
                pytest.fail("authentication failure entered the read scope")
    else:
        async with _native_reader_database(monkeypatch, tmp_path) as (
            database,
            actual_engine,
            schema,
            role,
            _statements,
        ):
            assert actual_engine is engine and isinstance(database, Database)
            assert seed.await_args.args[1:3] == (schema, role)
    cleanup.assert_awaited_once()
    assert cleanup.await_args.args[1:] == (engine, *seed.await_args.args[1:3])


async def test_reader_native_password_provisioning_errors_do_not_expose_statement():
    """A failed credential DDL reports only a neutral error with suppressed SQL context."""
    connection = SimpleNamespace(execute=AsyncMock(side_effect=[None, RuntimeError("private SQL statement")]))
    with pytest.raises(RuntimeError, match="^Reader test login password provisioning failed$") as caught:
        await _seed_native_reader(connection, "schema_test", "role_test", "0" * 32)
    assert caught.value.__suppress_context__ and caught.value.__cause__ is None


async def _native_reader_state(database, schema):
    """Bind the same actual outer XID, heap identity, version and native read lock."""
    return (
        await database.all(
            "SELECT pg_current_xact_id()::text,current_setting('transaction_isolation'),"
            "current_setting('transaction_read_only'),session_user,current_user,"
            "to_regclass(:relation)::oid::bigint,"
            "EXISTS(SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid() AND granted "
            "AND mode='AccessShareLock' AND relation=to_regclass(:relation)),"
            "EXISTS(SELECT 1 FROM pg_roles WHERE rolname=current_user AND rolcanlogin "
            "AND NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication "
            "AND NOT rolbypassrls AND has_table_privilege(current_user,to_regclass(:relation),'SELECT') "
            "AND NOT has_table_privilege(current_user,to_regclass(:relation),'INSERT,UPDATE,DELETE,TRUNCATE'))",
            relation=f'"{schema}".page',
        )
    )[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sql", "statement_timeout"])
async def test_reader_native_savepoint_preserves_outer_snapshot(monkeypatch, tmp_path, failure):
    """An optional count failure cannot reopen the Reader or drop its old-version pin."""
    from api.provider_profile_snapshot import provider_read_savepoint

    async with _native_reader_database(monkeypatch, tmp_path) as (database, publisher, schema, role, statements):
        async with database.reader_session() as session:
            assert statements == ["SELECT session_user, current_user"]
            await session.execute(text(f'LOCK TABLE ONLY "{schema}".page IN ACCESS SHARE MODE'))
            before = await _native_reader_state(database, schema)
            assert tuple(before[1:5]) == ("repeatable read", "on", role, role) and tuple(before[6:]) == (True, True)
            assert await database.scalar(f'SELECT marker FROM "{schema}".page') == "old"
            previous_timeout = await session.scalar(text("SELECT current_setting('statement_timeout')"))
            with pytest.raises(DBAPIError) as caught:
                async with provider_read_savepoint(session):
                    await _native_optional_count(database, session, schema, failure)
            assert caught.value.orig.sqlstate == ("22012" if failure == "sql" else "57014")
            assert await session.scalar(text("SELECT current_setting('statement_timeout')")) == previous_timeout
            assert await _native_reader_state(database, schema) == before
            assert await database.scalar(f'SELECT marker FROM "{schema}".page') == "old"
            # A separate actual publisher still cannot take the cutover lock.
            with pytest.raises(DBAPIError) as blocked:
                async with publisher.begin() as connection:
                    await connection.execute(text(f'LOCK TABLE ONLY "{schema}".page IN ACCESS EXCLUSIVE MODE NOWAIT'))
            assert blocked.value.orig.sqlstate == "55P03"
            assert database.engine is None  # No Writer fallback pool was ever opened.
        async with publisher.begin() as connection:
            await connection.execute(text(f'ALTER TABLE "{schema}".page RENAME TO page_previous'))
            await connection.execute(text(f'ALTER TABLE "{schema}".page_next RENAME TO page'))
        async with database.reader_session():
            assert await database.scalar(f'SELECT marker FROM "{schema}".page') == "new"
            after = await _native_reader_state(database, schema)
            assert after[0] != before[0] and after[5] != before[5]
            assert tuple(after[1:5]) == ("repeatable read", "on", role, role)
        assert database.engine is None


async def _native_optional_count(database, session, schema, failure, *, timeout_seconds=0.05):
    """Execute the same final aggregate deadline helper used by provider lists."""
    from api.provider_list_sql import _provider_list_count

    query = (
        f'SELECT count(*)/(count(*)-count(*)) FROM "{schema}".page'
        if failure == "sql"
        else f'SELECT count(*) FROM "{schema}".page CROSS JOIN pg_sleep(5)'
    )
    return await _provider_list_count(
        ConnectionProxy(database, session, None), query, {}, session, deadline=time.monotonic() + timeout_seconds
    )


@pytest.mark.asyncio
async def test_reader_native_external_cancellation_releases_pin(monkeypatch, tmp_path):
    """External cancellation is terminal; only scope cleanup releases its pinned heap."""
    from api.provider_profile_snapshot import provider_read_savepoint

    async with _native_reader_database(monkeypatch, tmp_path) as (database, publisher, schema, _role, statements):
        with pytest.raises(TimeoutError):
            async with database.reader_session() as session:
                await session.execute(text(f'LOCK TABLE ONLY "{schema}".page IN ACCESS SHARE MODE'))
                async with provider_read_savepoint(session):
                    await _native_cancelled_count(database, session, schema)
                pytest.fail("cancelled request resumed its page")
        assert not database.has_reader_session() and database.engine is None
        assert any("pg_sleep(5)" in statement for statement in statements)
        async with publisher.begin() as connection:
            await connection.execute(text(f'LOCK TABLE ONLY "{schema}".page IN ACCESS EXCLUSIVE MODE NOWAIT'))
            await connection.execute(text(f'ALTER TABLE "{schema}".page RENAME TO page_previous'))
            await connection.execute(text(f'ALTER TABLE "{schema}".page_next RENAME TO page'))
        async with database.reader_session():
            assert await database.scalar(f'SELECT marker FROM "{schema}".page') == "new"


async def _native_cancelled_count(database, session, schema):
    """Cancel the client before its longer native statement deadline can expire."""
    async with asyncio.timeout(0.05):
        await _native_optional_count(database, session, schema, "statement_timeout", timeout_seconds=5)


@pytest.mark.asyncio
async def test_statement_adapter_methods(monkeypatch):
    db = Database()
    session = _FakeSession()

    @asynccontextmanager
    async def session_ctx():
        yield session

    monkeypatch.setattr(db, "session", session_ctx)

    adapter = db.select(1)
    assert isinstance(adapter, SelectAdapter)
    assert await adapter.scalar() == 42
    assert await adapter.first() == 42
    assert await adapter.all() == [42]

    insert_adapter = db.insert(Table("t", MetaData(), Column("id", Integer)))
    assert isinstance(insert_adapter, InsertAdapter)
    await insert_adapter.status()
    stmt, _ = session.executed[-1]
    assert "INSERT" in str(stmt)

    delete_adapter = db.delete(Table("d", MetaData(), Column("id", Integer)))
    assert isinstance(delete_adapter, DeleteAdapter)
    await delete_adapter.status()
    stmt, _ = session.executed[-1]
    assert "DELETE" in str(stmt)


@pytest.mark.asyncio
async def test_connection_proxy_helpers(monkeypatch):
    executed_calls = []

    class FakeConnection:
        async def execute(self, stmt, params=None):
            executed_calls.append((stmt, params))
            return _FakeResult(value=99, rowcount=3)

    proxy = ConnectionProxy(SimpleNamespace(), FakeConnection(), SimpleNamespace())
    assert await proxy.all("SELECT 1") == [99]
    assert await proxy.first("SELECT 1") == 99
    assert await proxy.scalar("SELECT 1") == 99
    assert await proxy.status("DELETE") == 3
    assert executed_calls


@pytest.mark.asyncio
async def test_acquire_driver_avoids_managed_transaction_and_invalidates_on_error():
    driver_connection = SimpleNamespace()

    class FakeConnection:
        def __init__(self):
            self.invalidated = False

        async def get_raw_connection(self):
            return SimpleNamespace(driver_connection=driver_connection)

        async def invalidate(self):
            self.invalidated = True

    connection = FakeConnection()

    class ConnectContext:
        async def __aenter__(self):
            return connection

        async def __aexit__(self, exc_type, exc, tb):
            return False

    engine = SimpleNamespace(connect=lambda: ConnectContext())
    database = Database(engine=engine)

    with pytest.raises(RuntimeError, match="copy failed"):
        async with database.acquire_driver() as acquired:
            assert acquired is driver_connection
            raise RuntimeError("copy failed")

    assert connection.invalidated


@pytest.mark.asyncio
async def test_create_table_and_execute_ddl(monkeypatch):
    table = Table("things", MetaData(), Column("id", Integer))

    run_calls_by_name = {}

    class FakeBeginCtx:
        async def __aenter__(self):
            class Runner:
                async def run_sync(self_inner, fn, **kw):
                    run_calls_by_name["run"] = run_calls_by_name.get("run", 0) + 1

            return Runner()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def __init__(self):
            self.begin = lambda: FakeBeginCtx()

        def connect(self):
            class _Conn:
                async def __aenter__(self_inner):
                    return self_inner

                async def __aexit__(self_inner, exc_type, exc, tb):
                    return False

                async def execution_options(self_inner, **kwargs):
                    self_inner.kw = kwargs
                    return self_inner

                async def exec_driver_sql(self_inner, statement):
                    run_calls_by_name["ddl"] = statement

            return _Conn()

    db = Database(engine=FakeEngine())
    await db.create_table(table)
    assert run_calls_by_name.get("run") == 1

    await db.execute_ddl("VACUUM")
    assert run_calls_by_name["ddl"] == "VACUUM"


@pytest.fixture
def table_creation_database():
    connection = SimpleNamespace(
        dialect=dialect(),
        scalar=AsyncMock(return_value=True),
        exec_driver_sql=AsyncMock(),
        run_sync=AsyncMock(),
    )

    @asynccontextmanager
    async def begin():
        yield connection

    return Database(engine=SimpleNamespace(begin=begin)), connection


@pytest.mark.asyncio
async def test_create_table_skips_existing_schema_ddl(table_creation_database):
    database, connection = table_creation_database
    table = Table("things", MetaData(), Column("id", Integer), schema="tenant")
    connection.exec_driver_sql.side_effect = PermissionError("schema creation denied")

    await database.create_table(table, checkfirst=True)

    statement, parameters = connection.scalar.await_args.args
    assert str(statement) == "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema)"
    assert parameters == {"schema": "tenant"}
    connection.exec_driver_sql.assert_not_awaited()
    connection.run_sync.assert_awaited_once_with(table.create, checkfirst=True)


@pytest.mark.asyncio
async def test_create_table_creates_quoted_missing_schema(table_creation_database):
    database, connection = table_creation_database
    table = Table("things", MetaData(), Column("id", Integer), schema='tenant"area')
    connection.scalar.return_value = False

    await database.create_table(table, checkfirst=False)

    assert connection.scalar.await_args.args[1] == {"schema": 'tenant"area'}
    connection.exec_driver_sql.assert_awaited_once_with('CREATE SCHEMA IF NOT EXISTS "tenant""area"')
    connection.run_sync.assert_awaited_once_with(table.create, checkfirst=False)


@pytest.mark.asyncio
async def test_create_table_without_schema_skips_namespace_check(table_creation_database):
    database, connection = table_creation_database
    table = Table("things", MetaData(), Column("id", Integer))

    await database.create_table(table, checkfirst=True)

    connection.scalar.assert_not_awaited()
    connection.exec_driver_sql.assert_not_awaited()
    connection.run_sync.assert_awaited_once_with(table.create, checkfirst=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_operation", ["scalar", "exec_driver_sql", "run_sync"])
async def test_create_table_propagates_errors(table_creation_database, failed_operation):
    database, connection = table_creation_database
    table = Table("things", MetaData(), Column("id", Integer), schema="tenant")
    connection.scalar.return_value = False
    error = PermissionError("creation denied")
    getattr(connection, failed_operation).side_effect = error

    with pytest.raises(PermissionError) as raised:
        await database.create_table(table, checkfirst=True)

    assert raised.value is error
    if failed_operation == "scalar":
        connection.exec_driver_sql.assert_not_awaited()
    if failed_operation != "run_sync":
        connection.run_sync.assert_not_awaited()


@pytest.mark.asyncio
async def test_app_startup_prewarms_configured_pool(monkeypatch):
    listeners_by_name = {}

    class App:
        def listener(self, name):
            def register(function):
                listeners_by_name[name] = function
                return function

            return register

        def middleware(self, _name):
            return lambda function: function

    class Connection:
        async def __aenter__(self):
            engine.opened += 1
            engine.active += 1
            engine.peak = max(engine.peak, engine.active)
            return self

        async def __aexit__(self, *_args):
            engine.active -= 1

    engine = SimpleNamespace(
        active=0,
        opened=0,
        peak=0,
        pool=SimpleNamespace(size=lambda: 4),
        connect=lambda: Connection(),
    )
    database = Database(engine=engine)
    connect = AsyncMock()
    monkeypatch.setattr(database, "connect", connect)

    database.init_app(App())
    await listeners_by_name["before_server_start"](None, None)

    connect.assert_awaited_once_with()
    assert engine.opened == 4
    assert engine.peak == 4
    assert engine.active == 0
