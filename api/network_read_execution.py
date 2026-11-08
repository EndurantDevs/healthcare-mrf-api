# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify executed address statements before their rows reach callers or caches."""

import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import SimpleNamespace

import asyncpg
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.exc import DBAPIError, InterfaceError

from db.connection import ConnectionProxy
from process.entity_address_cutover_contract import postgres_sqlstate
from process.network_compact_layout import CompactAddressRelation, compact_identifier
from process.network_read_identity import (
    NetworkReadIdentityError,
    _bound_name_pins,
    _relation_pins,
    verify_bound_network_names,
    verify_consumed_network_relations,
)

_MARKER_PREFIX = "network-read:"
_MARKER = re.compile(r"/\* network-read:([0-9a-f]{64}) \*/")
_NAME_INVENTORIES = ContextVar("network_read_name_inventories", default=())
_SESSION_INVENTORY_KEY = "api_network_read_inventory"
_SQL_FAILURE_ISSUER = object()


@dataclass(frozen=True)
class _NameInventory:
    connection: object
    relations: tuple[CompactAddressRelation, ...]
    backend_pid: int
    absent_names: tuple[tuple[str, str], ...] = ()
    only_relations: tuple[CompactAddressRelation, ...] = ()


@dataclass(frozen=True)
class _SessionInventory:
    inventory: _NameInventory
    transaction: object


@dataclass(frozen=True)
class _ReadSQLContext:
    session: object
    transaction: object
    connection: object
    backend_pid: int
    inventories: tuple
    context_inventories: tuple
    installed: object
    statement: object
    relations: tuple[CompactAddressRelation, ...]
    scope: object
    read_statements: object
    savepoint: object


class _ReadSQLFailure(ServiceUnavailable):
    """An actual guarded database query failed before any result was available."""

    def __init__(self, context, original, issuer):
        super().__init__("Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"})
        self.context, self.original = context, original
        self.issuer = issuer
        self.recovery_started = False


def _read_sql_failure_cause(error, session):
    """Expose only an issued SQL failure for retry after its read scope has closed."""
    if (
        type(error) is _ReadSQLFailure
        and error.issuer is _SQL_FAILURE_ISSUER
        and type(error.context) is _ReadSQLContext
        and error.context.session is session
        and isinstance(error.original, (DBAPIError, asyncpg.PostgresError))
    ):
        return error.original
    return None


@dataclass(frozen=True)
class _OptionalReadRollback:
    failure: _ReadSQLFailure
    savepoint: object
    parent_savepoint: object
    issuer: object


def _installed_read_inventory(session):
    metadata = getattr(session, "info", {})
    if _SESSION_INVENTORY_KEY not in metadata:
        return None
    installed = metadata[_SESSION_INVENTORY_KEY]
    if type(installed) is not _SessionInventory or session.get_transaction() is not installed.transaction:
        raise _unavailable()
    return installed.inventory


def direct_heap_relation_sql(session, schema, table):
    """Render one fixed FROM target; only independently proved pins exclude children."""
    qualified = compact_identifier(schema) + "." + compact_identifier(table)
    installed = _installed_read_inventory(session)
    if installed is not None and any(
        (pin.schema_name, pin.table_name) == (schema, table) for pin in installed.only_relations
    ):
        return "ONLY " + qualified
    return qualified


async def install_network_read_inventory(session, relations, *, absent_names=(), only_relations=()):
    """Verify names and bind subsequent buffered reads to this exact session/transaction.

    Native authority is established by the caller. Installation grants no Source
    or serving authority; request cleanup must remove the private session entry.
    """
    try:
        driver, backend_pid = await _original_read_driver(session)
        transaction = session.get_transaction()
        if transaction is None or driver.is_in_transaction() is not True:
            raise _unavailable()
        _bound_name_pins(relations, absent_names, backend_pid)
        _relation_pins(only_relations, backend_pid)
        previous = _installed_read_inventory(session)
        if previous is not None:
            if previous.connection is not driver or previous.backend_pid != backend_pid:
                raise _unavailable()
            relations = (*previous.relations, *relations)
            absent_names = (*previous.absent_names, *absent_names)
            only_relations = tuple(dict.fromkeys((*previous.only_relations, *only_relations)))
        _bound_name_pins(relations, absent_names, backend_pid)
        if not set(only_relations).issubset(relations):
            raise _unavailable()
        await verify_bound_network_names(driver, relations, expected_backend_pid=backend_pid, absent_names=absent_names)
        current_driver, current_pid = await _original_read_driver(session)
        if (
            current_driver is not driver
            or current_pid != backend_pid
            or session.get_transaction() is not transaction
            or driver.is_in_transaction() is not True
        ):
            raise _unavailable()
    except Exception as error:
        raise _unavailable() from error
    session.info[_SESSION_INVENTORY_KEY] = _SessionInventory(
        _NameInventory(driver, relations, backend_pid, absent_names, only_relations), transaction
    )


@contextmanager
def network_read_name_inventory(connection, relations, *, absent_names=()):
    """Bind a trusted complete fixed-name inventory, without acquiring any locks.

    Setup must establish native authority independently. Only guarded buffered
    execute adapters use this inventory; entering a scope proves no consumption.
    """
    if not relations and not absent_names:
        if type(relations) is not tuple or type(absent_names) is not tuple:
            raise _unavailable()
        yield
        return
    try:
        backend_pid = connection.get_server_pid()
        _bound_name_pins(relations, absent_names, backend_pid)
    except Exception as error:
        raise _unavailable() from error
    inventory = _NameInventory(connection, relations, backend_pid, absent_names)
    token = _NAME_INVENTORIES.set((*_NAME_INVENTORIES.get(), inventory))
    try:
        yield
    finally:
        _NAME_INVENTORIES.reset(token)


def has_network_read_guard(statement):
    """Require the original session when an executed marker or fixed inventory is bound."""
    return bool(network_read_relations(statement) or _NAME_INVENTORIES.get())


def has_network_read_inventory(session=None):
    """Inspect fixed-name scope without compiling or inspecting a statement."""
    return bool(_NAME_INVENTORIES.get() or _SESSION_INVENTORY_KEY in getattr(session, "info", {}))


def _unavailable():
    return ServiceUnavailable("Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"})


def _read_context_unchanged(context):
    """Verify the captured identity and scopes without selecting a new baseline."""
    from api.network_address_scope import current_network_address_scope

    session, driver = context.session, context.connection
    scope = current_network_address_scope()
    if (
        session.get_transaction() is not context.transaction
        or session.in_transaction() is not True
        or driver.is_in_transaction() is not True
        or driver.get_server_pid() != context.backend_pid
        or _NAME_INVENTORIES.get() is not context.context_inventories
        or getattr(session, "info", {}).get(_SESSION_INVENTORY_KEY) is not context.installed
        or scope is not context.scope
        or (scope is not None and scope.read_statements is not context.read_statements)
        or network_read_relations(context.statement) != context.relations
    ):
        raise _unavailable()


async def _read_sql_context(session, statement, relations, inventories):
    """Capture the original transaction before the first driver-acquisition await."""
    from api.network_address_scope import current_network_address_scope

    transaction = session.get_transaction()
    context_inventories = _NAME_INVENTORIES.get()
    installed = getattr(session, "info", {}).get(_SESSION_INVENTORY_KEY)
    scope = current_network_address_scope()
    read_statements = scope.read_statements if scope is not None else None
    savepoint = session.get_nested_transaction() if hasattr(session, "get_nested_transaction") else None
    driver, backend_pid = await _original_read_driver(session)
    if (
        any(inventory.connection is not driver or inventory.backend_pid != backend_pid for inventory in inventories)
        or installed is not None
        and installed.transaction is not transaction
    ):
        raise _unavailable()
    context = _ReadSQLContext(
        session,
        transaction,
        driver,
        backend_pid,
        inventories,
        context_inventories,
        installed,
        statement,
        relations,
        scope,
        read_statements,
        savepoint,
    )
    _read_context_unchanged(context)
    return context


def _optional_database_error(error):
    """Only statement errors on a live PostgreSQL connection can be reconsidered."""
    original = getattr(error, "orig", None)
    if any(
        isinstance(
            candidate, (InterfaceError, asyncpg.InterfaceError, asyncpg.PostgresConnectionError, ConnectionError)
        )
        for candidate in (error, original, error.__cause__, getattr(original, "__cause__", None))
    ):
        return False
    if isinstance(error, DBAPIError):
        if isinstance(error, InterfaceError) or error.connection_invalidated:
            return False
    elif not isinstance(error, asyncpg.PostgresError) or isinstance(error, asyncpg.PostgresConnectionError):
        return False
    state = postgres_sqlstate(error)
    return bool(
        state
        and re.fullmatch(r"[0-9A-Z]{5}", state)
        and not state.startswith("08")
        and state not in {"57P01", "57P02", "57P03"}
    )


async def _rollback_optional_read(session, failure, savepoint, transaction, parent_savepoint):
    """Issue recovery evidence only after rolling back this exact owned savepoint."""
    if (
        type(failure) is not _ReadSQLFailure
        or failure.issuer is not _SQL_FAILURE_ISSUER
        or not _optional_database_error(failure.original)
    ):
        raise failure
    context = failure.context
    if (
        context.session is not session
        or context.transaction is not transaction
        or context.savepoint is not savepoint
        or savepoint is None
        or savepoint is transaction
        or getattr(savepoint, "nested", None) is not True
        or savepoint.is_active is not True
        or session.get_nested_transaction() is not savepoint
    ):
        raise _unavailable() from failure
    _read_context_unchanged(context)
    try:
        await savepoint.rollback()
    except Exception as error:
        raise _unavailable() from error
    if savepoint.is_active is not False:
        raise _unavailable() from failure
    return _OptionalReadRollback(failure, savepoint, parent_savepoint, _SQL_FAILURE_ISSUER)


async def _recover_optional_read(session, rollback):
    """Revalidate names after nested rollback; failed SQL proves no consumption."""
    try:
        if (
            type(rollback) is not _OptionalReadRollback
            or rollback.issuer is not _SQL_FAILURE_ISSUER
            or type(rollback.failure) is not _ReadSQLFailure
            or rollback.failure.issuer is not _SQL_FAILURE_ISSUER
        ):
            raise _unavailable()
        if rollback.failure.recovery_started:
            raise _unavailable()
        rollback.failure.recovery_started = True
        context = rollback.failure.context
        if (
            type(context) is not _ReadSQLContext
            or context.session is not session
            or context.savepoint is not rollback.savepoint
            or rollback.savepoint.is_active is not False
            or session.get_nested_transaction() is not rollback.parent_savepoint
        ):
            raise _unavailable()
        _read_context_unchanged(context)
        driver, pid = await _original_read_driver(session)
        if driver is not context.connection or pid != context.backend_pid:
            raise _unavailable()
        if context.relations:
            await verify_bound_network_names(driver, context.relations, expected_backend_pid=pid)
        for inventory in context.inventories:
            await verify_bound_network_names(
                driver,
                inventory.relations,
                expected_backend_pid=inventory.backend_pid,
                absent_names=inventory.absent_names,
            )
        driver, pid = await _original_read_driver(session)
        if driver is not context.connection or pid != context.backend_pid:
            raise _unavailable()
        _read_context_unchanged(context)
    except Exception as error:
        raise _unavailable() from error
    return rollback.failure.original


class NetworkReadStatements:
    """Possible server-generated statements; registration alone proves no consumption."""

    def __init__(self, scope_key):
        self.scope_key = scope_key
        self.relations_by_marker = {}

    def marker(self, relations):
        """Identify a possible server-generated query without claiming it was executed."""
        try:
            _relation_pins(relations, 1)
        except Exception as error:
            raise _unavailable() from error
        pins = tuple(
            sorted(set(relations), key=lambda relation: (relation.schema_name, relation.table_name, relation.table_oid))
        )
        payload = [self.scope_key, [(pin.schema_name, pin.table_name, pin.table_oid) for pin in pins]]
        identity = hashlib.sha256(json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
        self.relations_by_marker[identity] = pins
        return "/* network-read:" + identity + " */"


def network_read_relations(statement):
    """Find only markers in executed SQL, rejecting unknown or stale scoped statements."""
    from api.network_address_scope import current_network_address_scope

    sql = statement if type(statement) is str else str(statement.compile(dialect=PGDialect_asyncpg()))
    if _MARKER_PREFIX not in sql:
        return ()
    markers = _MARKER.findall(sql)
    if not markers or sql.count(_MARKER_PREFIX) != len(markers):
        raise _unavailable()
    scope = current_network_address_scope()
    if scope is None or scope.read_statements is None:
        raise _unavailable()
    relations = set()
    for marker in markers:
        pins = scope.read_statements.relations_by_marker.get(marker)
        if pins is None:
            raise _unavailable()
        relations.update(pins)
    return tuple(
        sorted(relations, key=lambda relation: (relation.schema_name, relation.table_name, relation.table_oid))
    )


async def execute_network_read(session, statement, parameters=None):
    """Execute and guard within the original borrowed session, before returning any rows."""
    inventories = _NAME_INVENTORIES.get()
    installed = _installed_read_inventory(session)
    if installed is not None:
        inventories = (*inventories, installed)
        if installed.only_relations:
            from api.network_direct_heap_reads import direct_heap_read_statement

            statement = direct_heap_read_statement(statement, installed.only_relations)
    relations = network_read_relations(statement)
    statement = text(statement) if type(statement) is str else statement
    if not relations and not inventories:
        return await session.execute(statement, parameters or {})
    context = await _read_sql_context(session, statement, relations, inventories)
    transaction, driver, backend_pid = context.transaction, context.connection, context.backend_pid
    try:
        buffered_result = await session.execute(statement, parameters or {})
    except Exception as error:
        if isinstance(error, (DBAPIError, asyncpg.PostgresError)):
            raise _ReadSQLFailure(context, error, _SQL_FAILURE_ISSUER) from error
        raise _unavailable() from error
    try:
        if session.get_transaction() is not transaction or driver.is_in_transaction() is not True:
            raise _unavailable()
        await _verify_read_inventories(driver, backend_pid, relations, inventories)
        current_driver, current_pid = await _original_read_driver(session)
        if current_driver is not driver or current_pid != backend_pid or session.get_transaction() is not transaction:
            raise _unavailable()
        _read_context_unchanged(context)
    except BaseException as error:
        try:
            buffered_result.close()
        except BaseException as close_error:
            error.add_note("Buffered result cleanup failed: " + type(close_error).__name__)
        if isinstance(error, NetworkReadIdentityError):
            raise _unavailable() from error
        raise
    return buffered_result


async def _verify_read_inventories(driver, backend_pid, relations, inventories):
    if relations:
        await verify_consumed_network_relations(driver, relations, expected_backend_pid=backend_pid)
    for inventory in inventories:
        await verify_bound_network_names(
            driver,
            inventory.relations,
            expected_backend_pid=inventory.backend_pid,
            **({"absent_names": inventory.absent_names} if inventory.absent_names else {}),
        )


async def _guarded_native_fetchrow(session, query, parameters):
    """Keep an opaque native result private until its original read context is verified."""
    installed = _installed_read_inventory(session)
    if installed is None:
        raise _unavailable()
    inventories = (*_NAME_INVENTORIES.get(), installed)
    relations = network_read_relations(query)
    context = await _read_sql_context(session, query, relations, inventories)
    transaction, driver, backend_pid = context.transaction, context.connection, context.backend_pid
    try:
        buffered = await driver.fetchrow(query, *parameters)
    except Exception as error:
        if isinstance(error, (DBAPIError, asyncpg.PostgresError)):
            raise _ReadSQLFailure(context, error, _SQL_FAILURE_ISSUER) from error
        raise _unavailable() from error
    try:
        await _verify_read_inventories(driver, backend_pid, relations, inventories)
        current_driver, current_pid = await _original_read_driver(session)
        if (
            session.get_transaction() is not transaction
            or current_driver is not driver
            or current_pid != backend_pid
            or driver.is_in_transaction() is not True
        ):
            raise _unavailable()
        _read_context_unchanged(context)
    except NetworkReadIdentityError as error:
        raise _unavailable() from error
    return buffered


async def bounded_network_read_rows(session, query, *parameters):
    """Reuse aggregate bounds while guarding the real native result before decoding.

    The installed inventory records physical identity; callers establish Source
    and serving authority independently before installing it.
    """
    from api.provider_service_source_policy import _bounded_rows

    async def fetchrow(statement, *arguments):
        """Guard the original native query before the existing decoder receives it."""
        return await _guarded_native_fetchrow(session, statement, arguments)

    return await _bounded_rows(SimpleNamespace(fetchrow=fetchrow), query, *parameters)


async def _original_read_driver(session):
    try:
        if session is None or session.in_transaction() is not True:
            raise _unavailable()
        transaction = session.get_transaction()
        if transaction is None:
            raise _unavailable()
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        backend_pid = driver.get_server_pid()
        if (
            type(backend_pid) is not int
            or not 0 < backend_pid <= 2147483647
            or session.get_transaction() is not transaction
            or session.in_transaction() is not True
            or driver.is_in_transaction() is not True
        ):
            raise _unavailable()
        return driver, backend_pid
    except Exception as error:
        raise _unavailable() from error


class NetworkReadConnectionProxy(ConnectionProxy):
    """Keep buffered proxy reads and their identity check inside the request transaction."""

    async def all(self, statement, **parameters):
        """Verify the buffered query before returning its rows."""
        return (await execute_network_read(self._connection, statement, parameters)).all()

    async def first(self, statement, **parameters):
        """Verify the buffered query before returning its first row."""
        return (await execute_network_read(self._connection, statement, parameters)).first()

    async def scalar(self, statement, **parameters):
        """Verify the buffered query before returning its first scalar."""
        return (await execute_network_read(self._connection, statement, parameters)).scalar()
