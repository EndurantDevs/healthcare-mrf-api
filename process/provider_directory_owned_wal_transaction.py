# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain one registry publication connection through commit and WAL sampling.

Only trusted, prepared callbacks use the existing borrowed-session bridge.
This owner changes no signed cap, global physical guard or accounting ledger.
Incomplete outcomes release no exposure; classification belongs to the caller.
"""

import asyncio
import contextvars
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from process.provider_directory_backend_wal_diagnostic import (
    BackendWalDiagnosticUnavailable,
    _result,
    begin_backend_wal_diagnostic,
    finish_backend_wal_diagnostic,
    sample_backend_wal_owner_boundary,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

_SETTINGS = (
    "max_parallel_workers",
    "max_parallel_workers_per_gather",
    "max_parallel_maintenance_workers",
    "debug_parallel_query",
    "search_path",
)
_READ_SETTINGS = "SELECT " + ",".join(f"pg_catalog.current_setting('{name}') AS {name}" for name in _SETTINGS)
_SET_SETTING = "SELECT pg_catalog.set_config($1,$2,false)"
_ACTIVE_OWNED_TRANSACTION = contextvars.ContextVar("registry_owned_wal_transaction", default=None)


def current_owned_wal_transaction(database):
    """Return only this task's actual containing native owner."""
    current = _ACTIVE_OWNED_TRANSACTION.get()
    if current is None or current[0] is not database or current[1] is not asyncio.current_task():
        return None
    binding = database._transaction_binding()
    if binding is None or binding.session is not current[2].session:
        return None
    return current[2]


@dataclass
class OwnedWalTransaction:
    session: object | None = None
    commit_state: str = "not_attempted"
    status: str = "accounting_incomplete"
    measurement: object | None = None
    owner_measurement: object | None = None
    owner_measurement_complete: bool = False
    cleanup_complete: bool = False
    connection: object | None = None
    retained_driver: object | None = None
    retained_pid: int | None = None

    @property
    def is_committed(self):
        """Report only a confirmed top-level commit."""
        return self.commit_state == "confirmed"


class OwnedWalTransactionError(RuntimeError):
    def __init__(self, outcome):
        super().__init__("provider_directory_owned_wal_transaction_incomplete")
        self.outcome = outcome


class OwnedWalTransactionCancelled(asyncio.CancelledError):
    def __init__(self, outcome):
        super().__init__("provider_directory_owned_wal_transaction_cancelled")
        self.outcome = outcome


def _driver_identity(driver, pid=None, *, outside_transaction=False):
    if (
        driver.is_closed() is not False
        or type(driver.get_server_pid()) is not int
        or driver.get_server_pid() <= 0
        or pid is not None
        and driver.get_server_pid() != pid
        or outside_transaction
        and driver.is_in_transaction() is not False
    ):
        raise RuntimeError("provider_directory_owned_wal_custody_changed")
    return driver.get_server_pid()


async def _settings(driver, pid):
    _driver_identity(driver, pid, outside_transaction=True)
    row = await driver.fetchrow(_READ_SETTINGS)
    _driver_identity(driver, pid, outside_transaction=True)
    if row is None or any(type(row[name]) is not str for name in _SETTINGS):
        raise RuntimeError("provider_directory_owned_wal_settings_unavailable")
    return {name: row[name] for name in _SETTINGS}


async def _set_settings(driver, pid, settings):
    for name, value in settings.items():
        _driver_identity(driver, pid, outside_transaction=True)
        await driver.execute(_SET_SETTING, name, value)
        _driver_identity(driver, pid, outside_transaction=True)
    if await _settings(driver, pid) != settings:
        raise RuntimeError("provider_directory_owned_wal_settings_changed")


async def _set_owned_settings(driver, pid):
    """Apply the same isolated native settings to either transaction owner."""
    await _set_settings(
        driver,
        pid,
        {
            "max_parallel_workers": "0",
            "max_parallel_workers_per_gather": "0",
            "max_parallel_maintenance_workers": "0",
            "debug_parallel_query": "off",
            "search_path": "pg_catalog, pg_temp",
        },
    )


async def _commit(session, outcome):
    outcome.commit_state = "attempted"
    await session.commit()
    outcome.commit_state = "confirmed"
    outcome.status = "committed_accounting_incomplete"


def _incomplete(outcome):
    outcome.measurement = None
    outcome.status = (
        "committed_accounting_incomplete"
        if outcome.is_committed
        else "commit_uncertain_accounting_incomplete"
        if outcome.commit_state in {"attempted", "uncertain"}
        else "accounting_incomplete"
    )


def _terminate_owned_driver(driver):
    """Confirm the original native endpoint is terminal without fetching another."""
    if driver is None:
        raise RuntimeError("provider_directory_owned_wal_retirement_incomplete")
    if driver.is_closed() is not True:
        driver.terminate()
    if driver.is_closed() is not True:
        raise RuntimeError("provider_directory_owned_wal_retirement_incomplete")


async def _retire_owned_connection(connection, driver, outcome):
    """Verify native termination before any explicit wrapper pool check-in."""
    _terminate_owned_driver(driver)
    if not connection.closed:
        await connection.invalidate()
        await connection.close()
    outcome.cleanup_complete = True


async def _retire_preserving_failure(connection, driver, outcome, failure):
    """Retain the first failure while leaving unresolved custody incomplete."""
    try:
        await _retire_owned_connection(connection, driver, outcome)
    except BaseException:
        failure.add_note("provider_directory_owned_wal_retirement_incomplete")


async def _restore_settings_or_retire(connection, driver, pid, saved, outcome):
    """Restore native settings or confirm retirement without replacing failure."""
    try:
        await _set_settings(driver, pid, saved)
    except BaseException as failure:
        await _retire_preserving_failure(connection, driver, outcome, failure)
        raise


async def _sample_owner_boundary(connection, driver, pid):
    try:
        if connection.closed or connection.invalidated:
            return None
        if (await connection.get_raw_connection()).driver_connection is not driver:
            return None
        return await sample_backend_wal_owner_boundary(driver, pid)
    except BackendWalDiagnosticUnavailable:
        return None


def _retain_owner_measurement(owner, baseline, final_samples, outcome):
    """Close the authorized drained cleanup interval under its original task.

    Checkout, final sampler self-tail and release remain unknown exposure;
    owner_measurement_complete stays false and grants no accounting authority.
    """
    if asyncio.current_task() is not owner or baseline is None or not final_samples or final_samples[0] is None:
        return
    try:
        if outcome.measurement is not None:
            _result(baseline, outcome.measurement.baseline)
            _result(outcome.measurement.final, final_samples[0])
        outcome.owner_measurement = _result(baseline, final_samples[0])
    except BackendWalDiagnosticUnavailable:
        outcome.owner_measurement = None


async def _finish(connection, session, driver, pid, saved, outcome, has_failed, final_samples):
    """Drain cleanup only after failed native custody is terminal."""
    try:
        if session is not None:
            if session.in_transaction():
                await session.rollback()
                if outcome.commit_state == "not_attempted":
                    outcome.commit_state = "rolled_back"
            if has_failed:
                _terminate_owned_driver(driver)
            await session.close()
        if has_failed:
            await _retire_owned_connection(connection, driver, outcome)
        elif connection.sync_connection is not None:
            if saved is not None:
                await _restore_settings_or_retire(connection, driver, pid, saved, outcome)
            final_samples.append(await _sample_owner_boundary(connection, driver, pid))
            await connection.close()
        outcome.cleanup_complete = True
    except BaseException as failure:
        outcome.measurement = None
        await _retire_preserving_failure(connection, driver, outcome, failure)
        raise


def _owned_transaction_origin(database):
    """Require native, exclusive ownership before checking out a connection."""
    if (
        database.engine is None
        or database.has_reader_session()
        or database._transaction_binding() is not None
        or database._has_bound_request_session()
    ):
        raise RuntimeError("provider_directory_owned_wal_requires_own_transaction")
    engine = database.engine
    if engine.dialect.name != "postgresql" or engine.dialect.driver != "asyncpg":
        raise RuntimeError("provider_directory_owned_wal_requires_native_driver")
    owner = asyncio.current_task()
    if owner is None:
        raise RuntimeError("provider_directory_owned_wal_requires_task")
    return engine, owner


async def _commit_and_measure(connection, session, driver, pid, owner, observation, outcome):
    """Commit and sample under the unchanged task and native connection."""
    if asyncio.current_task() is not owner or session.in_nested_transaction():
        raise RuntimeError("provider_directory_owned_wal_owner_changed")
    if connection.closed or connection.invalidated:
        outcome.commit_state = "uncertain"
        raise RuntimeError("provider_directory_owned_wal_custody_changed")
    if (await connection.get_raw_connection()).driver_connection is not driver:
        raise RuntimeError("provider_directory_owned_wal_custody_changed")
    _driver_identity(driver, pid)
    await drain_operation(_commit(session, outcome), preserve_cancellation=True)
    _driver_identity(driver, pid, outside_transaction=True)
    outcome.measurement = await finish_backend_wal_diagnostic(observation)
    outcome.status = "committed_measured"


@asynccontextmanager
async def registry_owned_wal_transaction(database):
    """Own commit/rollback and retain incomplete outcomes without refunds."""
    engine, owner = _owned_transaction_origin(database)
    connection = engine.connect()
    outcome = OwnedWalTransaction(connection=connection)
    session = driver = saved = None
    pid = None
    failure = None
    owner_baseline = None
    final_samples = []
    try:
        await drain_operation(connection.start(), preserve_cancellation=True)
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        pid = _driver_identity(driver, outside_transaction=True)
        outcome.retained_driver, outcome.retained_pid = driver, pid
        owner_baseline = await _sample_owner_boundary(connection, driver, pid)
        saved = await _settings(driver, pid)
        await _set_owned_settings(driver, pid)
        observation = await begin_backend_wal_diagnostic(driver)
        session = AsyncSession(bind=connection, expire_on_commit=False, autoflush=False)
        outcome.session = session
        await session.begin()
        try:
            async with database.bind_existing_session(session):
                token = _ACTIVE_OWNED_TRANSACTION.set((database, owner, outcome))
                try:
                    yield outcome
                finally:
                    _ACTIVE_OWNED_TRANSACTION.reset(token)
        except BaseException:
            if not session.in_transaction():
                outcome.commit_state = "uncertain"
            raise
        await _commit_and_measure(connection, session, driver, pid, owner, observation, outcome)
    except BaseException as error:
        failure = error
        _incomplete(outcome)
    finally:
        try:
            await drain_operation(
                _finish(connection, session, driver, pid, saved, outcome, failure is not None, final_samples),
                preserve_cancellation=True,
            )
        except BaseException as error:
            if failure is None:
                failure = error
            _incomplete(outcome)
    _retain_owner_measurement(owner, owner_baseline, final_samples, outcome)
    if failure is not None:
        error_type = (
            OwnedWalTransactionCancelled if isinstance(failure, asyncio.CancelledError) else OwnedWalTransactionError
        )
        raise error_type(outcome) from failure


def _retained_transaction_origin(connection):
    """Require one already-open native connection outside caller work."""
    from sqlalchemy.ext.asyncio import AsyncConnection

    if (
        not isinstance(connection, AsyncConnection)
        or connection.closed
        or connection.invalidated
        or connection.in_transaction()
        or connection.in_nested_transaction()
    ):
        raise RuntimeError("provider_directory_owned_wal_requires_own_transaction")
    if connection.dialect.name != "postgresql" or connection.dialect.driver != "asyncpg":
        raise RuntimeError("provider_directory_owned_wal_requires_native_driver")
    owner = asyncio.current_task()
    if owner is None:
        raise RuntimeError("provider_directory_owned_wal_requires_task")
    return owner


async def _finish_retained(connection, driver, pid, saved, outcome, has_failed, final_samples):
    """Keep healthy caller custody and verify failed native retirement first."""
    try:
        if connection.in_transaction():
            await connection.rollback()
            if outcome.commit_state == "not_attempted":
                outcome.commit_state = "rolled_back"
        if has_failed:
            await _retire_owned_connection(connection, driver, outcome)
        elif saved is not None:
            if connection.closed or connection.invalidated:
                raise RuntimeError("provider_directory_owned_wal_custody_changed")
            if (await connection.get_raw_connection()).driver_connection is not driver:
                raise RuntimeError("provider_directory_owned_wal_custody_changed")
            await _restore_settings_or_retire(connection, driver, pid, saved, outcome)
            final_samples.append(await _sample_owner_boundary(connection, driver, pid))
        outcome.cleanup_complete = True
    except BaseException as failure:
        outcome.measurement = None
        await _retire_preserving_failure(connection, driver, outcome, failure)
        raise


async def _begin_retained(connection):
    """Await the native AsyncTransaction without passing it to create_task."""
    await connection.begin()


@asynccontextmanager
async def registry_retained_wal_transaction(connection):
    """Own one existing native connection's transaction without global binding.

    The caller retains a healthy connection after final sampling/restoration.
    Enter at the existing begin boundary and exit at its existing release;
    no complete outcome is available while the work transaction remains open.
    """
    owner = _retained_transaction_origin(connection)
    outcome = OwnedWalTransaction(connection=connection)
    driver = saved = None
    pid = None
    failure = None
    owner_baseline = None
    final_samples = []
    try:
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        pid = _driver_identity(driver, outside_transaction=True)
        outcome.retained_driver, outcome.retained_pid = driver, pid
        owner_baseline = await _sample_owner_boundary(connection, driver, pid)
        saved = await _settings(driver, pid)
        await _set_owned_settings(driver, pid)
        observation = await begin_backend_wal_diagnostic(driver)
        await drain_operation(_begin_retained(connection), preserve_cancellation=True)
        try:
            yield outcome
        except BaseException:
            if not connection.in_transaction():
                outcome.commit_state = "uncertain"
            raise
        if connection.in_transaction() is not True or driver.is_in_transaction() is not True:
            outcome.commit_state = "uncertain"
            raise RuntimeError("provider_directory_owned_wal_custody_changed")
        if connection.closed or connection.invalidated:
            outcome.commit_state = "uncertain"
            raise RuntimeError("provider_directory_owned_wal_custody_changed")
        _driver_identity(driver, pid)
        await _commit_and_measure(connection, connection, driver, pid, owner, observation, outcome)
    except BaseException as error:
        failure = error
        _incomplete(outcome)
    finally:
        try:
            await drain_operation(
                _finish_retained(connection, driver, pid, saved, outcome, failure is not None, final_samples),
                preserve_cancellation=True,
            )
        except BaseException as error:
            if failure is None:
                failure = error
            _incomplete(outcome)
    _retain_owner_measurement(owner, owner_baseline, final_samples, outcome)
    if failure is not None:
        error_type = (
            OwnedWalTransactionCancelled if isinstance(failure, asyncio.CancelledError) else OwnedWalTransactionError
        )
        raise error_type(outcome) from failure
