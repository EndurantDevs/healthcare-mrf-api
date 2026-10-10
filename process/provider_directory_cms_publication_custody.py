# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original CMS publication owners; unavailable native observations stay held."""

import asyncio
from contextlib import asynccontextmanager, nullcontext

from process import provider_directory_owned_wal_transaction as owned
from process.provider_directory_backend_wal_diagnostic import (
    BackendWalDiagnosticUnavailable,
    begin_backend_wal_diagnostic,
    finish_backend_wal_diagnostic,
)
from process.uhc_flex_practitioner_async_safety import drain_operation


def is_bounded(fhir):
    """Enable custody only for the existing bounded profile admission."""
    admission = getattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)()
    return getattr(getattr(admission, "geometry", None), "bounded_admission", False) is True


@asynccontextmanager
async def database_session(fhir):
    """Keep the existing default-pool owner and borrowed SAVEPOINT boundaries."""
    if not is_bounded(fhir):
        async with fhir.db.transaction() as session:
            yield session
        return
    async with fhir.profile_artifact_custody.cutover_transaction(fhir, "cms_default_pool", None):
        yield fhir.db._transaction_binding().session


@asynccontextmanager
async def source_body(fhir, factory, capture_id, session):
    """Retain the original source factory/session under its actual containing owner."""
    if not is_bounded(fhir):
        yield None
        return
    groups = []
    identity = (
        "cms_publication_source",
        factory,
        capture_id,
        session,
        fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get(),
    )
    async with fhir.profile_control_custody.transaction(
        fhir, nullcontext(), identity=identity, enabled=True, groups=groups
    ):
        group = groups[0]
        fhir._provider_directory_profile_capacity_admission().wal_tracker.owned_control_transaction_groups.append(group)
        yield group


async def _original_exit(original, outcome, driver, pid, failure, is_intact):
    """Record only the original context's acknowledged commit or rollback."""
    is_active = outcome.session.in_transaction() is True and driver.is_in_transaction() is True
    if failure is None:
        outcome.commit_state = "attempted" if is_intact and is_active else "uncertain"
    await original.__aexit__(
        type(failure) if failure is not None else None, failure, failure.__traceback__ if failure is not None else None
    )
    if not is_intact:
        return
    try:
        owned._driver_identity(driver, pid, outside_transaction=True)
    except RuntimeError:
        outcome.commit_state = "uncertain"
        return
    if failure is None and is_active:
        outcome.commit_state = "confirmed"
    elif failure is not None and outcome.commit_state == "not_attempted":
        outcome.commit_state = "rolled_back"


async def _begin_observation(driver):
    try:
        return await begin_backend_wal_diagnostic(driver)
    except BackendWalDiagnosticUnavailable:
        return None


async def _is_intact(connection, session, driver, pid, task):
    if task is not asyncio.current_task() or session.in_nested_transaction():
        return False
    if connection.closed or connection.invalidated:
        return False
    if (await connection.get_raw_connection()).driver_connection is not driver:
        return False
    try:
        owned._driver_identity(driver, pid)
    except RuntimeError:
        return False
    return True


async def _observe_terminal(observation, outcome):
    if observation is not None and outcome.is_committed:
        try:
            outcome.measurement = await finish_backend_wal_diagnostic(observation)
        except BackendWalDiagnosticUnavailable:
            outcome.measurement = None
    if outcome.measurement is not None:
        outcome.status = "committed_measured"
    else:
        owned._incomplete(outcome)


@asynccontextmanager
async def capture_transaction(fhir, connection, session, outcome):
    """Observe the retained source connection without changing its settings or transaction."""
    driver = (await connection.get_raw_connection()).driver_connection
    pid = owned._driver_identity(driver, outside_transaction=True)
    outcome.retained_driver, outcome.retained_pid = driver, pid
    observation = await _begin_observation(driver)
    original = session.begin()
    await drain_operation(original.__aenter__(), preserve_cancellation=True)
    task = asyncio.current_task()
    token = owned._ACTIVE_OWNED_TRANSACTION.set((fhir.db, task, outcome))
    failure = None
    try:
        try:
            yield session
        except BaseException as error:
            failure = error
        is_intact = False
        try:
            is_intact = await _is_intact(connection, session, driver, pid, task)
        except BaseException as error:
            if failure is None:
                failure = error
        await drain_operation(
            _original_exit(original, outcome, driver, pid, failure, is_intact), preserve_cancellation=True
        )
        if failure is not None:
            raise failure
        await _observe_terminal(observation, outcome)
    except BaseException:
        owned._incomplete(outcome)
        raise
    finally:
        owned._ACTIVE_OWNED_TRANSACTION.reset(token)


def finish_capture(fhir, outcome, failure):
    """Consume only after the original session, lock and connection cleanup finishes."""
    if outcome is None:
        return
    outcome.cleanup_complete = outcome.connection.closed is True and (
        failure is None or outcome.retained_driver is not None and outcome.retained_driver.is_closed() is True
    )
    if failure is not None or not outcome.cleanup_complete:
        owned._incomplete(outcome)
    admission = fhir._provider_directory_profile_capacity_admission()
    groups = [
        group
        for group in admission.wal_tracker.owned_control_transaction_groups
        if group["original_outcome"] is outcome and group["identity"][0] == "cms_publication_source"
    ]
    for group in groups:
        if failure is not None and group["failure"] is None:
            group["failure"] = failure
    if failure is None and outcome.status == "committed_measured":
        fhir.profile_control_custody.consume_owned_groups(fhir, outcome, None, groups=groups)
