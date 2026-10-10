# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original artifact payload and control owners at existing terminal boundaries."""

import asyncio
import contextvars
import copy
from contextlib import asynccontextmanager

_STATEMENT = contextvars.ContextVar("provider_directory_artifact_statement_custody", default=None)
_CONTROL_STATEMENTS = contextvars.ContextVar("provider_directory_artifact_control_statements", default=None)


def _is_bounded(fhir):
    admission = fhir._provider_directory_profile_capacity_admission()
    return getattr(getattr(admission, "geometry", None), "bounded_admission", False) is True


@asynccontextmanager
async def payload_transaction(fhir, kind, relation_ref, batch, projection, statement, params, *, mutating=True):
    """Bind one original artifact coordinate, owner and root statement witness."""
    if not _is_bounded(fhir):
        async with fhir._provider_directory_profile_capacity_transaction():
            yield
        return
    custody = fhir.profile_payload_custody.statement(statement, dict(params)) if mutating else None
    identity = (
        "artifact_scope_payload" if mutating else "artifact_scope_zero_probe",
        kind,
        relation_ref,
        batch,
        projection,
        statement,
        copy.deepcopy(params),
    )
    token = _STATEMENT.set((asyncio.current_task(), custody))
    try:
        async with fhir._provider_directory_profile_capacity_transaction(
            control_identity=identity, statement_custody=custody
        ):
            yield
    finally:
        _STATEMENT.reset(token)


async def execute_insert(fhir, statement, params):
    """Execute the exact existing INSERT once; retain missing-owner uncertainty."""
    capture = _STATEMENT.get()
    if capture is None:
        return fhir._coerce_rowcount(await fhir.db.status(statement, **params))
    task, custody = capture
    if (
        task is not asyncio.current_task()
        or custody is None
        or statement != custody["original_statement"]
        or dict(params) != custody["original_params"]
    ):
        raise RuntimeError("provider_directory_artifact_statement_custody_invalid")
    return await fhir.profile_payload_custody.execute(fhir, custody)


async def status(fhir, operation, relation_ref, statement, **params):
    """Tag each original ordinary DDL/analyze/drop statement without another boundary."""
    if not _is_bounded(fhir):
        return await fhir._provider_directory_profile_capacity_status(statement, **params)
    async with fhir._provider_directory_profile_capacity_transaction(
        control_identity=(operation, relation_ref, statement, copy.deepcopy(params))
    ):
        return await fhir.db.status(statement, **params)


@asynccontextmanager
async def recovery_transaction(fhir, identity):
    """Keep one existing atomic recovery owner and all original body statements."""
    if not _is_bounded(fhir):
        async with fhir._provider_directory_profile_capacity_transaction():
            yield
        return
    statements = []
    token = _CONTROL_STATEMENTS.set((asyncio.current_task(), statements))
    try:
        async with fhir._provider_directory_profile_capacity_transaction(
            control_identity=("artifact_scope_recovery", identity, statements)
        ):
            yield
    finally:
        _CONTROL_STATEMENTS.reset(token)


async def recovery_status(fhir, statement, **params):
    """Record the exact lock/DDL/drop group under its original containing owner."""
    capture = _CONTROL_STATEMENTS.get()
    if capture is not None:
        task, statements = capture
        if task is not asyncio.current_task():
            raise RuntimeError("provider_directory_artifact_recovery_statement_owner_invalid")
        binding = fhir.db._transaction_binding()
        statements.append((statement, copy.deepcopy(params), None if binding is None else binding.session))
    return await fhir.db.status(statement, **params)


@asynccontextmanager
async def cutover_transaction(fhir, kind, identity):
    """Retain the genuine existing atomic owner; inner work cannot settle it."""
    async with fhir.profile_control_custody.transaction(
        fhir,
        fhir.db.transaction(),
        identity=("artifact_cutover", kind, identity, fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get()),
        enabled=_is_bounded(fhir),
    ):
        yield
