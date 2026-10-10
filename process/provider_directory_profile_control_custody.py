# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain original control groups until their genuine containing owner exits."""

import asyncio
from contextlib import asynccontextmanager

from process.provider_directory_backend_wal_diagnostic import BackendWalDiagnosticResult
from process.provider_directory_owned_wal_transaction import OwnedWalTransaction


def _retain_origin(group_by_field, outcome):
    if outcome is not None and group_by_field["original_outcome"] is None:
        group_by_field["original_outcome"] = outcome
        group_by_field["session"] = outcome.session
        group_by_field["connection"] = outcome.connection
        group_by_field["driver"] = outcome.retained_driver
        group_by_field["pid"] = outcome.retained_pid


def consume_owned_groups(fhir, outcome, failure, *, groups=None):
    """Consume each completed group_by_field once; pending inner work is never terminal."""
    admission = fhir._provider_directory_profile_capacity_admission()
    has_explicit_groups = groups is not None
    if not has_explicit_groups:
        groups = [] if admission is None else admission.wal_tracker.owned_control_transaction_groups
    matching_groups = [
        group_by_field
        for group_by_field in groups
        if not group_by_field["consumed"]
        and (
            has_explicit_groups or group_by_field["original_outcome"] is outcome or group_by_field["outcome"] is outcome
        )
    ]
    if not matching_groups or failure is not None:
        return
    measured = (
        isinstance(outcome, OwnedWalTransaction)
        and outcome.is_committed
        and outcome.cleanup_complete
        and outcome.status == "committed_measured"
        and isinstance(outcome.measurement, BackendWalDiagnosticResult)
        and outcome.measurement.baseline.identity == outcome.measurement.final.identity
        and outcome.retained_pid == outcome.measurement.final.identity.pid
    )
    for group_by_field in matching_groups:
        if not (
            measured
            and group_by_field["task"] is asyncio.current_task()
            and group_by_field["session"] is outcome.session
            and group_by_field["connection"] is outcome.connection
            and group_by_field["driver"] is outcome.retained_driver
            and group_by_field["driver"] is not None
            and type(group_by_field["pid"]) is int
            and group_by_field["pid"] == outcome.retained_pid
            and group_by_field["outcome"] is outcome
            and group_by_field["body_complete"] is True
            and group_by_field["original_outcome"] is outcome
            and group_by_field["failure"] is None
            and group_by_field["identity"] is group_by_field["original_identity"]
            and group_by_field["identity"] is not None
            and group_by_field["window"] is fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
            and group_by_field["admission"] is admission
            and group_by_field.get("statement_custody") is group_by_field.get("original_statement_custody")
            and (
                group_by_field.get("statement_custody") is None
                or fhir.profile_payload_custody.matches(
                    fhir, group_by_field["statement_custody"], outcome, asyncio.current_task()
                )
            )
        ):
            raise RuntimeError("provider_directory_profile_control_custody_incomplete")
    for group_by_field in matching_groups:
        group_by_field["consumed"] = True
        group_by_field["status"] = "complete_mixed_unclassified"


@asynccontextmanager
async def transaction(fhir, original, *, identity, enabled, groups=None, statement_custody=None):
    """Use the existing native owner at standalone boundaries, keep inner groups pending."""
    if not enabled:
        async with original:
            yield
        return
    admission = fhir._provider_directory_profile_capacity_admission()
    if groups is None:
        groups = admission.wal_tracker.owned_control_transaction_groups
        current_window = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get()
        if (
            groups
            and all(preceding_group["consumed"] is True for preceding_group in groups)
            and any(preceding_group["window"] is not current_window for preceding_group in groups)
        ):
            groups.clear()
    group_by_field = {
        "task": asyncio.current_task(),
        "admission": admission,
        "window": fhir._PROFILE_CAPACITY_MUTATION_WINDOW.get(),
        "identity": identity,
        "original_identity": identity,
        "statement_custody": statement_custody,
        "original_statement_custody": statement_custody,
        "original_outcome": None,
        "outcome": None,
        "session": None,
        "connection": None,
        "driver": None,
        "pid": None,
        "body_complete": False,
        "failure": None,
        "consumed": False,
        "status": "incomplete",
        "accounting_authority": False,
        "reservation_refund": False,
    }
    groups.append(group_by_field)
    containing = fhir.profile_owned_wal.current_owned_wal_transaction(fhir.db)
    if fhir.db._transaction_binding() is not None or fhir.db.has_reader_session():
        _retain_origin(group_by_field, containing)
        group_by_field["outcome"] = containing
        try:
            async with original:
                yield
                group_by_field["body_complete"] = True
        except BaseException as failure:
            group_by_field["failure"] = failure
            raise
        return
    async with _owned_control_group(fhir, group_by_field):
        yield


@asynccontextmanager
async def _owned_control_group(fhir, group_by_field):
    """Preserve the real native terminal outcome and original primary failure."""
    outcome = body_failure = None
    try:
        async with fhir.profile_owned_wal.registry_owned_wal_transaction(fhir.db) as outcome:
            _retain_origin(group_by_field, outcome)
            group_by_field["outcome"] = outcome
            try:
                yield
                group_by_field["body_complete"] = True
            except BaseException as failure:
                body_failure = failure
                raise
    except BaseException as failure:
        outcome = getattr(failure, "outcome", outcome)
        _retain_origin(group_by_field, outcome)
        group_by_field["outcome"] = outcome
        primary = body_failure if body_failure is not None else failure.__cause__ or failure
        group_by_field["failure"] = primary
        raise primary from None
    consume_owned_groups(fhir, outcome, None, groups=[group_by_field])
