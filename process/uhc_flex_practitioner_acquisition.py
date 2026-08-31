# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormant restartable orchestration for exact-cohort Practitioner twins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp

from db.connection import db
from process.uhc_flex_official_cohort_contract import (
    UHCFlexOfficialNPICohort,
)
from process.uhc_flex_official_cohort_store import (
    UHCFlexOfficialCohortSyncResult,
)
from process.uhc_flex_practitioner_acquisition_contract import (
    ProgressCallback,
    ROOT_ROLES,
    SHA256_PATTERN,
    strict_nonnegative_seconds,
    UHCFlexPractitionerAcquisitionConfig,
    UHCFlexPractitionerAcquisitionDependencies,
    UHCFlexPractitionerAcquisitionError,
    UHCFlexPractitionerAcquisitionProgress,
    UHCFlexPractitionerAcquisitionReceipt,
    UHCFlexPractitionerRootReceipt,
    UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_ATTEMPTS,
    UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_CONCURRENCY,
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS,
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY,
    UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS,
)
from process.uhc_flex_practitioner_acquisition_runtime import (
    default_dependencies,
    default_session_scope,
    run_root,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_registration import (
    uhc_flex_practitioner_endpoint_identity,
    UHCFlexPractitionerRegistrationResult,
)
from process.uhc_flex_practitioner_single_root_contract import (
    build_single_root_context,
    build_single_root_receipt,
    is_exact_single_root_admission,
    UHCFlexPractitionerSingleRootAdmission,
    UHCFlexPractitionerSingleRootContext,
    UHCFlexPractitionerSingleRootReceipt,
)
from process.uhc_flex_practitioner_store import (
    build_uhc_flex_practitioner_acquisition_identity,
    UHCFlexPractitionerAcquisitionIdentity,
    UHCFlexPractitionerAcquisitionSummary,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    canonical_semantic_projection_as_of,
    UHCFlexPractitionerTwinAdmission,
    UHCFlexPractitionerTwinStoreError,
)


_ROOT_ROLES = ROOT_ROLES
_SHA256_PATTERN = SHA256_PATTERN
_strict_nonnegative_seconds = strict_nonnegative_seconds
_default_session_scope = default_session_scope
_default_dependencies = default_dependencies
_run_root = run_root


@dataclass(frozen=True, slots=True)
class _AcquisitionContext:
    registration: UHCFlexPractitionerRegistrationResult
    cohort: UHCFlexOfficialNPICohort
    dataset_intent_id: str
    identity_by_role: dict[str, UHCFlexPractitionerAcquisitionIdentity]
    semantic_projection_as_of: str
    operation_key: str


def _validate_registration(
    registration: Any,
) -> UHCFlexPractitionerRegistrationResult:
    expected_endpoint_id = uhc_flex_practitioner_endpoint_identity().endpoint_id
    if (
        type(registration) is not UHCFlexPractitionerRegistrationResult
        or registration.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
        or registration.endpoint_id != expected_endpoint_id
    ):
        raise UHCFlexPractitionerAcquisitionError("source_drift")
    return registration


def _validated_cohort_sync(cohort_sync: Any) -> UHCFlexOfficialNPICohort:
    if (
        type(cohort_sync) is not UHCFlexOfficialCohortSyncResult
        or type(cohort_sync.cohort) is not UHCFlexOfficialNPICohort
    ):
        raise UHCFlexPractitionerAcquisitionError("cohort_drift")
    return cohort_sync.cohort


def _root_receipt(
    identity: UHCFlexPractitionerAcquisitionIdentity,
    summary: UHCFlexPractitionerAcquisitionSummary,
    elapsed_seconds: float,
) -> UHCFlexPractitionerRootReceipt:
    if (
        summary.acquisition_id != identity.acquisition_id
        or summary.expected_npi_count != identity.expected_npi_count
    ):
        raise UHCFlexPractitionerAcquisitionError("state")
    return UHCFlexPractitionerRootReceipt(
        acquisition_role=identity.acquisition_role,
        acquisition_id=identity.acquisition_id,
        run_id=identity.run_id,
        matched_count=summary.matched_count,
        unmatched_count=summary.unmatched_count,
        resource_count=summary.resource_count,
        terminal_set_sha256=summary.terminal_set_sha256,
        elapsed_seconds=elapsed_seconds,
        error_count=summary.error_count,
        cohort_complete=summary.cohort_complete,
    )


def _validate_admission(
    admission: Any,
    *,
    baseline: UHCFlexPractitionerAcquisitionSummary,
    candidate: UHCFlexPractitionerAcquisitionSummary,
    context: _AcquisitionContext,
) -> UHCFlexPractitionerTwinAdmission:
    if (
        type(admission) is not UHCFlexPractitionerTwinAdmission
        or admission.baseline_acquisition_id != baseline.acquisition_id
        or admission.candidate_acquisition_id != candidate.acquisition_id
        or admission.cohort_id != context.cohort.cohort_id
        or admission.dataset_intent_id != context.dataset_intent_id
        or admission.semantic_projection_as_of
        != context.semantic_projection_as_of
        or admission.operation_key != context.operation_key
        or admission.expected_npi_count != context.cohort.npi_count
        or baseline.terminal_set_sha256 != candidate.terminal_set_sha256
        or baseline.resource_count != candidate.resource_count
        or admission.terminal_set_sha256 != candidate.terminal_set_sha256
        or admission.resource_count != candidate.resource_count
        or admission.publication_authority is not True
    ):
        raise UHCFlexPractitionerAcquisitionError("state")
    return admission


def _validated_runtime_inputs(
    operation_key: str,
    semantic_projection_as_of: str,
    config: UHCFlexPractitionerAcquisitionConfig,
    dependencies: UHCFlexPractitionerAcquisitionDependencies | None,
) -> tuple[str, UHCFlexPractitionerAcquisitionDependencies]:
    if type(config) is not UHCFlexPractitionerAcquisitionConfig:
        raise ValueError("Flex Practitioner acquisition config is invalid")
    projection_date = canonical_semantic_projection_as_of(
        semantic_projection_as_of
    )
    if (
        type(operation_key) is not str
        or _SHA256_PATTERN.fullmatch(operation_key) is None
    ):
        raise ValueError("Flex Practitioner operation key is invalid")
    if config.enabled is not True:
        raise UHCFlexPractitionerAcquisitionError("disabled")
    runtime_dependencies = dependencies or _default_dependencies()
    if type(runtime_dependencies) is not UHCFlexPractitionerAcquisitionDependencies:
        raise ValueError("Flex Practitioner acquisition dependencies are invalid")
    return projection_date, runtime_dependencies


async def _initialize_context(
    *,
    operation_key: str,
    projection_date: str,
    dependencies: UHCFlexPractitionerAcquisitionDependencies,
    database: Any,
) -> _AcquisitionContext:
    registration = _validate_registration(
        await dependencies.register_source(database=database)
    )
    cohort = _validated_cohort_sync(
        await dependencies.sync_cohort(database=database)
    )
    dataset_intent_id = build_uhc_flex_practitioner_dataset_intent_id(
        cohort.cohort_id,
        projection_date,
        operation_key,
    )
    identity_by_role = {
        role: build_uhc_flex_practitioner_acquisition_identity(
            cohort,
            acquisition_role=role,
            run_id=build_uhc_flex_practitioner_run_id(dataset_intent_id, role),
            dataset_intent_id=dataset_intent_id,
        )
        for role in _ROOT_ROLES
    }
    for role in _ROOT_ROLES:
        created_count = await dependencies.initialize_root(
            identity_by_role[role],
            database=database,
        )
        if type(created_count) is not int or created_count not in {0, 1}:
            raise UHCFlexPractitionerAcquisitionError("state")
    return _AcquisitionContext(
        registration=registration,
        cohort=cohort,
        dataset_intent_id=dataset_intent_id,
        identity_by_role=identity_by_role,
        semantic_projection_as_of=projection_date,
        operation_key=operation_key,
    )


async def _revalidate_locked_inputs(
    context: _AcquisitionContext,
    *,
    dependencies: UHCFlexPractitionerAcquisitionDependencies,
    database: Any,
) -> None:
    replayed_registration = _validate_registration(
        await dependencies.register_source(database=database)
    )
    if (
        replayed_registration.source_id != context.registration.source_id
        or replayed_registration.endpoint_id != context.registration.endpoint_id
        or replayed_registration.created
    ):
        raise UHCFlexPractitionerAcquisitionError("source_drift")
    replayed_cohort_sync = await dependencies.sync_cohort(database=database)
    replayed_cohort = _validated_cohort_sync(replayed_cohort_sync)
    if replayed_cohort_sync.created or replayed_cohort != context.cohort:
        raise UHCFlexPractitionerAcquisitionError("cohort_drift")


async def _admit_root_pair(
    context: _AcquisitionContext,
    baseline: UHCFlexPractitionerAcquisitionSummary,
    candidate: UHCFlexPractitionerAcquisitionSummary,
    *,
    dependencies: UHCFlexPractitionerAcquisitionDependencies,
    database: Any,
) -> UHCFlexPractitionerTwinAdmission:
    admission: UHCFlexPractitionerTwinAdmission | None = None
    mismatch_error: UHCFlexPractitionerTwinStoreError | None = None
    async with database.transaction():
        await _revalidate_locked_inputs(
            context,
            dependencies=dependencies,
            database=database,
        )
        try:
            raw_admission = await dependencies.admit_twins(
                baseline.acquisition_id,
                candidate.acquisition_id,
                semantic_projection_as_of=context.semantic_projection_as_of,
                operation_key=context.operation_key,
                database=database,
            )
        except UHCFlexPractitionerTwinStoreError as error:
            if error.code != "mismatch":
                raise
            mismatch_error = error
        else:
            admission = _validate_admission(
                raw_admission,
                baseline=baseline,
                candidate=candidate,
                context=context,
            )
    if mismatch_error is not None:
        raise mismatch_error
    if admission is None:
        raise UHCFlexPractitionerAcquisitionError("state")
    return admission


async def _admit_single_root(
    context: UHCFlexPractitionerSingleRootContext,
    candidate: UHCFlexPractitionerAcquisitionSummary,
    *,
    dependencies: UHCFlexPractitionerAcquisitionDependencies,
    database: Any,
) -> UHCFlexPractitionerSingleRootAdmission:
    locked_context = _AcquisitionContext(
        registration=context.registration, cohort=context.cohort,
        dataset_intent_id=context.dataset_intent_id,
        identity_by_role={"candidate": context.candidate_identity},
        semantic_projection_as_of=context.semantic_projection_as_of,
        operation_key=context.operation_key,
    )
    admit_single_root = dependencies.admit_single_root
    if admit_single_root is None:
        from process.uhc_flex_practitioner_twin_store import (
            admit_uhc_flex_practitioner_single_root as admit_single_root,
        )
    async with database.transaction():
        await _revalidate_locked_inputs(
            locked_context, dependencies=dependencies, database=database
        )
        admission = await admit_single_root(
            candidate.acquisition_id,
            semantic_projection_as_of=context.semantic_projection_as_of,
            operation_key=context.operation_key,
            database=database,
        )
    if not is_exact_single_root_admission(context, candidate, admission):
        raise UHCFlexPractitionerAcquisitionError("state")
    return admission


def _build_receipt(
    context: _AcquisitionContext,
    baseline: UHCFlexPractitionerAcquisitionSummary,
    baseline_elapsed: float,
    candidate: UHCFlexPractitionerAcquisitionSummary,
    candidate_elapsed: float,
    admission: UHCFlexPractitionerTwinAdmission,
    elapsed_seconds: float,
) -> UHCFlexPractitionerAcquisitionReceipt:
    return UHCFlexPractitionerAcquisitionReceipt(
        operation_key=context.operation_key,
        semantic_projection_as_of=context.semantic_projection_as_of,
        source_id=context.registration.source_id,
        endpoint_id=context.registration.endpoint_id,
        cohort_id=context.cohort.cohort_id,
        official_dataset_id=context.cohort.official_dataset_id,
        official_dataset_hash=context.cohort.official_dataset_hash,
        official_content_proof_sha256=(
            context.cohort.official_content_proof_sha256
        ),
        dataset_intent_id=context.dataset_intent_id,
        expected_npi_count=context.cohort.npi_count,
        baseline=_root_receipt(
            context.identity_by_role["baseline"],
            baseline,
            baseline_elapsed,
        ),
        candidate=_root_receipt(
            context.identity_by_role["candidate"],
            candidate,
            candidate_elapsed,
        ),
        twin_attempt_id=admission.attempt_id,
        admission_id=admission.admission_id,
        elapsed_seconds=elapsed_seconds,
    )


async def acquire_uhc_flex_practitioner_twins(
    *,
    operation_key: str,
    semantic_projection_as_of: str,
    config: UHCFlexPractitionerAcquisitionConfig = (
        UHCFlexPractitionerAcquisitionConfig()
    ),
    database: Any = db,
    dependencies: UHCFlexPractitionerAcquisitionDependencies | None = None,
    progress_callback: ProgressCallback | None = None,
) -> UHCFlexPractitionerAcquisitionReceipt:
    """Acquire, independently seal, and admit one exact baseline/candidate pair."""

    projection_date, runtime_dependencies = _validated_runtime_inputs(
        operation_key,
        semantic_projection_as_of,
        config,
        dependencies,
    )
    started_at = runtime_dependencies.monotonic()
    context = await _initialize_context(
        operation_key=operation_key,
        projection_date=projection_date,
        dependencies=runtime_dependencies,
        database=database,
    )
    root_results = []
    for role in _ROOT_ROLES:
        root_results.append(
            await _run_root(
                context.identity_by_role[role],
                config=config,
                dependencies=runtime_dependencies,
                database=database,
                progress_callback=progress_callback,
            )
        )
    baseline, baseline_elapsed = root_results[0]
    candidate, candidate_elapsed = root_results[1]
    admission = await _admit_root_pair(
        context,
        baseline,
        candidate,
        dependencies=runtime_dependencies,
        database=database,
    )
    elapsed_seconds = runtime_dependencies.monotonic() - started_at
    _strict_nonnegative_seconds(elapsed_seconds, "total timing")
    return _build_receipt(
        context,
        baseline,
        baseline_elapsed,
        candidate,
        candidate_elapsed,
        admission,
        elapsed_seconds,
    )


async def acquire_uhc_flex_single_root(
    *,
    operation_key: str,
    semantic_projection_as_of: str,
    config: UHCFlexPractitionerAcquisitionConfig = UHCFlexPractitionerAcquisitionConfig(),
    database: Any = db,
    dependencies: UHCFlexPractitionerAcquisitionDependencies | None = None,
    progress_callback: ProgressCallback | None = None,
) -> UHCFlexPractitionerSingleRootReceipt:
    """Acquire and admit one reviewed candidate without creating a twin."""

    projection_date, runtime_dependencies = _validated_runtime_inputs(
        operation_key, semantic_projection_as_of, config, dependencies
    )
    started_at = runtime_dependencies.monotonic()
    registration = _validate_registration(await runtime_dependencies.register_source(database=database))
    cohort = _validated_cohort_sync(await runtime_dependencies.sync_cohort(database=database))
    context = build_single_root_context(
        registration, cohort, projection_date, operation_key
    )
    created_count = await runtime_dependencies.initialize_root(
        context.candidate_identity, database=database
    )
    if type(created_count) is not int or created_count not in {0, 1}:
        raise UHCFlexPractitionerAcquisitionError("state")
    candidate_started_at = runtime_dependencies.monotonic()
    while True:
        try:
            candidate, _ = await _run_root(
                context.candidate_identity,
                config=config,
                dependencies=runtime_dependencies,
                database=database,
                progress_callback=progress_callback,
            )
            break
        except UHCFlexPractitionerAcquisitionError as error:
            if error.code != "root_retryable":
                raise
            await runtime_dependencies.sleep(UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS)
    candidate_elapsed = runtime_dependencies.monotonic() - candidate_started_at
    _strict_nonnegative_seconds(candidate_elapsed, "root timing")
    admission = await _admit_single_root(
        context,
        candidate,
        dependencies=runtime_dependencies,
        database=database,
    )
    elapsed_seconds = runtime_dependencies.monotonic() - started_at
    _strict_nonnegative_seconds(elapsed_seconds, "total timing")
    return build_single_root_receipt(
        context,
        _root_receipt(
            context.candidate_identity,
            candidate,
            candidate_elapsed,
        ),
        admission,
        elapsed_seconds,
    )


__all__ = (
    "acquire_uhc_flex_practitioner_twins",
    "UHCFlexPractitionerAcquisitionConfig",
    "UHCFlexPractitionerAcquisitionDependencies",
    "UHCFlexPractitionerAcquisitionError",
    "UHCFlexPractitionerAcquisitionProgress",
    "UHCFlexPractitionerAcquisitionReceipt",
    "UHCFlexPractitionerRootReceipt",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_ATTEMPTS",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_CONCURRENCY",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY",
    "UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS",
)
