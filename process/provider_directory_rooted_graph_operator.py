# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off manual operations for one exact rooted Provider Directory graph."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from process.provider_directory_global_profile_followup_contract import (
    build_provider_directory_global_profile_followup,
    profile_followup_receipt_metadata,
)
from process.provider_directory_rooted_graph_operator_contract import (
    _canonical_json,
    _exact_operation_key,
    _exact_publication_acquisition_id,
    _operation_error,
    ACQUISITION_ENABLED_ENV,
    build_rooted_graph_operator_identities,
    build_rooted_graph_single_root_identity,
    OPERATOR_PHASES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256,
    PUBLICATION_ENABLED_ENV,
    ProviderDirectoryRootedGraphOperatorError,
    ProviderDirectoryRootedGraphOperatorIdentities,
    RootedGraphSingleIdentity,
    REGISTRATION_ENABLED_ENV,
    SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
    SINGLE_ROOT_ACQUISITION_PHASE,
    require_rooted_graph_operator_gate,
    rooted_graph_operator_contract_payload,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    single_root_operation_payload,
)


async def _register_source(database: Any) -> Any:
    from process.provider_directory_rooted_graph_registration import (
        ProviderDirectoryRootedGraphRegistrationResult,
        register_provider_directory_rooted_graph_source,
    )

    result = await register_provider_directory_rooted_graph_source(database=database)
    if type(result) is not ProviderDirectoryRootedGraphRegistrationResult:
        raise ProviderDirectoryRootedGraphOperatorError("evidence")
    return result


def _registration_json(result: Any) -> str:
    return _canonical_json(
        {
            "created": result.created,
            "endpoint_created": result.endpoint_created,
            "endpoint_id": result.endpoint_id,
            "operator_contract_sha256": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256
            ),
            "source_created": result.source_created,
            "source_id": result.source_id,
            "status": "registered",
        }
    )


async def register_rooted_graph_source_operation(
    *,
    database: Any,
) -> str:
    """Insert or exactly replay the dormant registry pair and nothing else."""

    require_rooted_graph_operator_gate("register")
    try:
        return _registration_json(await _register_source(database))
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ProviderDirectoryRootedGraphOperatorError:
        raise
    except Exception as error:
        raise _operation_error(error, "registration") from None


async def _select_exact_current_root(database: Any) -> Any:
    from process.provider_directory_dataset_scoped_publication import (
        exact_uhc_dataset_pair,
        lock_exact_current_dataset,
    )
    from process.provider_directory_dataset_scoped_publication_contract import (
        EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
        ExactCurrentDataset,
    )

    async with database.transaction():
        await database.scalar(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended(:lock_identity, 0));",
            lock_identity=EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
        )
        current = await lock_exact_current_dataset(
            database,
            pair=exact_uhc_dataset_pair(),
        )
    if current is None:
        raise ProviderDirectoryRootedGraphOperatorError("missing")
    if type(current) is not ExactCurrentDataset:
        raise ProviderDirectoryRootedGraphOperatorError("evidence")
    return current


def _root_receipt_payload(receipt: Any) -> dict[str, Any]:
    return {
        "acquisition_id": receipt.acquisition_id,
        "completed_count": receipt.completed_count,
        "edge_count": receipt.edge_count,
        "resource_count": receipt.resource_count,
        "rooted_graph_sha256": receipt.rooted_graph_sha256,
        "run_id": receipt.run_id,
    }


def _acquisition_json(
    current: Any,
    receipt: Any,
    admission: Any,
    operation_key: str,
) -> str:
    return _canonical_json(
        {
            "admission_id": admission.admission_id,
            "baseline": _root_receipt_payload(receipt.baseline),
            "candidate": _root_receipt_payload(receipt.candidate),
            "dataset_intent_id": receipt.dataset_intent_id,
            "operator_contract_sha256": (
                PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256
            ),
            "operation_key": operation_key,
            "publication_acquisition_id": admission.publication_acquisition_id,
            "root_dataset_hash": current.dataset_hash,
            "root_dataset_id": current.dataset_id,
            "root_dataset_variant": current.variant,
            "root_practitioner_resource_count": current.practitioner_resource_count,
            "rooted_graph_sha256": admission.rooted_graph_sha256,
            "status": "admitted",
            "twin_attempt_id": admission.attempt_id,
        }
    )


@dataclass(frozen=True, slots=True)
class _AcquisitionControls:
    concurrency: int
    max_attempts: int
    lease_seconds: int
    retry_base_seconds: float
    max_retry_seconds: float
    root_timeout_seconds: float


def _acquisition_config(controls: _AcquisitionControls) -> Any:
    from process.provider_directory_rooted_graph_acquisition import (
        ProviderDirectoryRootedGraphAcquisitionConfig,
    )

    return ProviderDirectoryRootedGraphAcquisitionConfig(
        enabled=True,
        concurrency=controls.concurrency,
        max_attempts=controls.max_attempts,
        lease_seconds=controls.lease_seconds,
        retry_base_seconds=controls.retry_base_seconds,
        max_retry_seconds=controls.max_retry_seconds,
        root_timeout_seconds=controls.root_timeout_seconds,
    )


def _require_acquisition_receipt(
    receipt: Any,
    identities: ProviderDirectoryRootedGraphOperatorIdentities,
    receipt_type: type[Any],
) -> None:
    """Reject evidence that is not the exact derived twin pair."""

    if (
        type(receipt) is not receipt_type
        or receipt.dataset_intent_id != identities.dataset_intent_id
        or receipt.baseline.acquisition_id != identities.baseline.acquisition_id
        or receipt.candidate.acquisition_id != identities.candidate.acquisition_id
    ):
        raise ProviderDirectoryRootedGraphOperatorError("evidence")
    if receipt.rooted_graphs_match is not True:
        raise ProviderDirectoryRootedGraphOperatorError("mismatch")


def _require_admission(
    admission: Any,
    current: Any,
    identities: ProviderDirectoryRootedGraphOperatorIdentities,
    receipt: Any,
    admission_type: type[Any],
) -> None:
    """Reject authority not bound to the exact candidate and current root."""

    if (
        type(admission) is not admission_type
        or admission.publication_acquisition_id != identities.candidate.acquisition_id
        or admission.comparison_acquisition_id != identities.baseline.acquisition_id
        or admission.dataset_intent_id != identities.dataset_intent_id
        or admission.root_dataset_id != current.dataset_id
        or admission.rooted_graph_sha256 != receipt.candidate.rooted_graph_sha256
        or admission.publication_authority is not True
    ):
        raise ProviderDirectoryRootedGraphOperatorError("admission")


async def _run_acquisition_phase(
    operation_key: str,
    controls: _AcquisitionControls,
    database: Any,
) -> str:
    """Compose exact selection, twin acquisition, and admission only."""

    from process.provider_directory_rooted_graph_acquisition import (
        acquire_provider_directory_rooted_graph_twins,
        ProviderDirectoryRootedGraphAcquisitionReceipt,
    )
    from process.provider_directory_rooted_graph_twin_store import (
        admit_provider_directory_rooted_graph_twins,
        ProviderDirectoryRootedGraphTwinAdmission,
    )

    config = _acquisition_config(controls)
    current = await _select_exact_current_root(database)
    identities = build_rooted_graph_operator_identities(
        current,
        operation_key=operation_key,
    )
    receipt = await acquire_provider_directory_rooted_graph_twins(
        identities.baseline,
        identities.candidate,
        config=config,
        database=database,
    )
    _require_acquisition_receipt(
        receipt,
        identities,
        ProviderDirectoryRootedGraphAcquisitionReceipt,
    )
    admission = await admit_provider_directory_rooted_graph_twins(
        receipt.baseline.acquisition_id,
        receipt.candidate.acquisition_id,
        database=database,
    )
    _require_admission(
        admission,
        current,
        identities,
        receipt,
        ProviderDirectoryRootedGraphTwinAdmission,
    )
    return _acquisition_json(current, receipt, admission, operation_key)


async def acquire_admit_rooted_graph_operation(
    *,
    operation_key: str,
    concurrency: int,
    max_attempts: int,
    lease_seconds: int,
    retry_base_seconds: float,
    max_retry_seconds: float,
    root_timeout_seconds: float,
    database: Any,
) -> str:
    """Derive, resume, seal, compare, and admit one exact pair; never publish."""

    require_rooted_graph_operator_gate("acquire")
    try:
        exact_operation_key = _exact_operation_key(operation_key)
    except ValueError:
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request") from None
    try:
        controls = _AcquisitionControls(
            concurrency=concurrency,
            max_attempts=max_attempts,
            lease_seconds=lease_seconds,
            retry_base_seconds=retry_base_seconds,
            max_retry_seconds=max_retry_seconds,
            root_timeout_seconds=root_timeout_seconds,
        )
        return await _run_acquisition_phase(
            exact_operation_key,
            controls,
            database,
        )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ProviderDirectoryRootedGraphOperatorError:
        raise
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "acquisition") from None


async def _run_single_root_acquisition_phase(
    operation_key: str,
    controls: _AcquisitionControls,
    database: Any,
) -> str:
    """Acquire and admit one exact candidate under reviewed policy one."""

    from process.provider_directory_rooted_graph_acquisition import (
        acquire_rooted_graph_single_root,
        ProviderDirectoryRootedGraphRootReceipt,
    )
    from process.provider_directory_rooted_graph_twin_store import (
        admit_rooted_graph_single_root,
        ProviderDirectoryRootedGraphTwinAdmission,
    )

    config = _acquisition_config(controls)
    current = await _select_exact_current_root(database)
    identity = build_rooted_graph_single_root_identity(
        current,
        operation_key=operation_key,
    )
    receipt = await acquire_rooted_graph_single_root(
        identity.candidate,
        config=config,
        database=database,
    )
    if (
        type(receipt) is not ProviderDirectoryRootedGraphRootReceipt
        or receipt.acquisition_role != "candidate"
        or receipt.acquisition_id != identity.candidate.acquisition_id
        or receipt.run_id != identity.candidate.run_id
    ):
        raise ProviderDirectoryRootedGraphOperatorError("evidence")
    admission = await admit_rooted_graph_single_root(
        receipt.acquisition_id,
        acquisition_operation_key=operation_key,
        database=database,
    )
    if (
        type(admission) is not ProviderDirectoryRootedGraphTwinAdmission
        or admission.publication_acquisition_id != receipt.acquisition_id
        or admission.attempt_id is not None
        or admission.comparison_acquisition_id is not None
        or admission.acquisition_operation_key != operation_key
        or admission.publication_authority is not True
    ):
        raise ProviderDirectoryRootedGraphOperatorError("admission")
    return _canonical_json(
        single_root_operation_payload(current, receipt, admission, operation_key)
    )


async def acquire_single_root_operation(
    *,
    operation_key: str,
    concurrency: int,
    max_attempts: int,
    lease_seconds: int,
    retry_base_seconds: float,
    max_retry_seconds: float,
    root_timeout_seconds: float,
    database: Any,
) -> str:
    """Acquire and admit one policy-one candidate; never publish."""

    require_rooted_graph_operator_gate(SINGLE_ROOT_ACQUISITION_PHASE)
    try:
        exact_operation_key = _exact_operation_key(operation_key)
        controls = _AcquisitionControls(
            concurrency=concurrency,
            max_attempts=max_attempts,
            lease_seconds=lease_seconds,
            retry_base_seconds=retry_base_seconds,
            max_retry_seconds=max_retry_seconds,
            root_timeout_seconds=root_timeout_seconds,
        )
        return await _run_single_root_acquisition_phase(
            exact_operation_key,
            controls,
            database,
        )
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ProviderDirectoryRootedGraphOperatorError:
        raise
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "acquisition") from None


def _publication_json(publication_result: Any) -> str:
    readiness = publication_result.readiness
    retry_exhausted_count = readiness.retry_exhausted_count
    is_retry_exhausted = type(retry_exhausted_count) is int and retry_exhausted_count > 0
    if readiness.cohort_complete is (not is_retry_exhausted):
        profile_dispatch_by_field = {
            **profile_followup_receipt_metadata(),
            "external_followup": (
                build_provider_directory_global_profile_followup(
                    source_id=readiness.source_id,
                    dataset_id=readiness.dataset_id,
                    parent_run_id=readiness.acquisition_root_run_id,
                )
            ),
            "operator_command_available": False,
            "required_external_global_dispatch": True,
            "status": "not_dispatched",
        }
    else:
        profile_dispatch_by_field = {
            "operator_command_available": False,
            "required_external_global_dispatch": False,
            "status": "not_applicable_incomplete_cohort",
        }
    publication_by_field = {
        "admission_id": readiness.admission_id,
        "dataset_hash": readiness.dataset_hash,
        "dataset_id": readiness.dataset_id,
        "operator_contract_sha256": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256
        ),
        "previous_dataset_id": readiness.previous_dataset_id,
        "profile_dispatch": profile_dispatch_by_field,
        "publication_acquisition_id": readiness.publication_acquisition_id,
        "replayed": publication_result.replayed,
        "resource_count": readiness.resource_count,
        "root_dataset_variant": readiness.root_dataset_variant,
        "rooted_graph_complete": readiness.rooted_graph_complete,
        "status": "published",
    }
    if not readiness.cohort_complete:
        publication_by_field["cohort_complete"] = False
        publication_by_field["retry_exhausted_count"] = retry_exhausted_count
    return _canonical_json(publication_by_field)


async def publish_admitted_rooted_graph_operation(
    *,
    publication_acquisition_id: str,
    batch_size: int,
    database: Any,
) -> str:
    """Publish or replay exactly one admitted candidate selector."""

    require_rooted_graph_operator_gate("publish")
    try:
        exact_selector = _exact_publication_acquisition_id(publication_acquisition_id)
    except ValueError:
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request") from None
    try:
        from process.provider_directory_rooted_graph_publication import (
            publish_provider_directory_rooted_graph_dataset,
            ProviderDirectoryRootedGraphPublicationResult,
        )

        publication_result = await publish_provider_directory_rooted_graph_dataset(
            exact_selector,
            database=database,
            batch_size=batch_size,
        )
        if (
            type(publication_result)
            is not ProviderDirectoryRootedGraphPublicationResult
            or publication_result.readiness.publication_acquisition_id != exact_selector
        ):
            raise ProviderDirectoryRootedGraphOperatorError("evidence")
        return _publication_json(publication_result)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except ProviderDirectoryRootedGraphOperatorError:
        raise
    except (TypeError, ValueError):
        raise ProviderDirectoryRootedGraphOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "publication") from None


__all__ = tuple(
    (
        "ACQUISITION_ENABLED_ENV OPERATOR_PHASES "
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_ID "
        "PROVIDER_DIRECTORY_ROOTED_GRAPH_OPERATOR_CONTRACT_SHA256 "
        "PUBLICATION_ENABLED_ENV ProviderDirectoryRootedGraphOperatorError "
        "ProviderDirectoryRootedGraphOperatorIdentities RootedGraphSingleIdentity "
        "REGISTRATION_ENABLED_ENV SINGLE_ROOT_ACQUISITION_ENABLED_ENV "
        "SINGLE_ROOT_ACQUISITION_PHASE acquire_admit_rooted_graph_operation "
        "acquire_single_root_operation build_rooted_graph_operator_identities "
        "build_rooted_graph_single_root_identity "
        "publish_admitted_rooted_graph_operation register_rooted_graph_source_operation "
        "require_rooted_graph_operator_gate rooted_graph_operator_contract_payload"
    ).split()
)
