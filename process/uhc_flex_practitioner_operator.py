# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Default-off operations for exact-cohort Flex Practitioner enrichment."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from process.provider_directory_global_profile_followup_contract import (
    build_provider_directory_global_profile_followup,
    profile_followup_receipt_metadata,
)


COHORT_ENABLED_ENV = "HLTHPRT_UHC_FLEX_PRACTITIONER_COHORT_ENABLED"
ACQUISITION_ENABLED_ENV = "HLTHPRT_UHC_FLEX_PRACTITIONER_ACQUISITION_ENABLED"
SINGLE_ROOT_ACQUISITION_ENABLED_ENV = (
    "HLTHPRT_UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ACQUISITION_ENABLED"
)
PUBLICATION_ENABLED_ENV = "HLTHPRT_UHC_FLEX_PRACTITIONER_PUBLICATION_ENABLED"
OPERATOR_PHASES = (
    "sync-cohort",
    "acquire-admit",
    "acquire-admit-single-root",
    "publish-admitted",
)
_GATE_BY_PHASE = {
    "sync-cohort": COHORT_ENABLED_ENV,
    "acquire-admit": ACQUISITION_ENABLED_ENV,
    "acquire-admit-single-root": SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
    "publish-admitted": PUBLICATION_ENABLED_ENV,
}
_PRESERVED_ERROR_CODES = frozenset(
    {
        "admission",
        "busy",
        "cohort_drift",
        "content",
        "evidence",
        "foreign_current",
        "identity",
        "mismatch",
        "missing",
        "progress",
        "replay",
        "root_retryable",
        "root_unsealable",
        "source_drift",
        "state",
    }
)
_MESSAGE_BY_CODE = {
    "acquisition": "Flex Practitioner acquisition failed",
    "admission": "Flex Practitioner admission is invalid",
    "busy": "Flex Practitioner source is busy",
    "cohort_drift": "Flex Practitioner official cohort has drifted",
    "content": "Flex Practitioner publication content is invalid",
    "disabled": "Flex Practitioner operator phase is disabled",
    "evidence": "Flex Practitioner operator evidence is invalid",
    "foreign_current": "Flex Practitioner current dataset is unrelated",
    "gate_conflict": "Flex Practitioner operator gates conflict",
    "identity": "Flex Practitioner operation identity is invalid",
    "invalid_request": "Flex Practitioner operator request is invalid",
    "mismatch": "Flex Practitioner independent roots do not match",
    "missing": "Flex Practitioner operation evidence is missing",
    "progress": "Flex Practitioner progress callback failed",
    "publication": "Flex Practitioner publication failed",
    "replay": "Flex Practitioner publication replay is not current",
    "root_retryable": "Flex Practitioner acquisition root is retryable",
    "root_unsealable": "Flex Practitioner acquisition root cannot be sealed",
    "source_drift": "Flex Practitioner exact source has drifted",
    "state": "Flex Practitioner operation state is invalid",
}


class UHCFlexPractitionerOperatorError(RuntimeError):
    """Expose one bounded failure without provider or transport details."""

    def __init__(self, code: str) -> None:
        self.code = code if code in _MESSAGE_BY_CODE else "evidence"
        super().__init__(_MESSAGE_BY_CODE[self.code])


def _is_enabled(variable_name: str) -> bool:
    return os.getenv(variable_name, "") == "true"


def require_uhc_flex_practitioner_operator_gate(phase: str) -> None:
    """Require exactly the selected one-shot phase before runtime imports."""

    expected_gate = _GATE_BY_PHASE.get(phase)
    if expected_gate is None:
        raise UHCFlexPractitionerOperatorError("invalid_request")
    if phase == "acquire-admit":
        raise UHCFlexPractitionerOperatorError("disabled")
    enabled_gates = {
        gate_name for gate_name in _GATE_BY_PHASE.values() if _is_enabled(gate_name)
    }
    if len(enabled_gates) > 1:
        raise UHCFlexPractitionerOperatorError("gate_conflict")
    if enabled_gates != {expected_gate}:
        raise UHCFlexPractitionerOperatorError("disabled")


def _json_text(payload_by_field: dict[str, Any]) -> str:
    try:
        return json.dumps(
            payload_by_field,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (MemoryError, OverflowError, RecursionError, TypeError, ValueError):
        raise UHCFlexPractitionerOperatorError("evidence") from None


def _cohort_result_json(sync_result: Any) -> str:
    cohort = sync_result.cohort
    return _json_text(
        {
            "cohort_complete": cohort.cohort_complete,
            "cohort_created": sync_result.created,
            "cohort_id": cohort.cohort_id,
            "endpoint_collection_complete": cohort.endpoint_collection_complete,
            "endpoint_complete": cohort.endpoint_complete,
            "npi_count": cohort.npi_count,
            "official_content_proof_sha256": (cohort.official_content_proof_sha256),
            "official_dataset_hash": cohort.official_dataset_hash,
            "official_dataset_id": cohort.official_dataset_id,
            "practitioner_resource_count": (cohort.practitioner_resource_count),
            "status": "sealed",
        }
    )


def _root_payload(root_receipt: Any) -> dict[str, Any]:
    return {
        "acquisition_id": root_receipt.acquisition_id,
        "elapsed_seconds": root_receipt.elapsed_seconds,
        "matched_count": root_receipt.matched_count,
        "resource_count": root_receipt.resource_count,
        "run_id": root_receipt.run_id,
        "terminal_set_sha256": root_receipt.terminal_set_sha256,
        "unmatched_count": root_receipt.unmatched_count,
    }


def _acquisition_result_json(receipt: Any) -> str:
    return _json_text(
        {
            "admission_id": receipt.admission_id,
            "baseline": _root_payload(receipt.baseline),
            "candidate": _root_payload(receipt.candidate),
            "cohort_id": receipt.cohort_id,
            "dataset_intent_id": receipt.dataset_intent_id,
            "elapsed_seconds": receipt.elapsed_seconds,
            "expected_npi_count": receipt.expected_npi_count,
            "official_content_proof_sha256": (receipt.official_content_proof_sha256),
            "official_dataset_hash": receipt.official_dataset_hash,
            "official_dataset_id": receipt.official_dataset_id,
            "operation_key": receipt.operation_key,
            "profile_delta_dispatch": {
                "operator_command_available": False,
                "status": "not_applicable_before_publication",
            },
            "semantic_projection_as_of": receipt.semantic_projection_as_of,
            "status": "admitted",
            "twin_attempt_id": receipt.twin_attempt_id,
        }
    )


def _single_root_acquisition_result_json(receipt: Any) -> str:
    return _json_text(
        {
            "admission_id": receipt.admission_id,
            "candidate": _root_payload(receipt.candidate),
            "cohort_id": receipt.cohort_id,
            "dataset_intent_id": receipt.dataset_intent_id,
            "elapsed_seconds": receipt.elapsed_seconds,
            "expected_npi_count": receipt.expected_npi_count,
            "official_content_proof_sha256": (
                receipt.official_content_proof_sha256
            ),
            "official_dataset_hash": receipt.official_dataset_hash,
            "official_dataset_id": receipt.official_dataset_id,
            "operation_key": receipt.operation_key,
            "profile_delta_dispatch": {
                "operator_command_available": False,
                "status": "not_applicable_before_publication",
            },
            "provider_directory_reviewed_root_policy_v1": (
                receipt.reviewed_root_policy_json
            ),
            "semantic_projection_as_of": receipt.semantic_projection_as_of,
            "status": "admitted",
        }
    )


def _publication_result_json(publication_result: Any) -> str:
    dataset_readiness = publication_result.readiness
    return _json_text(
        {
            "admission_id": dataset_readiness.admission_id,
            "candidate_acquisition_id": dataset_readiness.candidate_acquisition_id,
            "cohort_complete": dataset_readiness.cohort_complete,
            "cohort_id": dataset_readiness.cohort_id,
            "dataset_hash": dataset_readiness.dataset_hash,
            "dataset_id": dataset_readiness.dataset_id,
            "dataset_intent_id": dataset_readiness.dataset_intent_id,
            "endpoint_collection_complete": (
                dataset_readiness.endpoint_collection_complete
            ),
            "endpoint_complete": dataset_readiness.endpoint_complete,
            "operation_key": dataset_readiness.operation_key,
            "previous_dataset_id": dataset_readiness.previous_dataset_id,
            "profile_delta_dispatch": {
                **profile_followup_receipt_metadata(),
                "external_followup": (
                    build_provider_directory_global_profile_followup(
                        source_id=dataset_readiness.source_id,
                        dataset_id=dataset_readiness.dataset_id,
                        parent_run_id=dataset_readiness.acquisition_root_run_id,
                    )
                ),
                "operator_command_available": False,
                "required_external_global_dispatch": True,
                "status": "not_dispatched",
            },
            "replayed": publication_result.replayed,
            "resource_count": dataset_readiness.resource_count,
            "semantic_projection_as_of": (dataset_readiness.semantic_projection_as_of),
            "status": "published",
        }
    )


def _operation_error(error: Exception, default_code: str) -> Exception:
    error_code = getattr(error, "code", None)
    if type(error_code) is str and error_code in _PRESERVED_ERROR_CODES:
        return UHCFlexPractitionerOperatorError(error_code)
    return UHCFlexPractitionerOperatorError(default_code)


async def sync_uhc_flex_practitioner_cohort_operation(
    *,
    database: Any,
) -> str:
    """Seal or replay one exact official Practitioner NPI cohort."""

    require_uhc_flex_practitioner_operator_gate("sync-cohort")
    try:
        from process.uhc_flex_official_cohort_store import (
            sync_uhc_flex_official_cohort,
            UHCFlexOfficialCohortSyncResult,
        )

        result = await sync_uhc_flex_official_cohort(database=database)
        if type(result) is not UHCFlexOfficialCohortSyncResult:
            raise UHCFlexPractitionerOperatorError("evidence")
        return _cohort_result_json(result)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCFlexPractitionerOperatorError:
        raise
    except Exception as error:
        raise _operation_error(error, "evidence") from None


async def acquire_admit_uhc_flex_practitioner_operation(
    *,
    operation_key: str,
    semantic_projection_as_of: str,
    concurrency: int,
    max_attempts: int,
    lease_seconds: int,
    retry_base_seconds: float,
    max_retry_seconds: float,
    database: Any,
) -> str:
    """Resume, seal, compare, and admit exact baseline and candidate roots."""

    require_uhc_flex_practitioner_operator_gate("acquire-admit")
    try:
        from process.uhc_flex_practitioner_acquisition import (
            acquire_uhc_flex_practitioner_twins,
            UHCFlexPractitionerAcquisitionConfig,
            UHCFlexPractitionerAcquisitionReceipt,
        )

        config = UHCFlexPractitionerAcquisitionConfig(
            enabled=True,
            concurrency=concurrency,
            max_attempts=max_attempts,
            lease_seconds=lease_seconds,
            retry_base_seconds=retry_base_seconds,
            max_retry_seconds=max_retry_seconds,
        )
        receipt = await acquire_uhc_flex_practitioner_twins(
            operation_key=operation_key,
            semantic_projection_as_of=semantic_projection_as_of,
            config=config,
            database=database,
        )
        if type(receipt) is not UHCFlexPractitionerAcquisitionReceipt:
            raise UHCFlexPractitionerOperatorError("evidence")
        return _acquisition_result_json(receipt)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCFlexPractitionerOperatorError:
        raise
    except (TypeError, ValueError):
        raise UHCFlexPractitionerOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "acquisition") from None


async def acquire_uhc_flex_single_root_operation(
    *,
    operation_key: str,
    semantic_projection_as_of: str,
    concurrency: int,
    max_attempts: int,
    lease_seconds: int,
    retry_base_seconds: float,
    max_retry_seconds: float,
    database: Any,
) -> str:
    """Acquire and admit one explicit reviewed candidate root."""

    require_uhc_flex_practitioner_operator_gate("acquire-admit-single-root")
    try:
        from process.uhc_flex_practitioner_acquisition import (
            acquire_uhc_flex_single_root,
            UHCFlexPractitionerAcquisitionConfig,
        )
        from process.uhc_flex_practitioner_single_root_contract import (
            UHCFlexPractitionerSingleRootReceipt,
        )

        config = UHCFlexPractitionerAcquisitionConfig(
            enabled=True,
            concurrency=concurrency,
            max_attempts=max_attempts,
            lease_seconds=lease_seconds,
            retry_base_seconds=retry_base_seconds,
            max_retry_seconds=max_retry_seconds,
        )
        receipt = await acquire_uhc_flex_single_root(
            operation_key=operation_key,
            semantic_projection_as_of=semantic_projection_as_of,
            config=config,
            database=database,
        )
        if type(receipt) is not UHCFlexPractitionerSingleRootReceipt:
            raise UHCFlexPractitionerOperatorError("evidence")
        return _single_root_acquisition_result_json(receipt)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCFlexPractitionerOperatorError:
        raise
    except (TypeError, ValueError):
        raise UHCFlexPractitionerOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "acquisition") from None


async def publish_admitted_uhc_flex_practitioner_operation(
    *,
    candidate_acquisition_id: str,
    batch_size: int,
    database: Any,
) -> str:
    """Publish or replay one exact admitted candidate without dispatching Profile."""

    require_uhc_flex_practitioner_operator_gate("publish-admitted")
    try:
        from process.uhc_flex_practitioner_publication import (
            publish_uhc_flex_practitioner_dataset,
            UHCFlexPractitionerPublicationResult,
        )

        publication_result = await publish_uhc_flex_practitioner_dataset(
            candidate_acquisition_id,
            batch_size=batch_size,
            database=database,
        )
        if type(publication_result) is not UHCFlexPractitionerPublicationResult:
            raise UHCFlexPractitionerOperatorError("evidence")
        return _publication_result_json(publication_result)
    except (asyncio.CancelledError, TimeoutError):
        raise
    except UHCFlexPractitionerOperatorError:
        raise
    except (TypeError, ValueError):
        raise UHCFlexPractitionerOperatorError("invalid_request") from None
    except Exception as error:
        raise _operation_error(error, "publication") from None


__all__ = (
    "ACQUISITION_ENABLED_ENV",
    "COHORT_ENABLED_ENV",
    "OPERATOR_PHASES",
    "PUBLICATION_ENABLED_ENV",
    "SINGLE_ROOT_ACQUISITION_ENABLED_ENV",
    "UHCFlexPractitionerOperatorError",
    "acquire_admit_uhc_flex_practitioner_operation",
    "acquire_uhc_flex_single_root_operation",
    "publish_admitted_uhc_flex_practitioner_operation",
    "require_uhc_flex_practitioner_operator_gate",
    "sync_uhc_flex_practitioner_cohort_operation",
)
