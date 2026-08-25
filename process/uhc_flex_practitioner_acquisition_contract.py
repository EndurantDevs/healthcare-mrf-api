# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Public contracts for exact-cohort Flex Practitioner acquisition."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import math
import re
import time
from typing import Any

from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_store_contract import ACQUISITION_PATTERN
from process.uhc_flex_practitioner_transport import (
    UHC_FLEX_PRACTITIONER_MAX_RETRY_AFTER_SECONDS,
)
from process.uhc_flex_practitioner_twin_store_contract import (
    ADMISSION_PATTERN,
    ATTEMPT_PATTERN,
    build_uhc_flex_practitioner_dataset_intent_id,
    build_uhc_flex_practitioner_run_id,
    canonical_semantic_projection_as_of,
)


UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_CONCURRENCY = 4
UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY = 32
UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_ATTEMPTS = 3
UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS = 8
UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_RETRY_SECONDS = 1.0
UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS = (
    UHC_FLEX_PRACTITIONER_MAX_RETRY_AFTER_SECONDS
)

ROOT_ROLES = ("baseline", "candidate")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
RUN_PATTERN = re.compile(r"pdufpr_[0-9a-f]{48}\Z")
PROGRESS_PHASES = frozenset(
    {"root_started", "retry_released", "terminal", "root_sealed"}
)


class UHCFlexPractitionerAcquisitionError(RuntimeError):
    """Expose a bounded orchestration failure without request or response data."""

    def __init__(self, code: str = "state") -> None:
        message_by_code = {
            "disabled": "Flex Practitioner acquisition is disabled",
            "progress": "Flex Practitioner aggregate progress callback failed",
            "root_retryable": "Flex Practitioner acquisition root is retryable",
            "root_unsealable": "Flex Practitioner acquisition root cannot be sealed",
            "source_drift": "Flex Practitioner exact source changed during acquisition",
            "cohort_drift": "Flex Practitioner official cohort changed during acquisition",
            "state": "Flex Practitioner acquisition orchestration is invalid",
        }
        self.code = code if code in message_by_code else "state"
        super().__init__(message_by_code[self.code])


def strict_nonnegative_seconds(value: object, label: str) -> float:
    """Validate and return one finite, nonnegative duration."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Flex Practitioner {label} is invalid")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds < 0.0:
        raise ValueError(f"Flex Practitioner {label} is invalid")
    return seconds


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerAcquisitionConfig:
    """Manual-only execution bounds; construction alone never enables work."""

    enabled: bool = False
    concurrency: int = UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_CONCURRENCY
    max_attempts: int = UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_ATTEMPTS
    lease_seconds: int = 300
    retry_base_seconds: float = (
        UHC_FLEX_PRACTITIONER_ACQUISITION_DEFAULT_RETRY_SECONDS
    )
    max_retry_seconds: float = (
        UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS
    )

    def __post_init__(self) -> None:
        retry_base_seconds = strict_nonnegative_seconds(
            self.retry_base_seconds,
            "retry base seconds",
        )
        max_retry_seconds = strict_nonnegative_seconds(
            self.max_retry_seconds,
            "maximum retry seconds",
        )
        if (
            type(self.enabled) is not bool
            or type(self.concurrency) is not int
            or not 1
            <= self.concurrency
            <= UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_CONCURRENCY
            or type(self.max_attempts) is not int
            or not 1
            <= self.max_attempts
            <= UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_ATTEMPTS
            or type(self.lease_seconds) is not int
            or not 30 <= self.lease_seconds <= 3600
            or retry_base_seconds <= 0.0
            or retry_base_seconds > max_retry_seconds
            or max_retry_seconds
            > UHC_FLEX_PRACTITIONER_ACQUISITION_MAX_RETRY_SECONDS
        ):
            raise ValueError("Flex Practitioner acquisition config is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerAcquisitionProgress:
    """One aggregate-only progress observation with no member identity."""

    acquisition_role: str
    phase: str
    worker_count: int
    claim_count: int
    retry_count: int
    matched_count: int
    unmatched_count: int
    error_count: int

    def __post_init__(self) -> None:
        counts = (
            self.worker_count,
            self.claim_count,
            self.retry_count,
            self.matched_count,
            self.unmatched_count,
            self.error_count,
        )
        if (
            self.acquisition_role not in ROOT_ROLES
            or self.phase not in PROGRESS_PHASES
            or any(type(count) is not int or count < 0 for count in counts)
            or self.worker_count < 1
        ):
            raise ValueError("Flex Practitioner aggregate progress is invalid")


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerRootReceipt:
    """Compact proof that one role reached an error-free sealed census."""

    acquisition_role: str
    acquisition_id: str = field(repr=False)
    run_id: str = field(repr=False)
    matched_count: int
    unmatched_count: int
    resource_count: int
    terminal_set_sha256: str = field(repr=False)
    elapsed_seconds: float

    def __post_init__(self) -> None:
        if (
            self.acquisition_role not in ROOT_ROLES
            or type(self.acquisition_id) is not str
            or ACQUISITION_PATTERN.fullmatch(self.acquisition_id) is None
            or type(self.run_id) is not str
            or RUN_PATTERN.fullmatch(self.run_id) is None
            or any(
                type(count) is not int or count < 0
                for count in (
                    self.matched_count,
                    self.unmatched_count,
                    self.resource_count,
                )
            )
            or type(self.terminal_set_sha256) is not str
            or SHA256_PATTERN.fullmatch(self.terminal_set_sha256) is None
        ):
            raise ValueError("Flex Practitioner root receipt is invalid")
        strict_nonnegative_seconds(self.elapsed_seconds, "root timing")


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerAcquisitionReceipt:
    """Immutable acquisition and admission proof; it publishes nothing."""

    operation_key: str = field(repr=False)
    semantic_projection_as_of: str
    source_id: str = field(repr=False)
    endpoint_id: str = field(repr=False)
    cohort_id: str = field(repr=False)
    official_dataset_id: str = field(repr=False)
    official_dataset_hash: str = field(repr=False)
    official_content_proof_sha256: str = field(repr=False)
    dataset_intent_id: str = field(repr=False)
    expected_npi_count: int
    baseline: UHCFlexPractitionerRootReceipt
    candidate: UHCFlexPractitionerRootReceipt
    twin_attempt_id: str = field(repr=False)
    admission_id: str = field(repr=False)
    elapsed_seconds: float

    def __post_init__(self) -> None:
        projection_date = canonical_semantic_projection_as_of(
            self.semantic_projection_as_of
        )
        expected_intent_id = build_uhc_flex_practitioner_dataset_intent_id(
            self.cohort_id,
            projection_date,
            self.operation_key,
        )
        if (
            self.source_id != UHC_FLEX_PRACTITIONER_SOURCE_ID
            or type(self.endpoint_id) is not str
            or SHA256_PATTERN.fullmatch(self.endpoint_id) is None
            or type(self.official_dataset_id) is not str
            or not self.official_dataset_id
            or type(self.official_dataset_hash) is not str
            or SHA256_PATTERN.fullmatch(self.official_dataset_hash) is None
            or type(self.official_content_proof_sha256) is not str
            or SHA256_PATTERN.fullmatch(self.official_content_proof_sha256) is None
            or self.dataset_intent_id != expected_intent_id
            or type(self.expected_npi_count) is not int
            or self.expected_npi_count < 1
            or type(self.baseline) is not UHCFlexPractitionerRootReceipt
            or self.baseline.acquisition_role != "baseline"
            or type(self.candidate) is not UHCFlexPractitionerRootReceipt
            or self.candidate.acquisition_role != "candidate"
            or self.baseline.acquisition_id == self.candidate.acquisition_id
            or self.baseline.run_id
            != build_uhc_flex_practitioner_run_id(
                self.dataset_intent_id,
                "baseline",
            )
            or self.candidate.run_id
            != build_uhc_flex_practitioner_run_id(
                self.dataset_intent_id,
                "candidate",
            )
            or self.baseline.matched_count + self.baseline.unmatched_count
            != self.expected_npi_count
            or self.candidate.matched_count + self.candidate.unmatched_count
            != self.expected_npi_count
            or type(self.twin_attempt_id) is not str
            or ATTEMPT_PATTERN.fullmatch(self.twin_attempt_id) is None
            or type(self.admission_id) is not str
            or ADMISSION_PATTERN.fullmatch(self.admission_id) is None
        ):
            raise ValueError("Flex Practitioner acquisition receipt is invalid")
        strict_nonnegative_seconds(self.elapsed_seconds, "total timing")

    def __repr__(self) -> str:
        return (
            "<uhc-flex-practitioner-acquisition-receipt "
            f"expected={self.expected_npi_count} "
            f"matched={self.candidate.matched_count} "
            f"unmatched={self.candidate.unmatched_count}>"
        )


ProgressCallback = Callable[
    [UHCFlexPractitionerAcquisitionProgress],
    Awaitable[None] | None,
]


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerAcquisitionDependencies:
    """Narrow injection surface for offline orchestration tests."""

    register_source: Callable[..., Awaitable[Any]]
    sync_cohort: Callable[..., Awaitable[Any]]
    initialize_root: Callable[..., Awaitable[Any]]
    claim_work: Callable[..., Awaitable[Any]]
    fetch: Callable[..., Awaitable[Any]]
    complete_result: Callable[..., Awaitable[Any]]
    complete_error: Callable[..., Awaitable[Any]]
    release_work: Callable[..., Awaitable[Any]]
    seal_root: Callable[..., Awaitable[Any]]
    admit_twins: Callable[..., Awaitable[Any]]
    session_scope: Callable[[int], Any]
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    monotonic: Callable[[], float] = time.monotonic
    admit_single_root: Callable[..., Awaitable[Any]] | None = None


_PUBLIC_MODULE = "process.uhc_flex_practitioner_acquisition"
UHCFlexPractitionerAcquisitionError.__module__ = _PUBLIC_MODULE
UHCFlexPractitionerAcquisitionConfig.__module__ = _PUBLIC_MODULE
UHCFlexPractitionerAcquisitionProgress.__module__ = _PUBLIC_MODULE
UHCFlexPractitionerRootReceipt.__module__ = _PUBLIC_MODULE
UHCFlexPractitionerAcquisitionReceipt.__module__ = _PUBLIC_MODULE
UHCFlexPractitionerAcquisitionDependencies.__module__ = _PUBLIC_MODULE
