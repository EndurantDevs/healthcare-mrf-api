# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Configuration and sanitized contracts for UHC drug transport."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import Any, AsyncContextManager

import aiohttp


DEFAULT_MAX_FILE_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_TIMEOUT_SECONDS = 30 * 60
DEFAULT_CONCURRENCY = 4
MAX_CONCURRENCY = 8
DEFAULT_MAX_TOTAL_BYTES = 64 * 1024 * 1024 * 1024
DEFAULT_MIN_FREE_BYTES = 5 * 1024 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
USER_AGENT = "HealthPorta-UHC-Formulary-Artifacts/1.0"

CancelCheck = Callable[[], Awaitable[None] | None]
ClaimCheck = Callable[[], Awaitable[None]]
ProgressCallback = Callable[[int, int, str, str], Awaitable[None] | None]
SessionFactory = Callable[[aiohttp.ClientTimeout], AsyncContextManager[Any]]


class UHCDrugArtifactAcquisitionError(RuntimeError):
    """Report one bounded artifact-acquisition failure without source values."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        failure_evidence: tuple[str, ...] = (),
    ) -> None:
        allowed_evidence_codes = {
            "artifact_processing",
            "artifact_rejected",
            "retryable_transport",
        }
        if type(failure_evidence) is not tuple or any(
            type(evidence) is not str or evidence not in allowed_evidence_codes
            for evidence in failure_evidence
        ):
            raise ValueError("UHC drug acquisition failure evidence is invalid")
        self.retryable = retryable is True
        self.is_retryable = self.retryable
        self.failure_evidence = failure_evidence
        self.failure_count = len(failure_evidence)
        self.retryable_failure_count = failure_evidence.count("retryable_transport")
        super().__init__(message)


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value in (None, ""):
        return default
    try:
        configured_value = int(raw_value)
    except ValueError:
        raise UHCDrugArtifactAcquisitionError(
            f"{name} must be a positive integer"
        ) from None
    if not 0 < configured_value <= 2**63 - 1:
        raise UHCDrugArtifactAcquisitionError(f"{name} must be a positive integer")
    return configured_value


def uhc_drug_download_concurrency() -> int:
    """Return the bounded number of simultaneous drug-file downloads."""

    configured_concurrency = _positive_environment_integer(
        "HLTHPRT_UHC_FORMULARY_DOWNLOAD_CONCURRENCY",
        DEFAULT_CONCURRENCY,
    )
    if configured_concurrency > MAX_CONCURRENCY:
        raise UHCDrugArtifactAcquisitionError(
            "HLTHPRT_UHC_FORMULARY_DOWNLOAD_CONCURRENCY exceeds its bound"
        )
    return configured_concurrency


__all__ = (
    "CancelCheck",
    "ClaimCheck",
    "DEFAULT_CONCURRENCY",
    "DEFAULT_MAX_FILE_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "DEFAULT_MIN_FREE_BYTES",
    "DEFAULT_TIMEOUT_SECONDS",
    "DOWNLOAD_CHUNK_BYTES",
    "MAX_CONCURRENCY",
    "ProgressCallback",
    "SessionFactory",
    "UHCDrugArtifactAcquisitionError",
    "USER_AGENT",
    "uhc_drug_download_concurrency",
)
