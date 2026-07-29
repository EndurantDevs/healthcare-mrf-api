# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Safe diagnostics for one authenticated candidate-audit partition."""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

from process.ptg_parts.ptg2_batch_candidate_audit import (
    BatchCandidateAuditContractError,
    BatchCandidateAuditTransportError,
)
from process.ptg_parts.ptg2_partitioned_candidate_audit_contract import (
    PartitionedCandidateAuditRequest,
)


logger = logging.getLogger(__name__)

PartitionFailure = (
    BatchCandidateAuditContractError | BatchCandidateAuditTransportError
)
PartitionFailureCallback = Callable[
    [int, int, PartitionFailure],
    Awaitable[None],
]
PartitionProgressCallback = Callable[[int, int], Awaitable[None]]


def bind_partition_failure(
    failure: PartitionFailure,
    request: PartitionedCandidateAuditRequest,
) -> PartitionFailure:
    """Bind one safe failure reason to its authenticated request identity."""

    return failure.for_partition(
        partition_index=request.partition_index,
        partition_count=request.partition_count,
        partition_digest=request.partition_digest,
        plan_digest=request.plan_digest,
        request_digest=request.request_digest,
    )


async def publish_partition_failure(
    failure_callback: PartitionFailureCallback | None,
    *,
    completed: int,
    total: int,
    failure: PartitionFailure,
) -> None:
    """Report safe request identity without making observability part of the gate."""

    if failure_callback is None:
        return
    try:
        await failure_callback(completed, total, failure)
    except Exception:
        logger.debug(
            "candidate audit partition failure callback failed",
            exc_info=True,
        )


async def publish_partition_progress(
    progress_callback: PartitionProgressCallback | None,
    *,
    completed: int,
    total: int,
) -> None:
    """Report bounded counters without making observability part of the gate."""

    if progress_callback is None:
        return
    try:
        await progress_callback(completed, total)
    except Exception:
        logger.debug(
            "candidate audit partition progress callback failed",
            exc_info=True,
        )


__all__ = [
    "PartitionFailure",
    "PartitionFailureCallback",
    "PartitionProgressCallback",
    "bind_partition_failure",
    "publish_partition_failure",
    "publish_partition_progress",
]
