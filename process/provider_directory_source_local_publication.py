# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-local cutover for validated retained Provider Directory datasets."""

from __future__ import annotations

import asyncio
import math
from typing import Any, Awaitable, Callable


async def _validated_source_local_fence(fhir: Any, candidate: Any, source_id: str) -> Any:
    """Resolve only the requested candidate for source-local promotion."""
    fence = await fhir._resolve_provider_directory_artifact_datasets(
        [source_id],
        should_select_validated_candidates=True,
    )
    if (
        len(fence.datasets) != 1
        or len(fence.promotion_datasets) != 1
        or fence.datasets[0].source_id != source_id
        or fence.datasets[0].dataset_id != candidate.dataset_id
        or fence.datasets[0].endpoint_id != candidate.endpoint_id
        or fence.datasets[0].evidence_run_id != candidate.acquisition_root_run_id
    ):
        raise RuntimeError("provider_directory_source_local_fence_invalid")
    return fence


async def publish_validated_source_local_dataset(
    fhir: Any,
    candidate: Any,
    source_id: str,
    before_cutover: Callable[[Any], Awaitable[None]] | None = None,
    before_cutover_timeout_seconds: float | None = None,
    after_promotion: Callable[[], Awaitable[None]] | None = None,
) -> None:
    """Promote one exact candidate without replacing global artifacts."""

    fence = await _validated_source_local_fence(fhir, candidate, source_id)
    if before_cutover is not None and (
        before_cutover_timeout_seconds is None
        or not math.isfinite(before_cutover_timeout_seconds)
        or before_cutover_timeout_seconds <= 0
    ):
        raise ValueError("before_cutover requires a finite positive timeout")
    try:
        timeout_seconds = fhir._provider_directory_artifact_transaction_timeout_seconds(fence)
        async with asyncio.timeout(before_cutover_timeout_seconds or timeout_seconds) as cutover_timeout:
            async with fhir.db.transaction() as session:
                if before_cutover is not None:
                    await before_cutover(session)
                    cutover_timeout.reschedule(asyncio.get_running_loop().time() + timeout_seconds)
                await fhir.db.status(
                    f"SET LOCAL lock_timeout = '{fhir.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT}';"
                )
                await fhir.db.status(
                    f"SET LOCAL statement_timeout = '{fhir.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT}';"
                )
                await fhir._lock_artifact_cutover_fence(fence)
                fhir._tighten_provider_directory_artifact_cutover_timeout(
                    cutover_timeout,
                    fence,
                )
                await fhir._promote_provider_directory_artifact_datasets(fence)
                if after_promotion is not None:
                    await after_promotion()
    except Exception as promotion_error:
        try:
            is_cutover_committed = await fhir._is_provider_directory_dataset_cutover_committed(fence)
        except Exception:
            fhir.LOGGER.warning(
                "Source-local cutover acknowledgement and committed-state verification both failed",
                exc_info=True,
            )
            is_cutover_committed = False
        if not is_cutover_committed:
            raise
        fhir.LOGGER.warning(
            "Source-local cutover acknowledgement was lost after commit (%s); "
            "verified exact dataset and source pointers",
            type(promotion_error).__name__,
        )
