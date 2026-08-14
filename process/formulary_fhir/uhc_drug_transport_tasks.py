# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Settlement for bounded UHC formulary transport tasks."""

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from process.formulary_fhir.async_safety import drain_operation
from process.formulary_fhir.uhc_drug_acquisition_lease import UHCDrugSourceAcquisitionLeaseError
from process.formulary_fhir.uhc_drug_transport_contract import ProgressCallback
from process.formulary_fhir.uhc_drug_transport_contract import UHCDrugArtifactAcquisitionError


async def _invoke(callback: Callable[..., Any] | None, *args: Any) -> None:
    if callback is None:
        return
    callback_result = callback(*args)
    if inspect.isawaitable(callback_result):
        await callback_result


async def _join_cancelled_tasks(
    tasks: tuple[asyncio.Task[tuple[int, str, str, str, bool]], ...],
) -> None:
    await asyncio.gather(*tasks, return_exceptions=True)


def _failure_evidence(error: UHCDrugArtifactAcquisitionError) -> str:
    return "retryable_transport" if error.retryable else "artifact_processing"


async def _complete_pending_tasks(
    tasks: tuple[asyncio.Task[tuple[int, str, str, str, bool]], ...],
    *,
    progress_callback: ProgressCallback | None,
) -> tuple[int, tuple[str, ...]]:
    downloaded_byte_count = 0
    rejected_source_file_ids: list[str] = []
    failure_evidence_entries: list[str] = []
    try:
        for settled_count, completed_task in enumerate(asyncio.as_completed(tasks), start=1):
            try:
                artifact_bytes, family, file_name, source_file_id, is_rejected = await completed_task
            except asyncio.CancelledError:
                raise
            except UHCDrugSourceAcquisitionLeaseError:
                raise
            except UHCDrugArtifactAcquisitionError as error:
                failure_evidence_entries.append(_failure_evidence(error))
                continue
            except Exception:
                failure_evidence_entries.append("artifact_processing")
                continue
            if is_rejected:
                rejected_source_file_ids.append(source_file_id)
                continue
            downloaded_byte_count += artifact_bytes
            await _invoke(progress_callback, settled_count, len(tasks), family, file_name)
    except BaseException:
        for pending_task in tasks:
            pending_task.cancel()
        await drain_operation(_join_cancelled_tasks(tasks), preserve_cancellation=False)
        raise
    if failure_evidence_entries:
        immutable_evidence_entries = tuple(failure_evidence_entries)
        raise UHCDrugArtifactAcquisitionError(
            "UHC drug artifact set acquisition is incomplete",
            retryable=all(entry == "retryable_transport" for entry in immutable_evidence_entries),
            failure_evidence=immutable_evidence_entries,
        )
    return downloaded_byte_count, tuple(sorted(rejected_source_file_ids))


__all__ = ("_complete_pending_tasks", "_invoke", "_join_cancelled_tasks")
