# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed, payload-free diagnostics for unavailable batch outcomes."""

import logging
from enum import StrEnum

from process.custom_import.failure_diagnostics import classify_failure

logger = logging.getLogger(__name__)


class BatchFailureStage(StrEnum):
    SOURCE_RECEIVE = "source_receive"
    SOURCE_AUTHORIZE = "source_authorize"
    SOURCE_EXECUTE = "source_execute"
    SOURCE_RECEIPT = "source_receipt"
    ADMISSION_RECEIVE = "admission_receive"
    ADMISSION_AUTHORIZE = "admission_authorize"
    ADMISSION_RETAINED = "admission_retained"
    ADMISSION_EXECUTE = "admission_execute"
    ADMISSION_RECEIPT = "admission_receipt"


def log_batch_failure(stage: BatchFailureStage, error: Exception) -> None:
    """Never log exception text, request data, or uncertain commit state."""

    try:
        category, database_reason = classify_failure(error)
        logger.error(
            "custom_import_batch_unavailable stage=%s category=%s database_reason=%s",
            stage.value,
            category,
            database_reason,
            exc_info=False,
            stack_info=False,
        )
    except Exception:
        # Diagnostics cannot replace the existing unavailable response.
        return
