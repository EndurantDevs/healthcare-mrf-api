# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed, payload-free diagnostics for unavailable batch outcomes."""

import asyncio
import logging
from enum import StrEnum
from inspect import getattr_static

from sqlalchemy.exc import InterfaceError, OperationalError

from process.custom_import.failure_diagnostics import classify_failure, database_failure_details

logger = logging.getLogger(__name__)


def can_retry_batch_operation(error: Exception) -> bool:
    """Require an invalidated connection and completed, uncanceled cleanup."""

    try:
        task = asyncio.current_task()
        return (
            isinstance(error, (InterfaceError, OperationalError))
            and getattr_static(error, "connection_invalidated", False) is True
            and getattr_static(error, "_custom_import_retry_blocked", False) is False
            and (task is None or not task.cancelling())
        )
    except Exception:
        return False


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


def log_batch_failure(stage: BatchFailureStage, error: Exception, *, is_retry: bool = False) -> None:
    """Never log exception text, request data, or uncertain commit state."""

    try:
        category, database_reason = classify_failure(error)
        database_subtype, connection_invalidated, retry_blocked = database_failure_details(error)
        message_key = "custom_import_batch_retry_attempted" if is_retry is True else "custom_import_batch_unavailable"
        logger.error(
            f"{message_key} stage=%s category=%s database_reason=%s "
            "database_subtype=%s connection_invalidated=%s retry_blocked=%s",
            stage.value,
            category,
            database_reason,
            database_subtype,
            connection_invalidated,
            retry_blocked,
            exc_info=False,
            stack_info=False,
        )
    except Exception:
        # Diagnostics cannot replace the existing unavailable response.
        return
