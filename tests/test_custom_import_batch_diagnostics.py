# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded failure categories cannot serialize arbitrary exception identities."""

from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from api import control_batch_diagnostics as diagnostics


@pytest.mark.parametrize(
    "error,category",
    [
        (MemoryError("synthetic-detail"), "memory"),
        (TimeoutError("synthetic-detail"), "timeout"),
        (SQLAlchemyError("synthetic-detail"), "database"),
        (OSError("synthetic-detail"), "io"),
        (ValueError("synthetic-detail"), "invalid_state"),
        (RuntimeError("synthetic-detail"), "runtime"),
        (type("synthetic-detail", (Exception,), {})("synthetic-detail"), "unexpected"),
    ],
)
def test_diagnostics_use_only_closed_categories(caplog, error, category):
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == f"custom_import_batch_unavailable stage=source_execute category={category}"
    assert record.args == ("source_execute", category)
    assert not record.exc_info and record.exc_text is None and record.stack_info is None
    assert "synthetic-detail" not in caplog.text


def test_logging_failure_preserves_unavailable_outcome(monkeypatch):
    monkeypatch.setattr(diagnostics.logger, "error", Mock(side_effect=RuntimeError("synthetic-detail")))
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.ADMISSION_EXECUTE, RuntimeError("synthetic-detail"))
