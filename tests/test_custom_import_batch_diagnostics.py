# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded failure categories cannot serialize arbitrary exception identities."""

from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from api import control_batch_diagnostics as diagnostics


@pytest.mark.parametrize(
    "error,category,reason",
    [
        (MemoryError("synthetic-detail"), "memory", "not_applicable"),
        (TimeoutError("synthetic-detail"), "timeout", "not_applicable"),
        (SQLAlchemyTimeoutError("synthetic-detail"), "database", "connection_pool_timeout"),
        (SQLAlchemyError("synthetic-detail"), "database", "unknown"),
        (OSError("synthetic-detail"), "io", "not_applicable"),
        (ValueError("synthetic-detail"), "invalid_state", "not_applicable"),
        (RuntimeError("synthetic-detail"), "runtime", "not_applicable"),
        (type("synthetic-detail", (Exception,), {})("synthetic-detail"), "unexpected", "not_applicable"),
    ],
)
def test_diagnostics_use_only_closed_categories(caplog, error, category, reason):
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == (
        f"custom_import_batch_unavailable stage=source_execute category={category} database_reason={reason}"
    )
    assert record.args == ("source_execute", category, reason)
    assert not record.exc_info and record.exc_text is None and record.stack_info is None
    assert "synthetic-detail" not in caplog.text


def test_logging_failure_preserves_unavailable_outcome(monkeypatch):
    monkeypatch.setattr(diagnostics.logger, "error", Mock(side_effect=RuntimeError("synthetic-detail")))
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.ADMISSION_EXECUTE, RuntimeError("synthetic-detail"))


def test_api_log_has_only_the_allowlisted_database_reason(caplog):
    error = SQLAlchemyError("synthetic SQL and row text")
    driver = Exception("synthetic driver detail")
    driver.sqlstate = "42501"
    error.orig = driver
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error)
    assert caplog.records[0].args == ("source_execute", "database", "insufficient_privilege")
    assert "synthetic" not in caplog.text and caplog.records[0].exc_info is False
