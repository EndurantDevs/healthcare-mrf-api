# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded failure categories cannot serialize arbitrary exception identities."""

from unittest.mock import Mock

import pytest
from sqlalchemy.exc import InterfaceError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from api import control_batch_diagnostics as diagnostics


@pytest.mark.parametrize(
    "error,category,reason,subtype",
    [
        (MemoryError("synthetic-detail"), "memory", "not_applicable", "not_applicable"),
        (TimeoutError("synthetic-detail"), "timeout", "not_applicable", "not_applicable"),
        (SQLAlchemyTimeoutError("synthetic-detail"), "database", "connection_pool_timeout", "sqlalchemy_timeout"),
        (SQLAlchemyError("synthetic-detail"), "database", "unknown", "unknown"),
        (OSError("synthetic-detail"), "io", "not_applicable", "not_applicable"),
        (ValueError("synthetic-detail"), "invalid_state", "not_applicable", "not_applicable"),
        (RuntimeError("synthetic-detail"), "runtime", "not_applicable", "not_applicable"),
        (
            type("synthetic-detail", (Exception,), {})("synthetic-detail"),
            "unexpected",
            "not_applicable",
            "not_applicable",
        ),
    ],
)
def test_diagnostics_use_only_closed_categories(caplog, error, category, reason, subtype):
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == (
        f"custom_import_batch_unavailable stage=source_execute category={category} database_reason={reason} "
        f"database_subtype={subtype} connection_invalidated=False retry_blocked=False"
    )
    assert record.args == ("source_execute", category, reason, subtype, False, False)
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
    assert caplog.records[0].args == ("source_execute", "database", "insufficient_privilege", "unknown", False, False)
    assert "synthetic" not in caplog.text and caplog.records[0].exc_info is False


def test_interface_log_is_payload_free(caplog):
    class SensitiveDriverError(Exception):
        def __str__(self):
            raise AssertionError("driver error rendered")

    error = InterfaceError("synthetic SQL", {"synthetic": "parameters"}, Exception(), connection_invalidated=True)
    error.orig = SensitiveDriverError()
    error._custom_import_retry_blocked = True
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error)
    record = caplog.records[0]
    assert record.args == ("source_execute", "database", "unknown", "sqlalchemy_interface", True, True)
    assert not record.exc_info and record.exc_text is None and record.stack_info is None
    assert "synthetic" not in caplog.text and "SensitiveDriverError" not in caplog.text


def test_retried_failure_has_a_distinct_closed_message_key(caplog):
    error = InterfaceError("synthetic SQL", {"synthetic": "parameters"}, Exception(), connection_invalidated=True)
    diagnostics.log_batch_failure(diagnostics.BatchFailureStage.SOURCE_EXECUTE, error, is_retry=True)
    record = caplog.records[0]
    assert record.getMessage().startswith("custom_import_batch_retry_attempted stage=source_execute ")
    assert "custom_import_batch_unavailable" not in caplog.text
    assert record.args == ("source_execute", "database", "unknown", "sqlalchemy_interface", True, False)
    assert not record.exc_info and record.exc_text is None and record.stack_info is None
    assert "synthetic" not in caplog.text
