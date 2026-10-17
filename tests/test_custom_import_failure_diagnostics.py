# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed diagnostics retain useful identity without rendering exception data."""

import asyncio
import json

import pytest
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from process.custom_import import failure_diagnostics as diagnostics


@pytest.mark.parametrize(
    "error,category,reason",
    [
        (KeyboardInterrupt(), "canceled", "not_applicable"),
        (asyncio.CancelledError(), "canceled", "not_applicable"),
        (MemoryError(), "memory", "not_applicable"),
        (TimeoutError(), "timeout", "not_applicable"),
        (SQLAlchemyTimeoutError(), "database", "connection_pool_timeout"),
        (SQLAlchemyError(), "database", "unknown"),
        (OSError(), "io", "not_applicable"),
        (ValueError(), "invalid_state", "not_applicable"),
        (TypeError(), "invalid_state", "not_applicable"),
        (KeyError(), "invalid_state", "not_applicable"),
        (RuntimeError(), "runtime", "not_applicable"),
        (Exception(), "unexpected", "not_applicable"),
    ],
)
def test_category_precedence_is_closed(error, category, reason):
    assert diagnostics.classify_failure(error) == (category, reason)
    assert category in diagnostics.FAILURE_CATEGORIES and reason in diagnostics.DATABASE_REASONS


@pytest.mark.parametrize("attribute", ["sqlstate", "pgcode"])
@pytest.mark.parametrize(
    "code,reason",
    [
        ("08006", "connection_failure"),
        ("40001", "serialization_failure"),
        ("40P01", "deadlock_detected"),
        ("42501", "insufficient_privilege"),
        ("42P01", "undefined_table"),
        ("23505", "unique_violation"),
        ("53200", "out_of_memory"),
        ("57014", "query_canceled"),
        ("57P01", "admin_shutdown"),
    ],
)
def test_exact_driver_codes_are_mapped_without_sql_or_parameters(attribute, code, reason):
    driver = Exception("synthetic payload")
    setattr(driver, attribute, code)
    error = DBAPIError("synthetic SQL", {"synthetic": "parameters"}, driver)
    assert diagnostics.classify_failure(error) == ("database", reason)


@pytest.mark.parametrize("code", ["not-a-code", "57014\n", b"57014", 57014, ["57014"], None])
def test_unknown_driver_values_are_not_reflected(code):
    driver = Exception("synthetic payload")
    driver.sqlstate = code
    assert diagnostics.classify_failure(DBAPIError("synthetic SQL", {}, driver)) == ("database", "unknown")


def test_exception_properties_rendering_and_arbitrary_attribute_hooks_are_never_called():
    class OpaqueError(SQLAlchemyError):
        @property
        def sqlstate(self):
            raise AssertionError("property evaluated")

        @property
        def orig(self):
            raise AssertionError("property evaluated")

        def __getattribute__(self, name):
            if name in {"sqlstate", "pgcode", "orig", "__cause__"}:
                raise AssertionError("attribute hook evaluated")
            return super().__getattribute__(name)

        def __str__(self):
            raise AssertionError("exception rendered")

    with diagnostics.failure_context() as context:
        snapshot = context.snapshot(OpaqueError("synthetic private value"))
    assert snapshot["database_reason"] == "unknown"
    assert "synthetic" not in json.dumps(snapshot)


def test_driver_code_subclasses_cannot_run_hash_or_equality_hooks():
    class OpaqueCode(str):
        def __hash__(self):
            raise AssertionError("code hashed")

    error = SQLAlchemyError()
    error.sqlstate = OpaqueCode("57014")
    assert diagnostics.classify_failure(error) == ("database", "unknown")


def test_category_and_chain_checks_cannot_invoke_class_properties():
    class OpaqueValue:
        @property
        def __class__(self):
            raise AssertionError("class property evaluated")

    class OpaqueError(Exception, OpaqueValue):
        """Expose a hostile attribute while retaining the real exception type."""

    error = OpaqueError("synthetic primary")
    error.orig = OpaqueValue()
    assert diagnostics.classify_failure(error) == ("unexpected", "not_applicable")


def test_unavailable_attributes_cannot_replace_primary_failure(monkeypatch):
    def unavailable(*_arguments):
        raise RuntimeError("synthetic inspection failure")

    monkeypatch.setattr(diagnostics, "getattr_static", unavailable)
    assert diagnostics.classify_failure(SQLAlchemyError("synthetic primary")) == ("database", "unknown")


def test_cause_and_original_traversal_is_cycle_safe_and_keeps_primary_category():
    primary, driver = MemoryError(), SQLAlchemyError()
    driver.sqlstate = "53200"
    primary.orig = primary
    primary.__cause__ = driver
    driver.__cause__ = primary
    assert diagnostics.classify_failure(primary) == ("memory", "out_of_memory")


def test_nested_pool_timeout_is_recognized_without_relabeling_primary():
    error = RuntimeError()
    error.__cause__ = SQLAlchemyTimeoutError()
    assert diagnostics.classify_failure(error) == ("runtime", "connection_pool_timeout")


def test_driver_chain_traversal_has_a_fixed_bound():
    root = current = SQLAlchemyError()
    for _ in range(8):
        current.orig = SQLAlchemyError()
        current = current.orig
    current.sqlstate = "57014"
    assert diagnostics.classify_failure(root) == ("database", "unknown")


@pytest.mark.parametrize("identifier", [None, True, False, 0, -1, 2**63, 1.5, "41", object()])
def test_untrusted_identifiers_are_null(identifier):
    with diagnostics.failure_context() as context:
        diagnostics.failure_identifiers(execution_id=identifier, build_id=identifier)
        snapshot = context.snapshot(ValueError())
    assert snapshot["execution_id"] is None and snapshot["build_id"] is None


def test_mutable_context_survives_asyncio_run_and_thread_context_copy():
    async def operation():
        await asyncio.to_thread(diagnostics.failure_identifiers, execution_id=41, build_id=2**63 - 1)
        with diagnostics.failure_phase(diagnostics.FailurePhase.SOURCE):
            error = SQLAlchemyError("synthetic SQL")
            error.sqlstate = "42501"
            raise error

    with diagnostics.failure_context() as context:
        with pytest.raises(SQLAlchemyError) as caught:
            asyncio.run(operation())
        assert context.snapshot(caught.value) == {
            "contract_version": "custom-import-failure/v1",
            "phase": "source",
            "category": "database",
            "database_reason": "insufficient_privilege",
            "execution_id": 41,
            "build_id": 2**63 - 1,
        }
    with diagnostics.failure_context() as fresh:
        assert fresh.snapshot(RuntimeError())["execution_id"] is None


def test_primary_failure_is_frozen_before_cleanup_can_replace_the_exception():
    with diagnostics.failure_context() as context:
        with pytest.raises(SQLAlchemyError), diagnostics.failure_phase(diagnostics.FailurePhase.CAPTURE):
            diagnostics.record_failure(ValueError("synthetic primary"))
            with diagnostics.failure_phase(diagnostics.FailurePhase.DISCONNECT):
                raise SQLAlchemyError("synthetic cleanup")
        snapshot = context.snapshot(RuntimeError())
    assert (snapshot["phase"], snapshot["category"], snapshot["database_reason"]) == (
        "capture",
        "invalid_state",
        "not_applicable",
    )


def test_handled_failure_does_not_contaminate_a_later_operation():
    with diagnostics.failure_context() as context:
        with diagnostics.failure_phase(diagnostics.FailurePhase.CAPTURE):
            with pytest.raises(RuntimeError), diagnostics.failure_phase(diagnostics.FailurePhase.GRAPH):
                raise RuntimeError("synthetic handled failure")
        assert context.failure is None
        with pytest.raises(SQLAlchemyTimeoutError), diagnostics.failure_phase(diagnostics.FailurePhase.DISCONNECT):
            raise SQLAlchemyTimeoutError("synthetic cleanup")
        snapshot = context.snapshot(RuntimeError())
    assert (snapshot["phase"], snapshot["category"], snapshot["database_reason"]) == (
        "disconnect",
        "database",
        "connection_pool_timeout",
    )


def test_diagnostics_are_optional_outside_the_operator_command():
    diagnostics.failure_identifiers(execution_id=41)
    diagnostics.record_failure(RuntimeError())
    with diagnostics.failure_phase(diagnostics.FailurePhase.SOURCE):
        has_observed = True
    assert has_observed
