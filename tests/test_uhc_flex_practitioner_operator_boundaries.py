# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Failure, cleanup, and redaction edges for the Practitioner operator."""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import math
from pathlib import Path
import signal
import sys
from types import ModuleType
from typing import Any

import pytest

from process import uhc_flex_practitioner_operator as operator


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "smoke" / "uhc_flex_practitioner_operator.py"
OPERATION_KEY = "a" * 64
CANDIDATE_ACQUISITION_ID = "pdufpa_" + "b" * 48


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "uhc_flex_practitioner_operator_boundary_script",
        SCRIPT_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    script_module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = script_module
    module_spec.loader.exec_module(script_module)
    return script_module


def _enable_only(monkeypatch, selected_gate: str) -> None:
    for gate_name in (
        operator.COHORT_ENABLED_ENV,
        operator.ACQUISITION_ENABLED_ENV,
        operator.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected_gate else "false",
        )


def _allow_retired_operation_internals(monkeypatch) -> None:
    monkeypatch.setattr(
        operator,
        "require_uhc_flex_practitioner_operator_gate",
        lambda _: None,
    )


def test_invalid_phase_and_unserializable_receipt_fail_closed(monkeypatch) -> None:
    _enable_only(monkeypatch, operator.COHORT_ENABLED_ENV)
    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as phase_error:
        operator.require_uhc_flex_practitioner_operator_gate("unknown")
    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as json_error:
        operator._json_text({"invalid": math.nan})

    assert phase_error.value.code == "invalid_request"
    assert json_error.value.code == "evidence"


def test_internal_error_normalization_preserves_only_closed_codes() -> None:
    class StableError(RuntimeError):
        code = "busy"

    stable_error = StableError("private detail")
    normalized_stable = operator._operation_error(stable_error, "publication")
    normalized_unknown = operator._operation_error(
        RuntimeError("private detail"),
        "publication",
    )

    assert isinstance(
        normalized_stable,
        operator.UHCFlexPractitionerOperatorError,
    )
    assert normalized_stable.code == "busy"
    assert "private" not in str(normalized_stable)
    assert isinstance(
        normalized_unknown,
        operator.UHCFlexPractitionerOperatorError,
    )
    assert normalized_unknown.code == "publication"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase_failure", "expected_code"),
    [
        (None, "evidence"),
        (operator.UHCFlexPractitionerOperatorError("evidence"), "evidence"),
        (RuntimeError("private"), "evidence"),
    ],
)
async def test_cohort_phase_rejects_wrong_or_failed_results(
    monkeypatch,
    phase_failure,
    expected_code,
) -> None:
    _enable_only(monkeypatch, operator.COHORT_ENABLED_ENV)
    module_name = "process.uhc_flex_official_cohort_store"
    phase_module = ModuleType(module_name)

    class ExpectedResult:
        pass

    async def sync_cohort(*, database):
        del database
        if phase_failure is not None:
            raise phase_failure
        return object()

    phase_module.UHCFlexOfficialCohortSyncResult = ExpectedResult
    phase_module.sync_uhc_flex_official_cohort = sync_cohort
    monkeypatch.setitem(sys.modules, module_name, phase_module)

    with pytest.raises(Exception) as caught:
        await operator.sync_uhc_flex_practitioner_cohort_operation(database=object())

    assert getattr(caught.value, "code", None) == expected_code


def _acquisition_failure_module(phase_failure: BaseException | None) -> ModuleType:
    module_name = "process.uhc_flex_practitioner_acquisition"
    phase_module = ModuleType(module_name)

    class Config:
        def __init__(self, **config_by_field) -> None:
            if phase_failure is not None and isinstance(phase_failure, ValueError):
                raise phase_failure
            self.config_by_field = config_by_field

    class ExpectedReceipt:
        pass

    async def acquire_twins(**keyword_arguments):
        del keyword_arguments
        if phase_failure is not None:
            raise phase_failure
        return object()

    phase_module.UHCFlexPractitionerAcquisitionConfig = Config
    phase_module.UHCFlexPractitionerAcquisitionReceipt = ExpectedReceipt
    phase_module.acquire_uhc_flex_practitioner_twins = acquire_twins
    return phase_module


async def _run_acquisition_failure(monkeypatch, phase_failure):
    _enable_only(monkeypatch, operator.ACQUISITION_ENABLED_ENV)
    _allow_retired_operation_internals(monkeypatch)
    module_name = "process.uhc_flex_practitioner_acquisition"
    monkeypatch.setitem(
        sys.modules,
        module_name,
        _acquisition_failure_module(phase_failure),
    )
    return await operator.acquire_admit_uhc_flex_practitioner_operation(
        operation_key=OPERATION_KEY,
        semantic_projection_as_of="2026-08-10",
        concurrency=4,
        max_attempts=3,
        lease_seconds=300,
        retry_base_seconds=1.0,
        max_retry_seconds=60.0,
        database=object(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase_failure", "expected_code"),
    [
        (None, "evidence"),
        (ValueError("private"), "invalid_request"),
        (RuntimeError("private"), "acquisition"),
    ],
)
async def test_acquisition_phase_normalizes_failures(
    monkeypatch,
    phase_failure,
    expected_code,
) -> None:
    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        await _run_acquisition_failure(monkeypatch, phase_failure)

    assert caught.value.code == expected_code


@pytest.mark.asyncio
@pytest.mark.parametrize("phase_failure", [TimeoutError(), asyncio.CancelledError()])
async def test_acquisition_phase_preserves_control_flow(
    monkeypatch,
    phase_failure,
) -> None:
    with pytest.raises(type(phase_failure)):
        await _run_acquisition_failure(monkeypatch, phase_failure)


def _publication_failure_module(phase_failure: BaseException | None) -> ModuleType:
    module_name = "process.uhc_flex_practitioner_publication"
    phase_module = ModuleType(module_name)

    class ExpectedResult:
        pass

    async def publish(*args, **keyword_arguments):
        del args, keyword_arguments
        if phase_failure is not None:
            raise phase_failure
        return object()

    phase_module.UHCFlexPractitionerPublicationResult = ExpectedResult
    phase_module.publish_uhc_flex_practitioner_dataset = publish
    return phase_module


async def _run_publication_failure(monkeypatch, phase_failure):
    _enable_only(monkeypatch, operator.PUBLICATION_ENABLED_ENV)
    module_name = "process.uhc_flex_practitioner_publication"
    monkeypatch.setitem(
        sys.modules,
        module_name,
        _publication_failure_module(phase_failure),
    )
    return await operator.publish_admitted_uhc_flex_practitioner_operation(
        candidate_acquisition_id=CANDIDATE_ACQUISITION_ID,
        batch_size=500,
        database=object(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("phase_failure", "expected_code"),
    [
        (None, "evidence"),
        (ValueError("private"), "invalid_request"),
        (RuntimeError("private"), "publication"),
    ],
)
async def test_publication_phase_normalizes_failures(
    monkeypatch,
    phase_failure,
    expected_code,
) -> None:
    with pytest.raises(operator.UHCFlexPractitionerOperatorError) as caught:
        await _run_publication_failure(monkeypatch, phase_failure)

    assert caught.value.code == expected_code


@pytest.mark.asyncio
@pytest.mark.parametrize("phase_failure", [TimeoutError(), asyncio.CancelledError()])
async def test_publication_phase_preserves_control_flow(
    monkeypatch,
    phase_failure,
) -> None:
    with pytest.raises(type(phase_failure)):
        await _run_publication_failure(monkeypatch, phase_failure)


@pytest.mark.parametrize(
    ("validator_name", "raw_value"),
    [
        ("_projection_date", "2026-8-10"),
        ("_bounded_integer", "not-an-integer"),
        ("_bounded_integer", "01"),
        ("_retry_seconds", "not-a-number"),
        ("_retry_seconds", "nan"),
        ("_retry_seconds", " 1"),
    ],
)
def test_cli_scalar_validators_reject_noncanonical_values(
    validator_name,
    raw_value,
) -> None:
    script_module = _script_module()
    validator = getattr(script_module, validator_name)
    validator_arguments = (
        (raw_value, 1, 10) if validator_name == "_bounded_integer" else (raw_value,)
    )

    with pytest.raises(argparse.ArgumentTypeError):
        validator(*validator_arguments)


def test_cli_accepts_only_bounded_canonical_overrides() -> None:
    script_module = _script_module()
    with pytest.raises(argparse.ArgumentTypeError):
        script_module._projection_date("20260810")

    parsed_arguments = script_module._parser().parse_args(
        [
            "acquire-admit",
            "--operation-key",
            OPERATION_KEY,
            "--semantic-projection-as-of",
            "2026-08-10",
            "--concurrency",
            "6",
            "--max-attempts",
            "4",
            "--lease-seconds",
            "600",
            "--retry-base-seconds",
            "2.5",
            "--max-retry-seconds",
            "30",
        ]
    )

    assert parsed_arguments.concurrency == 6
    assert parsed_arguments.max_attempts == 4
    assert parsed_arguments.lease_seconds == 600
    assert parsed_arguments.retry_base_seconds == 2.5
    assert parsed_arguments.max_retry_seconds == 30.0


def test_script_bootstrap_restores_the_repository_import_path() -> None:
    while str(ROOT) in sys.path:
        sys.path.remove(str(ROOT))

    _script_module()

    assert sys.path[0] == str(ROOT)


class _FakeSignalLoop:
    def __init__(self, failing_signal: signal.Signals | None = None) -> None:
        self.failing_signal = failing_signal
        self.callbacks_by_signal: dict[signal.Signals, tuple[Any, tuple[Any, ...]]] = {}
        self.removed_signals: list[signal.Signals] = []

    def add_signal_handler(self, signal_number, callback, *arguments) -> None:
        if signal_number == self.failing_signal:
            raise RuntimeError("unsupported")
        self.callbacks_by_signal[signal_number] = (callback, arguments)

    def remove_signal_handler(self, signal_number) -> None:
        self.removed_signals.append(signal_number)
        self.callbacks_by_signal.pop(signal_number, None)


class _FakeTask:
    def __init__(self) -> None:
        self.canceled_by_signal: list[signal.Signals] = []

    def cancel(self, signal_number) -> None:
        self.canceled_by_signal.append(signal_number)


def test_signal_handlers_cancel_and_restore(monkeypatch) -> None:
    script_module = _script_module()
    operation_loop = _FakeSignalLoop()
    active_task = _FakeTask()
    monkeypatch.setattr(signal, "getsignal", lambda number: ("prior", number))
    restored_handlers = []
    monkeypatch.setattr(
        signal,
        "signal",
        lambda number, handler: restored_handlers.append((number, handler)),
    )

    registration = script_module._install_signal_handlers(
        active_task,
        operation_loop=operation_loop,
    )
    for signal_number, (callback, arguments) in tuple(
        operation_loop.callbacks_by_signal.items()
    ):
        callback(*arguments)
        assert arguments == (signal_number,)
    registration.restore()
    registration.restore()

    assert active_task.canceled_by_signal == list(script_module.SIGNALS)
    assert operation_loop.removed_signals == list(reversed(script_module.SIGNALS))
    assert len(restored_handlers) == 2


def test_partial_signal_setup_is_restored(monkeypatch) -> None:
    script_module = _script_module()
    operation_loop = _FakeSignalLoop(failing_signal=signal.SIGINT)
    monkeypatch.setattr(signal, "getsignal", lambda number: ("prior", number))
    restored_handlers = []
    monkeypatch.setattr(
        signal,
        "signal",
        lambda number, handler: restored_handlers.append((number, handler)),
    )

    with pytest.raises(RuntimeError, match="signal setup failed"):
        script_module._install_signal_handlers(
            _FakeTask(),
            operation_loop=operation_loop,
        )

    assert operation_loop.removed_signals == [signal.SIGTERM]
    assert restored_handlers == [(signal.SIGTERM, ("prior", signal.SIGTERM))]


@pytest.mark.asyncio
async def test_invalid_command_and_missing_task_are_rejected(monkeypatch) -> None:
    script_module = _script_module()
    with pytest.raises(RuntimeError, match="command is invalid"):
        await script_module._execute_operation(
            argparse.Namespace(command="unknown"),
            object(),
        )
    monkeypatch.setattr(asyncio, "current_task", lambda: None)
    with pytest.raises(RuntimeError, match="task is unavailable"):
        await script_module._run_operation(argparse.Namespace(command="sync-cohort"))


def test_stable_error_code_and_unsignaled_cancellation_exit_are_bounded() -> None:
    script_module = _script_module()

    class StableError(RuntimeError):
        code = "busy"

    assert script_module._operation_error_code(StableError()) == "busy"
    assert script_module._operation_error_code(RuntimeError()) == "failed"
    assert script_module._cancellation_exit_code(asyncio.CancelledError()) == 1
