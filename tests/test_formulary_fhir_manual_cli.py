# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CLI and control-plane isolation contracts for manual formulary sync."""

from __future__ import annotations

import asyncio

from click.testing import CliRunner
import pytest

import main
import process
import api.control_imports as control_imports_module
import process.formulary_fhir.manual_worker as manual_module
from api.control_imports import importer_names
from api.control_imports import importer_registry
from api.control_workers import worker_registry
from process.formulary_fhir.manual_worker import ManualSynchronizationError
from process.formulary_fhir.synchronizer import SynchronizationResult


COMMAND_NAME = "verify-formulary-fhir"
VALID_ARGUMENTS = [
    "--source-id",
    "source-alpha",
    "--run-id",
    "synthetic-run",
    "--cutoff",
    "2026-08-07T12:00:00Z",
    "--timeout-seconds",
    "60",
]


def _result() -> SynchronizationResult:
    return SynchronizationResult(
        "ffd_" + "1" * 48,
        "a" * 64,
        1,
        2,
        3,
        "b" * 64,
        "c" * 64,
        1,
        1,
        0,
        4,
        0,
        0,
    )


def _invoke(arguments: list[str]):
    return CliRunner().invoke(main.manage, [COMMAND_NAME, *arguments])


def test_manual_command_forwards_exact_required_values(monkeypatch):
    synchronization_calls: list[dict[str, object]] = []

    async def synchronize(**values):
        synchronization_calls.append(values)
        return _result()

    monkeypatch.setattr(
        manual_module,
        "synchronize_verified_dataset_manually",
        synchronize,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    command_result = _invoke(VALID_ARGUMENTS)

    assert command_result.exit_code == 0, command_result.output
    assert command_result.exception is None
    assert synchronization_calls == [
        {
            "source_id": "source-alpha",
            "run_id": "synthetic-run",
            "cutoff": "2026-08-07T12:00:00Z",
            "timeout_seconds": 60,
        }
    ]
    assert '"status":"verified"' in command_result.output
    assert "source-alpha" not in command_result.output
    assert "synthetic-run" not in command_result.output


@pytest.mark.parametrize(
    "missing_option",
    ["--source-id", "--run-id", "--cutoff", "--timeout-seconds"],
)
def test_manual_command_requires_every_explicit_input(monkeypatch, missing_option):
    synchronization_calls: list[bool] = []

    async def synchronize(**_values):
        synchronization_calls.append(True)
        return _result()

    monkeypatch.setattr(
        manual_module,
        "synchronize_verified_dataset_manually",
        synchronize,
    )
    option_index = VALID_ARGUMENTS.index(missing_option)
    arguments = VALID_ARGUMENTS[:option_index] + VALID_ARGUMENTS[option_index + 2 :]

    command_result = _invoke(arguments)

    assert command_result.exit_code == 2
    assert "Missing option" in command_result.output
    assert synchronization_calls == []


def test_manual_command_help_has_no_activation_or_publication_surface():
    command_result = _invoke(["--help"])

    assert command_result.exit_code == 0
    assert all(option in command_result.output for option in VALID_ARGUMENTS[::2])
    for forbidden_option in ("--publish", "--seed", "--activate", "--concurrency"):
        assert forbidden_option not in command_result.output


@pytest.mark.parametrize(
    ("raised_error", "expected_message"),
    [
        (
            ManualSynchronizationError("busy"),
            "FHIR formulary manual synchronization source is busy",
        ),
        (TimeoutError("private timeout"), "manual synchronization timed out"),
        (
            RuntimeError("https://private.invalid?token=secret"),
            "FHIR formulary manual synchronization failed",
        ),
    ],
)
def test_manual_command_errors_are_stable_and_redacted(
    monkeypatch,
    raised_error,
    expected_message,
):
    async def synchronize(**_values):
        raise raised_error

    monkeypatch.setattr(
        manual_module,
        "synchronize_verified_dataset_manually",
        synchronize,
    )
    monkeypatch.setattr(main, "_run_async", asyncio.run)

    command_result = _invoke(VALID_ARGUMENTS)

    assert command_result.exit_code == 1
    assert expected_message in command_result.output
    assert "private.invalid" not in command_result.output
    assert "token" not in command_result.output
    assert "secret" not in command_result.output


def test_manual_command_is_not_a_control_importer_or_worker():
    assert COMMAND_NAME in main.manage.commands
    assert COMMAND_NAME not in process.process_group.commands
    assert COMMAND_NAME not in process.process_group_end.commands
    assert COMMAND_NAME not in importer_names()
    assert COMMAND_NAME not in {
        entry["name"] for entry in importer_registry()
    }
    assert COMMAND_NAME not in control_imports_module._SINGLE_JOB_ADAPTERS
    assert COMMAND_NAME not in control_imports_module._CANCELABLE_IMPORTERS
    assert all(
        COMMAND_NAME not in worker_by_field["importers"]
        for worker_by_field in worker_registry()
    )
