# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""CLI contracts for the fixed default-off UHC formulary operator."""

from __future__ import annotations

import asyncio
import builtins
import importlib.util
from pathlib import Path
import signal
import sys
from types import ModuleType
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "smoke" / "uhc_formulary_operator.py"
RAW_SET_SHA256 = "a" * 64
RECEIPT_ID = "ffur_" + "b" * 48
INVALID_ARGUMENTS_JSON = '{"code":"invalid_arguments","status":"error"}\n'


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "uhc_formulary_operator_script",
        SCRIPT_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    script_module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_spec.name] = script_module
    module_spec.loader.exec_module(script_module)
    return script_module


def _forbid_runtime_imports(monkeypatch) -> None:
    original_import = builtins.__import__

    def guarded_import(module_name, *args, **kwargs):
        if module_name == "db.connection" or module_name.startswith(
            "process.formulary_fhir"
        ):
            raise AssertionError(
                f"runtime import before accepted arguments: {module_name}"
            )
        return original_import(module_name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)


def test_help_is_fixed_and_runtime_free(monkeypatch, capsys) -> None:
    """Help exposes only the exact hash and durable-receipt selectors."""

    script_module = _script_module()
    _forbid_runtime_imports(monkeypatch)

    with pytest.raises(SystemExit) as caught:
        script_module.run_command(["--help"])

    captured = capsys.readouterr()
    assert caught.value.code == 0
    assert captured.err == ""
    assert "acquire-admit" in captured.out
    assert "publish-admitted" in captured.out
    for forbidden_selector in (
        "--source-id",
        "--run-id",
        "--dataset-id",
        "--cutoff",
        "--generation",
        "--intent",
    ):
        assert forbidden_selector not in captured.out


@pytest.mark.parametrize(
    "arguments",
    [
        ["different", "--raw-set-sha256", RAW_SET_SHA256],
        ["acquire-admit", "--raw-set", RAW_SET_SHA256],
        ["acquire-admit", "--raw-set-sha256", "A" * 64],
        ["acquire-admit", "--raw-set-sha256", "secret-token"],
        ["publish-admitted", "--receipt-id", "ffur_secret-token"],
        ["publish-admitted", "--raw-set-sha256", RAW_SET_SHA256],
    ],
)
def test_invalid_arguments_are_redacted_before_runtime_import(
    monkeypatch,
    capsys,
    arguments,
) -> None:
    """Argument failures never echo caller-controlled selector values."""

    script_module = _script_module()
    _forbid_runtime_imports(monkeypatch)

    with pytest.raises(SystemExit) as caught:
        script_module.run_command(arguments)

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == INVALID_ARGUMENTS_JSON
    assert "secret" not in captured.err


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "command",
        "selector",
        "phase_module_name",
        "operation_name",
        "serializer_name",
        "selector_name",
    ),
    [
        (
            "acquire-admit",
            RAW_SET_SHA256,
            "process.formulary_fhir.uhc_drug_acquire_operation",
            "acquire_and_admit_uhc_drugs",
            "admission_result_json",
            "raw_set_sha256",
        ),
        (
            "publish-admitted",
            RECEIPT_ID,
            "process.formulary_fhir.uhc_drug_publish_operation",
            "publish_uhc_drug_receipt",
            "publication_result_json",
            "receipt_id",
        ),
    ],
)
async def test_execute_lazily_dispatches_only_one_phase(
    monkeypatch,
    command,
    selector,
    phase_module_name,
    operation_name,
    serializer_name,
    selector_name,
) -> None:
    """Publication does not import the acquisition phase and conversely."""

    script_module = _script_module()
    phase_module = ModuleType(phase_module_name)
    common_module_name = "process.formulary_fhir.uhc_drug_operation"
    common_module = ModuleType(common_module_name)
    database = object()
    operation_result = object()
    operation_calls: list[dict[str, Any]] = []

    async def fixed_operation(**keyword_arguments):
        operation_calls.append(keyword_arguments)
        return operation_result

    def fixed_serializer(candidate_result):
        assert candidate_result is operation_result
        return '{"status":"safe"}'

    setattr(phase_module, operation_name, fixed_operation)
    setattr(common_module, serializer_name, fixed_serializer)
    monkeypatch.setitem(sys.modules, phase_module_name, phase_module)
    monkeypatch.setitem(sys.modules, common_module_name, common_module)

    rendered = await script_module._execute_operation(
        command,
        selector,
        database,
    )

    assert rendered == '{"status":"safe"}'
    assert operation_calls == [
        {selector_name: selector, "database": database}
    ]


def test_success_prints_only_serializer_json(monkeypatch, capsys) -> None:
    """A successful operation prints one JSON object and no diagnostics."""

    script_module = _script_module()
    safe_payload = '{"receipt_id":"ffur_safe","status":"admitted"}'

    async def run_operation(command, selector):
        assert (command, selector) == ("acquire-admit", RAW_SET_SHA256)
        return safe_payload

    monkeypatch.setattr(script_module, "_run_operation", run_operation)

    exit_code = script_module.run_command(
        ["acquire-admit", "--raw-set-sha256", RAW_SET_SHA256]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == safe_payload + "\n"
    assert captured.err == ""


@pytest.mark.parametrize(
    ("raised_error", "expected_code", "expected_exit"),
    [
        (TimeoutError("private timeout"), "timeout", 1),
        (RuntimeError("private token=secret"), "failed", 1),
        (asyncio.CancelledError(signal.SIGINT), "canceled", 130),
        (asyncio.CancelledError(signal.SIGTERM), "canceled", 143),
    ],
)
def test_failures_and_signals_are_sanitized(
    monkeypatch,
    capsys,
    raised_error,
    expected_code,
    expected_exit,
) -> None:
    """Errors expose only a bounded code and signal-specific exit status."""

    script_module = _script_module()

    async def fail_operation(_command, _selector):
        raise raised_error

    monkeypatch.setattr(script_module, "_run_operation", fail_operation)

    exit_code = script_module.run_command(
        ["publish-admitted", "--receipt-id", RECEIPT_ID]
    )

    captured = capsys.readouterr()
    assert exit_code == expected_exit
    assert captured.out == ""
    assert captured.err == f'{{"code":"{expected_code}","status":"error"}}\n'
    assert "private" not in captured.err
    assert "secret" not in captured.err


@pytest.mark.asyncio
async def test_disconnect_drains_through_repeated_cancellation() -> None:
    """Database cleanup finishes before the original cancellation returns."""

    script_module = _script_module()
    disconnect_started = asyncio.Event()
    allow_disconnect = asyncio.Event()
    disconnect_events: list[str] = []

    class BlockingDatabase:
        async def disconnect(self) -> None:
            disconnect_started.set()
            await allow_disconnect.wait()
            disconnect_events.append("completed")

    drain_task = asyncio.create_task(
        script_module._drain_disconnect(
            BlockingDatabase(),
            preserve_cancellation=False,
        )
    )
    await disconnect_started.wait()
    drain_task.cancel(signal.SIGTERM)
    await asyncio.sleep(0)
    drain_task.cancel(signal.SIGINT)
    await asyncio.sleep(0)

    assert not drain_task.done()
    allow_disconnect.set()
    with pytest.raises(asyncio.CancelledError) as caught:
        await drain_task

    assert caught.value.args == (signal.SIGTERM,)
    assert disconnect_events == ["completed"]
