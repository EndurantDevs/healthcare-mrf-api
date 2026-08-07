# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CLI contracts for the fixed synthetic formulary seed candidate."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

from process.formulary_fhir.synthetic_canary_contract import CANARY_ENABLED_ENV


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts" / "smoke" / "formulary_fhir_synthetic_canary.py"


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "formulary_fhir_synthetic_canary_script",
        SCRIPT_PATH,
    )
    if module_spec is None or module_spec.loader is None:
        raise AssertionError("synthetic canary script is unavailable")
    script_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(script_module)
    return script_module


def test_script_help_exposes_only_fixed_candidate_command(monkeypatch, capsys):
    script_module = _script_module()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "--help"])

    with pytest.raises(SystemExit) as caught:
        script_module.main()

    output = capsys.readouterr().out
    assert caught.value.code == 0
    assert "verify-seed" in output
    assert "publish" not in output
    assert "source-id" not in output
    assert "run-id" not in output
    assert "cutoff" not in output


@pytest.mark.parametrize("disabled_value", [None, "", "0", "false", "typo"])
def test_script_is_default_off_with_safe_error(
    monkeypatch,
    capsys,
    disabled_value,
):
    if disabled_value is None:
        monkeypatch.delenv(CANARY_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(CANARY_ENABLED_ENV, disabled_value)
    script_module = _script_module()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "verify-seed"])

    exit_code = script_module.main()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'


def test_script_success_prints_only_candidate_json(monkeypatch, capsys):
    script_module = _script_module()
    safe_result = '{"dataset_id":"ffd_' + ("1" * 48) + '","status":"verified"}'

    async def run_candidate(command: str) -> str:
        assert command == "verify-seed"
        return safe_result

    monkeypatch.setattr(script_module, "_run", run_candidate)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "verify-seed"])

    exit_code = script_module.main()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == safe_result + "\n"
    assert captured.err == ""


def test_script_sanitizes_unexpected_error(monkeypatch, capsys):
    script_module = _script_module()

    async def fail_candidate(_command: str) -> str:
        raise RuntimeError(
            "https://private.example.invalid/fhir?token=secret-cursor"
        )

    monkeypatch.setattr(script_module, "_run", fail_candidate)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "verify-seed"])

    exit_code = script_module.main()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"failed","status":"error"}\n'
    assert "private" not in captured.err
    assert "secret" not in captured.err


def test_script_reports_timeout_with_stable_error(monkeypatch, capsys):
    script_module = _script_module()

    async def timeout_candidate(_command: str) -> str:
        raise TimeoutError("private timeout detail")

    monkeypatch.setattr(script_module, "_run", timeout_candidate)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "verify-seed"])

    exit_code = script_module.main()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"timeout","status":"error"}\n'
    assert "private" not in captured.err
