# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CLI contracts for the fixed synthetic seed publisher."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest

from process.formulary_fhir.synthetic_canary_contract import (
    SEED_PUBLICATION_ENABLED_ENV,
)


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    ROOT
    / "scripts"
    / "smoke"
    / "formulary_fhir_synthetic_seed_publisher.py"
)


def _script_module():
    module_spec = importlib.util.spec_from_file_location(
        "formulary_fhir_synthetic_seed_publisher_script",
        SCRIPT_PATH,
    )
    if module_spec is None or module_spec.loader is None:
        raise AssertionError("synthetic seed publisher script is unavailable")
    script_module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(script_module)
    return script_module


def test_script_help_exposes_only_fixed_publication_command(monkeypatch, capsys):
    script_module = _script_module()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "--help"])

    with pytest.raises(SystemExit) as caught:
        script_module.run_command()

    output = capsys.readouterr().out
    assert caught.value.code == 0
    assert "publish-seed" in output
    assert "verify-seed" not in output
    for forbidden_selector in (
        "--source-id",
        "--run-id",
        "--dataset-id",
        "--cutoff",
        "--generation",
        "--intent",
    ):
        assert forbidden_selector not in output


@pytest.mark.parametrize("disabled_setting", [None, "", "0", "false", "typo"])
def test_script_is_default_off_with_safe_error(
    monkeypatch,
    capsys,
    disabled_setting,
):
    if disabled_setting is None:
        monkeypatch.delenv(SEED_PUBLICATION_ENABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(SEED_PUBLICATION_ENABLED_ENV, disabled_setting)
    script_module = _script_module()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "publish-seed"])

    exit_code = script_module.run_command()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'


def test_script_success_prints_only_publication_json(monkeypatch, capsys):
    script_module = _script_module()
    safe_publication = (
        '{"dataset_id":"ffd_'
        + ("1" * 48)
        + '","generation":1,"status":"published"}'
    )

    async def run_publication(command: str) -> str:
        assert command == "publish-seed"
        return safe_publication

    monkeypatch.setattr(script_module, "_run", run_publication)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "publish-seed"])

    exit_code = script_module.run_command()

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == safe_publication + "\n"
    assert captured.err == ""


@pytest.mark.parametrize(
    "raised_error,expected_code",
    [
        (TimeoutError("private timeout"), "timeout"),
        (
            RuntimeError(
                "https://private.example.invalid/fhir?token=secret-cursor"
            ),
            "failed",
        ),
    ],
)
def test_script_sanitizes_failures(
    monkeypatch,
    capsys,
    raised_error,
    expected_code,
):
    script_module = _script_module()

    async def fail_publication(_command: str) -> str:
        raise raised_error

    monkeypatch.setattr(script_module, "_run", fail_publication)
    monkeypatch.setattr(sys, "argv", [str(SCRIPT_PATH), "publish-seed"])

    exit_code = script_module.run_command()

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == f'{{"code":"{expected_code}","status":"error"}}\n'
    assert "private" not in captured.err
    assert "secret" not in captured.err


@pytest.mark.asyncio
async def test_script_run_calls_only_fixed_publisher(monkeypatch):
    script_module = _script_module()
    events: list[str] = []

    class _Publication:
        pass

    async def publish():
        events.append("publish")
        return _Publication()

    def render(publication):
        assert type(publication) is _Publication
        events.append("render")
        return '{"status":"published"}'

    import process.formulary_fhir.synthetic_seed_publisher as publisher_module

    monkeypatch.setattr(publisher_module, "publish_synthetic_seed", publish)
    monkeypatch.setattr(publisher_module, "publication_result_json", render)

    assert await script_module._run("publish-seed") == '{"status":"published"}'
    assert events == ["publish", "render"]
    with pytest.raises(RuntimeError, match="command is invalid"):
        await script_module._run("different")
