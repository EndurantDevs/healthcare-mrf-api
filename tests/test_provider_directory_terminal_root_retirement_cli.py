# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Redaction and gate tests for the terminal root retirement CLI."""

from __future__ import annotations

import argparse
import json
from typing import Any

import pytest

from process import provider_directory_terminal_root_retirement_contract as contract
from scripts.smoke import provider_directory_terminal_root_retirement as cli


SELECTOR_ARGUMENTS = [
    "--source-id",
    "source-private",
    "--endpoint-id",
    "endpoint-private",
    "--dataset-id",
    "dataset-private",
    "--acquisition-root-run-id",
    "run-root-private",
    "--owner-run-id",
    "run-owner-private",
    "--expected-current-dataset-id",
    "dataset-current-private",
]


def test_dormant_gate_runs_before_parser_or_selectors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(contract.RETIREMENT_ENABLED_ENV, raising=False)
    monkeypatch.setattr(
        cli,
        "_parser",
        lambda: pytest.fail("dormant command reached argparse"),
    )

    exit_code = cli.run_command(["preview", *SELECTOR_ARGUMENTS])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'
    assert "private" not in captured.err


def test_enabled_parser_redacts_missing_and_unknown_arguments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(contract.RETIREMENT_ENABLED_ENV, "true")

    with pytest.raises(SystemExit) as missing:
        cli.run_command(["preview", "--source-id", "source-private"])
    assert missing.value.code == 2
    captured = capsys.readouterr()
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "private" not in captured.err

    with pytest.raises(SystemExit) as unknown:
        cli.run_command(["preview", *SELECTOR_ARGUMENTS, "--unknown-private"])
    assert unknown.value.code == 2
    captured = capsys.readouterr()
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "private" not in captured.err


def test_parser_separates_preview_from_token_bound_apply() -> None:
    preview = cli._parser().parse_args(["preview", *SELECTOR_ARGUMENTS])
    apply = cli._parser().parse_args(
        [
            "apply",
            *SELECTOR_ARGUMENTS,
            "--expected-evidence-sha256",
            "a" * 64,
        ]
    )

    assert preview.command == "preview"
    assert not hasattr(preview, "expected_evidence_sha256")
    assert apply.command == "apply"
    assert apply.expected_evidence_sha256 == "a" * 64


@pytest.mark.asyncio
async def test_execute_renders_identifier_free_preview_and_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(contract.RETIREMENT_ENABLED_ENV, "true")
    from process import provider_directory_terminal_root_retirement_operator as operator

    async def preview(_request: Any, *, database: Any) -> str:
        assert database is marker_database
        return "a" * 64

    async def apply(
        _request: Any, *, database: Any
    ) -> contract.TerminalRootRetirementResult:
        assert database is marker_database
        return contract.TerminalRootRetirementResult(
            retired=True,
            marker_sha256="b" * 64,
        )

    monkeypatch.setattr(operator, "preview_terminal_root_retirement", preview)
    monkeypatch.setattr(operator, "apply_terminal_root_retirement", apply)
    marker_database = object()
    preview_arguments = cli._parser().parse_args(["preview", *SELECTOR_ARGUMENTS])
    apply_arguments = cli._parser().parse_args(
        [
            "apply",
            *SELECTOR_ARGUMENTS,
            "--expected-evidence-sha256",
            "a" * 64,
        ]
    )

    preview_json = await cli._execute(preview_arguments, marker_database)
    apply_json = await cli._execute(apply_arguments, marker_database)

    assert json.loads(preview_json) == {
        "evidence_sha256": "a" * 64,
        "status": "ok",
    }
    assert json.loads(apply_json) == {
        "already_applied": False,
        "marker_sha256": "b" * 64,
        "retired": True,
        "status": "ok",
    }
    assert "private" not in preview_json + apply_json


def test_run_command_prints_closed_success_or_safe_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(contract.RETIREMENT_ENABLED_ENV, "true")

    async def success(_arguments: argparse.Namespace) -> str:
        return '{"evidence_sha256":"' + "a" * 64 + '","status":"ok"}'

    monkeypatch.setattr(cli, "_run_operator", success)
    assert cli.run_command(["preview", *SELECTOR_ARGUMENTS]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "evidence_sha256": "a" * 64,
        "status": "ok",
    }
    assert captured.err == ""

    async def failure(_arguments: argparse.Namespace) -> str:
        raise contract.TerminalRootRetirementError("evidence_invalid")

    monkeypatch.setattr(cli, "_run_operator", failure)
    assert cli.run_command(["preview", *SELECTOR_ARGUMENTS]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"evidence_invalid","status":"error"}\n'
