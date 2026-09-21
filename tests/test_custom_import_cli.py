# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for the bounded custom-import operator slice."""

from __future__ import annotations

import json
import subprocess
import sys
from io import BytesIO
from pathlib import Path

import pytest

from process.custom_import import cli
from process.custom_import.definition_store import RegisteredDefinition

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


class _TtyBytes(BytesIO):
    def is_tty(self) -> bool:
        return True

    isatty = is_tty


def test_validate_reads_bounded_json_stdin_and_returns_only_a_receipt(capsys):
    exit_code = cli.run_command(
        ["validate", "--format", "json"],
        stream=BytesIO((FIXTURES / "v1_valid.json").read_bytes()),
    )

    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    assert exit_code == 0
    assert captured.err == ""
    assert receipt["status"] == "valid"
    assert set(receipt) == {
        "definition_digest",
        "definition_revision",
        "schema_digest",
        "schema_revision",
        "status",
    }


def test_validate_rejects_tty_or_path_arguments_without_reflecting_input(capsys):
    exit_code = cli.run_command(["validate", "--format", "json"], stream=_TtyBytes())

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_definition","status":"error"}\n'

    with pytest.raises(SystemExit) as caught:
        cli.run_command(["validate", "--format", "json", "--input", "/tmp/synthetic-definition"])

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "synthetic-definition" not in captured.err


def test_module_cli_validates_piped_synthetic_input(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "custom_import_cli", "validate", "--format", "json"],
        cwd=Path(__file__).resolve().parents[1],
        env={"PYTHONPYCACHEPREFIX": str(tmp_path / "pycache"), "PYTHONWARNINGS": "error"},
        input=(FIXTURES / "v1_valid.json").read_bytes(),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0
    assert completed.stderr == b""
    assert json.loads(completed.stdout)["status"] == "valid"


def test_module_cli_redacts_application_import_failures():
    marker = "synthetic-secret-marker"
    completed = subprocess.run(
        [sys.executable, "-m", "custom_import_cli", "validate", "--format", "json"],
        cwd=Path(__file__).resolve().parents[1],
        env={"HLTHPRT_SECONDS_PER_MB": marker},
        input=(FIXTURES / "v1_valid.json").read_bytes(),
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 1
    assert completed.stdout == b""
    assert completed.stderr == b'{"code":"failed","status":"error"}\n'
    assert marker.encode() not in completed.stderr
    assert b"Traceback" not in completed.stderr


@pytest.mark.asyncio
async def test_registration_uses_the_injected_transaction_and_existing_registry(monkeypatch):
    session = object()
    calls = []
    expected = RegisteredDefinition(
        dataset_id=7,
        definition_revision_id=8,
        schema_revision_id=9,
        created=True,
    )

    async def register(injected_session, dataset_key, definition):
        calls.append((injected_session, dataset_key, definition))
        return expected

    monkeypatch.setattr("process.custom_import.definition_store.register_definition", register)
    result = await cli.register_definition_from_stdin(
        session,
        dataset_key="synthetic_dataset",
        definition_format="yaml",
        stream=BytesIO((FIXTURES / "v1_valid.yaml").read_bytes()),
    )

    assert result is expected
    assert calls[0][0] is session
    assert calls[0][1] == "synthetic_dataset"
    assert (
        calls[0][2].digest
        == cli.load_definition_from_stdin("json", stream=BytesIO((FIXTURES / "v1_valid.json").read_bytes())).digest
    )
