# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Selector-free CLI tests for reviewed Provider Directory subset state."""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path
import signal
import sys
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_fhir_subset_activation import (
    ReviewedSubsetActivationError,
)
from process import provider_directory_fhir_subset_activation_evidence as evidence_api
from tests.provider_directory_fhir_subset_activation_support import activation_inputs


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    ROOT
    / "scripts"
    / "smoke"
    / "provider_directory_fhir_reviewed_subset_state.py"
)
RUNBOOK_PATH = (
    ROOT / "docs/imports/provider-directory-reviewed-subset-activation.md"
)


def test_operator_is_packaged_and_absent_from_ordinary_runtime_paths():
    """Keep activation explicit, image-packaged, and unreachable by default."""

    runtime_paths = [
        ROOT / "main.py",
        ROOT / "process" / "__init__.py",
        ROOT / "api" / "control_imports.py",
        ROOT / "api" / "control_workers.py",
    ]
    runtime_paths.extend((ROOT / "api" / "endpoint").glob("*.py"))
    forbidden_runtime_names = (
        "sync_reviewed_subset_verified_state",
        "provider_directory_fhir_reviewed_subset_state",
        "HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED",
    )
    for runtime_path in runtime_paths:
        runtime_source = runtime_path.read_text(encoding="utf-8")
        for forbidden_runtime_name in forbidden_runtime_names:
            assert forbidden_runtime_name not in runtime_source

    dockerfile_source = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    workflow_source = (ROOT / ".github/workflows/ci.yml").read_text(
        encoding="utf-8"
    )
    assert "COPY process/ /opt/process/" in dockerfile_source
    assert "COPY scripts/ /opt/scripts/" in dockerfile_source
    assert "COPY specs/ /opt/specs/" in dockerfile_source
    assert str(SCRIPT_PATH.relative_to(ROOT)) in workflow_source


def test_runbook_binds_neutral_review_and_separate_publication():
    """Keep the one-shot activation boundary operationally explicit."""

    runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
    provider_guide = (
        ROOT / "docs/imports/provider-directory-fhir.md"
    ).read_text(encoding="utf-8")
    for required_text in (
        "source_contract_sha256",
        "verification_source_scope_sha256",
        "completion_proof_sha256",
        "HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED=true",
        "sync-verified-state",
        "render-neutral-evidence",
        "does not publish",
        "already_applied",
        "READ COMMITTED",
    ):
        assert required_text in runbook
    assert "provider-directory-reviewed-subset-activation.md" in provider_guide


def _script_module():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_fhir_reviewed_subset_state_cli",
        SCRIPT_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Database:
    def __init__(self):
        self.disconnected = False

    async def disconnect(self):
        self.disconnected = True


def test_parser_accepts_only_fixed_command_and_no_selectors(capsys):
    script_module = _script_module()

    parsed = script_module._parser().parse_args([script_module.COMMAND])
    assert vars(parsed) == {"command": script_module.COMMAND}
    evidence_arguments = script_module._parser().parse_args(
        [script_module.EVIDENCE_COMMAND]
    )
    assert vars(evidence_arguments) == {
        "command": script_module.EVIDENCE_COMMAND
    }
    with pytest.raises(SystemExit) as error:
        script_module._parser().parse_args(
            [script_module.COMMAND, "--source-id", "private"]
        )

    assert error.value.code == 2
    assert capsys.readouterr().err == (
        '{"code":"invalid_arguments","status":"error"}\n'
    )


@pytest.mark.asyncio
async def test_runner_returns_result_and_always_disconnects(monkeypatch):
    script_module = _script_module()
    database = _Database()

    async def execute_state_sync(selected_database):
        assert selected_database is database
        return '{"activated":true,"already_applied":false,"status":"ok"}'

    monkeypatch.setattr(
        script_module,
        "_execute_state_sync",
        execute_state_sync,
    )
    monkeypatch.setattr(
        script_module,
        "_install_signal_handlers",
        lambda _task: type(
            "Registration",
            (),
            {"restore": lambda self: None},
        )(),
    )

    rendered_result = await script_module._run_operator(
        script_module.COMMAND,
        database=database,
    )

    assert rendered_result == (
        '{"activated":true,"already_applied":false,"status":"ok"}'
    )
    assert database.disconnected is True


@pytest.mark.asyncio
async def test_evidence_command_renders_only_the_neutral_manifest(monkeypatch):
    script_module = _script_module()
    source_record, _dataset_rows, evidence = activation_inputs()
    database = _Database()
    evidence_reader = AsyncMock(return_value=evidence)
    monkeypatch.setattr(
        evidence_api,
        "reviewed_subset_activation_evidence",
        evidence_reader,
    )

    rendered_manifest = await script_module._execute_operation(
        database,
        script_module.EVIDENCE_COMMAND,
    )

    evidence_reader.assert_awaited_once_with(database=database)
    assert source_record["source_id"] not in rendered_manifest
    assert source_record["endpoint_id"] not in rendered_manifest
    assert '"desired_candidate_status":"verified_two_matching_' in (
        rendered_manifest
    )


@pytest.mark.parametrize(
    ("error", "expected_code"),
    (
        (ReviewedSubsetActivationError("disabled"), "disabled"),
        (ReviewedSubsetActivationError("evidence"), "evidence"),
        (RuntimeError("private"), "failed"),
    ),
)
def test_command_projects_closed_errors(monkeypatch, capsys, error, expected_code):
    script_module = _script_module()

    async def fail_operation(_command):
        raise error

    monkeypatch.setattr(script_module, "_run_operator", fail_operation)

    assert script_module.run_command([script_module.COMMAND]) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == (
        f'{{"code":"{expected_code}","status":"error"}}\n'
    )
    assert "private" not in output.err


@pytest.mark.parametrize(
    ("signal_number", "expected_code"),
    ((signal.SIGINT, 130), (signal.SIGTERM, 143), (None, 1)),
)
def test_command_projects_cancellation(monkeypatch, capsys, signal_number, expected_code):
    script_module = _script_module()

    async def cancel_operation(_command):
        if signal_number is None:
            raise asyncio.CancelledError()
        raise asyncio.CancelledError(signal_number)

    monkeypatch.setattr(script_module, "_run_operator", cancel_operation)

    assert script_module.run_command([script_module.COMMAND]) == expected_code
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == '{"code":"canceled","status":"error"}\n'
