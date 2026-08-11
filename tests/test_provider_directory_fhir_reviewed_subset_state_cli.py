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
from process.provider_directory_fhir_subset_activation_contract import (
    STATE_SYNC_ENABLED_ENV,
)
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_ENABLED_ENV,
    ReviewedSubsetAbandonmentError,
    ReviewedSubsetAbandonmentResult,
)
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    TERMINAL_DISPOSITION_ENABLED_ENV,
    ReviewedSubsetTerminalDispositionResult,
)
from process.provider_directory_fhir_subset_terminal_disposition_profile import (
    DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
    DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
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
        "abandon_reviewed_subset_expired_root",
        "provider_directory_fhir_reviewed_subset_state",
        "HLTHPRT_PROVIDER_DIRECTORY_SUBSET_STATE_SYNC_ENABLED",
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_ABANDONMENT_ENABLED",
        "dispose_reviewed_subset_census_drift_root",
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_"
        "TERMINAL_DISPOSITION_ENABLED",
        "dispose_v4_census_drift_root",
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_DIRECT_V4_"
        "TERMINAL_DISPOSITION_ENABLED",
        "dispose_v5_terminal_root",
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_DIRECT_V5_HTTP410_"
        "TERMINAL_DISPOSITION_ENABLED",
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
        "HLTHPRT_PROVIDER_DIRECTORY_REVIEWED_SUBSET_ABANDONMENT_ENABLED=true",
        "abandon-expired-root",
        "acquisition_abandoned",
        "seal commits before that guard is released",
        "does not publish, delete, reset, or reuse",
        "seal-direct-v5-http410-root",
        "six independently verified",
        "does not retry, validate, publish, activate, delete, or reuse",
    ):
        assert required_text in runbook
    assert "provider-directory-reviewed-subset-activation.md" in provider_guide
    assert (
        "HLTHPRT_PROVIDER_DIRECTORY_REST_PAGE_PREFETCH_SERVER_ISSUED_SUBSET=true"
        in provider_guide
    )


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
    abandonment_arguments = script_module._parser().parse_args(
        [script_module.ABANDON_COMMAND]
    )
    assert vars(abandonment_arguments) == {
        "command": script_module.ABANDON_COMMAND
    }
    terminal_arguments = script_module._parser().parse_args(
        [script_module.TERMINAL_DISPOSITION_COMMAND]
    )
    assert vars(terminal_arguments) == {
        "command": script_module.TERMINAL_DISPOSITION_COMMAND
    }
    direct_v4_arguments = script_module._parser().parse_args(
        [script_module.DIRECT_V4_TERMINAL_DISPOSITION_COMMAND]
    )
    assert vars(direct_v4_arguments) == {
        "command": script_module.DIRECT_V4_TERMINAL_DISPOSITION_COMMAND
    }
    direct_v5_arguments = script_module._parser().parse_args(
        [script_module.DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_COMMAND]
    )
    assert vars(direct_v5_arguments) == {
        "command": script_module.DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_COMMAND
    }
    with pytest.raises(SystemExit) as error:
        script_module._parser().parse_args(
            [script_module.COMMAND, "--source-id", "private"]
        )
    assert error.value.code == 2
    assert capsys.readouterr().err == (
        '{"code":"invalid_arguments","status":"error"}\n'
    )
    with pytest.raises(SystemExit) as abandonment_error:
        script_module._parser().parse_args(
            [script_module.ABANDON_COMMAND, "--dataset-id", "private"]
        )
    assert abandonment_error.value.code == 2
    assert capsys.readouterr().err == (
        '{"code":"invalid_arguments","status":"error"}\n'
    )


@pytest.mark.parametrize(
    ("command_name", "enabled_environment"),
    (
        ("COMMAND", STATE_SYNC_ENABLED_ENV),
        ("ABANDON_COMMAND", ABANDONMENT_ENABLED_ENV),
        (
            "TERMINAL_DISPOSITION_COMMAND",
            TERMINAL_DISPOSITION_ENABLED_ENV,
        ),
        (
            "DIRECT_V4_TERMINAL_DISPOSITION_COMMAND",
            DIRECT_V4_TERMINAL_DISPOSITION_ENABLED_ENV,
        ),
        (
            "DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_COMMAND",
            DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_ENABLED_ENV,
        ),
    ),
)
def test_disabled_mutations_stop_before_runtime_imports(
    monkeypatch,
    capsys,
    command_name,
    enabled_environment,
):
    """Emit one closed line without loading a database for a dormant command."""

    script_module = _script_module()
    command = getattr(script_module, command_name)
    operation = AsyncMock()
    monkeypatch.delenv(enabled_environment, raising=False)
    monkeypatch.setattr(script_module, "_execute_operation", operation)

    assert script_module._ENABLED_ENV_BY_COMMAND[command] == enabled_environment
    assert script_module.run_command([command]) == 1
    operation.assert_not_awaited()
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == '{"code":"disabled","status":"error"}\n'


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


@pytest.mark.asyncio
async def test_abandonment_command_renders_only_closed_disposition(monkeypatch):
    script_module = _script_module()
    database = _Database()
    abandonment_call = AsyncMock(
        return_value=ReviewedSubsetAbandonmentResult(abandoned=True)
    )
    monkeypatch.setattr(
        "process.provider_directory_fhir_subset_abandonment."
        "abandon_reviewed_subset_expired_root",
        abandonment_call,
    )

    rendered_result = await script_module._execute_operation(
        database,
        script_module.ABANDON_COMMAND,
    )

    abandonment_call.assert_awaited_once_with(database=database)
    assert rendered_result == (
        '{"abandoned":true,"already_applied":false,"status":"ok"}'
    )

    abandonment_call.return_value = ReviewedSubsetAbandonmentResult(
        abandoned=False
    )
    replay_result = await script_module._execute_operation(
        database,
        script_module.ABANDON_COMMAND,
    )
    assert replay_result == (
        '{"abandoned":false,"already_applied":true,"status":"ok"}'
    )


@pytest.mark.asyncio
async def test_terminal_command_renders_only_closed_disposition(monkeypatch):
    """Keep the mixed terminal seal selector-free and identifier-free."""

    script_module = _script_module()
    database = _Database()
    disposition_call = AsyncMock(
        return_value=ReviewedSubsetTerminalDispositionResult(disposed=True)
    )
    monkeypatch.setattr(
        "process.provider_directory_fhir_subset_terminal_disposition."
        "dispose_reviewed_subset_census_drift_root",
        disposition_call,
    )

    rendered_result = await script_module._execute_operation(
        database,
        script_module.TERMINAL_DISPOSITION_COMMAND,
    )

    disposition_call.assert_awaited_once_with(database=database)
    assert rendered_result == (
        '{"already_applied":false,"disposed":true,"status":"ok"}'
    )


@pytest.mark.asyncio
async def test_direct_v4_terminal_command_reuses_closed_disposition(monkeypatch):
    """Keep the direct-root profile on the existing identifier-free CLI."""

    script_module = _script_module()
    database = _Database()
    disposition_call = AsyncMock(
        return_value=ReviewedSubsetTerminalDispositionResult(disposed=True)
    )
    monkeypatch.setattr(
        "process.provider_directory_fhir_subset_terminal_disposition."
        "dispose_v4_census_drift_root",
        disposition_call,
    )

    rendered_result = await script_module._execute_operation(
        database,
        script_module.DIRECT_V4_TERMINAL_DISPOSITION_COMMAND,
    )

    disposition_call.assert_awaited_once_with(database=database)
    assert rendered_result == (
        '{"already_applied":false,"disposed":true,"status":"ok"}'
    )


@pytest.mark.asyncio
async def test_v5_http410_command_reuses_closed_disposition(monkeypatch):
    """Keep the exact HTTP-410 profile selector-free and identifier-free."""

    script_module = _script_module()
    database = _Database()
    disposition_call = AsyncMock(
        return_value=ReviewedSubsetTerminalDispositionResult(disposed=True)
    )
    monkeypatch.setattr(
        "process.provider_directory_fhir_subset_terminal_disposition."
        "dispose_v5_terminal_root",
        disposition_call,
    )

    rendered_result = await script_module._execute_operation(
        database,
        script_module.DIRECT_V5_HTTP410_TERMINAL_DISPOSITION_COMMAND,
    )

    disposition_call.assert_awaited_once_with(database=database)
    assert rendered_result == (
        '{"already_applied":false,"disposed":true,"status":"ok"}'
    )


@pytest.mark.parametrize(
    ("error", "expected_code"),
    (
        (ReviewedSubsetActivationError("disabled"), "disabled"),
        (ReviewedSubsetActivationError("evidence"), "evidence"),
        (ReviewedSubsetAbandonmentError("busy"), "busy"),
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
