# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Gate, redaction, cancellation, and dormancy tests for the graph CLI."""

from __future__ import annotations

import argparse
import ast
import asyncio
import builtins
import signal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_operator_contract as contract
from scripts.smoke import provider_directory_rooted_graph_operator as cli

ROOT = Path(__file__).resolve().parents[1]
OPERATION_KEY = "a" * 64
PUBLICATION_ACQUISITION_ID = "pdrga_" + "b" * 48


def _enable_only(monkeypatch: pytest.MonkeyPatch, selected: str) -> None:
    for gate_name in (
        cli.REGISTRATION_ENABLED_ENV,
        cli.ACQUISITION_ENABLED_ENV,
        cli.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
        cli.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.setenv(
            gate_name,
            "true" if gate_name == selected else "false",
        )


def test_retired_acquisition_parses_before_database_and_network_imports(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    for gate_name in (
        cli.REGISTRATION_ENABLED_ENV,
        cli.ACQUISITION_ENABLED_ENV,
        cli.SINGLE_ROOT_ACQUISITION_ENABLED_ENV,
        cli.PUBLICATION_ENABLED_ENV,
    ):
        monkeypatch.delenv(gate_name, raising=False)
    original_import = builtins.__import__

    def reject_runtime_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "db" or name.startswith(("db.", "aiohttp", "process.")):
            pytest.fail("disabled command reached a runtime import")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", reject_runtime_import)

    exit_code = cli.run_command(["acquire", "--operation-key", OPERATION_KEY])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'
    assert "private" not in captured.err


def test_cli_gate_constants_match_closed_contract() -> None:
    assert cli.REGISTRATION_ENABLED_ENV == contract.REGISTRATION_ENABLED_ENV
    assert cli.ACQUISITION_ENABLED_ENV == contract.ACQUISITION_ENABLED_ENV
    assert cli.SINGLE_ROOT_ACQUISITION_ENABLED_ENV == contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV
    assert cli.PUBLICATION_ENABLED_ENV == contract.PUBLICATION_ENABLED_ENV


@pytest.mark.parametrize("command", ("register", "acquire", "acquire-single-root", "publish"))
def test_all_cli_phases_remain_disabled_by_default(monkeypatch, capsys, command):
    _enable_only(monkeypatch, "")
    runtime = AsyncMock(side_effect=AssertionError("disabled phase reached runtime"))
    monkeypatch.setattr(cli, "_run_operator", runtime)
    arguments = [command]
    if command in {"acquire", "acquire-single-root"}:
        arguments.extend(("--operation-key", OPERATION_KEY))
    elif command == "publish":
        arguments.extend(("--publication-acquisition-id", PUBLICATION_ACQUISITION_ID))
    assert cli.run_command(arguments) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'
    runtime.assert_not_awaited()


def test_cli_conflicting_registration_gates_fail_after_parse(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(cli.REGISTRATION_ENABLED_ENV, "true")
    monkeypatch.setenv(cli.ACQUISITION_ENABLED_ENV, "true")
    monkeypatch.setenv(cli.PUBLICATION_ENABLED_ENV, "false")
    assert cli.run_command(["register"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"gate_conflict","status":"error"}\n'


def test_acquisition_stays_disabled_after_parse(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _enable_only(monkeypatch, cli.ACQUISITION_ENABLED_ENV)

    async def reject_runtime(_arguments: argparse.Namespace) -> str:
        pytest.fail("disabled twin phase reached runtime")

    monkeypatch.setattr(cli, "_run_operator", reject_runtime)

    assert cli.run_command(["acquire", "--operation-key", OPERATION_KEY]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"disabled","status":"error"}\n'


@pytest.mark.parametrize(
    "arguments",
    (
        ["acquire", "--operation-key", "PRIVATE-UPPERCASE"],
        ["publish", "--publication-acquisition-id", "private-latest"],
        [
            "publish",
            "--publication-acquisition-id",
            PUBLICATION_ACQUISITION_ID,
            "--batch-size",
            "4097",
        ],
    ),
)
def test_parser_rejects_invalid_or_broad_selectors_without_reflection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    arguments: list[str],
) -> None:
    selected_gate = cli.ACQUISITION_ENABLED_ENV if arguments[0] == "acquire" else cli.PUBLICATION_ENABLED_ENV
    _enable_only(monkeypatch, selected_gate)

    with pytest.raises(SystemExit) as caught:
        cli.run_command(arguments)

    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "private" not in captured.err.lower()


def test_parser_requires_resume_key_and_exact_publication_receipt() -> None:
    acquisition = cli._parser().parse_args(["acquire", "--operation-key", OPERATION_KEY])
    single_root = cli._parser().parse_args(["acquire-single-root", "--operation-key", OPERATION_KEY])
    publication = cli._parser().parse_args(
        [
            "publish",
            "--publication-acquisition-id",
            PUBLICATION_ACQUISITION_ID,
        ]
    )

    assert acquisition.operation_key == OPERATION_KEY
    assert single_root.operation_key == OPERATION_KEY
    assert acquisition.lease_seconds == 300
    assert publication.publication_acquisition_id == PUBLICATION_ACQUISITION_ID
    assert publication.batch_size == 4096
    assert not hasattr(publication, "source_id")
    assert not hasattr(publication, "latest")


def test_cli_batch_bound_matches_public_materialization_contract() -> None:
    from process.provider_directory_rooted_graph_publication_materialization import (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS,
    )

    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS == 4096
    publication = cli._parser().parse_args(["publish", "--publication-acquisition-id", PUBLICATION_ACQUISITION_ID])
    assert publication.batch_size == PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS


class _Registration:
    restored = False

    def restore(self) -> None:
        self.restored = True


class _Database:
    disconnected = False

    async def disconnect(self) -> None:
        self.disconnected = True


@pytest.mark.asyncio
async def test_cancellation_drains_database_and_restores_signals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_only(monkeypatch, cli.REGISTRATION_ENABLED_ENV)
    registration = _Registration()
    database = _Database()

    monkeypatch.setattr(
        cli,
        "_install_signal_handlers",
        lambda _task: registration,
    )

    async def canceled(_arguments: argparse.Namespace, _database: Any) -> str:
        raise asyncio.CancelledError(signal.SIGTERM)

    monkeypatch.setattr(cli, "_execute_selected_phase", canceled)
    arguments = cli._parser().parse_args(["register"])

    with pytest.raises(asyncio.CancelledError) as caught:
        await cli._run_operator(arguments, database=database)

    assert caught.value.args == (signal.SIGTERM,)
    assert database.disconnected is True
    assert registration.restored is True


def test_run_command_emits_only_canonical_json_on_cancel(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _enable_only(monkeypatch, cli.PUBLICATION_ENABLED_ENV)

    async def canceled(_arguments: argparse.Namespace) -> str:
        raise asyncio.CancelledError(signal.SIGINT)

    monkeypatch.setattr(cli, "_run_operator", canceled)

    exit_code = cli.run_command(
        [
            "publish",
            "--publication-acquisition-id",
            PUBLICATION_ACQUISITION_ID,
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 130
    assert captured.out == ""
    assert captured.err == '{"code":"canceled","status":"error"}\n'


def _assert_safe_top_level_imports(path: Path) -> None:
    """Reject eager acquisition or HTTP dependencies in operator modules."""

    import_names: set[str] = set()
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Import):
            import_names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            import_names.add(node.module)
    assert "aiohttp" not in import_names
    assert not any("rooted_graph_acquisition" in name for name in import_names)
    assert not any("rooted_graph_publication" in name for name in import_names)


def _assert_explicit_worker_operator_import(path: Path) -> None:
    source_text = path.read_text(encoding="utf-8")
    assert source_text.count("provider_directory_rooted_graph_operator") == 2
    tree = ast.parse(source_text)
    operator_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "process.provider_directory_rooted_graph_operator"
    ]
    assert len(operator_imports) == 2
    expected_imports_by_function = {
        "_acquire_pinned_root": {
            ("_AcquisitionControls", None),
            ("_select_exact_current_root", None),
            ("acquire_pinned_root_operation", None),
        },
        "prepare_rooted_graph_operation": {("build_rooted_graph_single_root_identity", None)},
    }
    for function_name, expected_aliases in expected_imports_by_function.items():
        function_node = next(
            node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name
        )
        local_imports = [node for node in function_node.body if node in operator_imports]
        assert len(local_imports) == 1
        assert {(alias.name, alias.asname) for alias in local_imports[0].names} == expected_aliases


def _assert_explicit_shared_task_entry() -> None:
    tree = ast.parse((ROOT / "process" / "provider_directory_fhir.py").read_text(encoding="utf-8"))
    entry = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "process_provider_directory_fhir_data"
    )
    dispatch = entry.body[1]
    assert isinstance(dispatch, ast.If)
    assert dispatch.orelse == []
    assert ast.dump(dispatch.test) == ast.dump(ast.parse('"rooted_graph" in (task or {})', mode="eval").body)
    assert len(dispatch.body) == 2
    imported, returned = dispatch.body
    assert isinstance(imported, ast.ImportFrom)
    assert imported.module == "process.provider_directory_rooted_graph_worker"
    assert [(alias.name, alias.asname) for alias in imported.names] == [("process_rooted_graph_task", None)]
    assert isinstance(returned, ast.Return) and isinstance(returned.value, ast.Await)
    assert isinstance(returned.value.value, ast.Call)
    assert isinstance(returned.value.value.func, ast.Name)
    assert returned.value.value.func.id == "process_rooted_graph_task"


def test_operator_is_packaged_but_not_scheduled_or_publicly_activated() -> None:
    operator_path = ROOT / "process" / "provider_directory_rooted_graph_operator.py"
    contract_path = ROOT / "process" / "provider_directory_rooted_graph_operator_contract.py"
    cli_path = ROOT / "scripts" / "smoke" / "provider_directory_rooted_graph_operator.py"
    worker_path = ROOT / "process" / "provider_directory_rooted_graph_worker.py"
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    allowed_manual_entrypoints = {operator_path, contract_path, cli_path}
    activation_paths = sorted(
        path
        for directory in ("api", "db", "process", "scripts", "service")
        for path in (ROOT / directory).rglob("*.py")
        if path not in allowed_manual_entrypoints
    )
    activation_paths.append(ROOT / "main.py")

    assert cli_path.is_file()
    assert (
        "COPY scripts/smoke/provider_directory_rooted_graph_operator.py "
        "/opt/scripts/smoke/provider_directory_rooted_graph_operator.py"
    ) in dockerfile
    for path in allowed_manual_entrypoints | {worker_path}:
        _assert_safe_top_level_imports(path)
    _assert_explicit_worker_operator_import(worker_path)
    _assert_explicit_shared_task_entry()
    for path in activation_paths:
        activation_source_text = path.read_text(encoding="utf-8")
        if path != worker_path:
            assert "provider_directory_rooted_graph_operator" not in activation_source_text
        assert contract.REGISTRATION_ENABLED_ENV not in activation_source_text
        assert contract.ACQUISITION_ENABLED_ENV not in activation_source_text
        assert contract.SINGLE_ROOT_ACQUISITION_ENABLED_ENV not in (activation_source_text)
        assert contract.PUBLICATION_ENABLED_ENV not in activation_source_text

    operator_source = operator_path.read_text(encoding="utf-8")
    acquire_source = operator_source.split(
        "async def acquire_admit_rooted_graph_operation",
        1,
    )[1].split("def _publication_json", 1)[0]
    assert "_register_source" not in acquire_source
    assert "publish_provider_directory_rooted_graph_dataset" not in acquire_source


@pytest.mark.asyncio
@pytest.mark.parametrize("task_kind", ("ordinary", "invalid-rooted", "unbound-rooted"))
async def test_shared_worker_requires_explicit_task_and_authenticated_runtime(monkeypatch, task_kind):
    from process import provider_directory_rooted_graph_worker as worker
    from tests.test_provider_directory_rooted_graph_worker import task

    request_by_field = {"ordinary": {}, "invalid-rooted": {"rooted_graph": {}}, "unbound-rooted": task()}[task_kind]
    execution = AsyncMock(side_effect=AssertionError("closed worker reached execution"))
    monkeypatch.setattr(worker, "_execute_rooted_graph_task", execution)
    monkeypatch.setattr(worker, "source_transaction_available", lambda: None)
    monkeypatch.setattr(worker, "current_rooted_graph_runtime", lambda _database: None)
    expected_error = "v2_runtime_required" if task_kind == "unbound-rooted" else "task_invalid"
    with pytest.raises(ValueError, match=expected_error):
        await worker.process_rooted_graph_task(SimpleNamespace(db=object()), {}, request_by_field)
    execution.assert_not_awaited()
