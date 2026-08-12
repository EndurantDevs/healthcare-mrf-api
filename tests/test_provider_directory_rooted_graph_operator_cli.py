# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Gate, redaction, cancellation, and dormancy tests for the graph CLI."""

from __future__ import annotations

import argparse
import asyncio
import ast
import builtins
from pathlib import Path
import signal
from typing import Any

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
    assert cli.PUBLICATION_ENABLED_ENV == contract.PUBLICATION_ENABLED_ENV


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
    selected_gate = (
        cli.ACQUISITION_ENABLED_ENV
        if arguments[0] == "acquire"
        else cli.PUBLICATION_ENABLED_ENV
    )
    _enable_only(monkeypatch, selected_gate)

    with pytest.raises(SystemExit) as caught:
        cli.run_command(arguments)

    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert "private" not in captured.err.lower()


def test_parser_requires_resume_key_and_exact_publication_receipt() -> None:
    acquisition = cli._parser().parse_args(
        ["acquire", "--operation-key", OPERATION_KEY]
    )
    publication = cli._parser().parse_args(
        [
            "publish",
            "--publication-acquisition-id",
            PUBLICATION_ACQUISITION_ID,
        ]
    )

    assert acquisition.operation_key == OPERATION_KEY
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
    publication = cli._parser().parse_args(
        ["publish", "--publication-acquisition-id", PUBLICATION_ACQUISITION_ID]
    )
    assert (
        publication.batch_size
        == PROVIDER_DIRECTORY_ROOTED_GRAPH_MATERIALIZATION_MAX_BATCH_ROWS
    )


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


def test_operator_is_packaged_but_not_scheduled_or_publicly_activated() -> None:
    operator_path = ROOT / "process" / "provider_directory_rooted_graph_operator.py"
    contract_path = (
        ROOT / "process" / "provider_directory_rooted_graph_operator_contract.py"
    )
    cli_path = (
        ROOT / "scripts" / "smoke" / "provider_directory_rooted_graph_operator.py"
    )
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
    assert "COPY scripts/ /opt/scripts/" in dockerfile
    for path in allowed_manual_entrypoints:
        _assert_safe_top_level_imports(path)
    for path in activation_paths:
        activation_source_text = path.read_text(encoding="utf-8")
        assert "provider_directory_rooted_graph_operator" not in activation_source_text
        assert contract.REGISTRATION_ENABLED_ENV not in activation_source_text
        assert contract.ACQUISITION_ENABLED_ENV not in activation_source_text
        assert contract.PUBLICATION_ENABLED_ENV not in activation_source_text

    operator_source = operator_path.read_text(encoding="utf-8")
    acquire_source = operator_source.split(
        "async def acquire_admit_rooted_graph_operation",
        1,
    )[1].split("def _publication_json", 1)[0]
    assert "_register_source" not in acquire_source
    assert "publish_provider_directory_rooted_graph_dataset" not in acquire_source
