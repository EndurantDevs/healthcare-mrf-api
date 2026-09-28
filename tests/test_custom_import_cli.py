# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for the bounded custom-import operator slice."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import UTC, datetime
from io import BytesIO, StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import log as sqlalchemy_log

from process.custom_import import cli, publication
from process.custom_import.definition_store import RegisteredDefinition
from process.custom_import.execution import ExecutionTransition
from process.custom_import.operator import CurrentGenerationStatus, ExecutionStatus, GenerationStatus, LeaseStatus
from process.custom_import.publication import PublicationReceipt

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


class _TtyBytes(BytesIO):
    def is_tty(self) -> bool:
        return True

    isatty = is_tty


class _NonBytesStream:
    def read(self, _size):
        return None


class _LifecycleTransaction:
    def __init__(self, database):
        self.database = database

    async def __aenter__(self):
        self.database.events.append("begin")
        return self.database.session

    async def __aexit__(self, exception_type, _exception, _traceback):
        if exception_type is not None:
            self.database.events.append("rollback")
            return
        self.database.events.append("commit")
        if self.database.commit_error is not None:
            self.database.events.append("rollback")
            raise self.database.commit_error


class _LifecycleDatabase:
    def __init__(
        self,
        session,
        *,
        commit_error=None,
        disconnect_error=None,
        echo_loggers=(),
        emit_output=False,
        new_engine=False,
    ):
        self.commit_error = commit_error
        self.disconnect_error = disconnect_error
        self.echo_loggers = echo_loggers
        self.emit_output = emit_output
        self.events = []
        self.new_engine = new_engine
        self.engine = None if new_engine else SimpleNamespace(echo=True)
        self.disconnected_echo = None
        self.session = session

    async def connect(self):
        self.events.append("connect")
        if self.engine is None:
            self.engine = SimpleNamespace(echo=os.getenv("HLTHPRT_DB_ECHO") == "1")
        if self.emit_output and self.engine.echo:
            print("synthetic-db-output")
            print("synthetic-db-error", file=sys.stderr)
        for logger in self.echo_loggers:
            sqlalchemy_log.InstanceLogger(True, logger.name)
            logger.error("synthetic-db-log")

    async def disconnect(self):
        self.events.append("disconnect")
        self.disconnected_echo = self.engine.echo
        if self.echo_loggers:
            try:
                raise RuntimeError("synthetic-pool-reset")
            except RuntimeError:
                for logger in self.echo_loggers:
                    logger.exception("synthetic-db-disconnect")
        if self.new_engine:
            self.engine = None
        if self.disconnect_error is not None:
            raise self.disconnect_error

    def transaction(self):
        return _LifecycleTransaction(self)


def _lifecycle_arguments(arguments):
    return cli._parse_arguments(arguments)


async def _lifecycle_receipt(arguments, database):
    rendered = await cli._run_lifecycle_command(_lifecycle_arguments(arguments), database=database)
    return json.loads(rendered)


def _execution_status(now):
    return ExecutionStatus(
        execution_id=17,
        dataset_id=5,
        definition_revision_id=7,
        schema_revision_id=11,
        capture_bundle_id=13,
        mechanism="local",
        state="completed",
        started_at=now,
        finished_at=now,
        created_at=now,
        updated_at=now,
        lease=LeaseStatus(fence=2, heartbeat_at=now, expires_at=now),
    )


def _generation_status(now):
    return GenerationStatus(
        generation_id=19,
        dataset_id=5,
        definition_revision_id=7,
        schema_revision_id=11,
        execution_id=17,
        capture_bundle_id=13,
        base_generation_id=None,
        root_count=1,
        family_count=1,
        created_at=now,
        publication_state="current",
        ever_published=True,
        seal=None,
        current=CurrentGenerationStatus(19, 7, 11, 4, now),
        no_change=None,
    )


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


def test_stdin_rejects_invalid_text_nonbytes_or_oversize_input():
    assert cli._read_stdin(StringIO("synthetic")) == b"synthetic"

    for stream in (
        StringIO("\ud800"),
        _NonBytesStream(),
        BytesIO(b"x" * (cli.MAX_DEFINITION_BYTES + 1)),
    ):
        with pytest.raises(cli._DefinitionInputError):
            cli._read_stdin(stream)


def test_load_definition_rejects_unsupported_format():
    with pytest.raises(cli._DefinitionInputError):
        cli.load_definition_from_stdin("synthetic", stream=BytesIO(b"{}"))


@pytest.mark.parametrize(
    ("failure", "expected_exit_code", "expected_receipt"),
    (
        (KeyboardInterrupt, 130, '{"code":"canceled","status":"error"}\n'),
        (RuntimeError, 1, '{"code":"failed","status":"error"}\n'),
    ),
)
def test_validate_redacts_interrupt_and_unexpected_failures(
    monkeypatch, capsys, failure, expected_exit_code, expected_receipt
):
    def raise_failure(*_args, **_kwargs):
        raise failure("synthetic-private-value")

    monkeypatch.setattr(cli, "load_definition_from_stdin", raise_failure)
    exit_code = cli.run_command(["validate", "--format", "json"])

    captured = capsys.readouterr()
    assert exit_code == expected_exit_code
    assert captured.out == ""
    assert captured.err == expected_receipt
    assert "synthetic-private-value" not in captured.err


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


@pytest.mark.asyncio
async def test_status_uses_exact_operator_transaction(monkeypatch):
    now = datetime(2026, 1, 2, tzinfo=UTC)
    session = object()
    database = _LifecycleDatabase(session)
    calls = []

    async def inspect_execution(injected_session, *, dataset_id, execution_id):
        calls.append(("execution", injected_session, dataset_id, execution_id))
        return _execution_status(now)

    async def inspect_generation(injected_session, *, dataset_id, generation_id):
        calls.append(("generation", injected_session, dataset_id, generation_id))
        return _generation_status(now)

    monkeypatch.setattr(cli, "inspect_execution", inspect_execution)
    monkeypatch.setattr(cli, "inspect_generation", inspect_generation)
    execution = await _lifecycle_receipt(["status", "--dataset-id", "5", "--execution-id", "17"], database)
    generation = await _lifecycle_receipt(["status", "--dataset-id", "5", "--generation-id", "19"], database)

    assert execution["resource"] == "execution"
    assert execution["lease_fence"] == 2
    assert generation["resource"] == "generation"
    assert generation["current_pointer_version"] == 4
    assert calls == [("execution", session, 5, 17), ("generation", session, 5, 19)]
    assert database.events == ["connect", "begin", "commit", "disconnect"] * 2


@pytest.mark.asyncio
async def test_cancel_noop_is_emitted_once(monkeypatch):
    session = object()
    database = _LifecycleDatabase(session)
    calls = []

    async def cancel(injected_session, *, execution_id):
        calls.append((injected_session, execution_id))
        return ExecutionTransition(execution_id, "completed", False)

    monkeypatch.setattr(cli, "request_cancellation", cancel)
    receipt = await _lifecycle_receipt(["cancel", "--execution-id", "23"], database)

    assert receipt == {"changed": False, "command": "cancel", "execution_id": 23, "state": "completed", "status": "ok"}
    assert calls == [(session, 23)]
    assert database.events == ["connect", "begin", "commit", "disconnect"]


def _publication_result(event_kind, request, *, replayed=False):
    return PublicationReceipt(
        31,
        request["dataset_id"],
        7,
        11,
        17,
        event_kind,
        request["expected_generation_id"],
        request["target_generation_id"],
        request["expected_pointer_version"],
        request["expected_pointer_version"] + 1,
        "a" * 64,
        replayed=replayed,
    )


@pytest.mark.asyncio
async def test_publication_commands_forward_pointer_preconditions(monkeypatch):
    session = object()
    database = _LifecycleDatabase(session)
    calls = []

    async def activate(injected_session, **request):
        calls.append(("activate", injected_session, request))
        return _publication_result("activated", request)

    async def rollback(injected_session, **request):
        calls.append(("rollback", injected_session, request))
        return _publication_result("rolled_back", request)

    monkeypatch.setattr(cli, "activate_generation", activate)
    monkeypatch.setattr(cli, "rollback_generation", rollback)
    initial = await _lifecycle_receipt(
        ["activate", "--dataset-id", "5", "--target-generation-id", "19", "--expected-pointer-version", "0"],
        database,
    )
    nonempty = await _lifecycle_receipt(
        [
            "activate", "--dataset-id", "5", "--target-generation-id", "20", "--expected-generation-id", "19",
            "--expected-pointer-version", "1",
        ],
        database,
    )
    rollback_receipt = await _lifecycle_receipt(
        [
            "rollback", "--dataset-id", "5", "--target-generation-id", "19", "--expected-generation-id", "20",
            "--expected-pointer-version", "2",
        ],
        database,
    )

    assert [receipt["committed_pointer_version"] for receipt in (initial, nonempty, rollback_receipt)] == [1, 2, 3]
    assert calls == [
        ("activate", session, {"dataset_id": 5, "target_generation_id": 19, "expected_generation_id": None, "expected_pointer_version": 0}),
        ("activate", session, {"dataset_id": 5, "target_generation_id": 20, "expected_generation_id": 19, "expected_pointer_version": 1}),
        ("rollback", session, {"dataset_id": 5, "target_generation_id": 19, "expected_generation_id": 20, "expected_pointer_version": 2}),
    ]
    assert database.events == ["connect", "begin", "commit", "disconnect"] * 3


def test_publication_preconditions_allow_initial_and_nonempty_but_reject_stale():
    publication._require_expected_pointer(None, expected_generation_id=None, expected_pointer_version=0)
    pointer = SimpleNamespace(generation_id=19, pointer_version=4)
    publication._require_expected_pointer(pointer, expected_generation_id=19, expected_pointer_version=4)

    with pytest.raises(publication.PublicationConflict, match="compare-and-swap"):
        publication._require_expected_pointer(pointer, expected_generation_id=19, expected_pointer_version=3)


@pytest.mark.asyncio
async def test_activate_preserves_exact_replay_receipt(monkeypatch):
    session = object()
    database = _LifecycleDatabase(session)
    calls = []

    async def activate(injected_session, **request):
        calls.append((injected_session, request))
        return _publication_result("activated", request, replayed=True)

    monkeypatch.setattr(cli, "activate_generation", activate)
    receipt = await _lifecycle_receipt(
        ["activate", "--dataset-id", "5", "--target-generation-id", "19", "--expected-pointer-version", "0"],
        database,
    )

    assert receipt["replayed"] is True
    assert calls == [(session, {"dataset_id": 5, "target_generation_id": 19, "expected_generation_id": None, "expected_pointer_version": 0})]
    assert database.events == ["connect", "begin", "commit", "disconnect"]


def test_activate_rejects_nonempty_pointer_without_generation(capsys):
    with pytest.raises(SystemExit) as caught:
        cli.run_command(
            ["activate", "--dataset-id", "5", "--target-generation-id", "19", "--expected-pointer-version", "4"]
        )

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'


def test_lifecycle_commit_failure_disconnects_and_redacts(monkeypatch, capsys):
    database = _LifecycleDatabase(object(), commit_error=RuntimeError("synthetic-private-lifecycle-value"))

    async def inspect_execution(*_arguments, **_keywords):
        return _execution_status(datetime(2026, 1, 2, tzinfo=UTC))

    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "inspect_execution", inspect_execution)
    exit_code = cli.run_command(["status", "--dataset-id", "5", "--execution-id", "17"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"failed","status":"error"}\n'
    assert "synthetic-private-lifecycle-value" not in captured.err
    assert database.events == ["connect", "begin", "commit", "rollback", "disconnect"]


@pytest.mark.parametrize(
    ("failure", "code", "exit_code"),
    [
        (cli.PublicationConflict("synthetic-conflict"), "conflict", 1),
        (cli.OperatorObjectNotFound("synthetic-missing-object"), "not_found", 1),
        (cli.ExecutionNotFound("synthetic-missing-execution"), "not_found", 1),
        (KeyboardInterrupt(), "canceled", 130),
    ],
)
def test_lifecycle_error_survives_disconnect_failure(monkeypatch, capsys, failure, code, exit_code):
    database = _LifecycleDatabase(object(), disconnect_error=RuntimeError("synthetic-private-disconnect-value"))

    async def inspect_execution(*_arguments, **_keywords):
        raise failure

    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "inspect_execution", inspect_execution)
    actual_exit_code = cli.run_command(["status", "--dataset-id", "5", "--execution-id", "17"])

    captured = capsys.readouterr()
    assert actual_exit_code == exit_code
    assert captured.out == ""
    assert json.loads(captured.err) == {"code": code, "status": "error"}
    assert database.events == ["connect", "begin", "rollback", "disconnect"]
    assert database.disconnected_echo is True


def test_lifecycle_disconnect_failure_after_commit_is_redacted(monkeypatch, capsys):
    database = _LifecycleDatabase(object(), disconnect_error=RuntimeError("synthetic-private-disconnect-value"))

    async def inspect_execution(*_arguments, **_keywords):
        return _execution_status(datetime(2026, 1, 2, tzinfo=UTC))

    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "inspect_execution", inspect_execution)
    exit_code = cli.run_command(["status", "--dataset-id", "5", "--execution-id", "17"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"failed","status":"error"}\n'
    assert "synthetic-private-disconnect-value" not in captured.err
    assert database.events == ["connect", "begin", "commit", "disconnect"]
    assert database.disconnected_echo is True


def test_lifecycle_logging_scope_preserves_pool_handler_and_removes_new_echo_handler(monkeypatch, capsys):
    marker = object()
    engine_logger = logging.getLogger(f"sqlalchemy.engine.cli_test_{id(marker)}")
    pool_logger = logging.getLogger(f"sqlalchemy.pool.cli_test_{id(marker)}")
    engine_handlers = tuple(engine_logger.handlers)
    pool_output = StringIO()
    pool_handler = logging.StreamHandler(pool_output)
    previous_disable = logging.root.manager.disable
    database = _LifecycleDatabase(object(), echo_loggers=(engine_logger, pool_logger), emit_output=True, new_engine=True)

    async def inspect_execution(*_arguments, **_keywords):
        return _execution_status(datetime(2026, 1, 2, tzinfo=UTC))

    pool_logger.addHandler(pool_handler)
    monkeypatch.setattr(cli, "db", database)
    monkeypatch.setattr(cli, "inspect_execution", inspect_execution)
    monkeypatch.setenv("HLTHPRT_DB_ECHO", "1")
    try:
        exit_code = cli.run_command(["status", "--dataset-id", "5", "--execution-id", "17"])
        is_pool_handler_preserved = pool_handler in pool_logger.handlers
        is_disable_restored = logging.root.manager.disable == previous_disable
    finally:
        pool_logger.removeHandler(pool_handler)
        pool_handler.close()
        logging.disable(previous_disable)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert json.loads(captured.out)["status"] == "ok"
    assert captured.err == ""
    assert pool_output.getvalue() == ""
    assert database.disconnected_echo is True
    assert database.engine is None
    assert tuple(engine_logger.handlers) == engine_handlers
    assert is_pool_handler_preserved
    assert is_disable_restored
    assert os.environ["HLTHPRT_DB_ECHO"] == "1"


def test_lifecycle_failures_are_redacted(monkeypatch, capsys):
    async def fail(*_arguments, **_keywords):
        raise RuntimeError("synthetic-private-lifecycle-value")

    monkeypatch.setattr(cli, "_run_lifecycle_command", fail)
    exit_code = cli.run_command(["status", "--dataset-id", "5", "--execution-id", "17"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert captured.err == '{"code":"failed","status":"error"}\n'
    assert "synthetic-private-lifecycle-value" not in captured.err


def test_module_cli_help_lists_lifecycle_commands(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "custom_import_cli", "--help"],
        cwd=Path(__file__).resolve().parents[1],
        env={"PYTHONPYCACHEPREFIX": str(tmp_path / "pycache"), "PYTHONWARNINGS": "error"},
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 0
    assert completed.stderr == b""
    for command in (b"status", b"cancel", b"activate", b"rollback"):
        assert command in completed.stdout
    assert b"resume" not in completed.stdout
