# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Database-free operator projection and cleanup checks; native collection is separate."""

import asyncio
import json
import logging
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_required_target_coverage_import as coverage_import

SNAPSHOT_ID = UUID("34567890-3456-7890-8345-34567890abcd")
PRIVATE_MARKER = "synthetic-private-diagnostic"


def _arguments(generation=None):
    arguments = ["--ledger-snapshot-id", str(SNAPSHOT_ID)]
    if generation is not None:
        arguments += ["--generation-id", str(generation)]
    return arguments


def _shape():
    """A pure projection input, not evidence of a successful database collection."""
    return {
        "component": "registry_required_target_coverage",
        "revision": 1,
        "targets": [{"target_key": "example:é", "priceable": None}],
        "totals": {"pricing_evidence_count": None},
        "provenance": {},
        "assessment": {"pricing": "not_assessed"},
    }


class _Database:
    """Record lifecycle decisions without opening a native connection."""

    def __init__(self, failure):
        self.failure = failure
        self.events = []
        self.engine = SimpleNamespace(echo=True)

    async def connect(self):
        self.events.append("connect")
        print(PRIVATE_MARKER)
        print(PRIVATE_MARKER, file=sys.stderr)
        logging.getLogger("synthetic.database").error(PRIVATE_MARKER)
        if self.failure == "connect":
            raise RuntimeError(PRIVATE_MARKER)

    async def disconnect(self):
        self.events.append("disconnect")
        print(PRIVATE_MARKER, file=sys.stderr)
        if self.failure == "disconnect":
            raise RuntimeError(PRIVATE_MARKER)

    @asynccontextmanager
    async def acquire_driver(self):
        self.events.append("acquire")
        if self.failure == "acquire":
            raise RuntimeError(PRIVATE_MARKER)
        try:
            yield self
        finally:
            self.events.append("release")

    @asynccontextmanager
    async def transaction(self, *, isolation, readonly):
        assert isolation == "repeatable_read" and readonly is True
        self.events.append("begin_readonly_rr")
        try:
            yield
            if self.failure == "commit":
                raise RuntimeError(PRIVATE_MARKER)
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


@pytest.mark.parametrize("generation", [None, 1, 9223372036854775807])
def test_fixed_arguments_preserve_exact_selector_types(generation):
    parsed = coverage_import._parser().parse_args(_arguments(generation))
    assert type(parsed.ledger_snapshot_id) is UUID and parsed.ledger_snapshot_id == SNAPSHOT_ID
    assert parsed.generation_id == generation


@pytest.mark.parametrize(
    "arguments",
    [
        [],
        ["--ledger-snapshot-id", PRIVATE_MARKER],
        _arguments() + ["--control-schema", PRIVATE_MARKER],
        _arguments() + ["--ledger", str(SNAPSHOT_ID)],
        _arguments() + ["--generation-id", "0"],
        _arguments() + ["--generation-id", "-1"],
        _arguments() + ["--generation-id", "01"],
        _arguments() + ["--generation-id", "+1"],
        _arguments() + ["--generation-id", "1.0"],
        _arguments() + ["--generation-id", " 1"],
        _arguments() + ["--generation-id", "١"],
        _arguments() + ["--generation-id", "9223372036854775808"],
    ],
)
def test_parser_refuses_values_before_database_construction(monkeypatch, capsys, arguments):
    monkeypatch.setattr(coverage_import, "Database", lambda: pytest.fail("No database for rejected selectors"))
    with pytest.raises(SystemExit) as caught:
        coverage_import.run_command(arguments)
    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'


def test_zero_ledger_uuid_refuses_before_database_construction(monkeypatch, capsys):
    monkeypatch.setattr(coverage_import, "Database", lambda: pytest.fail("No database for zero UUID"))
    assert coverage_import.run_command(["--ledger-snapshot-id", str(UUID(int=0))]) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'


def test_pure_projection_retains_full_report_and_unknown_pricing():
    report = _shape()
    encoded = coverage_import._report_json(report)
    assert json.loads(encoded) == report
    assert "é" in encoded and "\\u00e9" not in encoded
    assert json.loads(encoded)["targets"][0]["priceable"] is None
    assert set(json.loads(encoded)) == coverage_import._REPORT_FIELDS


@pytest.mark.parametrize(
    "change",
    [
        {"component": "other"},
        {"revision": True},
        {"revision": 2},
        {"targets": {}},
        {"totals": []},
        {"provenance": []},
        {"assessment": []},
        {"error": RuntimeError(PRIVATE_MARKER)},
        {"dsn": PRIVATE_MARKER},
    ],
)
def test_projection_refuses_changed_top_level_contract(change):
    with pytest.raises(ValueError):
        coverage_import._report_json({**_shape(), **change})


@pytest.mark.parametrize("field", sorted(coverage_import._REPORT_FIELDS))
def test_projection_requires_every_top_level_field(field):
    report = _shape()
    report.pop(field)
    with pytest.raises(ValueError):
        coverage_import._report_json(report)


def test_projection_bounds_bytes_and_json():
    report = _shape()
    report["provenance"]["padding"] = ""
    empty_bytes = len(coverage_import._report_json(report).encode())
    report["provenance"]["padding"] = "x" * (coverage_import.MAX_REPORT_BYTES - empty_bytes)
    assert len(coverage_import._report_json(report).encode()) == coverage_import.MAX_REPORT_BYTES
    report["provenance"]["padding"] += "é"
    with pytest.raises(ValueError, match="report_limit"):
        coverage_import._report_json(report)
    for value in [float("nan"), RuntimeError(PRIVATE_MARKER)]:
        report = _shape()
        report["totals"]["unexpected"] = value
        with pytest.raises((ValueError, TypeError)):
            coverage_import._report_json(report)


@pytest.mark.parametrize(
    "failure", ["connect", "acquire", "read", "report", "commit", "disconnect", "keyboard", "cancel"]
)
def test_control_failures_disconnect_and_emit_only_static_errors(monkeypatch, capsys, failure):
    database = _Database(failure)
    calls = []

    async def read(connection, ledger_snapshot_id, *, generation_id):
        calls.append((connection, ledger_snapshot_id, generation_id))
        assert database.engine.echo is False
        if failure == "read":
            raise RuntimeError(PRIVATE_MARKER)
        if failure == "keyboard":
            raise KeyboardInterrupt(PRIVATE_MARKER)
        if failure == "cancel":
            raise asyncio.CancelledError(PRIVATE_MARKER)
        # Shape-only values exercise report rejection and terminal cleanup failures.
        return {"error": PRIVATE_MARKER} if failure == "report" else _shape()

    monkeypatch.setattr(coverage_import, "Database", lambda: database)
    monkeypatch.setattr(coverage_import, "read_registry_required_target_coverage", read)
    previous_logging = logging.root.manager.disable
    assert coverage_import.run_command(_arguments(7)) == (130 if failure in {"keyboard", "cancel"} else 1)
    captured = capsys.readouterr()
    code = "canceled" if failure in {"keyboard", "cancel"} else "coverage_failed"
    assert captured.out == "" and captured.err == f'{{"code":"{code}","status":"error"}}\n'
    assert database.events[-1] == "disconnect"
    assert database.engine.echo is True and logging.root.manager.disable == previous_logging
    if failure not in {"connect", "acquire"}:
        assert calls == [(database, SNAPSHOT_ID, 7)]
        assert "begin_readonly_rr" in database.events and "release" in database.events
    if failure not in {"connect", "acquire", "disconnect"}:
        assert "rollback" in database.events


def test_async_cancellation_propagates_after_transaction_release_and_disconnect(monkeypatch):
    database = _Database("cancel")

    async def cancel(*_arguments, **_keywords):
        raise asyncio.CancelledError

    monkeypatch.setattr(coverage_import, "Database", lambda: database)
    monkeypatch.setattr(coverage_import, "read_registry_required_target_coverage", cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(coverage_import._run_report(SNAPSHOT_ID, None))
    assert database.events == ["connect", "acquire", "begin_readonly_rr", "rollback", "release", "disconnect"]


@pytest.mark.parametrize("cancellation", [KeyboardInterrupt, asyncio.CancelledError])
def test_cancellation_survives_disconnect_failure(monkeypatch, capsys, cancellation):
    database = _Database("disconnect")

    async def cancel(connection, ledger_snapshot_id, *, generation_id):
        assert (connection, ledger_snapshot_id, generation_id) == (database, SNAPSHOT_ID, 7)
        raise cancellation(PRIVATE_MARKER)

    monkeypatch.setattr(coverage_import, "Database", lambda: database)
    monkeypatch.setattr(coverage_import, "read_registry_required_target_coverage", cancel)
    previous_logging = logging.root.manager.disable
    assert coverage_import.run_command(_arguments(7)) == 130
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"canceled","status":"error"}\n'
    assert database.events == ["connect", "acquire", "begin_readonly_rr", "rollback", "release", "disconnect"]
    assert database.engine.echo is True and logging.root.manager.disable == previous_logging


@pytest.mark.parametrize("arguments", [["--help"], ["--ledger-snapshot-id", PRIVATE_MARKER], _arguments("01")])
def test_actual_module_preflight_needs_no_database_or_native_wheel(arguments, tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "process.registry_required_target_coverage_import", *arguments],
        env=os.environ | {"PYTHONPYCACHEPREFIX": str(tmp_path / "cold-cache"), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        check=False,
        timeout=30,
    )
    if arguments == ["--help"]:
        assert completed.returncode == 0 and completed.stderr == b""
        assert b"--ledger-snapshot-id" in completed.stdout and b"--generation-id" in completed.stdout
        assert b"--control-schema" not in completed.stdout
    else:
        assert completed.returncode == 2 and completed.stdout == b""
        assert completed.stderr == b'{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize(
    ("module_name", "arguments"),
    [
        ("process.registry_required_target_review_import", ["--ledger-snapshot-id", PRIVATE_MARKER]),
        ("process.registry_required_target_coverage_import", _arguments("01")),
    ],
)
def test_actual_module_rejects_arguments_before_heavy_runtime_import(tmp_path, module_name, arguments):
    """Primitive rejection works even when runtime dependencies cannot be imported."""
    blocker = tmp_path / "sitecustomize.py"
    blocker.write_text(
        "import sys\n"
        "class NoRuntimeImports:\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname in {'sanic', 'db', 'asyncpg', 'ptg2_address_canon'}:\n"
        "            raise AssertionError('runtime_import_before_argument_preflight')\n"
        "sys.meta_path.insert(0, NoRuntimeImports())\n"
    )
    environment_by_name = os.environ | {
        "PYTHONPATH": str(tmp_path) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    completed = subprocess.run(
        [sys.executable, "-m", module_name, *arguments],
        env=environment_by_name,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 2 and completed.stdout == b""
    assert completed.stderr == b'{"code":"invalid_arguments","status":"error"}\n'
