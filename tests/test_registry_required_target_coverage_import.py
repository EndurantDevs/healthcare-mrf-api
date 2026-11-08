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
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import registry_required_target_coverage as collector
from process import registry_required_target_coverage_import as coverage_import
from process.network_serving_read import PinnedNetworkServingManifest

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


def _collector_values():
    """Synthetic read results exercise collector control flow, not native admission."""
    report = _shape()
    return SimpleNamespace(
        manifest=PinnedNetworkServingManifest(
            7, str(SNAPSHOT_ID), "serving_fixture", 1, {"custom_membership": "b" * 64}, 1, "c" * 64, 1
        ),
        approved=SimpleNamespace(generation_id="b" * 64),
        descriptor={
            "component": "registry_required_target_ledger_validation",
            "revision": 1,
            "snapshot_id": str(SNAPSHOT_ID),
            "artifact_sha256": "a" * 64,
            "source_sha256": "b" * 64,
            "source_rows": 1,
            "target_count": 1,
        },
        coverage={"targets": report["targets"], "totals": {**report["totals"], "ledger_targets": 1}},
        rows=[
            {"isolation": "repeatable read", "readonly": "on"},
            {"metadata_valid": True, "document": "{}", "artifact_sha256": "a" * 64, "input_sha256": "b" * 64},
            {
                "candidate_id": SNAPSHOT_ID,
                "manifest_status": "current",
                "dataset_id": SNAPSHOT_ID,
                "schema_id": SNAPSHOT_ID,
                "producer_id": SNAPSHOT_ID,
                "source_recipes_json": "[]",
                "writer_closure": "{}",
                "current_head_in_snapshot": 7,
            },
            {"references_valid": True, "bundle": "{}", "review_count": 0, "ledger_count": 0, "review_references": "[]"},
            {"valid": True, "bounded": True, "native_input": "{}", "provenance": "[]"},
        ],
    )


@pytest.fixture
def collector_read(monkeypatch):
    state = _collector_values()
    database = state.database = _Database(None)
    database.is_in_transaction = Mock(return_value=True)
    database.fetchrow = AsyncMock(side_effect=state.rows)
    database.execute = AsyncMock(side_effect=AssertionError("Coverage collection cannot write"))
    state.manifest_read = AsyncMock(return_value=state.manifest)
    state.pin = AsyncMock(return_value=state.approved)
    state.recipes, state.bundle, state.selection = Mock(), Mock(), Mock(return_value={})
    state.ledger_native = Mock(side_effect=lambda _: json.dumps(state.descriptor).encode())
    state.coverage_native = Mock(side_effect=lambda _: json.dumps(state.coverage).encode())
    state.pricing = AsyncMock(side_effect=lambda _connection, report, _approved, **_options: report)
    monkeypatch.setitem(
        sys.modules,
        "ptg2_address_canon",
        SimpleNamespace(
            validate_registry_required_target_ledger_artifact=state.ledger_native,
            build_registry_network_coverage=state.coverage_native,
        ),
    )
    monkeypatch.setattr(coverage_import, "Database", lambda: database)
    for name, replacement in {
        "registry_schema": lambda: "control_fixture",
        "resolve_network_serving_manifest": state.manifest_read,
        "pin_retained_approved_membership_source": state.pin,
        "verify_source_recipe_metadata": state.recipes,
        "_verify_bundle": state.bundle,
        "verify_registry_source_selection_receipt": state.selection,
        "append_pricing_binding_metadata": state.pricing,
    }.items():
        monkeypatch.setattr(collector, name, replacement)
    return state


def _assert_collection_refusal(state, capsys, reads):
    previous_logging = logging.root.manager.disable
    assert coverage_import.run_command(_arguments(7)) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"coverage_failed","status":"error"}\n'
    assert state.database.events == ["connect", "acquire", "begin_readonly_rr", "rollback", "release", "disconnect"]
    assert state.database.fetchrow.await_count == reads
    assert state.database.engine.echo is True and logging.root.manager.disable == previous_logging
    state.pricing.assert_not_awaited()
    state.database.execute.assert_not_awaited()


def test_host_collector_reaches_pricing_only_after_complete_report(collector_read, capsys):
    state = collector_read
    assert coverage_import.run_command(_arguments(7)) == 0
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert captured.err == "" and report["targets"] == _shape()["targets"]
    assert report["provenance"]["serving"]["generation_id"] == 7
    assert report["provenance"]["approved_map_sha256"] == state.approved.generation_id
    assert report["assessment"]["pricing"] == "not_assessed"
    state.pricing.assert_awaited_once()
    assert state.pricing.await_args.args[1] == report
    assert state.database.fetchrow.await_count == 5
    assert state.database.events == ["connect", "acquire", "begin_readonly_rr", "commit", "release", "disconnect"]
    state.database.execute.assert_not_awaited()


@pytest.mark.parametrize(
    ("row", "change"),
    [
        (1, None),
        (1, {"metadata_valid": False}),
        (2, None),
        (2, {"candidate_id": UUID(int=1)}),
        (3, None),
        (3, {"references_valid": False}),
        (3, {"bundle": None}),
        (4, None),
        (4, {"valid": False}),
        (4, {"bounded": False}),
    ],
)
def test_collector_rejects_unverified_rows_before_later_reads(collector_read, capsys, row, change):
    state = collector_read
    state.rows[row] = None if change is None else {**state.rows[row], **change}
    _assert_collection_refusal(state, capsys, row + 1)
    assert state.ledger_native.call_count == int(row > 1)
    assert state.pin.await_count == int(row > 1)
    assert state.recipes.call_count == int(row > 2)
    assert state.bundle.call_count == int(row > 3)
    state.coverage_native.assert_not_called()
    state.selection.assert_not_called()


@pytest.mark.parametrize(
    ("row", "field", "limit", "reads"),
    [
        (1, "document", collector.MAX_LEDGER_TEXT_BYTES, 2),
        (3, "bundle", collector.MAX_BUNDLE_BYTES, 4),
        (4, "native_input", collector.MAX_NATIVE_INPUT_BYTES, 5),
        (3, "review_references", collector.MAX_NATIVE_INPUT_BYTES, 5),
    ],
)
def test_collector_enforces_encoded_byte_limits_before_next_stage(collector_read, capsys, row, field, limit, reads):
    state = collector_read
    state.rows[row][field] = "é" * (limit // 2 + 1)
    assert len(state.rows[row][field]) <= limit < len(state.rows[row][field].encode())
    _assert_collection_refusal(state, capsys, reads)
    assert state.ledger_native.call_count == int(row != 1)
    assert state.bundle.call_count == int(field in {"native_input", "review_references"})
    assert state.coverage_native.call_count == int(field == "review_references")
    state.selection.assert_not_called()


@pytest.mark.parametrize("result", [b"", "{}"])
def test_collector_rejects_native_result_framing_before_pin(collector_read, capsys, result):
    state = collector_read
    state.ledger_native.side_effect = None
    state.ledger_native.return_value = result
    _assert_collection_refusal(state, capsys, 2)
    state.ledger_native.assert_called_once()
    state.pin.assert_not_awaited()
    state.coverage_native.assert_not_called()


@pytest.mark.parametrize("change", [{"artifact_sha256": "c" * 64}, {"target_count": True}])
def test_collector_rejects_unbound_ledger_descriptor_before_pin(collector_read, capsys, change):
    state = collector_read
    state.descriptor.update(change)
    _assert_collection_refusal(state, capsys, 2)
    state.ledger_native.assert_called_once()
    state.pin.assert_not_awaited()
    state.coverage_native.assert_not_called()


def test_collector_rejects_approved_pin_mismatch_before_candidate_read(collector_read, capsys):
    state = collector_read
    state.manifest.source_generations["custom_membership"] = "c" * 64
    _assert_collection_refusal(state, capsys, 2)
    state.pin.assert_awaited_once_with(state.database, approved_revision=1, control_schema=None)
    state.recipes.assert_not_called()
    state.coverage_native.assert_not_called()


@pytest.mark.parametrize("change", [{"targets": []}, {"totals": {"ledger_targets": True}}])
def test_collector_rejects_incomplete_native_coverage_before_provenance(collector_read, capsys, change):
    state = collector_read
    state.coverage.update(change)
    _assert_collection_refusal(state, capsys, 5)
    state.coverage_native.assert_called_once()
    state.selection.assert_not_called()


def test_collector_rejects_aggregate_report_overflow_before_pricing(collector_read, capsys):
    state = collector_read
    state.rows[2]["writer_closure"] = json.dumps({"synthetic_padding": "x" * collector.MAX_REPORT_BYTES})
    _assert_collection_refusal(state, capsys, 5)
    state.coverage_native.assert_called_once()
    state.selection.assert_called_once_with(state.rows[2])


@pytest.mark.parametrize("generation", [True, 0, -1, "7", 9223372036854775808])
def test_collector_rejects_invalid_generation_before_transaction_or_read(monkeypatch, generation):
    connection = SimpleNamespace(is_in_transaction=Mock())
    read = AsyncMock()
    monkeypatch.setattr(collector, "_read", read)
    with pytest.raises(collector.RegistryRequiredTargetCoverageError, match="coverage_selector_invalid"):
        asyncio.run(collector.read_registry_required_target_coverage(connection, SNAPSHOT_ID, generation_id=generation))
    connection.is_in_transaction.assert_not_called()
    read.assert_not_awaited()
