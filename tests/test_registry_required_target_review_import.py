# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Review operator input, CLI orchestration and native failures without database resources."""

import asyncio
import hashlib
import json
import logging
import os
import struct
import subprocess
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import asyncpg
import pytest

from process import registry_required_target_review_import as review_import
from process import registry_required_target_review_references as references
from process import registry_required_target_review_store as store
from process.registry_record_store import RegistryAddressUnavailable
from process.registry_required_target_store import (
    MAX_INPUT_BYTES,
    RegistryRequiredTargetEdition,
    RegistryRequiredTargetError,
)

REVIEW_ID = UUID("45678901-4567-8901-8456-45678901abcd")
LEDGER_ID = UUID("34567890-3456-7890-8345-34567890abcd")
SOURCE_URL = "https://example.test/review.json"
PRIVATE_MARKER = "synthetic-private-review-marker"
_RECEIPT_FIELDS = {
    "status",
    "component",
    "revision",
    "parser_version",
    "snapshot_id",
    "source_sha256",
    "artifact_sha256",
    "ledger_snapshot_id",
    "ledger_artifact_sha256",
    "decision_count",
    "resolved_count",
    "physical_records",
    "evidence_id",
    "copy_sha256",
    "copy_bytes",
    "replayed",
}


def _edition(source):
    return RegistryRequiredTargetEdition(REVIEW_ID, SOURCE_URL, hashlib.sha256(source).hexdigest())


def _arguments(path, source):
    return [
        "--input-file",
        str(path),
        "--snapshot-id",
        str(REVIEW_ID),
        "--source-url",
        SOURCE_URL,
        "--input-sha256",
        hashlib.sha256(source).hexdigest(),
        "--ledger-snapshot-id",
        str(LEDGER_ID),
    ]


def _wire(input_bytes):
    """Only a wire fixture for pure output/refusal tests; it is never admitted."""
    artifact = json.dumps(
        {
            "component": "registry_required_target_review",
            "revision": 1,
            "parser_version": "registry-target-review-v2",
            "source_sha256": hashlib.sha256(input_bytes).hexdigest(),
            "ledger_snapshot_id": str(LEDGER_ID),
            "ledger_artifact_sha256": "a" * 64,
            "ledger_source_sha256": "b" * 64,
            "decisions": [
                {
                    "target_key": "fc:1",
                    "resolution_status": "unresolved",
                    "network_id": None,
                    "source_binding": None,
                    "evidence_reference": "Example evidence",
                    "evidence_sha256": "c" * 64,
                    "reason": "Pending review",
                }
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    fields = (REVIEW_ID.bytes, b"review:v2", struct.pack(">i", 1), b"accepted", b"\x01" + artifact, b"\x01[]")
    copy = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0" + struct.pack(">h", 6)
    copy += b"".join(struct.pack(">i", len(field)) + field for field in fields) + struct.pack(">h", -1)
    descriptor_by_field = {
        "component": "registry_required_target_review",
        "revision": 1,
        "parser_version": "registry-target-review-v2",
        "snapshot_id": str(REVIEW_ID),
        "source_sha256": hashlib.sha256(input_bytes).hexdigest(),
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "ledger_snapshot_id": str(LEDGER_ID),
        "ledger_artifact_sha256": "a" * 64,
        "decision_count": 1,
        "resolved_count": 0,
        "physical_records": 1,
    }
    return copy, descriptor_by_field


def _aggregate(source, replayed=False):
    copy, descriptor = _wire(source)
    return {
        **descriptor,
        "evidence_id": "required-target-review:" + str(REVIEW_ID),
        "copy_sha256": hashlib.sha256(copy).hexdigest(),
        "copy_bytes": len(copy),
        "replayed": replayed,
    }


class _FailureDatabase:
    """Observe orchestration; real admission calls are restricted to injected failures."""

    def __init__(self, failure):
        self.failure = failure
        self.events = []
        self.depth = 0
        self.engine = SimpleNamespace(echo=True)
        self.queries = []

    async def connect(self):
        self.events.append("connect")
        print(PRIVATE_MARKER)
        print(PRIVATE_MARKER, file=sys.stderr)
        logging.getLogger("synthetic.review.database").error(PRIVATE_MARKER)
        if self.failure == "connect":
            raise RuntimeError(PRIVATE_MARKER)

    async def disconnect(self):
        self.events.append("disconnect")
        print(PRIVATE_MARKER, file=sys.stderr)

    @asynccontextmanager
    async def acquire_driver(self):
        self.events.append("acquire")
        try:
            yield self
        finally:
            self.events.append("release")

    @asynccontextmanager
    async def transaction(self):
        self.depth += 1
        depth = self.depth
        self.events.append(f"begin:{depth}")
        try:
            yield
        except BaseException:
            self.events.append(f"rollback:{depth}")
            raise
        else:
            if self.failure != "operator_orchestration":
                pytest.fail("A fault-injection test must never fabricate admission success")
            self.events.append(f"commit:{depth}")
        finally:
            self.depth -= 1

    def is_in_transaction(self):
        return bool(self.depth)

    async def fetchrow(self, query, *arguments):
        self.queries.append((query, arguments))
        self.events.append("read")
        if self.failure == "keyboard":
            raise KeyboardInterrupt(PRIVATE_MARKER)
        if self.failure == "cancel":
            raise asyncio.CancelledError(PRIVATE_MARKER)
        if self.failure == "read":
            raise asyncpg.PostgresError(PRIVATE_MARKER)
        if self.failure == "missing_ledger":
            return None
        return {"document": "{}", "artifact_sha256": "a" * 64, "input_sha256": "b" * 64}

    async def execute(self, *_arguments):
        pytest.fail("Failure before native admission must not write")


@pytest.mark.parametrize("failure", ["digest_mismatch", "input_unavailable", "input_limit", "input_empty"])
def test_cli_whole_file_failures_precede_database_creation(monkeypatch, tmp_path, capsys, failure):
    source = b"review\nlast byte matters\n"
    if failure == "input_limit":
        source = b"x" * (MAX_INPUT_BYTES + 1)
    elif failure == "input_empty":
        source = b""
    path = tmp_path / "review.json"
    if failure != "input_unavailable":
        path.write_bytes(source)
    arguments = _arguments(path, source)
    if failure == "digest_mismatch":
        arguments[arguments.index("--input-sha256") + 1] = hashlib.sha256(source[:-1]).hexdigest()
    monkeypatch.setattr(review_import, "Database", lambda: pytest.fail("Input failure must precede database creation"))
    assert review_import.run_command(arguments) == 1
    captured = capsys.readouterr()
    assert (
        captured.out == ""
        and captured.err == json.dumps({"code": failure, "status": "error"}, separators=(",", ":")) + "\n"
    )


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--snapshot-id", str(UUID(int=0))),
        ("--ledger-snapshot-id", str(UUID(int=0))),
        ("--source-url", " "),
        ("--input-sha256", "A" * 64),
    ],
)
def test_invalid_metadata_is_static_before_database(monkeypatch, tmp_path, capsys, flag, value):
    arguments = _arguments(tmp_path / "review.json", b"source")
    arguments[arguments.index(flag) + 1] = value
    monkeypatch.setattr(review_import, "Database", lambda: pytest.fail("Invalid metadata must not open a database"))
    assert review_import.run_command(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize("failure", ["missing_ledger_id", "bad_ledger_id", "bad_review_id", "unknown", "abbreviation"])
def test_missing_bad_or_abbreviated_arguments_never_reflect_values(tmp_path, capsys, failure):
    arguments = _arguments(tmp_path / "review.json", b"source")
    if failure == "missing_ledger_id":
        arguments = arguments[:-2]
    elif failure == "bad_ledger_id":
        arguments[-1] = PRIVATE_MARKER
    elif failure == "bad_review_id":
        arguments[arguments.index("--snapshot-id") + 1] = PRIVATE_MARKER
    elif failure == "abbreviation":
        arguments[arguments.index("--ledger-snapshot-id")] = "--ledger-snapshot"
    else:
        arguments.extend(("--unknown", PRIVATE_MARKER))
    with pytest.raises(SystemExit) as caught:
        review_import.run_command(arguments)
    captured = capsys.readouterr()
    assert caught.value.code == 2 and captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize("replayed", [False, True])
def test_pure_receipt_is_closed_aggregate_and_ignores_unexpected_source_fields(replayed):
    source = b"source"
    admission_by_field = {
        **_aggregate(source, replayed),
        "raw_decisions": PRIVATE_MARKER,
        "input_file": PRIVATE_MARKER,
        "dsn": PRIVATE_MARKER,
        "status": "unexpected",
    }
    receipt = review_import._receipt_json(_edition(source), LEDGER_ID, admission_by_field)
    rendered = json.loads(receipt)
    assert set(rendered) == _RECEIPT_FIELDS and rendered == {"status": "ok", **_aggregate(source, replayed)}
    assert len(receipt.encode()) <= 4096
    assert all(value not in receipt for value in (PRIVATE_MARKER, SOURCE_URL, "postgresql://"))


@pytest.mark.parametrize("failure", [None, "owner", "admission", "receipt", "canceled"])
def test_protected_owner_orchestration_uses_the_review_admission_transaction(monkeypatch, tmp_path, capsys, failure):
    source_record = b"review\n"
    path = tmp_path / "review.json"
    path.write_bytes(source_record)
    database = _FailureDatabase("operator_orchestration")
    owner_role = "synthetic_registry_owner"

    async def assume_owner(connection, role):
        assert connection is database and role == owner_role and database.depth == 1
        assert database.events == ["connect", "acquire", "begin:1"]
        database.events.append("assume_owner")
        if failure == "owner":
            raise ValueError(PRIVATE_MARKER)

    async def admit(connection, actual_source, edition, ledger_snapshot_id):
        assert (connection, actual_source, edition, ledger_snapshot_id) == (
            database,
            source_record,
            _edition(source_record),
            LEDGER_ID,
        )
        assert database.depth == 1 and database.events[-1] == "assume_owner"
        database.events.append("admission")
        if failure == "admission":
            raise RegistryRequiredTargetError(PRIVATE_MARKER)
        if failure == "canceled":
            raise asyncio.CancelledError(PRIVATE_MARKER)
        return {**_aggregate(source_record), **({"ledger_snapshot_id": str(REVIEW_ID)} if failure == "receipt" else {})}

    monkeypatch.setattr(review_import, "Database", lambda: database)
    monkeypatch.setattr("process.registry_management_permissions.assume_registry_source_owner", assume_owner)
    monkeypatch.setattr(review_import, "admit_registry_required_target_review", admit)
    arguments = _arguments(path, source_record) + ["--protected-owner-role", owner_role]
    assert review_import.run_command(arguments) == (0 if failure is None else 130 if failure == "canceled" else 1)
    captured = capsys.readouterr()
    expected_roles = ["connect", "acquire", "begin:1", "assume_owner"]
    if failure != "owner":
        expected_roles.append("admission")
    expected_roles += ["commit:1" if failure is None else "rollback:1", "release", "disconnect"]
    assert database.events == expected_roles and database.depth == 0 and not database.queries
    assert PRIVATE_MARKER not in captured.out + captured.err and owner_role not in captured.out + captured.err
    if failure is None:
        assert json.loads(captured.out) == {"status": "ok", **_aggregate(source_record)} and captured.err == ""
    else:
        code = "canceled" if failure == "canceled" else "admission_failed"
        assert (
            captured.out == ""
            and captured.err == json.dumps({"code": code, "status": "error"}, separators=(",", ":")) + "\n"
        )


@pytest.mark.parametrize(
    "change",
    [
        {"revision": True},
        {"snapshot_id": str(LEDGER_ID)},
        {"ledger_snapshot_id": str(REVIEW_ID)},
        {"source_sha256": "a" * 64},
        {"decision_count": True},
        {"decision_count": 0},
        {"decision_count": 5001},
        {"resolved_count": True},
        {"resolved_count": 2},
        {"physical_records": True},
        {"physical_records": 2},
        {"copy_bytes": True},
        {"copy_bytes": 0},
        {"copy_sha256": PRIVATE_MARKER},
        {"artifact_sha256": "A" * 64},
        {"evidence_id": PRIVATE_MARKER},
        {"replayed": PRIVATE_MARKER},
    ],
)
def test_pure_receipt_refuses_mistyped_or_changed_identity_without_reflection(change):
    source = b"source"
    with pytest.raises(ValueError, match="^registry_required_target_review_receipt_invalid$"):
        review_import._receipt_json(_edition(source), LEDGER_ID, {**_aggregate(source), **change})


@pytest.mark.parametrize("failure", ["connect", "read", "missing_ledger", "native", "keyboard", "cancel"])
def test_real_admission_failure_rolls_back_disconnects_and_redacts(monkeypatch, tmp_path, capsys, failure):
    input_bytes = b"whole review source\nfinal record\n"
    path = tmp_path / "review.json"
    path.write_bytes(input_bytes)
    database = _FailureDatabase(failure)
    native_calls = []

    def unavailable(*arguments):
        native_calls.append(arguments)
        raise RuntimeError(PRIVATE_MARKER)

    monkeypatch.setattr(review_import, "Database", lambda: database)
    monkeypatch.setattr(
        store.importlib,
        "import_module",
        lambda _name: SimpleNamespace(encode_registry_required_target_review_artifact=unavailable),
    )
    previous_disable = logging.root.manager.disable
    assert review_import.run_command(_arguments(path, input_bytes)) == (130 if failure in {"keyboard", "cancel"} else 1)
    captured = capsys.readouterr()
    code = "canceled" if failure in {"keyboard", "cancel"} else "admission_failed"
    assert (
        captured.out == ""
        and captured.err == json.dumps({"code": code, "status": "error"}, separators=(",", ":")) + "\n"
    )
    assert database.events[-1] == "disconnect" and database.depth == 0 and database.engine.echo is True
    assert logging.root.manager.disable == previous_disable
    if failure != "connect":
        assert database.events[-4:] == ["rollback:2", "rollback:1", "release", "disconnect"]
        assert database.queries[0][1][0] == LEDGER_ID
    if failure == "native":
        assert native_calls == [(input_bytes, b"{}", str(REVIEW_ID))]


def test_direct_cancellation_preserves_exception_and_cleans_caller_transaction(monkeypatch):
    database = _FailureDatabase("cancel")
    source = b"source"
    monkeypatch.setattr(review_import, "Database", lambda: database)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(review_import._run_import(source, _edition(source), LEDGER_ID))
    assert database.events[-4:] == ["rollback:2", "rollback:1", "release", "disconnect"] and database.depth == 0


@pytest.mark.parametrize(
    "failure,expected_exception",
    [("keyboard", KeyboardInterrupt), ("cancel", asyncio.CancelledError), ("read", RuntimeError)],
)
def test_disconnect_error_preserves_cancellation_and_refuses_other_failures(
    monkeypatch, capsys, failure, expected_exception
):
    database = _FailureDatabase(failure)
    input_bytes = b"source"
    cleanup_error = RuntimeError(PRIVATE_MARKER)
    original_disconnect = database.disconnect

    async def disconnect():
        await original_disconnect()
        raise cleanup_error

    monkeypatch.setattr(database, "disconnect", disconnect)
    monkeypatch.setattr(review_import, "Database", lambda: database)
    previous_disable = logging.root.manager.disable
    with pytest.raises(expected_exception) as caught:
        asyncio.run(review_import._run_import(input_bytes, _edition(input_bytes), LEDGER_ID))
    if failure == "read":
        assert caught.value is cleanup_error
    assert database.events[-4:] == ["rollback:2", "rollback:1", "release", "disconnect"] and database.depth == 0
    assert database.engine.echo is True and logging.root.manager.disable == previous_disable
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""


@pytest.mark.parametrize("failure", [ImportError, AttributeError, RuntimeError, TypeError, ValueError])
def test_review_native_failures_are_static(monkeypatch, failure):
    def unavailable(_name):
        raise failure(PRIVATE_MARKER)

    monkeypatch.setattr(store.importlib, "import_module", unavailable)
    code = "input_invalid" if failure is ValueError else "native_unavailable"
    with pytest.raises(RegistryRequiredTargetError, match=f"^registry_required_target_review_{code}$"):
        store._encode(b"source", {"document": "{}", "artifact_sha256": "a" * 64}, _edition(b"source"), LEDGER_ID)


@pytest.mark.parametrize(
    "change",
    [
        {"component": "other"},
        {"revision": True},
        {"parser_version": "old"},
        {"snapshot_id": str(LEDGER_ID)},
        {"source_sha256": "b" * 64},
        {"ledger_snapshot_id": str(REVIEW_ID)},
        {"ledger_artifact_sha256": "b" * 64},
        {"artifact_sha256": "a" * 64},
        {"decision_count": True},
        {"decision_count": 0},
        {"resolved_count": True},
        {"resolved_count": 2},
        {"physical_records": True},
        {"extra": PRIVATE_MARKER},
    ],
)
def test_review_facade_refuses_stale_or_unbound_descriptor(monkeypatch, change):
    source = b"source"
    copy, descriptor = _wire(source)
    monkeypatch.setattr(
        store.importlib,
        "import_module",
        lambda _name: SimpleNamespace(
            encode_registry_required_target_review_artifact=lambda *_args: (
                copy,
                json.dumps({**descriptor, **change}).encode(),
            )
        ),
    )
    with pytest.raises(RegistryRequiredTargetError, match="^registry_required_target_review_native_unavailable$"):
        store._encode(source, {"document": "{}", "artifact_sha256": "a" * 64}, _edition(source), LEDGER_ID)


@pytest.mark.parametrize(
    "failure",
    [
        "arity",
        "list",
        "mutable",
        "field_count",
        "negative_length",
        "truncated",
        "extra_record",
        "jsonb_version",
        "changed_document",
        "duplicate_descriptor",
        "missing_export",
    ],
)
def test_review_facade_refuses_malformed_wire_and_descriptor(monkeypatch, failure):
    input_bytes = b"source"
    copy, descriptor = _wire(input_bytes)
    encoded = json.dumps(descriptor).encode()
    if failure in {"jsonb_version", "changed_document"}:
        cursor = 21
        for _ in range(4):
            cursor += 4 + struct.unpack_from(">i", copy, cursor)[0]
        cursor += 4
        if failure == "changed_document":
            cursor += 1
        copy = copy[:cursor] + (b"\x02" if failure == "jsonb_version" else b"[") + copy[cursor + 1 :]
    else:
        malformed_copy_by_failure = {
            "field_count": copy[:19] + struct.pack(">h", 5) + copy[21:],
            "negative_length": copy[:21] + struct.pack(">i", -1) + copy[25:],
            "truncated": copy[:30] + b"\xff\xff",
            "extra_record": copy[:-2] + copy[19:],
        }
        copy = malformed_copy_by_failure.get(failure, copy)
    if failure == "duplicate_descriptor":
        encoded = encoded[:-1] + b',"revision":1}'
    malformed_outputs = (copy, encoded)
    if failure == "arity":
        malformed_outputs = (copy,)
    elif failure == "list":
        malformed_outputs = list(malformed_outputs)
    elif failure == "mutable":
        malformed_outputs = (bytearray(copy), encoded)
    native = (
        SimpleNamespace()
        if failure == "missing_export"
        else SimpleNamespace(encode_registry_required_target_review_artifact=lambda *_args: malformed_outputs)
    )
    monkeypatch.setattr(store.importlib, "import_module", lambda _name: native)
    with pytest.raises(RegistryRequiredTargetError, match="^registry_required_target_review_native_unavailable$"):
        store._encode(input_bytes, {"document": "{}", "artifact_sha256": "a" * 64}, _edition(input_bytes), LEDGER_ID)


@pytest.mark.parametrize(
    "change",
    [
        {"component": "other"},
        {"revision": True},
        {"review_count": True},
        {"ledger_count": True},
        {"review_count": 2},
        {"ledger_count": 0},
        {"extra": PRIVATE_MARKER},
    ],
)
def test_bundle_facade_refuses_wrong_counts_booleans_and_closed_schema(monkeypatch, change):
    descriptor_by_field = {
        "component": "registry_required_target_review_validation",
        "revision": 1,
        "review_count": 1,
        "ledger_count": 1,
    }
    monkeypatch.setattr(
        references.importlib,
        "import_module",
        lambda _name: SimpleNamespace(
            validate_registry_required_target_review_artifacts=lambda _input: json.dumps(
                {**descriptor_by_field, **change}
            ).encode()
        ),
    )
    with pytest.raises(RegistryAddressUnavailable, match="^registry_source_binding_review_native_unavailable$"):
        references._verify_bundle(b"{}", 1, 1)


@pytest.mark.parametrize("failure", [ImportError, AttributeError, RuntimeError, TypeError, ValueError])
def test_bundle_native_failures_are_static(monkeypatch, failure):
    def unavailable(_name):
        raise failure(PRIVATE_MARKER)

    monkeypatch.setattr(references.importlib, "import_module", unavailable)
    if failure is ValueError:
        with pytest.raises(ValueError, match="^registry_source_binding_review_invalid$"):
            references._verify_bundle(b"{}", 1, 1)
    else:
        with pytest.raises(RegistryAddressUnavailable, match="^registry_source_binding_review_native_unavailable$"):
            references._verify_bundle(b"{}", 1, 1)


@pytest.mark.parametrize(
    "encoded",
    [
        b'{"component":"registry_required_target_review_validation","revision":1,"review_count":1,"ledger_count":1,"review_count":1}',
        b"{",
        b"\xff",
        "text",
        b"x" * 4097,
    ],
)
def test_bundle_facade_refuses_duplicate_invalid_or_unbounded_descriptor(monkeypatch, encoded):
    monkeypatch.setattr(
        references.importlib,
        "import_module",
        lambda _name: SimpleNamespace(validate_registry_required_target_review_artifacts=lambda _input: encoded),
    )
    with pytest.raises(RegistryAddressUnavailable, match="^registry_source_binding_review_native_unavailable$"):
        references._verify_bundle(b"{}", 1, 1)


def test_actual_cli_module_entry_has_static_argument_failure(tmp_path):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "process.registry_required_target_review_import",
            "--ledger-snapshot-id",
            PRIVATE_MARKER,
        ],
        env=os.environ | {"PYTHONPYCACHEPREFIX": str(tmp_path / "cold-cache"), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 2 and completed.stdout == b""
    assert completed.stderr == b'{"code":"invalid_arguments","status":"error"}\n'


def test_actual_cli_module_preflight_advertises_protected_owner_role(tmp_path):
    completed = subprocess.run(
        [sys.executable, "-m", "process.registry_required_target_review_import", "--help"],
        env=os.environ | {"PYTHONPYCACHEPREFIX": str(tmp_path / "cold-cache"), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0 and completed.stderr == b""
    assert b"--protected-owner-role" in completed.stdout


@pytest.mark.parametrize("failure", ["edition_type", "ledger_type", "ledger_zero", "digest"])
def test_invalid_review_precedes_all_io(monkeypatch, failure):
    source = b"source"
    edition, ledger_id = _edition(source), LEDGER_ID
    if failure == "edition_type":
        edition = SimpleNamespace(**vars(edition))
    elif failure == "ledger_type":
        ledger_id = str(ledger_id)
    elif failure == "ledger_zero":
        ledger_id = UUID(int=0)
    else:
        source += b"changed final byte"
    encode = Mock(side_effect=AssertionError("Invalid review must not invoke native encoding"))
    monkeypatch.setattr(store, "_encode", encode)
    with pytest.raises(RegistryRequiredTargetError, match="^registry_required_target_review_input_invalid$"):
        asyncio.run(store.admit_registry_required_target_review(None, source, edition, ledger_id))
    encode.assert_not_called()


def test_review_requires_transaction_before_read(monkeypatch):
    source = b"source"
    connection = SimpleNamespace(is_in_transaction=lambda: False, fetchrow=AsyncMock())
    encode = Mock(side_effect=AssertionError("No native work without caller transaction"))
    monkeypatch.setattr(store, "_encode", encode)
    with pytest.raises(RegistryRequiredTargetError, match="^registry_required_target_review_transaction_required$"):
        asyncio.run(store.admit_registry_required_target_review(connection, source, _edition(source), LEDGER_ID))
    connection.fetchrow.assert_not_awaited()
    encode.assert_not_called()


@pytest.mark.parametrize("failure", ["status", "short_stream"])
def test_incomplete_review_copy_rolls_back(monkeypatch, failure):
    input_bytes = b"source"
    copy, descriptor = _wire(input_bytes)
    connection = _FailureDatabase("landing")
    connection.execute = AsyncMock()
    connection.fetchval = AsyncMock(side_effect=AssertionError("Incomplete COPY must not validate landing"))

    async def copy_to_table(_table, **options):
        assert options["schema_name"] == "pg_temp" and options["format"] == "binary"
        expected = len(copy) - (failure == "short_stream")
        assert options["source"].read(expected) == copy[:expected]
        return "COPY 0" if failure == "status" else "COPY 1"

    connection.copy_to_table = AsyncMock(side_effect=copy_to_table)
    retain = AsyncMock(side_effect=AssertionError("Incomplete review must not be retained"))
    monkeypatch.setattr(store, "_encode", lambda *_arguments: (copy, descriptor))
    monkeypatch.setattr(store, "_is_replay_after_retention", retain)

    async def admit():
        async with connection.transaction():
            await store.admit_registry_required_target_review(connection, input_bytes, _edition(input_bytes), LEDGER_ID)

    with pytest.raises(RegistryRequiredTargetError, match="^registry_required_target_review_copy_incomplete$"):
        asyncio.run(admit())
    assert connection.events == ["begin:1", "begin:2", "read", "rollback:2", "rollback:1"]
    assert connection.depth == 0 and len(connection.queries) == 1
    connection.copy_to_table.assert_awaited_once()
    connection.fetchval.assert_not_awaited()
    retain.assert_not_awaited()
    assert connection.execute.await_count == 1
    assert connection.execute.await_args.args[0].startswith("CREATE TEMP TABLE")


@pytest.mark.parametrize("failure", ["no_references", "missing_bundle", "oversize"])
def test_review_reference_bounds_precede_verification(monkeypatch, failure):
    entry_by_field = {"references_valid": True, "reference_count": 0, "bundle": None}
    if failure != "no_references":
        entry_by_field["reference_count"] = 1
    if failure == "oversize":
        entry_by_field["bundle"] = "é" * (references.MAX_BUNDLE_BYTES // 2 + 1)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=entry_by_field))
    verify = Mock(side_effect=AssertionError("No native verification without a bounded referenced bundle"))
    monkeypatch.setattr(references, "_verify_bundle", verify)
    operation = references.require_required_target_review_references(connection, '"synthetic_registry"', '"landing"')
    if failure == "no_references":
        assert asyncio.run(operation) is None
    else:
        with pytest.raises(ValueError, match="^registry_source_binding_review_limit$"):
            asyncio.run(operation)
    verify.assert_not_called()
    connection.fetchrow.assert_awaited_once()
    query, limit = connection.fetchrow.await_args.args
    assert "FOR SHARE OF snapshot,observation" in query and limit == references.MAX_BUNDLE_BYTES
