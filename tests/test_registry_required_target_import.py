# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator receipts and native trust boundaries use synthetic, database-free evidence."""

import asyncio
import hashlib
import json
import logging
import struct
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID

import pytest

from process import registry_required_target_import as target_import
from process import registry_required_target_store as store

SNAPSHOT_ID = UUID("34567890-3456-7890-8345-34567890abcd")
SOURCE_URL = "https://example.test/required-networks.csv"
PRIVATE_MARKER = "synthetic-private-source-marker"
RECEIPT_FIELDS = {
    "status",
    "component",
    "revision",
    "parser_version",
    "snapshot_id",
    "source_sha256",
    "artifact_sha256",
    "source_rows",
    "target_count",
    "physical_records",
    "copy_sha256",
    "copy_bytes",
    "replayed",
}


def _edition(source):
    return store.RegistryRequiredTargetEdition(SNAPSHOT_ID, SOURCE_URL, hashlib.sha256(source).hexdigest())


def _arguments(path, source):
    return [
        "--input-file",
        str(path),
        "--snapshot-id",
        str(SNAPSHOT_ID),
        "--source-url",
        SOURCE_URL,
        "--input-sha256",
        hashlib.sha256(source).hexdigest(),
    ]


def _encoded(source):
    """Construct one independent six-column binary wire record, without a native dependency."""
    ledger_document_dict = {
        "component": "registry_required_target_ledger",
        "revision": 1,
        "parser_version": "registry-target-ledger-v1",
        "ledger": {
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "row_count": 1,
            "observations": [{"source_row_ordinal": 1, "raw_cells": ["Example"] * 8, "target_keys": ["target"]}],
            "targets": [{"target_key": "target", "fc_network_id": "1", "ribbon_id": None, "source_row_ordinals": [1]}],
        },
    }
    artifact = json.dumps(ledger_document_dict, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    fields = (SNAPSHOT_ID.bytes, b"ledger:v1", struct.pack(">i", 1), b"accepted", b"\x01" + artifact, b"\x01[]")
    copy = b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0" + struct.pack(">h", 6)
    copy += b"".join(struct.pack(">i", len(field)) + field for field in fields) + struct.pack(">h", -1)
    descriptor_by_field = {
        "component": "registry_required_target_ledger",
        "revision": 1,
        "parser_version": "registry-target-ledger-v1",
        "snapshot_id": str(SNAPSHOT_ID),
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "source_rows": 1,
        "target_count": 1,
        "physical_records": 1,
    }
    return copy, descriptor_by_field


def _native(monkeypatch, result):
    calls = []

    def encode(source, snapshot):
        calls.append((source, snapshot))
        return result

    monkeypatch.setattr(
        store.importlib, "import_module", lambda _name: SimpleNamespace(encode_registry_target_ledger_artifact=encode)
    )
    return calls


class _Database:
    """Observe pool cleanup and transaction outcomes without opening any real connection."""

    def __init__(self, failure=None):
        self.events = []
        self.failure = failure
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

    @asynccontextmanager
    async def acquire_driver(self):
        self.events.append("acquire")
        try:
            yield self
        finally:
            self.events.append("release")

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        try:
            yield
            if self.failure == "commit":
                raise RuntimeError(PRIVATE_MARKER)
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


def _receipt(source, replayed=False):
    copy, descriptor = _encoded(source)
    return {
        **descriptor,
        "copy_sha256": hashlib.sha256(copy).hexdigest(),
        "copy_bytes": len(copy),
        "replayed": replayed,
    }


@pytest.mark.parametrize("failure", ["digest_mismatch", "input_limit", "input_unavailable", "input_empty"])
def test_cli_input_failure_precedes_database_creation(monkeypatch, tmp_path, capsys, failure):
    source = b"whole file\nfinal record\n"
    if failure == "input_limit":
        source = b"x" * (store.MAX_INPUT_BYTES + 1)
    elif failure == "input_empty":
        source = b""
    path = tmp_path / "source.csv"
    if failure != "input_unavailable":
        path.write_bytes(source)
    arguments = _arguments(path, source)
    if failure == "digest_mismatch":
        arguments[-1] = hashlib.sha256(source[:-1]).hexdigest()

    def unexpected_database():
        raise AssertionError("Input validation must precede pool construction")

    monkeypatch.setattr(target_import, "Database", unexpected_database)
    assert target_import.run_command(arguments) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == json.dumps({"code": failure, "status": "error"}, separators=(",", ":")) + "\n"


@pytest.mark.parametrize("replayed", [False, True])
def test_cli_hashes_whole_file_and_emits_only_aggregate_receipt(monkeypatch, tmp_path, capsys, replayed):
    source = (PRIVATE_MARKER + "\nlast row with trailing spaces  \n").encode()
    path = tmp_path / "source.csv"
    path.write_bytes(source)
    database = _Database()
    calls = []

    async def admit(connection, actual_source, edition):
        calls.append((connection, actual_source, edition))
        assert database.engine.echo is False
        return _receipt(source, replayed)

    monkeypatch.setattr(target_import, "Database", lambda: database)
    monkeypatch.setattr(target_import, "admit_registry_required_targets", admit)
    previous_logging = logging.root.manager.disable
    assert target_import.run_command(_arguments(path, source)) == 0
    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    assert captured.out.count("\n") == 1 and captured.err == ""
    assert set(receipt) == RECEIPT_FIELDS and receipt == {"status": "ok", **_receipt(source, replayed)}
    assert all(value not in captured.out for value in (PRIVATE_MARKER, str(path), SOURCE_URL, "postgresql://"))
    assert calls == [(database, source, _edition(source))]
    assert database.events == ["connect", "acquire", "begin", "commit", "release", "disconnect"]
    assert database.engine.echo is True and logging.root.manager.disable == previous_logging


@pytest.mark.parametrize("extra", ["raw_cells", "input_file", "dsn"])
def test_cli_does_not_publish_unexpected_admission_data(monkeypatch, tmp_path, capsys, extra):
    source = b"source\n"
    path = tmp_path / "source.csv"
    path.write_bytes(source)
    database = _Database()

    async def admit(*_arguments):
        return {**_receipt(source), extra: PRIVATE_MARKER}

    monkeypatch.setattr(target_import, "Database", lambda: database)
    monkeypatch.setattr(target_import, "admit_registry_required_targets", admit)
    status = target_import.run_command(_arguments(path, source))
    captured = capsys.readouterr()
    assert PRIVATE_MARKER not in captured.out + captured.err
    if status == 0:
        assert captured.err == "" and set(json.loads(captured.out)) == RECEIPT_FIELDS
    else:
        assert status == 1 and captured.out == ""
        assert captured.err == '{"code":"admission_failed","status":"error"}\n'
    assert database.events[-1] == "disconnect"


@pytest.mark.parametrize(
    "change", [{"--source-url": " "}, {"--input-sha256": "A" * 64}, {"--snapshot-id": str(UUID(int=0))}]
)
def test_cli_invalid_metadata_is_static_and_never_constructs_database(monkeypatch, tmp_path, capsys, change):
    arguments = _arguments(tmp_path / "source.csv", b"source")
    for flag, value in change.items():
        arguments[arguments.index(flag) + 1] = value
    monkeypatch.setattr(target_import, "Database", lambda: pytest.fail("Invalid metadata must not open a database"))
    assert target_import.run_command(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize("arguments", [[], ["--snapshot-id", PRIVATE_MARKER], ["--unexpected", PRIVATE_MARKER]])
def test_cli_parser_refuses_bad_arguments_without_reflecting_values(capsys, arguments):
    with pytest.raises(SystemExit) as caught:
        target_import.run_command(arguments)
    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == "" and captured.err == '{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize("failure", ["connect", "admission", "commit", "keyboard", "canceled"])
def test_cli_failure_rolls_back_disconnects_and_redacts_errors(monkeypatch, tmp_path, capsys, failure):
    source = b"source\n"
    path = tmp_path / "source.csv"
    path.write_bytes(source)
    database = _Database(failure)

    async def admit(*_arguments):
        if failure == "admission":
            raise store.RegistryRequiredTargetError(PRIVATE_MARKER)
        if failure == "keyboard":
            raise KeyboardInterrupt(PRIVATE_MARKER)
        if failure == "canceled":
            raise asyncio.CancelledError(PRIVATE_MARKER)
        return _receipt(source)

    monkeypatch.setattr(target_import, "Database", lambda: database)
    monkeypatch.setattr(target_import, "admit_registry_required_targets", admit)
    assert target_import.run_command(_arguments(path, source)) == (130 if failure in ("keyboard", "canceled") else 1)
    captured = capsys.readouterr()
    code = "canceled" if failure in ("keyboard", "canceled") else "admission_failed"
    assert (
        captured.out == ""
        and captured.err == json.dumps({"code": code, "status": "error"}, separators=(",", ":")) + "\n"
    )
    assert database.events[-1] == "disconnect"
    if failure != "connect":
        assert database.events[-3:] == ["rollback", "release", "disconnect"]


def test_cancelled_import_releases_pool_and_preserves_cancellation(monkeypatch):
    source = b"source\n"
    database = _Database()

    async def admit(*_arguments):
        raise asyncio.CancelledError

    monkeypatch.setattr(target_import, "Database", lambda: database)
    monkeypatch.setattr(target_import, "admit_registry_required_targets", admit)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(target_import._run_import(source, _edition(source)))
    assert database.events[-3:] == ["rollback", "release", "disconnect"]


@pytest.mark.parametrize("primary_type", [KeyboardInterrupt, asyncio.CancelledError, ValueError])
def test_disconnect_error_preserves_cancellation_and_refuses_other_failures(monkeypatch, capsys, primary_type):
    input_bytes = b"source\n"
    database = _Database()
    primary_error = primary_type(PRIVATE_MARKER)
    cleanup_error = RuntimeError(PRIVATE_MARKER)
    original_disconnect = database.disconnect

    async def disconnect():
        await original_disconnect()
        raise cleanup_error

    async def admit(*_arguments):
        raise primary_error

    monkeypatch.setattr(database, "disconnect", disconnect)
    monkeypatch.setattr(target_import, "Database", lambda: database)
    monkeypatch.setattr(target_import, "admit_registry_required_targets", admit)
    expected_error = primary_error if primary_type in (KeyboardInterrupt, asyncio.CancelledError) else cleanup_error
    previous_disable = logging.root.manager.disable
    with pytest.raises(type(expected_error)) as caught:
        asyncio.run(target_import._run_import(input_bytes, _edition(input_bytes)))
    assert caught.value is expected_error
    assert database.events[-3:] == ["rollback", "release", "disconnect"]
    assert database.engine.echo is True and logging.root.manager.disable == previous_disable
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""


def test_facade_accepts_one_complete_descriptor_without_rewriting_source(monkeypatch):
    source = b"whole source\nlast row\n"
    copy, descriptor = _encoded(source)
    calls = _native(monkeypatch, (copy, json.dumps(descriptor).encode()))
    assert store._encode(source, _edition(source)) == (copy, descriptor)
    assert calls == [(source, str(SNAPSHOT_ID))]


@pytest.mark.parametrize("failure", [ImportError, AttributeError, RuntimeError, TypeError, ValueError])
def test_facade_missing_or_failed_native_is_static(monkeypatch, failure):
    def unavailable(_name):
        raise failure(PRIVATE_MARKER)

    monkeypatch.setattr(store.importlib, "import_module", unavailable)
    code = "input_invalid" if failure is ValueError else "native_unavailable"
    with pytest.raises(store.RegistryRequiredTargetError, match=f"^registry_required_target_{code}$"):
        store._encode(b"source", _edition(b"source"))


@pytest.mark.parametrize(
    "change",
    [
        {"component": "other"},
        {"revision": True},
        {"revision": 2},
        {"parser_version": "registry-target-ledger-v0"},
        {"snapshot_id": "45678901-4567-8901-8456-45678901abcd"},
        {"source_sha256": "a" * 64},
        {"artifact_sha256": "A" * 64},
        {"artifact_sha256": "a" * 64},
        {"source_rows": True},
        {"source_rows": 0},
        {"source_rows": 50_001},
        {"target_count": True},
        {"target_count": 0},
        {"target_count": 5001},
        {"physical_records": True},
        {"physical_records": 2},
        {"extra": PRIVATE_MARKER},
    ],
)
def test_facade_refuses_stale_or_unbound_descriptor(monkeypatch, change):
    source = b"source"
    copy, descriptor = _encoded(source)
    _native(monkeypatch, (copy, json.dumps({**descriptor, **change}).encode()))
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_native_unavailable$"):
        store._encode(source, _edition(source))


@pytest.mark.parametrize(
    "failure",
    [
        "missing_export",
        "list",
        "arity",
        "mutable_copy",
        "header",
        "trailer",
        "text_descriptor",
        "duplicate",
        "invalid_json",
    ],
)
def test_facade_refuses_malformed_native_result(monkeypatch, failure):
    source = b"source"
    copy, descriptor = _encoded(source)
    descriptor_bytes = json.dumps(descriptor).encode()
    malformed_by_failure = {
        "list": [copy, descriptor_bytes],
        "arity": (copy,),
        "mutable_copy": (bytearray(copy), descriptor_bytes),
        "header": (b"X" + copy[1:], descriptor_bytes),
        "trailer": (copy[:-2] + b"xx", descriptor_bytes),
        "text_descriptor": (copy, descriptor_bytes.decode()),
        "duplicate": (copy, descriptor_bytes[:-1] + b',"revision":1}'),
        "invalid_json": (copy, b"{"),
    }
    if failure == "missing_export":
        monkeypatch.setattr(store.importlib, "import_module", lambda _name: SimpleNamespace())
    else:
        _native(monkeypatch, malformed_by_failure[failure])
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_native_unavailable$"):
        store._encode(source, _edition(source))


@pytest.mark.parametrize("failure", ["digest", "empty", "oversize", "mutable", "edition_type"])
def test_admission_invalid_input_cannot_touch_connection_or_native(monkeypatch, failure):
    source = b"source"
    edition = _edition(source)
    if failure == "digest":
        source += b"changed final byte"
    elif failure == "empty":
        source = b""
        edition = _edition(source)
    elif failure == "oversize":
        source = b"x" * (store.MAX_INPUT_BYTES + 1)
        edition = _edition(source)
    elif failure == "mutable":
        source = bytearray(source)
    else:
        edition = SimpleNamespace(**vars(edition))

    def unexpected_native(*_arguments):
        raise AssertionError("Invalid input must not invoke native encoding")

    monkeypatch.setattr(store, "_encode", unexpected_native)
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_input_invalid$"):
        asyncio.run(store.admit_registry_required_targets(None, source, edition))


def test_admission_requires_caller_transaction_before_native_or_write(monkeypatch):
    source = b"source"
    connection = SimpleNamespace(is_in_transaction=lambda: False)
    monkeypatch.setattr(store, "_encode", lambda *_arguments: pytest.fail("No native work without caller transaction"))
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_transaction_required$"):
        asyncio.run(store.admit_registry_required_targets(connection, source, _edition(source)))


def test_cancellation_during_landing_preserves_exception_and_never_retains(monkeypatch):
    source = b"source"
    connection = _Database()
    connection.is_in_transaction = lambda: True
    copy, descriptor = _encoded(source)
    monkeypatch.setattr(store, "_encode", lambda *_arguments: (copy, descriptor))

    async def canceled_stage(*_arguments):
        raise asyncio.CancelledError

    async def unexpected_retention(*_arguments):
        pytest.fail("Canceled landing must not retain source evidence")

    monkeypatch.setattr(store, "_stage", canceled_stage)
    monkeypatch.setattr(store, "_is_replay_after_retention", unexpected_retention)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(store.admit_registry_required_targets(connection, source, _edition(source)))
    assert connection.events == ["begin", "rollback"]


@pytest.mark.parametrize(
    "failure",
    ["field_count", "negative_length", "overlong_field", "truncated", "extra_row", "jsonb_version", "changed_document"],
)
def test_facade_refuses_incomplete_or_unbound_single_copy_frame(monkeypatch, failure):
    source = b"source"
    copy, descriptor = _encoded(source)
    if failure in {"jsonb_version", "changed_document"}:
        cursor = 21
        for _ in range(4):
            length = struct.unpack_from(">i", copy, cursor)[0]
            cursor += 4 + length
        cursor += 4
        if failure == "changed_document":
            cursor += 1
        copy = copy[:cursor] + (b"\x02" if failure == "jsonb_version" else b"[") + copy[cursor + 1 :]
    else:
        malformed_by_failure = {
            "field_count": copy[:19] + struct.pack(">h", 5) + copy[21:],
            "negative_length": copy[:21] + struct.pack(">i", -1) + copy[25:],
            "overlong_field": copy[:21] + struct.pack(">i", len(copy)) + copy[25:],
            "truncated": copy[:30] + b"\xff\xff",
            "extra_row": copy[:-2] + copy[19:],
        }
        copy = malformed_by_failure[failure]
    _native(monkeypatch, (copy, json.dumps(descriptor).encode()))
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_native_unavailable$"):
        store._encode(source, _edition(source))


def test_facade_refuses_document_over_limit_even_when_copy_and_hash_fit(monkeypatch):
    source = b"source"
    copy, descriptor = _encoded(source)
    cursor = 21
    for _ in range(4):
        length = struct.unpack_from(">i", copy, cursor)[0]
        cursor += 4 + length
    length = struct.unpack_from(">i", copy, cursor)[0]
    artifact = copy[cursor + 5 : cursor + 4 + length]
    document = json.loads(artifact)
    document["ledger"]["observations"][0]["raw_cells"][0] = "x" * (
        store.MAX_ARTIFACT_BYTES + 1 - len(artifact) + len("Example")
    )
    artifact = json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    assert len(artifact) == store.MAX_ARTIFACT_BYTES + 1
    copy = copy[:cursor] + struct.pack(">i", len(artifact) + 1) + b"\x01" + artifact + copy[cursor + 4 + length :]
    assert len(copy) <= store.MAX_COPY_BYTES
    descriptor["artifact_sha256"] = hashlib.sha256(artifact).hexdigest()
    _native(monkeypatch, (copy, json.dumps(descriptor).encode()))
    with pytest.raises(store.RegistryRequiredTargetError, match="^registry_required_target_native_unavailable$"):
        store._encode(source, _edition(source))
