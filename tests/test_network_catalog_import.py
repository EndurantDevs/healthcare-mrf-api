"""Bounded operator input and receipt-only lifecycle checks."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_catalog_import as catalog_import
from process import network_catalog_store as catalog_store


def _catalog_row():
    return {
        "source_system": "fhir",
        "source_id": "source-v1",
        "scope_key": "medical",
        "evidence_id": "synthetic-evidence",
        "legacy_uuid": str(uuid4()),
        "reviewed_network_id": None,
        "display_name": "Synthetic Network",
        "aliases": ["Reviewed alias"],
    }


def _arguments(input_file, input_bytes):
    return [
        "--input-file",
        str(input_file),
        "--input-sha256",
        hashlib.sha256(input_bytes).hexdigest(),
        "--source-system",
        "fhir",
        "--source-id",
        "source-v1",
        "--alias-scope",
        "medical",
        "--reason",
        "Reviewed source names",
        "--idempotency-key",
        "source-v1-catalog",
        "--actor-user-id",
        str(uuid4()),
        "--actor-client-id",
        "system",
        "--api-role",
        "example_api",
        "--owner-role",
        "example_owner",
        "--control-schema",
        "example_registry",
    ]


@pytest.mark.parametrize(
    "invalid", ["digest", "duplicate", "fields", "context", "name", "rows", "bytes", "uuid", "nested"]
)
def test_invalid_batch_is_redacted_before_database_access(monkeypatch, tmp_path, capsys, invalid):
    catalog_row = _catalog_row()
    if invalid == "fields":
        catalog_row["unknown"] = "synthetic-private-detail"
    elif invalid == "context":
        catalog_row["source_id"] = "other-source"
    elif invalid == "name":
        catalog_row["display_name"] = "bad\nname"
    elif invalid == "uuid":
        catalog_row["legacy_uuid"] = "synthetic-private-detail"
    input_bytes = json.dumps([catalog_row]).encode()
    if invalid == "duplicate":
        input_bytes = input_bytes.replace(b'"display_name":', b'"display_name":"earlier","display_name":')
    elif invalid == "rows":
        input_bytes = b"[]"
    elif invalid == "bytes":
        input_bytes = b" " * (catalog_import.MAX_CATALOG_BYTES + 1)
    elif invalid == "nested":
        input_bytes = b"[" * 10000 + b"0" + b"]" * 10000
    input_file = tmp_path / "catalog.json"
    input_file.write_bytes(input_bytes)
    arguments = _arguments(input_file, b"different" if invalid == "digest" else input_bytes)

    def unexpected_database():
        raise AssertionError("Invalid input must not create a database")

    monkeypatch.setattr(catalog_import, "Database", unexpected_database)
    assert catalog_import.run_command(arguments) == 2
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"invalid_catalog_input","status":"error"}\n'


def test_argument_and_file_errors_do_not_reflect_input(tmp_path, capsys):
    with pytest.raises(SystemExit, match="2"):
        catalog_import.run_command(["--unknown=synthetic-private-detail"])
    assert capsys.readouterr().err == '{"code":"invalid_arguments","status":"error"}\n'
    assert catalog_import.run_command(_arguments(tmp_path / "absent.json", b"[]")) == 1
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == '{"code":"input_unavailable","status":"error"}\n'


class _OperatorDatabase:
    def __init__(self, failure_stage):
        self.engine = SimpleNamespace(echo=True)
        self.failure_stage = failure_stage
        self.events = []
        self.in_transaction = False

    async def connect(self):
        self.events.append("connect")
        if self.failure_stage == "connect":
            raise RuntimeError("synthetic-private-detail")

    async def disconnect(self):
        self.events.append("disconnect")
        if self.failure_stage == "disconnect":
            raise RuntimeError("synthetic-private-detail")

    @asynccontextmanager
    async def transaction(self):
        self.events.append("begin")
        self.in_transaction = True
        try:
            yield self
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
        finally:
            self.in_transaction = False


@pytest.mark.parametrize("failure_stage", [None, "connect", "operation", "disconnect", "cancel"])
def test_cli_transaction_pool_teardown_and_fixed_receipts(monkeypatch, tmp_path, capsys, failure_stage):
    database = _OperatorDatabase(failure_stage)
    input_bytes = json.dumps([_catalog_row()]).encode()
    input_file = tmp_path / "catalog.json"
    input_file.write_bytes(input_bytes)

    async def import_catalog(session, catalog_rows, actor, source_context, **keywords):
        assert session is database and database.in_transaction and database.engine.echo is False
        assert catalog_rows == json.loads(input_bytes) and actor.kind == "platform_admin"
        assert source_context.idempotency_key == "source-v1-catalog"
        assert keywords == {
            "api_role": "example_api",
            "owner_role": "example_owner",
            "control_schema": "example_registry",
        }
        database.events.append("operation")
        if failure_stage == "cancel":
            raise KeyboardInterrupt("synthetic-private-detail")
        if failure_stage == "operation":
            raise RuntimeError("synthetic-private-detail")
        return {"created_records": 1, "preserved_records": 0, "request_sha256": "a" * 64}

    monkeypatch.setattr(catalog_import, "Database", lambda: database)
    monkeypatch.setattr(catalog_import, "import_network_catalog", import_catalog)
    expected_status = 130 if failure_stage == "cancel" else 1 if failure_stage else 0
    assert catalog_import.run_command(_arguments(input_file, input_bytes)) == expected_status
    captured = capsys.readouterr()
    if failure_stage:
        code = "canceled" if failure_stage == "cancel" else "catalog_import_failed"
        assert captured.out == "" and json.loads(captured.err) == {"status": "error", "code": code}
        assert captured.err.count("\n") == 1
    else:
        assert captured.err == "" and captured.out.count("\n") == 1
        assert json.loads(captured.out) == {
            "status": "ok",
            "input_sha256": hashlib.sha256(input_bytes).hexdigest(),
            "created_records": 1,
            "preserved_records": 0,
            "request_sha256": "a" * 64,
        }
    assert database.events[-1] == "disconnect" and database.events.count("disconnect") == 1
    assert ("rollback" in database.events) == (failure_stage in {"operation", "cancel"})
    assert not database.in_transaction and database.engine.echo is True


@pytest.mark.parametrize("invalid", ["encoded_bytes", "context_type", "client_owner"])
def test_catalog_batch_rejects_invalid_operator_context_before_database(tmp_path, invalid):
    input_bytes = json.dumps([_catalog_row()]).encode()
    path = tmp_path / "catalog.json"
    path.write_bytes(input_bytes)
    parsed = catalog_import._parser().parse_args(_arguments(path, input_bytes))
    rows, actor, context = catalog_import._read_input(parsed)
    if invalid == "encoded_bytes":
        rows[0]["display_name"] = "x" * catalog_import.MAX_CATALOG_BYTES
    elif invalid == "context_type":
        context = SimpleNamespace(**context.__dict__)
    else:
        actor = replace(actor, kind="client_owner")
    with pytest.raises(ValueError):
        catalog_import._prepare_input(rows, actor, context)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["no_transaction", "missing_control", "restore_only", "operation_and_restore"])
async def test_catalog_role_cleanup_preserves_primary_failure(monkeypatch, tmp_path, outcome):
    input_bytes = json.dumps([_catalog_row()]).encode()
    path = tmp_path / "catalog.json"
    path.write_bytes(input_bytes)
    parsed = catalog_import._parser().parse_args(_arguments(path, input_bytes))
    catalog_rows, actor, context = catalog_import._read_input(parsed)
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"id": 1}), execute=AsyncMock())
    driver_wrapper = SimpleNamespace(
        get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=connection))
    )

    @asynccontextmanager
    async def savepoint():
        yield

    session = SimpleNamespace(
        in_transaction=lambda: outcome != "no_transaction",
        scalar=AsyncMock(return_value='"example_api"'),
        connection=AsyncMock(return_value=driver_wrapper),
        begin_nested=savepoint,
    )
    for name in ("verify_registry_management_permissions", "assume_registry_source_owner"):
        monkeypatch.setattr(catalog_import, name, AsyncMock())
    monkeypatch.setattr(
        catalog_import, "adopt_legacy_network_aliases", AsyncMock(return_value=[SimpleNamespace(network_id=42)])
    )
    seed = AsyncMock(return_value={"created_records": 1})
    monkeypatch.setattr(catalog_import, "seed_network_catalog_drafts", seed)
    if outcome == "missing_control":
        connection.fetchrow.return_value = None
    if outcome in {"restore_only", "operation_and_restore"}:
        connection.execute.side_effect = RuntimeError("role restore failed")
    if outcome == "operation_and_restore":
        seed.side_effect = ValueError("original import failed")
    error_type, message = {
        "no_transaction": (ValueError, "caller-owned transaction"),
        "missing_control": (ValueError, "revision control is missing"),
        "restore_only": (RuntimeError, "role restore failed"),
        "operation_and_restore": (ValueError, "original import failed"),
    }[outcome]
    with pytest.raises(error_type, match=message):
        await catalog_import.import_network_catalog(
            session,
            catalog_rows,
            actor,
            context,
            api_role="example_api",
            owner_role="example_owner",
            control_schema="example_registry",
        )
    if outcome == "no_transaction":
        session.connection.assert_not_awaited()
        connection.execute.assert_not_awaited()
    else:
        connection.execute.assert_awaited_once_with('SET LOCAL ROLE "example_api"')
    if outcome in {"no_transaction", "missing_control"}:
        seed.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["context_type", "client_owner", "reviewed_binding", "aliases_type"])
async def test_catalog_seed_refuses_untrusted_context_or_changed_reviewed_id(tmp_path, invalid):
    input_bytes = json.dumps([_catalog_row()]).encode()
    path = tmp_path / "catalog.json"
    path.write_bytes(input_bytes)
    parsed = catalog_import._parser().parse_args(_arguments(path, input_bytes))
    rows, actor, context = catalog_import._read_input(parsed)
    alias, fields = catalog_import._prepare_input(rows, actor, context)[0]
    if invalid == "context_type":
        context = SimpleNamespace(**context.__dict__)
    elif invalid == "client_owner":
        actor = replace(actor, kind="client_owner")
    elif invalid == "reviewed_binding":
        alias = replace(alias, reviewed_network_id=43)
    draft = catalog_store.NetworkCatalogDraftRow(alias, 42, **fields)
    if invalid == "aliases_type":
        draft = replace(draft, aliases={})
    with pytest.raises(catalog_store.NetworkCatalogSeedError):
        await catalog_store.seed_network_catalog_drafts(
            None,
            [draft],
            actor,
            context,
            api_role="example_api",
            owner_role="example_owner",
            control_schema="example_registry",
        )
