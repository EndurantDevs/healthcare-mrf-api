# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic end-to-end composition checks for the retained operator CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from io import BytesIO
from pathlib import Path

import pytest
from sqlalchemy import func, select

import process.custom_import.snowflake_operator_cli as operator_cli
import process.custom_import.snowflake_source_binding as source_binding
from db.models.custom_import import (
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportSourceBindingRevision,
)
from process.custom_import.definition import MAX_DEFINITION_BYTES, CustomImportDefinition
from process.custom_import.definition_store import DefinitionRegistrationError
from process.custom_import.runner import CandidateRunResult
from tests.custom_import_postgres_support import isolated_publication_case


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": [
                {
                    "id": "root_source",
                    "kind": "root",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "semantic_snapshot",
                }
            ],
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": [{"id": "npi", "slot": 1, "type": "string", "nullable": False}],
                },
                "children": [],
            },
            "aliases": {"root_source": {"ROOT_NPI": "npi"}},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _loaded_binding() -> source_binding.LoadedSnowflakeSourceBinding:
    definition = _definition()
    binding = source_binding.SnowflakeSourceBinding.from_json(
        json.dumps(
            {
                "contract": source_binding.SOURCE_BINDING_CONTRACT,
                "connector": source_binding.SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
                "definition_sha256": definition.digest,
                "schema_sha256": definition.schema_digest,
                "source_object": {"fingerprint_sha256": "2" * 64, "version": "snapshot-20260923"},
                "role": "synthetic_reader",
                "warehouse": "synthetic_load",
                "streams": [
                    {
                        "stream_id": "root_source",
                        "relation": ["synthetic", "public", "root_records"],
                        "source_snapshot_token_relation": ["synthetic", "public", "root_snapshots"],
                        "semantic_token_metadata_key": "semantic_snapshot",
                        "source_snapshot_token_column_identifier": "root_snapshot_token",
                        "columns": [{"field_id": "npi", "column_identifier": "root_npi"}],
                    }
                ],
            }
        )
    )
    approved_relations, bundle_bindings = binding.bundle_components(definition)
    return source_binding.LoadedSnowflakeSourceBinding(
        dataset_id=31,
        definition_revision_id=32,
        schema_revision_id=33,
        source_binding_revision_id=34,
        source_binding_sha256=bytes.fromhex(binding.digest),
        definition=definition,
        binding=binding,
        approved_relations=approved_relations,
        bundle_bindings=bundle_bindings,
    )


class _Database:
    def __init__(self) -> None:
        self.connected = 0
        self.disconnected = 0
        self.sessions = []

    async def connect(self) -> None:
        self.connected += 1

    async def disconnect(self) -> None:
        self.disconnected += 1

    @asynccontextmanager
    async def session(self):
        session = object()
        self.sessions.append(session)
        yield session


class _CredentialProvider:
    directories = []

    def __init__(self, directory) -> None:
        self.directories.append(directory)

    def __enter__(self):
        return self

    def __exit__(self, _exception_type, _exception, _traceback) -> None:
        return None

    def load_key_pair(self):
        raise AssertionError("synthetic composition must not contact credentials")


class _Adapter:
    def __init__(self, *, role: str, warehouse: str) -> None:
        self.role = role
        self.warehouse = warehouse

    def fetch_bundle(self, *_args):
        raise AssertionError("synthetic composition must not contact a source")


@pytest.mark.asyncio
async def test_operator_composes_only_retained_configuration_and_generated_sql(monkeypatch):
    database = _Database()
    loaded = _loaded_binding()
    captured_by_key = {}

    async def load(session, **identifiers):
        captured_by_key["load"] = (session, identifiers)
        return loaded

    async def run(session_factory, connector, request):
        statement = connector.build_statement(request.bundle_request)
        captured_by_key["session_factory"] = session_factory
        captured_by_key["request"] = request
        captured_by_key["statement"] = statement
        return CandidateRunResult(status="sealed_unpublished", execution_id=41)

    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", load)
    monkeypatch.setattr(operator_cli, "FixedLocalKeyPairCredentialProvider", _CredentialProvider)
    monkeypatch.setattr(operator_cli, "SnowflakePythonConnectorAdapter", _Adapter)
    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)

    operator_result = await operator_cli._run_retained_snowflake_binding(
        definition_revision_id=32,
        source_binding_revision_id=34,
        idempotency_key="synthetic-run",
        database=database,
    )

    assert operator_result.status == "sealed_unpublished"
    assert database.connected == database.disconnected == 1
    assert captured_by_key["load"][0] is database.sessions[0]
    assert captured_by_key["load"][1] == {"definition_revision_id": 32, "source_binding_revision_id": 34}
    assert captured_by_key["session_factory"] == database.session
    assert captured_by_key["request"].dataset_id == loaded.dataset_id
    assert captured_by_key["request"].source_binding_revision_id == loaded.source_binding_revision_id
    assert captured_by_key["request"].source_binding_sha256 == loaded.source_binding_sha256
    assert captured_by_key["request"].idempotency_key == "synthetic-run"
    assert _CredentialProvider.directories[-1] == operator_cli.FIXED_CREDENTIAL_DIRECTORY
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_RECORDS"' in captured_by_key["statement"].sql
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_SNAPSHOTS"' in captured_by_key["statement"].sql
    assert "SELECT" in captured_by_key["statement"].sql
    assert "DROP" not in captured_by_key["statement"].sql


def test_operator_cli_runs_the_retained_revision_command(monkeypatch, capsys):
    captured_by_key = {}

    async def execute(**arguments):
        captured_by_key.update(arguments)
        return CandidateRunResult(status="sealed_unpublished", execution_id=41)

    monkeypatch.setattr(operator_cli, "_run_retained_snowflake_binding", execute)

    exit_code = operator_cli.run_command(
        [
            "execute",
            "--definition-revision-id",
            "32",
            "--source-binding-revision-id",
            "34",
            "--idempotency-key",
            "synthetic-run",
        ]
    )

    captured_output = capsys.readouterr()
    assert exit_code == 0
    assert captured_by_key == {
        "definition_revision_id": 32,
        "source_binding_revision_id": 34,
        "idempotency_key": "synthetic-run",
    }
    assert captured_output.err == ""
    assert captured_output.out == '{"execution_id":41,"status":"sealed_unpublished"}\n'


def test_operator_rejects_sql_and_credential_path_arguments_without_reflecting_them(capsys):
    for forbidden in ("--sql", "--credential-path"):
        with pytest.raises(SystemExit) as caught:
            operator_cli.run_command(
                [
                    "execute",
                    "--definition-revision-id",
                    "32",
                    "--source-binding-revision-id",
                    "34",
                    "--idempotency-key",
                    "synthetic-run",
                    forbidden,
                    "synthetic-private-input",
                ]
            )
        captured = capsys.readouterr()
        assert caught.value.code == 2
        assert captured.out == ""
        assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'
        assert "synthetic-private-input" not in captured.err


def _registration_document():
    loaded = _loaded_binding()
    return {
        "dataset_key": "synthetic_registration",
        "definition": json.loads(loaded.definition.canonical),
        "source_binding": json.loads(loaded.binding.canonical),
    }


def _registration_stream():
    return BytesIO(json.dumps(_registration_document()).encode())


@pytest.mark.parametrize("scope", ("envelope", "definition", "binding", "stream", "source_object", "column"))
def test_registration_rejects_extra_secret_fields_without_reflecting_input(scope, capsys):
    document = _registration_document()
    objects_by_scope = {
        "envelope": document,
        "definition": document["definition"],
        "binding": document["source_binding"],
        "stream": document["source_binding"]["streams"][0],
        "source_object": document["source_binding"]["source_object"],
        "column": document["source_binding"]["streams"][0]["columns"][0],
    }
    objects_by_scope[scope]["credentials"] = "synthetic-private-input"

    assert operator_cli.run_command(["register"], stream=BytesIO(json.dumps(document).encode())) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_registration","status":"error"}\n'


@pytest.mark.parametrize("forbidden", ("--sql", "--input", "--credential-path", "--dataset-key"))
def test_registration_accepts_no_command_line_payloads(forbidden, capsys):
    with pytest.raises(SystemExit) as caught:
        operator_cli.run_command(["register", forbidden, "synthetic-private-input"])

    captured = capsys.readouterr()
    assert caught.value.code == 2
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_arguments","status":"error"}\n'


@pytest.mark.parametrize("invalid_shape", ("missing", "sequence", "duplicate", "oversize", "noncanonical", "sql"))
def test_registration_rejects_invalid_or_noncanonical_documents(invalid_shape, capsys):
    document = _registration_document()
    if invalid_shape == "missing":
        del document["source_binding"]
    elif invalid_shape == "noncanonical":
        document["source_binding"]["role"] = "synthetic_reader"
    elif invalid_shape == "sql":
        document["source_binding"]["streams"][0]["relation"][2] = "SELECT synthetic_private_input"
    payload = json.dumps(document).encode()
    if invalid_shape == "sequence":
        payload = b"[]"
    elif invalid_shape == "duplicate":
        payload = payload[:-1] + b',"dataset_key":"synthetic_duplicate"}'
    elif invalid_shape == "oversize":
        payload = b" " * (MAX_DEFINITION_BYTES + 1)

    assert operator_cli.run_command(["register"], stream=BytesIO(payload)) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == '{"code":"invalid_registration","status":"error"}\n'


@pytest.mark.asyncio
async def test_registration_rejects_digest_mismatch_before_database_access():
    database = _Database()
    document = _registration_document()
    document["source_binding"]["definition_sha256"] = "0" * 64

    with pytest.raises(source_binding.SnowflakeSourceBindingError):
        await operator_cli._register_snowflake_binding(stream=BytesIO(json.dumps(document).encode()), database=database)

    assert database.connected == database.disconnected == 0
    assert database.sessions == []


def test_registration_command_emits_only_the_safe_receipt(monkeypatch, capsys):
    registration_stream = _registration_stream()

    async def register(*, stream):
        assert stream is registration_stream
        _, definition, binding = operator_cli._registration_from_stdin(stream)
        return operator_cli._registration_receipt(
            source_binding.SnowflakeSourceBindingReceipt(
                dataset_id=31,
                definition_revision_id=32,
                schema_revision_id=33,
                source_binding_revision_id=34,
                revision_number=1,
                source_binding_sha256=bytes.fromhex(binding.digest),
                created=True,
            ),
            definition,
            binding,
        )

    monkeypatch.setattr(operator_cli, "_register_snowflake_binding", register)

    assert operator_cli.run_command(["register"], stream=registration_stream) == 0

    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "dataset_id": 31,
        "definition_revision_id": 32,
        "schema_revision_id": 33,
        "source_binding_revision_id": 34,
        "source_binding_revision": 1,
        "definition_sha256": _loaded_binding().definition.digest,
        "schema_sha256": _loaded_binding().definition.schema_digest,
        "source_binding_sha256": _loaded_binding().binding.digest,
        "status": "registered",
    }


@pytest.mark.parametrize(
    ("failure", "exit_code", "error_code"),
    (
        (source_binding.SnowflakeSourceBindingError, 1, "invalid_registration"),
        (DefinitionRegistrationError, 1, "invalid_registration"),
        (RuntimeError, 1, "failed"),
        (KeyboardInterrupt, 130, "canceled"),
    ),
)
def test_registration_redacts_failures(monkeypatch, capsys, failure, exit_code, error_code):
    async def register(**_arguments):
        raise failure("synthetic-private-input")

    monkeypatch.setattr(operator_cli, "_register_snowflake_binding", register)

    assert operator_cli.run_command(["register"], stream=_registration_stream()) == exit_code

    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {"code": error_code, "status": "error"}


def test_registration_entry_point_redacts_rejected_stdin():
    completed = subprocess.run(
        [sys.executable, "-m", "custom_import_snowflake_operator", "register"],
        cwd=Path(__file__).resolve().parents[1],
        env={"PYTHONWARNINGS": "error"},
        input=b'{"credentials":"synthetic-private-input"}',
        capture_output=True,
        check=False,
        timeout=30,
    )

    assert completed.returncode == 1
    assert completed.stdout == b""
    assert completed.stderr == b'{"code":"invalid_registration","status":"error"}\n'


@pytest.mark.asyncio
async def test_registration_operator_commits_and_replays_the_same_revisions():
    database = _Database()
    async with isolated_publication_case() as case:
        database.session = case.sessions
        first = json.loads(
            await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)
        )
        replay = json.loads(
            await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)
        )

        assert first.pop("status") == "registered"
        assert replay.pop("status") == "replayed"
        assert first == replay
        async with case.sessions() as session:
            loaded = await source_binding.load_snowflake_source_binding(
                session,
                definition_revision_id=first["definition_revision_id"],
                source_binding_revision_id=first["source_binding_revision_id"],
            )
            assert loaded.definition == _loaded_binding().definition
            assert loaded.binding == _loaded_binding().binding
            for model in (CustomImportDataset, CustomImportDefinitionRevision, CustomImportSourceBindingRevision):
                assert await session.scalar(select(func.count()).select_from(model)) == 1
        assert database.connected == database.disconnected == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ("binding_digest", "dataset_key", "revision_number"))
async def test_registration_operator_suppresses_receipt_when_committed_readback_mismatches(monkeypatch, mismatch):
    database = _Database()
    if mismatch == "binding_digest":
        original_load = operator_cli.load_snowflake_source_binding

        async def mismatched_load(*arguments, **keywords):
            loaded = await original_load(*arguments, **keywords)
            return replace(loaded, source_binding_sha256=b"x" * 32)

        monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", mismatched_load)
    else:
        original_register = operator_cli.register_snowflake_source_binding

        async def mismatched_register(*arguments, **keywords):
            if mismatch == "dataset_key":
                keywords["dataset_key"] = "synthetic_other_registration"
            receipt = await original_register(*arguments, **keywords)
            return (
                replace(receipt, revision_number=receipt.revision_number + 1)
                if mismatch == "revision_number"
                else receipt
            )

        monkeypatch.setattr(operator_cli, "register_snowflake_source_binding", mismatched_register)

    async with isolated_publication_case() as case:
        database.session = case.sessions
        with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError):
            await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)

        async with case.sessions() as session:
            for model in (CustomImportDataset, CustomImportDefinitionRevision, CustomImportSourceBindingRevision):
                assert await session.scalar(select(func.count()).select_from(model)) == 1
        assert database.connected == database.disconnected == 1


@pytest.mark.asyncio
async def test_registration_operator_rolls_back_when_persisted_readback_does_not_match(monkeypatch):
    database = _Database()
    original_load = source_binding.load_snowflake_source_binding

    async def mismatched_load(*arguments, **keywords):
        loaded = await original_load(*arguments, **keywords)
        return replace(loaded, source_binding_sha256=b"x" * 32)

    monkeypatch.setattr(source_binding, "load_snowflake_source_binding", mismatched_load)
    async with isolated_publication_case() as case:
        database.session = case.sessions
        with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError):
            await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)

        async with case.sessions() as session:
            for model in (CustomImportDataset, CustomImportDefinitionRevision, CustomImportSourceBindingRevision):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert database.connected == database.disconnected == 1
