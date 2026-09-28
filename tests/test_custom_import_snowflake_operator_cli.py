# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic end-to-end composition checks for the retained operator CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
import datetime as dt
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select

import process.custom_import.snowflake_operator_cli as operator_cli
import process.custom_import.snowflake_source_binding as source_binding
from db.models.custom_import import (
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportLease,
    CustomImportSourceBindingRevision,
)
from process.custom_import.definition import MAX_DEFINITION_BYTES, CustomImportDefinition
from process.custom_import.definition_store import DefinitionRegistrationError
from process.custom_import.execution import ExecutionSubmission
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


def _binding_receipt(
    loaded: source_binding.LoadedSnowflakeSourceBinding,
    *,
    created: bool = True,
    source_binding_sha256: bytes | None = None,
) -> source_binding.SnowflakeSourceBindingReceipt:
    return source_binding.SnowflakeSourceBindingReceipt(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        source_binding_revision_id=loaded.source_binding_revision_id,
        revision_number=1,
        source_binding_sha256=(
            loaded.source_binding_sha256 if source_binding_sha256 is None else source_binding_sha256
        ),
        created=created,
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


class _ResumeSession:
    def __init__(self, execution, lease, now: dt.datetime) -> None:
        self.execution = execution
        self.lease = lease
        self.now = now

    @asynccontextmanager
    async def begin(self):
        yield self

    async def get(self, model, execution_id):
        assert execution_id == self.execution.execution_id
        if model is CustomImportExecution:
            return self.execution
        if model is CustomImportLease:
            return self.lease
        raise AssertionError(f"unexpected resume model: {model!r}")


class _ResumeDatabase(_Database):
    def __init__(self, session: _ResumeSession) -> None:
        super().__init__()
        self.resume_session = session

    @asynccontextmanager
    async def session(self):
        self.sessions.append(self.resume_session)
        yield self.resume_session


def _resume_database(
    loaded: source_binding.LoadedSnowflakeSourceBinding,
    *,
    state: str = "running",
    capture_bundle_id: int | None = 41,
    lease_expired: bool = True,
    mismatch: str | None = None,
) -> _ResumeDatabase:
    now = dt.datetime(2026, 9, 28, 12, tzinfo=dt.UTC)
    execution = SimpleNamespace(
        execution_id=40,
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        source_binding_revision_id=(
            loaded.source_binding_revision_id + 1 if mismatch == "source_binding" else loaded.source_binding_revision_id
        ),
        idempotency_key="synthetic-resume",
        mechanism="local",
        state=state,
        capture_bundle_id=capture_bundle_id,
        request_identity_sha256=b"x" * 32 if mismatch == "request_identity" else None,
    )
    lease = SimpleNamespace(
        execution_id=execution.execution_id,
        fence=1,
        expires_at=now - dt.timedelta(seconds=1) if lease_expired else now + dt.timedelta(seconds=1),
    )
    return _ResumeDatabase(_ResumeSession(execution, lease, now))


def _install_resume_preflight(monkeypatch, database, loaded):
    async def load(session, **identifiers):
        assert session is database.resume_session
        assert identifiers == {
            "definition_revision_id": loaded.definition_revision_id,
            "source_binding_revision_id": loaded.source_binding_revision_id,
        }
        return loaded

    async def lookup(session, **arguments):
        assert session is database.resume_session
        database.resume_lookup_arguments = arguments
        if database.resume_session.execution.request_identity_sha256 is None:
            database.resume_session.execution.request_identity_sha256 = arguments["request_identity_sha256"]
        return ExecutionSubmission(
            execution_id=database.resume_session.execution.execution_id,
            state=database.resume_session.execution.state,
            created=False,
            capture_bundle_id=database.resume_session.execution.capture_bundle_id,
        )

    async def database_time(session):
        assert session is database.resume_session
        return session.now

    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", load)
    monkeypatch.setattr(operator_cli, "lookup_execution_request", lookup)
    monkeypatch.setattr(operator_cli, "database_now", database_time)


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


def test_resume_cli_dispatches_the_exact_retained_identity(monkeypatch, capsys):
    captured_by_key = {}

    async def resume(**arguments):
        captured_by_key.update(arguments)
        return CandidateRunResult(status="canceled", execution_id=41)

    monkeypatch.setattr(operator_cli, "_run_resumed_snowflake_binding", resume)

    exit_code = operator_cli.run_command(
        [
            "resume",
            "--definition-revision-id",
            "32",
            "--source-binding-revision-id",
            "34",
            "--idempotency-key",
            "synthetic-resume",
        ]
    )

    captured_output = capsys.readouterr()
    assert exit_code == 0
    assert captured_by_key == {
        "definition_revision_id": 32,
        "source_binding_revision_id": 34,
        "idempotency_key": "synthetic-resume",
    }
    assert captured_output.err == ""
    assert captured_output.out == '{"execution_id":41,"status":"canceled"}\n'


def test_resume_cli_emits_an_optional_generation_identifier(monkeypatch, capsys):
    async def resume(**_operation_keywords):
        return CandidateRunResult(status="sealed_unpublished", execution_id=41, generation_id=19)

    monkeypatch.setattr(operator_cli, "_run_resumed_snowflake_binding", resume)

    exit_code = operator_cli.run_command(
        [
            "resume",
            "--definition-revision-id",
            "32",
            "--source-binding-revision-id",
            "34",
            "--idempotency-key",
            "synthetic-resume",
        ]
    )

    captured_output = capsys.readouterr()
    assert exit_code == 0
    assert captured_output.err == ""
    assert captured_output.out == '{"execution_id":41,"generation_id":19,"status":"sealed_unpublished"}\n'


def test_resume_cli_redacts_invalid_candidate_output(monkeypatch, capsys):
    async def resume(**_operation_keywords):
        return CandidateRunResult(status="invalid", execution_id=41)

    monkeypatch.setattr(operator_cli, "_run_resumed_snowflake_binding", resume)

    exit_code = operator_cli.run_command(
        [
            "resume",
            "--definition-revision-id",
            "32",
            "--source-binding-revision-id",
            "34",
            "--idempotency-key",
            "synthetic-resume",
        ]
    )

    captured_output = capsys.readouterr()
    assert exit_code == 1
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"failed","status":"error"}\n'
    assert "invalid" not in captured_output.err


@pytest.mark.parametrize(
    ("command_arguments", "rejected_value"),
    (
        (
            [
                "resume",
                "--definition-revision-id",
                "0",
                "--source-binding-revision-id",
                "34",
                "--idempotency-key",
                "synthetic-resume",
            ],
            "0",
        ),
        (
            [
                "resume",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                str(2**63),
                "--idempotency-key",
                "synthetic-resume",
            ],
            str(2**63),
        ),
        (
            [
                "resume",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
                "--idempotency-key",
                "invalid key",
            ],
            "invalid key",
        ),
    ),
)
def test_resume_cli_rejects_invalid_retained_identity_arguments_without_echo(command_arguments, rejected_value, capsys):
    with pytest.raises(SystemExit) as caught:
        operator_cli.run_command(command_arguments)

    captured_output = capsys.readouterr()
    assert caught.value.code == 2
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"invalid_arguments","status":"error"}\n'
    assert rejected_value not in captured_output.err


def test_resume_cli_redacts_unavailable_or_ambiguous_failures(monkeypatch, capsys):
    async def resume(**_arguments):
        raise operator_cli._ResumeUnavailableError("synthetic-private-input")

    monkeypatch.setattr(operator_cli, "_run_resumed_snowflake_binding", resume)

    assert (
        operator_cli.run_command(
            [
                "resume",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
                "--idempotency-key",
                "synthetic-resume",
            ]
        )
        == 1
    )

    captured_output = capsys.readouterr()
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"failed","status":"error"}\n'
    assert "synthetic-private-input" not in captured_output.err


def test_resume_cli_keeps_cancellation_when_cleanup_fails(monkeypatch, capsys):
    database = _resume_database(_loaded_binding())
    database.engine = SimpleNamespace(echo=True)

    async def noisy_connect():
        database.connected += 1
        print("synthetic database output")
        print("synthetic database output", file=sys.stderr)

    async def failing_disconnect():
        database.disconnected += 1
        print("synthetic database output")
        print("synthetic database output", file=sys.stderr)
        raise RuntimeError("synthetic cleanup failure")

    async def interrupted_load(*_arguments, **_keywords):
        raise KeyboardInterrupt

    resume_operation = operator_cli._run_resumed_snowflake_binding

    async def resume(**arguments):
        return await resume_operation(**arguments, database=database)

    monkeypatch.setattr(database, "connect", noisy_connect)
    monkeypatch.setattr(database, "disconnect", failing_disconnect)
    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", interrupted_load)
    monkeypatch.setattr(operator_cli, "_run_resumed_snowflake_binding", resume)

    assert (
        operator_cli.run_command(
            [
                "resume",
                "--definition-revision-id",
                "32",
                "--source-binding-revision-id",
                "34",
                "--idempotency-key",
                "synthetic-resume",
            ]
        )
        == 130
    )

    captured_output = capsys.readouterr()
    assert captured_output.out == ""
    assert captured_output.err == '{"code":"canceled","status":"error"}\n'
    assert database.connected == database.disconnected == 1
    assert database.engine.echo is True


@pytest.mark.asyncio
async def test_resume_replays_only_the_exact_bound_capture_without_source_access(monkeypatch):
    loaded = _loaded_binding()
    database = _resume_database(loaded)
    captured_by_key = {}
    accesses = []
    _install_resume_preflight(monkeypatch, database, loaded)

    def forbidden(*arguments, **keywords):
        accesses.append((arguments, keywords))
        raise AssertionError("resume source access")

    def unexpected_source_component(*_arguments, **_keywords):
        pytest.fail("resume must not construct source access")

    async def run(session_factory, connector, request):
        captured_by_key["session_factory"] = session_factory
        captured_by_key["request"] = request
        captured_by_key["statement"] = connector.build_statement(request.bundle_request)
        return CandidateRunResult(status="sealed_unpublished", execution_id=40)

    monkeypatch.setattr(operator_cli, "_resume_source_access_forbidden", forbidden)
    monkeypatch.setattr(operator_cli, "FixedLocalKeyPairCredentialProvider", unexpected_source_component)
    monkeypatch.setattr(operator_cli, "SnowflakePythonConnectorAdapter", unexpected_source_component)
    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)

    candidate_result = await operator_cli._run_resumed_snowflake_binding(
        definition_revision_id=32,
        source_binding_revision_id=34,
        idempotency_key="synthetic-resume",
        database=database,
    )

    request = captured_by_key["request"]
    assert candidate_result == CandidateRunResult(status="sealed_unpublished", execution_id=40)
    assert captured_by_key["session_factory"] == database.session
    assert request.dataset_id == loaded.dataset_id
    assert request.definition_revision_id == loaded.definition_revision_id
    assert request.schema_revision_id == loaded.schema_revision_id
    assert request.source_binding_revision_id == loaded.source_binding_revision_id
    assert request.source_binding_sha256 == loaded.source_binding_sha256
    assert request.idempotency_key == "synthetic-resume"
    assert captured_by_key["statement"].request == request.bundle_request
    assert database.resume_lookup_arguments["source_binding_revision_id"] == loaded.source_binding_revision_id
    assert database.resume_lookup_arguments["request_identity_sha256"] == operator_cli.bundle_request_identity_sha256(
        request.bundle_request,
        captured_by_key["statement"],
        source_binding_sha256=loaded.source_binding_sha256,
    )
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_RECORDS"' in captured_by_key["statement"].sql
    assert accesses == []
    assert database.connected == database.disconnected == 1


def test_resume_connector_refuses_source_acquisition():
    loaded = _loaded_binding()
    connector = operator_cli._resume_connector(loaded)
    bundle_request = connector.prepare_request(loaded.definition, bindings=loaded.bundle_bindings)

    with pytest.raises(AssertionError, match="resume must not acquire a source"):
        connector.acquire(bundle_request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "capture_bundle_id", "lease_expired", "mismatch"),
    (
        ("queued", 41, True, None),
        ("failed", 41, True, None),
        ("canceled", 41, True, None),
        ("completed", 41, True, None),
        ("no_change", 41, True, None),
        ("running", 41, False, None),
        ("running", None, True, None),
        ("running", 41, True, "request_identity"),
        ("running", 41, True, "source_binding"),
    ),
)
async def test_resume_refuses_nonreplayable_execution_states(
    monkeypatch,
    state,
    capture_bundle_id,
    lease_expired,
    mismatch,
):
    loaded = _loaded_binding()
    database = _resume_database(
        loaded,
        state=state,
        capture_bundle_id=capture_bundle_id,
        lease_expired=lease_expired,
        mismatch=mismatch,
    )
    calls = []
    _install_resume_preflight(monkeypatch, database, loaded)

    async def run(*_arguments, **_keywords):
        calls.append(True)
        return CandidateRunResult(status="activated", execution_id=40)

    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)

    with pytest.raises(operator_cli._ResumeUnavailableError):
        await operator_cli._run_resumed_snowflake_binding(
            definition_revision_id=32,
            source_binding_revision_id=34,
            idempotency_key="synthetic-resume",
            database=database,
        )

    assert calls == []
    assert database.connected == database.disconnected == 1


@pytest.mark.asyncio
async def test_resume_acknowledges_an_expired_cancellation_without_source_access(monkeypatch):
    loaded = _loaded_binding()
    database = _resume_database(loaded, state="canceling")
    calls = []
    _install_resume_preflight(monkeypatch, database, loaded)

    async def run(_session_factory, connector, request):
        calls.append((connector, request))
        connector.build_statement(request.bundle_request)
        return CandidateRunResult(status="canceled", execution_id=40)

    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)

    result = await operator_cli._run_resumed_snowflake_binding(
        definition_revision_id=32,
        source_binding_revision_id=34,
        idempotency_key="synthetic-resume",
        database=database,
    )

    assert result == CandidateRunResult(status="canceled", execution_id=40)
    assert len(calls) == 1
    assert database.connected == database.disconnected == 1


@pytest.mark.asyncio
async def test_resume_does_not_retry_after_a_post_preflight_lease_race(monkeypatch):
    loaded = _loaded_binding()
    database = _resume_database(loaded)
    calls = []
    _install_resume_preflight(monkeypatch, database, loaded)

    async def run(*_arguments, **_keywords):
        calls.append(True)
        return CandidateRunResult(status="not_claimed", execution_id=40)

    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)

    with pytest.raises(operator_cli._ResumeUnavailableError):
        await operator_cli._run_resumed_snowflake_binding(
            definition_revision_id=32,
            source_binding_revision_id=34,
            idempotency_key="synthetic-resume",
            database=database,
        )

    assert calls == [True]
    assert database.connected == database.disconnected == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("lookup_outcome", ("conflict", "missing"))
async def test_resume_requires_an_unambiguous_retained_execution(monkeypatch, lookup_outcome):
    loaded = _loaded_binding()
    session = object()
    lookup_calls = []

    async def lookup(injected_session, **lookup_keywords):
        assert injected_session is session
        lookup_calls.append(lookup_keywords)
        if lookup_outcome == "conflict":
            raise operator_cli.IdempotencyConflict("synthetic identity conflict")
        return None

    monkeypatch.setattr(operator_cli, "lookup_execution_request", lookup)

    with pytest.raises(operator_cli._ResumeUnavailableError):
        await operator_cli._require_exact_resumable_execution(
            session,
            loaded_binding=loaded,
            idempotency_key="synthetic-resume",
            request_identity_sha256=b"x" * 32,
        )

    assert lookup_calls == [
        {
            "dataset_id": loaded.dataset_id,
            "definition_revision_id": loaded.definition_revision_id,
            "schema_revision_id": loaded.schema_revision_id,
            "idempotency_key": "synthetic-resume",
            "mechanism": "local",
            "request_identity_sha256": b"x" * 32,
            "source_binding_revision_id": loaded.source_binding_revision_id,
        }
    ]


@pytest.mark.asyncio
async def test_resume_surfaces_cleanup_failure_after_candidate_success(monkeypatch):
    loaded = _loaded_binding()
    database = _resume_database(loaded)
    calls = []
    _install_resume_preflight(monkeypatch, database, loaded)

    async def run(*_unused, **_operation_keywords):
        calls.append(True)
        return CandidateRunResult(status="sealed_unpublished", execution_id=40)

    async def failing_disconnect():
        database.disconnected += 1
        raise RuntimeError("synthetic cleanup failure")

    monkeypatch.setattr(operator_cli, "run_snowflake_bundle_candidate", run)
    monkeypatch.setattr(database, "disconnect", failing_disconnect)

    with pytest.raises(RuntimeError, match="synthetic cleanup failure"):
        await operator_cli._run_resumed_snowflake_binding(
            definition_revision_id=32,
            source_binding_revision_id=34,
            idempotency_key="synthetic-resume",
            database=database,
        )

    assert calls == [True]
    assert database.connected == database.disconnected == 1


@pytest.mark.parametrize("command", ("execute", "resume"))
def test_operator_rejects_sql_and_credential_path_arguments_without_reflecting_them(command, capsys):
    for forbidden in ("--sql", "--credential-path"):
        with pytest.raises(SystemExit) as caught:
            operator_cli.run_command(
                [
                    command,
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


def test_registration_receipt_rejects_invalid_immutable_results():
    loaded = _loaded_binding()
    invalid_results = (
        object(),
        _binding_receipt(loaded, source_binding_sha256=b"\x00" * 32),
    )

    for invalid_result in invalid_results:
        with pytest.raises(ValueError, match="operator registration result is invalid"):
            operator_cli._registration_receipt(invalid_result, loaded.definition, loaded.binding)


@pytest.mark.asyncio
async def test_committed_registration_receipt_requires_exact_readback(monkeypatch):
    loaded = _loaded_binding()
    registration = _binding_receipt(loaded)
    identity_rows = []
    load_calls = []

    async def load(injected_session, **identifiers):
        assert injected_session is session
        load_calls.append(identifiers)
        return loaded

    async def execute(_statement):
        return SimpleNamespace(all=lambda: tuple(identity_rows))

    session = SimpleNamespace(execute=execute)
    monkeypatch.setattr(operator_cli, "load_snowflake_source_binding", load)

    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="committed registration"):
        await operator_cli._committed_registration_receipt(
            session,
            dataset_key="synthetic_registration",
            registration=registration,
            definition=loaded.definition,
            binding=loaded.binding,
        )

    identity_rows.append(("synthetic_registration", registration.revision_number))
    rendered = await operator_cli._committed_registration_receipt(
        session,
        dataset_key="synthetic_registration",
        registration=registration,
        definition=loaded.definition,
        binding=loaded.binding,
    )

    assert json.loads(rendered)["status"] == "registered"
    assert (
        load_calls
        == [
            {
                "definition_revision_id": loaded.definition_revision_id,
                "source_binding_revision_id": loaded.source_binding_revision_id,
            }
        ]
        * 2
    )


@pytest.mark.asyncio
async def test_registration_reads_back_the_committed_receipt_before_output(monkeypatch):
    loaded = _loaded_binding()
    registration = _binding_receipt(loaded)
    database = _resume_database(loaded)
    calls = []

    async def register(injected_session, *, dataset_key, definition, binding):
        calls.append(("register", injected_session, dataset_key, definition, binding))
        return registration

    async def committed(injected_session, **receipt_keywords):
        calls.append(("committed", injected_session, receipt_keywords))
        return '{"status":"registered"}'

    monkeypatch.setattr(operator_cli, "register_snowflake_source_binding", register)
    monkeypatch.setattr(operator_cli, "_committed_registration_receipt", committed)

    rendered = await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)

    assert json.loads(rendered) == {"status": "registered"}
    assert calls == [
        ("register", database.resume_session, "synthetic_registration", loaded.definition, loaded.binding),
        (
            "committed",
            database.resume_session,
            {
                "dataset_key": "synthetic_registration",
                "registration": registration,
                "definition": loaded.definition,
                "binding": loaded.binding,
            },
        ),
    ]
    assert database.connected == database.disconnected == 1
    assert database.sessions == [database.resume_session, database.resume_session]


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
