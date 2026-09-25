# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact retained evidence reads reject untrusted selectors and broad credentials."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text, update

from api import control_execution_evidence as reader
from db.models.custom_import import CustomImportPublicationEvent, CustomImportSourceBindingRevision
from process.custom_import.operator import (
    CurrentGenerationStatus,
    ExecutionEvidenceExecution,
    ExecutionEvidenceGeneration,
    ExecutionEvidenceStatus,
    GenerationSealStatus,
    NoChangeStatus,
    OperatorObjectNotFound,
)
from process.custom_import.publication import activate_generation, record_no_change
from tests.custom_import_postgres_support import (
    _quoted_publication_schema,
    _seed_completed_generation,
    _seed_no_change_execution,
    _seed_publication_identity,
    digest,
    isolated_publication_case,
)

_NOW = dt.datetime(2026, 1, 2, tzinfo=dt.UTC)


def _request(*, candidate_generation_id=31, key="synthetic-run", bearer="synthetic-read-token"):
    return SimpleNamespace(
        headers={"Authorization": f"Bearer {bearer}"},
        body=json.dumps(
            {
                "dataset_id": 7,
                "definition_revision_id": 11,
                "idempotency_key": key,
                "candidate_generation_id": candidate_generation_id,
            }
        ).encode(),
        query_string="",
    )


def _payload(reply):
    assert reply.headers["Cache-Control"] == "no-store"
    return json.loads(reply.body)


def _evidence(*, state="completed", publication_state="current", generation=True, binding=True, no_change=False):
    execution = ExecutionEvidenceExecution(
        execution_id=23,
        dataset_id=7,
        definition_revision_id=11,
        schema_revision_id=13,
        capture_bundle_id=29 if generation else None,
        mechanism="local",
        state=state,
        failure_class="candidate_rejected" if state == "failed" else "canceled" if state == "canceled" else None,
        started_at=_NOW,
        finished_at=_NOW,
        created_at=_NOW,
        updated_at=_NOW,
    )
    seal = GenerationSealStatus(
        sealing_fence=2,
        root_count=1,
        family_count=1,
        generation_family_count=1,
        family_child_count=0,
        winner_count=1,
        profile_count=0,
        root_scalar_count=1,
        child_scalar_count=0,
        materialization_sha256="a" * 64,
        effective_output_sha256="b" * 64,
        sealed_at=_NOW,
    )
    return ExecutionEvidenceStatus(
        execution=execution,
        definition_sha256="c" * 64,
        schema_sha256="d" * 64,
        source_binding_revision_id=19 if binding else None,
        source_binding_sha256="e" * 64 if binding else None,
        capture_manifest_sha256="f" * 64 if generation else None,
        current=CurrentGenerationStatus(31, 11, 13, 3, _NOW) if generation else None,
        generation=(
            ExecutionEvidenceGeneration(
                31,
                "0" * 64,
                publication_state,
                seal,
                NoChangeStatus(17, 2, "8" * 64, "9" * 64, _NOW) if no_change else None,
            )
            if generation
            else None
        ),
    )


class _Session:
    def __init__(self, *, dataset_id=7, locator_rows=None):
        self.dataset_id = dataset_id
        self.locator_rows = locator_rows
        self.statements = []
        self.active = False

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin(self):
        self.active = True
        try:
            yield self
        finally:
            self.active = False

    async def execute(self, statement):
        self.statements.append(str(statement))
        if len(self.statements) == 1:
            assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            return None
        locator_rows = self.locator_rows
        if locator_rows is None:
            locator_rows = [SimpleNamespace(execution_id=23, dataset_id=self.dataset_id)]
        return SimpleNamespace(all=lambda: locator_rows)


@pytest.fixture
def read_token(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-read-token")
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")


@pytest.mark.asyncio
async def test_auth_precedes_body_and_database_and_rejects_broad_control_token(read_token, monkeypatch):
    session = _Session()

    class _UnreadableRequest:
        headers = {"Authorization": "Bearer synthetic-control-token"}

        @property
        def body(self):
            raise AssertionError("unauthorized body must not be read")

    reply = await reader.serve_execution_evidence(_UnreadableRequest(), session)
    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}
    assert session.statements == []
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-control-token")
    reply = await reader.serve_execution_evidence(_request(bearer="synthetic-control-token"), session)
    assert reply.status == 403 and session.statements == []
    monkeypatch.delenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN")
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 403 and session.statements == []


@pytest.mark.asyncio
async def test_non_ascii_bearer_is_forbidden_before_body_and_database(read_token):
    session = _Session()

    class _UnreadableRequest:
        headers = {"Authorization": "Bearer synthétic-token"}

        @property
        def body(self):
            raise AssertionError("unauthorized body must not be read")

    reply = await reader.serve_execution_evidence(_UnreadableRequest(), session)
    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}
    assert session.statements == []


@pytest.mark.asyncio
async def test_non_bearer_scheme_is_forbidden_without_database_access(read_token):
    session = _Session()
    request = _request()
    request.headers = {"Authorization": "Token synthetic-read-token"}

    reply = await reader.serve_execution_evidence(request, session)

    assert reply.status == 403 and _payload(reply) == {"error": "forbidden"}
    assert session.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b"{}",
        b'{"dataset_id":7,"dataset_id":8}',
        b'{"dataset_id":true,"definition_revision_id":11,"idempotency_key":"synthetic-run","candidate_generation_id":31}',
        b'{"dataset_id":7,"definition_revision_id":11,"idempotency_key":"unsafe key","candidate_generation_id":31}',
        b'{"dataset_id":7,"definition_revision_id":11,"idempotency_key":"synthetic-run","candidate_generation_id":0}',
        b"[7,11]",
    ],
)
async def test_closed_request_rejects_invalid_pins_without_database(read_token, body):
    session = _Session()
    request = _request()
    request.body = body
    reply = await reader.serve_execution_evidence(request, session)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}
    assert session.statements == []


@pytest.mark.asyncio
async def test_query_string_and_existing_transaction_fail_before_evidence_read(read_token):
    session = _Session()
    request = _request()
    request.query_string = "candidate_generation_id=31"
    reply = await reader.serve_execution_evidence(request, session)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}
    assert session.statements == []

    session.active = True
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 503 and _payload(reply) == {"error": "evidence_unavailable"}
    assert session.statements == []


@pytest.mark.asyncio
async def test_exact_current_candidate_projects_only_safe_retained_fields(read_token, monkeypatch):
    session = _Session()

    async def inspect(_session, **pins):
        assert _session is session
        assert pins == {"dataset_id": 7, "execution_id": 23, "candidate_generation_id": 31}
        return _evidence()

    monkeypatch.setattr(reader, "inspect_execution_evidence", inspect)
    reply = await reader.serve_execution_evidence(_request(), session)
    payload = _payload(reply)
    assert reply.status == 200
    assert payload["execution"] == {
        "execution_id": 23,
        "dataset_id": 7,
        "definition_revision_id": 11,
        "schema_revision_id": 13,
        "state": "completed",
        "failure_class": None,
        "finished_at": "2026-01-02T00:00:00Z",
    }
    assert payload["generation"]["current_pointer"] == {"generation_id": 31, "pointer_version": 3}
    assert payload["generation"]["no_change"] is None
    assert payload["source_binding"] == {"revision_id": 19, "sha256": "e" * 64}
    assert "synthetic-run" not in json.dumps(payload)
    assert "generation_sha256" not in json.dumps(payload)
    assert session.statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"


@pytest.mark.asyncio
async def test_locator_dataset_mismatch_does_not_call_inspector(read_token, monkeypatch):
    session = _Session(dataset_id=8)

    async def inspect(*_args, **_kwargs):
        pytest.fail("mismatched locator must not inspect a candidate")

    monkeypatch.setattr(reader, "inspect_execution_evidence", inspect)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 404 and _payload(reply) == {"error": "not_found"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("locator_rows", "status", "error"),
    [
        ([], 404, "not_found"),
        ([SimpleNamespace(execution_id=23, dataset_id=7)] * 2, 503, "evidence_unavailable"),
    ],
)
async def test_missing_or_ambiguous_execution_locator_never_selects_candidate(
    read_token, monkeypatch, locator_rows, status, error
):
    session = _Session(locator_rows=locator_rows)

    async def inspect(*_args, **_kwargs):
        pytest.fail("an unbound execution locator must not inspect a candidate")

    monkeypatch.setattr(reader, "inspect_execution_evidence", inspect)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == status and _payload(reply) == {"error": error}


@pytest.mark.asyncio
async def test_foreign_candidate_rejects_and_stale_publication_is_non_activatable(read_token, monkeypatch):
    session = _Session()

    async def foreign(*_args, **_kwargs):
        raise OperatorObjectNotFound("foreign candidate")

    monkeypatch.setattr(reader, "inspect_execution_evidence", foreign)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 404 and _payload(reply) == {"error": "not_found"}

    async def stale(*_args, **_kwargs):
        return _evidence(publication_state="sealed_unpublished")

    monkeypatch.setattr(reader, "inspect_execution_evidence", stale)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 200
    assert _payload(reply)["generation"]["current_pointer"] is None

    async def superseded(*_args, **_kwargs):
        return _evidence(publication_state="superseded")

    monkeypatch.setattr(reader, "inspect_execution_evidence", superseded)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 200
    assert _payload(reply)["generation"]["publication_state"] == "superseded"

    async def unbound(*_args, **_kwargs):
        return _evidence(binding=False)

    monkeypatch.setattr(reader, "inspect_execution_evidence", unbound)
    reply = await reader.serve_execution_evidence(_request(), session)
    assert reply.status == 503 and _payload(reply) == {"error": "evidence_unavailable"}


@pytest.mark.asyncio
async def test_no_change_is_terminal_but_not_current(read_token, monkeypatch):
    async def unchanged(*_args, **_kwargs):
        return _evidence(state="no_change", publication_state="no_change", no_change=True)

    monkeypatch.setattr(reader, "inspect_execution_evidence", unchanged)
    reply = await reader.serve_execution_evidence(_request(), _Session())
    response_document = _payload(reply)
    assert reply.status == 200
    assert response_document["execution"]["state"] == "no_change"
    assert response_document["generation"]["current_pointer"] is None
    assert response_document["generation"]["no_change"] == {
        "base_generation_id": 17,
        "base_pointer_version": 2,
        "effective_output_sha256": "8" * 64,
        "receipt_sha256": "9" * 64,
        "sealed_at": "2026-01-02T00:00:00Z",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "evidence",
    [
        replace(_evidence(), execution=replace(_evidence().execution, finished_at=_NOW.replace(tzinfo=None))),
        replace(_evidence(), execution=replace(_evidence().execution, dataset_id=8)),
        _evidence(state="no_change", publication_state="current", no_change=True),
        _evidence(publication_state="no_change", no_change=True),
        replace(_evidence(), current=CurrentGenerationStatus(99, 11, 13, 3, _NOW)),
    ],
    ids=["naive-terminal-time", "foreign-execution", "invalid-no-change", "invalid-completed", "stale-pointer"],
)
async def test_inconsistent_retained_projection_fails_closed(read_token, monkeypatch, evidence):
    async def inspect(*_args, **_kwargs):
        return evidence

    monkeypatch.setattr(reader, "inspect_execution_evidence", inspect)
    reply = await reader.serve_execution_evidence(_request(), _Session())
    assert reply.status == 503 and _payload(reply) == {"error": "evidence_unavailable"}


@pytest.mark.asyncio
async def test_route_passes_only_request_and_scoped_session_to_reader(monkeypatch):
    session = object()
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=session))

    async def serve(actual_request, actual_session):
        assert actual_request is request
        assert actual_session is session
        return "scoped reply"

    monkeypatch.setattr(reader, "serve_execution_evidence", serve)
    assert await reader.execution_evidence(request) == "scoped reply"


@pytest.mark.asyncio
async def test_explicit_null_candidate_requires_failed_or_canceled_without_generation(read_token, monkeypatch):
    session = _Session()

    async def failed(*_args, **_kwargs):
        return _evidence(state="failed", generation=False)

    monkeypatch.setattr(reader, "inspect_execution_evidence", failed)
    reply = await reader.serve_execution_evidence(_request(candidate_generation_id=None), session)
    payload = _payload(reply)
    assert reply.status == 200
    assert payload["execution"]["failure_class"] == "candidate_rejected"
    assert payload["generation"] is None and payload["capture"] is None

    async def bad(*_args, **_kwargs):
        evidence = _evidence(state="failed")
        return replace(evidence, execution=replace(evidence.execution, state="failed"))

    monkeypatch.setattr(reader, "inspect_execution_evidence", bad)
    reply = await reader.serve_execution_evidence(_request(candidate_generation_id=None), session)
    assert reply.status == 503 and _payload(reply) == {"error": "evidence_unavailable"}


async def _seed_bound_graph(case):
    suffix = uuid.uuid4().hex
    async with case.sessions() as session, session.begin():
        seed = await _seed_publication_identity(session, suffix)
        canonical = '{"synthetic":true}'
        contract = "custom-import/source-binding/v1"
        binding = CustomImportSourceBindingRevision(
            dataset_id=seed.dataset.dataset_id,
            definition_revision_id=seed.definition_revision.definition_revision_id,
            schema_revision_id=seed.schema_revision.schema_revision_id,
            revision_number=1,
            binding_contract=contract,
            connector_kind="snowflake_bundle",
            definition_sha256=seed.definition_revision.definition_sha256,
            schema_sha256=seed.schema_revision.schema_sha256,
            source_object_fingerprint_sha256=digest("synthetic-object"),
            source_object_version="synthetic-v1",
            canonical_binding=canonical,
            binding_sha256=hashlib.sha256(f"{contract}:{canonical}".encode()).digest(),
        )
        session.add(binding)
        await session.flush()
        first_execution, first_generation = await _seed_completed_generation(
            session, seed, suffix, 1, None, source_binding_revision_id=binding.source_binding_revision_id
        )
        second_execution, second_generation = await _seed_completed_generation(
            session, seed, suffix, 2, first_generation, source_binding_revision_id=binding.source_binding_revision_id
        )
        no_change_execution, no_change_generation, no_change_token = await _seed_no_change_execution(
            session, seed, suffix, source_binding_revision_id=binding.source_binding_revision_id
        )
        pins = SimpleNamespace(
            dataset_id=seed.dataset.dataset_id,
            definition_revision_id=seed.definition_revision.definition_revision_id,
            first_execution_id=first_execution.execution_id,
            first_generation_id=first_generation.generation_id,
            first_key=first_execution.idempotency_key,
            second_generation_id=second_generation.generation_id,
            second_key=second_execution.idempotency_key,
            no_change_execution_id=no_change_execution.execution_id,
            no_change_generation_id=no_change_generation.generation_id,
            no_change_key=no_change_execution.idempotency_key,
            no_change_token=no_change_token,
            binding_id=binding.source_binding_revision_id,
        )
    async with case.sessions() as session, session.begin():
        await activate_generation(
            session,
            dataset_id=pins.dataset_id,
            target_generation_id=pins.first_generation_id,
            expected_generation_id=None,
            expected_pointer_version=0,
        )
    return pins


def _bound_request(pins, *, dataset_id=None, candidate_generation_id=None, key=None):
    request = _request()
    request.body = json.dumps(
        {
            "dataset_id": pins.dataset_id if dataset_id is None else dataset_id,
            "definition_revision_id": pins.definition_revision_id,
            "idempotency_key": pins.first_key if key is None else key,
            "candidate_generation_id": pins.first_generation_id
            if candidate_generation_id is None
            else candidate_generation_id,
        }
    ).encode()
    return request


async def _read_bound_case(case, request):
    async with case.sessions() as session:
        return await reader.serve_execution_evidence(request, session)


async def _assert_retained_no_change(case, pins):
    async with case.sessions() as session, session.begin():
        await record_no_change(
            session,
            dataset_id=pins.dataset_id,
            execution_id=pins.no_change_execution_id,
            expected_generation_id=pins.first_generation_id,
            expected_pointer_version=1,
            candidate_generation_id=pins.no_change_generation_id,
            lease_fence=1,
            lease_token=pins.no_change_token,
        )
    reply = await _read_bound_case(
        case, _bound_request(pins, candidate_generation_id=pins.no_change_generation_id, key=pins.no_change_key)
    )
    assert reply.status == 200
    response_document = _payload(reply)
    assert response_document["execution"]["state"] == "no_change"
    assert response_document["generation"]["publication_state"] == "no_change"
    assert response_document["generation"]["current_pointer"] is None
    assert response_document["generation"]["no_change"]["base_generation_id"] == pins.first_generation_id


@pytest.mark.asyncio
async def test_retained_locator_and_current_publication_use_one_read_snapshot(read_token):
    """Exercise real joined identity, stale publication, and foreign selectors."""

    async with isolated_publication_case() as case:
        pins = await _seed_bound_graph(case)
        reply = await _read_bound_case(case, _bound_request(pins))
        response_document = _payload(reply)
        assert reply.status == 200
        assert response_document["execution"]["execution_id"] == pins.first_execution_id
        assert response_document["generation"]["current_pointer"] == {
            "generation_id": pins.first_generation_id,
            "pointer_version": 1,
        }
        assert response_document["source_binding"]["revision_id"] == pins.binding_id
        reply = await _read_bound_case(case, _bound_request(pins, candidate_generation_id=pins.second_generation_id))
        assert reply.status == 404 and _payload(reply) == {"error": "not_found"}
        reply = await _read_bound_case(case, _bound_request(pins, dataset_id=pins.dataset_id + 1))
        assert reply.status == 404 and _payload(reply) == {"error": "not_found"}
        await _assert_retained_no_change(case, pins)
        async with case.sessions() as session, session.begin():
            await activate_generation(
                session,
                dataset_id=pins.dataset_id,
                target_generation_id=pins.second_generation_id,
                expected_generation_id=pins.first_generation_id,
                expected_pointer_version=1,
            )
        reply = await _read_bound_case(case, _bound_request(pins))
        assert reply.status == 200
        assert _payload(reply)["generation"]["publication_state"] == "superseded"
        assert _payload(reply)["generation"]["current_pointer"] is None
        reply = await _read_bound_case(
            case, _bound_request(pins, candidate_generation_id=pins.second_generation_id, key=pins.second_key)
        )
        assert reply.status == 200
        assert _payload(reply)["generation"]["current_pointer"]["pointer_version"] == 2


@pytest.mark.asyncio
async def test_legacy_nonfinal_publication_is_not_superseded_evidence(read_token):
    """A synthetic pre-finality event cannot certify historical publication."""

    async with isolated_publication_case() as case:
        pins = await _seed_bound_graph(case)
        async with case.sessions() as session, session.begin():
            await activate_generation(
                session,
                dataset_id=pins.dataset_id,
                target_generation_id=pins.second_generation_id,
                expected_generation_id=pins.first_generation_id,
                expected_pointer_version=1,
            )
        event_table = f"{_quoted_publication_schema(case.schema_name)}.custom_import_publication_event"
        async with case.sessions() as session, session.begin():
            # Only this disposable schema bypasses immutability to model a migrated legacy row.
            await session.execute(
                text(f"ALTER TABLE {event_table} DISABLE TRIGGER custom_import_publication_event_immutable_row_guard")
            )
            changed = await session.execute(
                update(CustomImportPublicationEvent)
                .where(CustomImportPublicationEvent.execution_id == pins.first_execution_id)
                .values(finality_contract=None)
            )
            await session.execute(
                text(f"ALTER TABLE {event_table} ENABLE TRIGGER custom_import_publication_event_immutable_row_guard")
            )
            assert changed.rowcount == 1
        reply = await _read_bound_case(case, _bound_request(pins))
        evidence_document = _payload(reply)
        assert reply.status == 200
        assert evidence_document["execution"]["execution_id"] == pins.first_execution_id
        assert evidence_document["generation"]["publication_state"] == "sealed_unpublished"
        assert evidence_document["generation"]["seal"] is not None
        assert evidence_document["generation"]["current_pointer"] is None
