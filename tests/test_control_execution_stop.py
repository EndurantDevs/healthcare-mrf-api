# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Private stop control requires exact source pins and a distinct token."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import process.custom_import.snowflake_operator_cli as operator_cli
from api import control_execution_evidence as evidence
from api import control_execution_stop as stop
from db.models.custom_import import CustomImportExecution
from process.custom_import import execution as lifecycle
from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector
from process.custom_import.snowflake_candidate import bundle_request_identity_sha256
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_snowflake_operator_cli import _Database, _loaded_binding, _registration_stream


def _request(*, bearer="synthetic-stop-token", document=None):
    return SimpleNamespace(
        headers={"Authorization": "Bearer " + bearer},
        body=json.dumps(
            document
            or {
                "dataset_id": 7,
                "definition_revision_id": 11,
                "source_binding_revision_id": 19,
                "idempotency_key": "synthetic-run",
            }
        ).encode(),
        query_string="",
    )


class _Session:
    def __init__(self):
        self.begins = 0

    def in_transaction(self):
        return False

    @asynccontextmanager
    async def begin(self):
        self.begins += 1
        yield


def _payload(reply):
    assert reply.headers["Cache-Control"] == "no-store"
    return json.loads(reply.body)


@pytest.mark.asyncio
async def test_bound_cancellation_requires_retained_request_identity_before_reservation():
    with pytest.raises(ValueError, match="request_identity_sha256 is required"):
        await lifecycle.request_bound_execution_cancellation(
            None,
            dataset_id=7,
            definition_revision_id=11,
            schema_revision_id=13,
            source_binding_revision_id=19,
            idempotency_key="synthetic-run",
            request_identity_sha256=None,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("observed", "error"),
    [(None, lifecycle.ExecutionNotFound), (object(), lifecycle.IdempotencyConflict)],
)
async def test_stop_finalization_requires_an_existing_exact_request(monkeypatch, observed, error):
    async def no_database_result(*_args, **_kwargs):
        return None

    async def retained_execution(*_args, **_kwargs):
        return observed

    monkeypatch.setattr(lifecycle, "_lock_dataset", no_database_result)
    monkeypatch.setattr(lifecycle, "_locked_submission_execution", retained_execution)
    monkeypatch.setattr(lifecycle, "_has_matching_execution_identity", lambda *_args: False)
    with pytest.raises(error):
        await lifecycle.finalize_stopped_execution_request(
            SimpleNamespace(in_transaction=lambda: True),
            dataset_id=7,
            definition_revision_id=11,
            schema_revision_id=13,
            source_binding_revision_id=19,
            idempotency_key="synthetic-run",
            request_identity_sha256=b"0" * 32,
        )


@pytest.mark.asyncio
async def test_stop_reconstructs_the_worker_request_digest_without_source_access(monkeypatch):
    loaded = _loaded_binding()

    async def load(_session, **pins):
        assert pins == {
            "definition_revision_id": loaded.definition_revision_id,
            "source_binding_revision_id": loaded.source_binding_revision_id,
        }
        return loaded

    monkeypatch.setattr(stop, "load_snowflake_source_binding", load)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=loaded.approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=stop._no_source_access),
        adapter=SimpleNamespace(fetch_bundle=stop._no_source_access),
    )
    bundle_request = connector.prepare_request(loaded.definition, bindings=loaded.bundle_bindings)
    expected = bundle_request_identity_sha256(
        bundle_request,
        connector.build_statement(bundle_request),
        source_binding_sha256=loaded.source_binding_sha256,
    )
    assert await stop._retained_identity(
        object(), loaded.dataset_id, loaded.definition_revision_id, loaded.source_binding_revision_id
    ) == (loaded.schema_revision_id, expected)
    with pytest.raises(ValueError, match="binding dataset differs"):
        await stop._retained_identity(
            object(), loaded.dataset_id + 1, loaded.definition_revision_id, loaded.source_binding_revision_id
        )


@pytest.mark.asyncio
async def test_stop_requires_distinct_token_before_database_access(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-read-token")
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-control-token")
    session = _Session()
    assert (
        await stop.serve_execution_stop(_request(bearer="synthetic-read-token"), session, finalize=False)
    ).status == 403
    assert session.begins == 0
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-read-token")
    assert (
        await stop.serve_execution_stop(_request(bearer="synthetic-read-token"), session, finalize=False)
    ).status == 403
    assert session.begins == 0


@pytest.mark.asyncio
async def test_stop_uses_retained_digest_and_exact_phase(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    calls = []

    async def identity(_session, dataset_id, definition_revision_id, source_binding_revision_id):
        assert (dataset_id, definition_revision_id, source_binding_revision_id) == (7, 11, 19)
        return 13, b"0" * 32

    async def transition(_session, **pins):
        calls.append(pins)
        return SimpleNamespace(execution_id=23, state="canceled")

    monkeypatch.setattr(stop, "_retained_identity", identity)
    monkeypatch.setattr(stop, "request_bound_execution_cancellation", transition)
    monkeypatch.setattr(stop, "finalize_stopped_execution_request", transition)
    for finalize in (False, True):
        reply = await stop.serve_execution_stop(_request(), _Session(), finalize=finalize)
        assert reply.status == 200
        assert _payload(reply) == {"execution_id": 23, "state": "canceled"}
    assert (
        calls
        == [
            {
                "dataset_id": 7,
                "definition_revision_id": 11,
                "schema_revision_id": 13,
                "source_binding_revision_id": 19,
                "idempotency_key": "synthetic-run",
                "request_identity_sha256": b"0" * 32,
            }
        ]
        * 2
    )


@pytest.mark.asyncio
async def test_stop_rejects_duplicate_or_extra_pins(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    session = _Session()
    request = _request()
    request.body = request.body[:-1] + b',"dataset_id":8}'
    assert _payload(await stop.serve_execution_stop(request, session, finalize=False)) == {"error": "invalid_request"}
    document = json.loads(_request().body)
    document["request_identity_sha256"] = "0" * 64
    assert _payload(await stop.serve_execution_stop(_request(document=document), session, finalize=True)) == {
        "error": "invalid_request"
    }
    assert session.begins == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_body", (b"", b" " * 513, b"\xff", b"[]", b'{"dataset_id":true}'))
async def test_stop_rejects_malformed_bodies_before_database(monkeypatch, invalid_body):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    request = _request()
    request.body = invalid_body
    session = _Session()
    reply = await stop.serve_execution_stop(request, session, finalize=False)
    assert reply.status == 400 and _payload(reply) == {"error": "invalid_request"}
    assert session.begins == 0


@pytest.mark.asyncio
async def test_stop_rejects_query_keys_and_non_bearer_authorization(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    session = _Session()
    request = _request()
    request.headers = {"Authorization": "Basic synthetic-stop-token"}
    assert (await stop.serve_execution_stop(request, session, finalize=False)).status == 403
    request = _request()
    request.query_string = "finalize=true"
    assert (await stop.serve_execution_stop(request, session, finalize=False)).status == 400
    request = _request()
    document = json.loads(request.body)
    document["idempotency_key"] = "invalid key"
    request.body = json.dumps(document).encode()
    assert (await stop.serve_execution_stop(request, session, finalize=False)).status == 400
    assert session.begins == 0


@pytest.mark.asyncio
async def test_stop_requires_its_own_transaction(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    for session in (None, SimpleNamespace(in_transaction=lambda: True)):
        reply = await stop.serve_execution_stop(_request(), session, finalize=True)
        assert reply.status == 503 and _payload(reply) == {"error": "stop_unavailable"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure, status, code",
    (
        (stop.IdempotencyConflict, 409, "identity_conflict"),
        (RuntimeError, 503, "stop_unavailable"),
    ),
)
async def test_stop_redacts_identity_and_database_failures(monkeypatch, failure, status, code):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")

    async def unavailable(*_arguments):
        raise failure("synthetic-private-source-detail")

    monkeypatch.setattr(stop, "_retained_identity", unavailable)
    session = _Session()
    reply = await stop.serve_execution_stop(_request(), session, finalize=True)
    assert reply.status == status and _payload(reply) == {"error": code}
    assert session.begins == 1


@pytest.mark.asyncio
async def test_stop_routes_preserve_exact_phase_and_session(monkeypatch):
    request = _request()
    session = _Session()
    request.ctx = SimpleNamespace(sa_session=session)
    phases = []
    result = object()

    async def serve(route_request, route_session, *, finalize):
        assert route_request is request and route_session is session
        phases.append(finalize)
        return result

    monkeypatch.setattr(stop, "serve_execution_stop", serve)
    assert await stop.execution_stop_request(request) is result
    assert await stop.execution_stop_finalize(request) is result
    assert phases == [False, True]
    with pytest.raises(RuntimeError, match="source access is unavailable"):
        stop._no_source_access()


@pytest.mark.asyncio
async def test_pre_cancelled_bound_request_is_retained_as_terminal_evidence(monkeypatch):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-read-token")
    database = _Database()
    async with isolated_publication_case() as case:
        database.session = case.sessions
        registered = json.loads(
            await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)
        )
        async with case.sessions() as session:
            loaded = await stop.load_snowflake_source_binding(
                session,
                definition_revision_id=registered["definition_revision_id"],
                source_binding_revision_id=registered["source_binding_revision_id"],
            )
        pins_by_field = {
            "dataset_id": loaded.dataset_id,
            "definition_revision_id": loaded.definition_revision_id,
            "source_binding_revision_id": loaded.source_binding_revision_id,
            "idempotency_key": "synthetic-stop-run",
        }
        async with case.sessions() as session:
            cancelled = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=False)
        assert cancelled.status == 200
        assert _payload(cancelled)["state"] == "canceled"
        evidence_request = SimpleNamespace(
            headers={"Authorization": "Bearer synthetic-read-token"},
            body=json.dumps(
                {
                    "dataset_id": loaded.dataset_id,
                    "definition_revision_id": loaded.definition_revision_id,
                    "idempotency_key": "synthetic-stop-run",
                    "candidate_generation_id": None,
                }
            ).encode(),
            query_string="",
        )
        async with case.sessions() as session:
            observed = await evidence.serve_execution_evidence(evidence_request, session)
        assert observed.status == 200
        assert _payload(observed)["execution"]["state"] == "canceled"
        async with case.sessions() as session:
            finalized = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=True)
        assert finalized.status == 200
        assert _payload(finalized) == _payload(cancelled)


async def _claimed_execution(case, worker_token):
    """Register and claim one synthetic execution for stop-route checks."""

    database = _Database()
    database.session = case.sessions
    registered = json.loads(
        await operator_cli._register_snowflake_binding(stream=_registration_stream(), database=database)
    )
    pins_by_field = {
        "dataset_id": registered["dataset_id"],
        "definition_revision_id": registered["definition_revision_id"],
        "source_binding_revision_id": registered["source_binding_revision_id"],
        "idempotency_key": "synthetic-claimed-stop",
    }
    async with case.sessions() as session:
        async with session.begin():
            schema_id, identity = await stop._retained_identity(
                session,
                pins_by_field["dataset_id"],
                pins_by_field["definition_revision_id"],
                pins_by_field["source_binding_revision_id"],
            )
            submission = await lifecycle.reserve_execution(
                session,
                **pins_by_field,
                schema_revision_id=schema_id,
                mechanism="local",
                request_identity_sha256=identity,
            )
    async with case.sessions() as session:
        async with session.begin():
            grant = await lifecycle.claim_execution(session, execution_id=submission.execution_id, token=worker_token)
    assert grant is not None
    return pins_by_field, submission, grant


@pytest.mark.asyncio
async def test_claimed_stop_finalizes_a_persisted_engine_row(monkeypatch):
    """The private stop phases fence a claimed worker and retain finality."""

    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_EVIDENCE_READ_TOKEN", "synthetic-read-token")
    worker_token = "synthetic-worker"
    async with isolated_publication_case() as case:
        pins_by_field, submission, grant = await _claimed_execution(case, worker_token)

        async with case.sessions() as session:
            premature = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=True)
        assert premature.status == 503
        async with case.sessions() as session:
            requested = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=False)
        assert requested.status == 200
        assert _payload(requested) == {"execution_id": submission.execution_id, "state": "canceling"}
        async with case.sessions() as session:
            async with session.begin():
                stale = await lifecycle.finish_execution(
                    session,
                    execution_id=submission.execution_id,
                    fence=grant.fence,
                    token=worker_token,
                    terminal_state="completed",
                )
        assert stale.state == "canceling" and not stale.changed

        # The caller proves worker absence before invoking this second phase.
        async with case.sessions() as session:
            finalized = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=True)
        assert finalized.status == 200
        assert _payload(finalized) == {"execution_id": submission.execution_id, "state": "canceled"}
        async with case.sessions() as session:
            execution_row = await session.get(CustomImportExecution, submission.execution_id)
            assert execution_row is not None
            assert execution_row.state == "canceled" and execution_row.terminal_reason == "worker_stopped"
            assert execution_row.finished_at is not None

        evidence_request = SimpleNamespace(
            headers={"Authorization": "Bearer synthetic-read-token"},
            body=json.dumps(
                {
                    "dataset_id": pins_by_field["dataset_id"],
                    "definition_revision_id": pins_by_field["definition_revision_id"],
                    "idempotency_key": pins_by_field["idempotency_key"],
                    "candidate_generation_id": None,
                }
            ).encode(),
            query_string="",
        )
        async with case.sessions() as session:
            observed = await evidence.serve_execution_evidence(evidence_request, session)
        assert observed.status == 200
        assert _payload(observed)["execution"]["state"] == "canceled"
        async with case.sessions() as session:
            replay = await stop.serve_execution_stop(_request(document=pins_by_field), session, finalize=True)
        assert replay.status == 200 and _payload(replay) == _payload(finalized)
