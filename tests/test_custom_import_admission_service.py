# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable admission authority, reconstruction, and commit-boundary checks."""

import asyncio
import base64
import json
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.compat import Header

import api.control_admission_batch as service
import process.custom_import.admission_authorization as auth
import process.custom_import.admission_sql as admission
import process.custom_import.build_source as staging
from process.custom_import.snowflake_source_binding import _loaded_snowflake_source_binding
from tests.admission_transport_test_support import AuthorizedRequest
from tests.test_custom_import_build_source import _request as existing_request
from tests.test_custom_import_snowflake_source_binding import _definition, _loaded_rows, _v2_binding

NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
ORIGIN = "https://engine.example.invalid"
TOKEN = b"opaque-DO-NOT-LOG\x00\xff"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


@pytest.fixture(autouse=True)
def _request_boundary(monkeypatch):
    monkeypatch.setattr(service, "SourceBuildRequest", AuthorizedRequest)


def _wire(*, digest="a" * 64, cursor=0):
    ring = auth.AdmissionKeyring("synthetic", (("synthetic", b"s" * 32),))
    permit_by_name = {
        "audience": "custom-import-engine",
        "contract": auth.PERMIT_CONTRACT,
        "dataset_id": 11,
        "definition_revision_id": 12,
        "schema_revision_id": 13,
        "source_binding_revision_id": 14,
        "source_binding_sha256": digest,
        "idempotency_key": "synthetic-run",
        "issued_at": "2030-01-01T00:00:00Z",
        "expires_at": "2030-01-01T00:15:00Z",
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "path": auth.ADMISSION_PATH,
        "origin": ORIGIN,
    }
    signed = auth.sign_permit(
        canonical(permit_by_name),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(hours=1),
        keyring=ring,
    )
    headers = Header(
        [
            ("Content-Type", "application/json"),
            ("Authorization", auth.encode_lease_bearer(TOKEN)),
            (auth.CONTEXT_HEADER, signed.context),
            (auth.KEY_ID_HEADER, signed.key_id),
            (auth.SIGNATURE_HEADER, signed.signature),
        ]
    )
    body = canonical({"build_id": 51, "execution_id": 40, "fence": 7, "expected_after_occurrence_id": cursor})
    request = SimpleNamespace(headers=headers, body=body, method="POST", path=auth.ADMISSION_PATH, query_string="")
    verified = auth.verify_request(
        headers=[(str(name), header_value) for name, header_value in headers.items()],
        body=body,
        method="POST",
        path=auth.ADMISSION_PATH,
        query_string="",
        trusted_now=NOW,
        expected_origin=ORIGIN,
        keyring=ring,
    )
    return request, verified, ring


def _request(**changes):
    original = existing_request()
    values_by_name = {item.name: getattr(original, item.name) for item in fields(original)}
    values_by_name.update(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        execution_id=40,
        fence=7,
        lease_token=TOKEN,
        build_deadline_at=NOW + timedelta(hours=1),
    )
    return AuthorizedRequest(**{**values_by_name, **changes})


class Result:
    def __init__(self, value):
        self.value = value

    def one(self):
        return self.value

    def one_or_none(self):
        return self.value

    def scalar_one(self):
        return self.value

    def scalar_one_or_none(self):
        return self.value


class Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.session.events.append("begin")
        self.session.active = True

    async def __aexit__(self, kind, error, traceback):
        self.session.events.append("rollback" if error else "commit")
        if error is None:
            self.session.durable.extend(self.session.pending)
        self.session.pending.clear()
        self.session.active = False
        if error is None and self.session.commit_error is not None:
            raise self.session.commit_error


class Session:
    def __init__(self, values=(), *, commit_error=None):
        self.values = list(values)
        self.info, self.events, self.statements, self.pending, self.durable = {}, [], [], [], []
        self.active, self.commit_error = False, commit_error

    async def __aenter__(self):
        self.events.append("open")
        return self

    async def __aexit__(self, *args):
        self.events.append("close")

    def begin(self):
        return Transaction(self)

    async def execute(self, statement, parameters=None):
        self.statements.append(statement)
        return Result(self.values.pop(0) if self.values else None)


def _retained(monkeypatch):
    definition = _definition()
    binding = _v2_binding(definition)
    rows = _loaded_rows(definition, binding)
    rows[0].binding_contract = binding.contract
    loaded = _loaded_snowflake_source_binding(*rows)
    wire, verified, ring = _wire(digest=loaded.source_binding_sha256.hex())
    policy = binding.processing_policy.build
    build = SimpleNamespace(
        base_generation_id=None,
        base_pointer_version=0,
        complete_scope=True,
        page_row_limit=policy.page_row_limit,
        page_byte_limit=policy.page_byte_limit,
        statement_timeout_ms=policy.statement_timeout_ms,
        build_deadline_at=NOW + timedelta(hours=1),
    )
    execution = SimpleNamespace(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        idempotency_key="synthetic-run",
        source_binding_revision_id=14,
    )
    session = Session([None, None, (build, execution)])
    monkeypatch.setattr(service, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    return wire, verified, ring, session, build, execution, loaded


@pytest.mark.asyncio
async def test_retained_reconstruction_is_read_only_and_closes_before_admission(monkeypatch):
    wire, verified, ring, session, build, _, loaded = _retained(monkeypatch)

    async def admit(factory, request, build_id, cursor, *, admission_permit):
        assert session.events[-1] == "close" and not session.active
        assert request.definition == loaded.definition and request.lease_token == TOKEN
        assert request.build_deadline_at == build.build_deadline_at
        assert request.authorization_expires_at == verified.permit.expires_at
        assert request.lease_seconds == loaded.binding.processing_policy.build.lease_seconds
        assert (build_id, cursor, admission_permit) == (51, 0, verified.permit)
        return admission.AdmissionResult("graph", 80, 80, 0)

    monkeypatch.setattr(admission, "admit_source_batch", admit)
    reply = await service.serve_admission_batch(
        wire,
        lambda: session,
        keyring=ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )
    assert reply.status == 200 and reply.headers["Cache-Control"] == "no-store"
    assert json.loads(reply.body) == {
        "execution_id": 40,
        "build_id": 51,
        "fence": 7,
        "phase": "graph",
        "after_occurrence_id": 80,
        "rows_processed": 80,
        "candidate_error_count": 0,
    }
    assert str(session.statements[0]) == "SET TRANSACTION READ ONLY"
    assert not session.pending and not session.durable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("idempotency_key", "other-run"),
        ("source_binding_revision_id", 99),
    ],
)
async def test_retained_execution_cannot_redirect_permit(monkeypatch, field, value):
    wire, _, ring, session, _, execution, _ = _retained(monkeypatch)
    setattr(execution, field, value)
    decision = AsyncMock()
    monkeypatch.setattr(admission, "admit_source_batch", decision)
    reply = await service.serve_admission_batch(
        wire,
        lambda: session,
        keyring=ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )
    assert reply.status == 503
    decision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("name,value", [(auth.CONTEXT_HEADER, "invalid"), ("Authorization", "Bearer invalid!")])
async def test_header_duplicates_fail_before_database(monkeypatch, name, value):
    wire, _, ring = _wire()
    wire.headers.add(name.lower(), value)
    loader = AsyncMock()
    monkeypatch.setattr(service, "_retained_request", loader)
    reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
    assert reply.status == 403 and reply.headers["Cache-Control"] == "no-store"
    loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_purpose_header_and_collapsed_mapping_fail_before_database(monkeypatch):
    for headers in (Header([("X-Custom-Import-Admission-Extra", "no")]), {"Authorization": "Bearer no"}):
        wire, _, ring = _wire()
        if isinstance(headers, Header):
            wire.headers.extend(headers)
        else:
            wire.headers = headers
        reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
        assert reply.status == 403


@pytest.mark.asyncio
async def test_lost_ack_retry_returns_original_cursor_conflict(monkeypatch):
    wire, verified, ring = _wire(cursor=7)
    monkeypatch.setattr(
        service,
        "_retained_request",
        AsyncMock(return_value=_request(authorization_expires_at=verified.permit.expires_at)),
    )
    operation = AsyncMock(side_effect=admission.AdmissionError("custom_import_build_progress_conflict"))
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
    assert reply.status == 409 and json.loads(reply.body) == {"error": "admission_progress_conflict"}
    assert operation.await_args.args[3] == 7
    operation.assert_awaited_once()


def _authority_document():
    return canonical(
        {
            "contract": auth.KEYRING_CONTRACT,
            "active_key_id": "synthetic",
            "keys": [
                {"key_id": "synthetic", "key_base64url": base64.urlsafe_b64encode(b"s" * 32).rstrip(b"=").decode()}
            ],
        }
    )


@pytest.mark.asyncio
async def test_bootstrap_is_disabled_without_dedicated_configuration(monkeypatch):
    monkeypatch.delenv(service.KEYRING_FILE_ENV, raising=False)
    monkeypatch.delenv(service.ORIGIN_ENV, raising=False)
    app = SimpleNamespace(ctx=SimpleNamespace())
    await service.initialize_admission_authority(app, None)
    assert app.ctx.custom_import_admission_authority is None
    wire = SimpleNamespace(app=app)
    reply = await service.admission_batch(wire)
    assert reply.status == 503 and reply.headers["Cache-Control"] == "no-store"


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_origin", "missing_file", "wrong_origin", "wrong_purpose", "oversized"])
async def test_partial_or_invalid_bootstrap_fails_closed_without_reflection(monkeypatch, tmp_path, fault):
    keyring_path = tmp_path / "synthetic-keyring.json"
    document = _authority_document()
    if fault == "wrong_purpose":
        document = document.replace(auth.KEYRING_CONTRACT.encode(), b"unrelated-keyring/v1")
    if fault == "oversized":
        document += b" " * 4097
    keyring_path.write_bytes(document)
    monkeypatch.setenv(service.KEYRING_FILE_ENV, str(keyring_path))
    monkeypatch.setenv(service.ORIGIN_ENV, ORIGIN)
    if fault == "missing_origin":
        monkeypatch.delenv(service.ORIGIN_ENV)
    if fault == "missing_file":
        monkeypatch.delenv(service.KEYRING_FILE_ENV)
    if fault == "wrong_origin":
        monkeypatch.setenv(service.ORIGIN_ENV, "http://DO-NOT-LOG.invalid")
    app = SimpleNamespace(ctx=SimpleNamespace())
    with pytest.raises(service.AdmissionUnavailable) as failure:
        await service.initialize_admission_authority(app, None)
    assert failure.value.args == ("custom_import_admission_unavailable",)
    assert failure.value.__suppress_context__ and app.ctx.custom_import_admission_authority is None


@pytest.mark.asyncio
async def test_bootstrap_pins_keyring_and_origin_before_requests(monkeypatch, tmp_path):
    keyring_path = tmp_path / "synthetic-keyring.json"
    keyring_path.write_bytes(_authority_document())
    monkeypatch.setenv(service.KEYRING_FILE_ENV, str(keyring_path))
    monkeypatch.setenv(service.ORIGIN_ENV, ORIGIN)
    app = SimpleNamespace(ctx=SimpleNamespace())
    await service.initialize_admission_authority(app, None)
    ring, origin = app.ctx.custom_import_admission_authority
    assert ring == _wire()[2] and origin == ORIGIN
    keyring_path.unlink()
    monkeypatch.setenv(service.ORIGIN_ENV, "https://other.example.invalid")
    assert app.ctx.custom_import_admission_authority == (ring, ORIGIN)


class BodyStream:
    def __init__(self, *chunks):
        self.chunks = chunks
        self.request_max_size = None

    async def __aiter__(self):
        assert self.request_max_size == service.MAX_BODY_BYTES
        for chunk in self.chunks:
            yield chunk


def _streaming_request(*chunks):
    wire, _, ring = _wire()
    wire.app = SimpleNamespace(ctx=SimpleNamespace(custom_import_admission_authority=(ring, ORIGIN)))
    wire.stream = BodyStream(*chunks)
    wire.body = b""
    return wire


@pytest.mark.asyncio
@pytest.mark.parametrize("chunks", [(b"x" * 513,), (b"x" * 256, b"x" * 257)])
async def test_wrapper_rejects_oversized_stream_before_service(monkeypatch, chunks):
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_admission_batch", operation)
    request = _streaming_request(*chunks)
    reply = await service.admission_batch(request)
    assert reply.status == 413 and reply.headers["Cache-Control"] == "no-store"
    assert request.body == b""
    operation.assert_not_awaited()


@pytest.mark.asyncio
async def test_wrapper_keeps_fixed_origin_and_existing_pool_at_body_boundary(monkeypatch):
    operation = AsyncMock(return_value=service._reply({"synthetic": True}))
    monkeypatch.setattr(service, "serve_admission_batch", operation)
    session_factory = object()
    monkeypatch.setattr(service, "db", SimpleNamespace(session_factory=session_factory))
    request = _streaming_request(b"x" * 256, b"x" * 256)
    request.headers.extend([("Host", "forged.invalid"), ("X-Forwarded-Host", "forged.invalid")])
    assert (await service.admission_batch(request)).status == 200
    assert request.body == b"x" * 512
    assert operation.await_args.args == (request, session_factory)
    assert operation.await_args.kwargs["expected_origin"] == ORIGIN
    assert operation.await_args.kwargs["trusted_now"].tzinfo == timezone.utc


@pytest.mark.asyncio
async def test_wrapper_rejects_duplicate_authority_headers_before_read(monkeypatch):
    operation = AsyncMock()
    monkeypatch.setattr(service, "serve_admission_batch", operation)
    request = _streaming_request(b"not-read")
    request.headers.extend([("Authorization", "Bearer synthetic-duplicate")])
    reply = await service.admission_batch(request)
    assert reply.status == 403 and reply.headers["Cache-Control"] == "no-store"
    assert request.stream.request_max_size is None and request.body == b""
    operation.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        staging.LeaseAuthorityLost("DO-NOT-LOG"),
        service.CancellationRequested("DO-NOT-LOG"),
    ],
)
async def test_authority_loss_is_metadata_only_and_never_terminalizes_execution(monkeypatch, error):
    wire, verified, ring = _wire()
    monkeypatch.setattr(
        service,
        "_retained_request",
        AsyncMock(return_value=_request(authorization_expires_at=verified.permit.expires_at)),
    )
    operation = AsyncMock(side_effect=error)
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
    assert reply.status == 409 and json.loads(reply.body) == {"error": "admission_authority_lost"}
    assert b"DO-NOT-LOG" not in reply.body
    operation.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancelled_request_propagates_without_synthesizing_a_receipt(monkeypatch):
    wire, _, ring = _wire()
    monkeypatch.setattr(service, "_retained_request", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)


@pytest.mark.asyncio
async def test_missing_deadline_hook_fails_closed_before_opening_a_session(monkeypatch):
    _, verified, _ = _wire()
    monkeypatch.setattr(service, "SourceBuildRequest", SimpleNamespace(__dataclass_fields__={}))
    with pytest.raises(service.AdmissionUnavailable):
        await service._retained_request(None, verified)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["digest", "policy", "bounds"])
async def test_missing_or_mismatched_retained_policy_and_digest_never_admit(monkeypatch, change):
    wire, _, ring, session, build, _, loaded = _retained(monkeypatch)
    if change == "digest":
        loaded = replace(loaded, source_binding_sha256=b"x" * 32)
    elif change == "policy":
        loaded = replace(loaded, binding=replace(loaded.binding, processing_policy=None))
    else:
        build.page_byte_limit += 1
    monkeypatch.setattr(service, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    operation = AsyncMock()
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    reply = await service.serve_admission_batch(
        wire,
        lambda: session,
        keyring=ring,
        expected_origin=ORIGIN,
        trusted_now=NOW,
    )
    assert reply.status == 503
    operation.assert_not_awaited()


@pytest.mark.parametrize("name", ["X-Custom-Import-Source-Key-Id", "X-Custom-Import-Source-Context"])
async def test_cross_purpose_headers_deny_before_admission_query(monkeypatch, name):
    wire, _, ring = _wire()
    wire.headers.add(name, "not-authority")
    operation = AsyncMock()
    monkeypatch.setattr(service, "_retained_request", operation)
    reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
    assert reply.status == 403
    operation.assert_not_awaited()


@pytest.mark.parametrize(
    "phase,cursor,count",
    [
        ("admission", 0, 0),
        ("graph", 1, 0),
        ("rejected", 1, 0),
        ("admission", 0, 1),
    ],
)
def test_service_never_emits_a_noop_or_inconsistent_zero_row_ack(phase, cursor, count):
    _, verified, _ = _wire()
    with pytest.raises(service.AdmissionUnavailable):
        service._receipt(verified, admission.AdmissionResult(phase, cursor, count, 0))


async def test_admission_unknown_commit_is_metadata_only_no_retry(monkeypatch, caplog):
    wire, verified, ring = _wire()
    monkeypatch.setattr(
        service,
        "_retained_request",
        AsyncMock(
            return_value=_request(
                authorization_expires_at=verified.permit.expires_at,
            )
        ),
    )
    operation = AsyncMock(side_effect=RuntimeError("uncertain COMMIT do-not-log"))
    monkeypatch.setattr(admission, "admit_source_batch", operation)
    reply = await service.serve_admission_batch(wire, None, keyring=ring, expected_origin=ORIGIN, trusted_now=NOW)
    assert reply.status == 503 and json.loads(reply.body) == {"error": "admission_unavailable"}
    assert reply.headers["Cache-Control"] == "no-store" and not caplog.records
    operation.assert_awaited_once()
