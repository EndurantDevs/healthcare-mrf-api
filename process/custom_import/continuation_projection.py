# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One nonce-authenticated owner projection step for a retained processing epoch."""

from __future__ import annotations

import asyncio
import datetime as dt
import hmac
import json
import os
import re
import ssl
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path

import httpx
from sanic.exceptions import BadRequest, PayloadTooLarge
from sqlalchemy import text

from db.models.custom_import import CustomImportBuildAttempt, CustomImportExecution, CustomImportLease
from db.models.custom_import_continuation import CustomImportBuildContinuation
from process.custom_import import admission_authorization as wire
from process.custom_import import admission_worker as worker
from process.custom_import import query_projection_worker as projection_worker
from process.custom_import.build_continuation import _request
from process.custom_import.continuation_runtime import execution_continuation_matches, verify_continuation_scope
from process.custom_import.execution import MAX_LEASE_SECONDS, IdempotencyConflict, lease_token_sha256
from process.custom_import.query_projection_build import ProjectionCursorConflict, projection_step
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.runner_registry import database_now
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from process.custom_import.source_batch import _read_session

PROJECTION_PATH = "/control/v1/custom-import/build-continuation-projection"
ORIGIN_ENV = "HLTHPRT_CUSTOM_IMPORT_CONTINUATION_PROJECTION_ORIGIN"
CA_PATH = Path("/var/run/custom-import-processing/writer-ca.pem")
MAX_BODY_BYTES = 2048
MAX_RESPONSE_SECONDS = 300
_FIELDS = frozenset({"authorization_id", "continuation_id", "build_id", "expected_progress"})


class ContinuationProjectionForbidden(ValueError):
    """No current retained processing authority was established."""


def _forbidden():
    return ContinuationProjectionForbidden("custom_import_continuation_projection_forbidden")


def _nonce(headers):
    """Preserve duplicate headers and reject every other writer purpose."""

    if not callable(getattr(headers, "getall", None)):
        raise _forbidden()
    if any(name.lower().startswith("x-custom-import-") or name.lower() == "cookie" for name, _ in headers.items()):
        raise _forbidden()
    authorization, content_type = headers.getall("Authorization", []), headers.getall("Content-Type", [])
    if len(authorization) != 1 or content_type != ["application/json"]:
        raise _forbidden()
    value = authorization[0]
    if type(value) is not str or len(value) != 50 or not value.startswith("Bearer "):
        raise _forbidden()
    nonce = wire._base64url_decode(value[7:], 32)
    if len(nonce) != 32:
        raise _forbidden()
    return nonce


def _document(raw):
    document = wire._object(raw, MAX_BODY_BYTES)
    if document.keys() != _FIELDS or type(document["authorization_id"]) is not str:
        raise _forbidden()
    if re.fullmatch(r"[0-9a-f]{64}", document["authorization_id"], re.ASCII) is None:
        raise _forbidden()
    for name in ("continuation_id", "build_id"):
        wire._positive_id(document[name])
    projection_worker._progress(document["expected_progress"])
    return document


async def receive_continuation_projection_body(request):
    """Bound stream accumulation before any database access or lease renewal."""

    _nonce(request.headers)
    request.stream.request_max_size = MAX_BODY_BYTES
    chunks, byte_count = [], 0
    async for chunk in request.stream:
        byte_count += len(chunk)
        if byte_count > MAX_BODY_BYTES:
            raise PayloadTooLarge("continuation projection body exceeds limit")
        chunks.append(chunk)
    request.body = b"".join(chunks)


def _build_matches(authorization, build):
    return build is not None and all(
        getattr(authorization, authority_name) == getattr(build, build_name)
        for authority_name, build_name in (
            *(
                (name, name)
                for name in (
                    "build_id",
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "execution_id",
                    "capture_bundle_id",
                    "request_identity_sha256",
                    "base_generation_id",
                    "base_pointer_version",
                )
            ),
            ("producer_fence", "producing_fence"),
            ("producer_token_sha256", "producing_token_sha256"),
            ("original_deadline_at", "build_deadline_at"),
        )
    )


def _renewal_seconds(lease, now):
    """Preserve the last granted interval, not its diminishing remaining lifetime."""

    if lease.heartbeat_at is None or lease.heartbeat_at > now:
        raise _forbidden()
    seconds = int((lease.expires_at - lease.heartbeat_at).total_seconds())
    if seconds < 1:
        raise _forbidden()
    return min(MAX_LEASE_SECONDS, seconds)


async def _retained_request(session_factory, document, nonce):
    """Read immutable policy and authenticate before opening a write transaction."""

    async with asyncio.timeout(5), _read_session(session_factory) as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout = '2000ms'"))
        authority = await session.get(CustomImportBuildContinuation, document["continuation_id"])
        if (
            authority is None
            or authority.authorization_id != document["authorization_id"]
            or authority.build_id != document["build_id"]
        ):
            raise _forbidden()
        execution = await session.get(CustomImportExecution, authority.execution_id)
        lease = await session.get(CustomImportLease, authority.execution_id)
        build = await session.get(CustomImportBuildAttempt, authority.build_id)
        now = await database_now(session)
        if (
            execution is None
            or lease is None
            or execution.state != "running"
            or not _build_matches(authority, build)
            or build.phase != "output"
            or build.generation_id is None
            or not execution_continuation_matches(
                authority, execution, lease, document["continuation_id"], lease_token_sha256(nonce), now
            )
        ):
            raise _forbidden()
        retained = await _request(session, authority, build, nonce, _renewal_seconds(lease, now))
        verify_continuation_scope(retained, execution, lease, authority, now, build=build)
        return retained, build.generation_id, build.capture_bundle_id, lease.expires_at


async def serve_continuation_projection(request, session_factory):
    """Execute one existing bounded step; never start an epoch or replay SOURCE."""

    from api.control_admission_batch import _reply, extend_batch_response_deadline

    try:
        if request.method != "POST" or request.path != PROJECTION_PATH or request.query_string:
            raise _forbidden()
        nonce = _nonce(request.headers)
        if getattr(request, "stream", None) is not None:
            await receive_continuation_projection_body(request)
        document = _document(request.body)
        expected = projection_worker._progress(document["expected_progress"])
    except PayloadTooLarge:
        return _reply({"error": "projection_body_too_large"}, 413)
    except ValueError, TypeError, UnicodeError, BadRequest, worker.AdmissionTransportError:
        return _reply({"error": "projection_forbidden"}, 403)
    except Exception:
        return _reply({"error": "projection_unavailable"}, 503)
    try:
        retained, generation_id, capture_bundle_id, expires_at = await _retained_request(
            session_factory, document, nonce
        )
        now = dt.datetime.now(dt.UTC)
        operation_deadline = extend_batch_response_deadline(
            request, expires_at=min(expires_at, now + dt.timedelta(seconds=MAX_RESPONSE_SECONDS)), trusted_now=now
        )
        if operation_deadline is not None:
            operation_deadline = min(operation_deadline, asyncio.get_running_loop().time() + MAX_RESPONSE_SECONDS)
            retained = replace(retained, operation_deadline=operation_deadline)
        operation_timeout = (
            asyncio.timeout(MAX_RESPONSE_SECONDS)
            if operation_deadline is None
            else asyncio.timeout_at(operation_deadline)
        )
        async with operation_timeout:
            receipt = await projection_step(session_factory, retained, document["build_id"], expected=expected)
        pins = projection_worker._pins(document["build_id"], retained.execution_id, retained.fence, expected)
        receipt_document = asdict(receipt)
        projection_worker.validate_receipt(
            receipt_document, pins, generation_id=generation_id, capture_bundle_id=capture_bundle_id, before=expected
        )
        return _reply(receipt_document)
    except ContinuationProjectionForbidden:
        return _reply({"error": "projection_forbidden"}, 403)
    except ProjectionCursorConflict:
        return _reply({"error": "projection_progress_conflict"}, 409)
    except IdempotencyConflict, LeaseAuthorityLost, CancellationRequested:
        return _reply({"error": "projection_authority_lost"}, 409)
    except Exception:
        return _reply({"error": "projection_unavailable"}, 503)


@dataclass(frozen=True, slots=True, kw_only=True)
class ContinuationProjectionTransport:
    """Fixed TLS destination with current nonce authority, not a generic API key."""

    expected_origin: str
    authorization_id: str
    continuation_id: int
    build_id: int
    nonce: bytes = field(repr=False)
    tls_context: ssl.SSLContext = field(repr=False, compare=False)
    transport: httpx.MockTransport | None = field(default=None, repr=False, compare=False)
    _client: httpx.AsyncClient = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        wire._origin(self.expected_origin)
        if (
            type(self.nonce) is not bytes
            or len(self.nonce) != 32
            or type(self.tls_context) is not ssl.SSLContext
            or self.tls_context.verify_mode != ssl.CERT_REQUIRED
            or not self.tls_context.check_hostname
            or (self.transport is not None and type(self.transport) is not httpx.MockTransport)
            or type(self.authorization_id) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.authorization_id, re.ASCII) is None
        ):
            raise worker._unavailable()
        wire._positive_id(self.continuation_id)
        wire._positive_id(self.build_id)
        object.__setattr__(
            self,
            "_client",
            httpx.AsyncClient(
                verify=self.tls_context,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
                headers={"Accept": "application/json", "Accept-Encoding": "identity", "Cache-Control": "no-store"},
            ),
        )

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *arguments):
        return await self._client.__aexit__(*arguments)

    def bind_request(self, request):
        """Keep the current epoch without imposing its historical launch expiry."""
        context = getattr(request, "continuation", None)
        if (
            context is None
            or context.continuation_id != self.continuation_id
            or context.build_id != self.build_id
            or type(request.lease_token) is not bytes
            or not hmac.compare_digest(request.lease_token, self.nonce)
        ):
            raise worker._unavailable()
        return request

    async def send(self, request, pins, *, generation_id, capture_bundle_id, before):
        """Send once; the existing caller owns one read-only lost-ACK observation."""

        self.bind_request(request)
        if pins != projection_worker._pins(self.build_id, request.execution_id, request.fence, before):
            raise worker._unavailable()
        document_by_field = dict(
            authorization_id=self.authorization_id,
            continuation_id=self.continuation_id,
            build_id=self.build_id,
            expected_progress=asdict(before),
        )
        body = json.dumps(
            document_by_field, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")
        ).encode("ascii")
        _document(body)
        return projection_worker._receipt(
            await self._exchange(request, body),
            pins,
            generation_id=generation_id,
            capture_bundle_id=capture_bundle_id,
            before=before,
        )

    async def _exchange(self, request, body):
        budget = min(MAX_RESPONSE_SECONDS, request.lease_seconds)
        outbound = self._client.build_request(
            "POST",
            self.expected_origin + PROJECTION_PATH,
            content=body,
            headers={"Authorization": wire.encode_lease_bearer(self.nonce), "Content-Type": "application/json"},
            timeout=httpx.Timeout(budget, connect=min(5, budget)),
        )
        outbound.headers.pop("cookie", None)
        try:
            async with asyncio.timeout(budget):
                reply = await self._client.send(outbound, stream=True)
                try:
                    return await worker._bounded_response_body(reply, maximum=1024)
                finally:
                    await reply.aclose()
        except httpx.HTTPError, OSError, TimeoutError, worker.AdmissionTransportError:
            failure = worker._unavailable()
        finally:
            self._client.cookies.clear()
        raise failure


@asynccontextmanager
async def continuation_projection_transport(authorization, request):
    """Read only the signed launch's fixed origin environment and CA mount."""

    if not configured_fields(request.definition):
        yield None
        return
    origin = wire._origin(os.environ.get(ORIGIN_ENV))
    context = ssl.create_default_context(cafile=str(CA_PATH))
    async with ContinuationProjectionTransport(
        expected_origin=origin,
        authorization_id=authorization["authorization_id"],
        continuation_id=request.continuation.continuation_id,
        build_id=request.continuation.build_id,
        nonce=request.lease_token,
        tls_context=context,
    ) as transport:
        yield transport
