# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One fixed admission handoff using an already authenticated launch permit.

The launch bootstrap must bind the exact SignedPermit bytes and independently
approved origin before constructing this transport. This module has no signing
key, file/configuration loader, SQL endpoint, automatic retry, or lease claim.
An uncertain response is reconciled under the original lease, not interpreted
as a rollback. Integrate outside the caller's local page transaction.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import ssl
from dataclasses import dataclass, field, replace
from typing import ClassVar

import httpx

from process.custom_import import admission_authorization as authorization
from process.custom_import.build_source import SourceBuildRequest, _page_session
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost

_MAX_RESPONSE_BYTES = 512
_POST_ADMISSION_PHASES = frozenset({"graph", "rejected", "output", "verifying", "verified"})
_RECEIPT_FIELDS = frozenset(
    {
        "execution_id",
        "build_id",
        "fence",
        "phase",
        "after_occurrence_id",
        "rows_processed",
        "candidate_error_count",
    }
)


class AdmissionTransportError(RuntimeError):
    """No committed admission outcome is established by the HTTP exchange."""


def _unavailable():
    return AdmissionTransportError("custom_import_admission_transport_unavailable")


def _utcnow():
    return dt.datetime.now(dt.UTC)


@dataclass(frozen=True, slots=True)
class AdmissionBatchReceipt:
    execution_id: int
    build_id: int
    fence: int
    phase: str
    after_occurrence_id: int
    rows_processed: int
    candidate_error_count: int


@dataclass(frozen=True, slots=True)
class RetainedAdmissionProgress:
    """A locked retained observation, never a replacement for a batch receipt."""

    execution_id: int
    build_id: int
    fence: int
    phase: str
    after_occurrence_id: int
    source_occurrence_count: int
    candidate_error_count: int


class AdmissionReconciliationRequired(AdmissionTransportError):
    """Keep the exact attempted cursor even if a later observation has advanced."""

    def __init__(self, pins, progress):
        super().__init__("custom_import_admission_reconciliation_required")
        self.pins = pins
        self.progress = progress


def _header(reply, name):
    values = reply.headers.get_list(name)
    if not values:
        return None
    if len(values) != 1 or not values[0].isascii() or not 0 < len(values[0]) <= 128:
        raise _unavailable()
    return values[0].lower()


async def _bounded_response_body(reply, *, maximum=_MAX_RESPONSE_BYTES):
    """Accept only a bounded, unencoded response with the required no-store headers."""

    if (
        reply.status_code != 200
        or _header(reply, "content-type") not in {"application/json", "application/json; charset=utf-8"}
        or _header(reply, "cache-control") != "no-store"
        or _header(reply, "content-encoding") not in {None, "identity"}
        or reply.headers.get_list("set-cookie")
    ):
        raise _unavailable()
    declared = _header(reply, "content-length")
    transfer = _header(reply, "transfer-encoding")
    if (
        transfer not in {None, "chunked"}
        or declared is not None
        and (
            transfer is not None
            or not declared.isdecimal()
            or len(declared) > len(str(maximum))
            or not 0 < int(declared) <= maximum
        )
    ):
        raise _unavailable()
    body = bytearray()
    async for chunk in reply.aiter_raw(chunk_size=maximum + 1):
        if len(body) + len(chunk) > maximum:
            raise _unavailable()
        body.extend(chunk)
    if not body or declared is not None and len(body) != int(declared):
        raise _unavailable()
    return body


def _receipt(body, pins):
    """Validate one committed receipt against the original execution, fence and cursor."""

    try:
        parsed = json.loads(
            body.decode("ascii"),
            object_pairs_hook=authorization._unique_object,
            parse_constant=authorization._reject_number,
            parse_float=authorization._reject_number,
            parse_int=authorization._integer,
        )
    except UnicodeError, ValueError, RecursionError:
        parsed = None
    if type(parsed) is not dict or parsed.keys() != _RECEIPT_FIELDS:
        raise _unavailable()
    for name in _RECEIPT_FIELDS - {"phase"}:
        minimum = 1 if name in {"execution_id", "build_id", "fence"} else 0
        maximum = 100_000 if name == "rows_processed" else authorization.MAX_BIGINT
        if type(parsed[name]) is not int or not minimum <= parsed[name] <= maximum:
            raise _unavailable()
    if (
        any(parsed[name] != getattr(pins, name) for name in ("execution_id", "build_id", "fence"))
        or type(parsed["phase"]) is not str
        or parsed["phase"] not in {"admission", "graph", "rejected"}
        or parsed["after_occurrence_id"] < pins.expected_after_occurrence_id
        or parsed["rows_processed"] > 0
        and parsed["after_occurrence_id"] <= pins.expected_after_occurrence_id
        or parsed["rows_processed"] == 0
        and (
            parsed["phase"] not in {"graph", "rejected"}
            or parsed["after_occurrence_id"] != pins.expected_after_occurrence_id
        )
    ):
        raise _unavailable()
    return AdmissionBatchReceipt(**parsed)


@dataclass(frozen=True, slots=True, kw_only=True)
class AdmissionBatchTransport:
    """Run-scoped client; inputs come only from the verified launch bootstrap."""

    expected_origin: str
    signed_permit: authorization.SignedPermit = field(repr=False)
    launch_expires_at: dt.datetime
    tls_context: ssl.SSLContext = field(repr=False, compare=False)
    transport: httpx.MockTransport | None = field(default=None, repr=False, compare=False)
    _permit: authorization.AdmissionPermit = field(init=False, repr=False)
    _client: httpx.AsyncClient = field(init=False, repr=False, compare=False)
    _source: ClassVar[bool] = False

    def __post_init__(self):
        if (
            type(self.tls_context) is not ssl.SSLContext
            or self.tls_context.verify_mode != ssl.CERT_REQUIRED
            or not self.tls_context.check_hostname
        ):
            raise _unavailable()
        if type(self.signed_permit) is not authorization.SignedPermit or (
            self.transport is not None and type(self.transport) is not httpx.MockTransport
        ):
            raise _unavailable()
        try:
            permit = authorization._permit(
                authorization._base64url_decode(self.signed_permit.context, 2_048),
                expected_origin=self.expected_origin,
                trusted_now=_utcnow(),
                is_source=self._source,
            )
            signature = authorization._base64url_decode(self.signed_permit.signature, 32)
            is_valid = (
                type(self.signed_permit.key_id) is str
                and authorization._KEY_ID.fullmatch(self.signed_permit.key_id) is not None
                and len(signature) == 32
                and permit.expires_at <= authorization._trusted_utc(self.launch_expires_at)
            )
        except authorization.AdmissionAuthorizationError:
            is_valid = False
        if not is_valid:
            raise _unavailable()
        object.__setattr__(self, "_permit", permit)
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

    def require_candidate_binding(self, request):
        """Check every launch pin before any candidate claim or source access."""

        if (
            any(
                getattr(request, name, None) != getattr(self._permit, name)
                for name in (
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "source_binding_revision_id",
                    "idempotency_key",
                )
            )
            or type(getattr(request, "source_binding_sha256", None)) is not bytes
            or request.source_binding_sha256.hex() != self._permit.source_binding_sha256
            or getattr(getattr(request, "bundle_request", None), "processing_policy", None) is None
            or not self._permit.issued_at <= _utcnow() < self._permit.expires_at
        ):
            raise _unavailable()

    def bind_request(self, request):
        """Add request-local expiry without changing any retained build bound."""

        if not isinstance(request, SourceBuildRequest) or (
            any(
                getattr(request, name) != getattr(self._permit, name)
                for name in (
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                )
            )
            or request.authorization_expires_at not in {None, self._permit.expires_at}
        ):
            raise _unavailable()
        return replace(request, authorization_expires_at=self._permit.expires_at)

    async def send(self, request, pins):
        """Attempt once after local transaction close; never select a new cursor."""

        if (
            type(pins) is not authorization.BatchPins
            or pins.execution_id != request.execution_id
            or pins.fence != request.fence
            or self.bind_request(request).authorization_expires_at != request.authorization_expires_at
        ):
            raise _unavailable()
        for name in authorization._BODY_FIELDS:
            authorization._positive_id(getattr(pins, name), minimum=0 if name == "expected_after_occurrence_id" else 1)
        body = json.dumps(
            {name: getattr(pins, name) for name in authorization._BODY_FIELDS},
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("ascii")
        return _receipt(await self._exchange(request, body), pins)

    async def _exchange(self, request, body):
        """One bounded POST for the hardcoded admission or SOURCE subclass."""

        now = _utcnow()
        budget = min(
            (self._permit.expires_at - now).total_seconds(),
            (request.build_deadline_at - now).total_seconds(),
            request.lease_seconds,
        )
        if now < self._permit.issued_at or budget <= 0:
            raise LeaseAuthorityLost("admission authorization expired before handoff")
        if not 1 <= len(body) <= 512:
            raise _unavailable()
        context_header = authorization.SOURCE_CONTEXT_HEADER if self._source else authorization.CONTEXT_HEADER
        key_header = authorization.SOURCE_KEY_ID_HEADER if self._source else authorization.KEY_ID_HEADER
        signature_header = authorization.SOURCE_SIGNATURE_HEADER if self._source else authorization.SIGNATURE_HEADER
        headers_by_name = {
            "Authorization": authorization.encode_lease_bearer(request.lease_token),
            "Content-Type": "application/json",
            context_header: self.signed_permit.context,
            key_header: self.signed_permit.key_id,
            signature_header: self.signed_permit.signature,
        }
        outbound = self._client.build_request(
            "POST",
            self.expected_origin + (authorization.SOURCE_PATH if self._source else authorization.ADMISSION_PATH),
            content=body,
            headers=headers_by_name,
            timeout=httpx.Timeout(budget, connect=min(5, budget)),
        )
        # Never merge any response cookie into this fixed bearer-only request.
        outbound.headers.pop("cookie", None)
        try:
            async with asyncio.timeout(budget):
                reply = await self._client.send(outbound, stream=True)
                try:
                    return await _bounded_response_body(reply, maximum=1_024 if self._source else 512)
                finally:
                    await reply.aclose()
        except httpx.HTTPError, OSError, TimeoutError, AdmissionTransportError:
            failure = _unavailable()
        finally:
            # HTTPX receives cookies before response validation; never forward them.
            self._client.cookies.clear()
        raise failure


async def _retained_progress(session_factory, request, build_id):
    async with _page_session(session_factory, request, build_id) as (_session, build):
        if build.phase not in _POST_ADMISSION_PHASES | {"admission"}:
            raise _unavailable()
        return RetainedAdmissionProgress(
            request.execution_id,
            build_id,
            request.fence,
            build.phase,
            build.admission_after_occurrence_id,
            build.source_occurrence_count,
            build.candidate_error_count,
        )


async def admit_next_batch(session_factory, request, build_id, transport):
    """Release all local locks before HTTP; reconcile uncertain outcomes once.

    The caller must not already hold a page transaction. A reconciliation error
    carries the original attempted pins and a fresh retained observation, or
    None when unavailable. Even an unchanged cursor does not prove rollback.
    No exception here grants a new lease or authorizes a blind HTTP replay.
    """

    request = transport.bind_request(request)
    progress = await _retained_progress(session_factory, request, build_id)
    if progress.phase in _POST_ADMISSION_PHASES:
        return progress
    pins = authorization.BatchPins(build_id, request.execution_id, progress.after_occurrence_id, request.fence)
    try:
        return await transport.send(request, pins)
    except AdmissionTransportError:
        retained = None
    try:
        retained = await _retained_progress(session_factory, request, build_id)
        if retained.after_occurrence_id < pins.expected_after_occurrence_id:
            retained = None
    except CancellationRequested, LeaseAuthorityLost:
        raise
    except Exception:
        retained = None
    raise AdmissionReconciliationRequired(pins, retained)
