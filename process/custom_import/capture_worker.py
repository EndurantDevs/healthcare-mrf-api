# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One-shot authenticated capture handoff; no signing key or local-owner fallback."""

from __future__ import annotations

import asyncio
import ssl
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from process.custom_import import admission_authorization as common
from process.custom_import import capture_authorization as authorization
from process.custom_import import capture_wire
from process.custom_import.admission_worker import AdmissionTransportError, _bounded_response_body, _header


class CaptureTransportError(RuntimeError):
    """The exchange does not establish a committed capture outcome."""


def _unavailable():
    return CaptureTransportError("custom_import_capture_transport_unavailable")


def _utcnow():
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True, kw_only=True)
class CaptureHandoffTransport:
    """Construct only from an authenticated launch bootstrap and approved TLS origin."""

    expected_origin: str
    signed_permit: common.SignedPermit = field(repr=False)
    launch_expires_at: datetime
    tls_context: ssl.SSLContext = field(repr=False, compare=False)
    transport: httpx.MockTransport | None = field(default=None, repr=False, compare=False)
    _permit: authorization.CapturePermit = field(init=False, repr=False)
    _client: httpx.AsyncClient = field(init=False, repr=False, compare=False)
    _acquisition_deadline: datetime | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if (
            type(self.tls_context) is not ssl.SSLContext
            or self.tls_context.verify_mode != ssl.CERT_REQUIRED
            or not self.tls_context.check_hostname
            or type(self.signed_permit) is not common.SignedPermit
            or self.transport is not None
            and type(self.transport) is not httpx.MockTransport
        ):
            raise _unavailable()
        try:
            permit = authorization._permit(
                common._base64url_decode(self.signed_permit.context, 4_096),
                expected_origin=self.expected_origin,
                trusted_now=_utcnow(),
            )
            if (
                permit.expires_at > common._trusted_utc(self.launch_expires_at)
                or type(self.signed_permit.key_id) is not str
                or common._KEY_ID.fullmatch(self.signed_permit.key_id) is None
                or len(common._base64url_decode(self.signed_permit.signature, 32)) != 32
            ):
                raise _unavailable()
        except ValueError:
            raise _unavailable() from None
        object.__setattr__(self, "_permit", permit)
        object.__setattr__(
            self,
            "_client",
            httpx.AsyncClient(
                verify=self.tls_context,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
                headers={"Accept-Encoding": "identity", "Cache-Control": "no-store"},
            ),
        )

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *arguments):
        return await self._client.__aexit__(*arguments)

    def require_candidate_binding(self, request):
        """Check every approved content/policy pin before claim or source access."""

        from process.custom_import.https_candidate import https_request_identity_sha256
        from process.custom_import.object_candidate import object_request_identity_sha256

        permit = self._permit
        identity_of = (
            object_request_identity_sha256
            if getattr(request, "source_kind", "https") != "https"
            else lambda candidate: https_request_identity_sha256(candidate, raw_originals=True)
        )
        try:
            is_matched = (
                all(
                    getattr(request, name) == getattr(permit, name)
                    for name in (
                        "dataset_id",
                        "definition_revision_id",
                        "schema_revision_id",
                        "idempotency_key",
                        "timeout_seconds",
                        "limits",
                        "maximum_dataset_retained_bytes",
                    )
                )
                and request.definition.digest == permit.definition_sha256
                and identity_of(request).hex() == permit.request_identity_sha256
                and capture_wire.expected_content_sha256(
                    request.definition, request.objects, request.source_snapshot_token
                )
                == permit.expected_content_sha256
                and permit.issued_at <= _utcnow() < permit.expires_at
            )
        except ValueError, TypeError, AttributeError:
            is_matched = False
        if not is_matched:
            raise _unavailable()

    @asynccontextmanager
    async def _exchange(self, request, grant, operation, *, content=b""):
        """Never resend an uncertain POST or retain response cookies."""

        self.require_candidate_binding(request)
        common._positive_id(grant.execution_id)
        common._positive_id(grant.fence)
        remaining = (self._permit.expires_at - _utcnow()).total_seconds()
        if operation == "bundle":
            if self._acquisition_deadline is None:
                raise _unavailable()
            remaining = min(remaining, (self._acquisition_deadline - _utcnow()).total_seconds())
        budget = min(300, remaining)
        if budget <= 0:
            raise _unavailable()
        prefix = "X-Custom-Import-Capture-"
        headers_by_name = {
            "Content-Type": "application/octet-stream",
            "Authorization": common.encode_lease_bearer(request.lease_token),
            prefix + "Context": self.signed_permit.context,
            prefix + "Key-Id": self.signed_permit.key_id,
            prefix + "Signature": self.signed_permit.signature,
            prefix + "Execution-Id": str(grant.execution_id),
            prefix + "Fence": str(grant.fence),
        }
        outbound = self._client.build_request(
            "POST",
            self.expected_origin + authorization.CAPTURE_PATH + "-" + operation,
            content=content,
            headers=headers_by_name,
            timeout=httpx.Timeout(budget, connect=min(5, budget)),
        )
        outbound.headers.pop("cookie", None)
        try:
            async with asyncio.timeout(budget):
                reply = await self._client.send(outbound, stream=True)
                try:
                    yield reply
                finally:
                    await reply.aclose()
        except httpx.HTTPError, httpx.StreamError, OSError, TimeoutError, ValueError, AdmissionTransportError:
            raise _unavailable() from None
        finally:
            self._client.cookies.clear()

    async def preflight(self, request, grant):
        """Retain the server's original acquisition deadline, never a renewed budget."""

        async with self._exchange(request, grant, "preflight") as reply:
            raw = await _bounded_response_body(reply, maximum=256)
            document = common._object(bytes(raw), 256)
        try:
            if document.keys() != {"acquisition_deadline_at"}:
                raise _unavailable()
            deadline = datetime.fromisoformat(document["acquisition_deadline_at"])
            common._trusted_utc(deadline)
            if not _utcnow() < deadline <= self._permit.expires_at:
                raise _unavailable()
            if self._acquisition_deadline is not None:
                deadline = min(deadline, self._acquisition_deadline)
            object.__setattr__(self, "_acquisition_deadline", deadline)
            return asyncio.get_running_loop().time() + (deadline - _utcnow()).total_seconds()
        except ValueError, TypeError:
            raise _unavailable() from None

    async def register(self, request, grant, captures):
        """Attempt one bounded commit; failure leaves reconciliation to retained state."""

        async with self._exchange(
            request, grant, "bundle", content=capture_wire.iter_capture_body(captures, limits=self._permit.limits)
        ) as reply:
            raw = await _bounded_response_body(reply, maximum=128)
            document = common._object(bytes(raw), 128)
            if document.keys() != {"capture_bundle_id"}:
                raise _unavailable()
            return common._positive_id(document["capture_bundle_id"])

    async def retained(self, request, grant, bundle_id):
        """Read the exact bound originals over authenticated TLS, never owner SQL."""

        common._positive_id(bundle_id)
        async with self._exchange(request, grant, "retained") as reply:
            if (
                reply.status_code != 200
                or _header(reply, "content-type") != "application/octet-stream"
                or _header(reply, "cache-control") != "no-store"
                or _header(reply, "content-encoding") not in {None, "identity"}
                or reply.headers.get_list("set-cookie")
                or _header(reply, "x-custom-import-capture-bundle-id") != str(bundle_id)
            ):
                raise _unavailable()
            captures = await capture_wire.read_capture_body(
                reply.aiter_raw(chunk_size=self._permit.limits.read_chunk_bytes), limits=self._permit.limits
            )
            if (
                capture_wire.manifested_content_sha256(
                    request.definition, tuple(capture.manifest for capture in captures)
                )
                != self._permit.expected_content_sha256
            ):
                raise _unavailable()
            return captures
