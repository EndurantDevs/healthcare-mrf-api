# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Load a fixed, custody-verified admission stage before claiming an execution.

The launcher must authenticate these files and expose an immutable read-only
mount. This loader has no signing key and does not establish that custody itself.
Both execute and resume must call it with the database-loaded source binding
before entering a runner, then close the returned transport after that run.
"""

from __future__ import annotations

import hashlib
import os
import re
import ssl
import stat
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass, field
from pathlib import Path

from process.custom_import import admission_authorization as authorization
from process.custom_import import admission_worker, source_worker
from process.custom_import.processing_policy import ProcessingPolicy
from process.custom_import.snowflake import _credential_file_identity, _validate_fixed_credential_directory
from process.custom_import.snowflake_source_binding import LoadedSnowflakeSourceBinding

FIXED_ADMISSION_DIRECTORY = Path("/run/custom-import-operator")
_LAUNCH_NAME = "admission-launch.json"
_PERMIT_NAME = ".admission-permit.json"
_SOURCE_PERMIT_NAME = ".source-permit.json"
_WRITER_CA_NAME = ".writer-ca.pem"
_MAX_CA_BYTES = 65_536
_LAUNCH_CONTRACT = "custom-import/admission-launch/v1"
_LAUNCH_FIELDS = {
    "contract",
    "origin",
    "admission_permit_sha256",
    "source_permit_sha256",
    "writer_ca_sha256",
    "expires_at",
}


@dataclass(frozen=True, slots=True)
class WriterBatchTransports:
    """The complete authenticated pair; neither permit can enable legacy mode."""

    admission: admission_worker.AdmissionBatchTransport = field(repr=False)
    source: source_worker.SourceBatchTransport = field(repr=False)
    _stack: AsyncExitStack = field(default_factory=AsyncExitStack, init=False, repr=False, compare=False)

    async def __aenter__(self):
        try:
            await self._stack.enter_async_context(self.admission)
            await self._stack.enter_async_context(self.source)
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(self, *arguments):
        return await self._stack.__aexit__(*arguments)

    def require_candidate_binding(self, request):
        """Deny either mismatched permit before claiming a candidate."""

        self.admission.require_candidate_binding(request)
        self.source.require_candidate_binding(request)


def _unavailable():
    raise ValueError("admission launch is unavailable") from None


def _read_mount(directory, name, maximum):
    """Read bounded owner-only bytes relative to one pinned directory."""

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory)
    except FileNotFoundError:
        return None
    with os.fdopen(descriptor, "rb") as mounted:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != 0o400
            or not 0 < before.st_size <= maximum
        ):
            _unavailable()
        raw = mounted.read(maximum + 1)
        after = os.fstat(descriptor)
        if len(raw) != before.st_size or _credential_file_identity(before) != _credential_file_identity(after):
            _unavailable()
        return raw


def _stage_files():
    """Only a genuinely absent directory or all four absent files select legacy."""

    try:
        expected = os.stat(FIXED_ADMISSION_DIRECTORY, follow_symlinks=False)
    except FileNotFoundError:
        return None
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    directory = os.open(FIXED_ADMISSION_DIRECTORY, flags)
    try:
        opened = os.fstat(directory)
        _validate_fixed_credential_directory(opened)
        if _credential_file_identity(expected) != _credential_file_identity(opened):
            _unavailable()
        contents = []
        for name, maximum in (
            (_LAUNCH_NAME, 2_048),
            (_PERMIT_NAME, 4_096),
            (_SOURCE_PERMIT_NAME, 4_096),
            (_WRITER_CA_NAME, _MAX_CA_BYTES),
        ):
            contents.append(_read_mount(directory, name, maximum))
        if _credential_file_identity(opened) != _credential_file_identity(os.fstat(directory)):
            _unavailable()
        if contents == [None, None, None, None]:
            return None
        if None in contents:
            _unavailable()
        return contents
    finally:
        os.close(directory)


def _require_retained_scope(permit, loaded, idempotency_key):
    """Compare the entire permit scope with an already verified retained load."""

    if not isinstance(loaded, LoadedSnowflakeSourceBinding) or not isinstance(
        loaded.binding.processing_policy, ProcessingPolicy
    ):
        _unavailable()
    ProcessingPolicy.from_mapping(loaded.binding.processing_policy.to_mapping())
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "source_binding_revision_id"):
        value = getattr(loaded, name)
        if type(value) is not int or value != getattr(permit, name):
            _unavailable()
    if (
        type(loaded.source_binding_sha256) is not bytes
        or loaded.source_binding_sha256.hex() != permit.source_binding_sha256
        or type(idempotency_key) is not str
        or idempotency_key != permit.idempotency_key
    ):
        _unavailable()


def _staged_permit(raw, launch, now, *, is_source=False):
    digest_field = "source_permit_sha256" if is_source else "admission_permit_sha256"
    if hashlib.sha256(raw).hexdigest() != launch[digest_field]:
        _unavailable()
    document = authorization._object(raw, 4_096)
    if document.keys() != {"context", "key_id", "signature"}:
        _unavailable()
    signed = authorization.SignedPermit(**document)
    if (
        type(signed.key_id) is not str
        or authorization._KEY_ID.fullmatch(signed.key_id) is None
        or len(authorization._base64url_decode(signed.signature, 32)) != 32
    ):
        _unavailable()
    permit = authorization._permit(
        authorization._base64url_decode(signed.context, 2_048),
        expected_origin=launch["origin"],
        trusted_now=now,
        is_source=is_source,
    )
    if permit.expires_at > authorization._canonical_utc(launch["expires_at"]):
        _unavailable()
    return signed, permit


def _writer_tls_context(raw, expected_digest):
    """Trust only the exact admitted certificate bundle, never system or environment CAs."""

    if (
        not 0 < len(raw) <= _MAX_CA_BYTES
        or hashlib.sha256(raw).hexdigest() != expected_digest
        or re.fullmatch(
            rb"(?:[ \t\r\n]*-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+-----END CERTIFICATE-----[ \t\r\n]*)+",
            raw,
        )
        is None
    ):
        _unavailable()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cadata=raw.decode("ascii"))
    return context


def load_admission_launch(
    loaded: LoadedSnowflakeSourceBinding, *, idempotency_key: str
) -> WriterBatchTransports | None:
    """Return both fixed transports, or None only when every stage file is absent.

    The origin and original launch expiry come only from the independent fixed
    stage. Permit parsing here does not replace server-side signature, retained
    scope, lease, fence, cursor and pre-commit authorization checks.
    """

    try:
        stage = _stage_files()
        if stage is None:
            return None
        launch_bytes, permit_bytes, source_bytes, ca_bytes = stage
        launch = authorization._object(launch_bytes, 2_048)
        if launch.keys() != _LAUNCH_FIELDS or launch["contract"] != _LAUNCH_CONTRACT:
            _unavailable()
        origin = authorization._origin(launch["origin"])
        expires_at = authorization._canonical_utc(launch["expires_at"])
        now = admission_worker._utcnow()
        if now >= expires_at:
            _unavailable()
        signed, permit = _staged_permit(permit_bytes, launch, now)
        source_signed, source_permit = _staged_permit(source_bytes, launch, now, is_source=True)
        if any(
            getattr(permit, name) != getattr(source_permit, name)
            for name in asdict(permit)
            if name not in {"contract", "path"}
        ):
            _unavailable()
        _require_retained_scope(permit, loaded, idempotency_key)
        tls_context = _writer_tls_context(ca_bytes, launch["writer_ca_sha256"])
        return WriterBatchTransports(
            admission_worker.AdmissionBatchTransport(
                expected_origin=origin, signed_permit=signed, launch_expires_at=expires_at, tls_context=tls_context
            ),
            source_worker.SourceBatchTransport(
                expected_origin=origin,
                signed_permit=source_signed,
                launch_expires_at=expires_at,
                tls_context=tls_context,
            ),
        )
    except OSError, ValueError, AttributeError, TypeError, admission_worker.AdmissionTransportError:
        _unavailable()
