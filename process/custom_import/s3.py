# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact path-style S3 signing over the existing guarded HTTPS acquisition."""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from urllib.parse import quote, urlsplit

from botocore.auth import S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from process.custom_import import https
from process.custom_import._source_text import validate_snapshot_token
from process.custom_import.capture import SealedCapture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import _COMPRESSIONS, _FORMATS, SourceStream, canonical_json
from process.custom_import.s3_credentials import S3CredentialProvider, S3Credentials

_CONTRACT = "custom-import/s3-object/v1"
_MAX_SIGNATURE_SECONDS = 604800


class S3AcquisitionError(ValueError):
    """A stable failure code without locators, signing keys or driver details."""


def _bounded_text(value: object, maximum_bytes: int) -> str:
    if not isinstance(value, str) or not value or not value.isprintable():
        raise ValueError
    if len(value.encode("utf-8")) > maximum_bytes:
        raise ValueError
    return value


def _endpoint(value: str) -> str:
    _bounded_text(value, 2048)
    parsed = urlsplit(value)
    checked = https._https_url(value)
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment or "?" in value:
        raise ValueError
    return str(checked.origin())


@dataclass(frozen=True)
class S3Object:
    """One exact original object; only explicit path-style addressing is supported.

    Printable UTF-8 keys are literal, including dot segments and percent signs;
    signed URI encoding is preserved exactly and redirects are not followed.
    ETag is not accepted as a content digest. No listing or endpoint discovery occurs.
    """

    stream_id: str
    endpoint: str = field(repr=False)
    region: str = field(repr=False)
    bucket: str = field(repr=False)
    key: str = field(repr=False)
    expected_sha256: str = field(repr=False)
    expected_bytes: int
    version_id: str | None = field(default=None, repr=False)
    addressing: str = "path"

    def __post_init__(self) -> None:
        try:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", self.stream_id):
                raise ValueError
            object.__setattr__(self, "endpoint", _endpoint(self.endpoint))
            if self.addressing != "path" or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.region):
                raise ValueError
            if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket):
                raise ValueError
            if any(fragment in self.bucket for fragment in ("..", ".-", "-.")):
                raise ValueError
            _bounded_text(self.key, 1024)
            if self.version_id is not None:
                _bounded_text(self.version_id, 1024)
            https._validate_identity(self.expected_sha256, self.expected_bytes, CaptureLimits(), 1)
        except Exception:
            raise S3AcquisitionError("s3_request_invalid") from None


def _unsigned_url(source_object: S3Object) -> str:
    encoded_path = "/" + source_object.bucket + "/" + quote(source_object.key, safe="/")
    address = source_object.endpoint + encoded_path
    if source_object.version_id is not None:
        address += "?versionId=" + quote(source_object.version_id, safe="")
    return address


def s3_object_identity_sha256(source_object: S3Object) -> bytes:
    """Bind the stable source descriptor, excluding credentials and signature refresh."""
    try:
        if not isinstance(source_object, S3Object):
            raise ValueError
        checked = S3Object(**asdict(source_object))
        document_by_key = {"contract": _CONTRACT, **asdict(checked)}
        return hashlib.sha256((_CONTRACT + "\0object\0" + canonical_json(document_by_key)).encode("utf-8")).digest()
    except Exception:
        raise S3AcquisitionError("s3_request_invalid") from None


def _signed_url(source_object: S3Object, credentials: S3Credentials, expires_seconds: int) -> str:
    if not isinstance(credentials, S3Credentials):
        raise ValueError
    checked = S3Credentials(**asdict(credentials))
    signer = S3SigV4QueryAuth(
        Credentials(checked.access_key_id, checked.secret_access_key, checked.session_token),
        "s3",
        source_object.region,
        expires=expires_seconds,
    )
    request = AWSRequest(method="GET", url=_unsigned_url(source_object))
    request.context["timestamp"] = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    # The convenience add_auth logs canonical requests, including private locators and tokens.
    signer._modify_request_before_signing(request)
    canonical_request = signer.canonical_request(request)
    signature = signer.signature(signer.string_to_sign(request, canonical_request), request)
    signer._inject_signature_to_request(request, signature)
    return request.url


def validate_s3_object(
    source_object: S3Object, source_stream: SourceStream, *, limits: CaptureLimits, timeout_seconds: float
) -> None:
    """Check descriptor/stream/budgets without credentials, clocks, files or network."""
    try:
        s3_object_identity_sha256(source_object)
        if not isinstance(limits, CaptureLimits):
            raise ValueError
        checked_limits = CaptureLimits(**asdict(limits))
        if any(bound > getattr(CaptureLimits(), name) for name, bound in asdict(checked_limits).items()):
            raise ValueError
        https._validate_identity(
            source_object.expected_sha256, source_object.expected_bytes, checked_limits, timeout_seconds
        )
        if timeout_seconds > _MAX_SIGNATURE_SECONDS:
            raise ValueError
        if (
            not isinstance(source_stream, SourceStream)
            or source_stream.stream_id != source_object.stream_id
            or source_stream.format not in _FORMATS
            or source_stream.compression not in _COMPRESSIONS
        ):
            raise ValueError
    except Exception:
        raise S3AcquisitionError("s3_request_invalid") from None


def _remaining_seconds(deadline: float) -> float:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise TimeoutError
    return remaining


async def acquire_s3(
    source_object: S3Object,
    source_stream: SourceStream,
    *,
    credential_provider: S3CredentialProvider,
    source_snapshot_token: str,
    limits: CaptureLimits,
    timeout_seconds: float,
) -> SealedCapture:
    """Sign one exact GET, then delegate all network/decoder ownership to HTTPS.

    Validation precedes credential I/O. One deadline covers bounded synchronous
    credential loading and signing as well as the existing HTTPS acquisition.
    The provider remains caller-owned on success, failure, timeout and cancellation.
    """
    try:
        validate_s3_object(source_object, source_stream, limits=limits, timeout_seconds=timeout_seconds)
        validate_snapshot_token(source_snapshot_token)
        if not callable(getattr(credential_provider, "load_credentials", None)):
            raise ValueError
    except Exception:
        raise S3AcquisitionError("s3_request_invalid") from None
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    try:
        async with asyncio.timeout_at(deadline):
            credentials = credential_provider.load_credentials()
            expires_seconds = math.ceil(_remaining_seconds(deadline))
            signed_url = _signed_url(source_object, credentials, expires_seconds)
            await asyncio.sleep(0)
            return await https.acquire_https(
                signed_url,
                source_stream,
                source_snapshot_token=source_snapshot_token,
                expected_sha256=source_object.expected_sha256,
                expected_bytes=source_object.expected_bytes,
                limits=limits,
                timeout_seconds=_remaining_seconds(deadline),
                exact_url=True,
            )
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except TimeoutError:
        raise S3AcquisitionError("s3_acquisition_timeout") from None
    except Exception:
        raise S3AcquisitionError("s3_acquisition_failed") from None
