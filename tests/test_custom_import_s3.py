# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic S3 signing and guarded transport, without sockets or cloud access."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import hmac
import logging
import os
import time
import traceback
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qsl, quote

import aiohttp
import botocore.auth
import botocore.credentials
import botocore.session
import pytest

from process.custom_import import s3
from process.custom_import.capture import iter_records, verify_capture
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.s3_credentials import FixedLocalS3CredentialProvider, S3Credentials
from tests.test_custom_import_https import _LIMITS, _PAYLOAD, _STREAM, _assert_closed, _Response
from tests.test_custom_import_https import no_network as no_network
from tests.test_custom_import_https import transport as transport
from tests.test_custom_import_s3_credentials import _credential_file


class _Provider:
    def __init__(self, credentials=None):
        self.credentials = credentials or S3Credentials("synthetic-access", "synthetic-secret", "synthetic-token+/=%")
        self.loads = 0
        self.closed = False

    def load_credentials(self):
        self.loads += 1
        return self.credentials


def _object(**changes):
    descriptor = s3.S3Object(
        stream_id="root",
        endpoint="https://objects.example",
        region="us-east-1",
        bucket="synthetic-bucket",
        key="directory/records.csv",
        expected_sha256=hashlib.sha256(_PAYLOAD).hexdigest(),
        expected_bytes=len(_PAYLOAD),
    )
    return replace(descriptor, **changes)


async def _acquire(provider, **changes):
    arguments_by_name = dict(
        source_object=_object(),
        source_stream=_STREAM,
        credential_provider=provider,
        source_snapshot_token="synthetic-snapshot",
        limits=_LIMITS,
        timeout_seconds=5,
    )
    arguments_by_name.update(changes)
    return await s3.acquire_s3(**arguments_by_name)


def _signature_from_wire(wire_url, secret):
    query_by_name = dict(parse_qsl(wire_url.raw_query_string))
    scope = query_by_name["X-Amz-Credential"].split("/", 1)[1]
    canonical_query = "&".join(
        sorted(
            quote(name, safe="-_.~") + "=" + quote(value, safe="-_.~")
            for name, value in query_by_name.items()
            if name != "X-Amz-Signature"
        )
    )
    canonical_request = "\n".join(
        ("GET", wire_url.raw_path, canonical_query, f"host:{wire_url.raw_authority}\n", "host", "UNSIGNED-PAYLOAD")
    )
    string_to_sign = "\n".join(
        ("AWS4-HMAC-SHA256", query_by_name["X-Amz-Date"], scope, hashlib.sha256(canonical_request.encode()).hexdigest())
    )
    signing_key = ("AWS4" + secret).encode()
    for scope_component in scope.split("/"):
        signing_key = hmac.digest(signing_key, scope_component.encode(), "sha256")
    return hmac.new(signing_key, string_to_sign.encode(), "sha256").hexdigest()


@pytest.mark.parametrize(
    "key",
    (
        "nested/spaced name.csv",
        "é/東京+%.csv",
        "/double//slash.csv",
        "literal?hash#.csv",
        "a/../b",
        "./a",
        "a?b#c&d=e;f:@,$!()[]*.csv",
    ),
)
async def test_exact_get_signature_survives_transport_encoding(transport, monkeypatch, key):
    monkeypatch.setattr(s3, "datetime", SimpleNamespace(now=lambda _zone: datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)))
    provider = _Provider()
    source_object = _object(key=key, version_id="version+/=% &é", endpoint="https://objects.example:8443")
    response = _Response()
    transport.responses = [response]
    captured = await _acquire(provider, source_object=source_object)
    method, wire_url, _options = transport.requests[0]
    query_by_name = dict(parse_qsl(wire_url.raw_query_string))
    assert method == "GET" and wire_url.raw_path == "/synthetic-bucket/" + quote(key, safe="/")
    assert query_by_name["versionId"] == source_object.version_id
    assert query_by_name["X-Amz-Security-Token"] == provider.credentials.session_token
    assert query_by_name["X-Amz-Credential"] == "synthetic-access/20260102/us-east-1/s3/aws4_request"
    assert query_by_name["X-Amz-Signature"] == _signature_from_wire(wire_url, provider.credentials.secret_access_key)
    assert query_by_name["X-Amz-SignedHeaders"] == "host"
    assert str(wire_url) == s3._signed_url(source_object, provider.credentials, int(query_by_name["X-Amz-Expires"]))
    assert captured.payload == _PAYLOAD and provider.loads == 1 and not provider.closed
    verify_capture(captured, _STREAM, limits=_LIMITS)
    _assert_closed(transport, [response])


def test_identity_survives_signature_refresh_and_endpoint_canonicalization(monkeypatch):
    source_object = _object(endpoint="https://OBJECTS.example:443/")
    expected = s3.s3_object_identity_sha256(_object())
    first = s3._signed_url(source_object, _Provider().credentials, 60)
    monkeypatch.setattr(s3, "datetime", SimpleNamespace(now=lambda _zone: datetime(2027, 1, 1, tzinfo=UTC)))
    second = s3._signed_url(source_object, S3Credentials("rotated-access", "rotated-secret"), 120)
    assert first != second and s3.s3_object_identity_sha256(source_object) == expected
    assert all(marker not in repr(source_object) for marker in ("objects.example", "directory/", "synthetic-bucket"))


@pytest.mark.parametrize(
    "change",
    (
        {"stream_id": "other"},
        {"endpoint": "https://other.example"},
        {"region": "us-west-2"},
        {"bucket": "other-bucket"},
        {"key": "other.csv"},
        {"version_id": "v2"},
        {"expected_bytes": 0},
        {"expected_sha256": "a" * 64},
    ),
)
def test_every_stable_descriptor_member_changes_identity(change):
    assert s3.s3_object_identity_sha256(_object(**change)) != s3.s3_object_identity_sha256(_object())


@pytest.mark.parametrize(
    "change",
    (
        {"endpoint": "http://objects.example"},
        {"endpoint": "https://objects.example/prefix"},
        {"endpoint": "https://objects.example?"},
        {"endpoint": "https://objects.example?versionId=v1"},
        {"endpoint": "https://owner@objects.example"},
        {"endpoint": "https://127.0.0.1"},
        {"endpoint": "https://[::1]"},
        {"endpoint": "https://objects.example/#fragment"},
        {"stream_id": "../stream"},
        {"region": "us-east-1/*"},
        {"bucket": "other/objects"},
        {"bucket": "a..b"},
        {"addressing": "virtual"},
        {"key": ""},
        {"key": "\x00"},
        {"key": "é" * 513},
        {"key": "\ud800"},
        {"version_id": ""},
        {"version_id": "v" * 1025},
        {"expected_sha256": "etag-is-not-sha256"},
        {"expected_bytes": True},
        {"expected_bytes": -1},
        {"expected_bytes": 64 * 1024 * 1024 + 1},
    ),
)
def test_invalid_descriptor_has_only_a_redacted_error(change):
    with pytest.raises(s3.S3AcquisitionError, match="^s3_request_invalid$") as failure:
        _object(**change)
    assert failure.value.__cause__ is None


@pytest.mark.parametrize(
    "change",
    (
        {"source_stream": replace(_STREAM, stream_id="other")},
        {"source_stream": replace(_STREAM, format="unknown")},
        {"source_stream": replace(_STREAM, compression="unknown")},
        {"source_snapshot_token": ""},
        {"limits": None},
        {"limits": CaptureLimits(maximum_compressed_bytes=128 * 1024 * 1024)},
        {"timeout_seconds": 0},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": True},
        {"timeout_seconds": 604801},
        {"source_object": object()},
    ),
)
async def test_invalid_acquisition_never_loads_credentials_or_opens_transport(transport, change):
    provider = _Provider()
    with pytest.raises(s3.S3AcquisitionError, match="^s3_request_invalid$"):
        await _acquire(provider, **change)
    assert provider.loads == 0 and transport.requests == transport.sessions == transport.spools == []


def test_validation_is_clock_and_provider_free(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("validation must be side-effect-free")

    monkeypatch.setattr(s3, "datetime", SimpleNamespace(now=forbidden))
    monkeypatch.setattr(s3, "_signed_url", forbidden)
    s3.validate_s3_object(_object(), _STREAM, limits=_LIMITS, timeout_seconds=5)


async def test_signing_has_no_ambient_discovery_or_sensitive_debug_logs(transport, monkeypatch, caplog):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("ambient discovery or verbose signing is forbidden")

    for name in ("Session", "get_session"):
        monkeypatch.setattr(botocore.session, name, forbidden)
    monkeypatch.setattr(botocore.credentials, "create_credential_resolver", forbidden)
    monkeypatch.setattr(botocore.auth.S3SigV4QueryAuth, "add_auth", forbidden)
    monkeypatch.setenv("AWS_PROFILE", "synthetic-unused-profile")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "synthetic-unused-ambient-key")
    with caplog.at_level(logging.DEBUG, logger="botocore.auth"):
        transport.responses = [_Response()]
        await _acquire(_Provider())
    assert not [record for record in caplog.records if record.name == "botocore.auth"]


@pytest.mark.parametrize(
    "failure", (RuntimeError("private-driver-details"), aiohttp.ClientSSLError(None, OSError("private-TLS-details")))
)
async def test_transport_errors_are_redacted_and_resources_close(transport, failure):
    provider = _Provider()
    transport.responses = [failure]
    with pytest.raises(s3.S3AcquisitionError) as caught:
        await _acquire(provider)
    rendered = "".join(traceback.format_exception(caught.value))
    assert "private-" not in rendered and "synthetic-token" not in rendered and caught.value.__cause__ is None
    assert not provider.closed
    _assert_closed(transport)


@pytest.mark.parametrize("mismatch", ("hash", "length", "decoded"))
async def test_content_and_decoder_limits_remain_authoritative(transport, mismatch):
    payload = gzip.compress(_PAYLOAD * 500, mtime=0) if mismatch == "decoded" else _PAYLOAD
    source_object = _object(expected_sha256=hashlib.sha256(payload).hexdigest(), expected_bytes=len(payload))
    if mismatch == "hash":
        source_object = replace(source_object, expected_sha256="0" * 64)
    if mismatch == "length":
        source_object = replace(source_object, expected_bytes=len(payload) - 1)
    response = _Response((payload,))
    transport.responses = [response]
    with pytest.raises(s3.S3AcquisitionError):
        await _acquire(
            _Provider(),
            source_object=source_object,
            source_stream=replace(_STREAM, compression="gzip" if mismatch == "decoded" else "none"),
        )
    _assert_closed(transport, [response])


async def test_gzip_keeps_original_bytes_and_existing_decoder(transport):
    payload = gzip.compress(_PAYLOAD, mtime=0)
    source_object = _object(expected_sha256=hashlib.sha256(payload).hexdigest(), expected_bytes=len(payload))
    stream = replace(_STREAM, compression="gzip")
    transport.responses = [_Response((payload,))]
    captured = await _acquire(_Provider(), source_object=source_object, source_stream=stream)
    assert captured.payload == payload
    assert [record.values for record in iter_records(captured, stream, limits=_LIMITS)] == [
        {"id": "1", "name": "example"}
    ]
    _assert_closed(transport)


@pytest.mark.parametrize("addresses", (["127.0.0.1"], ["8.8.8.8", "10.0.0.1"]))
async def test_dns_policy_remains_strict(transport, addresses):
    transport.dns_answers = [addresses]
    transport.responses = [_Response()]
    with pytest.raises(s3.S3AcquisitionError):
        await _acquire(_Provider())
    _assert_closed(transport)


@pytest.mark.parametrize("location", ("https://other.example/object", "/same-origin-object"))
async def test_signed_redirects_are_denied(transport, location):
    response = _Response(status=307, headers=(("Location", location),))
    transport.responses = [response]
    with pytest.raises(s3.S3AcquisitionError):
        await _acquire(_Provider())
    assert len(transport.requests) == 1
    _assert_closed(transport, [response])


@pytest.mark.parametrize("failure", (False, True))
async def test_fixed_provider_handle_is_owned_by_caller(transport, tmp_path, failure):
    directory, _path = _credential_file(tmp_path)
    with FixedLocalS3CredentialProvider(directory) as provider:
        descriptor = provider._descriptor
        transport.responses = [RuntimeError("synthetic transport failure") if failure else _Response()]
        if failure:
            with pytest.raises(s3.S3AcquisitionError):
                await _acquire(provider)
        else:
            assert (await _acquire(provider)).payload == _PAYLOAD
        os.fstat(descriptor)
    with pytest.raises(OSError):
        os.fstat(descriptor)
    _assert_closed(transport)


@pytest.mark.parametrize("stage", ("credentials", "signing"))
async def test_synchronous_preparation_cannot_outlive_deadline(transport, monkeypatch, stage):
    provider = _Provider()
    original = provider.load_credentials if stage == "credentials" else s3._signed_url

    def delayed(*arguments):
        time.sleep(0.02)
        return original(*arguments)

    monkeypatch.setattr(
        provider if stage == "credentials" else s3,
        "load_credentials" if stage == "credentials" else "_signed_url",
        delayed,
    )
    with pytest.raises(s3.S3AcquisitionError, match="^s3_acquisition_timeout$"):
        await _acquire(provider, timeout_seconds=0.005)
    assert transport.requests == [] and not provider.closed


@pytest.mark.parametrize("cancel", (False, True))
async def test_body_timeout_or_cancellation_drains_owned_resources(transport, cancel):
    provider = _Provider()
    response = _Response(pause=True)
    transport.responses = [response]
    task = asyncio.create_task(_acquire(provider, timeout_seconds=1 if cancel else 0.03))
    await asyncio.wait_for(response.started.wait(), timeout=1)
    if cancel:
        task.cancel("private-cancellation-marker")
    with pytest.raises(asyncio.CancelledError if cancel else s3.S3AcquisitionError) as caught:
        await task
    assert "private-cancellation-marker" not in str(caught.value) and not provider.closed
    _assert_closed(transport, [response])
