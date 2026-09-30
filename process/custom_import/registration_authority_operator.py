# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Register canonical input with only a fixed mounted, scoped capability."""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import hashlib
import hmac
import json
import os
import stat
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from process.custom_import.cli import _receipt_only_database_output
from process.custom_import.definition import canonical_json, load_json_definition
from process.custom_import.registration_authority import _SHA256, _authority_id, _receipt_text, _semantic_input_digest
from process.custom_import.snowflake import _credential_file_identity

FIXED_AUTHORITY_DIRECTORY = Path("/run/custom-import-operator")
_CONTRACT = "custom-import-registration-authority/v1"
_FIELDS = {"contract_version", "authority_id", "engine_origin", "input_sha256", "token_sha256", "expires_at"}
_MAX_AUTHORITY_BYTES = 2048
_MAX_RESPONSE_BYTES = 8192
_TIMEOUT_SECONDS = 15


def _unavailable():
    raise ValueError("registration authority is unavailable") from None


def _validate_directory(metadata):
    """Require a real root or effective-user directory without untrusted writes."""

    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid not in {0, os.geteuid()}
        or stat.S_IMODE(metadata.st_mode) & 0o022
    ):
        _unavailable()


def _validate_file(metadata, maximum):
    """Accept owner-only files or root-held read-only native group mounts."""

    mode = stat.S_IMODE(metadata.st_mode)
    groups = {os.getegid(), *os.getgroups()}
    is_owner_read = metadata.st_uid == os.geteuid() and mode == 0o400
    is_group_mount = metadata.st_uid == 0 and metadata.st_gid in groups and mode == 0o440
    if (
        not stat.S_ISREG(metadata.st_mode)
        or not (is_owner_read or is_group_mount)
        or not 0 < metadata.st_size <= maximum
    ):
        _unavailable()


def _read_mount(directory, name, maximum):
    """Read bounded stable regular bytes relative to one held directory."""

    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(name, flags, dir_fd=directory)
    with os.fdopen(descriptor, "rb") as mounted_file:
        before = os.fstat(descriptor)
        _validate_file(before, maximum)
        raw = mounted_file.read(maximum + 1)
        after = os.fstat(descriptor)
        _validate_file(after, maximum)
        if len(raw) != before.st_size or _credential_file_identity(before) != _credential_file_identity(after):
            _unavailable()
        return raw


def _authority_files():
    """Open only the two fixed registrar files without following symlinks."""

    expected = os.stat(FIXED_AUTHORITY_DIRECTORY, follow_symlinks=False)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    directory = os.open(FIXED_AUTHORITY_DIRECTORY, flags)
    try:
        opened = os.fstat(directory)
        _validate_directory(opened)
        if (expected.st_dev, expected.st_ino) != (opened.st_dev, opened.st_ino):
            _unavailable()
        capability = _read_mount(directory, "registration-capability", 32)
        if len(capability) != 32:
            _unavailable()
        authority_by_name = load_json_definition(
            _read_mount(directory, "registration-authority.json", _MAX_AUTHORITY_BYTES)
        )
        _validate_directory(os.fstat(directory))
        return authority_by_name, capability
    finally:
        os.close(directory)


def _origin(origin):
    """Accept a bounded fixed HTTPS origin without credentials or routing parts."""

    if (
        type(origin) is not str
        or not 0 < len(origin) <= 512
        or not origin.isascii()
        or any(not 32 < ord(character) < 127 for character in origin)
        or "?" in origin
        or "#" in origin
    ):
        _unavailable()
    parsed = urlsplit(origin)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.port is not None
        and not 0 < parsed.port <= 65535
    ):
        _unavailable()
    return origin.removesuffix("/")


def _require_authority(authority_by_name, capability, registration):
    """Validate the closed version, semantic pins, destination and UTC expiry syntax."""

    if type(authority_by_name) is not dict or set(authority_by_name) != _FIELDS:
        _unavailable()
    if authority_by_name["contract_version"] != _CONTRACT:
        _unavailable()
    _authority_id(authority_by_name["authority_id"])
    _origin(authority_by_name["engine_origin"])
    for name in ("input_sha256", "token_sha256"):
        value = authority_by_name[name]
        if type(value) is not str or _SHA256.fullmatch(value) is None:
            _unavailable()
    expiry = authority_by_name["expires_at"]
    if type(expiry) is not str or not 0 < len(expiry) <= 40 or not expiry.endswith(("Z", "+00:00")):
        _unavailable()
    # The server permits completed retries after expiry and denies unfinished work.
    if dt.datetime.fromisoformat(expiry).utcoffset() != dt.timedelta(0):
        _unavailable()
    if not hmac.compare_digest(hashlib.sha256(capability).hexdigest(), authority_by_name["token_sha256"]):
        _unavailable()
    if not hmac.compare_digest(_semantic_input_digest(registration).hex(), authority_by_name["input_sha256"]):
        _unavailable()


def _header(reply, name):
    """Return one bounded ASCII header without merging duplicate values."""

    header_values = reply.headers.get_list(name)
    if not header_values:
        return None
    if len(header_values) != 1 or not header_values[0].isascii() or not 0 < len(header_values[0]) <= 128:
        _unavailable()
    return header_values[0].lower()


async def _read_response(reply):
    """Accept one no-store JSON receipt and stop before retaining excess bytes."""

    if (
        reply.status_code != 200
        or _header(reply, "content-type") not in {"application/json", "application/json; charset=utf-8"}
        or _header(reply, "cache-control") != "no-store"
        or _header(reply, "content-encoding") not in {None, "identity"}
        or reply.headers.get("set-cookie") is not None
    ):
        _unavailable()
    declared = _header(reply, "content-length")
    if declared is not None and (not declared.isdecimal() or len(declared) > 10 or int(declared) > _MAX_RESPONSE_BYTES):
        _unavailable()
    body = bytearray()
    async for chunk in reply.aiter_bytes(chunk_size=_MAX_RESPONSE_BYTES + 1):
        if len(body) + len(chunk) > _MAX_RESPONSE_BYTES:
            _unavailable()
        body.extend(chunk)
    if not body or declared is not None and len(body) != int(declared):
        _unavailable()
    return load_json_definition(bytes(body))


async def _request(authority_by_name, capability, registration, transport):
    """Send one scoped registration request with verified TLS and a total deadline."""

    encoded = base64.urlsafe_b64encode(capability).decode("ascii").rstrip("=")
    headers_by_name = {
        "Authorization": "Bearer " + encoded,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-store",
    }
    path = "/control/v1/custom-import/registration-authorities/" + authority_by_name["authority_id"] + "/register"
    async with (
        asyncio.timeout(_TIMEOUT_SECONDS),
        httpx.AsyncClient(
            verify=True,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(_TIMEOUT_SECONDS, connect=5),
            transport=transport,
        ) as client,
        client.stream(
            "POST",
            _origin(authority_by_name["engine_origin"]) + path,
            content=canonical_json(registration).encode("utf-8"),
            headers=headers_by_name,
        ) as reply,
    ):
        return await _read_response(reply)


async def register_authority(dataset_key, definition, binding, *, transport=None):
    """Return only a graph-bound receipt without connecting a local database."""

    try:
        with _receipt_only_database_output(None):
            if transport is not None and type(transport) is not httpx.MockTransport:
                _unavailable()
            registration_by_name = {
                "dataset_key": dataset_key,
                "definition": json.loads(definition.canonical),
                "source_binding": json.loads(binding.canonical),
            }
            authority_by_name, capability = _authority_files()
            _require_authority(authority_by_name, capability, registration_by_name)
            receipt_by_name = await _request(authority_by_name, capability, registration_by_name, transport)
            rendered = _receipt_text(receipt_by_name)
            expected_by_name = {
                "definition_sha256": definition.digest,
                "schema_sha256": definition.schema_digest,
                "source_binding_sha256": binding.digest,
            }
            if any(receipt_by_name[name] != digest for name, digest in expected_by_name.items()):
                _unavailable()
            return rendered
    except Exception:
        _unavailable()
