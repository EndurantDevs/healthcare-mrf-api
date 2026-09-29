# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict read requests and source-hidden, authenticated pagination state."""

import base64
import hashlib
import hmac
import json
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

CURSOR_KEY_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CURSOR_KEY"
KINDS = frozenset({"organizations", "sites", "payers", "plans", "networks", "medical-groups", "practitioner-roles"})
_OPAQUE = re.compile(r"[A-Za-z0-9_-]+\Z")
_SOURCE = re.compile(r"[a-z0-9_-]{1,64}\Z")
_CURSOR_TTL = 900


class DirectoryReadError(RuntimeError):
    """A public boundary error whose details never contain private source values."""

    def __init__(self, status):
        self.status = status
        super().__init__("provider directory read rejected")


@dataclass(frozen=True)
class DirectoryRead:
    """One normalized and bounded gateway-compatible query scope."""

    kind: str
    source_id: str
    shape: str
    entity_id: str | None
    generation_id: str | None
    limit: int = 25
    cursor: str | None = None

    def scope(self, generation_id):
        """Bind cursor authentication to every selector, including page size."""
        return [self.kind, self.source_id, self.shape, self.entity_id, generation_id, self.limit]


def _query_fields(query, shape):
    if not isinstance(query, str) or not query or len(query.encode("utf-8", errors="replace")) > 4096:
        raise DirectoryReadError(400)
    if re.search(r"%(?![0-9A-Fa-f]{2})", query):
        raise DirectoryReadError(400)
    try:
        pairs = parse_qsl(query, keep_blank_values=True, strict_parsing=True, encoding="utf-8", errors="strict")
    except ValueError, UnicodeError:
        raise DirectoryReadError(400) from None
    allowed = {"source_id", "generation_id"} if shape == "entity" else {"source_id", "generation_id", "limit", "cursor"}
    fields_by_name = dict(pairs)
    if len(fields_by_name) != len(pairs) or not set(fields_by_name) <= allowed:
        raise DirectoryReadError(400)
    return fields_by_name


def parse_directory_read(kind, entity_id, shape, query):
    """Reject duplicate, malformed and unbounded selectors before database access."""
    if kind not in KINDS or shape not in {"entities", "entity", "relationships"}:
        raise DirectoryReadError(404)
    try:
        if entity_id is not None and kind == "payers":
            if not isinstance(entity_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", entity_id):
                raise ValueError
        else:
            entity_id = str(UUID(entity_id)) if entity_id is not None else None
    except ValueError, TypeError, AttributeError:
        raise DirectoryReadError(404) from None
    if (shape == "entities") != (entity_id is None):
        raise DirectoryReadError(404)
    fields_by_name = _query_fields(query, shape)
    source_id = fields_by_name.get("source_id", "")
    if not _SOURCE.fullmatch(source_id):
        raise DirectoryReadError(400)
    for field_name, max_length in (("generation_id", 128), ("cursor", 512)):
        field_value = fields_by_name.get(field_name)
        if field_value is not None and (len(field_value) > max_length or not _OPAQUE.fullmatch(field_value)):
            raise DirectoryReadError(400)
    if "cursor" in fields_by_name and "generation_id" not in fields_by_name:
        raise DirectoryReadError(400)
    raw_limit = fields_by_name.get("limit", "25")
    if not re.fullmatch(r"[1-9][0-9]{0,2}", raw_limit) or int(raw_limit) > 100:
        raise DirectoryReadError(400)
    return DirectoryRead(
        kind,
        source_id,
        shape,
        entity_id,
        fields_by_name.get("generation_id"),
        int(raw_limit),
        fields_by_name.get("cursor"),
    )


def directory_cursor_key():
    """Require an operator-configured 256-bit key; never use a process-local fallback."""
    encoded = os.getenv(CURSOR_KEY_ENV, "")
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", encoded):
        raise DirectoryReadError(503)
    key = base64.urlsafe_b64decode(encoded + "=")
    if base64.urlsafe_b64encode(key).decode().rstrip("=") != encoded:
        raise DirectoryReadError(503)
    return key


def opaque_directory_key(key, prefix, *identity):
    """Hide source identifiers with a domain-separated keyed digest."""
    encoded = json.dumps([prefix, *identity], ensure_ascii=True, separators=(",", ":")).encode()
    return prefix + hmac.new(key, encoded, hashlib.sha256).hexdigest()


def issue_directory_cursor(key, query, generation_id, position, now=None):
    """Encrypt one seek position with a fixed expiry and the complete query scope."""
    expires = int(time.time() if now is None else now) + _CURSOR_TTL
    plaintext = json.dumps([position, expires], separators=(",", ":")).encode()
    nonce = os.urandom(12)
    scope = json.dumps(query.scope(generation_id), separators=(",", ":")).encode()
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, b"provider-directory.cursor.v1\0" + scope)
    return base64.urlsafe_b64encode(nonce + ciphertext).decode().rstrip("=")


def read_directory_cursor(key, query, generation_id, now=None):
    """Fail stale, expired or altered cursors closed without exposing their contents."""
    if query.cursor is None:
        return None
    try:
        encoded = query.cursor
        decoded = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        if base64.urlsafe_b64encode(decoded).decode().rstrip("=") != encoded:
            raise ValueError
        scope = json.dumps(query.scope(generation_id), separators=(",", ":")).encode()
        plaintext = AESGCM(key).decrypt(decoded[:12], decoded[12:], b"provider-directory.cursor.v1\0" + scope)
        position, expires = json.loads(plaintext)
        if type(expires) is not int or not int(time.time() if now is None else now) < expires:
            raise ValueError
        if query.kind == "payers" and query.shape == "entities":
            if not isinstance(position, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", position):
                raise ValueError
        elif query.shape == "relationships" and not (
            query.source_id == "cms-npd" and query.kind in {"networks", "payers"}
        ):
            if type(position) is not int or not 0 < position < 2**63:
                raise ValueError
        elif type(position) is not str or str(UUID(position)) != position:
            raise ValueError
        return position
    except ValueError, TypeError, UnicodeError, InvalidTag:
        raise DirectoryReadError(409) from None
