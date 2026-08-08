# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opaque, source-hidden cursors for current FHIR formulary pages."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import datetime as dt
import hashlib
import json
import os
import re
from typing import Mapping

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from api.formulary_fhir_serving import FHIRFormularyInvalidRequestError
from api.formulary_fhir_serving import FHIRFormularyServingUnavailableError


_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,512}\Z")
_KEY_PATTERN = re.compile(r"[A-Za-z0-9_-]{43}\Z")
FHIR_FORMULARY_DATASET_ID_PATTERN = re.compile(r"^ffd_[0-9a-f]{48}$")
_CURSOR_KINDS = frozenset({"formularies", "aliases", "drugs"})
_CURSOR_AAD = b"healthporta.fhir-formulary-serving-cursor.v1"
FHIR_FORMULARY_CURSOR_KEY_ENV = "HLTHPRT_FHIR_FORMULARY_CURSOR_KEY"


@dataclass(frozen=True, slots=True)
class FHIRFormularyPageCursor:
    """Validated page position bound to one public query scope."""

    marker: str
    last_id: str


def current_fhir_formulary_marker(
    dataset_id: object,
    generation: object,
    published_at: object,
    *,
    private_identity: tuple[str, ...] = (),
) -> str:
    """Hash one immutable current-publication identity for a sealed cursor."""

    if (
        type(dataset_id) is not str
        or FHIR_FORMULARY_DATASET_ID_PATTERN.fullmatch(dataset_id) is None
        or type(generation) is not int
        or generation <= 0
        or type(published_at) is not dt.datetime
        or published_at.tzinfo is None
        or type(private_identity) is not tuple
        or any(
            type(identity) is not str
            or not identity
            or len(identity) > 64
            or not identity.isprintable()
            for identity in private_identity
        )
    ):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary current evidence is invalid"
        )
    timestamp_text = published_at.astimezone(dt.UTC).isoformat().replace(
        "+00:00", "Z"
    )
    marker_fields = (dataset_id, str(generation), timestamp_text, *private_identity)
    return hashlib.sha256("\0".join(marker_fields).encode("ascii")).hexdigest()


def _canonical_scope(scope_by_field: Mapping[str, object]) -> str:
    if type(scope_by_field) is not dict or not scope_by_field:
        raise FHIRFormularyInvalidRequestError("FHIR formulary scope is invalid")
    for field_name, field_value in scope_by_field.items():
        if (
            type(field_name) is not str
            or not field_name
            or type(field_value) not in {str, bool, int, type(None)}
        ):
            raise FHIRFormularyInvalidRequestError(
                "FHIR formulary scope is invalid"
            )
    return json.dumps(
        scope_by_field,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _scope_digest(scope_by_field: Mapping[str, object]) -> str:
    canonical_scope = _canonical_scope(scope_by_field).encode("ascii")
    return hashlib.sha256(canonical_scope).hexdigest()


def _cursor_json(
    kind: str,
    scope_by_field: Mapping[str, object],
    marker: str,
    last_id: str,
) -> bytes:
    if (
        kind not in _CURSOR_KINDS
        or type(marker) is not str
        or not marker
        or len(marker) > 128
        or type(last_id) is not str
        or not last_id
        or len(last_id) > 64
    ):
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    payload_by_field = {
        "kind": kind,
        "last": last_id,
        "marker": marker,
        "scope": _scope_digest(scope_by_field),
        "version": 1,
    }
    return json.dumps(
        payload_by_field,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def encode_fhir_formulary_cursor(
    *,
    kind: str,
    scope_by_field: Mapping[str, object],
    marker: str,
    last_id: str,
    environment: Mapping[str, str] | None = None,
) -> str:
    """Encrypt and authenticate one source-hidden page cursor."""

    key = require_fhir_formulary_cursor_configuration(environment)
    nonce = os.urandom(12)
    ciphertext = AESGCM(key).encrypt(
        nonce,
        _cursor_json(kind, scope_by_field, marker, last_id),
        _CURSOR_AAD,
    )
    return base64.urlsafe_b64encode(
        nonce + ciphertext
    ).decode("ascii").rstrip("=")


def _reject_duplicate_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    payload_by_field: dict[str, object] = {}
    for field_name, field_value in pairs:
        if field_name in payload_by_field:
            raise ValueError("duplicate cursor field")
        payload_by_field[field_name] = field_value
    return payload_by_field


def require_fhir_formulary_cursor_configuration(
    environment: Mapping[str, str] | None = None,
) -> bytes:
    """Return one exact 256-bit serving key or fail before data access."""

    environment_values = os.environ if environment is None else environment
    key_text = environment_values.get(FHIR_FORMULARY_CURSOR_KEY_ENV, "")
    if not _KEY_PATTERN.fullmatch(key_text):
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary cursor configuration is unavailable"
        )
    try:
        key = base64.b64decode(key_text + "=", altchars=b"-_", validate=True)
    except ValueError:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary cursor configuration is unavailable"
        ) from None
    canonical_key = base64.urlsafe_b64encode(key).decode("ascii").rstrip("=")
    if len(key) != 32 or canonical_key != key_text:
        raise FHIRFormularyServingUnavailableError(
            "FHIR formulary cursor configuration is unavailable"
        )
    return key


def decode_fhir_formulary_cursor(
    raw_cursor: object,
    *,
    kind: str,
    scope_by_field: Mapping[str, object],
    environment: Mapping[str, str] | None = None,
) -> FHIRFormularyPageCursor | None:
    """Decode one canonical cursor and require its exact public query scope."""

    if raw_cursor is None:
        return None
    if type(raw_cursor) is not str or not _CURSOR_PATTERN.fullmatch(raw_cursor):
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    key = require_fhir_formulary_cursor_configuration(environment)
    try:
        padding = "=" * (-len(raw_cursor) % 4)
        token_bytes = base64.b64decode(
            raw_cursor + padding,
            altchars=b"-_",
            validate=True,
        )
        canonical_token = base64.urlsafe_b64encode(token_bytes).decode(
            "ascii"
        ).rstrip("=")
        if canonical_token != raw_cursor:
            raise ValueError("cursor encoding is not canonical")
        if len(token_bytes) < 29:
            raise ValueError("cursor is truncated")
        cursor_bytes = AESGCM(key).decrypt(
            token_bytes[:12],
            token_bytes[12:],
            _CURSOR_AAD,
        )
        payload_by_field = json.loads(
            cursor_bytes.decode("ascii"),
            object_pairs_hook=_reject_duplicate_fields,
        )
    except (InvalidTag, UnicodeError, ValueError, json.JSONDecodeError):
        raise FHIRFormularyInvalidRequestError(
            "FHIR formulary cursor is invalid"
        ) from None
    expected_fields = {"kind", "last", "marker", "scope", "version"}
    if type(payload_by_field) is not dict or set(payload_by_field) != expected_fields:
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    marker = payload_by_field.get("marker")
    last_id = payload_by_field.get("last")
    if (
        type(payload_by_field.get("version")) is not int
        or payload_by_field.get("version") != 1
        or payload_by_field.get("kind") != kind
        or payload_by_field.get("scope") != _scope_digest(scope_by_field)
        or type(marker) is not str
        or not marker
        or len(marker) > 128
        or type(last_id) is not str
        or not last_id
        or len(last_id) > 64
    ):
        raise FHIRFormularyInvalidRequestError("FHIR formulary cursor is invalid")
    return FHIRFormularyPageCursor(marker=marker, last_id=last_id)


__all__ = (
    "FHIRFormularyPageCursor",
    "FHIR_FORMULARY_DATASET_ID_PATTERN",
    "FHIR_FORMULARY_CURSOR_KEY_ENV",
    "current_fhir_formulary_marker",
    "decode_fhir_formulary_cursor",
    "encode_fhir_formulary_cursor",
    "require_fhir_formulary_cursor_configuration",
)
