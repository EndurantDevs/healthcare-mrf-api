# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Encode and read the keyset cursor of nearby provider pages."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import uuid
from typing import Any

import sanic.exceptions

_CURSOR_VERSION = 1
_CURSOR_ERRORS = (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError)


def _total_source(counted_with: Any) -> str:
    """Identify the resolved plan snapshots, filter year and code mappings a total was counted with."""
    return hashlib.sha256(json.dumps(counted_with, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]


def _cursor_payload(raw: str) -> dict[str, Any]:
    value = str(raw or "").strip()
    if not value or len(value) > 2048:
        raise ValueError("invalid cursor length")
    return json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))


def encode_nearby_cursor(
    scope: str,
    distance_meters: float,
    npi: int,
    address_key: str,
    counted: tuple[int, Any] | None = None,
) -> str:
    """Encode the last served identity and, when known, the exact total with what it was counted with."""
    payload = {
        "v": _CURSOR_VERSION,
        "s": scope,
        "d": float(distance_meters),
        "n": int(npi),
        "a": str(address_key).lower(),
    }
    if counted is not None:
        payload["t"] = [int(counted[0]), _total_source(counted[1])]
    encoded = base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return encoded.rstrip(b"=").decode("ascii")


def decode_nearby_cursor(raw: str, scope: str) -> tuple[float, int, str]:
    """Return the keyset position of a cursor issued for the same filters."""
    try:
        payload = _cursor_payload(raw)
        distance = float(payload["d"])
        npi = int(payload["n"])
        address_key = str(uuid.UUID(str(payload["a"])))
        if payload.get("v") != _CURSOR_VERSION:
            raise ValueError("unsupported cursor version")
        if payload.get("s") != scope:
            raise ValueError("cursor filters do not match this request")
        if not math.isfinite(distance) or distance < 0 or npi <= 0:
            raise ValueError("invalid cursor values")
        return distance, npi, address_key
    except _CURSOR_ERRORS as exc:
        raise sanic.exceptions.InvalidUsage("cursor is invalid or does not match the current geo filters") from exc


def carried_cursor_total(raw: str, counted_with: Any) -> int | None:
    """Return the opening page's total while the request resolves to the same snapshots and filters."""
    try:
        total, source = _cursor_payload(raw)["t"]
    except _CURSOR_ERRORS:
        return None
    if type(total) is not int or not 0 <= total < 2**53 or source != _total_source(counted_with):
        return None
    return total
