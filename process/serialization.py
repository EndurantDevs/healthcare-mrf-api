# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared helpers for serializing ARQ job payloads."""

from __future__ import annotations

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from ruamel.ext.msgpack import packb, unpackb


def _normalize_for_msgpack(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, time):
        return {"__type__": "time", "value": value.isoformat()}
    if isinstance(value, BaseException):
        return {
            "__type__": "exception",
            "name": value.__class__.__name__,
            "message": str(value),
        }
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return list(value)
    return value


def _default_encoder(value: Any) -> Any:
    normalized = _normalize_for_msgpack(value)
    if normalized is value:
        return {"__type__": "repr", "repr": repr(value)}
    return normalized


def _restore_special(value: Any) -> Any:
    if isinstance(value, dict):
        type_tag = value.get("__type__")
        if type_tag == "datetime":
            try:
                return datetime.fromisoformat(value["value"])
            except (KeyError, TypeError, ValueError):
                return value.get("value")
        if type_tag == "date":
            try:
                return date.fromisoformat(value["value"])
            except (KeyError, TypeError, ValueError):
                return value.get("value")
        if type_tag == "time":
            try:
                return time.fromisoformat(value["value"])
            except (KeyError, TypeError, ValueError):
                return value.get("value")
        if type_tag == "repr":
            return value.get("repr")
        # Exceptions stay as plain dict payloads so callers can inspect them.
        if type_tag == "exception":
            return value
        return {key: _restore_special(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_restore_special(item) for item in value]
    return value


def serialize_job(payload: Any) -> bytes:
    """Serialize payloads for ARQ, normalising unsupported types."""
    normalized = _normalize_for_msgpack(payload)
    return packb(normalized, datetime=True, default=_default_encoder)


def deserialize_job(payload: bytes) -> Any:
    """Inverse of ``serialize_job``."""
    unpacked = unpackb(payload, timestamp=3, raw=False)
    return _restore_special(unpacked)


__all__ = ["serialize_job", "deserialize_job"]