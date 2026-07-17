# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical value, URL, and semantic-hash helpers for PTG2."""

from __future__ import annotations

import datetime
import hashlib
import html
import json
import os
from dataclasses import asdict, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dateutil.parser import parse as parse_date

from process.ptg_parts.config import PTG2_HASH_MODE_ENV
from process.ptg_parts.domain import PTG2_MONEY_KEYS, PTG2_SET_LIKE_KEYS, PTG2_STRIPPED_QUERY_PARAMS


def sha256_bytes(value: bytes) -> str:
    """Return the hexadecimal SHA-256 digest for raw bytes."""
    return hashlib.sha256(value).hexdigest()


def hash_prefix(value: str, length: int = 16) -> str:
    """Return a stable digest prefix for text."""
    return str(value)[:length]


def normalize_money(value: Any) -> str | None:
    """Normalize a monetary value to canonical decimal text."""
    if value is None:
        return None
    if isinstance(value, float):
        raise TypeError("Float money values are not accepted; use Decimal, int, or string")
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, int):
        decimal_value = Decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            decimal_value = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid money value: {value!r}") from exc
    else:
        raise TypeError(f"Unsupported money value type: {type(value).__name__}")
    if not decimal_value.is_finite():
        raise ValueError("Money value must be finite")
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal(1)), "f")
    return format(normalized, "f")


def money_number(value: Any) -> int | float | None:
    """Convert normalized money text to a numeric value."""
    try:
        normalized = normalize_money(value)
    except (TypeError, ValueError, InvalidOperation):
        return None
    if normalized is None:
        return None
    if "." in normalized:
        return float(normalized)
    return int(normalized)


def normalize_date(value: Any) -> str | None:
    """Normalize supported date values to ISO text."""
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        candidate = text[:10]
        try:
            return datetime.date.fromisoformat(candidate).isoformat()
        except ValueError:
            candidate = ""
    try:
        parsed = parse_date(text)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid date value: {value!r}") from exc
    if isinstance(parsed, datetime.datetime):
        return parsed.date().isoformat()
    return parsed.isoformat()


def _canonical_key(key: Any) -> str:
    return str(key or "").strip().lower()


def _canonical_sort_key(value: Any) -> str:
    normalized = _canonicalize_for_json(value)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _canonicalize_for_json(input_value: Any, key: str | None = None) -> Any:
    key_name = _canonical_key(key)
    if is_dataclass(input_value):
        input_value = asdict(input_value)
    if isinstance(input_value, Enum):
        return input_value.value
    if isinstance(input_value, Decimal):
        return normalize_money(input_value)
    if isinstance(input_value, float) and key_name in PTG2_MONEY_KEYS:
        return normalize_money(input_value)
    if isinstance(input_value, (datetime.date, datetime.datetime)):
        return normalize_date(input_value)
    if isinstance(input_value, tuple):
        return _canonicalize_for_json(list(input_value), key=key)
    if isinstance(input_value, list):
        normalized_list = [
            _canonicalize_for_json(list_entry, key=key)
            for list_entry in input_value
        ]
        if key_name in PTG2_SET_LIKE_KEYS:
            return sorted(normalized_list, key=_canonical_sort_key)
        return normalized_list
    if isinstance(input_value, dict):
        normalized_mapping: dict[str, Any] = {}
        for child_key in sorted(input_value.keys(), key=str):
            child_value = input_value[child_key]
            child_key_text = str(child_key)
            child_key_name = _canonical_key(child_key_text)
            if child_key_name in PTG2_MONEY_KEYS:
                normalized_mapping[child_key_text] = normalize_money(child_value)
            elif child_key_name.endswith("_date") or child_key_name in {"expiration_date", "last_updated_on"}:
                normalized_mapping[child_key_text] = normalize_date(child_value)
            else:
                normalized_mapping[child_key_text] = _canonicalize_for_json(
                    child_value, key=child_key_text
                )
        return normalized_mapping
    return input_value


def canonical_json_dumps(value: Any) -> str:
    """
    Stable JSON representation for PTG2 semantic hashes. None is emitted as
    explicit JSON null; dict keys are sorted; known set-like arrays are sorted.
    """
    return json.dumps(
        _canonicalize_for_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def semantic_hash(value: Any, domain: str | None = None) -> str:
    """Return the configured deterministic hash for canonicalized PTG2 data."""
    payload = {"domain": domain, "payload": _canonicalize_for_json(value)} if domain else _canonicalize_for_json(value)
    payload_bytes = canonical_json_dumps(payload).encode("utf-8")
    mode = str(os.getenv(PTG2_HASH_MODE_ENV, "checksum64")).strip().lower()
    if mode == "sha256":
        return sha256_bytes(payload_bytes)
    if mode in {"blake2", "blake2b", "blake2_128"}:
        return hashlib.blake2b(payload_bytes, digest_size=16).hexdigest()
    checksum = int.from_bytes(hashlib.sha256(payload_bytes).digest()[:8], byteorder="big", signed=False) & (
        (1 << 63) - 1
    )
    return f"{checksum:016x}"


def semantic_sha256(value: Any, domain: str | None = None) -> str:
    """Return a full semantic SHA-256 independent of the compact hash mode."""
    payload = _canonicalize_for_json(value)
    if domain:
        payload = {"domain": domain, "payload": payload}
    payload_bytes = canonical_json_dumps(payload).encode("utf-8")
    return sha256_bytes(payload_bytes)


def canonicalize_url(url: str) -> str:
    """Return a normalized URL with tracking parameters and fragments removed."""
    parsed = urlsplit(str(url).strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if (scheme == "https" and netloc.endswith(":443")) or (scheme == "http" and netloc.endswith(":80")):
        netloc = netloc.rsplit(":", 1)[0]
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in PTG2_STRIPPED_QUERY_PARAMS:
            continue
        query_pairs.append((key, value))
    query = urlencode(sorted(query_pairs), doseq=True)
    return urlunsplit((scheme, netloc, parsed.path or "/", query, ""))


def normalize_tic_source_url(url: str) -> str:
    """Normalize known payer TOC download URLs that point at stale wrappers."""
    raw_url = html.unescape(str(url or "").strip())
    parsed = urlsplit(raw_url)
    if parsed.netloc.lower() == "www.asrhealthbenefits.com":
        path = parsed.path.rstrip("/")
        if path.lower() == "/home/umbraco/surface/mrfdownload/index":
            query_parameter_map = {
                key.lower(): query_value
                for key, query_value in parse_qsl(
                    parsed.query, keep_blank_values=True
                )
            }
            group_number = query_parameter_map.get("g") or query_parameter_map.get(
                "groupnumber"
            )
            file_id = query_parameter_map.get("i") or query_parameter_map.get(
                "fileid"
            )
            file_type = query_parameter_map.get("t") or query_parameter_map.get(
                "filetype"
            )
            if group_number and file_id and file_type:
                return urlunsplit(
                    (
                        parsed.scheme or "https",
                        parsed.netloc,
                        "/umbraco/surface/mrfdownload",
                        urlencode(
                            {
                                "groupNumber": group_number,
                                "fileType": file_type,
                                "fileId": file_id,
                            }
                        ),
                        "",
                    )
                )
    return raw_url


def normalize_import_month(value: str | datetime.date | None) -> datetime.date:
    """Return the first day of the requested import month or the current month."""
    if value is None:
        today = datetime.date.today()
        return datetime.date(today.year, today.month, 1)
    if isinstance(value, datetime.datetime):
        return datetime.date(value.year, value.month, 1)
    if isinstance(value, datetime.date):
        return datetime.date(value.year, value.month, 1)
    normalized = normalize_date(value)
    if normalized is None:
        raise ValueError("import month cannot be blank")
    parsed = datetime.date.fromisoformat(normalized)
    return datetime.date(parsed.year, parsed.month, 1)
