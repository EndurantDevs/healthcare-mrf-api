# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small row-normalization helpers shared by PTG import paths."""

from __future__ import annotations

import datetime
import hashlib
from contextlib import suppress
from typing import Any

from dateutil.parser import parse as parse_date

from process.ptg_parts.canonical import canonical_json_dumps

NPI_MIN = 1_000_000_000
NPI_MAX = 9_999_999_999


def _make_checksum(*values: Any) -> int:
    digest = hashlib.sha256(canonical_json_dumps(list(values)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & ((1 << 63) - 1)


def _coerce_date(value: Any) -> datetime.date | None:
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        with suppress(ValueError):
            return datetime.date.fromisoformat(text[:10])
    try:
        parsed = parse_date(text)
    except (ValueError, TypeError):
        return None
    if isinstance(parsed, datetime.datetime):
        return parsed.date()
    return parsed


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalized_modifier_list(value: Any) -> list[str]:
    modifiers: set[str] = set()
    for item in _as_list(value):
        if item is None:
            continue
        for part in str(item).split(","):
            modifier = part.strip().upper()
            if modifier:
                modifiers.add(modifier)
    return sorted(modifiers)


def _as_int_list(value: Any) -> list[int]:
    integer_values: list[int] = []
    for item in _as_list(value):
        try:
            integer_values.append(int(str(item).strip()))
        except (TypeError, ValueError):
            continue
    return integer_values


def _is_valid_npi(value: int) -> bool:
    return NPI_MIN <= int(value) <= NPI_MAX


def _normalized_npi_list(value: Any) -> list[int]:
    return sorted({number for number in _as_int_list(value) if _is_valid_npi(number)})


def _normalize_tin_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_tin_value(value: Any) -> str:
    text = str(value or "").strip().upper()
    return "".join(ch for ch in text if ch.isalnum())


def _provider_group_identity_hash(tin_info: dict[str, Any] | None, npi_list: Any) -> int:
    tin_info = tin_info or {}
    return _make_checksum(
        "provider_group",
        _normalize_tin_type(tin_info.get("type")),
        _normalize_tin_value(tin_info.get("value")),
        _normalized_npi_list(npi_list),
    )


def _provider_group_hash_prefix(provider_group_hash: int) -> str:
    return f"{int(provider_group_hash):016x}"[:16]


def _normalize_code_component(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text.upper() if text else None
