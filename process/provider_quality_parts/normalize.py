# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CSV row normalization helpers for provider-quality imports."""

from __future__ import annotations

import re
from typing import Any

from process.provider_quality_parts.config import MAX_NPI


def _to_float(value: Any) -> float | None:
    if value in (None, "", "*", "NA"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value in (None, "", "*", "NA"):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return None


def _to_npi(value: Any) -> int | None:
    if value in (None, "", "*", "NA"):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit():
        return None
    npi = int(text)
    if npi <= 0 or npi > MAX_NPI:
        return None
    return npi


def _normalize_zcta(value: Any) -> str | None:
    if value in (None, "", "*", "NA"):
        return None
    text = str(value).strip()
    if not text:
        return None
    # Prefer a standalone 5-digit token (e.g. "01001", "ZCTA5 01001", "01001.0").
    zcta_tokens = re.findall(r"(?<!\d)(\d{5})(?!\d)", text)
    if zcta_tokens:
        return zcta_tokens[-1]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 5:
        return digits
    # CDC LOCATION can look like "ZCTA5 01001" => digits "501001".
    if len(digits) == 6 and digits.startswith("5"):
        return digits[1:]
    if len(digits) > 5:
        return digits[-5:]
    return None


def _pick_first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _pick_first_ci(row: dict[str, Any], *keys: str) -> Any:
    lowered = {str(key).strip().lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(str(key).strip().lower())
        if value not in (None, ""):
            return value
    return None
