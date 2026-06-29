# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Canonical phone/fax helpers with an optional Rust/PyO3 fast path."""

from __future__ import annotations

import importlib
import logging
import re
from collections.abc import Iterable, Sequence
from typing import Any


logger = logging.getLogger(__name__)

ContactRow = tuple[Any, Any, Any]
_FAST_MODULE: Any | None = None
_FAST_MODULE_CHECKED = False
_DEFAULT_US_COUNTRIES = {
    "",
    "US",
    "USA",
    "UNITED STATES",
    "UNITEDSTATES",
    "UNITED STATES OF AMERICA",
}


def _as_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _row_tuple(row: Sequence[Any]) -> ContactRow:
    if len(row) != 3:
        raise ValueError(f"Contact canonical batch rows must have 3 fields, got {len(row)}")
    return tuple(_as_optional_text(value) for value in row)  # type: ignore[return-value]


def _digits_only(value: str) -> str:
    return re.sub(r"[^0-9]", "", value)


def _split_extension(value: str) -> tuple[str, str | None]:
    for marker in ("extension", "ext.", "ext", ";ext=", "#", " x ", " x", "x"):
        split = _split_extension_on_marker(value, marker)
        if split:
            return split
    return value, None


def _split_extension_on_marker(value: str, marker: str) -> tuple[str, str] | None:
    lower = value.lower()
    index = lower.rfind(marker)
    if index < 0:
        return None
    if marker == "x":
        previous = value[index - 1] if index > 0 else None
        if previous is not None and not (previous.isdigit() or previous == ")" or previous.isspace()):
            return None
    main = value[:index].rstrip()
    suffix = value[index + len(marker) :].strip()
    if len(_digits_only(main)) < 7:
        return None
    extension = _digits_only(suffix)
    if not extension or len(extension) > 16:
        return None
    if any(ch.isalpha() for ch in suffix):
        return None
    return main, extension


def _empty_contact() -> dict[str, str | bool | None]:
    return {
        "number": None,
        "extension": None,
        "is_international": False,
        "valid_for_fallback": False,
    }


def _python_canonicalize_number(raw: str | None, country_code: str | None) -> dict[str, str | bool | None]:
    if raw is None:
        return _empty_contact()
    text = raw.strip()
    if not text:
        return _empty_contact()

    main_text, extension = _split_extension(text)
    digits = _digits_only(main_text)
    if not digits:
        return _empty_contact()

    country = (country_code or "").strip().upper()
    explicit_international = main_text.lstrip().startswith("+")
    default_us = country in _DEFAULT_US_COUNTRIES

    if len(digits) == 10 and default_us:
        return {
            "number": digits,
            "extension": extension,
            "is_international": False,
            "valid_for_fallback": True,
        }
    if len(digits) == 11 and digits.startswith("1") and default_us:
        return {
            "number": digits[1:],
            "extension": extension,
            "is_international": False,
            "valid_for_fallback": True,
        }
    if explicit_international and 8 <= len(digits) <= 15:
        return {
            "number": digits,
            "extension": extension,
            "is_international": True,
            "valid_for_fallback": False,
        }
    return _empty_contact()


def _python_canonicalize(row: ContactRow) -> dict[str, str | bool | None]:
    phone_raw, fax_raw, country_code = row
    phone = _python_canonicalize_number(phone_raw, country_code)
    fax = _python_canonicalize_number(fax_raw, country_code)
    return {
        "phone_number": phone["number"],
        "phone_extension": phone["extension"],
        "phone_is_international": phone["is_international"],
        "phone_valid_for_fallback": phone["valid_for_fallback"],
        "fax_number": fax["number"],
        "fax_number_digits": fax["number"],
        "fax_extension": fax["extension"],
        "fax_is_international": fax["is_international"],
        "fax_valid_for_fallback": fax["valid_for_fallback"],
    }


def _fast_module() -> Any | None:
    global _FAST_MODULE, _FAST_MODULE_CHECKED  # pylint: disable=global-statement
    if _FAST_MODULE_CHECKED:
        return _FAST_MODULE
    _FAST_MODULE_CHECKED = True
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    if not hasattr(module, "canonicalize_contact_batch"):
        return None
    _FAST_MODULE = module
    return _FAST_MODULE


def canonicalize_batch(rows: Iterable[Sequence[Any]]) -> list[dict[str, str | bool | None]]:
    prepared = [_row_tuple(row) for row in rows]
    module = _fast_module()
    if module is not None:
        try:
            return list(module.canonicalize_contact_batch(prepared))
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Rust/PyO3 contact canonicalizer failed; using Python fallback: %s", exc)
    return [_python_canonicalize(row) for row in prepared]


def canonicalize_one(row: Sequence[Any]) -> dict[str, str | bool | None]:
    return canonicalize_batch([row])[0]
