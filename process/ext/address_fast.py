# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fast batch facade for canonical address identity helpers.

The optional Rust/PyO3 module is a throughput optimization only. This facade
keeps the same output shape when the wheel is absent or stale.
"""

from __future__ import annotations

import importlib
import logging
import re
from collections.abc import Iterable, Sequence
from typing import Any

from process.ext import address_canon


logger = logging.getLogger(__name__)

AddressRow = tuple[Any, Any, Any, Any, Any, Any]
_FAST_MODULE: Any | None = None
_FAST_MODULE_CHECKED = False


def _as_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _row_tuple(row: Sequence[Any]) -> AddressRow:
    if len(row) != 6:
        raise ValueError(f"Address canonical batch rows must have 6 fields, got {len(row)}")
    return tuple(_as_optional_text(value) for value in row)


def _zip4_norm(value: str | None) -> str | None:
    digits = re.sub(r"[^0-9]", "", value or "")
    zip4 = digits[5:9]
    return zip4 or None


def _python_canonicalize(row: AddressRow) -> dict[str, str | None]:
    first_line, second_line, city, state, zip_code, country = row
    identity_key = address_canon.identity_key_v1(first_line, second_line, city, state, zip_code, country)
    premise_identity_key = address_canon.premise_identity_key_v1(
        first_line,
        second_line,
        city,
        state,
        zip_code,
        country,
    )
    address_key = address_canon.key_from_identity(identity_key)
    premise_key = address_canon.key_from_identity(premise_identity_key)
    return {
        "address_key": str(address_key) if address_key else None,
        "identity_key": identity_key,
        "premise_key": str(premise_key) if premise_key else None,
        "premise_identity_key": premise_identity_key,
        "line1_norm": address_canon.street_norm(first_line, second_line),
        "unit_norm": address_canon.unit_norm(first_line, second_line),
        "city_norm": address_canon.city_norm(city),
        "state_code": address_canon.state_code(state),
        "zip5": address_canon.zip5_norm(zip_code),
        "zip4": _zip4_norm(zip_code),
        "country_code": address_canon.country_code(country),
    }


def _fast_module() -> Any | None:
    global _FAST_MODULE, _FAST_MODULE_CHECKED
    if _FAST_MODULE_CHECKED:
        return _FAST_MODULE
    _FAST_MODULE_CHECKED = True
    try:
        module = importlib.import_module("ptg2_address_canon")
    except ImportError:
        return None
    try:
        version = module.canon_version()
    except Exception as exc:
        logger.warning("Rust/PyO3 address canonicalizer version check failed; using Python fallback: %s", exc)
        return None
    if not address_canon._canon_version_matches(version):
        logger.warning(
            "Rust/PyO3 address canonicalizer version mismatch; using Python fallback "
            "(rust=%s python=%s)",
            version,
            address_canon.current_canon_version(),
        )
        return None
    _FAST_MODULE = module
    return _FAST_MODULE


def canonicalize_batch(rows: Iterable[Sequence[Any]]) -> list[dict[str, str | None]]:
    prepared = [_row_tuple(row) for row in rows]
    module = _fast_module()
    if module is not None:
        try:
            return list(module.canonicalize_batch(prepared))
        except Exception as exc:
            logger.warning("Rust/PyO3 address canonicalizer failed; using Python fallback: %s", exc)
    return [_python_canonicalize(row) for row in prepared]


def canonicalize_one(row: Sequence[Any]) -> dict[str, str | None]:
    return canonicalize_batch([row])[0]
