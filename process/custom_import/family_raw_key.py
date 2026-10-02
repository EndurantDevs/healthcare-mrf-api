# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded raw equality evidence for the sealed-Parquet scalar key domain."""

from __future__ import annotations

import hashlib
from decimal import Decimal

from process.custom_import.definition import canonical_json

RAW_FAMILY_KEY_CONTRACT = "custom-import/raw-family-key/v1"
_PREFIX = canonical_json({"contract": RAW_FAMILY_KEY_CONTRACT, "values": []})[:-2]
_SUFFIX = "]}"


class RawFamilyKeyError(ValueError):
    """A raw key cannot be represented within the admitted scalar/byte domain."""


def raw_family_key_evidence(
    key_values: tuple[object, ...] | None,
    *,
    maximum_canonical_bytes: int,
) -> tuple[str, bytes] | None:
    """Encode the existing raw key after replay and before typed normalization.

    Pass the existing family ``_key``/``_parent_key`` result: missing or
    unhashable values yield ``None`` there. NULL and ordinary unhashable scalar
    mistakes also yield no evidence here. Other adapters and hashable objects
    are outside this contract. The caller must compare full documents within
    a digest bucket; this helper supplies no collision or admission proof.
    """

    if type(maximum_canonical_bytes) is not int or maximum_canonical_bytes <= 0:
        raise RawFamilyKeyError("raw family key byte limit must be a positive integer")
    if key_values is None:
        return None
    if type(key_values) is not tuple or not key_values:
        raise RawFamilyKeyError("raw family key must be a non-empty tuple")
    if any(key_value is None or type(key_value).__hash__ is None for key_value in key_values):
        return None
    byte_count = len(_PREFIX) + len(_SUFFIX)
    fragments: list[str] = []
    for key_value in key_values:
        separator_bytes = int(bool(fragments))
        remaining_bytes = maximum_canonical_bytes - byte_count - separator_bytes
        if remaining_bytes <= 0:
            raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
        token = _value_token(key_value, maximum_bytes=remaining_bytes)
        fragment = canonical_json(token)
        try:
            fragment_bytes = fragment.encode("utf-8")
        except UnicodeEncodeError:
            raise RawFamilyKeyError("raw family key must be valid UTF-8") from None
        byte_count += separator_bytes + len(fragment_bytes)
        if byte_count > maximum_canonical_bytes:
            raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
        fragments.append(fragment)
    canonical = _PREFIX + ",".join(fragments) + _SUFFIX
    digest = hashlib.sha256(RAW_FAMILY_KEY_CONTRACT.encode("ascii") + b":" + canonical.encode("utf-8")).digest()
    return canonical, digest


def _value_token(value: object, *, maximum_bytes: int) -> dict[str, object]:
    """Preserve exact strings and the one shared finite numeric equality class."""

    if type(value) is str:
        if len(value) > maximum_bytes:
            raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
        return {"type": "string", "value": value}
    if type(value) not in (bool, int, Decimal):
        raise RawFamilyKeyError("raw family key contains an unsupported scalar")
    number = value if type(value) is Decimal else Decimal(value)
    if not number.is_finite():
        raise RawFamilyKeyError("raw family key numbers must be finite")
    if number.is_zero():
        return {"type": "number", "sign": 0, "coefficient": "0", "exponent": 0}
    sign, digits, exponent = number.as_tuple()
    significant_end = len(digits)
    while digits[significant_end - 1] == 0:
        significant_end -= 1
    if significant_end > maximum_bytes:
        raise RawFamilyKeyError("raw family key exceeds the canonical byte limit")
    coefficient = "".join(str(digit) for digit in digits[:significant_end])
    return {
        "type": "number",
        "sign": sign,
        "coefficient": coefficient,
        "exponent": exponent + len(digits) - significant_end,
    }
