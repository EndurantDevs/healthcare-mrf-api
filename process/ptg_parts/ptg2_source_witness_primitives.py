# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small framing and validation primitives for strict V3 source witnesses."""

from __future__ import annotations

import struct
from typing import Any, Mapping


U32 = struct.Struct(">I")


def sha256_hex(raw_value: Any, *, field_name: str) -> str:
    """Validate and normalize one SHA-256 hexadecimal value."""

    normalized = str(raw_value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise RuntimeError(f"strict V3 source witness has invalid {field_name}")
    return normalized


def nonnegative_int(
    source_mapping: Mapping[str, Any],
    field_name: str,
    *,
    error_field_name: str,
) -> int:
    """Read one strict nonnegative integer from witness metadata."""

    field_value = source_mapping.get(field_name)
    if type(field_value) is not int or field_value < 0:
        raise RuntimeError(
            f"strict V3 source witness has invalid {error_field_name}"
        )
    return field_value


def read_u32(
    binary_payload: bytes,
    payload_offset: int,
    *,
    field_name: str,
) -> tuple[int, int]:
    """Read a network-order unsigned length with a strict bounds check."""

    if payload_offset + U32.size > len(binary_payload):
        raise RuntimeError(
            f"strict V3 source witness is truncated before {field_name}"
        )
    return (
        U32.unpack_from(binary_payload, payload_offset)[0],
        payload_offset + U32.size,
    )


__all__ = ["U32", "nonnegative_int", "read_u32", "sha256_hex"]
