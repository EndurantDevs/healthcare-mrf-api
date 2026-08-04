# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable public references for atomic PTG negotiated-rate options."""

from __future__ import annotations

import base64
import hashlib
import hmac
from collections.abc import Iterable, Mapping
from typing import Any

from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


RATE_OPTION_REF_PREFIX = "ro1_"
_RATE_OPTION_REF_DOMAIN = b"healthporta.ptg.rate-option-ref.v1\x00"
_LINEAGE_FIELDS = (
    "provider_set_ref",
    "price_set_ref",
    "rate_pack_ref",
)
_LINEAGE_TAGS = (b"\x01", b"\x02", b"\x03")
_LINEAGE_REF_BYTES = 16
_URLSAFE_BASE64_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)


class PTG2RateOptionRefError(PTG2ManifestArtifactError):
    """An atomic rate option cannot produce a stable public reference."""


def _decoded_lineage_ref(component_name: str, value: str) -> bytes:
    if (
        type(value) is not str
        or len(value) != _LINEAGE_REF_BYTES * 2
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise PTG2RateOptionRefError(
            f"rate option {component_name} is invalid"
        )
    return bytes.fromhex(value)


def encode_rate_option_ref(
    *,
    provider_set_ref: str,
    price_set_ref: str,
    rate_pack_ref: str,
) -> str:
    """Hash the exact serving-lineage tuple into a stable opaque reference."""

    component_values = (provider_set_ref, price_set_ref, rate_pack_ref)
    payload = bytearray(_RATE_OPTION_REF_DOMAIN)
    for field_tag, field_name, value in zip(
        _LINEAGE_TAGS,
        _LINEAGE_FIELDS,
        component_values,
        strict=True,
    ):
        payload.extend(field_tag)
        payload.extend(_decoded_lineage_ref(field_name, value))
    digest = hashlib.sha256(bytes(payload)).digest()
    encoded_digest = base64.urlsafe_b64encode(digest).rstrip(b"=")
    return RATE_OPTION_REF_PREFIX + encoded_digest.decode("ascii")


def validate_rate_option_ref(option: Mapping[str, Any]) -> str:
    """Recompute one supplied reference and reject incomplete or tampered data."""

    if not isinstance(option, Mapping):
        raise PTG2RateOptionRefError("rate option record is invalid")
    expected_ref = encode_rate_option_ref(
        provider_set_ref=option.get("provider_set_ref"),
        price_set_ref=option.get("price_set_ref"),
        rate_pack_ref=option.get("rate_pack_ref"),
    )
    supplied_ref = option.get("rate_option_ref")
    has_valid_shape = (
        type(supplied_ref) is str
        and supplied_ref.startswith(RATE_OPTION_REF_PREFIX)
        and len(supplied_ref) == len(RATE_OPTION_REF_PREFIX) + 43
        and all(
            character in _URLSAFE_BASE64_CHARACTERS
            for character in supplied_ref.removeprefix(RATE_OPTION_REF_PREFIX)
        )
    )
    if not has_valid_shape or not hmac.compare_digest(supplied_ref, expected_ref):
        raise PTG2RateOptionRefError("rate option reference is invalid")
    return supplied_ref


def validate_rate_option_ref_consistency(
    options: Iterable[Mapping[str, Any]],
) -> None:
    """Reject one public reference that describes divergent option content."""

    content_by_ref: dict[str, tuple[Any, ...]] = {}
    for option in options:
        option_ref = validate_rate_option_ref(option)
        option_content_values = tuple(
            option.get(field_name) for field_name in _LINEAGE_FIELDS
        )
        option_content_values += (option.get("prices"),)
        previous_content = content_by_ref.setdefault(
            option_ref,
            option_content_values,
        )
        if previous_content != option_content_values:
            raise PTG2RateOptionRefError(
                "rate option reference maps to divergent content"
            )
