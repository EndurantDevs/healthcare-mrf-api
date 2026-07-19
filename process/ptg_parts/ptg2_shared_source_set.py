# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical source-set metadata for reusable strict PTG V3 layouts."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PTG2_V3_SOURCE_SET_CONTRACT = "sorted_raw_container_sha256_bytes_v1"
_ORDERED_SOURCE_ORDINAL_DIGEST_DOMAIN = (
    b"ptg2_v3_ordered_source_ordinal_digest_v1\0"
)


def _normalized_sha256(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"strict shared V3 input is missing a valid {field_name}")
    return normalized


def shared_source_set_metadata(
    raw_container_sha256_values: Iterable[Any],
) -> dict[str, Any]:
    """Seal a complete source set without exposing its individual identities."""

    raw_hashes = sorted(
        _normalized_sha256(value, field_name="raw_container_sha256")
        for value in raw_container_sha256_values
    )
    if not raw_hashes:
        raise ValueError("strict shared V3 source set requires at least one source")
    if len(raw_hashes) != len(set(raw_hashes)):
        raise ValueError("strict shared V3 source set contains duplicate raw containers")
    digest = hashlib.sha256()
    for raw_hash in raw_hashes:
        digest.update(bytes.fromhex(raw_hash))
    return {
        "contract": PTG2_V3_SOURCE_SET_CONTRACT,
        "source_count": len(raw_hashes),
        "raw_container_sha256_digest": digest.hexdigest(),
    }


def ordered_source_ordinal_digest(
    raw_container_sha256_values: Iterable[Any],
) -> str:
    """Bind every dense source ordinal to its exact raw container identity."""

    raw_hashes = tuple(
        _normalized_sha256(value, field_name="raw_container_sha256")
        for value in raw_container_sha256_values
    )
    if not raw_hashes or len(raw_hashes) != len(set(raw_hashes)):
        raise ValueError("strict shared V3 ordered source scope is invalid")
    digest = hashlib.sha256()
    digest.update(_ORDERED_SOURCE_ORDINAL_DIGEST_DOMAIN)
    for source_ordinal, raw_hash in enumerate(raw_hashes):
        digest.update(source_ordinal.to_bytes(4, "big", signed=False))
        digest.update(bytes.fromhex(raw_hash))
    return digest.hexdigest()


__all__ = [
    "PTG2_V3_SOURCE_SET_CONTRACT",
    "ordered_source_ordinal_digest",
    "shared_source_set_metadata",
]
