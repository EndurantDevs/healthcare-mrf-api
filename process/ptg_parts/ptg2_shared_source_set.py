# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical source-set metadata for reusable strict PTG V3 layouts."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Iterable


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PTG2_V3_SOURCE_SET_CONTRACT = "sorted_raw_container_sha256_bytes_v1"


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


__all__ = ["PTG2_V3_SOURCE_SET_CONTRACT", "shared_source_set_metadata"]
