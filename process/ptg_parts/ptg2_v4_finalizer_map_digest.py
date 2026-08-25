# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical digest for packed V4 finalizer-map descriptors."""

from __future__ import annotations

import hashlib
import struct
from typing import Any, Iterable, Mapping


_KIND_DOMAIN = b"PTG2V4FINALIZERPACKS\x01"
_ROOT_DOMAIN = b"PTG2V4FINALIZERMAPROOT\x01"
_PACK_DESCRIPTOR = struct.Struct(">IqIqIIQQ32s")


def new_v4_finalizer_kind_digest(object_kind: str) -> Any:
    """Start one ordered object-kind descriptor digest."""

    kind = str(object_kind).encode("utf-8")
    if not kind or len(kind) > 64:
        raise ValueError("packed finalizer object_kind is invalid")
    digest = hashlib.sha256()
    digest.update(_KIND_DOMAIN)
    digest.update(struct.pack(">I", len(kind)))
    digest.update(kind)
    return digest


def update_v4_finalizer_kind_digest(
    digest: Any,
    map_pack: Any,
) -> None:
    """Bind one canonical pack descriptor and its transitive CAS identity."""

    normalized_hash = bytes(map_pack.map_block.block_hash)
    if len(normalized_hash) != 32:
        raise ValueError("packed finalizer map block hash is invalid")
    digest.update(
        _PACK_DESCRIPTOR.pack(
            int(map_pack.pack_no),
            int(map_pack.first_coordinate[0]),
            int(map_pack.first_coordinate[1]),
            int(map_pack.last_coordinate[0]),
            int(map_pack.last_coordinate[1]),
            int(map_pack.coordinate_count),
            int(map_pack.entry_count),
            int(map_pack.logical_byte_count),
            normalized_hash,
        )
    )


def v4_finalizer_map_root_digest(
    kind_digests: Mapping[str, bytes],
    *,
    required_object_kinds: Iterable[str],
) -> bytes:
    """Compose exact per-kind descriptors in the contract's fixed kind order."""

    required_kinds = tuple(map(str, required_object_kinds))
    if len(set(required_kinds)) != len(required_kinds) or set(kind_digests) != set(
        required_kinds
    ):
        raise ValueError("packed finalizer kind digest set is incomplete")
    digest = hashlib.sha256()
    digest.update(_ROOT_DOMAIN)
    digest.update(struct.pack(">I", len(required_kinds)))
    for object_kind in required_kinds:
        kind = object_kind.encode("utf-8")
        kind_digest = bytes(kind_digests[object_kind])
        if len(kind_digest) != 32:
            raise ValueError("packed finalizer kind digest is invalid")
        digest.update(struct.pack(">I", len(kind)))
        digest.update(kind)
        digest.update(kind_digest)
    return digest.digest()


__all__ = (
    "new_v4_finalizer_kind_digest",
    "update_v4_finalizer_kind_digest",
    "v4_finalizer_map_root_digest",
)
