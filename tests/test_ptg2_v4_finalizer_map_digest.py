# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    new_v4_finalizer_kind_digest,
    update_v4_finalizer_kind_digest,
    v4_finalizer_map_root_digest,
)


def _map_pack(*, pack_no: int = 0, map_block_hash: bytes = b"m" * 32):
    return SimpleNamespace(
        pack_no=pack_no,
        first_coordinate=(1, 0),
        last_coordinate=(2, 0),
        coordinate_count=2,
        entry_count=3,
        logical_byte_count=4,
        map_block=SimpleNamespace(block_hash=map_block_hash),
    )


def _kind_digest(object_kind: str, *, pack_no: int = 0) -> bytes:
    digest = new_v4_finalizer_kind_digest(object_kind)
    update_v4_finalizer_kind_digest(digest, _map_pack(pack_no=pack_no))
    return digest.digest()


def test_finalizer_map_digest_binds_order_and_pack_identity():
    kinds = ("a", "b")
    digest_by_kind = {kind: _kind_digest(kind) for kind in kinds}

    canonical = v4_finalizer_map_root_digest(
        digest_by_kind,
        required_object_kinds=kinds,
    )

    assert canonical != v4_finalizer_map_root_digest(
        digest_by_kind,
        required_object_kinds=reversed(kinds),
    )
    assert digest_by_kind["a"] != _kind_digest("a", pack_no=1)


def test_finalizer_map_digest_rejects_incomplete_or_invalid_contract():
    with pytest.raises(ValueError, match="incomplete"):
        v4_finalizer_map_root_digest(
            {"a": _kind_digest("a")},
            required_object_kinds=("a", "b"),
        )
    with pytest.raises(ValueError, match="object_kind"):
        new_v4_finalizer_kind_digest("")
    with pytest.raises(ValueError, match="block hash"):
        update_v4_finalizer_kind_digest(
            new_v4_finalizer_kind_digest("a"),
            _map_pack(map_block_hash=b"short"),
        )
