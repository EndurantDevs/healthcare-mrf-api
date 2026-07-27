# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts


OWNER_A = bytes.fromhex("10" * 16)
OWNER_B = bytes.fromhex("20" * 16)
OWNER_C = bytes.fromhex("30" * 16)
MEMBER_A = bytes.fromhex("40" * 16)
MEMBER_B = bytes.fromhex("50" * 16)
MEMBER_C = bytes.fromhex("60" * 16)


def _standard_bytes(
    *,
    owners: tuple[tuple[bytes, tuple[bytes, ...]], ...] = (
        (OWNER_A, (MEMBER_A,)),
        (OWNER_B, (MEMBER_B, MEMBER_C)),
        (OWNER_C, ()),
    ),
    magic: bytes = artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    version: int = artifacts.PTG2_MANIFEST_VERSION,
) -> bytes:
    payload = bytearray(
        artifacts._MEMBERSHIP_HEADER.pack(magic, version, len(owners))
    )
    offset = 0
    for owner, members in owners:
        payload.extend(
            artifacts._MEMBERSHIP_INDEX_RECORD.pack(
                owner, offset, len(members)
            )
        )
        offset += len(members)
    for _owner, members in owners:
        for member in members:
            payload.extend(member)
    return bytes(payload)


def _dense_bytes(
    *,
    owners: tuple[tuple[bytes, tuple[int, ...]], ...] = (
        (OWNER_A, (0,)),
        (OWNER_B, (1, 2)),
        (OWNER_C, ()),
    ),
    members: tuple[bytes, ...] = (MEMBER_A, MEMBER_B, MEMBER_C),
    magic: bytes = artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    version: int = artifacts.PTG2_MANIFEST_VERSION,
) -> bytes:
    encoded_records = bytearray(
        artifacts._DENSE_MEMBERSHIP_HEADER.pack(
            magic, version, len(owners), len(members)
        )
    )
    offset = 0
    for owner, local_ids in owners:
        encoded_records.extend(
            artifacts._MEMBERSHIP_INDEX_RECORD.pack(
                owner, offset, len(local_ids)
            )
        )
        offset += len(local_ids)
    for member in members:
        encoded_records.extend(member)
    for _owner, local_ids in owners:
        for local_id in local_ids:
            encoded_records.extend(
                artifacts._DENSE_MEMBER_RECORD.pack(local_id)
            )
    return bytes(encoded_records)


def _write(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    return path


def _metadata(
    payload: bytes,
    *,
    dense: bool,
    owner_count: int = 3,
    member_count: int = 3,
    member_global_count: int = 3,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "record_format": (
            artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_FORMAT
            if dense
            else artifacts.PTG2_MANIFEST_MEMBERSHIP_FORMAT
        ),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_count": len(payload),
        "owner_count": owner_count,
        "member_count": member_count,
    }
    if dense:
        metadata["member_global_count"] = member_global_count
    return metadata


def test_standard_membership_reader_and_lookup_preserve_order_and_bounds(
    tmp_path: Path,
) -> None:
    payload = _standard_bytes()
    path = _write(tmp_path / "standard.sidecar", payload)
    metadata = _metadata(payload, dense=False)

    entries = artifacts.read_global_sidecar_entries(path, metadata=metadata)

    assert tuple(entry.owner for entry in entries) == (
        OWNER_A,
        OWNER_B,
        OWNER_C,
    )
    assert artifacts.lookup_global_sidecar_members(
        path, OWNER_B, metadata=metadata, max_members=1
    ) == (MEMBER_B,)
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("25" * 16), metadata=metadata
    ) == ()
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("05" * 16), metadata=metadata
    ) == ()
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("35" * 16), metadata=metadata
    ) == ()


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"", "missing its header"),
        (
            _standard_bytes(magic=b"BADMAGIC"),
            "invalid magic header",
        ),
        (
            _standard_bytes(version=2),
            "unsupported global membership sidecar version",
        ),
        (
            artifacts._MEMBERSHIP_HEADER.pack(
                artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, 1
            ),
            "ended inside the owner index",
        ),
        (
            _standard_bytes(
                owners=((OWNER_B, (MEMBER_A,)), (OWNER_A, (MEMBER_B,)))
            ),
            "owners must be sorted and unique",
        ),
        (
            _standard_bytes(owners=((OWNER_A, (MEMBER_B, MEMBER_A)),)),
            "members must be sorted and unique",
        ),
        (
            _standard_bytes(owners=((OWNER_A, (MEMBER_A,)),))[:-1],
            "member block is truncated",
        ),
        (
            _standard_bytes(owners=((OWNER_A, (MEMBER_A,)),)) + b"x",
            "trailing bytes",
        ),
    ),
)
def test_standard_membership_reader_rejects_structural_corruption(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = _write(tmp_path / "standard-corrupt.sidecar", payload)

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.read_global_sidecar_entries(path)


@pytest.mark.parametrize(
    ("metadata_update", "message"),
    (
        ({"owner_count": 4}, "entry count mismatch"),
        ({"member_count": 4}, "member count mismatch"),
        ({"sha256": "00" * 32}, "checksum mismatch"),
        ({"byte_count": 1}, "byte_count mismatch"),
    ),
)
def test_standard_membership_reader_authenticates_manifest_metadata(
    tmp_path: Path,
    metadata_update: dict[str, object],
    message: str,
) -> None:
    payload = _standard_bytes()
    path = _write(tmp_path / "standard-metadata.sidecar", payload)
    metadata = {**_metadata(payload, dense=False), **metadata_update}

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.read_global_sidecar_entries(path, metadata=metadata)


def test_standard_many_lookup_uses_both_cache_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _standard_bytes()
    path = _write(tmp_path / "standard-many.sidecar", payload)
    metadata = _metadata(payload, dense=False)

    assert artifacts.lookup_global_sidecar_members_many(path, []) == {}
    monkeypatch.setattr(
        artifacts, "_is_sidecar_mmap_cache_enabled", lambda: False
    )
    assert artifacts.lookup_global_sidecar_members_many(
        path, (OWNER_A, OWNER_A, OWNER_B), metadata=metadata, max_members=1
    ) == {OWNER_A: (MEMBER_A,), OWNER_B: (MEMBER_B,)}

    monkeypatch.setattr(
        artifacts, "_is_sidecar_mmap_cache_enabled", lambda: True
    )
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", {})
    assert artifacts.lookup_global_sidecar_members_many(
        path, (OWNER_A, OWNER_B), metadata=metadata, max_members=0
    ) == {OWNER_A: (), OWNER_B: ()}


@pytest.mark.parametrize(
    ("payload", "metadata", "message"),
    (
        (b"x", None, "missing its header"),
        (_standard_bytes(magic=b"BADMAGIC"), None, "invalid magic"),
        (_standard_bytes(version=2), None, "unsupported global"),
        (
            _standard_bytes(),
            {"record_format": artifacts.PTG2_MANIFEST_MEMBERSHIP_FORMAT},
            "non-negative byte count",
        ),
        (
            _standard_bytes(),
            {
                "record_format": artifacts.PTG2_MANIFEST_MEMBERSHIP_FORMAT,
                "byte_count": 1,
            },
            "byte_count mismatch",
        ),
    ),
)
def test_single_lookup_rejects_untrusted_sidecar_metadata_and_headers(
    tmp_path: Path,
    payload: bytes,
    metadata: dict[str, object] | None,
    message: str,
) -> None:
    path = _write(tmp_path / "standard-lookup.sidecar", payload)
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.lookup_global_sidecar_members(
            path, OWNER_A, metadata=metadata
        )


def test_dense_membership_reader_and_lookup_resolve_local_dictionary_ids(
    tmp_path: Path,
) -> None:
    payload = _dense_bytes()
    path = _write(tmp_path / "dense.sidecar", payload)
    metadata = _metadata(payload, dense=True)

    entries = artifacts.read_global_sidecar_entries(path, metadata=metadata)

    assert entries[1].members == (MEMBER_B, MEMBER_C)
    assert artifacts.lookup_global_sidecar_members(
        path, OWNER_B, metadata=metadata, max_members=1
    ) == (MEMBER_B,)
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("25" * 16), metadata=metadata
    ) == ()
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("05" * 16), metadata=metadata
    ) == ()
    assert artifacts.lookup_global_sidecar_members(
        path, bytes.fromhex("35" * 16), metadata=metadata
    ) == ()


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (
            artifacts._DENSE_MEMBERSHIP_HEADER.pack(
                artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC, 1, 1, 1
            ),
            "ended inside the dictionary",
        ),
        (
            _dense_bytes(magic=b"BADMAGIC"),
            "invalid magic header",
        ),
        (
            _dense_bytes(version=2),
            "unsupported dense global membership sidecar version",
        ),
        (
            _dense_bytes(
                owners=((OWNER_B, (0,)), (OWNER_A, (1,))),
                members=(MEMBER_A, MEMBER_B),
            ),
            "owners must be sorted and unique",
        ),
        (
            _dense_bytes(
                owners=((OWNER_A, (1, 0)),),
                members=(MEMBER_A, MEMBER_B),
            ),
            "members must be sorted and unique",
        ),
        (
            _dense_bytes(owners=((OWNER_A, (3,)),)),
            "member id is out of range",
        ),
        (
            _dense_bytes(owners=((OWNER_A, (0,)),))[:-1],
            "member block is truncated",
        ),
        (
            _dense_bytes(owners=((OWNER_A, (0,)),)) + b"x",
            "trailing bytes",
        ),
    ),
)
def test_dense_membership_reader_rejects_structural_corruption(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = _write(tmp_path / "dense-corrupt.sidecar", payload)
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.read_global_sidecar_entries(path)


@pytest.mark.parametrize(
    ("metadata_update", "message"),
    (
        ({"owner_count": 4}, "entry count mismatch"),
        ({"member_count": 4}, "member count mismatch"),
        ({"member_global_count": 4}, "dictionary count mismatch"),
    ),
)
def test_dense_membership_reader_authenticates_summary_counts(
    tmp_path: Path,
    metadata_update: dict[str, object],
    message: str,
) -> None:
    payload = _dense_bytes()
    path = _write(tmp_path / "dense-metadata.sidecar", payload)
    metadata = {**_metadata(payload, dense=True), **metadata_update}

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.read_global_sidecar_entries(path, metadata=metadata)


def test_dense_many_lookup_uses_cache_and_uncached_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _dense_bytes()
    path = _write(tmp_path / "dense-many.sidecar", payload)
    metadata = _metadata(payload, dense=True)
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", {})
    for enabled in (False, True):
        monkeypatch.setattr(
            artifacts,
            "_is_sidecar_mmap_cache_enabled",
            lambda enabled=enabled: enabled,
        )
        assert artifacts.lookup_global_sidecar_members_many(
            path, (OWNER_A, OWNER_B), metadata=metadata
        ) == {
            OWNER_A: (MEMBER_A,),
            OWNER_B: (MEMBER_B, MEMBER_C),
        }


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"x", "missing its header"),
        (_dense_bytes(magic=b"BADMAGIC"), "invalid magic"),
        (_dense_bytes(version=2), "unsupported dense"),
        (
            _dense_bytes(owners=((OWNER_A, (3,)),)),
            "member id is out of range",
        ),
    ),
)
def test_dense_single_lookup_fails_closed_on_corruption(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = _write(tmp_path / "dense-lookup.sidecar", payload)
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.lookup_global_sidecar_members(path, OWNER_A)


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"", "invalid magic header"),
        (_standard_bytes(version=2), "unsupported global"),
        (
            artifacts._MEMBERSHIP_HEADER.pack(
                artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC, 1, 1
            ),
            "ended inside the owner index",
        ),
        (
            _standard_bytes(owners=((OWNER_A, (MEMBER_A,)),)) + b"x",
            "member block is misaligned",
        ),
        (
            _dense_bytes(owners=((OWNER_A, (0,)),)) + b"x",
            "member block is misaligned",
        ),
    ),
)
def test_membership_fence_metadata_rejects_unbounded_or_misaligned_files(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = _write(tmp_path / "fence.sidecar", payload)
    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.membership_index_fence_metadata(path)


def test_membership_fence_metadata_rejects_noncanonical_owner_ranges(
    tmp_path: Path,
) -> None:
    for name, payload, message in (
        (
            "unordered",
            _standard_bytes(
                owners=((OWNER_B, (MEMBER_A,)), (OWNER_A, (MEMBER_B,)))
            ),
            "owner index is not ordered",
        ),
        (
            "unowned",
            _standard_bytes(
                owners=((OWNER_A, (MEMBER_A,)),)
            )
            + MEMBER_B,
            "owner member range is invalid|unowned member records",
        ),
    ):
        path = _write(tmp_path / f"{name}.sidecar", payload)
        with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
            artifacts.membership_index_fence_metadata(path)
