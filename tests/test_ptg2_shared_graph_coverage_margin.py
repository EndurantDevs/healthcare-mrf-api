# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import mmap
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts


OWNER_A = bytes.fromhex("10" * 16)
OWNER_B = bytes.fromhex("20" * 16)
MEMBER_A = bytes.fromhex("40" * 16)


def _standard_payload(
    *,
    magic: bytes = artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC,
    version: int = artifacts.PTG2_MANIFEST_VERSION,
    include_index: bool = True,
    include_member: bool = True,
) -> bytes:
    payload = bytearray(artifacts._MEMBERSHIP_HEADER.pack(magic, version, 1))
    if include_index:
        payload.extend(
            artifacts._MEMBERSHIP_INDEX_RECORD.pack(OWNER_A, 0, 1)
        )
    if include_member:
        payload.extend(MEMBER_A)
    return bytes(payload)


def _dense_payload(
    *,
    magic: bytes = artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC,
    version: int = artifacts.PTG2_MANIFEST_VERSION,
    entry_count: int = 1,
    member_global_count: int = 1,
    include_index: bool = True,
    include_dictionary: bool = True,
    include_member: bool = True,
) -> bytes:
    payload = bytearray(
        artifacts._DENSE_MEMBERSHIP_HEADER.pack(
            magic,
            version,
            entry_count,
            member_global_count,
        )
    )
    if include_index:
        payload.extend(
            artifacts._MEMBERSHIP_INDEX_RECORD.pack(OWNER_A, 0, 1)
        )
    if include_dictionary:
        payload.extend(MEMBER_A)
    if include_member:
        payload.extend(artifacts._DENSE_MEMBER_RECORD.pack(0))
    return bytes(payload)


@contextmanager
def _mapped_file(
    path: Path,
    payload: bytes,
) -> Iterator[mmap.mmap]:
    path.write_bytes(payload)
    with path.open("rb") as sidecar_file:
        with mmap.mmap(
            sidecar_file.fileno(),
            0,
            access=mmap.ACCESS_READ,
        ) as mapped:
            yield mapped


def _close_mmap_cache(
    sidecar_map: dict[str, tuple[Any, mmap.mmap, int, int]],
) -> None:
    for sidecar_file, mapped, _size, _mtime_ns in tuple(
        sidecar_map.values()
    ):
        mapped.close()
        sidecar_file.close()
    sidecar_map.clear()


@pytest.mark.parametrize(
    ("payload", "metadata", "message"),
    (
        (
            artifacts._MEMBERSHIP_HEADER.pack(
                artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC,
                1,
                0,
            ),
            {
                "byte_count": (
                    artifacts.PTG2_MANIFEST_MEMBERSHIP_HEADER_SIZE
                ),
                "owner_count": 1,
            },
            "entry count mismatch",
        ),
        (
            _standard_payload(include_index=False, include_member=False),
            None,
            "ended inside the owner index",
        ),
        (
            _standard_payload(include_member=False),
            None,
            "member block is truncated",
        ),
    ),
)
def test_single_membership_lookup_rejects_geometry_mismatches(
    tmp_path: Path,
    payload: bytes,
    metadata: dict[str, object] | None,
    message: str,
) -> None:
    path = tmp_path / f"single-{message.split()[0]}.bin"
    path.write_bytes(payload)

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts.lookup_global_sidecar_members(
            path,
            OWNER_A,
            metadata=metadata,
        )


def test_many_standard_lookup_rejects_cached_header_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sidecar_map: dict[str, tuple[Any, mmap.mmap, int, int]] = {}
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", sidecar_map)
    monkeypatch.setattr(
        artifacts,
        "_is_sidecar_mmap_cache_enabled",
        lambda: True,
    )
    try:
        for name, sidecar_bytes, message in (
            ("short", b"x", "missing its header"),
            (
                "magic",
                _standard_payload(magic=b"BADMAGIC"),
                "invalid magic header",
            ),
        ):
            path = tmp_path / f"cached-{name}.bin"
            path.write_bytes(sidecar_bytes)
            with pytest.raises(
                artifacts.PTG2ManifestArtifactError,
                match=message,
            ):
                artifacts.lookup_global_sidecar_members_many(
                    path,
                    (OWNER_A,),
                )
    finally:
        _close_mmap_cache(sidecar_map)


@pytest.mark.parametrize(
    ("name", "sidecar_bytes", "metadata", "message"),
    (
        ("short", b"x", None, "missing its header"),
        (
            "magic",
            _standard_payload(magic=b"BADMAGIC"),
            None,
            "invalid magic header",
        ),
        (
            "version",
            _standard_payload(version=2),
            None,
            "unsupported global membership sidecar version",
        ),
        (
            "count",
            _standard_payload(),
            {"owner_count": 2},
            "entry count mismatch",
        ),
        (
            "index",
            _standard_payload(include_index=False, include_member=False),
            None,
            "ended inside the owner index",
        ),
        (
            "member",
            _standard_payload(include_member=False),
            None,
            "member block is truncated",
        ),
    ),
)
def test_many_standard_lookup_rejects_geometry_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    sidecar_bytes: bytes,
    metadata: dict[str, object] | None,
    message: str,
) -> None:
    monkeypatch.setattr(
        artifacts,
        "_is_sidecar_mmap_cache_enabled",
        lambda: False,
    )
    path = tmp_path / f"uncached-{name}.bin"
    path.write_bytes(sidecar_bytes)
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match=message,
    ):
        artifacts.lookup_global_sidecar_members_many(
            path,
            (OWNER_A,),
            metadata=metadata,
        )


def test_many_standard_lookup_handles_absence_and_private_magic(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        artifacts,
        "_is_sidecar_mmap_cache_enabled",
        lambda: False,
    )
    valid_path = tmp_path / "uncached-valid.bin"
    valid_path.write_bytes(_standard_payload())
    assert artifacts.lookup_global_sidecar_members_many(
        valid_path,
        (OWNER_B,),
    ) == {OWNER_B: ()}

    with _mapped_file(
        tmp_path / "private-standard.bin",
        _standard_payload(magic=b"BADMAGIC"),
    ) as mapped:
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match="invalid magic header",
        ):
            artifacts._lookup_standard_sidecar_members(mapped, OWNER_A)


def test_dense_readers_reject_header_errors(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="missing its header",
    ):
        artifacts._read_dense_sidecar_entries(b"x")
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="invalid magic header",
    ):
        artifacts._read_dense_sidecar_entries(
            _dense_payload(magic=b"BADMAGIC")
        )

    with _mapped_file(tmp_path / "dense-short.bin", b"x") as mapped:
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match="missing its header",
        ):
            artifacts._lookup_dense_sidecar_members(mapped, OWNER_A)
    with _mapped_file(
        tmp_path / "dense-magic.bin",
        _dense_payload(magic=b"BADMAGIC"),
    ) as mapped:
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match="invalid magic header",
        ):
            artifacts._lookup_dense_sidecar_members(mapped, OWNER_A)


def test_dense_lookup_rejects_dictionary_geometry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        artifacts,
        "_is_sidecar_mmap_cache_enabled",
        lambda: False,
    )
    dense_cases = (
        (
            "version",
            _dense_payload(version=2),
            None,
            "unsupported dense global membership sidecar version",
        ),
        (
            "entry-count",
            _dense_payload(),
            {"owner_count": 2},
            "entry count mismatch",
        ),
        (
            "dictionary-count",
            _dense_payload(),
            {"member_global_count": 2},
            "dictionary count mismatch",
        ),
        (
            "dictionary",
            _dense_payload(
                entry_count=0,
                member_global_count=1,
                include_index=False,
                include_dictionary=False,
                include_member=False,
            ),
            None,
            "ended inside the dictionary",
        ),
        (
            "member",
            _dense_payload(include_member=False),
            None,
            "member block is truncated",
        ),
    )
    for name, sidecar_bytes, metadata, message in dense_cases:
        path = tmp_path / f"dense-{name}.bin"
        path.write_bytes(sidecar_bytes)
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match=message,
        ):
            artifacts.lookup_global_sidecar_members_many(
                path,
                (OWNER_A,),
                metadata=metadata,
            )


def test_manifest_sidecar_paths_and_metadata_are_strict(
    tmp_path: Path,
) -> None:
    for resolver in (
        artifacts._mapping_sidecar,
        artifacts._membership_sidecar,
    ):
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match="does not include",
        ):
            resolver({})

    for sidecar in (
        {},
        {"path": "/absolute.bin"},
        {"path": "../outside.bin"},
    ):
        with pytest.raises(artifacts.PTG2ManifestArtifactError):
            artifacts._sidecar_path(tmp_path, sidecar)

    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="must be JSON objects",
    ):
        artifacts._validate_sidecars(tmp_path, {"sidecars": ["invalid"]})

    sidecar_path = tmp_path / "sidecar.bin"
    sidecar_path.write_bytes(b"sidecar")
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="SHA-256 checksum",
    ):
        artifacts._validate_sidecar_metadata(
            sidecar_path,
            {"sha256": "short", "byte_count": 7},
        )
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="non-negative byte count",
    ):
        artifacts._validate_sidecar_metadata(
            sidecar_path,
            {"sha256": hashlib.sha256(b"sidecar").hexdigest()},
        )

    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="unexpected record format",
    ):
        artifacts._validate_membership_record_format(
            {"record_format": "unsupported"}
        )


def test_membership_fences_reject_partial_headers_and_owner_ranges(
    tmp_path: Path,
) -> None:
    for name, magic in (
        ("dense", artifacts.PTG2_MANIFEST_DENSE_MEMBERSHIP_MAGIC),
        ("standard", artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC),
    ):
        path = tmp_path / f"{name}-partial.bin"
        path.write_bytes(magic + b"\x00")
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match="missing its header",
        ):
            artifacts.membership_index_fence_metadata(path)

    invalid_range_path = tmp_path / "invalid-range.bin"
    invalid_range_path.write_bytes(
        artifacts._MEMBERSHIP_HEADER.pack(
            artifacts.PTG2_MANIFEST_MEMBERSHIP_MAGIC,
            artifacts.PTG2_MANIFEST_VERSION,
            1,
        )
        + artifacts._MEMBERSHIP_INDEX_RECORD.pack(OWNER_A, 1, 0)
    )
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="owner member range is invalid",
    ):
        artifacts.membership_index_fence_metadata(invalid_range_path)


def test_v3_graph_contract_requires_at_least_one_observed_shard() -> None:
    serving_index = {
        "arch_version": "postgres_binary_v3",
        "provider_membership_graph": {
            "artifact_version": (
                artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION
            ),
            "artifact_names": sorted(
                artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_ARTIFACT_NAMES
            ),
            "storage": "postgresql_chunks_v1",
        },
        "artifacts": {},
    }

    assert artifacts.v3_graph_contract_errors(serving_index) == [
        artifacts.PTG2_PROVIDER_MEMBERSHIP_GRAPH_VERSION
    ]
