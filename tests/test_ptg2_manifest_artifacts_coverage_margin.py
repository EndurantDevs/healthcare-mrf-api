# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import mmap
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts


OWNER_A = bytes.fromhex("10" * 16)
OWNER_B = bytes.fromhex("20" * 16)
MEMBER_A = bytes.fromhex("40" * 16)
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
    "invalid_global_id",
    ("0" * 30, "z" * 32, b"x"),
)
def test_global_id_contract_rejects_noncanonical_values(
    invalid_global_id: str | bytes,
) -> None:
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="global id strings|global ids must",
    ):
        artifacts._normalize_global_id(invalid_global_id)


def test_dense_id_mapping_enforces_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(artifacts, "_UINT32_MAX", 0)
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="exceeds uint32 capacity",
    ):
        artifacts.build_dense_id_mapping((OWNER_A, OWNER_B))

def test_manifest_and_membership_schema_errors_fail_closed(
    tmp_path: Path,
) -> None:
    non_object_path = tmp_path / "non-object.json"
    non_object_path.write_text("[]", encoding="utf-8")
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="must be a JSON object",
    ):
        artifacts.read_manifest(non_object_path)

    object_path = tmp_path / "object.json"
    object_path.write_text("{}", encoding="utf-8")
    assert artifacts.read_manifest(
        object_path,
        validate_sidecars=False,
    ) == {}

    artifacts.write_global_membership_sidecar(
        tmp_path,
        "membership",
        {OWNER_A: (MEMBER_A,)},
    )
    manifest_path = tmp_path / "membership.manifest.json"
    manifest = artifacts.read_manifest(manifest_path)
    invalid_cases = (
        ({"version": 2}, "unsupported PTG2 manifest version"),
        ({"artifact_type": "other"}, "unsupported PTG2 artifact type"),
        (
            {
                "sidecars": [
                    {
                        **manifest["sidecars"][0],
                        "record_format": "unsupported",
                    }
                ]
            },
            "unexpected record format",
        ),
    )
    for index, (updates, message) in enumerate(invalid_cases):
        invalid_manifest_dict = {**manifest, **updates}
        invalid_path = tmp_path / f"invalid-{index}.manifest.json"
        artifacts.write_manifest(invalid_path, invalid_manifest_dict)
        with pytest.raises(
            artifacts.PTG2ManifestArtifactError,
            match=message,
        ):
            artifacts.read_global_membership_sidecar(invalid_path)


@pytest.mark.parametrize(
    ("metadata", "message"),
    (
        ({"byte_count": None}, "non-negative byte count"),
        ({"byte_count": 1}, "byte_count mismatch"),
    ),
)
def test_cached_mmap_authenticates_byte_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    metadata: dict[str, object],
    message: str,
) -> None:
    path = tmp_path / "mapped.sidecar"
    path.write_bytes(b"mapped")
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", {})

    with pytest.raises(artifacts.PTG2ManifestArtifactError, match=message):
        artifacts._cached_sidecar_mmap(path, metadata=metadata)


def test_cached_mmap_reopens_changed_files_and_closes_failed_opens(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sidecar_map: dict[str, tuple[Any, mmap.mmap, int, int]] = {}
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", sidecar_map)
    path = tmp_path / "changing.sidecar"
    path.write_bytes(b"first")

    try:
        first_mapping = artifacts._cached_sidecar_mmap(
            path,
            metadata={"byte_count": 5},
        )
        assert bytes(first_mapping) == b"first"

        path.write_bytes(b"second!")
        second_mapping = artifacts._cached_sidecar_mmap(
            path,
            metadata={"byte_count": 7},
        )
        assert bytes(second_mapping) == b"second!"
        assert first_mapping.closed

        empty_path = tmp_path / "empty.sidecar"
        empty_path.write_bytes(b"")
        with pytest.raises(ValueError):
            artifacts._cached_sidecar_mmap(empty_path)
        assert str(empty_path.resolve()) not in sidecar_map
    finally:
        _close_mmap_cache(sidecar_map)
