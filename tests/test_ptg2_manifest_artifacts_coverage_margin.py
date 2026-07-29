# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import io
import json
import mmap
import struct
from pathlib import Path
from typing import Any, Iterator

import pytest

from process.ptg_parts import ptg2_manifest_artifacts as artifacts


OWNER_A = bytes.fromhex("10" * 16)
OWNER_B = bytes.fromhex("20" * 16)
MEMBER_A = bytes.fromhex("40" * 16)
PRICE_A = bytes.fromhex("70" * 16)


def _serving_payload(
    magic: bytes,
    header: dict[str, object],
    *,
    dictionary: bytes = b"",
    index: bytes = b"",
    body: bytes = b"",
) -> bytes:
    encoded_header = json.dumps(
        header,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return (
        magic
        + struct.pack("<I", len(encoded_header))
        + encoded_header
        + dictionary
        + index
        + body
    )


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


def test_dense_id_and_varint_primitives_enforce_bounds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(artifacts, "_UINT32_MAX", 0)
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="exceeds uint32 capacity",
    ):
        artifacts.build_dense_id_mapping((OWNER_A, OWNER_B))

    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="must be non-negative",
    ):
        artifacts._write_uvarint(io.BytesIO(), -1)

    encoded = io.BytesIO()
    artifacts._write_uvarint(encoded, 129)
    assert encoded.getvalue() == b"\x81\x01"

    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="must be 16 bytes",
    ):
        artifacts._id_text(b"x")


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


def test_existing_serving_sidecar_rejects_unusable_files(
    tmp_path: Path,
) -> None:
    sidecar_lookup = {
        "name": "serving",
        "magic": artifacts.PTG2_SERVING_BY_CODE_MAGIC,
        "expected_format": artifacts.PTG2_SERVING_BY_CODE_FORMAT,
        "kind": artifacts.PTG2_SERVING_BY_CODE_ARTIFACT_KIND,
        "expected_row_count": 1,
    }
    malformed_path = tmp_path / "malformed.bin"
    malformed_path.write_bytes(b"x" * 20)
    assert artifacts._existing_serving_sidecar_path_entry(
        path=malformed_path,
        **sidecar_lookup,
    ) is None

    invalid_count_path = tmp_path / "invalid-count.bin"
    invalid_count_path.write_bytes(
        _serving_payload(
            artifacts.PTG2_SERVING_BY_CODE_MAGIC,
            {
                "format": artifacts.PTG2_SERVING_BY_CODE_FORMAT,
                "row_count": ["invalid"],
            },
        )
    )
    assert artifacts._existing_serving_sidecar_path_entry(
        path=invalid_count_path,
        **sidecar_lookup,
    ) is None

    mismatched_count_path = tmp_path / "mismatched-count.bin"
    mismatched_count_path.write_bytes(
        _serving_payload(
            artifacts.PTG2_SERVING_BY_CODE_MAGIC,
            {
                "format": artifacts.PTG2_SERVING_BY_CODE_FORMAT,
                "row_count": 2,
            },
        )
    )
    assert artifacts._existing_serving_sidecar_path_entry(
        path=mismatched_count_path,
        **sidecar_lookup,
    ) is None


def test_serving_writers_reuse_complete_outputs(
    tmp_path: Path,
) -> None:
    code_path = tmp_path / "by-code.bin"
    provider_path = tmp_path / "by-provider.bin"
    code_rows = ((1, 2, 3, PRICE_A),)
    provider_rows = ((2, 1, 3, PRICE_A),)
    artifacts.write_serving_by_code_sidecar(
        code_path,
        code_rows,
        expected_row_count=1,
    )
    artifacts.write_serving_by_provider_set_sidecar(
        provider_path,
        provider_rows,
        expected_row_count=1,
    )

    def forbidden_rows() -> Iterator[tuple[()]]:
        raise AssertionError("reused sidecars must not consume input rows")
        yield ()

    assert artifacts.write_serving_by_code_sidecar(
        code_path,
        forbidden_rows(),
        expected_row_count=1,
    )["row_count"] == 1
    assert artifacts.write_serving_by_provider_set_sidecar(
        provider_path,
        forbidden_rows(),
        expected_row_count=1,
    )["row_count"] == 1


def test_serving_writers_handle_empty_and_malformed_rows(
    tmp_path: Path,
) -> None:
    assert artifacts.write_serving_by_code_sidecar(
        tmp_path / "empty-code.bin",
        (),
    )["row_count"] == 0
    assert artifacts.write_serving_by_provider_set_sidecar(
        tmp_path / "empty-provider.bin",
        (),
    )["row_count"] == 0

    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="at least four columns",
    ):
        artifacts.write_serving_by_code_sidecar(
            tmp_path / "short-code.bin",
            ((1, 2, 3),),
        )
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="ordered by provider_set_key",
    ):
        artifacts.write_serving_by_code_sidecar(
            tmp_path / "unordered-code.bin",
            ((1, 2, 3, PRICE_A), (1, 1, 3, PRICE_A)),
        )
    with pytest.raises(
        artifacts.PTG2ManifestArtifactError,
        match="at least four columns",
    ):
        artifacts.write_serving_by_provider_set_sidecar(
            tmp_path / "short-provider.bin",
            ((1, 2, 3),),
        )


def test_provider_set_price_dictionary_bounds_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    header_dict = {
        "format": artifacts.PTG2_SERVING_BY_PROVIDER_SET_FORMAT,
        "row_count": 1,
        "provider_set_count": 1,
        "code_count": 1,
        "price_set_count": 1,
        "pattern_count": 1,
    }
    sidecar_bytes = _serving_payload(
        artifacts.PTG2_SERVING_BY_PROVIDER_SET_MAGIC,
        header_dict,
        dictionary=PRICE_A,
        index=artifacts._SERVING_BLOCK_INDEX_RECORD.pack(7, 0, 1),
        body=bytes((1, 9, 1, 2, 1)),
    )
    path = tmp_path / "invalid-price-key.bin"
    path.write_bytes(sidecar_bytes)
    sidecar_map: dict[str, tuple[Any, mmap.mmap, int, int]] = {}
    monkeypatch.setattr(artifacts, "_SIDE_CAR_MMAP_CACHE", sidecar_map)
    try:
        for lookup in (
            artifacts.lookup_serving_by_provider_set_sidecar,
            artifacts.lookup_serving_by_provider_set_patterns,
        ):
            with pytest.raises(
                artifacts.PTG2ManifestArtifactError,
                match="price key is out of range",
            ):
                lookup(path, 7)
    finally:
        _close_mmap_cache(sidecar_map)
