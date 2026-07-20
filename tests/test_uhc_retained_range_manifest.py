from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import process.uhc_retained_range_manifest as range_manifest
from process.uhc_retained_range_manifest import (
    RANGE_CANONICALIZATION_ID,
    RANGE_CONTRACT_ID,
    RANGE_CONTRACT_VERSION,
    load_verified_range_manifest,
    range_manifest_path,
    retained_raw_path,
)
from process.uhc_retained_types import UHCRetainedAdmissionError
from tests.uhc_retained_registry_test_support import (
    PRODUCER_BUILD_ID,
    records_payload,
    write_retained_fixture,
)


def _fixture(tmp_path: Path):
    return write_retained_fixture(tmp_path, records_payload(), range_count=4)


def _load(fixture, **updates):
    arguments = {
        "raw_path": fixture["raw_path"],
        "manifest_path": fixture["manifest_path"],
        "expected_artifact_sha256": fixture["artifact_sha256"],
        "expected_artifact_bytes": fixture["artifact_byte_count"],
        "expected_manifest_sha256": fixture["manifest_sha256"],
        "expected_manifest_bytes": fixture["manifest_byte_count"],
        "expected_range_count": fixture["range_count"],
        "producer_build_id": fixture["producer_build_id"],
    }
    arguments.update(updates)
    return load_verified_range_manifest(**arguments)


def _rewrite_manifest(fixture, mutation) -> None:
    path = fixture["manifest_path"]
    manifest = json.loads(path.read_text(encoding="ascii"))
    mutation(manifest)
    encoded = json.dumps(
        manifest,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    path.write_bytes(encoded)
    fixture["manifest_sha256"] = hashlib.sha256(encoded).hexdigest()
    fixture["manifest_byte_count"] = len(encoded)


def test_loads_exact_native_range_manifest_and_optional_raw_rehash(tmp_path):
    fixture = _fixture(tmp_path)

    raw_artifact, ranges = _load(fixture)
    raw_artifact_without_second_hash, ranges_without_second_hash = _load(
        fixture,
        verify_raw_bytes=False,
    )

    assert raw_artifact == raw_artifact_without_second_hash
    assert ranges == ranges_without_second_hash
    assert raw_artifact.sha256 == fixture["artifact_sha256"]
    assert raw_artifact.record_count == 16
    assert raw_artifact.range_count == 4
    assert raw_artifact.contract_version == RANGE_CONTRACT_VERSION
    assert raw_artifact.producer_build_id == PRODUCER_BUILD_ID
    assert raw_artifact.canonical_byte_count == sum(
        raw_range.canonical_byte_count for raw_range in ranges
    )
    assert [raw_range.range_ordinal for raw_range in ranges] == list(range(4))
    assert ranges[0].record_start == 0
    assert ranges[-1].record_end == 16


@pytest.mark.parametrize(
    "mutation",
    [
        lambda manifest: manifest.update(extra=True),
        lambda manifest: manifest.pop("range_set_sha256"),
        lambda manifest: manifest.update(contract_id="wrong"),
        lambda manifest: manifest.update(contract_version=True),
        lambda manifest: manifest.update(canonicalization_id="wrong"),
        lambda manifest: manifest.update(producer_build_id="other"),
        lambda manifest: manifest.update(range_count=5),
        lambda manifest: manifest.update(ranges={}),
        lambda manifest: manifest.update(raw_artifact=[]),
        lambda manifest: manifest["raw_artifact"].update(extra=True),
        lambda manifest: manifest["raw_artifact"].update(file_name="other.json"),
        lambda manifest: manifest["raw_artifact"].update(sha256="0" * 64),
        lambda manifest: manifest["raw_artifact"].update(byte_count=0),
        lambda manifest: manifest["raw_artifact"].update(record_count=True),
        lambda manifest: manifest["ranges"].pop(),
        lambda manifest: manifest["ranges"].__setitem__(0, []),
        lambda manifest: manifest["ranges"][0].update(extra=True),
        lambda manifest: manifest["ranges"][0].update(range_ordinal=1),
        lambda manifest: manifest["ranges"][0].update(raw_byte_start=2**63),
        lambda manifest: manifest["ranges"][0].update(raw_byte_end=0),
        lambda manifest: manifest["ranges"][0].update(raw_byte_count=1),
        lambda manifest: manifest["ranges"][0].update(raw_sha256="bad"),
        lambda manifest: manifest["ranges"][0].update(record_start=1),
        lambda manifest: manifest["ranges"][0].update(record_end=0),
        lambda manifest: manifest["ranges"][0].update(record_count=1),
        lambda manifest: manifest["ranges"][0].update(canonical_sha256="bad"),
        lambda manifest: manifest["ranges"][0].update(canonical_byte_count=0),
        lambda manifest: manifest["ranges"][1].update(raw_byte_start=0),
        lambda manifest: manifest["ranges"][-1].update(record_end=15),
        lambda manifest: manifest.update(range_set_sha256="0" * 64),
    ],
)
def test_manifest_mutations_fail_closed(tmp_path, mutation):
    fixture = _fixture(tmp_path)
    _rewrite_manifest(fixture, mutation)

    with pytest.raises(UHCRetainedAdmissionError):
        _load(fixture, verify_raw_bytes=False)


@pytest.mark.parametrize(
    ("producer_build_id", "valid"),
    [
        ("", False),
        ("x" * 257, False),
        ("line\nbreak", False),
        ("build-1", True),
    ],
)
def test_producer_build_id_is_bounded_printable_ascii(
    tmp_path,
    producer_build_id,
    valid,
):
    fixture = _fixture(tmp_path)
    _rewrite_manifest(
        fixture,
        lambda manifest: manifest.update(producer_build_id=producer_build_id),
    )
    if valid:
        _load(fixture, producer_build_id=producer_build_id)
    else:
        with pytest.raises(UHCRetainedAdmissionError):
            _load(fixture, producer_build_id=producer_build_id)


def test_manifest_rejects_duplicate_keys_non_object_and_invalid_utf8(tmp_path):
    fixture = _fixture(tmp_path)
    invalid_payloads = (
        b'{"contract_id":"a","contract_id":"b"}',
        b"[]",
        b"\xff",
    )
    for encoded in invalid_payloads:
        fixture["manifest_path"].write_bytes(encoded)
        fixture["manifest_sha256"] = hashlib.sha256(encoded).hexdigest()
        fixture["manifest_byte_count"] = len(encoded)
        with pytest.raises(UHCRetainedAdmissionError):
            _load(fixture, verify_raw_bytes=False)


def test_manifest_and_raw_exact_hashes_paths_and_regular_files_are_required(
    tmp_path,
):
    fixture = _fixture(tmp_path)
    with pytest.raises(UHCRetainedAdmissionError):
        _load(fixture, expected_manifest_sha256="0" * 64)
    with pytest.raises(UHCRetainedAdmissionError):
        _load(fixture, expected_manifest_bytes=1)
    with pytest.raises(UHCRetainedAdmissionError):
        _load(fixture, expected_artifact_sha256="0" * 64)

    outside_raw = tmp_path / "outside.json"
    outside_raw.write_bytes(fixture["source_bytes"])
    with pytest.raises(UHCRetainedAdmissionError, match="canonical"):
        _load(fixture, raw_path=outside_raw, verify_raw_bytes=False)

    outside_manifest = tmp_path / "other.manifest.json"
    outside_manifest.write_bytes(fixture["manifest_path"].read_bytes())
    with pytest.raises(UHCRetainedAdmissionError, match="canonical"):
        _load(fixture, manifest_path=outside_manifest, verify_raw_bytes=False)

    original_raw = fixture["raw_path"]
    original_raw.unlink()
    original_raw.symlink_to(outside_raw)
    with pytest.raises(UHCRetainedAdmissionError):
        _load(fixture)


def test_manifest_read_is_bounded_and_rejects_symlink(tmp_path):
    fixture = _fixture(tmp_path)
    manifest_path = fixture["manifest_path"]
    manifest_path.write_bytes(b"x" * (range_manifest._MAX_RANGE_MANIFEST_BYTES + 1))
    with pytest.raises(UHCRetainedAdmissionError, match="byte limit"):
        range_manifest._read_manifest_bytes(manifest_path)

    target = tmp_path / "target.json"
    target.write_bytes(b"{}")
    manifest_path.unlink()
    manifest_path.symlink_to(target)
    with pytest.raises(UHCRetainedAdmissionError, match="invalid"):
        range_manifest._read_manifest_bytes(manifest_path)


@pytest.mark.parametrize("range_count", [3, 257, True])
def test_range_paths_reject_unsupported_counts(tmp_path, range_count):
    with pytest.raises(UHCRetainedAdmissionError):
        range_manifest_path(tmp_path, "0" * 64, range_count)


def test_content_addressed_range_paths_are_versioned(tmp_path):
    artifact_sha256 = "a" * 64

    assert retained_raw_path(tmp_path, artifact_sha256).name == (
        f"raw-{artifact_sha256}.json"
    )
    assert range_manifest_path(tmp_path, artifact_sha256, 4).name == (
        f"raw-{artifact_sha256}-ranges-4-v2.manifest.json"
    )
    assert RANGE_CONTRACT_ID.endswith(".v2")
    assert RANGE_CANONICALIZATION_ID.endswith(".v1")
