# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior proofs for immutable multipart PTG rate-file dispatch."""

from __future__ import annotations

import hashlib
from dataclasses import replace

import pytest

from process.ptg_parts.domain import (
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
)
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
    assert_frozen_input_compatibility,
    build_frozen_rate_jobs,
    canonical_frozen_rate_file_set_json,
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
    validate_frozen_artifacts,
    validate_frozen_processed_results,
)

_SHARED_VECTOR_DIGEST = (
    "0dc4d3eb4e5f8ca05820025bd8cc1117a01b8c78292a67e7576fa8a1d425dc72"
)
_SHARED_VECTOR_PREIMAGE = (
    '{"contract":"ptg_frozen_rate_file_set_v1","files":['
    '{"canonical_url":"https://rates.example.test/2026-07_network-rates_0001_of_02.json.gz",'
    '"content_length":101,"engine_source_file_version_id":"aaaabbbbccccdddd",'
    '"engine_source_identity_hash":"1111222233334444","etag":"\\"part-1\\"",'
    '"last_modified":null,"logical_hash_deferred":false,'
    '"logical_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
    '"ordinal":1,'
    '"raw_sha256":"1111111111111111111111111111111111111111111111111111111111111111",'
    '"source_type":"in_network"},'
    '{"canonical_url":"https://rates.example.test/2026-07_network-rates_0002_of_02.json.gz",'
    '"content_length":202,"engine_source_file_version_id":"eeeeffff00001111",'
    '"engine_source_identity_hash":"5555666677778888","etag":"\\"part-2\\"",'
    '"last_modified":null,"logical_hash_deferred":true,"logical_sha256":null,'
    '"ordinal":2,'
    '"raw_sha256":"2222222222222222222222222222222222222222222222222222222222222222",'
    '"source_type":"in_network"}]}'
)


def _descriptor(
    ordinal: int,
    *,
    host: str = "rates.example.com",
    source_type: str = "in_network",
    logical_hash_deferred: bool = False,
) -> dict[str, object]:
    raw_sha256 = hashlib.sha256(f"raw:{ordinal}".encode()).hexdigest()
    logical_sha256 = hashlib.sha256(f"logical:{ordinal}".encode()).hexdigest()
    descriptor_by_field: dict[str, object] = {
        "source_type": source_type,
        "canonical_url": f"https://{host}/2026-07/part-{ordinal:03}.json.gz",
        "content_length": 10_000 + ordinal,
        "etag": f'"part-{ordinal:03}-v1"',
        "last_modified": "Mon, 27 Jul 2026 10:00:00 GMT",
        "raw_sha256": raw_sha256,
        "logical_sha256": None if logical_hash_deferred else logical_sha256,
        "logical_hash_deferred": logical_hash_deferred,
        "engine_source_identity_hash": f"{ordinal:016x}",
        "engine_source_file_version_id": f"{ordinal + 1024:016x}",
        "ordinal": ordinal,
    }
    return descriptor_by_field


def _frozen_set(count: int) -> tuple[list[dict[str, object]], str]:
    files = [_descriptor(ordinal) for ordinal in range(1, count + 1)]
    return files, frozen_rate_file_set_sha256(files)


def test_cross_repo_golden_vector_freezes_exact_json_preimage_and_digest():
    files = [
        {
            "source_type": "in_network",
            "canonical_url": (
                "https://rates.example.test/"
                "2026-07_network-rates_0001_of_02.json.gz"
            ),
            "content_length": 101,
            "etag": '"part-1"',
            "last_modified": None,
            "raw_sha256": "11" * 32,
            "logical_sha256": "aa" * 32,
            "logical_hash_deferred": False,
            "engine_source_identity_hash": "1111222233334444",
            "engine_source_file_version_id": "aaaabbbbccccdddd",
            "ordinal": 1,
        },
        {
            "source_type": "in_network",
            "canonical_url": (
                "https://rates.example.test/"
                "2026-07_network-rates_0002_of_02.json.gz"
            ),
            "content_length": 202,
            "etag": '"part-2"',
            "last_modified": None,
            "raw_sha256": "22" * 32,
            "logical_sha256": None,
            "logical_hash_deferred": True,
            "engine_source_identity_hash": "5555666677778888",
            "engine_source_file_version_id": "eeeeffff00001111",
            "ordinal": 2,
        },
    ]

    assert canonical_frozen_rate_file_set_json(files) == _SHARED_VECTOR_PREIMAGE
    assert frozen_rate_file_set_sha256(files) == _SHARED_VECTOR_DIGEST


@pytest.mark.parametrize("count", [2, 35, 128])
def test_frozen_set_accepts_bounded_complete_cardinalities(count):
    files, digest = _frozen_set(count)

    normalized, normalized_digest = normalize_frozen_rate_file_set(
        list(reversed(files)),
        digest,
    )

    assert len(normalized) == count
    assert [item["ordinal"] for item in normalized] == list(range(1, count + 1))
    assert normalized_digest == digest


def test_frozen_set_digest_is_deterministic_for_shuffled_input():
    files, expected_digest = _frozen_set(35)

    shuffled = files[::2] + files[1::2]

    assert frozen_rate_file_set_sha256(shuffled) == expected_digest
    assert FROZEN_RATE_FILE_SET_CONTRACT == "ptg_frozen_rate_file_set_v1"


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda files: files.__setitem__(1, {**files[1], "ordinal": 3}), "ordinals"),
        (lambda files: files.__setitem__(1, {**files[1], "ordinal": 1}), "ordinals"),
        (lambda files: files.__setitem__(2, {**files[2], "ordinal": 4}), "ordinals"),
    ],
)
def test_frozen_set_rejects_missing_duplicate_or_extra_ordinal(mutator, message):
    files = [_descriptor(1), _descriptor(2), _descriptor(3)]
    mutator(files)

    with pytest.raises(FrozenRateFileValidationError, match=message):
        frozen_rate_file_set_sha256(files)


@pytest.mark.parametrize(
    "replacement",
    [
        {"source_type": "allowed_amounts"},
        {"canonical_url": "https://other.example.net/part-002.json.gz"},
    ],
)
def test_frozen_set_rejects_mixed_type_or_origin(replacement):
    files = [_descriptor(1), {**_descriptor(2), **replacement}]

    with pytest.raises(FrozenRateFileValidationError, match="same source type and HTTPS origin"):
        frozen_rate_file_set_sha256(files)


@pytest.mark.parametrize(
    "duplicate_field",
    [
        "canonical_url",
        "raw_sha256",
        "logical_sha256",
        "engine_source_identity_hash",
        "engine_source_file_version_id",
    ],
)
def test_frozen_set_rejects_duplicate_file_or_content_identity(duplicate_field):
    first = _descriptor(1)
    second = _descriptor(2)
    second[duplicate_field] = first[duplicate_field]

    with pytest.raises(FrozenRateFileValidationError, match="unique"):
        frozen_rate_file_set_sha256([first, second])


@pytest.mark.parametrize(
    "url",
    [
        "http://rates.example.com/part.json.gz",
        "https://rates.example.com/part.json.gz?X-Amz-Signature=secret",
        "https://user:secret@rates.example.com/part.json.gz",
        "https://rates.example.com:444/part.json.gz",
        "https://RATES.example.com/part.json.gz",
    ],
)
def test_frozen_set_rejects_unsafe_signed_or_noncanonical_urls(url):
    files = [{**_descriptor(1), "canonical_url": url}, _descriptor(2)]

    with pytest.raises(FrozenRateFileValidationError, match="canonical query-free HTTPS"):
        frozen_rate_file_set_sha256(files)


@pytest.mark.parametrize(
    "updates",
    [
        {"content_length": None},
        {"content_length": 0},
        {"raw_sha256": None},
        {"raw_sha256": "A" * 64},
        {"logical_sha256": None, "logical_hash_deferred": False},
        {"logical_sha256": "b" * 64, "logical_hash_deferred": True},
        {"etag": 'W/"weak"', "last_modified": None},
        {"etag": None, "last_modified": None},
        {"engine_source_identity_hash": "abc"},
        {"engine_source_file_version_id": "z" * 16},
    ],
)
def test_frozen_set_rejects_missing_or_ambiguous_evidence(updates):
    files = [{**_descriptor(1), **updates}, _descriptor(2)]

    with pytest.raises(FrozenRateFileValidationError):
        frozen_rate_file_set_sha256(files)


def test_frozen_set_rejects_digest_mismatch_in_constant_contract():
    files, _digest = _frozen_set(2)

    with pytest.raises(FrozenRateFileValidationError, match="set SHA-256"):
        normalize_frozen_rate_file_set(files, "f" * 64)


def test_frozen_set_request_cap_is_checked(monkeypatch):
    files, digest = _frozen_set(2)
    monkeypatch.setattr(
        "process.ptg_parts.frozen_rate_files.FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES",
        100,
    )

    with pytest.raises(FrozenRateFileValidationError, match="request-size cap"):
        normalize_frozen_rate_file_set(files, digest)


def test_frozen_jobs_are_exact_and_propagate_plan_network_metadata():
    files, digest = _frozen_set(128)
    normalized, _ = normalize_frozen_rate_file_set(files, digest)
    plan_rows = [{"plan_id": "plan-a", "plan_market_type": "group"}]

    jobs = build_frozen_rate_jobs(
        normalized,
        plan_info=plan_rows,
        source_network_names=["Network A"],
    )

    assert len(jobs) == 128
    assert [job["_ptg_progress_index"] for job in jobs] == list(range(128))
    assert {job["_ptg_progress_total"] for job in jobs} == {128}
    assert all(job["plan_info"] == plan_rows for job in jobs)
    assert all(job["source_network_names"] == ["Network A"] for job in jobs)
    assert all(job["_frozen_rate_file"]["ordinal"] == index for index, job in enumerate(jobs, 1))


def test_frozen_input_is_mutually_exclusive_with_legacy_discovery_inputs():
    files, _digest = _frozen_set(2)

    for incompatible in (
        {"in_network_url": "https://rates.example.com/single.json.gz"},
        {"allowed_url": "https://rates.example.com/allowed.json.gz"},
        {"toc_urls": ["https://rates.example.com/toc.json"]},
        {"toc_list": "/tmp/toc-list"},
        {"file_url_contains": ["part"]},
        {"max_files": 1},
    ):
        with pytest.raises(FrozenRateFileValidationError, match="mutually exclusive"):
            assert_frozen_input_compatibility(files, **incompatible)

    assert_frozen_input_compatibility(files, max_files=2)
    assert_frozen_input_compatibility(None, in_network_url="https://example.com/single")


def _artifacts(
    descriptor: dict[str, object],
    tmp_path,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    raw_path = tmp_path / "raw.json"
    logical_path = tmp_path / "logical.json"
    raw_path.write_bytes(b"raw")
    logical_path.write_bytes(b"logical")
    raw = PTG2RawArtifact(
        original_url=str(descriptor["canonical_url"]),
        canonical_url=str(descriptor["canonical_url"]),
        raw_path=str(raw_path),
        raw_storage_uri=str(raw_path),
        raw_sha256=str(descriptor["raw_sha256"]),
        byte_count=int(descriptor["content_length"]),
        head=PTG2HeadMetadata(
            url=str(descriptor["canonical_url"]),
            status=200,
            etag=str(descriptor["etag"]),
            content_length=int(descriptor["content_length"]),
            last_modified=str(descriptor["last_modified"]),
            supports_head=True,
        ),
    )
    logical = PTG2LogicalArtifact(
        logical_path=str(logical_path),
        logical_sha256=str(descriptor["logical_sha256"]),
        byte_count=20_000,
    )
    return raw, logical


@pytest.mark.parametrize(
    ("raw_change", "logical_change", "message"),
    [
        ({"byte_count": 1}, {}, "body content length"),
        ({"raw_sha256": "f" * 64}, {}, "raw SHA-256"),
        (
            {"head": PTG2HeadMetadata(url="", etag='"other"', content_length=10_001)},
            {},
            "ETag",
        ),
        ({}, {"logical_sha256": "f" * 64}, "logical SHA-256"),
    ],
)
def test_frozen_artifact_mismatch_cleans_fresh_output(
    tmp_path,
    raw_change,
    logical_change,
    message,
):
    descriptor = _descriptor(1)
    raw, logical = _artifacts(descriptor, tmp_path)
    raw = replace(raw, **raw_change)
    logical = replace(logical, **logical_change)

    with pytest.raises(FrozenRateFileMismatchError, match=message):
        validate_frozen_artifacts(descriptor, raw, logical)

    assert not (tmp_path / "raw.json").exists()
    assert not (tmp_path / "logical.json").exists()


def test_frozen_artifact_mismatch_preserves_retained_cache(tmp_path):
    descriptor = _descriptor(1)
    raw, logical = _artifacts(descriptor, tmp_path)
    raw = replace(raw, raw_sha256="f" * 64, reused=True)
    logical = replace(logical, reused=True)

    with pytest.raises(FrozenRateFileMismatchError, match="raw SHA-256"):
        validate_frozen_artifacts(descriptor, raw, logical)

    assert (tmp_path / "raw.json").exists()
    assert (tmp_path / "logical.json").exists()


def test_processed_result_proof_requires_exact_source_version_cardinality():
    files, digest = _frozen_set(2)
    normalized, _ = normalize_frozen_rate_file_set(files, digest)
    results = [
        {
            "source_type": item["source_type"],
            "url": item["canonical_url"],
            "success": True,
            "summary": {
                **item,
                "raw_byte_count": item["content_length"],
                "verification_mode": "downloaded",
            },
        }
        for item in normalized
    ]

    proof = validate_frozen_processed_results(normalized, results)

    assert len(proof) == 2
    assert [item["ordinal"] for item in proof] == [1, 2]
    with pytest.raises(FrozenRateFileMismatchError, match="cardinality"):
        validate_frozen_processed_results(normalized, results[:1])
