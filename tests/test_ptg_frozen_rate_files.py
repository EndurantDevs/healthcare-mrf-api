# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contract proofs for immutable multipart PTG rate-file dispatch."""

from __future__ import annotations

import hashlib

import pytest

from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_SET_CONTRACT,
    FrozenRateFileValidationError,
    assert_frozen_input_compatibility,
    build_frozen_rate_jobs,
    canonical_frozen_rate_file_set_json,
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
)
from tests.ptg_frozen_test_support import (
    frozen_descriptor_by_ordinal,
    frozen_rate_file_set,
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
    descriptor_by_field = frozen_descriptor_by_ordinal(ordinal)
    descriptor_by_field["canonical_url"] = (
        f"https://{host}/2026-07/part-{ordinal:03}.json.gz"
    )
    descriptor_by_field["source_type"] = source_type
    descriptor_by_field["logical_hash_deferred"] = logical_hash_deferred
    if logical_hash_deferred:
        descriptor_by_field["logical_sha256"] = None
    return descriptor_by_field


def test_cross_repo_golden_vector_freezes_json_preimage_and_digest():
    frozen_rate_files = [
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

    assert (
        canonical_frozen_rate_file_set_json(frozen_rate_files)
        == _SHARED_VECTOR_PREIMAGE
    )
    assert (
        frozen_rate_file_set_sha256(frozen_rate_files)
        == _SHARED_VECTOR_DIGEST
    )


@pytest.mark.parametrize("count", [2, 35, 128])
def test_frozen_set_accepts_bounded_complete_cardinalities(count):
    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(count)

    normalized_files, normalized_digest = normalize_frozen_rate_file_set(
        list(reversed(frozen_rate_files)),
        frozen_set_digest,
    )

    assert len(normalized_files) == count
    assert [
        descriptor["ordinal"] for descriptor in normalized_files
    ] == list(range(1, count + 1))
    assert normalized_digest == frozen_set_digest


def test_frozen_set_digest_is_deterministic_for_shuffled_input():
    frozen_rate_files, expected_digest = frozen_rate_file_set(35)
    shuffled_files = (
        frozen_rate_files[::2] + frozen_rate_files[1::2]
    )

    assert frozen_rate_file_set_sha256(shuffled_files) == expected_digest
    assert FROZEN_RATE_FILE_SET_CONTRACT == "ptg_frozen_rate_file_set_v1"


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda rate_files: rate_files.__setitem__(
                1,
                {**rate_files[1], "ordinal": 3},
            ),
            "ordinals",
        ),
        (
            lambda rate_files: rate_files.__setitem__(
                1,
                {**rate_files[1], "ordinal": 1},
            ),
            "ordinals",
        ),
        (
            lambda rate_files: rate_files.__setitem__(
                2,
                {**rate_files[2], "ordinal": 4},
            ),
            "ordinals",
        ),
    ],
)
def test_frozen_set_rejects_non_dense_ordinals(mutator, message):
    frozen_rate_files = [
        _descriptor(1),
        _descriptor(2),
        _descriptor(3),
    ]
    mutator(frozen_rate_files)

    with pytest.raises(FrozenRateFileValidationError, match=message):
        frozen_rate_file_set_sha256(frozen_rate_files)


def test_frozen_set_rejects_mixed_origin():
    frozen_rate_files = [
        _descriptor(1),
        {
            **_descriptor(2),
            "canonical_url": "https://other.example.net/part-002.json.gz",
        },
    ]

    with pytest.raises(
        FrozenRateFileValidationError,
        match="same source type and HTTPS origin",
    ):
        frozen_rate_file_set_sha256(frozen_rate_files)


def test_frozen_multipart_rejects_allowed_amounts_before_work():
    frozen_rate_files = [
        _descriptor(1, source_type="allowed_amounts"),
        _descriptor(2, source_type="allowed_amounts"),
    ]

    with pytest.raises(
        FrozenRateFileValidationError,
        match="only in_network",
    ):
        frozen_rate_file_set_sha256(frozen_rate_files)


@pytest.mark.parametrize(
    ("field_name", "invalid_length"),
    [
        ("engine_source_identity_hash", 15),
        ("engine_source_identity_hash", 17),
        ("engine_source_identity_hash", 31),
        ("engine_source_identity_hash", 33),
        ("engine_source_identity_hash", 63),
        ("engine_source_identity_hash", 65),
        ("engine_source_file_version_id", 15),
        ("engine_source_file_version_id", 17),
        ("engine_source_file_version_id", 31),
        ("engine_source_file_version_id", 33),
        ("engine_source_file_version_id", 63),
        ("engine_source_file_version_id", 65),
    ],
)
def test_frozen_set_rejects_other_engine_id_widths(
    field_name,
    invalid_length,
):
    frozen_rate_files = [
        {
            **_descriptor(1),
            field_name: "a" * invalid_length,
        },
        _descriptor(2),
    ]

    with pytest.raises(FrozenRateFileValidationError):
        frozen_rate_file_set_sha256(frozen_rate_files)


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
def test_frozen_set_rejects_duplicate_file_identity(duplicate_field):
    first_descriptor = _descriptor(1)
    second_descriptor = _descriptor(2)
    second_descriptor[duplicate_field] = first_descriptor[duplicate_field]

    with pytest.raises(FrozenRateFileValidationError, match="unique"):
        frozen_rate_file_set_sha256(
            [first_descriptor, second_descriptor]
        )


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
def test_frozen_set_rejects_unsafe_or_noncanonical_urls(url):
    frozen_rate_files = [
        {**_descriptor(1), "canonical_url": url},
        _descriptor(2),
    ]

    with pytest.raises(
        FrozenRateFileValidationError,
        match="canonical query-free HTTPS",
    ):
        frozen_rate_file_set_sha256(frozen_rate_files)


@pytest.mark.parametrize(
    "updates_by_field",
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
def test_frozen_set_rejects_missing_or_ambiguous_evidence(
    updates_by_field,
):
    frozen_rate_files = [
        {**_descriptor(1), **updates_by_field},
        _descriptor(2),
    ]

    with pytest.raises(FrozenRateFileValidationError):
        frozen_rate_file_set_sha256(frozen_rate_files)


def test_frozen_set_rejects_digest_mismatch():
    frozen_rate_files, _frozen_set_digest = frozen_rate_file_set(2)

    with pytest.raises(
        FrozenRateFileValidationError,
        match="set SHA-256",
    ):
        normalize_frozen_rate_file_set(frozen_rate_files, "f" * 64)


def test_frozen_set_request_cap_is_checked(monkeypatch):
    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(2)
    monkeypatch.setattr(
        "process.ptg_parts.frozen_rate_files."
        "FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES",
        100,
    )

    with pytest.raises(
        FrozenRateFileValidationError,
        match="request-size cap",
    ):
        normalize_frozen_rate_file_set(
            frozen_rate_files,
            frozen_set_digest,
        )


def test_frozen_jobs_propagate_plan_network_metadata():
    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(128)
    normalized_files, _ = normalize_frozen_rate_file_set(
        frozen_rate_files,
        frozen_set_digest,
    )
    plan_rows = [{"plan_id": "plan-a", "plan_market_type": "group"}]

    frozen_jobs = build_frozen_rate_jobs(
        normalized_files,
        plan_info=plan_rows,
        source_network_names=["Network A"],
    )

    assert len(frozen_jobs) == 128
    assert [
        frozen_job["_ptg_progress_index"] for frozen_job in frozen_jobs
    ] == list(range(128))
    assert {
        frozen_job["_ptg_progress_total"] for frozen_job in frozen_jobs
    } == {128}
    assert all(
        frozen_job["plan_info"] == plan_rows
        for frozen_job in frozen_jobs
    )
    assert all(
        frozen_job["source_network_names"] == ["Network A"]
        for frozen_job in frozen_jobs
    )
    assert all(
        frozen_job["_frozen_rate_file"]["ordinal"] == ordinal
        for ordinal, frozen_job in enumerate(frozen_jobs, 1)
    )


def test_frozen_input_is_mutually_exclusive_with_discovery_inputs():
    frozen_rate_files, _frozen_set_digest = frozen_rate_file_set(2)
    incompatible_options = (
        {"in_network_url": "https://rates.example.com/single.json.gz"},
        {"allowed_url": "https://rates.example.com/allowed.json.gz"},
        {"toc_urls": ["https://rates.example.com/toc.json"]},
        {"toc_list": "/tmp/toc-list"},
        {"file_url_contains": ["part"]},
        {"max_files": 1},
    )

    for incompatible_options_by_name in incompatible_options:
        with pytest.raises(
            FrozenRateFileValidationError,
            match="mutually exclusive",
        ):
            assert_frozen_input_compatibility(
                frozen_rate_files,
                **incompatible_options_by_name,
            )

    assert_frozen_input_compatibility(frozen_rate_files, max_files=2)
    assert_frozen_input_compatibility(
        None,
        in_network_url="https://example.com/single",
    )
