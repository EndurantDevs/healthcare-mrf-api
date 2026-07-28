# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Hash-mode and cross-service digest proofs for frozen multipart PTG."""

from __future__ import annotations

import pytest

from process.ptg_parts.canonical import semantic_hash
from process.ptg_parts.frozen_rate_files import (
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
)
from tests.ptg_frozen_test_support import frozen_descriptor_by_ordinal


_SHARED_ENGINE_ID_WIDTH_DIGESTS = (
    (
        16,
        "cf717fc893f83f880bb4e7913a094fff"
        "889a0ff1badca2a9081c33c671edb9fc",
    ),
    (
        32,
        "6fb4100a5a8b34797c0a63d66d53daa5"
        "d04f9aaf465c6c87832e3d23b5c927ed",
    ),
    (
        64,
        "a583134230426a5b71242a47d5debba5d"
        "b8cd6a873458bdcb45e9ae3134b6a4f",
    ),
)
_GENERATED_ENGINE_ID_VECTORS = (
    (
        "checksum64",
        (16, 16),
        "23c7f76e006f36e6",
        "50e65518c09b5608",
    ),
    (
        "blake2",
        (32, 32),
        "e6839d0f22b5148bef185384f3cc0ebe",
        "1ceb2a8a8b7314524e9e904067607d03",
    ),
    (
        "sha256",
        (64, 32),
        "a3c7f76e006f36e6a79c9952196628ee"
        "a4e4c1cff1551a804d5fdd915585e62e",
        "b63d181649c3ee55da59078af6fe0bb8",
    ),
)


def _shared_engine_id_width_vector(
    engine_id_width: int,
) -> list[dict[str, object]]:
    common_by_ordinal = (
        (1, 101, "11", "aa", False, "1", "a"),
        (2, 202, "22", None, True, "2", "b"),
    )
    return [
        {
            "source_type": "in_network",
            "canonical_url": (
                "https://rates.example.test/"
                f"2026-07_network-rates_{ordinal:04}_of_02.json.gz"
            ),
            "content_length": content_length,
            "etag": f'"part-{ordinal}"',
            "last_modified": None,
            "raw_sha256": raw_pair * 32,
            "logical_sha256": (
                None if logical_pair is None else logical_pair * 32
            ),
            "logical_hash_deferred": logical_hash_deferred,
            "engine_source_identity_hash": identity_digit
            * engine_id_width,
            "engine_source_file_version_id": version_digit
            * engine_id_width,
            "ordinal": ordinal,
        }
        for (
            ordinal,
            content_length,
            raw_pair,
            logical_pair,
            logical_hash_deferred,
            identity_digit,
            version_digit,
        ) in common_by_ordinal
    ]


def _generated_engine_ids() -> tuple[str, str]:
    source_identity_hash = semantic_hash(
        {
            "source_type": "in_network",
            "canonical_url": (
                "https://rates.example.test/part-1.json.gz"
            ),
        },
        domain="source_identity",
    )
    source_file_version_id = semantic_hash(
        {
            "source_identity_hash": source_identity_hash,
            "raw_sha256": "11" * 32,
            "logical_sha256": "aa" * 32,
            "content_identity_kind": "logical_json_sha256_v1",
            "etag": '"part-1"',
            "content_length": 101,
            "last_modified": None,
        },
        domain="source_file_version",
    )[:32]
    return source_identity_hash, source_file_version_id


def _descriptors_with_engine_ids(
    identity_hash: str,
    version_id: str,
) -> list[dict[str, object]]:
    first_descriptor = frozen_descriptor_by_ordinal(1)
    second_descriptor = frozen_descriptor_by_ordinal(2)
    first_descriptor["engine_source_identity_hash"] = identity_hash
    first_descriptor["engine_source_file_version_id"] = version_id
    second_descriptor["engine_source_identity_hash"] = (
        "0" * len(identity_hash)
    )
    second_descriptor["engine_source_file_version_id"] = (
        "0" * len(version_id)
    )
    return [first_descriptor, second_descriptor]


@pytest.mark.parametrize(
    ("engine_id_width", "expected_digest"),
    _SHARED_ENGINE_ID_WIDTH_DIGESTS,
)
def test_cross_repo_golden_vectors_freeze_engine_id_width_digests(
    engine_id_width,
    expected_digest,
):
    frozen_rate_files = _shared_engine_id_width_vector(engine_id_width)

    assert frozen_rate_file_set_sha256(frozen_rate_files) == expected_digest


@pytest.mark.parametrize(
    (
        "hash_mode",
        "expected_widths",
        "expected_identity_hash",
        "expected_version_id",
    ),
    _GENERATED_ENGINE_ID_VECTORS,
)
def test_frozen_set_accepts_generated_engine_hash_mode_vectors(
    monkeypatch,
    hash_mode,
    expected_widths,
    expected_identity_hash,
    expected_version_id,
):
    monkeypatch.setenv("HLTHPRT_PTG2_HASH_MODE", hash_mode)
    actual_identity_hash, actual_version_id = _generated_engine_ids()

    assert actual_identity_hash == expected_identity_hash
    assert actual_version_id == expected_version_id
    assert (
        len(actual_identity_hash),
        len(actual_version_id),
    ) == expected_widths

    frozen_rate_files = _descriptors_with_engine_ids(
        actual_identity_hash,
        actual_version_id,
    )
    digest = frozen_rate_file_set_sha256(frozen_rate_files)
    normalized_files, normalized_digest = normalize_frozen_rate_file_set(
        list(reversed(frozen_rate_files)),
        digest,
    )
    assert normalized_digest == digest
    assert normalized_files[0]["engine_source_file_version_id"] == (
        expected_version_id
    )


def test_frozen_set_accepts_64_character_engine_file_version_ids():
    frozen_rate_files = _shared_engine_id_width_vector(64)
    expected_digest = frozen_rate_file_set_sha256(frozen_rate_files)

    normalized_files, normalized_digest = normalize_frozen_rate_file_set(
        frozen_rate_files,
        expected_digest,
    )

    assert normalized_digest == expected_digest
    assert normalized_files[0]["engine_source_file_version_id"] == "a" * 64
