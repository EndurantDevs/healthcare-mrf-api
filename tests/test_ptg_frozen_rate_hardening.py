# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CAS safety and exact-response proofs for frozen multipart PTG files."""

from __future__ import annotations

import importlib
from dataclasses import replace

import pytest

from process.ptg_parts.domain import PTG2HeadMetadata
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
    frozen_rate_file_set_sha256,
    normalize_frozen_rate_file_set,
    validate_frozen_artifacts,
    validate_frozen_processed_results,
)
from tests.ptg_frozen_test_support import (
    frozen_artifacts,
    frozen_descriptor_by_ordinal,
    frozen_rate_file_set,
)


source_download = importlib.import_module(
    "process.ptg_parts.source_download"
)


@pytest.mark.parametrize(
    ("raw_change_by_field", "logical_change_by_field", "message"),
    [
        ({"byte_count": 1}, {}, "body content length"),
        ({"raw_sha256": "f" * 64}, {}, "raw SHA-256"),
        (
            {
                "head": PTG2HeadMetadata(
                    url="",
                    etag='"other"',
                    content_length=10_001,
                )
            },
            {},
            "ETag",
        ),
        ({}, {"logical_sha256": "f" * 64}, "logical SHA-256"),
    ],
)
def test_mismatch_never_unlinks_published_cas_output(
    tmp_path,
    raw_change_by_field,
    logical_change_by_field,
    message,
):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(raw_artifact, **raw_change_by_field)
    logical_artifact = replace(
        logical_artifact,
        **logical_change_by_field,
    )

    with pytest.raises(FrozenRateFileMismatchError, match=message):
        validate_frozen_artifacts(
            descriptor_by_field,
            raw_artifact,
            logical_artifact,
        )

    assert (tmp_path / "raw.json").exists()
    assert (tmp_path / "logical.json").exists()


def test_mismatch_preserves_retained_cache(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        raw_sha256="f" * 64,
        reused=True,
    )
    logical_artifact = replace(logical_artifact, reused=True)

    with pytest.raises(FrozenRateFileMismatchError, match="raw SHA-256"):
        validate_frozen_artifacts(
            descriptor_by_field,
            raw_artifact,
            logical_artifact,
        )

    assert (tmp_path / "raw.json").exists()
    assert (tmp_path / "logical.json").exists()


def test_mismatch_preserves_raw_logical_alias(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    aliased_path = tmp_path / "shared-cas-object"
    aliased_path.write_bytes(b"shared")
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        raw_path=str(aliased_path),
        raw_storage_uri=str(aliased_path),
        raw_sha256="f" * 64,
    )
    logical_artifact = replace(
        logical_artifact,
        logical_path=str(aliased_path),
    )

    with pytest.raises(FrozenRateFileMismatchError, match="raw SHA-256"):
        validate_frozen_artifacts(
            descriptor_by_field,
            raw_artifact,
            logical_artifact,
        )

    assert aliased_path.read_bytes() == b"shared"


def test_retained_artifact_uses_sealed_local_proof_without_head(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        reused=True,
        head=PTG2HeadMetadata(
            url=str(descriptor_by_field["canonical_url"]),
            supports_head=False,
        ),
    )
    logical_artifact = replace(logical_artifact, reused=True)

    validate_frozen_artifacts(
        descriptor_by_field,
        raw_artifact,
        logical_artifact,
    )


def test_retained_artifact_rejects_explicit_head_mismatch(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        reused=True,
        head=PTG2HeadMetadata(
            url=str(descriptor_by_field["canonical_url"]),
            status=200,
            etag='"changed"',
            content_length=int(descriptor_by_field["content_length"]),
            last_modified=str(descriptor_by_field["last_modified"]),
            supports_head=True,
        ),
    )

    with pytest.raises(FrozenRateFileMismatchError, match="ETag"):
        validate_frozen_artifacts(
            descriptor_by_field,
            raw_artifact,
            logical_artifact,
        )


def test_fresh_artifact_accepts_exact_get_evidence_without_head(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        head=PTG2HeadMetadata(
            url=str(descriptor_by_field["canonical_url"]),
            status=200,
            etag=str(descriptor_by_field["etag"]),
            content_length=int(descriptor_by_field["content_length"]),
            last_modified=str(descriptor_by_field["last_modified"]),
            supports_head=False,
        ),
    )

    validate_frozen_artifacts(
        descriptor_by_field,
        raw_artifact,
        logical_artifact,
    )


def test_fresh_artifact_rejects_redirected_final_get_url(tmp_path):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )
    raw_artifact = replace(
        raw_artifact,
        head=replace(
            raw_artifact.head,
            url="https://rates.example.com/2026-07/different.json.gz",
            supports_head=False,
        ),
    )

    with pytest.raises(
        FrozenRateFileMismatchError,
        match="different canonical URL",
    ):
        validate_frozen_artifacts(
            descriptor_by_field,
            raw_artifact,
            logical_artifact,
        )


def test_frozen_set_rejects_aggregate_byte_budget(monkeypatch):
    frozen_rate_files = [
        frozen_descriptor_by_ordinal(ordinal)
        for ordinal in range(1, 129)
    ]
    monkeypatch.setenv(
        "HLTHPRT_PTG2_FROZEN_TOTAL_MAX_BYTES",
        "1280000",
    )

    with pytest.raises(
        FrozenRateFileValidationError,
        match="aggregate content length",
    ):
        frozen_rate_file_set_sha256(frozen_rate_files)


def test_frozen_set_accepts_max_parts_within_byte_budget(monkeypatch):
    frozen_rate_files = [
        frozen_descriptor_by_ordinal(ordinal)
        for ordinal in range(1, 129)
    ]
    expected_total = sum(
        int(descriptor["content_length"])
        for descriptor in frozen_rate_files
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_FROZEN_TOTAL_MAX_BYTES",
        str(expected_total),
    )

    assert len(frozen_rate_file_set_sha256(frozen_rate_files)) == 64


def test_processed_proof_requires_exact_source_version_cardinality():
    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(2)
    normalized_files, _ = normalize_frozen_rate_file_set(
        frozen_rate_files,
        frozen_set_digest,
    )
    processed_results = [
        {
            "source_type": descriptor["source_type"],
            "url": descriptor["canonical_url"],
            "success": True,
            "summary": {
                **descriptor,
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            },
        }
        for descriptor in normalized_files
    ]

    proof_rows = validate_frozen_processed_results(
        normalized_files,
        processed_results,
    )

    assert len(proof_rows) == 2
    assert [proof_row["ordinal"] for proof_row in proof_rows] == [1, 2]
    with pytest.raises(FrozenRateFileMismatchError, match="cardinality"):
        validate_frozen_processed_results(
            normalized_files,
            processed_results[:1],
        )


@pytest.mark.parametrize("verification_mode", [None, "", "head_only"])
def test_processed_proof_rejects_ambiguous_verification_mode(
    verification_mode,
):
    """Published multipart proof must name an implemented byte verifier."""

    frozen_rate_files, frozen_set_digest = frozen_rate_file_set(2)
    normalized_files, _ = normalize_frozen_rate_file_set(
        frozen_rate_files,
        frozen_set_digest,
    )
    processed_results = [
        {
            "source_type": descriptor["source_type"],
            "url": descriptor["canonical_url"],
            "success": True,
            "summary": {
                **descriptor,
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": verification_mode,
            },
        }
        for descriptor in normalized_files
    ]

    with pytest.raises(
        FrozenRateFileMismatchError,
        match="verification_mode",
    ):
        validate_frozen_processed_results(
            normalized_files,
            processed_results,
        )


@pytest.mark.asyncio
async def test_download_aggregation_preserves_typed_mismatch(
    monkeypatch,
    tmp_path,
):
    descriptor_by_field = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(
        descriptor_by_field,
        tmp_path,
    )

    async def download(*_args, **_kwargs):
        return raw_artifact

    def raise_frozen_mismatch(*_args, **_kwargs):
        raise FrozenRateFileMismatchError("frozen body drift")

    monkeypatch.setattr(
        source_download,
        "download_raw_artifact",
        download,
    )
    monkeypatch.setattr(
        source_download,
        "_should_materialize_logical_artifact",
        lambda _path: False,
    )
    monkeypatch.setattr(
        source_download,
        "logical_artifact_identity",
        lambda *_args, **_kwargs: logical_artifact,
    )
    monkeypatch.setattr(
        source_download,
        "validate_frozen_artifacts",
        raise_frozen_mismatch,
    )

    with pytest.raises(FrozenRateFileMismatchError, match="frozen body drift"):
        await source_download._download_ptg_job_artifact(
            {
                "url": descriptor_by_field["canonical_url"],
                "_frozen_rate_file": descriptor_by_field,
            },
            reuse_raw_artifacts=True,
            max_bytes=None,
            keep_partial_artifacts=False,
        )
