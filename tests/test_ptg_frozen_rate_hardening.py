# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CAS safety and exact-response proofs for frozen multipart PTG files."""

from __future__ import annotations

import hashlib
import importlib
from dataclasses import replace

import pytest

from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.domain import (
    PTG2HeadMetadata,
    PTG2LogicalArtifact,
)
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


class _HeadlessDownloadHarness:
    def __init__(self) -> None:
        self.body = b"{}"
        self.raw_sha256 = hashlib.sha256(self.body).hexdigest()
        self.canonical_url = "https://rates.example.test/part.json"

    async def head_metadata(self, _url):
        return PTG2HeadMetadata(
            url=self.canonical_url,
            supports_head=False,
        )

    async def single_get(self, *, path, **_kwargs):
        path.write_bytes(self.body)
        return (
            hashlib.sha256(self.body),
            len(self.body),
            '"from-get"',
            len(self.body),
            "Mon, 27 Jul 2026 10:00:00 GMT",
            self.canonical_url,
            200,
            None,
            "application/json",
        )

    async def reject_range(self, *_args, **_kwargs):
        raise AssertionError("frozen download must use one exact GET")

    def prepared_store(self, temporary_path):
        artifact_store = PTG2ArtifactStore(temporary_path / "store")
        descriptor_by_field = {
            **frozen_descriptor_by_ordinal(1),
            "canonical_url": self.canonical_url,
            "content_length": len(self.body),
            "etag": '"from-get"',
            "last_modified": "Mon, 27 Jul 2026 10:00:00 GMT",
            "raw_sha256": self.raw_sha256,
            "logical_sha256": None,
            "logical_hash_deferred": True,
        }
        existing_cas_path = artifact_store.artifact_path(
            self.raw_sha256,
            kind="raw",
        )
        existing_cas_path.parent.mkdir(parents=True, exist_ok=True)
        existing_cas_path.write_bytes(self.body)
        return artifact_store, descriptor_by_field, existing_cas_path

    def install(self, monkeypatch):
        monkeypatch.setattr(
            source_download,
            "fetch_head_metadata",
            self.head_metadata,
        )
        monkeypatch.setattr(
            source_download,
            "_download_raw_artifact_single_get",
            self.single_get,
        )
        monkeypatch.setattr(
            source_download,
            "_probe_http_range_support",
            self.reject_range,
        )


@pytest.mark.asyncio
async def test_fresh_headless_download_preserves_exact_get_evidence(
    monkeypatch,
    tmp_path,
):
    """Fresh HEAD-less input uses exact final GET evidence and keeps CAS."""

    harness = _HeadlessDownloadHarness()
    (
        artifact_store,
        descriptor_by_field,
        existing_cas_path,
    ) = harness.prepared_store(
        tmp_path
    )
    harness.install(monkeypatch)

    raw_artifact = await source_download._download_raw_artifact_locked(
        harness.canonical_url,
        store=artifact_store,
        canonical_url=harness.canonical_url,
        reuse_raw_artifacts=False,
        max_bytes=None,
        keep_partial_artifacts=False,
        exact_get_evidence=True,
    )
    logical_artifact = PTG2LogicalArtifact(
        logical_path=raw_artifact.raw_path,
        logical_sha256=raw_artifact.raw_sha256,
        byte_count=raw_artifact.byte_count,
        logical_hash_deferred=True,
    )

    validate_frozen_artifacts(
        descriptor_by_field,
        raw_artifact,
        logical_artifact,
    )
    assert raw_artifact.head is not None
    assert raw_artifact.head.supports_head is False
    assert raw_artifact.head.status == 200
    assert raw_artifact.head.url == harness.canonical_url
    with pytest.raises(FrozenRateFileMismatchError, match="ETag"):
        validate_frozen_artifacts(
            {**descriptor_by_field, "etag": '"different"'},
            raw_artifact,
            logical_artifact,
        )
    assert existing_cas_path.read_bytes() == harness.body
