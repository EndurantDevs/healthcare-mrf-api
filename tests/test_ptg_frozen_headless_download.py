# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact GET evidence when a frozen multipart server omits HEAD metadata."""

from __future__ import annotations

import hashlib
import gzip
import importlib

import pytest

from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.ptg_parts.artifact_streams import (
    PTG2_DEFER_LOGICAL_HASH_BYTES_ENV,
    logical_artifact_identity,
)
from process.ptg_parts.domain import PTG2HeadMetadata
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    validate_frozen_artifacts,
    validate_frozen_processed_results,
)
from tests.ptg_frozen_test_support import frozen_descriptor_by_ordinal


source_download = importlib.import_module(
    "process.ptg_parts.source_download"
)


class _HeadlessDownloadHarness:
    def __init__(self) -> None:
        self.body = gzip.compress(b"{}")
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


async def _download_headless_artifacts(
    harness: _HeadlessDownloadHarness,
    artifact_store: PTG2ArtifactStore,
):
    """Download once and derive the deferred logical identity."""

    raw_artifact = await source_download._download_raw_artifact_locked(
        harness.canonical_url,
        store=artifact_store,
        canonical_url=harness.canonical_url,
        reuse_raw_artifacts=False,
        max_bytes=None,
        keep_partial_artifacts=False,
        exact_get_evidence=True,
    )
    logical_artifact = logical_artifact_identity(
        raw_artifact.raw_path,
        raw_sha256=raw_artifact.raw_sha256,
        raw_byte_count=raw_artifact.byte_count,
        allow_deferred=True,
    )
    return raw_artifact, logical_artifact


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
    ) = harness.prepared_store(tmp_path)
    harness.install(monkeypatch)
    monkeypatch.setenv(PTG2_DEFER_LOGICAL_HASH_BYTES_ENV, "1")

    raw_artifact, logical_artifact = await _download_headless_artifacts(
        harness,
        artifact_store,
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
    processed_proof = validate_frozen_processed_results(
        [descriptor_by_field],
        [
            {
                "source_type": descriptor_by_field["source_type"],
                "url": descriptor_by_field["canonical_url"],
                "success": True,
                "summary": {
                    **descriptor_by_field,
                    "logical_sha256": raw_artifact.raw_sha256,
                    "raw_byte_count": raw_artifact.byte_count,
                    "verification_mode": "downloaded",
                },
            }
        ],
    )
    assert processed_proof[0]["logical_sha256"] is None
    assert processed_proof[0]["logical_hash_deferred"] is True
    assert existing_cas_path.read_bytes() == harness.body
