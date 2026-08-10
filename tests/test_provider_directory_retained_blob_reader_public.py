# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path

import pytest

from process.provider_directory_retained_artifact_contract import (
    RetainedArtifactError,
)
from process.provider_directory_retained_blob_store import (
    open_retained_artifact_blob,
)
from tests.provider_directory_retained_reader_support import (
    write_retained_artifact_blob,
)


pytest_plugins = ("tests.provider_directory_retained_reader_fixtures",)


def test_public_blob_reader_requires_and_verifies_complete_sequential_bytes(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"complete sequential retained bytes\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        artifact_bytes,
    )

    with open_retained_artifact_blob(
        artifact_sha256,
        len(artifact_bytes),
    ) as reader:
        assert reader.read(7) == artifact_bytes[:7]
        assert reader.read(0) == b""
        assert reader.read() == artifact_bytes[7:]
        assert reader.read() == b""


def test_public_blob_reader_rejects_partial_and_digest_mismatched_reads(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"reader digest boundary\n"
    artifact_sha256, blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        artifact_bytes,
    )
    with pytest.raises(RetainedArtifactError, match="digest_mismatch"):
        with open_retained_artifact_blob(
            artifact_sha256,
            len(artifact_bytes),
        ) as reader:
            assert reader.read(1) == artifact_bytes[:1]

    blob_path.write_bytes(b"X" * len(artifact_bytes))
    with pytest.raises(RetainedArtifactError, match="digest_mismatch"):
        with open_retained_artifact_blob(
            artifact_sha256,
            len(artifact_bytes),
        ) as reader:
            assert reader.read() == b"X" * len(artifact_bytes)


def test_public_blob_reader_preserves_consumer_exception_and_closes(
    retained_artifact_test_root: Path,
) -> None:
    artifact_bytes = b"consumer exception retained bytes\n"
    artifact_sha256, _blob_path = write_retained_artifact_blob(
        retained_artifact_test_root,
        artifact_bytes,
    )
    captured_reader = None

    with pytest.raises(RuntimeError, match="consumer failed"):
        with open_retained_artifact_blob(
            artifact_sha256,
            len(artifact_bytes),
        ) as reader:
            captured_reader = reader
            assert reader.read(1) == artifact_bytes[:1]
            raise RuntimeError("consumer failed")

    assert captured_reader is not None
    with pytest.raises(RetainedArtifactError, match="blob_read_failed"):
        captured_reader.read(1)
