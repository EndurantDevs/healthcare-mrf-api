# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

import process.clinical_reference_sources as sources


class _ByteResponse:
    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def __enter__(self):
        return self

    def __exit__(self, *_exception):
        return False

    def read(self, *_args):
        return next(self._chunks)


def _write_artifact_generation(artifact_path, content, source_url):
    artifact_path.write_bytes(content)
    manifest_map = {
        "source_url": source_url,
        "downloaded_at": "2026-07-24T00:00:00Z",
        "byte_count": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }
    sources._manifest_path(artifact_path).write_text(
        json.dumps(manifest_map),
        encoding="utf-8",
    )
    return manifest_map


def test_manifest_publish_failure_restores_previous_generation(
    monkeypatch,
    tmp_path,
):
    artifact_path = tmp_path / "artifact.zip"
    previous_manifest = _write_artifact_generation(
        artifact_path,
        b"previous-generation",
        "https://example.test/previous.zip",
    )
    manifest_path = sources._manifest_path(artifact_path)
    replace_file = sources.os.replace

    def fail_manifest_replace(source_path, destination_path):
        if Path(destination_path) == manifest_path:
            raise OSError("synthetic manifest publication failure")
        replace_file(source_path, destination_path)

    monkeypatch.setattr(sources.os, "replace", fail_manifest_replace)
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _ByteResponse([b"new-generation", b""]),
    )

    with pytest.raises(RuntimeError, match="manifest publication failure"):
        sources._download_url(
            "https://example.test/new.zip",
            artifact_path,
            force=True,
        )

    assert artifact_path.read_bytes() == b"previous-generation"
    assert json.loads(manifest_path.read_text()) == previous_manifest
    assert list(tmp_path.glob(".artifact.zip.rollback-*.*.tmp")) == []


def test_first_manifest_publish_failure_leaves_no_visible_artifact(
    monkeypatch,
    tmp_path,
):
    artifact_path = tmp_path / "artifact.zip"
    manifest_path = sources._manifest_path(artifact_path)
    replace_file = sources.os.replace

    def fail_manifest_replace(source_path, destination_path):
        if Path(destination_path) == manifest_path:
            raise OSError("synthetic manifest publication failure")
        replace_file(source_path, destination_path)

    monkeypatch.setattr(sources.os, "replace", fail_manifest_replace)
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _ByteResponse([b"first-generation", b""]),
    )

    with pytest.raises(RuntimeError, match="manifest publication failure"):
        sources._download_url(
            "https://example.test/first.zip",
            artifact_path,
        )

    assert not artifact_path.exists()
    assert not manifest_path.exists()
    assert list(tmp_path.glob(".artifact.zip.rollback-*.*.tmp")) == []


def test_next_download_recovers_generation_interrupted_after_artifact_swap(
    monkeypatch,
    tmp_path,
):
    artifact_path = tmp_path / "artifact.zip"
    previous_manifest = _write_artifact_generation(
        artifact_path,
        b"previous-generation",
        "https://example.test/previous.zip",
    )
    rollback_path, had_artifact = sources._create_publication_rollback(
        artifact_path
    )
    replacement_path = tmp_path / "replacement.zip"
    replacement_path.write_bytes(b"interrupted-generation")
    sources.os.replace(replacement_path, artifact_path)
    open_response = Mock()
    monkeypatch.setattr(sources.urllib.request, "urlopen", open_response)

    recovered_path = sources._download_url(
        "https://example.test/previous.zip",
        artifact_path,
    )

    assert had_artifact is True
    assert recovered_path == artifact_path
    assert artifact_path.read_bytes() == b"previous-generation"
    restored_manifest = json.loads(
        sources._manifest_path(artifact_path).read_text()
    )
    assert restored_manifest == previous_manifest
    assert not rollback_path.exists()
    open_response.assert_not_called()
