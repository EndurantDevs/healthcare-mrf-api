# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor

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


class _BlockingResponse(_ByteResponse):
    def __init__(self, content, download_active, release_download):
        super().__init__([content, b""])
        self.download_active = download_active
        self.release_download = release_download

    def read(self, *_args):
        artifact_chunk = super().read()
        if not artifact_chunk:
            self.download_active.set()
            assert self.release_download.wait(timeout=5)
        return artifact_chunk


def _assert_manifest_matches_artifact(artifact_path, payload_by_url):
    artifact_content = artifact_path.read_bytes()
    manifest_map = json.loads(
        artifact_path.with_suffix(".zip.manifest.json").read_text()
    )
    expected_url = next(
        source_url
        for source_url, source_content in payload_by_url.items()
        if source_content == artifact_content
    )
    assert manifest_map["source_url"] == expected_url
    assert manifest_map["byte_count"] == len(artifact_content)
    assert manifest_map["sha256"] == hashlib.sha256(artifact_content).hexdigest()


def test_concurrent_download_manifest_matches_final_artifact(
    monkeypatch,
    tmp_path,
):
    artifact_path = tmp_path / "artifact.zip"
    first_download_active = threading.Event()
    release_first_download = threading.Event()
    payload_by_url = {
        "https://example.test/first.zip": b"first-content",
        "https://example.test/second.zip": b"second-content",
    }

    def open_response(request, timeout):
        assert timeout == 3600
        if request.full_url.endswith("first.zip"):
            return _BlockingResponse(
                payload_by_url[request.full_url],
                first_download_active,
                release_first_download,
            )
        return _ByteResponse([payload_by_url[request.full_url], b""])

    monkeypatch.setattr(sources.urllib.request, "urlopen", open_response)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_download = executor.submit(
            sources._download_url,
            "https://example.test/first.zip",
            artifact_path,
            force=True,
        )
        assert first_download_active.wait(timeout=5)
        active_temporary_paths = list(tmp_path.glob(".artifact.zip.*.tmp"))
        second_download = executor.submit(
            sources._download_url,
            "https://example.test/second.zip",
            artifact_path,
            force=True,
        )
        threading.Event().wait(0.1)
        assert active_temporary_paths
        assert all(path.exists() for path in active_temporary_paths)
        release_first_download.set()
        downloads = [first_download, second_download]
        assert [download.result(timeout=5) for download in downloads] == [
            artifact_path,
            artifact_path,
        ]

    _assert_manifest_matches_artifact(artifact_path, payload_by_url)
