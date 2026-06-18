# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from process.ext import utils


def test_download_cache_dir_override_keeps_non_tmp_cache(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DOWNLOAD_CACHE_DIR", "/work/download-cache")

    assert utils._resolve_download_cache_dir("/var/cache/healthporta") == "/var/cache/healthporta"


def test_download_cache_dir_override_moves_tmp_cache(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DOWNLOAD_CACHE_DIR", "/work/download-cache")

    assert utils._resolve_download_cache_dir("/tmp") == "/work/download-cache"


def test_download_cache_dir_override_ignored_when_unset(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DOWNLOAD_CACHE_DIR", raising=False)

    assert utils._resolve_download_cache_dir("/tmp") == "/tmp"
