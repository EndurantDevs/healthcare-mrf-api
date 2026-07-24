# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import process.clinical_reference_sources as sources
from process.control_cancel import ImportCancelledError

clinical = importlib.import_module("process.clinical_reference")


class _ByteResponse:
    def __init__(self, chunks):
        self._chunks = iter(chunks)

    def __enter__(self):
        return self

    def __exit__(self, *_exception):
        return False

    def read(self, *_args):
        next_chunk = next(self._chunks)
        if isinstance(next_chunk, Exception):
            raise next_chunk
        return next_chunk


def test_source_selection_and_request_defaults_are_explicit(monkeypatch, tmp_path):
    """Environment defaults are normalized while restricted sources stay gated."""
    monkeypatch.setenv("HLTHPRT_CLINICAL_REFERENCE_IMPORT_ID", " release-2026! ")
    monkeypatch.setenv("HLTHPRT_CLINICAL_REFERENCE_TEST_LIMIT", "7")
    monkeypatch.setenv("HLTHPRT_UMLS_API_KEY", "synthetic-key")
    monkeypatch.setenv("HLTHPRT_ENABLE_RESTRICTED_TERMINOLOGIES", "yes")

    request = clinical._build_request(
        True,
        None,
        " ICD10CM, SNOMED, ",
        str(tmp_path),
        True,
        "run-1",
    )

    assert request.import_suffix == "release2026"
    assert request.selected_source_names == {"icd10cm", "snomed"}
    assert request.source_test_limit == 7
    assert request.umls_key == "synthetic-key"
    assert request.force_download is True
    assert clinical._artifact_root(str(tmp_path)) == tmp_path
    assert list(clinical._batch([{"code": "1"}, {"code": "2"}], size=1)) == [
        [{"code": "1"}],
        [{"code": "2"}],
    ]

    monkeypatch.delenv("HLTHPRT_CLINICAL_REFERENCE_IMPORT_ID")
    assert clinical._normalize_import_id(None).isdigit()
    assert clinical._normalize_import_id("!!!").isdigit()


def test_cancel_probe_supports_dsn_settings_and_fail_open(monkeypatch):
    """Both Redis configuration paths recognize cancellation and outages fail open."""
    captured_connections = []

    class CancelClient:
        def __init__(self, cancel_value):
            self.cancel_value = cancel_value

        def get(self, key):
            assert key == "cancel:run-2"
            return self.cancel_value

    class RedisFactory:
        @staticmethod
        def from_url(redis_dsn, **timeouts):
            captured_connections.append((redis_dsn, timeouts))
            return CancelClient(b"1")

        def __new__(cls, **connection):
            captured_connections.append(connection)
            return CancelClient("1")

    monkeypatch.setattr(sources.redis, "Redis", RedisFactory)
    monkeypatch.setattr(
        sources,
        "build_redis_settings",
        lambda: SimpleNamespace(host="cache", port=6379, password="pw", database=4),
    )
    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://cache/4")
    assert sources._is_cancel_requested("run-2") is True

    monkeypatch.delenv("HLTHPRT_REDIS_ADDRESS")
    assert sources._is_cancel_requested("run-2") is True
    assert sources._is_cancel_requested(None) is False

    monkeypatch.setattr(
        sources,
        "build_redis_settings",
        lambda: (_ for _ in ()).throw(OSError("cache unavailable")),
    )
    assert sources._is_cancel_requested("run-2") is False
    assert captured_connections[0][0] == "redis://cache/4"


def test_download_publishes_manifest_and_reuses_cache(monkeypatch, tmp_path):
    """A completed artifact is atomically published with provenance and then reused."""
    opened_requests = []

    def open_response(request, timeout):
        opened_requests.append((request.full_url, timeout))
        return _ByteResponse([b"alpha", b"-beta", b""])

    monkeypatch.setattr(sources.urllib.request, "urlopen", open_response)
    monkeypatch.setattr(sources, "_now_isoformat", lambda: "2026-07-24T00:00:00Z")
    artifact_path = tmp_path / "nested" / "artifact.zip"

    downloaded_path = sources._download_url(
        "https://example.test/artifact.zip",
        artifact_path,
        run_id=None,
    )
    cached_path = sources._download_url(
        "https://example.test/artifact.zip",
        artifact_path,
    )
    manifest_map = json.loads(
        artifact_path.with_suffix(".zip.manifest.json").read_text(encoding="utf-8")
    )

    assert downloaded_path == cached_path == artifact_path
    assert artifact_path.read_bytes() == b"alpha-beta"
    assert manifest_map == {
        "source_url": "https://example.test/artifact.zip",
        "downloaded_at": "2026-07-24T00:00:00Z",
        "byte_count": 10,
        "sha256": hashlib.sha256(b"alpha-beta").hexdigest(),
    }
    assert opened_requests == [("https://example.test/artifact.zip", 3600)]


def test_download_failure_redacts_and_discards_partial(monkeypatch, tmp_path):
    """Failed and cancelled downloads never expose credentials or retain partial bytes."""
    artifact_path = tmp_path / "restricted.zip"
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _ByteResponse(
            [b"partial", OSError("apiKey=visible-secret&reason=broken")]
        ),
    )

    with pytest.raises(RuntimeError) as failure:
        sources._download_url(
            "https://example.test/restricted.zip",
            artifact_path,
            api_key="visible-secret",
        )

    assert "visible-secret" not in str(failure.value)
    assert "apiKey=<redacted>" in str(failure.value)
    assert not artifact_path.with_suffix(".zip.tmp").exists()
    assert not artifact_path.exists()

    cancel_checks = iter((None, ImportCancelledError("cancelled")))

    def cancel_after_start(_run_id):
        cancel_outcome = next(cancel_checks)
        if isinstance(cancel_outcome, Exception):
            raise cancel_outcome

    monkeypatch.setattr(sources, "_raise_if_cancelled", cancel_after_start)
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _ByteResponse([b"partial", b""]),
    )
    with pytest.raises(ImportCancelledError, match="cancelled"):
        sources._download_url(
            "https://example.test/cancel.zip",
            artifact_path,
            run_id="run-3",
        )
    assert not artifact_path.with_suffix(".zip.tmp").exists()


@pytest.mark.parametrize(
    ("release_maps", "expected_version"),
    [
        ([{"releaseVersion": "old"}, {"current": True, "releaseVersion": "new"}], "new"),
        ([{"releaseVersion": "fallback"}], "fallback"),
    ],
)
def test_release_selection_prefers_current(monkeypatch, release_maps, expected_version):
    """UMLS release discovery prefers the current marker and has a documented fallback."""
    response = _ByteResponse([json.dumps(release_maps).encode()])
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: response,
    )

    assert sources._release_current("synthetic-release")["releaseVersion"] == expected_version


def test_release_selection_rejects_empty_catalog(monkeypatch):
    """An empty UMLS release response is a hard source-contract failure."""
    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _ByteResponse([b"[]"]),
    )
    with pytest.raises(RuntimeError, match="No UMLS release"):
        sources._release_current("missing-release")


@pytest.mark.asyncio
async def test_product_rxcuis_close_connections_on_success_and_failure(monkeypatch):
    """MED-RT product discovery applies limits and closes every opened connection."""

    class ProductConnection:
        def __init__(self):
            self.closed = False

        async def fetch(self, query):
            assert "unnest(rxnorm_ids)" in query
            return [{"rxcui": "11"}, {"rxcui": None}, {"rxcui": "22"}]

        async def close(self):
            self.closed = True

    product_connection = ProductConnection()

    async def connect_success(**connection):
        assert connection["database"] == "postgres"
        return product_connection

    monkeypatch.setenv("HLTHPRT_RX_DB_DATABASE", "postgres")
    monkeypatch.setattr(sources.asyncpg, "connect", connect_success)
    assert await sources._load_product_rxcuis(1) == ["11"]
    assert product_connection.closed is True

    async def connect_failure(**_connection):
        raise OSError("database unavailable")

    monkeypatch.setattr(sources.asyncpg, "connect", connect_failure)
    assert await sources._load_product_rxcuis() == []


def test_rxclass_retries_then_surfaces_last_error(monkeypatch):
    """Transient RxClass failures retry, while the final upstream error remains visible."""
    open_attempts = []
    retry_delays = []

    def flaky_open(*_args, **_kwargs):
        open_attempts.append("attempt")
        if len(open_attempts) < 3:
            raise OSError("temporary")
        return _ByteResponse(
            [b'{"rxclassDrugInfoList":{"rxclassDrugInfo":[{"rela":"may_treat"}]}}']
        )

    monkeypatch.setattr(sources.urllib.request, "urlopen", flaky_open)
    monkeypatch.setattr(sources.time, "sleep", retry_delays.append)
    assert sources._rxclass_for_rxcui("123") == [{"rela": "may_treat"}]
    assert len(open_attempts) == 3
    assert retry_delays == [0.5, 1.0]

    monkeypatch.setattr(
        sources.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("permanent")),
    )
    with pytest.raises(OSError, match="permanent"):
        sources._rxclass_for_rxcui("456")


@pytest.mark.asyncio
async def test_medrt_loader_deduplicates_classes_and_tolerates_lookup_failure(
    monkeypatch,
):
    """Valid RxClass concepts publish once while a failed RxCUI remains non-fatal."""
    monkeypatch.setattr(
        sources,
        "_load_product_rxcuis",
        lambda *_args: _async_value(["100", "200"]),
    )

    def rxclass_lookup(rxcui):
        if rxcui == "200":
            raise OSError("lookup unavailable")
        return [
            {
                "rela": "may_treat",
                "rxclassMinConceptItem": {
                    "classId": "D0001",
                    "className": "Synthetic condition",
                    "classType": "DISEASE",
                },
            },
            {"rxclassMinConceptItem": {"classId": "", "className": ""}},
        ]

    monkeypatch.setattr(sources, "_rxclass_for_rxcui", rxclass_lookup)
    monkeypatch.setenv("HLTHPRT_MEDRT_RXCLASS_CONCURRENCY", "1")
    concept_rows, synonym_rows, relationship_rows = (
        await sources._load_medrt_from_rxclass(True)
    )

    assert [(entry["code_system"], entry["code"]) for entry in concept_rows] == [
        ("MESH", "D0001")
    ]
    assert len(synonym_rows) == 1
    assert relationship_rows[0]["relationship"] == "may_treat"


async def _async_value(value):
    return value
