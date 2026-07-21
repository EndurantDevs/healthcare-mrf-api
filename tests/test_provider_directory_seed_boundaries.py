# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for seed resolution and synchronous FHIR requests."""

from __future__ import annotations

import importlib
import io
import ssl
import urllib.error

importer = importlib.import_module("process.provider_directory_fhir")


class _Response:
    status = 200

    def __init__(self, body=b'{"resourceType":"Bundle"}'):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, _size=-1):
        body, self.body = self.body, b""
        return body


def test_seed_deduplication_covers_source_and_anonymous_retest_rows():
    retained_map = {
        "source_id": "same",
        "org_name": "Example",
        "plan_name": "Plan",
        "api_base": "https://example.test/fhir",
        "metadata_json": {},
    }
    duplicate_map = {
        **retained_map,
        "api_base": "https://old.test/fhir",
        "metadata_json": {"provider_directory_override": "manual"},
    }
    anonymous_map = {
        "source_id": None,
        "org_name": "Anonymous",
        "plan_name": "Plan",
        "api_base": "https://anonymous.test/fhir",
        "seed_source": "provider-directory-db-retest",
    }

    assert importer._dedupe_source_rows(
        [retained_map, duplicate_map, anonymous_map]
    ) == [retained_map, anonymous_map]


def test_seed_database_resolver_covers_explicit_and_downloaded_paths(
    monkeypatch,
    tmp_path,
):
    explicit_path, explicit_tmp = importer._resolve_seed_db(
        str(tmp_path / "explicit.db"),
        None,
    )
    assert explicit_path == tmp_path / "explicit.db"
    assert explicit_tmp is None
    monkeypatch.setattr(
        importer,
        "_download_seed_db",
        lambda _url, destination: destination,
    )
    downloaded_path, temporary_directory = importer._resolve_seed_db(
        None,
        "https://example.test/provider-directory.db",
    )
    assert downloaded_path is not None
    assert temporary_directory is not None
    temporary_directory.cleanup()


def test_retest_resolver_covers_absent_explicit_and_downloaded_paths(
    monkeypatch,
    tmp_path,
):
    explicit_path, explicit_tmp = importer._resolve_retest_results(
        str(tmp_path / "retest.json"),
        None,
    )
    assert explicit_path == tmp_path / "retest.json"
    assert explicit_tmp is None
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_RETEST_RESULTS_URL", raising=False)
    assert importer._resolve_retest_results(None, None) == (None, None)
    monkeypatch.setattr(
        importer,
        "_download_retest_results",
        lambda _url, destination: destination,
    )
    downloaded_path, temporary_directory = importer._resolve_retest_results(
        None,
        "https://example.test/retest.json",
    )
    assert downloaded_path is not None
    assert temporary_directory is not None
    temporary_directory.cleanup()


def test_tls_context_can_disable_certificate_verification(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")

    context = importer._ssl_context()

    assert context.check_hostname is False
    assert context.verify_mode == ssl.CERT_NONE


def test_sync_get_covers_success_http_error_and_transport_error(monkeypatch):
    monkeypatch.setattr(
        importer.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(),
    )
    status, payload_by_field, error, _elapsed = importer._fetch_json_sync(
        "https://example.test/fhir",
        timeout=1,
        extra_headers={"X-Test": "yes"},
    )
    assert (status, payload_by_field["resourceType"], error) == (
        200,
        "Bundle",
        None,
    )

    http_error = urllib.error.HTTPError(
        "https://example.test/fhir",
        429,
        "limited",
        {"Retry-After": "7"},
        io.BytesIO(b'{"issue":[]}'),
    )
    monkeypatch.setattr(
        importer.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(http_error),
    )
    status, payload_by_field, error, _elapsed = importer._fetch_json_sync(
        "https://example.test/fhir",
        timeout=1,
    )
    assert status == 429
    assert payload_by_field[importer.SOURCE_RETRY_AFTER_FIELD] == "7"
    assert error is None

    monkeypatch.setattr(
        importer.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("offline")),
    )
    assert "offline" in (
        importer._fetch_json_sync("https://example.test/fhir", timeout=1)[2]
        or ""
    )


def test_sync_post_covers_custom_headers_and_success(monkeypatch):
    monkeypatch.setattr(
        importer.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(),
    )

    observed = importer._post_json_sync(
        "https://example.test/graphql",
        {"query": "{}"},
        timeout=1,
        extra_headers={"X-Test": "yes"},
    )

    assert observed[:3] == (200, {"resourceType": "Bundle"}, None)
