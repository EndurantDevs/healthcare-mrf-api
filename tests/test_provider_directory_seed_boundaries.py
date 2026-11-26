# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for seed resolution and synchronous FHIR requests."""

from __future__ import annotations

import importlib
import io
import ssl
import urllib.error
from unittest.mock import Mock

from api.provider_directory_sources import provider_directory_source_catalog

importer = importlib.import_module("process.provider_directory_fhir")


def test_baked_seed_registry_resolves_generated_and_configured_sources_without_remote_io(monkeypatch):
    unavailable = Mock(side_effect=AssertionError("remote catalog must not resolve baked sources"))
    monkeypatch.setattr(importer, "_remote_supplemental_catalogs", unavailable)
    monkeypatch.setattr(importer, "_reviewed_provider_directory_candidate_seed_rows", unavailable)
    monkeypatch.setattr(importer.urllib.request, "urlopen", unavailable)

    seed_rows, metrics_by_catalog = importer.source_selection.baked_seed_rows(importer)
    assert (seed_rows, metrics_by_catalog) == importer.source_selection.baked_seed_rows(importer)
    assert all(seed_row in seed_rows for seed_row in importer.source_selection.configured_seed_rows())
    assert len(seed_rows) == sum(catalog["rows"] for catalog in metrics_by_catalog.values())
    assert list(metrics_by_catalog) == [
        "anthem",
        "health_partners_plans",
        "pacificsource",
        "capital_blue_cross_products",
        "aetna_provider_directorydata",
        "christus_health_plan",
        "mvp_health_care",
        "kaiser",
        "configured_sources",
        "provider_directory_blockers",
    ]
    capital_rows, capital_metrics = importer.source_selection.baked_seed_rows(
        importer, source_query="Capital Blue Cross"
    )
    assert capital_rows == importer._capital_blue_cross_product_seed_rows()
    assert capital_metrics["capital_blue_cross_products"]["rows"] == 2
    assert sum(catalog["rows"] for catalog in capital_metrics.values()) == 2
    source_records = [importer._source_row_from_seed(seed_row) for seed_row in capital_rows]
    assert len({source_record["source_id"] for source_record in source_records}) == 2
    assert len({source_record["api_base"] for source_record in source_records}) == 2
    unavailable.assert_not_called()


def test_optum_catalog_source_resolves_for_named_enrollment():
    entry = next(item for item in provider_directory_source_catalog()["items"] if item["entry_id"] == "uhc")
    seeds, _ = importer.source_selection.baked_seed_rows(importer)
    sources = [importer._source_row_from_seed(seed) for seed in seeds]
    matching_sources = [source for source in sources if source["source_id"] in entry["source_ids"]]
    assert len(matching_sources) == 1
    source = matching_sources[0]

    assert entry["runnable"] is True
    assert importer._resource_acquisition_blocked_reason(source) is None
    endpoint = importer._provider_directory_api_endpoint_row(source)
    assert endpoint["canonical_api_base"] == entry["canonical_base"]


def test_supplemental_catalog_preserves_order_and_metrics_through_baked_registry(monkeypatch):
    reviewed_seed_by_field = {"id": "reviewed"}
    remote_seed_by_field = {"id": "remote"}
    baked_seed_by_field = {"id": "baked"}
    remote_metrics_by_catalog = {"remote": {"source": "remote", "rows": 1}}
    baked_metrics_by_catalog = {"baked": {"source": "baked", "rows": 1}}
    monkeypatch.setattr(
        importer, "_reviewed_provider_directory_candidate_seed_rows", Mock(return_value=[reviewed_seed_by_field])
    )
    monkeypatch.setattr(
        importer,
        "_remote_supplemental_catalogs",
        Mock(return_value=([remote_seed_by_field], remote_metrics_by_catalog)),
    )
    resolver = Mock(return_value=([baked_seed_by_field], baked_metrics_by_catalog))
    monkeypatch.setattr(importer.source_selection, "baked_seed_rows", resolver)

    seed_rows, metrics_by_catalog = importer._seed_rows_from_supplemental_catalogs(source_query="Example")

    assert seed_rows == [reviewed_seed_by_field, remote_seed_by_field, baked_seed_by_field]
    assert metrics_by_catalog == {
        "catalogs": {
            "reviewed_provider_directory_candidates": {
                "source": importer.REVIEWED_PROVIDER_DIRECTORY_CANDIDATE_SOURCE,
                "rows": 1,
            },
            **remote_metrics_by_catalog,
            **baked_metrics_by_catalog,
        },
        "rows": 3,
    }
    resolver.assert_called_once_with(importer, source_query="Example")


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

    assert importer._dedupe_source_rows([retained_map, duplicate_map, anonymous_map]) == [retained_map, anonymous_map]


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
    assert "offline" in (importer._fetch_json_sync("https://example.test/fhir", timeout=1)[2] or "")


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
