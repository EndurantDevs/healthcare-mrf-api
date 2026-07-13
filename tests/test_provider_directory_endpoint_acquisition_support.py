import urllib.error

import pytest

from scripts.research import provider_directory_endpoint_acquisition_support as support


class _JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return self.payload


def test_import_control_get_retries_transient_transport_failure(monkeypatch):
    attempts = iter(
        [
            urllib.error.URLError("policy not ready"),
            _JsonResponse(b'{"run_id":"run_ready"}'),
        ]
    )
    retry_delays = []

    def open_request(*_args, **_kwargs):
        result = next(attempts)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(support.urllib.request, "urlopen", open_request)
    monkeypatch.setattr(support.time, "sleep", retry_delays.append)
    client = support.ImportControlHttpClient(
        "http://import-control:8095",
        "TEST_IMPORT_CONTROL_TOKEN",
    )

    assert client.get_run("run_ready") == {"run_id": "run_ready"}
    assert retry_delays == [support.CONTROL_GET_RETRY_DELAYS_SECONDS[0]]


def test_import_control_post_does_not_retry_ambiguous_transport_failure(
    monkeypatch,
):
    request_calls = []
    retry_delays = []

    def reject_request(*_args, **_kwargs):
        request_calls.append(True)
        raise urllib.error.URLError("response lost")

    monkeypatch.setattr(support.urllib.request, "urlopen", reject_request)
    monkeypatch.setattr(support.time, "sleep", retry_delays.append)
    client = support.ImportControlHttpClient(
        "http://import-control:8095",
        "TEST_IMPORT_CONTROL_TOKEN",
    )

    with pytest.raises(RuntimeError, match="import-control request failed"):
        client.create_run({"importer": "provider-directory-fhir"})

    assert len(request_calls) == 1
    assert retry_delays == []


def _scan_acquisition_entry():
    return {
        "canonical_base": support.SCAN_PROVIDER_DIRECTORY_BASE,
        "classification": "acquisition",
        "source_ids": ["pdfhir_736ccaa7958218d4daeaf2e6"],
        "resources": ["Practitioner"],
    }


def _scan_success_metrics(entry):
    source_ids = list(entry["source_ids"])
    return {
        "source_ids": source_ids,
        "source_import_sources_selected": len(source_ids),
        "source_import_groups_attempted": 1,
        "resource_fetch_completed_source_ids": {
            "Practitioner": source_ids,
        },
        "resource_fetch_stats": {
            "Practitioner": {
                "sources_completed": 1,
                "sources_bounded": 0,
                "sources_failed": 0,
            }
        },
    }


def _add_last_updated_proof(resource_stats, count):
    resource_stats.update(
        {
            "last_updated_partition_sources": 1,
            "last_updated_completeness_verified_sources": 1,
            **{
                metric_name: count
                for metric_name in support.LAST_UPDATED_PROOF_METRIC_NAMES
            },
        }
    )


@pytest.mark.parametrize("resource_count", [0, 17])
def test_scan_metrics_require_reconciled_last_updated_proof(resource_count):
    entry = _scan_acquisition_entry()
    metrics = _scan_success_metrics(entry)
    _add_last_updated_proof(
        metrics["resource_fetch_stats"]["Practitioner"],
        resource_count,
    )

    assert support.acquisition_metric_errors(entry, metrics) == []


def test_scan_metrics_reject_missing_or_drifted_last_updated_proof():
    entry = _scan_acquisition_entry()
    metrics = _scan_success_metrics(entry)

    missing_errors = support.acquisition_metric_errors(entry, metrics)

    assert any(
        "lacks verified last-updated completeness proof" in error
        for error in missing_errors
    )
    _add_last_updated_proof(
        metrics["resource_fetch_stats"]["Practitioner"],
        17,
    )
    metrics["resource_fetch_stats"]["Practitioner"][
        "last_updated_unfiltered_post"
    ] = 18

    assert (
        "Practitioner last-updated completeness counts do not reconcile"
        in support.acquisition_metric_errors(entry, metrics)
    )
