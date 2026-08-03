# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import io
import json
import urllib.error

import pytest

from scripts.research import provider_directory_api_evidence_harness as harness
from scripts.research import provider_directory_api_evidence_support as support
from tests.test_provider_directory_api_evidence_harness import (
    AddressAwareClient,
    SOURCE_A,
    _api_config,
    _detail_payload,
    _source_summary_map,
)

def test_mapped_witness_detail_sends_exact_address_key():
    """Strict witness detail checks request only the proven address."""
    address_key = "00000000-0000-0000-0000-000000000001"
    witness = support.MappedEvidenceWitness(
        SOURCE_A,
        1234567890,
        "PractitionerRole",
        "role-1",
        address_key=address_key,
    )

    client = AddressAwareClient(SOURCE_A, address_key)
    witness_check = support._evaluate_witness(witness, client, 40.0)

    assert witness_check["detail_evidence_present"] is True
    assert witness_check["provider_search_evidence_present"] is True
    assert client.request_list == [
        (
            "providers/1234567890",
            {
                "include_sources": "true",
                "include_evidence": "true",
                "address_key": address_key,
            },
        ),
        (
            "providers",
            {
                "npi": "1234567890",
                "include_sources": "true",
                "include_evidence": "true",
                "address_key": address_key,
            },
        ),
    ]


@pytest.mark.parametrize(
    ("latency_ms", "latency_slo_ms", "expected"),
    [(40.0, 40.0, True), (40.01, 40.0, False), (400.0, 0.0, True)],
)
def test_latency_slo_boundary(latency_ms, latency_slo_ms, expected):
    result = support.HttpResult(200, latency_ms, {}, None)

    assert support.is_within_latency_slo(result, latency_slo_ms) is expected


def test_required_source_fails_when_successful_api_response_exceeds_latency_slo():
    selection = support.SourceSelection("acquired", SOURCE_A, "acquisition", True)
    sample = support.OverlaySample(SOURCE_A, 1234567890, None)

    class SlowClient:
        def get_json(self, _path, _params):
            return support.HttpResult(200, 40.01, _detail_payload(SOURCE_A), None)

    source_result = support.evaluate_source(
        selection,
        [sample],
        SlowClient(),
        support.SourceEvaluationContext(5, 40.0),
    )

    assert source_result["status"] == "fail"
    assert source_result["checks"][0]["detail_source_present"] is True
    assert source_result["checks"][0]["detail_within_latency_slo"] is False


def test_source_provenance_requires_fhir_and_accepts_both_id_shapes():
    provider_row_map = {
        "provider_directory_sources": [_source_summary_map(SOURCE_A, source_ids=True)]
    }

    assert support.has_row_source_provenance(provider_row_map, SOURCE_A) is True
    provider_row_map["provider_directory_sources"][0]["catalog_aliases_verified"] = True
    assert support.has_row_source_provenance(provider_row_map, SOURCE_A) is False
    provider_row_map["provider_directory_sources"][0][
        "catalog_aliases_verified"
    ] = False
    provider_row_map["provider_directory_sources"][0]["source"] = "not_fhir"
    assert support.has_row_source_provenance(provider_row_map, SOURCE_A) is False


def test_http_error_and_report_redaction_never_include_raw_secret_text():
    def opener(_request, timeout):
        assert timeout == 3.0
        raise urllib.error.HTTPError(
            "https://api.example.test?token=very-secret-token",
            401,
            "Unauthorized",
            {},
            io.BytesIO(b'{"message":"very-secret-token"}'),
        )

    result = support.ProviderDirectoryApiClient(_api_config(), opener=opener).get_json(
        "providers/1", {}
    )
    assert (result.status_code, result.error) == (401, "http_error")
    assert "very-secret-token" not in json.dumps(result.__dict__)
    assert harness.redact_sensitive(
        {"headers": {"Authorization": "secret"}, "ok": 1}
    ) == {"ok": 1}


def test_main_returns_nonzero_only_for_required_source_failures(monkeypatch, capsys):
    async def failed_run(_args):
        return {"summary": {"required_sources_failed": 1}}

    monkeypatch.setattr(harness, "run", failed_run)
    assert harness.main([]) == 1
    assert '"required_sources_failed": 1' in capsys.readouterr().out
