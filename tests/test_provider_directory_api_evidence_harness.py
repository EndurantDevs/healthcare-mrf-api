# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import io
import json
import urllib.error

import pytest

from scripts.research import provider_directory_api_evidence_harness as harness
from scripts.research import provider_directory_api_evidence_support as support


SOURCE_A = "pdfhir_0123456789abcdef01234567"
SOURCE_B = "pdfhir_abcdef0123456789abcdef01"
SOURCE_C = "pdfhir_111111111111111111111111"


def _manifest():
    return {
        "entries": [
            {
                "entry_id": "acquired",
                "classification": "acquisition",
                "source_ids": [SOURCE_A],
            },
            {
                "entry_id": "probe",
                "classification": "probe_only",
                "source_ids": [SOURCE_B],
            },
            {
                "entry_id": "external",
                "classification": "external",
                "source_ids": [SOURCE_C],
            },
        ]
    }


def _source_summary_map(source_id, *, source_ids=False):
    summary_map = {
        "source": "provider_directory_fhir",
        "catalog_aliases_verified": False,
        "catalog_aliases": [{"source_id": source_id, "org_name": "Example"}],
        "practitioner_roles": [{"resource_id": "role-1"}],
        "insurance_plans": [{"resource_id": "plan-1"}],
        "networks": [{"resource_id": "network-1"}],
        "evidence_metadata": {"returned": 1},
    }
    if source_ids:
        summary_map["source_ids"] = [source_id]
    return summary_map


def _detail_payload(source_id):
    return {
        "data": {
            "npi": {
                "address_list": [
                    {
                        "provider_directory_sources": [
                            _source_summary_map(source_id, source_ids=True)
                        ]
                    }
                ]
            }
        }
    }


def _candidate_payload(source_id):
    return {
        "data": {
            "candidates": [
                {
                    "npi": 1234567890,
                    "provider_directory_sources": [_source_summary_map(source_id)],
                }
            ]
        }
    }


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return self.rows


class FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self.payload = payload

    def getcode(self):
        return self.status

    def read(self, _limit):
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _api_config(base_url="https://api.example.test/api/v1", **overrides):
    return support.ApiConfig(
        base_url=base_url,
        bearer_token="very-secret-token",
        api_key=None,
        api_key_header="X-API-Key",
        timeout_seconds=3.0,
        **overrides,
    )


def _harness_config(tmp_path, entry_ids=("acquired",), max_sources=100):
    return harness.HarnessConfig(
        manifest_path=tmp_path / "manifest.json",
        schema="mrf",
        entry_ids=entry_ids,
        source_ids=(),
        max_sources=max_sources,
        samples_per_source=1,
        candidate_limit=5,
        api_latency_slo_ms=0.0,
    )


def test_selection_carries_required_state_and_rejects_truncation():
    selections = harness.resolve_source_selection(_manifest(), max_sources=3)

    assert [(item.classification, item.required) for item in selections] == [
        ("acquisition", True),
        ("probe_only", False),
        ("external", True),
    ]
    with pytest.raises(ValueError, match="exceed max_sources"):
        harness.resolve_source_selection(_manifest(), max_sources=2)


def test_overlay_query_is_current_deterministic_and_has_no_role_scan():
    sql = harness.overlay_sample_sql("mrf")

    assert '"mrf".provider_directory_address_overlay' in sql
    assert "overlay.source_id = current_source.source_id" in sql
    assert "overlay.last_seen_run_id = current_source.run_id" in sql
    assert "SELECT DISTINCT ON (overlay.npi)" in sql
    assert "(overlay.address_key IS NOT NULL) DESC" in sql
    assert "(NULLIF(overlay.phone_number, '') IS NOT NULL) DESC" in sql
    assert "LIMIT $2" in sql
    assert "provider_directory_practitioner_role" not in sql


@pytest.mark.asyncio
async def test_required_missing_evidence_fails_but_probe_only_missing_evidence_skips(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())

    report = await harness.build_report(
        _harness_config(tmp_path, entry_ids=("acquired", "probe")),
        FakeConn([]),
        support.ApiConfig(None, None, None, "X-API-Key", 3.0, data_only=True),
    )

    acquired, probe = report["sources"]
    assert (acquired["status"], acquired["required"]) == ("fail", True)
    assert acquired["reason"] == "required_current_overlay_dataset_evidence_not_found"
    assert (probe["status"], probe["required"]) == ("skip", False)
    assert report["summary"]["required_sources_failed"] == 1


@pytest.mark.asyncio
async def test_api_layer_routes_envelopes_and_typed_source_variants(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())
    conn = FakeConn(
        [{"source_id": SOURCE_A, "npi": 1234567890, "phone_number": "5550101234"}]
    )
    observed_requests = []

    def opener(request, timeout):
        observed_requests.append(
            (request.full_url, dict(request.header_items()), timeout)
        )
        if request.full_url.startswith(
            "https://api.example.test/api/v1/providers/match-candidates?"
        ):
            return FakeResponse(200, _candidate_payload(SOURCE_A))
        return FakeResponse(200, _detail_payload(SOURCE_A))

    report = await harness.build_report(
        _harness_config(tmp_path),
        conn,
        _api_config(),
        support.ProviderDirectoryApiClient(_api_config(), opener=opener),
    )

    source_result = report["sources"][0]
    assert source_result["status"] == "pass"
    assert source_result["checks"][0]["detail_source_present"] is True
    assert source_result["checks"][0]["phone_source_present"] is True
    assert source_result["checks"][0]["detail_within_latency_slo"] is True
    assert source_result["checks"][0]["phone_within_latency_slo"] is True
    assert [url.split("?")[0] for url, _, _ in observed_requests] == [
        "https://api.example.test/api/v1/providers/1234567890",
        "https://api.example.test/api/v1/providers/match-candidates",
    ]
    assert all(
        "include_sources=true" in url and "include_evidence=true" in url
        for url, _, _ in observed_requests
    )
    assert any(
        dict(headers).get("Authorization") == "Bearer very-secret-token"
        for _, headers, _ in observed_requests
    )
    assert "very-secret-token" not in json.dumps(report)
    assert "5550101234" not in json.dumps(report)


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
        candidate_limit=5,
        api_latency_slo_ms=40.0,
        api_skip_reason=None,
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
