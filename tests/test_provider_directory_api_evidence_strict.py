# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from scripts.research import provider_directory_api_evidence_harness as harness
from scripts.research import provider_directory_api_evidence_support as support


SOURCE_A = "pdfhir_0123456789abcdef01234567"
SOURCE_C = "pdfhir_111111111111111111111111"


def _manifest():
    return {
        "entries": [
            {
                "entry_id": "acquired",
                "classification": "acquisition",
                "source_ids": [SOURCE_A],
                "resources": [
                    "InsurancePlan",
                    "PractitionerRole",
                    "Organization",
                    "OrganizationAffiliation",
                ],
            }
        ]
    }


def _harness_config(tmp_path, *, require_mapped_evidence=False):
    return harness.HarnessConfig(
        manifest_path=tmp_path / "manifest.json",
        schema="mrf",
        entry_ids=("acquired",),
        source_ids=(),
        max_sources=100,
        samples_per_source=1,
        candidate_limit=5,
        api_latency_slo_ms=0.0,
        require_mapped_evidence=require_mapped_evidence,
    )


def _detail_payload():
    return {
        "data": {
            "npi": {
                "address_list": [
                    {
                        "provider_directory_sources": [
                            {
                                "source": "provider_directory_fhir",
                                "catalog_aliases_verified": False,
                                "source_ids": [SOURCE_A],
                            }
                        ]
                    }
                ]
            }
        }
    }


class FakeConn:
    def __init__(self, row_list):
        self.row_list = row_list

    async def fetch(self, _sql, *_args):
        return self.row_list


class DetailClient:
    def get_json(self, _path, _params):
        return support.HttpResult(200, 1.0, _detail_payload())


@pytest.mark.asyncio
async def test_require_mapped_evidence_reports_inconclusive_without_positive_witness(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())
    conn = FakeConn([{"source_id": SOURCE_A, "npi": 1234567890, "phone_number": None}])

    report = await harness.build_report(
        _harness_config(tmp_path, require_mapped_evidence=True),
        conn,
        support.ApiConfig(None, None, None, "X-API-Key", 3.0, data_only=True),
    )

    assert report["summary"]["required_sources_failed"] == 0
    assert report["summary"]["mapped_role_plan_network_witnesses"] == 0
    assert report["summary"]["mapped_evidence_completion"] == "inconclusive"
    assert report["summary"]["completion_inconclusive"] is True


@pytest.mark.asyncio
async def test_strict_completion_is_inconclusive_without_api_calls(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())

    async def overlay_samples(_conn, **_kwargs):
        return {SOURCE_A: [support.OverlaySample(SOURCE_A, 1234567890, None)]}

    async def mapped_witnesses(_conn, **_kwargs):
        return {
            SOURCE_A: [
                support.MappedEvidenceWitness(
                    SOURCE_A,
                    1234567890,
                    "PractitionerRole",
                    "role-1",
                )
            ]
        }

    monkeypatch.setattr(harness, "fetch_overlay_samples", overlay_samples)
    monkeypatch.setattr(harness, "fetch_mapped_evidence_witnesses", mapped_witnesses)
    report = await harness.build_report(
        _harness_config(tmp_path, require_mapped_evidence=True),
        FakeConn([]),
        support.ApiConfig(None, None, None, "X-API-Key", 3.0, data_only=True),
    )

    assert report["summary"]["mapped_role_plan_network_witnesses"] == 1
    assert report["summary"]["mapped_evidence_passed_capabilities"] == 0
    assert report["summary"]["mapped_evidence_completion"] == "inconclusive"
    assert report["summary"]["completion_inconclusive"] is True


def test_strict_completion_requires_every_selected_declared_capability(tmp_path):
    source_result_list = [
        {
            "mapped_evidence_capabilities": {
                "practitioner_role": {
                    "declared": True,
                    "state": "pass",
                    "completion_witness_count": 1,
                },
                "organization_affiliation": {
                    "declared": False,
                    "state": "not_applicable",
                    "completion_witness_count": 0,
                },
            }
        },
        {
            "mapped_evidence_capabilities": {
                "practitioner_role": {
                    "declared": False,
                    "state": "not_applicable",
                    "completion_witness_count": 0,
                },
                "organization_affiliation": {
                    "declared": True,
                    "state": "not_observed",
                    "completion_witness_count": 0,
                },
            }
        },
    ]

    summary_map = harness.mapped_completion_summary(
        require_mapped_evidence=True,
        source_result_list=source_result_list,
        witness_list_by_source={SOURCE_A: [], SOURCE_C: []},
        witness_probe_error=None,
    )

    assert summary_map["mapped_evidence_declared_capabilities"] == 2
    assert summary_map["mapped_evidence_passed_capabilities"] == 1
    assert summary_map["mapped_evidence_incomplete_capabilities"] == 1
    assert summary_map["completion_inconclusive"] is True


def test_bare_affiliation_does_not_complete_plan_or_network_gate():
    source_result_list = [
        {
            "mapped_evidence_capabilities": {
                "practitioner_role": {
                    "declared": False,
                    "state": "not_applicable",
                    "completion_witness_count": 0,
                },
                "organization_affiliation": {
                    "declared": True,
                    "state": "pass",
                    "completion_witness_count": 0,
                },
            }
        }
    ]

    summary_map = harness.mapped_completion_summary(
        require_mapped_evidence=True,
        source_result_list=source_result_list,
        witness_list_by_source={SOURCE_A: []},
        witness_probe_error=None,
    )

    assert summary_map["mapped_evidence_passed_capabilities"] == 0
    assert summary_map["completion_inconclusive"] is True


@pytest.mark.asyncio
async def test_optional_witness_probe_failure_does_not_fail_baseline(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())

    class FailingWitnessConn:
        def __init__(self):
            self.call_count = 0

        async def fetch(self, _sql, *_args):
            self.call_count += 1
            if self.call_count == 1:
                return [
                    {
                        "source_id": SOURCE_A,
                        "npi": 1234567890,
                        "phone_number": None,
                    }
                ]
            raise RuntimeError("safe test failure")

    report = await harness.build_report(
        _harness_config(tmp_path),
        FailingWitnessConn(),
        support.ApiConfig(
            "https://api.example.test/api/v1",
            "secret-token",
            None,
            "X-API-Key",
            3.0,
        ),
        DetailClient(),
    )

    source_result = report["sources"][0]
    assert source_result["status"] == "pass"
    assert report["summary"]["required_sources_failed"] == 0
    assert report["summary"]["mapped_evidence_probe_failed"] is True
    capability_map = source_result["mapped_evidence_capabilities"]["practitioner_role"]
    assert capability_map["state"] == "not_observed"
    assert capability_map["reason"] == "mapped_evidence_probe_failed"


def test_main_returns_nonzero_for_completion_inconclusive(monkeypatch, capsys):
    async def inconclusive_run(_args):
        return {
            "summary": {
                "required_sources_failed": 0,
                "completion_inconclusive": True,
            }
        }

    monkeypatch.setattr(harness, "run", inconclusive_run)
    assert harness.main(["--require-mapped-evidence"]) == 1
    assert '"completion_inconclusive": true' in capsys.readouterr().out
