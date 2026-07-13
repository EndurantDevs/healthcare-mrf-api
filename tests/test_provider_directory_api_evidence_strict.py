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

    assert report["summary"]["mapped_plan_network_context_witnesses"] == 0
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


def test_bare_affiliation_completes_its_capability_without_plan_network_context():
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

    assert summary_map["mapped_evidence_passed_capabilities"] == 1
    assert summary_map["mapped_plan_network_context_witnesses"] == 0
    assert summary_map["completion_inconclusive"] is False


def _completed_empty_row(*, resource_type="OrganizationAffiliation"):
    diagnostic_map = {
        "complete": True,
        "bounded": False,
        "error": None,
        "next_url_remaining": False,
        "rows_fetched": 0,
        "rows_written": 0,
    }
    return {
        "source_id": SOURCE_A,
        "resource_type": resource_type,
        "dataset_id": "dataset-arkansas",
        "acquisition_root_run_id": "run-root",
        "terminal_run_id": "run-root",
        "dataset_resource_count": 0,
        "terminal_importer": "provider-directory-fhir",
        "terminal_status": "succeeded",
        "terminal_finished_at": "2026-07-13T12:00:00Z",
        "terminal_error": None,
        "terminal_params": {
            "source_ids": [SOURCE_A],
            "resources": resource_type,
        },
        "terminal_metrics": {
            "source_ids": [SOURCE_A],
            "resource_rows": {resource_type: 0},
            "resource_fetch_completed_source_ids": {resource_type: [SOURCE_A]},
            "resource_fetch_stats": {
                resource_type: {
                    "sources_attempted": 1,
                    "sources_completed": 1,
                    "sources_bounded": 0,
                    "sources_failed": 0,
                    "sources_empty": 1,
                    "rows_fetched": 0,
                }
            },
        },
        "publication_metadata_json": {
            "completion_proof_v1": {
                "acquisition_root_run_id": "run-root",
                "terminal_run_id": "run-root",
                "source_ids": [SOURCE_A],
                "selected_resources": [resource_type],
                "resource_diagnostics": {resource_type: diagnostic_map},
            }
        },
    }


def test_arkansas_completed_empty_proof_completes_declared_affiliation():
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    proof = evidence_db._completion_proof_from_row(
        _completed_empty_row(), SOURCE_A, "OrganizationAffiliation"
    )
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("OrganizationAffiliation",)
    )
    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        None,
        support.SourceEvaluationContext(5, 0.0, "data_only_mode"),
        completion_proofs={"OrganizationAffiliation": proof},
    )

    capability = source_result["mapped_evidence_capabilities"][
        "organization_affiliation"
    ]
    assert proof["state"] == "completed_empty"
    assert capability["state"] == "completed_empty"
    summary_map = harness.mapped_completion_summary(
        require_mapped_evidence=True,
        source_result_list=[source_result],
        witness_list_by_source={SOURCE_A: []},
        witness_probe_error=None,
    )
    assert summary_map["mapped_evidence_completion"] == "pass"


def test_positive_standalone_affiliation_is_not_a_provider_surface_gap():
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    row = _completed_empty_row()
    row["dataset_resource_count"] = 7
    row["provider_surface_evidence_present"] = False

    proof = evidence_db._completion_proof_from_row(
        row, SOURCE_A, "OrganizationAffiliation"
    )

    assert proof == {
        "state": "provider_surface_not_applicable",
        "dataset_id": "dataset-arkansas",
        "dataset_resource_count": 7,
    }


def test_positive_affiliation_with_provider_evidence_stays_strict():
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    row = _completed_empty_row()
    row["dataset_resource_count"] = 7
    row["provider_surface_evidence_present"] = True

    proof = evidence_db._completion_proof_from_row(
        row, SOURCE_A, "OrganizationAffiliation"
    )

    assert proof["state"] == "positive"


def test_current_legacy_metadata_is_accepted_only_with_complete_empty_diagnostics():
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    row = _completed_empty_row()
    row["publication_metadata_json"] = row["publication_metadata_json"][
        "completion_proof_v1"
    ]

    assert (
        evidence_db._completion_proof_from_row(
            row, SOURCE_A, "OrganizationAffiliation"
        )["state"]
        == "completed_empty"
    )


def test_database_json_null_terminal_error_is_accepted():
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    row = _completed_empty_row()
    row["terminal_error"] = "null"

    assert (
        evidence_db._completion_proof_from_row(
            row, SOURCE_A, "OrganizationAffiliation"
        )["state"]
        == "completed_empty"
    )


@pytest.mark.parametrize(
    ("mutate", "expected_state"),
    [
        (lambda row: row.pop("publication_metadata_json"), "unproven"),
        (
            lambda row: row["publication_metadata_json"]["completion_proof_v1"].update(
                source_ids=[SOURCE_C]
            ),
            "unproven",
        ),
        (
            lambda row: row["publication_metadata_json"]["completion_proof_v1"][
                "resource_diagnostics"
            ]["OrganizationAffiliation"].update(bounded=True),
            "unproven",
        ),
        (
            lambda row: row["publication_metadata_json"]["completion_proof_v1"][
                "resource_diagnostics"
            ]["OrganizationAffiliation"].update(error="http_500"),
            "unproven",
        ),
        (lambda row: row.update(dataset_resource_count=1), "positive"),
    ],
)
def test_current_dataset_completion_reader_fails_closed(mutate, expected_state):
    from scripts.research import provider_directory_api_evidence_db as evidence_db

    row = _completed_empty_row()
    mutate(row)
    assert (
        evidence_db._completion_proof_from_row(
            row, SOURCE_A, "OrganizationAffiliation"
        )["state"]
        == expected_state
    )


@pytest.mark.asyncio
async def test_completion_probe_failure_is_inconclusive(monkeypatch, tmp_path):
    monkeypatch.setattr(harness, "load_manifest", lambda _path: _manifest())

    async def overlay_samples(_conn, **_kwargs):
        return {SOURCE_A: [support.OverlaySample(SOURCE_A, 1234567890, None)]}

    async def mapped_witnesses(_conn, **_kwargs):
        return {SOURCE_A: []}

    async def failed_completion_probe(_conn, **_kwargs):
        raise RuntimeError("safe test failure")

    monkeypatch.setattr(harness, "fetch_overlay_samples", overlay_samples)
    monkeypatch.setattr(harness, "fetch_mapped_evidence_witnesses", mapped_witnesses)
    monkeypatch.setattr(
        harness, "fetch_current_dataset_completion_proofs", failed_completion_probe
    )
    report = await harness.build_report(
        _harness_config(tmp_path, require_mapped_evidence=True),
        FakeConn([]),
        support.ApiConfig(None, None, None, "X-API-Key", 3.0, data_only=True),
    )

    capability = report["sources"][0]["mapped_evidence_capabilities"][
        "organization_affiliation"
    ]
    assert capability["reason"] == "current_dataset_completion_probe_failed"
    assert report["summary"]["current_dataset_completion_probe_failed"] is True
    assert report["summary"]["completion_inconclusive"] is True


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
