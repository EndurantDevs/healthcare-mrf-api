# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
from pathlib import Path

from scripts.research import provider_directory_api_evidence_db as evidence_db
from scripts.research import provider_directory_api_evidence_matrix as matrix
from scripts.research import provider_directory_api_evidence_support as support


SOURCE_ID = "pdfhir_0123456789abcdef01234567"
ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"
PROVENANCE = support.SourceProvenance(
    SOURCE_ID,
    "endpoint-1",
    "dataset-1",
    "https://directory.example.test/fhir",
    "Example Directory",
    "Example Plan",
)


def _source_summary() -> dict:
    return {
        "source": "provider_directory_fhir",
        "catalog_aliases_verified": False,
        "source_ids": [SOURCE_ID],
    }


def _build_provenance_entry(**field_overrides) -> dict:
    provenance_entry_dict = {
        "source_id": SOURCE_ID,
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "api_base": "https://directory.example.test/fhir",
        "org_name": "Example Directory",
        "plan_name": "Example Plan",
    }
    provenance_entry_dict.update(field_overrides)
    return provenance_entry_dict


def _build_detail_response(
    *, endpoint_id: str = "endpoint-1", detail_geo_present: bool = True
) -> dict:
    source_provenance_dict = _build_provenance_entry(endpoint_id=endpoint_id)
    fact_evidence_dict = {
        **source_provenance_dict,
        "resource_type": "PractitionerRole",
        "resource_id": "role-1",
    }
    return {
        "data": {
            "npi": {
                "address_list": [
                    {
                        "address_key": ADDRESS_KEY,
                        "lat": 40.0 if detail_geo_present else None,
                        "long": -75.0 if detail_geo_present else None,
                        "provider_directory_sources": [_source_summary()],
                    }
                ],
                "provider_directory_profile": {"sources": [source_provenance_dict]},
                "provider_directory_profile_evidence": {
                    "sources": [source_provenance_dict],
                    "facts": {
                        "accepting_patients": {
                            "items": [{"evidence": [fact_evidence_dict]}]
                        }
                    },
                },
            }
        }
    }


def _candidate_payload(*, npi: int = 1234567890, count: int = 1) -> dict:
    return {
        "data": {
            "candidates": [
                {
                    "npi": npi + index,
                    "provider_directory_sources": [_source_summary()],
                }
                for index in range(count)
            ]
        }
    }


class MatrixClient:
    def __init__(
        self,
        *,
        endpoint_id: str = "endpoint-1",
        latency_ms: float = 1.0,
        geo_candidate_present: bool = True,
        detail_geo_present: bool = True,
    ):
        self.endpoint_id = endpoint_id
        self.latency_ms = latency_ms
        self.geo_candidate_present = geo_candidate_present
        self.detail_geo_present = detail_geo_present
        self.calls = []

    def get_json(self, path, params):
        self.calls.append((path, dict(params)))
        is_candidate_request = path.endswith("match-candidates")
        is_geo_request = is_candidate_request and "lat" in params
        payload = (
            _candidate_payload(npi=9876543210, count=20)
            if is_geo_request and not self.geo_candidate_present
            else _candidate_payload()
            if is_candidate_request
            else _build_detail_response(
                endpoint_id=self.endpoint_id,
                detail_geo_present=self.detail_geo_present,
            )
        )
        return support.HttpResult(200, self.latency_ms, payload)


def _selection() -> support.SourceSelection:
    return support.SourceSelection(
        "example",
        SOURCE_ID,
        "acquisition",
        True,
        ("Practitioner", "PractitionerRole"),
        "R4",
        ("A", "P", "G", "F", "V"),
        ("Practitioner", "PractitionerRole"),
    )


def _build_overlay_sample(**field_overrides) -> support.OverlaySample:
    sample_kwargs_by_name = {
        "source_id": SOURCE_ID,
        "npi": 1234567890,
        "phone": "5550101234",
        "address_key": ADDRESS_KEY,
        "latitude": 40.0,
        "longitude": -75.0,
    }
    sample_kwargs_by_name.update(field_overrides)
    return support.OverlaySample(**sample_kwargs_by_name)


def test_matrix_api_checks_pass_and_mask_phone():
    client = MatrixClient()
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample()],
        client,
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    assert result["status"] == "pass"
    assert set(result["verification_matrix"][0]) == {"A", "P", "G", "F", "V"}
    assert {check["state"] for check in result["verification_matrix"][0].values()} == {"pass"}
    assert result["expected_fhir_provenance"] == _build_provenance_entry()
    assert [path for path, _params in client.calls] == [
        "providers/1234567890",
        "providers/match-candidates",
        "providers/match-candidates",
    ]
    assert client.calls[0][1]["address_key"] == ADDRESS_KEY
    assert client.calls[2][1]["radius_miles"] == "0.1"
    assert "5550101234" not in json.dumps(result)


def test_matrix_fails_when_profile_provenance_does_not_match_current_dataset():
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample()],
        MatrixClient(endpoint_id="endpoint-other"),
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    checks = result["verification_matrix"][0]
    assert result["status"] == "fail"
    assert checks["F"]["reason"] == "profile_fact_evidence_not_found"
    assert checks["V"]["reason"] == "exact_profile_provenance_not_found"


def test_matrix_accepts_detail_geo_without_candidate():
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample()],
        MatrixClient(geo_candidate_present=False),
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    checks = result["verification_matrix"][0]
    assert result["status"] == "pass"
    assert {checks[code]["state"] for code in ("A", "P", "F", "V")} == {
        "pass"
    }
    assert checks["G"] == {
        "state": "pass",
        "geo_match_candidates": {"status_code": 200, "latency_ms": 1.0},
        "geo_candidate_source_present": False,
        "geo_detail": {"status_code": 200, "latency_ms": 1.0},
    }


def test_matrix_fails_when_exact_detail_geo_is_missing():
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample()],
        MatrixClient(detail_geo_present=False),
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    checks = result["verification_matrix"][0]
    assert result["status"] == "fail"
    assert checks["G"] == {
        "state": "fail",
        "reason": "exact_geo_provenance_not_found",
        "geo_match_candidates": {"status_code": 200, "latency_ms": 1.0},
        "geo_candidate_source_present": True,
        "geo_detail": {"status_code": 200, "latency_ms": 1.0},
    }


def test_matrix_marks_missing_overlay_inputs_without_silent_passes():
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample(phone=None, address_key=None, latitude=None, longitude=None)],
        MatrixClient(),
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    checks = result["verification_matrix"][0]
    assert result["status"] == "fail"
    assert checks["A"]["state"] == "missing_data"
    assert checks["P"]["state"] == "missing_data"
    assert checks["G"]["state"] == "missing_data"


def test_matrix_fails_all_http_checks_when_latency_exceeds_slo():
    result = support.evaluate_source(
        _selection(),
        [_build_overlay_sample()],
        MatrixClient(latency_ms=40.01),
        support.SourceEvaluationContext(5, 40.0),
        expected_provenance=PROVENANCE,
    )

    checks = result["verification_matrix"][0]
    assert result["status"] == "fail"
    assert {checks[code]["state"] for code in ("A", "P", "G", "F", "V")} == {"fail"}


def test_profile_source_matrix_selects_all_22_and_expresses_alohr_graphql_contract():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (root / "specs/provider_directory_endpoint_acquisition_manifest.json").read_text()
    )
    profile_spec = matrix.load_matrix_source_spec()
    selections = matrix.resolve_matrix_source_selection(manifest, profile_spec)

    assert len(selections) == 22
    alohr = next(selection for selection in selections if selection.entry_id == "alohr")
    assert alohr.resource_profile == "ALOHR_GRAPHQL_R4"
    assert alohr.resources == (
        "Practitioner",
        "Organization",
        "Location",
        "PractitionerRole",
    )
    assert "OrganizationAffiliation" not in alohr.resources
    assert all(selection.matrix_checks == ("A", "P", "G", "F", "V") for selection in selections)


def test_current_provenance_query_fences_one_published_dataset_per_source():
    sql = evidence_db.current_source_provenance_sql("mrf")

    assert '"mrf".provider_directory_source' in sql
    assert '"mrf".provider_directory_api_endpoint' in sql
    assert '"mrf".provider_directory_endpoint_dataset' in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "HAVING COUNT(*) = 1" in sql
    assert "COALESCE(" in sql
    assert "endpoint.canonical_api_base" in sql
