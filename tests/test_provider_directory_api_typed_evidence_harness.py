# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from scripts.research import provider_directory_api_evidence_db as evidence_db
from scripts.research import provider_directory_api_evidence_harness as harness
from scripts.research import provider_directory_api_evidence_support as support


SOURCE_A = "pdfhir_0123456789abcdef01234567"
ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"


def _source_summary_map(*, network_name="Example Network"):
    network_map = {
        "resource_type": "Organization",
        "resource_id": "network-1",
        "reference": "Organization/network-1",
    }
    if network_name is not None:
        network_map["name"] = network_name
    return {
        "source": "provider_directory_fhir",
        "catalog_aliases_verified": False,
        "catalog_aliases": [{"source_id": SOURCE_A, "org_name": "Example"}],
        "source_ids": [SOURCE_A],
        "practitioner_role_ids": ["role-1"],
        "practitioner_roles": [
            {
                "resource_type": "PractitionerRole",
                "source_id": SOURCE_A,
                "resource_id": "role-1",
            }
        ],
        "organization_affiliation_ids": ["affiliation-1"],
        "insurance_plans": [{"resource_id": "plan-1"}],
        "networks": [network_map],
    }


def _detail_payload():
    return {
        "data": {
            "npi": {
                "address_list": [
                    {"provider_directory_sources": [_source_summary_map()]}
                ]
            }
        }
    }


def _search_payload(*, network_name="Example Network"):
    return {
        "data": {
            "items": [
                {
                    "npi": 1234567890,
                    "address_list": [
                        {
                            "provider_directory_sources": [
                                _source_summary_map(network_name=network_name)
                            ]
                        }
                    ],
                }
            ]
        }
    }


def _typed_witness_list():
    common_evidence = (
        ("plan-1",),
        (support.NetworkWitness("network-1", True, True),),
    )
    return [
        support.MappedEvidenceWitness(
            SOURCE_A,
            1234567890,
            "PractitionerRole",
            "role-1",
            *common_evidence,
            address_key=ADDRESS_KEY,
        ),
        support.MappedEvidenceWitness(
            SOURCE_A,
            1234567890,
            "OrganizationAffiliation",
            "affiliation-1",
            *common_evidence,
            address_key=ADDRESS_KEY,
        ),
    ]


class SequencedEvidenceConn:
    def __init__(self):
        self.calls = []
        self.response_list = [
            [
                {
                    "source_id": SOURCE_A,
                    "resource_type": "PractitionerRole",
                    "resource_id": "role-1",
                    "npi": 1234567890,
                    "address_key": ADDRESS_KEY,
                }
            ],
            [
                {
                    "source_id": SOURCE_A,
                    "role_id": "role-1",
                    "evidence_type": "role",
                    "resource_id": "role-1",
                },
                {
                    "source_id": SOURCE_A,
                    "role_id": "role-1",
                    "evidence_type": "insurance_plan",
                    "resource_id": "plan-1",
                },
                {
                    "source_id": SOURCE_A,
                    "role_id": "role-1",
                    "evidence_type": "network",
                    "resource_id": "network-1",
                    "name": "Example Network",
                    "reference": "Organization/network-1",
                },
            ],
        ]

    async def fetch(self, sql, *args):
        self.calls.append((sql, args))
        return self.response_list.pop(0)


class TypedEvidenceClient:
    def __init__(self, *, search_network_name="Example Network"):
        self.calls = []
        self.search_network_name = search_network_name

    def get_json(self, path, params):
        self.calls.append((path, params))
        response_payload = (
            _search_payload(network_name=self.search_network_name)
            if path == "providers"
            else _detail_payload()
        )
        return support.HttpResult(200, 1.0, response_payload)


def test_mapped_witness_query_is_current_dataset_fenced_and_per_source_bounded():
    sql = evidence_db.mapped_evidence_candidate_sql("mrf")

    assert "current_endpoint_counts AS MATERIALIZED" in sql
    assert "HAVING COUNT(*) = 1" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert '"mrf".provider_directory_dataset_resource' in sql
    assert "overlay.source_id = current_source.source_id" in sql
    assert "overlay.last_seen_run_id = current_source.run_id" in sql
    assert "overlay.resource_type = requested.resource_type" in sql
    assert "ORDER BY overlay.resource_id, overlay.npi, overlay.address_key" in sql
    assert "LIMIT $3" in sql
    assert "provider_directory_practitioner_role" not in sql

    affiliation_sql = evidence_db._asyncpg_affiliation_evidence_sql("mrf")
    assert "requested_affiliations AS" in affiliation_sql
    assert "WHERE resource.resource_type = 'OrganizationAffiliation'" in affiliation_sql
    assert "LIMIT 8192" in affiliation_sql
    assert ":affiliation_ids" not in affiliation_sql


def test_current_dataset_completion_query_fences_one_dataset_per_source():
    sql = evidence_db.current_dataset_completion_sql("mrf")

    assert "requested_sources AS MATERIALIZED" in sql
    assert "SELECT DISTINCT source_id FROM requested_resources" in sql
    assert "HAVING COUNT(*) = 1" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert 'LEFT JOIN "mrf".import_run AS terminal_run' in sql
    assert "resource.dataset_id = dataset.dataset_id" in sql
    assert "provider_surface_evidence_present" in sql
    assert "overlay.source_id = requested.source_id" in sql
    assert "overlay.last_seen_run_id = dataset.acquisition_root_run_id" in sql
    assert "overlay.resource_type = requested.resource_type" in sql


@pytest.mark.asyncio
async def test_mapped_witness_fetch_uses_exact_production_role_builder():
    """The bounded probe must reuse the same exact-key SQL as API serving."""
    conn = SequencedEvidenceConn()
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("PractitionerRole",)
    )

    witness_list_by_source = await evidence_db.fetch_mapped_evidence_witnesses(
        conn,
        schema="mrf",
        selections=[selection],
        witnesses_per_resource=1,
    )

    role_sql, role_args = conn.calls[1]
    assert len(conn.calls) == 2
    assert "requested_roles AS" in role_sql
    assert "WHERE resource.resource_type = 'PractitionerRole'" in role_sql
    assert "LIMIT 8192" in role_sql
    assert ":role_ids" not in role_sql
    assert role_args == ([SOURCE_A], ["role-1"])
    assert witness_list_by_source[SOURCE_A] == [_typed_witness_list()[0]]


def test_positive_typed_witnesses_pass_detail_and_search_surfaces():
    """Both public provider surfaces must expose each exact typed witness."""
    selection = support.SourceSelection(
        "acquired",
        SOURCE_A,
        "acquisition",
        True,
        ("PractitionerRole", "OrganizationAffiliation"),
    )
    client = TypedEvidenceClient()

    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        client,
        support.SourceEvaluationContext(5, 40.0),
        witnesses=_typed_witness_list(),
    )

    assert source_result["status"] == "pass"
    capability_by_name = source_result["mapped_evidence_capabilities"]
    assert capability_by_name["practitioner_role"]["state"] == "pass"
    assert capability_by_name["organization_affiliation"]["state"] == "pass"
    search_call_list = [call for call in client.calls if call[0] == "providers"]
    assert len(search_call_list) == 2


def test_affiliation_only_witness_completes_without_plan_network_context():
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("OrganizationAffiliation",)
    )
    witness = support.MappedEvidenceWitness(
        SOURCE_A, 1234567890, "OrganizationAffiliation", "affiliation-1"
    )

    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        TypedEvidenceClient(),
        support.SourceEvaluationContext(5, 40.0),
        witnesses=[witness],
    )

    capability = source_result["mapped_evidence_capabilities"][
        "organization_affiliation"
    ]
    assert witness.supports_completion is True
    assert witness.supports_plan_network_context is False
    assert capability["state"] == "pass"


def test_completed_empty_proof_contradicting_exact_affiliation_fails_strictly():
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("OrganizationAffiliation",)
    )
    witness = support.MappedEvidenceWitness(
        SOURCE_A, 1234567890, "OrganizationAffiliation", "affiliation-1"
    )
    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        TypedEvidenceClient(),
        support.SourceEvaluationContext(5, 40.0),
        witnesses=[witness],
        completion_proofs={"OrganizationAffiliation": {"state": "completed_empty"}},
    )

    capability = source_result["mapped_evidence_capabilities"][
        "organization_affiliation"
    ]
    summary = harness.mapped_completion_summary(
        require_mapped_evidence=True,
        source_result_list=[source_result],
        witness_list_by_source={SOURCE_A: [witness]},
        witness_probe_error=None,
    )
    assert capability["state"] == "fail"
    assert capability["reason"] == "completion_proof_contradicts_mapped_witness"
    assert summary["completion_inconclusive"] is True


def test_default_gate_reports_typed_mismatch_without_failing_baseline():
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("PractitionerRole",)
    )

    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        TypedEvidenceClient(search_network_name=None),
        support.SourceEvaluationContext(5, 40.0),
        witnesses=[_typed_witness_list()[0]],
    )

    assert source_result["status"] == "pass"
    capability_map = source_result["mapped_evidence_capabilities"]["practitioner_role"]
    assert capability_map["state"] == "fail"
    assert (
        source_result["mapped_evidence_checks"][0]["provider_search_evidence_present"]
        is False
    )


def test_declared_empty_and_undeclared_capability_states_do_not_fail_baseline():
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("PractitionerRole",)
    )
    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        TypedEvidenceClient(),
        support.SourceEvaluationContext(5, 40.0),
    )

    assert source_result["status"] == "pass"
    capability_by_name = source_result["mapped_evidence_capabilities"]
    assert capability_by_name["practitioner_role"]["state"] == "not_observed"
    assert capability_by_name["organization_affiliation"]["state"] == "not_applicable"


def test_standalone_affiliation_completion_is_not_required_on_provider_surface():
    selection = support.SourceSelection(
        "acquired", SOURCE_A, "acquisition", True, ("OrganizationAffiliation",)
    )

    source_result = support.evaluate_source(
        selection,
        [support.OverlaySample(SOURCE_A, 1234567890, None)],
        TypedEvidenceClient(),
        support.SourceEvaluationContext(5, 40.0),
        completion_proofs={
            "OrganizationAffiliation": {
                "state": "provider_surface_not_applicable"
            }
        },
    )

    capability = source_result["mapped_evidence_capabilities"][
        "organization_affiliation"
    ]
    summary = harness.mapped_completion_summary(
        require_mapped_evidence=True,
        source_result_list=[source_result],
        witness_list_by_source={SOURCE_A: []},
        witness_probe_error=None,
    )

    assert capability["state"] == "provider_surface_not_applicable"
    assert summary["mapped_evidence_completion"] == "pass"
    assert summary["completion_inconclusive"] is False
