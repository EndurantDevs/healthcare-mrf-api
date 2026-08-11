# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process.provider_directory_rooted_graph_identity import (
    build_provider_directory_rooted_graph_scope,
)
from process.provider_directory_rooted_graph_query import (
    build_insurance_plan_census_query,
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_provider_directory_rooted_graph_query_result,
    provider_directory_rooted_graph_error_terminal_sha256,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphCensusClaim,
    ProviderDirectoryRootedGraphWorkClaim,
    build_provider_directory_rooted_graph_acquisition_identity,
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)


API_BASE = "https://directory.synthetic.test/fhir/R4"
ENDPOINT_ID = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_ID
ENDPOINT_SIGNATURE = PROVIDER_DIRECTORY_ROOTED_GRAPH_ENDPOINT_SIGNATURE_SHA256
DATASET_HASH = "c" * 64
ROOT_PROOF = "d" * 64
ROOT_ID = "practitioner.synthetic-1"


def _scope():
    return build_provider_directory_rooted_graph_scope(
        root_dataset_variant="uhc_flex_practitioner",
        root_publication_contract_id=(
            "healthporta.provider-directory.uhc-flex-practitioner-"
            "dataset-publication.v1"
        ),
        root_source_id="synthetic-root-source",
        root_endpoint_id="9" * 64,
        acquisition_source_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        acquisition_endpoint_id=ENDPOINT_ID,
        source_authority_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_AUTHORITY_ID,
        root_dataset_id="synthetic-practitioner-dataset-1",
        root_dataset_hash=DATASET_HASH,
        root_content_proof_sha256=ROOT_PROOF,
        root_resource_count=1,
    )


def _identity(role: str = "baseline"):
    return build_provider_directory_rooted_graph_acquisition_identity(
        _scope(),
        root_cohort_id="synthetic-cohort-v1",
        endpoint_signature_sha256=ENDPOINT_SIGNATURE,
        acquisition_role=role,
        run_id="pdrgr_" + ("1" if role == "baseline" else "2") * 48,
        dataset_intent_id="pdrgi_" + "3" * 48,
    )


def _claim(spec, role: str = "baseline", attempt: int = 1):
    identity = _identity(role)
    return ProviderDirectoryRootedGraphWorkClaim(
        acquisition_id=identity.acquisition_id,
        scope_id=identity.scope_id,
        query_id=spec.query_id,
        query_identity_sha256=spec.query_identity_sha256,
        kind=spec.kind,
        resource_type=spec.resource_type,
        reference_type=spec.reference_type,
        reference_id=spec.reference_id,
        closure_scope=spec.closure_scope,
        attempt=attempt,
        lease_token=("4" if role == "baseline" else "5") * 64,
    )


def test_acquisition_identity_binds_exact_root_but_keeps_twin_scope_shared() -> None:
    baseline = _identity("baseline")
    candidate = _identity("candidate")

    assert baseline.acquisition_id != candidate.acquisition_id
    assert baseline.scope_id == candidate.scope_id
    assert baseline.root_dataset_id == candidate.root_dataset_id
    assert baseline.root_cohort_id == "synthetic-cohort-v1"
    assert baseline.rooted_graph_complete is False
    assert baseline.endpoint_collection_complete is False
    assert baseline.endpoint_complete is False
    with pytest.raises(ValueError, match="rooted_graph"):
        replace(baseline, root_dataset_hash="0" * 64)


def test_initial_and_discovered_work_retain_exact_provenance() -> None:
    """Bind every derived query to its parent resource or edge witness."""

    scope_id = _scope().scope_id
    role = build_provider_directory_rooted_graph_work_spec(
        scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    role_result = build_provider_directory_rooted_graph_query_result(
        _claim(role),
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-1",
                "practitioner": {"reference": f"Practitioner/{ROOT_ID}"},
                "organization": {"reference": "Organization/org.synthetic-1"},
            }
        ],
    )
    organization_edge = next(
        edge
        for edge in role_result.edges
        if edge.target_resource_type == "Organization"
    )
    direct = build_provider_directory_rooted_graph_work_spec(
        scope_id,
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Organization",
            resource_id="org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=role.query_id,
        discovered_source_type="PractitionerRole",
        discovered_source_id="role.synthetic-1",
        discovered_edge_sha256=organization_edge.edge_sha256,
    )
    affiliation = build_provider_directory_rooted_graph_work_spec(
        scope_id,
        build_provider_directory_organization_affiliation_query(
            API_BASE,
            "org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=direct.query_id,
        discovered_source_type="Organization",
        discovered_source_id="org.synthetic-1",
    )

    assert role.closure_scope == "root"
    assert direct.discovered_edge_sha256 == organization_edge.edge_sha256
    assert affiliation.discovered_source_id == "org.synthetic-1"
    with pytest.raises(ValueError, match="rooted_graph_work_invalid"):
        replace(direct, discovered_edge_sha256=None)


def test_census_claim_retains_exact_sorted_root_network_anchor_set() -> None:
    """Expose the immutable DB-derived anchor set beside the census work claim."""

    census = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id,
        build_insurance_plan_census_query(API_BASE),
        closure_scope="census",
    )
    census_claim = ProviderDirectoryRootedGraphCensusClaim(
        work_claim=_claim(census),
        root_network_references=(
            "Organization/network.synthetic-a",
            "Organization/network.synthetic-b",
        ),
    )

    assert census.closure_scope == "census"
    assert census_claim.root_network_references == (
        "Organization/network.synthetic-a",
        "Organization/network.synthetic-b",
    )
    with pytest.raises(ValueError, match="census_claim_invalid"):
        replace(
            census_claim,
            root_network_references=(
                "Organization/network.synthetic-b",
                "Organization/network.synthetic-a",
            ),
        )
    with pytest.raises(ValueError, match="census_claim_invalid"):
        replace(
            census_claim,
            root_network_references=(
                "Organization/network.synthetic-a",
                "Organization/network.synthetic-a",
            ),
        )
    with pytest.raises(ValueError, match="census_claim_invalid"):
        replace(census_claim, root_network_references=None)


def test_full_plan_census_is_retained_before_local_network_intersection() -> None:
    spec = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id,
        build_insurance_plan_census_query(API_BASE),
        closure_scope="census",
    )
    query_result = build_provider_directory_rooted_graph_query_result(
        _claim(spec),
        [
            {
                "resourceType": "InsurancePlan",
                "id": "plan.synthetic-a",
                "network": [{"reference": "Organization/network.synthetic-a"}],
            },
            {
                "resourceType": "InsurancePlan",
                "id": "plan.synthetic-b",
                "network": [{"reference": "Organization/network.synthetic-b"}],
            },
        ],
        advertised_total=2,
        terminal_page_count=3,
        reachable_network_references=("Organization/network.synthetic-a",),
    )

    assert query_result.advertised_total == 2
    assert query_result.terminal_page_count == 3
    assert {
        resource_witness.resource_id: resource_witness.closure_scope
        for resource_witness in query_result.resources
    } == {
        "plan.synthetic-a": "plan",
        "plan.synthetic-b": "census",
    }
    assert {
        (edge.target_resource_id, edge.closure_scope) for edge in query_result.edges
    } == {
        ("network.synthetic-a", "plan"),
        ("network.synthetic-b", "census"),
    }
    with pytest.raises(ValueError, match="census"):
        build_provider_directory_rooted_graph_query_result(
            _claim(spec),
            [query_result.resources[0].payload_json_text],
            advertised_total=1,
        )


def test_terminal_roots_ignore_acquisition_role_attempt_and_lease_token() -> None:
    spec = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    resources = [
        {
            "resourceType": "PractitionerRole",
            "id": "role.synthetic-1",
            "practitioner": {"reference": f"Practitioner/{ROOT_ID}"},
        }
    ]
    baseline = build_provider_directory_rooted_graph_query_result(
        _claim(spec, "baseline", 1),
        resources,
    )
    candidate = build_provider_directory_rooted_graph_query_result(
        _claim(spec, "candidate", 7),
        resources,
    )

    assert baseline.terminal_record_sha256 == candidate.terminal_record_sha256
    assert baseline.result_sha256 == candidate.result_sha256
    assert provider_directory_rooted_graph_error_terminal_sha256(
        _claim(spec, "baseline"),
        "response_invalid",
    ) == provider_directory_rooted_graph_error_terminal_sha256(
        _claim(spec, "candidate", 4),
        "response_invalid",
    )


@pytest.mark.parametrize(
    ("spec", "resource"),
    [
        (
            build_provider_directory_rooted_graph_work_spec(
                _scope().scope_id,
                build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
                closure_scope="root",
            ),
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-wrong-root",
                "practitioner": {"reference": "Practitioner/other.synthetic"},
            },
        ),
        (
            build_provider_directory_rooted_graph_work_spec(
                _scope().scope_id,
                build_provider_directory_organization_affiliation_query(
                    API_BASE,
                    "org.synthetic-1",
                ),
                closure_scope="root",
                discovered_by_query_id="pdrgq_" + "9" * 48,
                discovered_source_type="Organization",
                discovered_source_id="org.synthetic-1",
            ),
            {
                "resourceType": "OrganizationAffiliation",
                "id": "affiliation.synthetic-wrong-root",
                "participatingOrganization": {
                    "reference": "Organization/other.synthetic"
                },
            },
        ),
    ],
)
def test_exact_search_results_must_match_the_queried_reference(spec, resource) -> None:
    with pytest.raises(ValueError, match="rooted_graph_result_invalid"):
        build_provider_directory_rooted_graph_query_result(
            _claim(spec),
            [resource],
        )
