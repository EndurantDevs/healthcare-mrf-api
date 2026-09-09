# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-request budgets never become invented resource-completeness proof."""

from dataclasses import fields, replace
import json

import pytest

from process.fhir_request_failure_policy import FHIR_REQUEST_FAILURE_POLICY_ID
from process.provider_directory_rooted_graph_operator import _publication_json
from process.provider_directory_rooted_graph_publication_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
    ProviderDirectoryRootedGraphPublicationResult,
)
from process.provider_directory_rooted_graph_request_coverage import (
    has_matching_rooted_publication_coverage,
    has_matching_rooted_request_coverage,
    validate_rooted_request_coverage,
)
from tests.provider_directory_rooted_graph_publication_test_support import readiness


def coverage(**changes):
    return {
        "policy_id": FHIR_REQUEST_FAILURE_POLICY_ID,
        "total_requests": 100,
        "failed_requests": 1,
        "rooted_total_requests": 3,
        "rooted_failed_requests": 1,
        "resource_coverage": "unknown",
        **changes,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"total_requests": 50},
        {"failed_requests": 2},
        {"failed_requests": 0},
        {"total_requests": True},
        {"total_requests": 2**53},
        {"rooted_failed_requests": 4},
        {"rooted_total_requests": 100},
        {"rooted_total_requests": 0},
        {"resource_coverage": "complete"},
        {"missing_resource_count": 1},
        {"policy_id": "unreviewed"},
    ],
)
def test_rejects_boundary_inflation_and_fabricated_coverage(changes):
    with pytest.raises(ValueError, match="request_coverage_invalid"):
        validate_rooted_request_coverage(coverage(**changes))


def test_binds_unique_rooted_work_and_inherited_flex_failures():
    proof = coverage()
    assert validate_rooted_request_coverage(proof) == proof
    assert has_matching_rooted_request_coverage(proof, completed_count=2, error_count=1)
    assert not has_matching_rooted_request_coverage(proof, completed_count=9, error_count=1)
    assert not has_matching_rooted_request_coverage(None, completed_count=2, error_count=1)
    assert has_matching_rooted_publication_coverage(
        proof, retry_exhausted_count=0, rooted_graph_complete=False
    )
    assert not has_matching_rooted_publication_coverage(
        proof, retry_exhausted_count=1, rooted_graph_complete=False
    )
    assert not has_matching_rooted_publication_coverage(
        proof, retry_exhausted_count=0, rooted_graph_complete=True
    )


def test_partial_readiness_requires_v2_and_never_dispatches_global_profile():
    ready = replace(
        readiness(),
        request_failure_coverage=coverage(),
        rooted_graph_complete=False,
        publication_contract_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
    )
    result = json.loads(_publication_json(ProviderDirectoryRootedGraphPublicationResult(ready, False)))
    assert result["request_failure_coverage"] == coverage()
    assert result["rooted_graph_complete"] is False
    assert result["cohort_complete"] is True
    assert result["retry_exhausted_count"] == 0
    assert result["profile_dispatch"]["required_external_global_dispatch"] is False
    assert "external_followup" not in result["profile_dispatch"]
    with pytest.raises(ValueError):
        replace(ready, publication_contract_id=readiness().publication_contract_id)
    with pytest.raises(ValueError):
        replace(ready, rooted_graph_complete=True)


def test_inherited_flex_failure_does_not_claim_a_rooted_failure():
    ready = replace(
        readiness(),
        cohort_complete=False,
        retry_exhausted_count=1,
        request_failure_coverage=coverage(rooted_failed_requests=0),
        publication_contract_id=PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID,
    )
    assert ready.rooted_graph_complete is True
    assert ready.request_failure_coverage["resource_coverage"] == "unknown"
    assert readiness().request_failure_coverage is None


def _partial_publication_inputs():
    from process.provider_directory_rooted_graph_publication import build_rooted_graph_dataset_identity
    from process.provider_directory_rooted_graph_single_root_contract import derive_single_root_identity
    from tests.provider_directory_rooted_graph_publication_test_support import exact_current, resource_counts
    from tests.test_provider_directory_rooted_graph_request_admission import _admit, _partial_root

    current = exact_current(retry_exhausted_count=1)
    expected = derive_single_root_identity(current, operation_key="e" * 64).candidate
    root = _partial_root(insurance_plan_count=None, insurance_plan_page_count=None)
    root = replace(root, **{
        field.name: getattr(expected, field.name)
        for field in fields(root) if hasattr(expected, field.name)
    })
    admission = _admit(root)
    return build_rooted_graph_dataset_identity(admission, current), admission, resource_counts()


def test_partial_identity_and_metadata_bind_the_admitted_source_proof():
    from process.provider_directory_rooted_graph_publication import provider_directory_rooted_graph_publication_metadata

    identity, admission, counts = _partial_publication_inputs()
    metadata = provider_directory_rooted_graph_publication_metadata(
        identity, admission, previous_dataset_id=identity.root_dataset_id, resource_counts=counts
    )
    assert identity.publication_contract_id == PROVIDER_DIRECTORY_ROOTED_GRAPH_PARTIAL_PUBLICATION_CONTRACT_ID
    assert metadata["request_failure_coverage"] == admission.request_failure_coverage
    assert metadata["rooted_graph_complete"] is False
    assert metadata["cohort_complete"] is False
    assert metadata["insurance_plan_count"] is None
    with pytest.raises(ValueError):
        replace(identity, request_failure_coverage=None)
    with pytest.raises(ValueError):
        replace(identity, retry_exhausted_count=2)


@pytest.mark.asyncio
async def test_partial_publication_header_serializes_proof_without_complete_flag():
    from process.provider_directory_rooted_graph_publication_store import _insert_headers
    from tests.test_provider_directory_rooted_graph_publication_store_boundaries import _ScriptedDatabase

    identity, admission, counts = _partial_publication_inputs()
    database = _ScriptedDatabase(statuses=(1, 1))
    await _insert_headers(database, identity, admission, counts)
    _method, statement, parameters = database.calls[-1]
    assert "CAST(:request_failure_coverage AS jsonb)" in statement
    assert json.loads(parameters["request_failure_coverage"]) == admission.request_failure_coverage
    assert parameters["rooted_graph_complete"] is False
    assert parameters["census_insurance_plan_count"] is None
