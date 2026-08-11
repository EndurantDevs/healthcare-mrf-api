# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS,
)
from process.provider_directory_rooted_graph_query import (
    build_insurance_plan_census_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
    ProviderDirectoryRootedGraphQueryResult,
    ProviderDirectoryRootedGraphResourceWitness,
    _edge_hash,
    _query_edges,
    _resource_hash,
    _sha256_text,
    _terminal_hash,
    build_provider_directory_rooted_graph_missing_witness,
    build_provider_directory_rooted_graph_query_result,
    provider_directory_rooted_graph_error_terminal_sha256,
    validate_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import (
    build_provider_directory_rooted_graph_work_spec,
)
from tests.test_provider_directory_rooted_graph_store_contract import (
    API_BASE,
    ROOT_ID,
    _claim,
    _identity,
    _scope,
)


def _role_result():
    identity = _identity()
    spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    claim = _claim(spec)
    query_result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-boundary",
                "practitioner": {"reference": f"Practitioner/{ROOT_ID}"},
                "organization": {"reference": "Organization/org.synthetic-1"},
            }
        ],
    )
    return claim, query_result


def _direct_claim(resource_id: str = "org.synthetic-1"):
    spec = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id,
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Organization",
            resource_id=resource_id,
        ),
        closure_scope="root",
        discovered_by_query_id="pdrgq_" + "8" * 48,
        discovered_source_type="PractitionerRole",
        discovered_source_id="role.synthetic-boundary",
        discovered_edge_sha256="7" * 64,
    )
    return _claim(spec)


def _census_claim():
    spec = build_provider_directory_rooted_graph_work_spec(
        _scope().scope_id,
        build_insurance_plan_census_query(API_BASE),
        closure_scope="census",
    )
    return _claim(spec)


def _resource_witness(
    resource_by_field: dict[str, object],
    closure_scope: str,
) -> ProviderDirectoryRootedGraphResourceWitness:
    canonical_payload = json.dumps(
        resource_by_field,
        separators=(",", ":"),
        sort_keys=True,
    )
    return ProviderDirectoryRootedGraphResourceWitness(
        resource_type=str(resource_by_field["resourceType"]),
        resource_id=str(resource_by_field["id"]),
        payload_sha256=_sha256_text(canonical_payload),
        payload_json_text=canonical_payload,
        closure_scope=closure_scope,
    )


def _query_result_for_witnesses(claim, resource_witnesses, advertised_total=None):
    resources = tuple(resource_witnesses)
    edges = _query_edges(resources)
    resource_hash = _resource_hash(resources)
    edge_hash = _edge_hash(edges)
    result_hash = _sha256_text(resource_hash + "\x1f" + edge_hash)
    terminal_hash = _terminal_hash(
        claim,
        result_hash,
        len(resources),
        len(edges),
        advertised_total,
        1,
    )
    return ProviderDirectoryRootedGraphQueryResult(
        query_id=claim.query_id,
        result_sha256=result_hash,
        terminal_record_sha256=terminal_hash,
        resources=resources,
        edges=edges,
        resource_set_sha256=resource_hash,
        edge_set_sha256=edge_hash,
        advertised_total=advertised_total,
        terminal_page_count=1,
    )


def test_result_builder_rejects_a_reference_hidden_outside_reviewed_paths() -> None:
    with pytest.raises(ValueError):
        build_provider_directory_rooted_graph_query_result(
            _direct_claim(),
            [
                {
                    "resourceType": "Organization",
                    "id": "org.synthetic-1",
                    "endpoint": [{"reference": "Endpoint/reviewed.synthetic-1"}],
                    "endpoint[0]": {"reference": "Organization/hidden.synthetic-1"},
                }
            ],
        )


def test_result_builder_rejects_nonfinite_or_wrongly_shaped_inputs() -> None:
    claim, query_result = _role_result()
    invalid_calls = (
        lambda: build_provider_directory_rooted_graph_query_result(claim, object()),
        lambda: build_provider_directory_rooted_graph_query_result(
            claim,
            [],
            advertised_total=1,
        ),
        lambda: build_provider_directory_rooted_graph_query_result(claim, [None]),
        lambda: build_provider_directory_rooted_graph_query_result(
            claim,
            [json.loads(query_result.resources[0].payload_json_text)] * 2,
        ),
        lambda: build_provider_directory_rooted_graph_query_result(
            _direct_claim(),
            [],
        ),
        lambda: build_provider_directory_rooted_graph_query_result(
            "not-a-claim",
            [],
        ),
        lambda: build_provider_directory_rooted_graph_query_result(
            claim,
            [],
            terminal_page_count=0,
        ),
    )
    for invalid_call in invalid_calls:
        with pytest.raises(ValueError):
            invalid_call()


def test_result_builder_binds_nested_plan_net_network_edge_path() -> None:
    identity = _identity()
    spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    claim = _claim(spec)
    query_result = build_provider_directory_rooted_graph_query_result(
        claim,
        [
            {
                "resourceType": "PractitionerRole",
                "id": "role.synthetic-network",
                "practitioner": {"reference": f"Practitioner/{ROOT_ID}"},
                "extension": [
                    {
                        "url": "urn:synthetic:nesting",
                        "extension": [
                            {
                                "url": (
                                    PROVIDER_DIRECTORY_ROOTED_GRAPH_PLAN_NET_NETWORK_EXTENSION_URLS[
                                        0
                                    ]
                                ),
                                "valueReference": {
                                    "reference": "Organization/network.synthetic-1"
                                },
                            }
                        ],
                    }
                ],
            }
        ],
    )

    [network_edge, practitioner_edge] = sorted(
        query_result.edges,
        key=lambda edge: edge.field_path,
    )
    assert network_edge.field_path == ("extension[0].extension[0].valueReference")
    assert network_edge.target_resource_type == "Organization"
    assert network_edge.target_resource_id == "network.synthetic-1"
    assert practitioner_edge.field_path == "practitioner"


def test_resource_and_edge_witnesses_recompute_every_identity() -> None:
    _claim_value, query_result = _role_result()
    resource_witness = query_result.resources[0]
    edge_witness = query_result.edges[0]

    for change in (
        {"payload_json_text": None},
        {"payload_json_text": "not-json"},
        {"payload_sha256": "0" * 64},
    ):
        with pytest.raises(ValueError, match="resource_invalid"):
            replace(resource_witness, **change)
    for change in (
        {"source_resource_id": "invalid/id"},
        {"edge_sha256": "0" * 64},
    ):
        with pytest.raises(ValueError, match="edge_invalid"):
            replace(edge_witness, **change)
    with pytest.raises(ValueError, match="result_invalid"):
        replace(query_result, result_sha256="0" * 64)


def test_claim_binding_rejects_disconnected_exact_direct_and_census_results() -> None:
    role_claim, _role_query_result = _role_result()
    wrong_role = _resource_witness(
        {
            "resourceType": "PractitionerRole",
            "id": "role.synthetic-other",
            "practitioner": {"reference": "Practitioner/other.synthetic"},
        },
        "root",
    )
    direct_claim = _direct_claim()
    direct_result = build_provider_directory_rooted_graph_query_result(
        direct_claim,
        [{"resourceType": "Organization", "id": "org.synthetic-1"}],
    )
    assert direct_result.resources[0].resource_id == "org.synthetic-1"
    wrong_direct = _resource_witness(
        {"resourceType": "Organization", "id": "org.synthetic-other"},
        "root",
    )
    census_claim = _census_claim()
    wrong_census = _resource_witness(
        {"resourceType": "InsurancePlan", "id": "plan.synthetic-1"},
        "root",
    )
    invalid_pairs = (
        (role_claim, _query_result_for_witnesses(role_claim, (wrong_role,))),
        (direct_claim, _query_result_for_witnesses(direct_claim, (wrong_direct,))),
        (
            census_claim,
            _query_result_for_witnesses(
                census_claim,
                (wrong_census,),
                advertised_total=1,
            ),
        ),
    )
    for claim, query_result in invalid_pairs:
        with pytest.raises(ValueError, match="result_invalid"):
            validate_provider_directory_rooted_graph_query_result(
                claim,
                query_result,
            )


def test_result_claim_and_terminal_witnesses_reject_identity_drift() -> None:
    claim, query_result = _role_result()
    with pytest.raises(ValueError, match="result_invalid"):
        validate_provider_directory_rooted_graph_query_result(
            "not-a-claim",
            query_result,
        )
    mismatched_claim = replace(claim, query_id="pdrgq_" + "6" * 48)
    with pytest.raises(ValueError, match="result_invalid"):
        validate_provider_directory_rooted_graph_query_result(
            mismatched_claim,
            query_result,
        )
    with pytest.raises(ValueError, match="result_invalid"):
        validate_provider_directory_rooted_graph_query_result(
            claim,
            replace(query_result, terminal_record_sha256="0" * 64),
        )
    with pytest.raises(ValueError, match="claim_invalid"):
        provider_directory_rooted_graph_error_terminal_sha256(
            "not-a-claim",
            "response_invalid",
        )
    with pytest.raises(ValueError, match="error_invalid"):
        provider_directory_rooted_graph_error_terminal_sha256(claim, "BAD")


def test_direct_missing_witness_is_exact_non_error_terminal_proof() -> None:
    direct_claim = _direct_claim()
    missing_response_json_text = json.dumps(
        {
            "resourceType": "OperationOutcome",
            "issue": [
                {"severity": "error", "code": "processing"},
                {"severity": "information", "code": "informational"},
            ],
        },
        separators=(",", ":"),
    )
    missing_response_sha256 = _sha256_text(missing_response_json_text)
    missing_response_bytes = len(missing_response_json_text.encode("utf-8"))
    not_found = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        404,
        missing_response_sha256,
        missing_response_bytes,
        missing_response_json_text,
    )
    gone = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        410,
        missing_response_sha256,
        missing_response_bytes,
        missing_response_json_text,
    )

    assert not_found.query_id == direct_claim.query_id
    assert not_found.result_sha256 == gone.result_sha256
    assert not_found.resource_set_sha256 == _resource_hash(())
    assert not_found.edge_set_sha256 == _edge_hash(())
    assert not_found.terminal_record_sha256 != gone.terminal_record_sha256
    for claim, status in (
        (direct_claim, 200),
        (direct_claim, 404.0),
        (_role_result()[0], 404),
    ):
        with pytest.raises(ValueError, match="missing_invalid"):
            build_provider_directory_rooted_graph_missing_witness(
                claim,
                status,
                missing_response_sha256,
                missing_response_bytes,
                missing_response_json_text,
            )
    with pytest.raises(ValueError, match="missing_invalid"):
        build_provider_directory_rooted_graph_missing_witness(
            direct_claim,
            404,
            missing_response_sha256,
            missing_response_bytes,
            missing_response_json_text.replace(
                '"issue":', '"resourceType":"forged","issue":'
            ),
        )


def test_sealed_summary_accepts_only_complete_error_free_roots() -> None:
    identity = _identity()
    summary = ProviderDirectoryRootedGraphAcquisitionSummary(
        acquisition_id=identity.acquisition_id,
        scope_id=identity.scope_id,
        completed_count=1,
        error_count=0,
        resource_count=1,
        edge_count=1,
        terminal_set_sha256="1" * 64,
        resource_set_sha256="2" * 64,
        edge_set_sha256="3" * 64,
        rooted_graph_sha256="4" * 64,
        rooted_graph_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
    )
    assert summary.completed_count == 1
    for change in (
        {"completed_count": -1},
        {"error_count": 1},
        {"rooted_graph_complete": False},
    ):
        with pytest.raises(ValueError, match="summary_invalid"):
            replace(summary, **change)
