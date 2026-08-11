# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import json

import pytest

from process.provider_directory_rooted_graph_query import (
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
    build_rooted_graph_direct_read,
)
from process.provider_directory_rooted_graph_result_contract import (
    ProviderDirectoryRootedGraphAcquisitionSummary,
    _sha256_text,
    build_provider_directory_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_result_store import (
    _summary_from_row,
    complete_provider_directory_rooted_graph_error,
    complete_provider_directory_rooted_graph_missing,
    complete_provider_directory_rooted_graph_result,
    seal_provider_directory_rooted_graph_acquisition,
)
from process.provider_directory_rooted_graph_store import (
    _claim_from_row,
    claim_provider_directory_rooted_graph_work,
    heartbeat_provider_directory_rooted_graph_work,
    initialize_provider_directory_rooted_graph_acquisition,
    release_provider_directory_rooted_graph_work,
)
from process.provider_directory_rooted_graph_store_contract import (
    INTENT_PATTERN,
    RUN_PATTERN,
    ProviderDirectoryRootedGraphStoreError,
    _canonical_json,
    _strict_hash,
    _strict_identifier,
    _strict_text,
    build_provider_directory_rooted_graph_acquisition_identity,
    build_provider_directory_rooted_graph_work_spec,
)
from process.provider_directory_rooted_graph_store_support import (
    assert_identity_row,
    function_ref,
    identity_fields,
    row_fields,
    schema_name,
)
from tests.test_provider_directory_rooted_graph_store import (
    _Database,
    _claim_row,
    _header,
)
from tests.test_provider_directory_rooted_graph_store_contract import (
    API_BASE,
    ROOT_ID,
    _claim,
    _identity,
    _scope,
)


def _role_claim_and_query_result():
    identity = _identity()
    role_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    claim = _claim(role_spec)
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


def _discovered_direct_spec(identity):
    claim, query_result = _role_claim_and_query_result()
    organization_edge = next(
        edge
        for edge in query_result.edges
        if edge.target_resource_type == "Organization"
    )
    return build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_rooted_graph_direct_read(
            api_base=API_BASE,
            resource_type="Organization",
            resource_id="org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=claim.query_id,
        discovered_source_type="PractitionerRole",
        discovered_source_id="role.synthetic-boundary",
        discovered_edge_sha256=organization_edge.edge_sha256,
    )


def _sealed_row(identity):
    return {
        **identity_fields(identity),
        "status": "sealed",
        "completed_count": 1,
        "error_count": 0,
        "resource_count": 1,
        "edge_count": 1,
        "terminal_set_sha256": "1" * 64,
        "resource_set_sha256": "2" * 64,
        "edge_set_sha256": "3" * 64,
        "rooted_graph_sha256": "4" * 64,
        "rooted_graph_complete": True,
        "endpoint_collection_complete": False,
        "endpoint_complete": False,
    }


def test_store_contract_helpers_reject_noncanonical_values() -> None:
    invalid_calls = (
        lambda: _canonical_json(object()),
        lambda: _strict_identifier("bad", RUN_PATTERN),
        lambda: _strict_identifier(None, INTENT_PATTERN),
        lambda: _strict_hash("bad"),
        lambda: _strict_text(" padded ", 32),
        lambda: build_provider_directory_rooted_graph_acquisition_identity(
            "not-a-scope",
            root_cohort_id="synthetic",
            endpoint_signature_sha256="1" * 64,
            acquisition_role="baseline",
            run_id="pdrgr_" + "2" * 48,
            dataset_intent_id="pdrgi_" + "3" * 48,
        ),
    )
    for invalid_call in invalid_calls:
        with pytest.raises(ValueError):
            invalid_call()
    with pytest.raises(ValueError, match="identity_invalid"):
        replace(_identity(), storage_contract_id="wrong-contract")


def test_work_specs_and_claims_reject_forged_shapes() -> None:
    identity = _identity()
    role_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_practitioner_role_query(API_BASE, ROOT_ID),
        closure_scope="root",
    )
    direct_spec = _discovered_direct_spec(identity)
    affiliation_spec = build_provider_directory_rooted_graph_work_spec(
        identity.scope_id,
        build_provider_directory_organization_affiliation_query(
            API_BASE,
            "org.synthetic-1",
        ),
        closure_scope="root",
        discovered_by_query_id=direct_spec.query_id,
        discovered_source_type="Organization",
        discovered_source_id="org.synthetic-1",
    )
    invalid_specs = (
        {"query_identity_json_text": "not-json"},
        {"query_identity_sha256": "0" * 64},
        {"discovered_by_query_id": "pdrgq_" + "1" * 48},
    )
    for change in invalid_specs:
        with pytest.raises(ValueError, match="work_invalid"):
            replace(role_spec, **change)
    with pytest.raises(ValueError, match="work_invalid"):
        replace(direct_spec, discovered_source_type="Patient")
    with pytest.raises(ValueError, match="work_invalid"):
        replace(affiliation_spec, discovered_edge_sha256="1" * 64)
    with pytest.raises(ValueError, match="query_invalid"):
        build_provider_directory_rooted_graph_work_spec(
            identity.scope_id,
            "not-a-query",
            closure_scope="root",
        )
    claim = _claim(role_spec)
    with pytest.raises(ValueError, match="claim_invalid"):
        replace(claim, reference_id="invalid/id")
    with pytest.raises(ValueError, match="claim_invalid"):
        replace(claim, kind="direct_read")


def test_schema_and_row_helpers_fail_closed(monkeypatch) -> None:
    identity = _identity()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        schema_name()
    monkeypatch.setenv("DB_SCHEMA", "bad-schema")
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA")
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        schema_name()
    monkeypatch.setenv("DB_SCHEMA", "synthetic_schema")
    assert function_ref("synthetic_function") == (
        '"synthetic_schema"."synthetic_function"'
    )
    assert row_fields(None) == {}

    class MappingRow:
        _mapping = {"status": "building"}

    assert row_fields(MappingRow()) == {"status": "building"}
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        assert_identity_row(identity, {})
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        assert_identity_row(
            identity,
            {
                **identity_fields(identity),
                "status": "building",
                "root_dataset_hash": "0" * 64,
            },
        )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        assert_identity_row(identity, {**identity_fields(identity), "status": "bad"})


@pytest.mark.asyncio
async def test_initialization_rejects_root_drift_and_incomplete_work_census() -> None:
    identity = _identity()
    with pytest.raises(ValueError, match="identity_invalid"):
        await initialize_provider_directory_rooted_graph_acquisition("bad")
    sealed_database = _Database(first_rows=(_sealed_row(identity),))
    assert (
        await initialize_provider_directory_rooted_graph_acquisition(
            identity,
            database=sealed_database,
        )
        == 1
    )
    missing_endpoint_database = _Database(
        first_rows=(
            _header(identity),
            {"work_count": 0, "role_count": 0, "plan_count": 0},
        ),
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await initialize_provider_directory_rooted_graph_acquisition(
            identity,
            database=missing_endpoint_database,
        )
    incomplete_database = _Database(
        first_rows=(
            _header(identity),
            {"work_count": 2, "role_count": 1, "plan_count": 1},
        ),
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await initialize_provider_directory_rooted_graph_acquisition(
            identity,
            database=incomplete_database,
        )


@pytest.mark.asyncio
async def test_claim_and_heartbeat_boundaries() -> None:
    identity = _identity()
    direct_spec = _discovered_direct_spec(identity)
    for kwargs in (
        {"acquisition_id": "bad"},
        {"acquisition_id": identity.acquisition_id, "query_id": "bad"},
        {"acquisition_id": identity.acquisition_id, "lease_seconds": 1},
    ):
        with pytest.raises(ValueError):
            await claim_provider_directory_rooted_graph_work(
                database=_Database(), **kwargs
            )
    assert (
        await claim_provider_directory_rooted_graph_work(
            identity.acquisition_id,
            database=_Database(first_rows=(None,)),
        )
        is None
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        _claim_from_row({"acquisition_id": "bad"})
    claim = _claim(direct_spec)
    await heartbeat_provider_directory_rooted_graph_work(
        claim,
        database=_Database(status_counts=(1,)),
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as lost_error:
        await heartbeat_provider_directory_rooted_graph_work(
            claim,
            database=_Database(status_counts=(0,)),
        )
    assert lost_error.value.code == "lease_lost"
    with pytest.raises(ValueError, match="claim_invalid"):
        await heartbeat_provider_directory_rooted_graph_work("bad")
    with pytest.raises(ValueError, match="lease_invalid"):
        await heartbeat_provider_directory_rooted_graph_work(claim, lease_seconds=1)
    with pytest.raises(ValueError, match="claim_invalid"):
        await release_provider_directory_rooted_graph_work("bad")


@pytest.mark.asyncio
async def test_result_storage_is_count_fenced_and_frontier_is_atomic() -> None:
    """Reject partial writes and register the frontier in one transaction."""

    claim, query_result = _role_claim_and_query_result()
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as resource_error:
        await complete_provider_directory_rooted_graph_result(
            claim,
            query_result,
            database=_Database(status_counts=(0,)),
        )
    assert resource_error.value.code == "state"
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as edge_error:
        await complete_provider_directory_rooted_graph_result(
            claim,
            query_result,
            database=_Database(status_counts=(1, 0)),
        )
    assert edge_error.value.code == "state"
    success_database = _Database(
        first_rows=({"canonical_api_base": API_BASE},),
        status_counts=(1, 1, 1, 1, 1),
    )
    await complete_provider_directory_rooted_graph_result(
        claim,
        query_result,
        database=success_database,
    )
    terminal_index = next(
        index
        for index, (statement, _) in enumerate(success_database.statements)
        if "SET status = 'completed'" in statement
    )
    derived_index = next(
        index
        for index, (statement, parameters) in enumerate(success_database.statements)
        if "INSERT INTO" in statement and parameters.get("kind") == "direct_read"
    )
    assert terminal_index < derived_index
    assert success_database.transaction_count == 1
    assert any(
        parameters.get("action") == "derive"
        for _, parameters in success_database.statements
    )


@pytest.mark.asyncio
async def test_error_terminalization_is_lease_fenced() -> None:
    """Reject lost error terminalizations and malformed error outcomes."""

    claim, _ = _role_claim_and_query_result()
    await complete_provider_directory_rooted_graph_error(
        claim,
        error_code="response_invalid",
        database=_Database(status_counts=(1,)),
    )
    with pytest.raises(ProviderDirectoryRootedGraphStoreError) as terminal_error:
        await complete_provider_directory_rooted_graph_error(
            claim,
            error_code="response_invalid",
            database=_Database(status_counts=(0,)),
        )
    assert terminal_error.value.code == "lease_lost"
    with pytest.raises(ValueError, match="claim_invalid"):
        await complete_provider_directory_rooted_graph_error(
            "bad",
            error_code="response_invalid",
        )
    with pytest.raises(ValueError, match="error_invalid"):
        await complete_provider_directory_rooted_graph_error(claim, error_code="BAD")


@pytest.mark.asyncio
async def test_direct_missing_terminalization_is_lease_fenced_and_non_error() -> None:
    direct_claim = _claim(_discovered_direct_spec(_identity()))
    database = _Database(status_counts=(1,))
    missing_response_json_text = json.dumps(
        {
            "resourceType": "OperationOutcome",
            "issue": [{"severity": "error", "code": "not-found"}],
        },
        separators=(",", ":"),
    )
    missing_response_sha256 = _sha256_text(missing_response_json_text)
    missing_response_bytes = len(missing_response_json_text.encode("utf-8"))
    await complete_provider_directory_rooted_graph_missing(
        direct_claim,
        missing_http_status=404,
        missing_response_sha256=missing_response_sha256,
        missing_response_bytes=missing_response_bytes,
        missing_response_json_text=missing_response_json_text,
        database=database,
    )
    sql, parameters = database.statements[-1]
    assert "status = 'completed'" in sql
    assert "missing_http_status = :missing_http_status" in sql
    assert "error_code = NULL" in sql
    assert parameters["missing_http_status"] == 404
    with pytest.raises(ValueError, match="missing_invalid"):
        await complete_provider_directory_rooted_graph_missing(
            direct_claim,
            missing_http_status=500,
            missing_response_sha256=missing_response_sha256,
            missing_response_bytes=missing_response_bytes,
            missing_response_json_text=missing_response_json_text,
            database=_Database(),
        )


@pytest.mark.asyncio
async def test_sealing_handles_idempotence_invalid_rows_and_missing_fixed_point() -> (
    None
):
    identity = _identity()
    sealed_row = _sealed_row(identity)
    sealed_summary = await seal_provider_directory_rooted_graph_acquisition(
        identity,
        database=_Database(first_rows=(sealed_row,)),
    )
    assert isinstance(sealed_summary, ProviderDirectoryRootedGraphAcquisitionSummary)
    building_database = _Database(first_rows=(_header(identity), None))
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        await seal_provider_directory_rooted_graph_acquisition(
            identity,
            database=building_database,
        )
    completed_database = _Database(first_rows=(_header(identity), sealed_row))
    completed_summary = await seal_provider_directory_rooted_graph_acquisition(
        identity,
        database=completed_database,
    )
    assert completed_summary.rooted_graph_complete is True
    with pytest.raises(ValueError, match="identity_invalid"):
        await seal_provider_directory_rooted_graph_acquisition("bad")
    with pytest.raises(ProviderDirectoryRootedGraphStoreError):
        _summary_from_row({})
