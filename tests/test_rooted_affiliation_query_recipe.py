# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transport-recipe compatibility components; native custody is verified separately."""

import hashlib
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_rooted_publication_freeze as freeze
from process import provider_directory_rooted_graph_history_validation as history
from process import provider_directory_rooted_graph_publication_candidate as candidate
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_rooted_graph_http import rebind_provider_directory_rooted_graph_query
from process.provider_directory_rooted_graph_input_fence import root_input_fence_sql
from process.provider_directory_rooted_graph_query import (
    build_provider_directory_organization_affiliation_query,
    build_provider_directory_practitioner_role_query,
)
from process.provider_directory_rooted_graph_result_contract import (
    build_rooted_graph_query_result,
    validate_rooted_graph_query_result,
)
from process.provider_directory_rooted_graph_store_contract import build_provider_directory_rooted_graph_work_spec
from process.provider_directory_rooted_graph_terminal import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID,
    rooted_graph_query_filter_validation_sql,
)
from tests.test_provider_directory_rooted_graph_frozen_validation import (
    _seal,
    _validate,
)
from tests.test_provider_directory_rooted_graph_frozen_validation import (
    builder as builder,
)
from tests.test_provider_directory_rooted_graph_frozen_validation import (
    frozen as frozen,
)
from tests.test_provider_directory_rooted_graph_handoff import rooted_handoff as rooted_handoff
from tests.test_provider_directory_rooted_graph_history_validation import _family, _policy
from tests.test_provider_directory_rooted_graph_native_adapter import (
    _materialize,
)
from tests.test_provider_directory_rooted_graph_native_adapter import (
    issued_scope as issued_scope,
)
from tests.test_provider_directory_rooted_graph_native_adapter import (
    materialization as materialization,
)
from tests.test_provider_directory_rooted_graph_publication_freeze import SCHEMA, _current_bodies
from tests.test_provider_directory_rooted_graph_store_contract import API_BASE, _claim, _scope


def _result(resource_type="OrganizationAffiliation", *, empty=False):
    query = (
        build_provider_directory_organization_affiliation_query(API_BASE, "organization-one")
        if resource_type == "OrganizationAffiliation"
        else build_provider_directory_practitioner_role_query(API_BASE, "practitioner-one")
    )
    discovery = (
        {
            "discovered_by_query_id": "pdrgq_" + "a" * 48,
            "discovered_source_type": "Organization",
            "discovered_source_id": "organization-one",
        }
        if resource_type == "OrganizationAffiliation"
        else {}
    )
    claim = _claim(
        build_provider_directory_rooted_graph_work_spec(_scope().scope_id, query, closure_scope="root", **discovery)
    )
    reference_field = "participatingOrganization" if resource_type == "OrganizationAffiliation" else "practitioner"
    resource_by_field = {
        "resourceType": resource_type,
        "id": "result-one",
        reference_field: {"reference": query.reference},
    }
    return claim, build_rooted_graph_query_result(
        claim, [] if empty else [resource_by_field], advertised_total=int(not empty)
    )


def _terminal_formula(claim, result, recipe=""):
    fields = (
        "healthporta.provider-directory.rooted-graph-terminal-record.v2",
        claim.query_identity_sha256,
        "completed",
        result.result_sha256,
        str(len(result.resources)),
        str(len(result.edges)),
        "" if result.advertised_total is None else str(result.advertised_total),
        str(result.terminal_page_count),
        "true",
        recipe,
    )
    return hashlib.sha256("\x1f".join(fields).encode()).hexdigest()


@pytest.mark.parametrize("empty", [False, True])
def test_filtered_terminal_rejects_old_completed_proof_without_changing_logical_query(empty):
    claim, result = _result(empty=empty)
    query = build_provider_directory_organization_affiliation_query(API_BASE, "organization-one")
    assert query.query_id(claim.scope_id) == claim.query_id
    assert rebind_provider_directory_rooted_graph_query(API_BASE, claim).url == query.url
    assert "&role=provider&" in query.url
    assert result.terminal_record_sha256 == _terminal_formula(
        claim, result, "healthporta.provider-directory.rooted-graph-affiliation-provider-filter.v1"
    )
    validate_rooted_graph_query_result(claim, result)
    legacy = replace(result, terminal_record_sha256=_terminal_formula(claim, result))
    with pytest.raises(ValueError, match="rooted_graph_result_invalid"):
        validate_rooted_graph_query_result(claim, legacy)
    if result.resources:
        assert "code" not in result.resources[0].payload_json_text


def test_other_query_terminal_formula_remains_compatible():
    claim, result = _result("PractitionerRole")
    assert result.terminal_record_sha256 == _terminal_formula(claim, result)
    validate_rooted_graph_query_result(claim, result)


@pytest.mark.asyncio
@pytest.mark.parametrize("receipt", [False, None, 1, {"closed": True, "valid": False, "rows": 1}])
async def test_old_sealed_source_stops_before_candidate_lookup_or_writes(materialization, receipt):
    scope = materialization
    scope.runtime.connection.fetchval.return_value = receipt
    with pytest.raises(GenericCandidateError, match="rooted_query_recipe_changed"):
        await _materialize(scope)
    scope.retained.assert_not_awaited()
    assert scope.calls.mock_calls == []
    query, acquisition_id, cursor, limit = scope.runtime.connection.fetchval.await_args.args
    assert query == rooted_graph_query_filter_validation_sql(scope.runtime.catalog_schema)
    assert acquisition_id == min(scope.admission.publication_acquisition_id, scope.admission.comparison_acquisition_id)
    assert (cursor, limit) == ("", 4096)


@pytest.mark.asyncio
@pytest.mark.parametrize("recipe", [None, "older-recipe"])
async def test_old_prepared_handoff_cannot_replay(rooted_handoff, recipe):
    document = deepcopy(rooted_handoff.document)
    metadata = document["identity"]["metadata"]
    metadata.pop("query_filter_recipe_id")
    if recipe is not None:
        metadata["query_filter_recipe_id"] = recipe
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=document))
    with pytest.raises(GenericCandidateError, match="rooted_query_recipe_changed"):
        await candidate.retained_rooted_handoff(rooted_handoff.runtime, connection, document["identity"]["dataset_id"])
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_marker_alone_cannot_validate_old_completed_searches(frozen):
    frozen.query_recipes_valid = False
    assert frozen.document["identity"]["metadata"]["query_filter_recipe_id"] == (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID
    )
    with pytest.raises(GenericCandidateError, match="rooted_query_recipe_changed"):
        await _validate(frozen)
    frozen.cursor.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_sealing_cannot_mint_recipe_before_closed_snapshot_audit(builder):
    builder.connection.fetchval.side_effect = [{"closed": False, "valid": True, "rows": 0}]
    with pytest.raises(GenericCandidateError, match="rooted_query_recipe_changed"):
        await _seal(builder)
    builder.connection.fetch.assert_not_awaited()
    builder.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("comparison", [None, "pdrga_" + "2" * 48])
async def test_recipe_audit_pages_single_or_comparison_roots_without_resource_reads(comparison):
    admission_by_field = {"publication_acquisition_id": "pdrga_" + "1" * 48, "comparison_acquisition_id": comparison}
    identities = sorted({identity for identity in admission_by_field.values() if identity is not None})
    first, last = "pdrgq_" + "1" * 48, "pdrgq_" + "2" * 48
    pages = [
        {"closed": True, "valid": True, "rows": 4096, "last_query_id": first},
        {"closed": True, "valid": True, "rows": 1, "last_query_id": last},
        {"closed": True, "valid": True, "rows": 0, "last_query_id": None},
    ]
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=pages * len(identities)))
    await candidate.validate_rooted_query_filter_recipes(connection, "sample", admission_by_field)
    assert [call.args[1:] for call in connection.fetchval.await_args_list] == [
        (identity, cursor, 4096) for identity in identities for cursor in ("", first, last)
    ]


def test_final_and_ancestor_proofs_require_recipe_in_existing_protected_handoff():
    final = freeze.native_receipt_sql("sample")
    ancestor = root_input_fence_sql("sample", "$1", "$2::jsonb", variant="rooted_combined")
    for query in (final, ancestor):
        assert "->'identity'->'metadata'->>'query_filter_recipe_id'" in query
        assert PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID in query
        assert "preparation.handoff=" in query
        assert "preparation.preparation_id" not in query
    audit = rooted_graph_query_filter_validation_sql("sample")
    assert "acquisition_id=$1 AND query_id>$2 ORDER BY query_id LIMIT $3" in audit
    assert '"sample"."provider_directory_rooted_graph_work"' in audit
    assert "payload" not in audit and "FOR SHARE" not in audit


def test_history_adoption_preserves_legacy_terminal_validation_without_authorizing_publication():
    sql, _members = history._render(_policy(), _family())
    assert PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID in sql
    assert "terminal_record_sha256 IS DISTINCT FROM terminal_digest\n" in sql
    assert "AND terminal_record_sha256 IS DISTINCT FROM legacy_terminal_digest" in sql
    assert "convert_to(terminal_input,'UTF8')" in sql
    assert "__" not in sql


def test_work_guard_preserves_all_original_checks_and_only_extends_completed_affiliation():
    original = _current_bodies()[freeze.WORK_GUARD]
    successor = freeze.wrapped_body(freeze.WORK_GUARD, SCHEMA, original)
    extension = freeze._work_guard_extension()
    assert successor.replace(extension, "", 1) == original
    assert "NEW.kind='exact_reference_search' AND NEW.resource_type='OrganizationAffiliation'" in extension
    assert "NEW.terminal_record_sha256 IS DISTINCT FROM expected_terminal" in extension
    assert successor.index(extension) > successor.index("IF NEW.missing_http_status IS NOT NULL THEN")
    assert successor.index(extension) < successor.index("IF NEW.resource_count IS DISTINCT FROM actual_resource_count")
    for field in ("expected_result", "actual_resource_count", "actual_edge_count", "NEW.terminal_page_count"):
        assert field in extension


@pytest.mark.parametrize(
    "old,new",
    [
        ("NEW.kind='exact_reference_search'", "NEW.kind='direct_read'"),
        ("NEW.resource_type='OrganizationAffiliation'", "NEW.resource_type='PractitionerRole'"),
        ("IF NEW.kind=", "IF NEW.missing_http_status IS NOT NULL AND NEW.kind="),
        (PROVIDER_DIRECTORY_ROOTED_GRAPH_AFFILIATION_FILTER_RECIPE_ID, "tampered-recipe"),
    ],
)
def test_work_guard_custody_refuses_changed_kind_missing_or_recipe(old, new):
    original = _current_bodies()[freeze.WORK_GUARD]
    successor = freeze.wrapped_body(freeze.WORK_GUARD, SCHEMA, original)
    assert old in successor
    assert not freeze.is_approved_successor(
        freeze.WORK_GUARD, SCHEMA, successor.replace(old, new, 1), freeze.ORIGINAL_DIGESTS[freeze.WORK_GUARD]
    )
