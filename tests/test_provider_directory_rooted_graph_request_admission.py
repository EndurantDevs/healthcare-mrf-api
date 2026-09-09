# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Partial request evidence is source-bound, explicit, and single-root only."""

from dataclasses import fields, replace
from datetime import UTC, datetime
import json
from types import SimpleNamespace

import pytest

from process.fhir_request_failure_policy import (
    FHIR_REQUEST_FAILURE_POLICY_ID,
    FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
)
from process.provider_directory_rooted_graph_single_root_contract import (
    single_root_operation_payload,
)
from process.provider_directory_rooted_graph_twin_contract import (
    ProviderDirectoryRootedGraphTwinError,
    _digest_identifier,
    build_provider_directory_rooted_graph_twin_attempt,
    build_rooted_graph_single_root_admission,
)
from process.provider_directory_rooted_graph_twin_store import (
    _admission_from_row,
    _insert_authority,
    _root_from_row,
    _store_single_root_authority,
)
from tests.provider_directory_rooted_graph_publication_test_support import sealed_roots
from tests.test_provider_directory_rooted_graph_twin_store_boundaries import (
    _ScriptedDatabase,
    _record,
    _root_row,
)


def _coverage(*, rooted_total=3, rooted_failed=1):
    return {
        "policy_id": FHIR_REQUEST_FAILURE_POLICY_ID,
        "resource_coverage": FHIR_REQUEST_FAILURE_RESOURCE_COVERAGE,
        "total_requests": 1000 + rooted_total,
        "failed_requests": 1 + rooted_failed,
        "rooted_total_requests": rooted_total,
        "rooted_failed_requests": rooted_failed,
    }


def _partial_root(**changes):
    candidate = sealed_roots()[1]
    return replace(
        candidate,
        **{
            "completed_count": 2,
            "error_count": 1,
            "request_failure_coverage": _coverage(),
            **changes,
        },
    )


def _admit(root):
    return build_rooted_graph_single_root_admission(
        root,
        acquisition_operation_key="e" * 64,
        admitted_at=datetime(2026, 9, 9, tzinfo=UTC),
    )


@pytest.mark.parametrize("json_text", (False, True))
def test_partial_single_root_admission_keeps_unknown_census_and_error_proof(
    json_text,
) -> None:
    root = _partial_root(insurance_plan_count=None, insurance_plan_page_count=None)
    admission = _admit(root)
    assert admission.insurance_plan_count is None
    assert admission.insurance_plan_page_count is None
    assert admission.used_work_items - admission.completed_count == 1
    assert admission.request_failure_coverage == _coverage()
    row = _record(admission)
    if json_text:
        row["request_failure_coverage"] = json.dumps(row["request_failure_coverage"])
        row["reviewed_root_policy_json"] = json.dumps(row["reviewed_root_policy_json"])
    assert _admission_from_row(row) == admission


def test_stored_coverage_rejects_malformed_json() -> None:
    row = _record(_admit(_partial_root()))
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        _admission_from_row({**row, "request_failure_coverage": "not json"})


def test_source_budget_can_admit_zero_rooted_completions_without_fabricating_one() -> (
    None
):
    root = _partial_root(
        completed_count=0,
        error_count=3,
        resource_count=0,
        edge_count=0,
        used_resource_rows=0,
        used_edge_rows=0,
        insurance_plan_count=None,
        insurance_plan_page_count=None,
        request_failure_coverage=_coverage(rooted_failed=3),
    )
    assert _admit(root).completed_count == 0
    assert _admit(root).used_work_items == 3


@pytest.mark.parametrize(
    "changes",
    (
        {"request_failure_coverage": None},
        {"request_failure_coverage": _coverage(rooted_total=4)},
        {"request_failure_coverage": _coverage(rooted_failed=2)},
        {"request_failure_coverage": {**_coverage(), "failed_requests": 21}},
        {"pending_count": 1},
        {"leased_count": 1},
        {"used_work_items": 2},
        {"insurance_plan_count": None},
    ),
)
def test_partial_sealed_root_rejects_unbound_or_nonterminal_evidence(changes) -> None:
    with pytest.raises(ValueError, match="sealed_root_invalid"):
        _partial_root(**changes)


def test_unknown_plan_census_requires_rooted_failure_not_only_inherited_failure() -> (
    None
):
    with pytest.raises(ValueError, match="sealed_root_invalid"):
        _partial_root(
            completed_count=3,
            error_count=0,
            request_failure_coverage=_coverage(rooted_failed=0),
            insurance_plan_count=None,
            insurance_plan_page_count=None,
        )


def test_partial_root_is_ineligible_for_twin_comparison() -> None:
    baseline = sealed_roots()[0]
    with pytest.raises(ValueError, match="twin_lineage_invalid"):
        build_provider_directory_rooted_graph_twin_attempt(
            baseline, _partial_root(), attempted_at=datetime(2026, 9, 9, tzinfo=UTC)
        )


def test_admission_identity_preserves_legacy_null_proof_and_binds_partial_proof() -> (
    None
):
    legacy = _admit(sealed_roots()[1])
    legacy_values = tuple(
        (
            json.dumps(
                getattr(legacy, field.name), separators=(",", ":"), sort_keys=True
            )
            if field.name == "reviewed_root_policy_json"
            else getattr(legacy, field.name)
        )
        for field in fields(legacy)
        if field.name not in {"admission_id", "admitted_at", "request_failure_coverage"}
    )
    assert legacy.admission_id == _digest_identifier(
        "pdrgad_", (legacy.admission_contract_id, *legacy_values)
    )
    admission = _admit(_partial_root())
    changed = _admit(
        _partial_root(request_failure_coverage={**_coverage(), "total_requests": 2003})
    )
    assert admission.admission_id != changed.admission_id
    with pytest.raises(ValueError, match="admission_invalid"):
        replace(admission, request_failure_coverage=changed.request_failure_coverage)


@pytest.mark.parametrize("rooted_failed", (0, 1))
def test_stored_graph_completeness_tracks_rooted_errors_only(rooted_failed) -> None:
    root = _partial_root(
        completed_count=3 - rooted_failed,
        error_count=rooted_failed,
        request_failure_coverage=_coverage(rooted_failed=rooted_failed),
    )
    row_by_field = {**_root_row(root), "rooted_graph_complete": rooted_failed == 0}
    assert _root_from_row(row_by_field) == root
    with pytest.raises(ProviderDirectoryRootedGraphTwinError):
        _root_from_row({**row_by_field, "rooted_graph_complete": rooted_failed != 0})


@pytest.mark.asyncio
async def test_authority_insert_serializes_explicit_coverage_as_jsonb() -> None:
    admission = _admit(_partial_root())
    database = _ScriptedDatabase()
    await _insert_authority(database, admission)
    sql, parameters = database.status_calls[-1]
    assert "CAST(:request_failure_coverage AS jsonb)" in sql
    assert json.loads(parameters["request_failure_coverage"]) == _coverage()


def test_single_operation_payload_records_policy_without_changing_acquisition_identity() -> (
    None
):
    root = _partial_root()
    admission = _admit(root)
    current = SimpleNamespace(
        dataset_hash=root.root_dataset_hash,
        dataset_id=root.root_dataset_id,
        variant=root.root_dataset_variant,
        cohort_complete=True,
    )
    payload = single_root_operation_payload(current, root, admission, "e" * 64)
    assert payload["request_failure_coverage"] == _coverage()
    assert payload["acquisition"]["acquisition_id"] == root.acquisition_id


@pytest.mark.asyncio
async def test_existing_authority_replays_without_rechecking_new_budget() -> None:
    historical = _admit(sealed_roots()[1])
    expected = replace(historical, admitted_at=datetime(2026, 9, 10, tzinfo=UTC))
    database = _ScriptedDatabase(first_rows=(_record(historical),))
    assert await _store_single_root_authority(database, expected) == historical
    assert database.status_calls == []


@pytest.mark.asyncio
async def test_old_seal_cannot_mint_new_proofless_partial_authority() -> None:
    historical = _admit(sealed_roots()[1])
    database = _ScriptedDatabase(first_rows=(None,), scalars=(_coverage(rooted_failed=0),))
    with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
        await _store_single_root_authority(database, historical)
    assert database.status_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("json_text", (False, True))
async def test_fresh_authority_requires_exact_computed_partial_proof(json_text) -> None:
    expected = _admit(_partial_root())
    proof = json.dumps(_coverage()) if json_text else _coverage()
    database = _ScriptedDatabase(
        first_rows=(None, _record(expected)), scalars=(proof,),
    )
    assert await _store_single_root_authority(database, expected) == expected
    assert len(database.status_calls) == 1


@pytest.mark.asyncio
async def test_fresh_complete_authority_accepts_exact_null_proof() -> None:
    expected = _admit(sealed_roots()[1])
    database = _ScriptedDatabase(first_rows=(None, _record(expected)), scalars=(None,))
    assert await _store_single_root_authority(database, expected) == expected
    assert len(database.status_calls) == 1


@pytest.mark.asyncio
async def test_authority_replay_rejects_drift_without_fresh_insert() -> None:
    expected = _admit(sealed_roots()[1])
    other = build_rooted_graph_single_root_admission(
        sealed_roots()[1], acquisition_operation_key="f" * 64,
        admitted_at=expected.admitted_at,
    )
    for row in (_record(other), {**_record(expected), "admission_id": "invalid"}):
        database = _ScriptedDatabase(first_rows=(row,))
        with pytest.raises(ProviderDirectoryRootedGraphTwinError, match="state"):
            await _store_single_root_authority(database, expected)
        assert database.status_calls == []


@pytest.mark.asyncio
async def test_fresh_authority_preserves_budget_rejection_without_insert() -> None:
    class OverBudgetDatabase(_ScriptedDatabase):
        async def scalar(self, _statement, **_parameters):
            raise RuntimeError("provider_directory_fhir_request_failure_budget_exceeded")

    database = OverBudgetDatabase(first_rows=(None,))
    with pytest.raises(RuntimeError, match="budget_exceeded"):
        await _store_single_root_authority(database, _admit(sealed_roots()[1]))
    assert database.status_calls == []
