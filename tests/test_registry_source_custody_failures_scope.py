# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate CHECK scopes cannot borrow another acquisition's authority."""

from copy import deepcopy

import pytest

from process import provider_directory_source_acquisition_custody as custody

_ACQUISITION = "pdrga_" + "1" * 48
_FOREIGN_ACQUISITION = "pdrga_" + "2" * 48


def _candidate(*, root=None, attached=True, loading=False):
    root = root or custody.BULK_ROOTS[1]
    name = "incoming_resource"
    constraints = []
    if attached:
        constraints.append(_scope_constraint(name + "_scope", _ACQUISITION))
    return dict(
        name=name,
        root=root,
        attached=attached,
        loading=loading,
        scope_values=[_ACQUISITION],
        catalog=dict(constraints=constraints),
        dependencies=[],
    )


def _scope_constraint(name, acquisition):
    return dict(
        oid="41",
        conname=name,
        contype="c",
        constraint_definition=f"CHECK (acquisition_id::text = '{acquisition}'::text)",
    )


@pytest.mark.parametrize("attached", [False, True])
def test_candidate_scope_accepts_only_its_exact_acquisition_without_mutating_catalog(attached):
    candidate = _candidate(attached=attached)
    before = deepcopy(candidate)
    custody._require_relation_checks(candidate, {})
    assert candidate == before


@pytest.mark.parametrize("fault", ["missing", "duplicate_alias", "foreign", "detached_with_scope"])
def test_candidate_scope_refuses_missing_ambiguous_foreign_or_unattached_authority(fault):
    candidate = _candidate()
    constraints = candidate["catalog"]["constraints"]
    error = "candidate_scope_changed"
    if fault == "missing":
        constraints.clear()
    elif fault == "duplicate_alias":
        constraints.append(_scope_constraint("pdrg_candidate_scope", _ACQUISITION))
    elif fault == "foreign":
        constraints[0] = _scope_constraint(candidate["name"] + "_scope", _FOREIGN_ACQUISITION)
        error = "scope_expression_changed"
    else:
        candidate["attached"] = False
    before = deepcopy(candidate)
    with pytest.raises(ValueError, match="source_acquisition_" + error):
        custody._require_relation_checks(candidate, {})
    assert candidate == before


def test_resource_candidate_cannot_smuggle_an_additional_executable_check():
    candidate = _candidate()
    candidate["catalog"]["constraints"].append(
        dict(oid="42", conname="foreign_function", contype="c", constraint_definition="CHECK (untrusted())")
    )
    with pytest.raises(ValueError, match="source_acquisition_candidate_checks_changed"):
        custody._require_relation_checks(candidate, {})


@pytest.mark.parametrize("root", [custody.BULK_ROOTS[0], custody.BULK_ROOTS[2]])
def test_loading_work_heap_must_not_execute_finished_checks(root):
    candidate = _candidate(root=root, loading=True)
    custody._require_relation_checks(candidate, {})
    candidate["catalog"]["constraints"].append(
        dict(oid="42", conname="finished_check", contype="c", constraint_definition="CHECK (true)")
    )
    with pytest.raises(ValueError, match="source_acquisition_loading_checks_changed"):
        custody._require_relation_checks(candidate, {})


def test_finished_work_heap_requires_the_complete_expected_check_set():
    root = custody.BULK_ROOTS[0]
    candidate = _candidate(root=root)
    expected_by_root = {root: {"finished_check": "check(true)"}, "_dependencies": {root: []}}
    with pytest.raises(ValueError, match="source_acquisition_checks_changed"):
        custody._require_relation_checks(candidate, expected_by_root)
    candidate["catalog"]["constraints"].append(
        dict(oid="42", conname="finished_check", contype="c", constraint_definition="CHECK (true)")
    )
    custody._require_relation_checks(candidate, expected_by_root)
