# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full protected fingerprint checks with native IO replaced; no custody authority is supplied."""

import asyncio
from copy import deepcopy

import pytest

from process import provider_directory_cms_history_postimage as execution
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_owner_data_phases import final_case as final_case
from tests.test_provider_directory_cms_owner_permissions import owner_case as owner_case
from tests.test_provider_directory_cms_owner_postimage import structure_case as structure_case
from tests.test_provider_directory_cms_routine_bodies import body_rows as body_rows
from tests.test_provider_directory_cms_routine_settings import routines as routines


@pytest.fixture
def staged_case(final_case):
    case = final_case
    case.connection.is_in_transaction = lambda: True
    case.connection.is_closed = lambda: False
    case.profile.update(execution.custody._evidence_digests(case.final))
    return case


async def _recheck(case, *, seconds=3):
    return await execution.require_cms_history_prework_postimage(
        case.connection,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        deadline=asyncio.get_running_loop().time() + seconds,
    )


async def test_fresh_full_inventory_matches_the_post_owner_profile_without_reissued_pins(staged_case):
    case = staged_case
    original = deepcopy(case.final)
    observed, bounds = await _recheck(case)
    assert observed is case.final and bounds == {} and case.final == original
    case.collect.assert_awaited_once_with(case.connection, case.storage, case.policy, case.profile)


async def test_read_only_postimage_check_uses_the_separate_finite_prework_budget(staged_case):
    assert (await _recheck(staged_case, seconds=15))[0] is staged_case.final


@pytest.mark.parametrize("seconds", [-1, float("nan"), float("inf")])
async def test_invalid_postimage_budget_refuses_before_collection(staged_case, seconds):
    with pytest.raises(ValueError, match="deadline_required"):
        await _recheck(staged_case, seconds=seconds)
    staged_case.collect.assert_not_awaited()


@pytest.mark.parametrize("field", ["is_in_transaction", "is_closed"])
async def test_postimage_without_live_transaction_refuses_before_collection(staged_case, field):
    setattr(staged_case.connection, field, lambda: field == "is_closed")
    with pytest.raises(ValueError, match="transaction_required"):
        await _recheck(staged_case)
    staged_case.collect.assert_not_awaited()


@pytest.mark.parametrize("field", ["canonical", "private", "routines", "roles"])
async def test_unplanned_current_catalog_change_cannot_become_an_expected_pin(staged_case, field):
    native = staged_case.final[field]
    if isinstance(native, dict):
        native["unapproved"] = True
    else:
        native.append({"unapproved": True})
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _recheck(staged_case)


@pytest.mark.parametrize("fault", [None, "oid", "tree", "validated", "columns", "extra", "missing"])
def test_only_independently_proved_history_constraint_is_projected(fault):
    constraint_by_field = dict(
        oid="901",
        conname="cms_history_bound",
        conrelid="100",
        contype="c",
        conbin="fixed tree",
        convalidated=True,
        conkey=[1],
    )
    inventory_by_kind = {
        "canonical": {
            "relations": [
                dict(
                    oid=100,
                    relname="witness",
                    native={
                        "constraints": [[deepcopy(constraint_by_field), "CHECK (fixed)"], [{"oid": "902"}, "unchanged"]]
                    },
                )
            ]
        }
    }
    bounds_by_root = {
        "witness": dict(
            old_oid=100, native={"constraint": dict(oid=901, tree="fixed tree", validated=True, columns=[1])}
        )
    }
    if fault in {"oid", "tree", "validated", "columns"}:
        field = {"oid": "oid", "tree": "conbin", "validated": "convalidated", "columns": "conkey"}[fault]
        changed = {"oid": "999", "tree": "unapproved", "validated": False, "columns": [2]}[fault]
        inventory_by_kind["canonical"]["relations"][0]["native"]["constraints"][0][0][field] = changed
    elif fault == "extra":
        bounds_by_root["other"] = deepcopy(bounds_by_root["witness"])
    elif fault == "missing":
        inventory_by_kind["canonical"]["relations"] = []
    original = deepcopy(inventory_by_kind)
    if fault:
        with pytest.raises(ValueError, match="history_bound_changed"):
            execution._without_history_bounds(inventory_by_kind, bounds_by_root)
    else:
        projected_inventory = execution._without_history_bounds(inventory_by_kind, bounds_by_root)
        assert projected_inventory["canonical"]["relations"][0]["native"]["constraints"] == [
            [{"oid": "902"}, "unchanged"]
        ]
    assert inventory_by_kind == original
