# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable metadata comparisons remain separate from protected CMS authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_preparation_policy as policy_checks
from tests.test_provider_directory_cms_preparation_custody import evidence


@pytest.fixture
def policy_case(evidence, monkeypatch):
    evidence.profile.update(contract=policy_checks.CONTRACT, fixed_descendant_oids=[])
    evidence.observe = AsyncMock(return_value=evidence.observed)
    monkeypatch.setattr(policy_checks, "_observe", evidence.observe)
    return evidence


async def _compare(case):
    return await policy_checks.compare_cms_preparation_policy(
        case.connection, storage_binding=case.storage, policy=case.policy, cms_profile=case.profile
    )


@pytest.mark.asyncio
async def test_explicit_policy_compares_detached_pin_and_exact_actor(policy_case):
    case = policy_case
    assert await _compare(case) == case.observed
    case.observe.assert_awaited_once_with(case.connection, case.storage, case.policy, [])
    assert case.connection.fetchrow.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [None, {}, [True], [0], [2**32], [2, 2], [3, 2], ["2"]])
async def test_retained_oid_set_must_be_closed_and_canonical_before_io(policy_case, retained):
    case = policy_case
    case.profile["fixed_descendant_oids"] = retained
    with pytest.raises(ValueError, match="retained_descendants_required"):
        await _compare(case)
    case.connection.fetchrow.assert_not_awaited()
    case.observe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["legacy", "missing", "extra", "storage", "policy"])
async def test_new_contract_does_not_relax_existing_profile_fields(policy_case, change):
    case = policy_case
    if change == "legacy":
        case.profile["contract"] = policy_checks.custody.CONTRACT
    elif change == "missing":
        del case.profile["fixed_descendant_oids"]
    elif change == "extra":
        case.profile["allow_unknown_children"] = True
    elif change == "storage":
        case.profile["storage"]["database_oid"] += 1
    else:
        case.profile["policy"]["owner_role"] = "other_owner"
    with pytest.raises(ValueError):
        await _compare(case)
    case.connection.fetchrow.assert_not_awaited()
    case.observe.assert_not_awaited()


@pytest.mark.asyncio
async def test_caller_cannot_rewrite_retained_baseline_during_comparison(policy_case):
    case = policy_case

    async def mutate(*arguments):
        case.profile["fixed_descendant_oids"].append(91)
        case.profile["canonical_sha256"] = "9" * 64
        return {**case.observed, "canonical_sha256": "9" * 64}

    case.observe.side_effect = mutate
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _compare(case)
    assert case.observe.await_args.args[-1] == []


@pytest.mark.asyncio
async def test_transaction_replacement_refuses(policy_case):
    case = policy_case
    case.connection.fetchrow.side_effect = [case.actor.copy(), {**case.actor, "transaction": "2/2"}]
    with pytest.raises(ValueError, match="original_publisher_changed"):
        await _compare(case)


def _relation(oid, name, parents=()):
    return dict(oid=oid, nspname="catalog", relname=name, native={"parents": list(parents)})


def test_complete_descendant_walk_does_not_omit_nested_children():
    family_rows = [_relation(1, "root"), _relation(2, "child", [1]), _relation(3, "grandchild", [2])]
    assert policy_checks._family_oids(family_rows, {1}) == {1, 2, 3}


@pytest.fixture
def checked_clone_family():
    """Represent the raw output after complete dynamic allocation validation."""
    witness, resource = _relation(1, "witness"), _relation(2, "resource")
    dynamic, retained = _relation(3, "dynamic", [2]), _relation(4, "retained", [2])
    for relation in (witness, resource, dynamic, retained):
        relation["native"].update(constraints=[], triggers=[])
    witness["native"]["constraints"] = [
        [dict(oid="10", contype="f", conrelid="1", confrelid="2"), "fixed base FK"],
        [dict(oid="11", contype="f", conrelid="1", confrelid="3"), "dynamic clone"],
        [dict(oid="12", contype="f", conrelid="1", confrelid="4"), "retained clone"],
    ]
    dynamic["native"]["triggers"] = [
        dict(tgisinternal=True, tgconstraint="11", tgconstrrelid="1"),
        dict(tgisinternal=False, tgconstraint="0", tgconstrrelid="0"),
    ]
    retained["native"]["triggers"] = [dict(tgisinternal=True, tgconstraint="12", tgconstrrelid="1")]
    return [witness, resource, dynamic, retained]


def test_only_checked_dynamic_clone_leaves_fingerprint_without_mutating_raw_family(checked_clone_family):
    family = checked_clone_family
    before = deepcopy(family)
    fixed = policy_checks._fixed_catalog_relations(family, {1, 2, 4}, {3})
    assert [relation["oid"] for relation in fixed] == [1, 2, 4]
    assert [native["oid"] for native, _ in fixed[0]["native"]["constraints"]] == ["10", "12"]
    assert fixed[1:] == [family[1], family[3]]
    assert family == before
    original_digest = policy_checks.custody._digest(fixed)
    family[0]["native"]["constraints"][0][0]["confrelid"] = "99"
    assert (
        policy_checks.custody._digest(policy_checks._fixed_catalog_relations(family, {1, 2, 4}, {3})) != original_digest
    )


@pytest.mark.parametrize("field,value", [("contype", "c"), ("conrelid", "9"), ("confrelid", "9"), ("oid", "9")])
def test_checked_child_hook_must_match_actual_fixed_constraint(checked_clone_family, field, value):
    checked_clone_family[0]["native"]["constraints"][1][0][field] = value
    with pytest.raises(ValueError, match="foreign_clones_changed"):
        policy_checks._fixed_catalog_relations(checked_clone_family, {1, 2, 4}, {3})


def test_fixed_descendant_clones_stay_exact(checked_clone_family):
    family = checked_clone_family
    assert policy_checks._fixed_catalog_relations(family, {1, 2, 3, 4}, set()) == family


@pytest.mark.asyncio
@pytest.mark.parametrize("collection", ["objects", "v2"])
async def test_disjoint_allocations_stay_out_of_fixed_role_and_catalog_fingerprints(evidence, monkeypatch, collection):
    """Every dynamic child is checked; only exact fixed objects feed role pins."""
    root, history = _relation(1, policy_checks.TABLES[0]), _relation(2, "history", [1])
    fixed_by_field = {"relations": [root, history], "migration_plan": {"complete": True}}
    private_rows = [_relation(4, "private")]
    initial_rows = [root, history, _relation(3, "first", [1])]
    changed_rows = [*initial_rows, _relation(5, "second", [1])]
    canonical = AsyncMock(
        side_effect=[(deepcopy(fixed_by_field), initial_rows), (deepcopy(fixed_by_field), changed_rows)]
    )
    monkeypatch.setattr(policy_checks, "_canonical", canonical)
    monkeypatch.setattr(policy_checks, "_coordinates", AsyncMock())
    monkeypatch.setattr(policy_checks.custody, "_inventory", AsyncMock(return_value=private_rows))
    frontier_rows = [{"oid": 7}]
    monkeypatch.setattr(policy_checks.custody, "_callee_frontier", AsyncMock(return_value=frontier_rows))

    async def role_snapshot(query, oids, namespaces, routines, actors):
        assert query == policy_checks.custody._ROLES_SQL
        assert oids == [1, 2, 4] and routines == [7]
        return {"fixed": True}

    connection = SimpleNamespace(
        execute=AsyncMock(),
        fetch=AsyncMock(return_value=[dict(oid=7, native={})]),
        fetchval=AsyncMock(side_effect=role_snapshot),
    )
    if collection == "objects":
        observed = await policy_checks._collect_objects(connection, evidence.storage, evidence.policy, [])
        assert observed == dict(canonical=fixed_by_field, private=private_rows, routines=[dict(oid=7, native={})])
        connection.fetchval.assert_not_awaited()
    else:
        observed = await policy_checks._observe(connection, evidence.storage, evidence.policy, [])
        assert observed["canonical_sha256"] == policy_checks.custody._digest(fixed_by_field)
        assert observed["roles_sha256"] == policy_checks.custody._digest({"fixed": True})
        connection.fetchval.assert_awaited_once()
    assert canonical.await_count == 2
    assert "ONLY" in connection.execute.await_args.args[0] and "NOWAIT" in connection.execute.await_args.args[0]
    assert '"first"' not in connection.execute.await_args.args[0]
    assert '"second"' not in connection.execute.await_args.args[0]


@pytest.mark.asyncio
async def test_v2_collector_keeps_original_role_parameters_and_four_digests(evidence, monkeypatch):
    objects_by_kind = dict(
        canonical={"relations": [{"oid": 9}, {"oid": 2}], "migration_plan": {"complete": True}},
        private=[{"oid": 7}],
        routines=[{"oid": 21, "native": {"fixed": True}}, {"oid": 18, "native": {"fixed": False}}],
    )
    before = deepcopy(objects_by_kind)
    collector = AsyncMock(return_value=objects_by_kind)
    monkeypatch.setattr(policy_checks, "_collect_objects", collector)
    roles_by_field = {"roles": [], "reach": [[1, 2, True, False]], "settings": []}
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=roles_by_field))
    observed = await policy_checks._collect(connection, evidence.storage, evidence.policy, [31])
    collector.assert_awaited_once_with(connection, evidence.storage, evidence.policy, [31])
    connection.fetchval.assert_awaited_once_with(
        policy_checks.custody._ROLES_SQL,
        [2, 7, 9],
        [evidence.storage.catalog_schema, evidence.storage.storage_schema],
        [21, 18],
        [evidence.policy.owner_role, evidence.policy.publisher_role, *evidence.policy.runtime_roles],
    )
    expected_by_kind = {**before, "roles": roles_by_field}
    assert list(observed) == ["canonical", "private", "routines", "roles"]
    assert observed == expected_by_kind and objects_by_kind == before
    assert policy_checks.custody._evidence_digests(observed) == policy_checks.custody._evidence_digests(
        expected_by_kind
    )
