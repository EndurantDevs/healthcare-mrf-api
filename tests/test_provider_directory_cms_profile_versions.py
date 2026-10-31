# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected evidence versions select exact collectors and keep legacy pins unchanged."""

import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_canonical_writer_fence as writers
from process import provider_directory_cms_preparation_policy as policy_checks
from tests.test_provider_directory_cms_canonical_writer_fence import authentic_callees as authentic_callees
from tests.test_provider_directory_cms_canonical_writer_fence import case as case
from tests.test_provider_directory_cms_preparation_custody import evidence as evidence


@pytest.mark.parametrize("contract", [policy_checks.CONTRACT, policy_checks.DEPENDENCY_CONTRACT])
def test_version_is_preserved_in_detached_validated_profile(evidence, contract):
    evidence.profile.update(contract=contract, fixed_descendant_oids=[5, 9])
    before = deepcopy(evidence.profile)
    validated = policy_checks._profile(evidence.profile, evidence.storage, evidence.policy)
    assert validated == before and evidence.profile == before
    validated["fixed_descendant_oids"].append(11)
    assert evidence.profile == before


@pytest.mark.asyncio
@pytest.mark.parametrize("contract", [policy_checks.CONTRACT, policy_checks.DEPENDENCY_CONTRACT])
async def test_protected_version_selects_only_its_collector(evidence, monkeypatch, contract):
    from process import provider_directory_cms_dependency_policy as dependencies

    evidence.profile.update(contract=contract, fixed_descendant_oids=[5, 9])
    original = AsyncMock(return_value={"original": "raw v2"})
    complete = AsyncMock(return_value={"complete": "raw v3"})
    monkeypatch.setattr(policy_checks, "_collect", original)
    monkeypatch.setattr(dependencies, "_collect", complete)
    chosen, unused = (original, complete) if contract == policy_checks.CONTRACT else (complete, original)
    observed = await policy_checks._collect_profile(
        evidence.connection, evidence.storage, evidence.policy, evidence.profile
    )
    assert observed == chosen.return_value
    chosen.assert_awaited_once_with(evidence.connection, evidence.storage, evidence.policy, [5, 9])
    unused.assert_not_awaited()


@pytest.mark.asyncio
async def test_v2_observation_preserves_existing_call_shape(evidence, monkeypatch):
    evidence.profile.update(contract=policy_checks.CONTRACT, fixed_descendant_oids=[5])
    original = AsyncMock(return_value=evidence.observed)
    monkeypatch.setattr(policy_checks, "_observe", original)
    assert (
        await policy_checks._observe_profile(evidence.connection, evidence.storage, evidence.policy, evidence.profile)
        == evidence.observed
    )
    original.assert_awaited_once_with(evidence.connection, evidence.storage, evidence.policy, [5])


@pytest.mark.asyncio
async def test_v3_observation_hashes_every_complete_raw_inventory(evidence, monkeypatch):
    evidence.profile.update(contract=policy_checks.DEPENDENCY_CONTRACT, fixed_descendant_oids=[])
    raw_by_kind = dict(canonical={"dependency_relations": ["complete"]}, private=[], routines=[], roles={})
    complete = AsyncMock(return_value=raw_by_kind)
    old = AsyncMock(side_effect=AssertionError("v3 cannot use the old digest inventory"))
    monkeypatch.setattr(policy_checks, "_collect_profile", complete)
    monkeypatch.setattr(policy_checks, "_observe", old)
    assert await policy_checks._observe_profile(
        evidence.connection, evidence.storage, evidence.policy, evidence.profile
    ) == policy_checks.custody._evidence_digests(raw_by_kind)
    complete.assert_awaited_once()
    old.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_version_refuses_before_any_collector(evidence, monkeypatch):
    evidence.profile.update(contract="cms-preparation-custody-evidence-v4", fixed_descendant_oids=[])
    collector = AsyncMock()
    monkeypatch.setattr(policy_checks, "_collect", collector)
    with pytest.raises(ValueError, match="profile_required"):
        await policy_checks._collect_profile(evidence.connection, evidence.storage, evidence.policy, evidence.profile)
    collector.assert_not_awaited()


def _dependency_writer(case, monkeypatch):
    from process import provider_directory_cms_canonical_writer_fence as writers
    from process import provider_directory_cms_dependency_policy as dependencies
    from tests.test_provider_directory_cms_canonical_writer_fence import _relation

    parents = [_relation(17, "read_dependency")]
    histories = [_relation(oid, name) for oid, name in enumerate(dependencies.HISTORY_ROOTS, 18)]
    plan_rows = [{"work_oid": 18, "resource_oid": 19}]
    physical_rows = [dict(oid=oid, native={"rowtypes": []}) for oid in [1, 2, 3, 17, 18, 19]]
    evidence_by_kind = dict(
        canonical={
            **deepcopy(case.canonical_reader.return_value[0]),
            "dependency_relations": parents,
            "dependency_histories": histories,
            "dependency_migration_plan": {dependencies.PROVENANCE_ROOT: plan_rows},
            "physical": physical_rows,
        },
        private=[],
        routines=[{"oid": 99}],
        roles={},
    )
    profile = case.arguments["cms_profile"]
    profile["contract"] = policy_checks.DEPENDENCY_CONTRACT
    profile.update(writers.custody._evidence_digests(evidence_by_kind))
    monkeypatch.setattr(dependencies, "_parents", AsyncMock(side_effect=lambda *arguments: deepcopy(parents)))
    monkeypatch.setattr(
        dependencies, "_history_relations", AsyncMock(side_effect=lambda *arguments: deepcopy(histories))
    )
    monkeypatch.setattr(
        dependencies, "_read_migration_plan", AsyncMock(side_effect=lambda *arguments: deepcopy(plan_rows))
    )
    monkeypatch.setattr(
        policy_checks,
        "_collect_profile",
        AsyncMock(side_effect=lambda *arguments: deepcopy(evidence_by_kind)),
    )
    return evidence_by_kind


@pytest.mark.asyncio
async def test_v3_writer_retains_full_read_closure_without_payload_locks(case, monkeypatch):
    from process import provider_directory_cms_canonical_writer_fence as writers
    from tests.test_provider_directory_cms_canonical_writer_fence import _acquire, _prepare

    evidence_by_kind = _dependency_writer(case, monkeypatch)
    prepared = await _prepare(case)
    retained = writers.json.loads(prepared.native)
    assert retained["read_dependencies"] == evidence_by_kind["canonical"]["dependency_relations"]
    assert retained["dependency_histories"] == evidence_by_kind["canonical"]["dependency_histories"]
    assert retained["dependency_migration_plan"] == evidence_by_kind["canonical"]["dependency_migration_plan"]
    assert "retained_routines" not in retained
    fence = await _acquire(case, prepared)
    await writers.require_cms_canonical_writer_fence(case.connection, fence)
    for call in case.connection.fetchval.await_args_list:
        if call.args[0] in {writers._OWNERS_SQL, writers._DEPENDENCIES_SQL, writers._DEFINER_AUTHORITY_SQL}:
            assert call.args[1] == [1, 2, 3, 17, 18, 19]
    for call in case.connection.fetch.await_args_list:
        if call.args[0] in {writers.custody._ROUTINES_SQL, writers._FOREIGN_SQL}:
            assert call.args[1] == [1, 2, 3, 17, 18, 19]
    assert case.connection.execute.await_count == 1
    assert all(
        relation["relname"] not in case.connection.execute.await_args.args[0]
        for field in ("dependency_relations", "dependency_histories")
        for relation in evidence_by_kind["canonical"][field]
    )


@pytest.mark.asyncio
async def test_v3_read_dependency_change_refuses_before_terminal_mutation(case, monkeypatch):
    from tests.test_provider_directory_cms_canonical_writer_fence import _acquire, _prepare

    evidence_by_kind = _dependency_writer(case, monkeypatch)
    prepared = await _prepare(case)
    evidence_by_kind["canonical"]["dependency_relations"][0]["relowner"] += 1
    with pytest.raises(ValueError, match="read_dependency_changed"):
        await _acquire(case, prepared)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["oid", "relowner", "native"])
async def test_v3_history_catalog_drift_refuses_before_terminal_work(case, monkeypatch, field):
    from tests.test_provider_directory_cms_canonical_writer_fence import _acquire, _prepare

    evidence_by_kind = _dependency_writer(case, monkeypatch)
    prepared = await _prepare(case)
    history = evidence_by_kind["canonical"]["dependency_histories"][0]
    history[field] = {"parents": [9]} if field == "native" else history[field] + 1
    with pytest.raises(ValueError, match="dependency_history_changed"):
        await _acquire(case, prepared)


@pytest.mark.asyncio
async def test_v3_provenance_row_is_rechecked_after_terminal_lock(case, monkeypatch):
    from process import provider_directory_cms_dependency_policy as dependencies
    from tests.test_provider_directory_cms_canonical_writer_fence import _prepare

    evidence_by_kind = _dependency_writer(case, monkeypatch)
    prepared = await _prepare(case)
    evidence_by_kind["canonical"]["dependency_migration_plan"][dependencies.PROVENANCE_ROOT][0]["work_oid"] += 1
    case.state.locks = True
    fence = writers._WriterFence(prepared, asyncio.get_running_loop().time() + 5)
    with pytest.raises(ValueError, match="dependency_plan_changed"):
        await writers.require_cms_canonical_writer_fence(case.connection, fence)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["dependency_histories", "dependency_migration_plan", "physical"])
async def test_v3_all_new_canonical_fields_remain_digest_authenticated(case, monkeypatch, field):
    from tests.test_provider_directory_cms_canonical_writer_fence import _prepare

    evidence_by_kind = _dependency_writer(case, monkeypatch)
    evidence_by_kind["canonical"][field] = []
    with pytest.raises(ValueError, match="protected_profile_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_retained_canonical_base_must_match_fresh_fixed_family(case, monkeypatch):
    from tests.test_provider_directory_cms_canonical_writer_fence import _prepare

    _dependency_writer(case, monkeypatch)
    case.canonical_reader.return_value[0]["migration_plan"] = {"changed": True}
    with pytest.raises(ValueError, match="canonical_policy_changed"):
        await _prepare(case)
    case.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_v3_source_proof_roots_are_retained_with_complete_canonical_evidence(case, monkeypatch):
    evidence_by_kind = _dependency_writer(case, monkeypatch)
    profile = case.arguments["cms_profile"]
    fingerprints, routine_oids, canonical = await writers._profile_fingerprints(
        case.connection, case.arguments["storage_binding"], case.arguments["policy"], profile, True
    )
    assert fingerprints == writers.custody._evidence_digests(evidence_by_kind)
    assert routine_oids == [99]
    assert canonical == evidence_by_kind["canonical"]
