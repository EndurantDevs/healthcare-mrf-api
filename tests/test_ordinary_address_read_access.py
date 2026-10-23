# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary replacements retain exact reader access inside their original cut."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import code_catalog_snapshot as catalog
from process import provider_directory_artifact_read_access as access
from process import provider_directory_serving_recovery as recovery


def _grant(principal, *, column=None, privilege="SELECT", grantable=False):
    return catalog._AccessGrant(column, principal, privilege, grantable, "ordinary_owner")


def _capture(*, incumbent=None, successor=None):
    return (object(), SimpleNamespace(execute=AsyncMock()), "actor", "relations", incumbent, successor)


def _swap():
    return SimpleNamespace(
        live_cls=SimpleNamespace(__main_table__="live"), stage_cls=SimpleNamespace(__tablename__="stage")
    )


@pytest.mark.asyncio
async def test_ordinary_replay_keeps_exact_select_scope_and_rechecks_exclusive(monkeypatch):
    grants = (
        _grant("reader"),
        _grant("column_reader", column="value", grantable=True),
        _grant(None),
        _grant("writer", privilege="UPDATE"),
    )
    incumbent = catalog._RelationAccess("ordinary_owner", grants)
    private = catalog._RelationAccess("ordinary_owner", ())
    successor = catalog._RelationAccess("ordinary_owner", grants[:-1])
    before = _capture(incumbent=incumbent, successor=private)
    after = (*before[:5], successor)
    capture = AsyncMock(side_effect=[before, after, after])
    monkeypatch.setattr(access, "_ordinary_access", capture)
    proofs = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    await access.recheck_ordinary_read_access(object(), proofs)
    assert [call.args[0] for call in before[1].execute.await_args_list] == [
        'GRANT SELECT ON TABLE "sample"."stage" TO "reader"',
        'GRANT SELECT ("value") ON TABLE "sample"."stage" TO "column_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."stage" TO PUBLIC',
    ]
    assert capture.await_args.kwargs == {"exclusive": True}


@pytest.mark.asyncio
async def test_first_publication_does_not_invent_reader_grants(monkeypatch):
    private = catalog._RelationAccess("ordinary_owner", ())
    captured = _capture(successor=private)
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=captured))
    proofs = await access.prepare_ordinary_read_access(object(), "sample", (_swap(), _swap()))
    assert len(proofs) == 2
    captured[1].execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["stage_visible", "incumbent_grant", "stage_grant", "oid", "actor"])
async def test_ordinary_replay_refuses_privacy_or_drift(monkeypatch, damage):
    incumbent = catalog._RelationAccess("ordinary_owner", (_grant("reader"),))
    private = catalog._RelationAccess("ordinary_owner", ())
    before = _capture(incumbent=incumbent, successor=private)
    after = (*before[:5], incumbent)
    match damage:
        case "stage_visible":
            before = (*before[:5], incumbent)
        case "incumbent_grant":
            after = (*after[:4], private, after[5])
        case "stage_grant":
            after = (*after[:5], catalog._RelationAccess("ordinary_owner", (_grant("other"),)))
        case "oid":
            after = (*after[:3], "other-relations", *after[4:])
        case "actor":
            after = (*after[:2], "other-transaction", *after[3:])
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(side_effect=[before, after]))
    with pytest.raises(RuntimeError, match="ordinary_address_read_access_"):
        await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))


@pytest.mark.asyncio
async def test_prepared_read_access_cannot_cross_task_or_transaction(monkeypatch):
    captured = _capture(successor=catalog._RelationAccess("ordinary_owner", ()))
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=captured))
    proof = (await access.prepare_ordinary_read_access(object(), "sample", (_swap(),)))[0]
    with pytest.raises(RuntimeError, match="writer_changed"):
        await asyncio.create_task(access.recheck_ordinary_read_access(object(), (proof,)))
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=(*captured[:2], "new-xid", *captured[3:])))
    with pytest.raises(RuntimeError, match="grants_changed"):
        await access.recheck_ordinary_read_access(object(), (proof,))


def _relations():
    return [
        dict(
            name=name,
            oid=oid,
            relowner=1,
            owned=True,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
        )
        for name, oid in (("live", 11), ("stage", 12))
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "owner", "rls", "kind", "stage_missing", "missing_lock"])
async def test_ordinary_capture_requires_owner_plain_heaps_and_native_locks(monkeypatch, damage):
    from process import provider_directory_serving_recovery_receipts as receipts

    rows = deepcopy(_relations())
    if damage == "owner":
        rows[0]["owned"] = False
    elif damage == "rls":
        rows[1]["relrowsecurity"] = True
    elif damage == "kind":
        rows[1]["relkind"] = "v"
    elif damage == "stage_missing":
        rows[1]["oid"] = None
    driver = SimpleNamespace(
        fetch=AsyncMock(return_value=rows), fetchval=AsyncMock(return_value=1 if damage == "missing_lock" else 2)
    )
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), driver, {})))
    monkeypatch.setattr(
        access, "_relation_access", AsyncMock(return_value=catalog._RelationAccess("ordinary_owner", ()))
    )
    if damage is not None:
        with pytest.raises(RuntimeError, match="ordinary_address_read_access_"):
            await access._ordinary_access(object(), "sample", "live", "stage")
    else:
        await access._ordinary_access(object(), "sample", "live", "stage", exclusive=True)
        assert driver.fetchval.await_args.args[1:] == (["AccessExclusiveLock"], [11, 12])


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "no_owner_usage", "different_owners", "unsupported_isolation"])
async def test_ordinary_capture_requires_effective_native_rights_to_one_shared_owner(monkeypatch, damage):
    """Inherited ownership is native authority; table grants and mixed owners cannot replace it."""
    from process import provider_directory_serving_recovery_receipts as receipts

    relations = _relations()
    for relation in relations:
        relation["owned"] = False
    if damage == "different_owners":
        relations[0]["relowner"] = 2
    driver = SimpleNamespace(
        fetch=AsyncMock(return_value=relations),
        fetchval=AsyncMock(side_effect=[damage not in {"no_owner_usage", "unsupported_isolation"}, 2]),
    )
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), driver, {"owner": 3})))
    monkeypatch.setattr(
        access, "_relation_access", AsyncMock(return_value=catalog._RelationAccess("ordinary_owner", ()))
    )
    if damage:
        with pytest.raises(RuntimeError, match="ordinary_address_read_access_owner_changed"):
            await access._ordinary_access(object(), "sample", "live", "stage")
    else:
        await access._ordinary_access(object(), "sample", "live", "stage", exclusive=True)
    calls = driver.fetchval.await_args_list
    if damage == "different_owners":
        assert calls == []
    else:
        assert calls[0].args == (access._INHERITED_OWNER_USAGE_SQL, 1)
        assert "current_setting('transaction_isolation')='read committed'" in calls[0].args[0]
        assert "UNION" in calls[0].args[0] and "WHERE membership.inherit_option" in calls[0].args[0]
        assert len(calls) == (1 if damage else 2)


@pytest.mark.asyncio
async def test_ordinary_read_recheck_refuses_revoked_native_owner_rights(monkeypatch):
    """The final native permission check cannot reuse authority captured before revocation."""
    from process import provider_directory_serving_recovery_receipts as receipts

    rows = _relations()
    for row in rows:
        row["owned"] = False
    session, database = object(), object()
    driver = SimpleNamespace(
        fetch=AsyncMock(return_value=rows),
        fetchval=AsyncMock(side_effect=[True, 2, True, 2, False]),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(session, driver, {"owner": 3})))
    monkeypatch.setattr(
        access, "_relation_access", AsyncMock(return_value=catalog._RelationAccess("ordinary_owner", ()))
    )
    proofs = await access.prepare_ordinary_read_access(database, "sample", (_swap(),))
    with pytest.raises(RuntimeError, match="ordinary_address_read_access_owner_changed"):
        await access.recheck_ordinary_read_access(database, proofs)
    assert driver.execute.await_count == 0
    assert driver.fetchval.await_count == 5


@pytest.mark.asyncio
async def test_unmarked_address_uses_same_read_replay_under_existing_cutover(monkeypatch):
    prepare, recheck = AsyncMock(return_value=(object(),)), AsyncMock()
    monkeypatch.setattr(access, "prepare_ordinary_read_access", prepare)
    monkeypatch.setattr(access, "recheck_ordinary_read_access", recheck)
    database, swaps = object(), (_swap(),)
    await recovery.before_address(database, "sample", swaps, {})
    prepare.assert_awaited_once_with(database, "sample", swaps)
    recheck.assert_awaited_once_with(database, prepare.return_value)


@pytest.mark.asyncio
async def test_protected_address_keeps_its_existing_access_contract(monkeypatch):
    prepare = AsyncMock()
    monkeypatch.setattr(access, "prepare_ordinary_read_access", prepare)
    await recovery.before_address(
        object(), "sample", (_swap(),), {"snapshot_contract": "entity_address_unified.postgres.v2"}
    )
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_artifact_wrapper_uses_exact_stage_pair_and_refuses_other_schema(monkeypatch):
    prepare = AsyncMock(return_value=(object(),))
    monkeypatch.setattr(access, "_prepare_read_relations", prepare)
    stage = SimpleNamespace(schema="sample", target_relation="overlay", stage_table="incoming")
    result = await access.prepare_ordinary_artifact_reads(object(), "sample", (stage,))
    assert result is prepare.return_value
    assert list(prepare.await_args.args[2]) == [("overlay", "incoming")]
    prepare.reset_mock()
    with pytest.raises(RuntimeError, match="schema_changed"):
        await access.prepare_ordinary_artifact_reads(object(), "other", (stage,))
    prepare.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_generic", [False, True])
async def test_unmarked_global_and_generic_overlay_keep_reads_before_swap(monkeypatch, is_generic):
    from process import provider_directory_generic_source_artifacts as artifacts

    prepare, recheck = AsyncMock(return_value=(object(),)), AsyncMock()
    monkeypatch.setattr(access, "prepare_ordinary_artifact_reads", prepare)
    monkeypatch.setattr(access, "recheck_ordinary_read_access", recheck)
    monkeypatch.setattr(artifacts, "current", lambda _directory: SimpleNamespace(recovery=None) if is_generic else None)
    directory = SimpleNamespace(db=object(), _schema=lambda: "sample")
    stage = SimpleNamespace(schema="sample", target_relation=recovery.receipts.ROOTS[0], stage_table="incoming")
    await recovery.retain_overlay(directory, stage)
    prepare.assert_awaited_once_with(directory.db, "sample", (stage,))
    recheck.assert_awaited_once_with(directory.db, prepare.return_value)


@pytest.mark.asyncio
async def test_marked_overlay_rechecks_prepared_reads_before_retaining_old(monkeypatch):
    from process import provider_directory_generic_source_artifacts as artifacts

    events = []
    original = object()
    selection_by_field = {
        "read_access": (object(),),
        "actor": "writer",
        "before": {"roots": {recovery.receipts.ROOTS[0]: original}},
    }
    monkeypatch.setattr(artifacts, "current", lambda _directory: SimpleNamespace(recovery=selection_by_field))
    monkeypatch.setattr(
        access, "prepare_ordinary_artifact_reads", AsyncMock(side_effect=AssertionError("already prepared"))
    )

    async def recheck(*args):
        events.append("reads rechecked")

    async def retain(*args):
        assert events == ["reads rechecked"] and args[-1] is original
        events.append("old retained")

    monkeypatch.setattr(access, "recheck_ordinary_read_access", recheck)
    monkeypatch.setattr(recovery.receipts, "retain", retain)
    monkeypatch.setattr(recovery.receipts, "ordinary", AsyncMock(return_value=(None, None, "writer")))
    directory = SimpleNamespace(db=object(), _schema=lambda: "sample")
    stage = SimpleNamespace(schema="sample", target_relation=recovery.receipts.ROOTS[0])
    await recovery.retain_overlay(directory, stage)
    assert events == ["reads rechecked", "old retained"]


@pytest.mark.asyncio
async def test_source_output_owner_keeps_existing_verified_capability_path(monkeypatch):
    from process import provider_directory_output_snapshot_custody as outputs

    capability = object()
    token = outputs._OUTPUT.set(capability)
    verifier = AsyncMock()
    monkeypatch.setattr(outputs, "_require_capability", verifier)
    prepare = AsyncMock(side_effect=AssertionError("ordinary grants must not run for Source owner"))
    monkeypatch.setattr(access, "prepare_ordinary_artifact_reads", prepare)
    try:
        await recovery.retain_overlay(object(), SimpleNamespace(target_relation=recovery.receipts.ROOTS[0]))
    finally:
        outputs._OUTPUT.reset(token)
    verifier.assert_awaited_once_with(capability)
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_marked_overlay_copies_reads_before_hashing_successor(monkeypatch):
    events = []
    stage = SimpleNamespace(schema="sample", target_relation=recovery.receipts.ROOTS[0], stage_table="incoming")
    selection_by_field = {"generation": "generation", "stage": stage, "run_id": None}
    directory = SimpleNamespace(db=object(), _schema=lambda: "sample")

    async def prepare(_database, _schema, name):
        events.append("hash " + name)
        return object()

    async def copy(*args):
        assert len(events) == 2
        events.append("copy reads")
        return (object(),)

    monkeypatch.setattr(recovery, "_load_history", AsyncMock(return_value={}))
    monkeypatch.setattr(recovery.receipts, "lock", AsyncMock())
    monkeypatch.setattr(recovery.receipts, "prepare_relation", prepare)
    monkeypatch.setattr(recovery.receipts, "prepared_state", AsyncMock(return_value={}))
    monkeypatch.setattr(recovery.receipts, "ordinary", AsyncMock(return_value=(None, None, "writer")))
    monkeypatch.setattr(access, "prepare_ordinary_artifact_reads", copy)
    await recovery.begin_overlay(directory, selection_by_field)
    assert events == ["hash " + root for root in recovery.receipts.ROOTS] + ["copy reads", "hash incoming"]
    assert selection_by_field["read_access"] is not None
