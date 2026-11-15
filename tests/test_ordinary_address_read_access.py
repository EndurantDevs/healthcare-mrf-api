# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary replacements retain exact reader access inside their original cut."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_read_access as access
from process import provider_directory_serving_recovery as recovery


def _read_driver(*, grants=(), relations=None):
    from process.native_publication_read_access import _RELATIONS_SQL

    selected_relations = _relations() if relations is None else relations

    async def fetch(statement, *_parameters):
        return selected_relations if statement == _RELATIONS_SQL else grants

    return SimpleNamespace(
        fetch=AsyncMock(side_effect=fetch), execute=AsyncMock(), fetchval=AsyncMock(), is_in_transaction=lambda: True
    )


def _swap():
    return SimpleNamespace(
        live_cls=SimpleNamespace(__main_table__="live"), stage_cls=SimpleNamespace(__tablename__="stage")
    )


@pytest.mark.asyncio
async def test_ordinary_replay_keeps_exact_select_scope_and_rechecks_exclusive(monkeypatch):
    grants = [
        {
            "column_name": column,
            "rolname": role,
            "grantee": grantee,
            "privilege_type": "SELECT",
            "is_grantable": grantable,
        }
        for column, role, grantee, grantable in [
            (None, "reader", 2, False),
            ("value", "column_reader", 3, True),
            (None, None, 0, False),
        ]
    ]
    driver = _read_driver(grants=grants)
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=driver))
    selections = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    await access.recheck_ordinary_read_access(object(), selections)
    expected_statements = [
        'GRANT SELECT ON TABLE "sample"."stage" TO "reader"',
        'GRANT SELECT ("value") ON TABLE "sample"."stage" TO "column_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."stage" TO PUBLIC',
    ]
    assert [
        call.args[0] for call in driver.execute.await_args_list if call.args[0].startswith("GRANT")
    ] == expected_statements * 2
    assert selections[0].__dict__ == {"schema": "sample", "live_table": "live", "stage_table": "stage"}
    driver.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_first_publication_does_not_invent_reader_grants(monkeypatch):
    relations = _relations()
    relations[0]["oid"] = None
    driver = _read_driver(relations=relations)
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=driver))
    selections = await access.prepare_ordinary_read_access(object(), "sample", (_swap(), _swap()))
    assert len(selections) == 2
    assert not any(call.args[0].startswith("GRANT") for call in driver.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["stage_visible", "incumbent_grant", "stage_grant", "oid", "actor"])
async def test_ordinary_replay_refuses_privacy_or_drift(monkeypatch, damage):
    """Native failures propagate; old actor/catalog fields are absent from portable name selections."""
    driver = _read_driver()
    if damage in {"incumbent_grant", "stage_grant"}:
        driver.execute.side_effect = PermissionError("native read grant denied")
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=driver))
    if damage in {"incumbent_grant", "stage_grant"}:
        with pytest.raises(PermissionError, match="native read grant denied"):
            await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    else:
        selections = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
        assert selections[0].__dict__ == {"schema": "sample", "live_table": "live", "stage_table": "stage"}
    driver.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_read_access_cannot_cross_task_or_transaction(monkeypatch):
    """The declared names are portable; each executing transaction uses its actual native access."""
    first, second = _read_driver(), _read_driver()
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(side_effect=[first, second]))
    selections = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    await asyncio.create_task(access.recheck_ordinary_read_access(object(), selections))
    assert first.execute.await_count and second.execute.await_count
    assert selections[0].__dict__ == {"schema": "sample", "live_table": "live", "stage_table": "stage"}


def _relations():
    return [
        dict(
            name=name,
            oid=oid,
            relowner=1,
            relkind="r",
            relpersistence="p",
            relrowsecurity=False,
            relforcerowsecurity=False,
        )
        for name, oid in (("live", 11), ("stage", 12))
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "rls", "kind", "stage_missing", "missing_lock"])
async def test_ordinary_capture_requires_owner_plain_heaps_and_native_locks(monkeypatch, damage):
    from process import reference_family_archive as native

    relations = deepcopy(_relations())
    if damage == "rls":
        relations[1]["relrowsecurity"] = True
    elif damage == "kind":
        relations[1]["relkind"] = "v"
    elif damage == "stage_missing":
        relations[1]["oid"] = None
    driver = _read_driver(relations=relations)
    monkeypatch.setattr(native, "_native_model_copy_driver", AsyncMock(return_value=driver))
    database = SimpleNamespace(
        _transaction_binding=lambda: SimpleNamespace(session=SimpleNamespace(in_transaction=lambda: True))
    )
    if damage in {"rls", "kind", "stage_missing"}:
        with pytest.raises(RuntimeError, match="publication_read_access_"):
            await access.prepare_ordinary_read_access(database, "sample", (_swap(),))
    else:
        await access.prepare_ordinary_read_access(database, "sample", (_swap(),))
        assert any("IN ACCESS SHARE MODE NOWAIT" in call.args[0] for call in driver.execute.await_args_list)
    driver.fetchval.assert_not_awaited()


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
        "generation": 4,
        "before": {"roots": {recovery.receipts.ROOTS[0]: original}},
    }
    monkeypatch.setattr(artifacts, "current", lambda _directory: SimpleNamespace(recovery=selection_by_field))
    monkeypatch.setattr(
        access, "prepare_ordinary_artifact_reads", AsyncMock(side_effect=AssertionError("already prepared"))
    )

    async def recheck(*args):
        events.append("reads rechecked")

    async def retain(*args, **kwargs):
        assert events == ["reads rechecked"] and args[-1] is original
        assert kwargs == {"generation": 4}
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
