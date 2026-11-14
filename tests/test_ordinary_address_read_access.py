# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary replacements replay current explicit SELECT on named tables."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_read_access as access
from process import provider_directory_serving_recovery as recovery


def _grant(principal, *, column=None, grantable=False):
    return dict(column_sql=column, principal_sql=principal, is_grantable=grantable)


def _driver(grants=(), stage_grants=()):
    return SimpleNamespace(
        fetch=AsyncMock(side_effect=lambda _sql, _schema, _table, reads: grants if reads else stage_grants),
        fetchval=AsyncMock(return_value=True),
        execute=AsyncMock(),
    )


def _swap():
    return SimpleNamespace(
        live_cls=SimpleNamespace(__main_table__="live"), stage_cls=SimpleNamespace(__tablename__="stage")
    )


@pytest.mark.asyncio
async def test_ordinary_final_replay_preserves_explicit_select(monkeypatch):
    driver = _driver(
        (_grant('"reader"'), _grant('"column_reader"', column='"value"', grantable=True), _grant("PUBLIC"))
    )
    capture = AsyncMock(return_value=driver)
    monkeypatch.setattr(access, "_ordinary_access", capture)
    proofs = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    driver.fetch.assert_not_called()
    driver.execute.assert_not_called()
    await access.recheck_ordinary_read_access(object(), proofs)
    assert [call.args[0] for call in driver.execute.await_args_list] == [
        'GRANT SELECT ON TABLE "sample"."stage" TO "reader"',
        'GRANT SELECT ("value") ON TABLE "sample"."stage" TO "column_reader" WITH GRANT OPTION',
        'GRANT SELECT ON TABLE "sample"."stage" TO PUBLIC',
    ]
    assert capture.await_args.kwargs == {"exclusive": True}
    assert driver.fetch.await_args.args[1:] == ("sample", "live", True)


@pytest.mark.asyncio
async def test_final_replay_refreshes_grants_in_later_transactions(monkeypatch):
    driver = _driver()
    driver.fetch.side_effect = [(), (_grant('"first_reader"'),), (), (_grant('"latest_reader"'),)]
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=driver))
    proofs = await access.prepare_ordinary_read_access(object(), "sample", (_swap(),))
    await access.recheck_ordinary_read_access(object(), proofs)
    await asyncio.create_task(access.recheck_ordinary_read_access(object(), proofs))
    assert [call.args[0] for call in driver.execute.await_args_list] == [
        'GRANT SELECT ON TABLE "sample"."stage" TO "first_reader"',
        'GRANT SELECT ON TABLE "sample"."stage" TO "latest_reader"',
    ]


@pytest.mark.asyncio
async def test_first_publication_does_not_invent_reader_grants(monkeypatch):
    driver = _driver()
    monkeypatch.setattr(access, "_ordinary_access", AsyncMock(return_value=driver))
    proofs = await access.prepare_ordinary_read_access(object(), "sample", (_swap(), _swap()))
    await access.recheck_ordinary_read_access(object(), proofs)
    assert len(proofs) == 2
    driver.execute.assert_not_called()


async def test_replay_clears_candidate_only_grants_before_copying_live_read_access():
    driver = _driver(
        (_grant('"reader"'),),
        (_grant('"extra"'), _grant('"extra"'), _grant('"extra"', column='"value"')),
    )
    await access._replay_select(driver, "sample", "live", "stage")
    assert [call.args[0] for call in driver.execute.await_args_list] == [
        'REVOKE ALL PRIVILEGES ON TABLE "sample"."stage" FROM "extra" CASCADE',
        'REVOKE ALL PRIVILEGES ("value") ON TABLE "sample"."stage" FROM "extra" CASCADE',
        'GRANT SELECT ON TABLE "sample"."stage" TO "reader"',
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("exclusive", [False, True])
@pytest.mark.parametrize("live_exists", [False, True])
async def test_ordinary_access_acquires_named_locks(monkeypatch, exclusive, live_exists):
    from process import provider_directory_serving_recovery_receipts as receipts

    driver = _driver()
    driver.fetchval.return_value = live_exists
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), driver, None)))
    assert await access._ordinary_access(object(), "sample", "live", "stage", exclusive=exclusive) is driver
    tables = '"sample"."live","sample"."stage"' if live_exists else '"sample"."stage"'
    mode = "ACCESS EXCLUSIVE" if exclusive else "SHARE"
    driver.execute.assert_awaited_once_with(f"LOCK TABLE {tables} IN {mode} MODE NOWAIT")
    driver.fetch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("live,stage,error", [("same", "same", RuntimeError), ("invalid.name", "stage", ValueError)])
async def test_ordinary_access_validates_names_before_database_calls(monkeypatch, live, stage, error):
    from process import provider_directory_serving_recovery_receipts as receipts

    ordinary = AsyncMock()
    monkeypatch.setattr(receipts, "ordinary", ordinary)
    with pytest.raises(error):
        await access._ordinary_access(object(), "sample", live, stage)
    ordinary.assert_not_called()


@pytest.mark.asyncio
async def test_ordinary_recheck_rejects_malformed_names(monkeypatch):
    capture = AsyncMock()
    monkeypatch.setattr(access, "_ordinary_access", capture)
    with pytest.raises(ValueError, match="read_access_invalid"):
        await access.recheck_ordinary_read_access(object(), (object(),))
    capture.assert_not_called()


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
    monkeypatch.setattr(recovery, "_journal", AsyncMock(return_value={}))
    monkeypatch.setattr(recovery.receipts, "lock", AsyncMock())
    monkeypatch.setattr(recovery.receipts, "prepare_relation", prepare)
    monkeypatch.setattr(recovery.receipts, "prepared_state", AsyncMock(return_value={"metadata": {}, "roots": {}}))
    monkeypatch.setattr(recovery.receipts, "ordinary", AsyncMock(return_value=(None, None, "writer")))
    monkeypatch.setattr(access, "prepare_ordinary_artifact_reads", copy)
    await recovery.begin_overlay(directory, selection_by_field)
    assert events == ["hash " + root for root in recovery.receipts.ROOTS] + ["copy reads", "hash incoming"]
    assert selection_by_field["read_access"] is not None
