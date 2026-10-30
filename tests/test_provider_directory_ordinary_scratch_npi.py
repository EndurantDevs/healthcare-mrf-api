# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Issued scratch custody and bounded NPI repair refuse owner or scope substitution."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_ordinary_scratch_npi as repair
from process import provider_directory_owned_wal_transaction as owned
from tests.provider_directory_ordinary_scratch_npi_support import issued, scratch_case
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


async def test_original_owner_gets_target_oids():
    case = scratch_case()
    with issued(case):
        captured, selected = repair._owner(case.directory, case.admission)
        assert captured is case.capture
        assert selected == tuple((root, case.names[root], case.oids[root]) for root in repair.npi.TARGETS)
        with pytest.raises(RuntimeError, match="custody_changed"):
            await _foreign_owner(case)
    assert case.admission._ordinary_artifact_scope is None


async def _foreign_owner(case):
    async def inspect():
        return repair._owner(case.directory, case.admission)

    return await asyncio.create_task(inspect())


@pytest.mark.parametrize(
    "damage",
    [
        "mapping",
        "oid",
        "measured",
        "external",
        "fence",
        "resources",
        "execution",
        "profile",
        "not_started",
        "cutover",
        "capture",
    ],
)
async def test_scope_drift_refuses_before_io(damage):
    case = scratch_case()
    with issued(case):
        _damage_scope(case, damage)
        with pytest.raises((RuntimeError, ValueError)):
            repair._owner(case.directory, case.admission)


def _damage_scope(case, damage):
    directory, admission = case.directory, case.admission
    root, name = repair.npi.TARGETS[0], case.names[repair.npi.TARGETS[0]]
    actions_by_name = {
        "mapping": lambda: directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({**case.names, root: root}),
        "oid": lambda: directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS.set({**case.oids, root: 999}),
        "measured": lambda: admission._relations.update({("fixture", name): 999}),
        "external": lambda: admission._external_relations.add(("fixture", name)),
        "fence": lambda: directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(None),
        "resources": lambda: directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.set(("other",)),
        "execution": lambda: directory._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(object()),
        "profile": lambda: directory._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object()),
        "not_started": lambda: setattr(admission, "_started", False),
        "cutover": lambda: setattr(admission, "_cutover_active", True),
        "capture": lambda: setattr(admission, "captured_inputs", {"scope_payload": case.capture.scope_payload}),
    }
    actions_by_name[damage]()


@pytest.mark.parametrize("damage", ["schema", "run", "seen", "empty", "source", "transaction", "source_actor"])
async def test_selector_cannot_expand_signed_scope(monkeypatch, damage):
    case = scratch_case()
    with issued(case):
        arguments_by_field = dict(db_schema=None, run_id=None, source_ids=list(case.fence.source_ids), seen_table=None)
        if damage in {"schema", "run", "seen"}:
            arguments_by_field[{"schema": "db_schema", "run": "run_id", "seen": "seen_table"}[damage]] = "other"
        elif damage in {"empty", "source"}:
            arguments_by_field["source_ids"] = [] if damage == "empty" else ["other"]
        elif damage == "transaction":
            case.directory.db._transaction_binding = lambda: object()
        else:
            monkeypatch.setattr(repair.source, "is_source_storage_required", lambda: True)
        with pytest.raises(RuntimeError, match="custody_changed"):
            await repair.backfill_ordinary_scratch_npis(case.directory, **arguments_by_field)


async def test_scope_restores_after_failure():
    case = scratch_case()
    with pytest.raises(ValueError, match="stop"):
        with issued(case):
            raise ValueError("stop")
    assert case.admission._ordinary_artifact_scope is None and preparation._ACTIVE.get() is None


async def test_raw_maps_confer_no_authority(monkeypatch):
    case = scratch_case()
    with issued(case):
        case.admission._ordinary_artifact_scope = None
        monkeypatch.setattr(repair.npi, "_capture_operation", AsyncMock())
        with pytest.raises(RuntimeError, match="custody_changed"):
            await repair.npi.backfill_resource_id_npis(case.directory, source_ids=list(case.fence.source_ids))
        repair.npi._capture_operation.assert_not_awaited()


@pytest.mark.parametrize("bad_count", [False, True])
async def test_each_keyset_checks_bounded_growth(monkeypatch, bad_count):
    case = scratch_case()
    with issued(case):
        directory, admission = case.directory, case.admission
        root = repair.npi.TARGETS[0]
        source_id = case.fence.source_ids[0]
        directory.db.first = AsyncMock(
            side_effect=[
                dict(expected=1, changed=0 if bad_count else 1, last_source=source_id, last_resource="1234567890"),
                dict(expected=0, changed=0, last_source=None, last_resource=None),
            ]
        )
        directory.db.scalar = AsyncMock(return_value=0)
        growth = AsyncMock()
        monkeypatch.setattr(repair, "check_ordinary_retained_growth", growth)
        operation = repair._repair_target(
            directory,
            admission,
            (root, case.names[root], case.oids[root]),
            dict(run_id=None, source_ids=[source_id]),
            None,
        )
        if bad_count:
            with pytest.raises(RuntimeError, match="custody_changed"):
                await operation
            assert directory.db.first.await_count == growth.await_count == 1
        else:
            assert await operation == 1
            assert directory.db.first.await_count == growth.await_count == 2
            first, second = directory.db.first.await_args_list
            assert first.kwargs["after_source"] is None
            assert (second.kwargs["after_source"], second.kwargs["after_resource"]) == (source_id, "1234567890")
            assert first.kwargs["batch_size"] == min(512, admission.plan.batch_size)
            assert 'UPDATE "fixture"."' + case.names[root] + '"' in first.args[0]
            assert 'UPDATE "fixture"."' + root + '"' not in first.args[0]


async def test_absent_admission_preserves_route():
    assert await repair.backfill_ordinary_scratch_npis(SimpleNamespace(), None, None, None, None) is None


@pytest.mark.parametrize("damage", [None, "owner", "oid", "schema", "missing", "layout"])
async def test_target_recheck_follows_both_locks(monkeypatch, damage):
    case = scratch_case()
    actor_by_field = dict(owner=case.capture.scope_payload["owner_oid"])
    catalog_rows = [
        dict(oid=case.oids[root], owner=actor_by_field["owner"], schema_oid=case.capture.scope_payload["schema_oid"])
        for root in repair.npi.TARGETS
    ]
    if damage in {"owner", "oid", "schema"}:
        catalog_rows[0][{"schema": "schema_oid"}.get(damage, damage)] += 1
    elif damage == "missing":
        catalog_rows[0] = None
    driver = SimpleNamespace(fetchrow=AsyncMock(side_effect=catalog_rows))
    case.directory.db.status = AsyncMock()
    monkeypatch.setattr(repair.receipts, "ordinary", AsyncMock(return_value=(None, driver, actor_by_field)))
    native_layout = AsyncMock(side_effect=ValueError("layout changed") if damage == "layout" else None)
    monkeypatch.setattr(repair.layouts, "capture_scope_layout", native_layout)
    measured_relations = [
        preparation.OwnedRelation("fixture", case.names[root], case.oids[root], 8192, "u")
        for root in repair.npi.TARGETS
    ]
    monkeypatch.setattr(case.admission, "measure", AsyncMock(return_value=measured_relations))
    with issued(case):
        _capture, selected = repair._owner(case.directory, case.admission)
        if damage is None:
            assert await repair._lock_targets(case.directory, case.admission, selected) == actor_by_field
            assert native_layout.await_count == 2
        else:
            with pytest.raises((RuntimeError, ValueError)):
                await repair._lock_targets(case.directory, case.admission, selected)
    lock = case.directory.db.status.await_args.args[0]
    assert "SHARE ROW EXCLUSIVE MODE NOWAIT" in lock
    assert all(case.names[root] in lock for root in repair.npi.TARGETS)


def _owned_observers(monkeypatch, case, fail_second):
    async def ordinary(database):
        outcome = owned.current_owned_wal_transaction(database)
        assert outcome is not None and outcome.session is database._transaction_binding().session
        return outcome.session, outcome.retained_driver, dict(owner=case.capture.scope_payload["owner_oid"])

    @asynccontextmanager
    async def bounded_sql(directory, admission):
        assert directory is case.directory and admission is case.admission
        await ordinary(directory.db)
        yield

    async def lock_targets(directory, _admission, _selected):
        return (await ordinary(directory.db))[2]

    async def repair_target(directory, _admission, selected, _selector, _decision):
        await ordinary(directory.db)
        if fail_second and selected[0] == repair.npi.TARGETS[1]:
            raise ValueError("second target refused")
        return 1

    monkeypatch.setattr(case.admission, "assert_ready", AsyncMock())
    monkeypatch.setattr(case.directory.db, "scalar", AsyncMock(return_value=None))
    monkeypatch.setattr(repair.preparation, "nonprofile_sql_transaction", bounded_sql)
    monkeypatch.setattr(repair.receipts, "ordinary", ordinary)
    monkeypatch.setattr(repair, "_lock_targets", lock_targets)
    monkeypatch.setattr(repair, "_repair_target", repair_target)
    monkeypatch.setattr(repair, "check_ordinary_retained_growth", AsyncMock())


@pytest.mark.parametrize("fail_second", [False, True])
async def test_repairs_commit_or_rollback_together(owned_wave, monkeypatch, fail_second):
    pool, database, *_rest = owned_wave
    case = scratch_case(database)
    _owned_observers(monkeypatch, case, fail_second)
    with issued(case):
        operation = repair.backfill_ordinary_scratch_npis(case.directory, None, None, list(case.fence.source_ids), None)
        if fail_second:
            with pytest.raises(owned.OwnedWalTransactionError) as caught:
                await operation
            assert isinstance(caught.value.__cause__, ValueError)
            assert str(caught.value.__cause__) == "second target refused"
            assert caught.value.outcome.cleanup_complete and not caught.value.outcome.is_committed
        else:
            assert await operation == dict(Organization=1, Practitioner=1)
    assert len(pool.connections) == 1 and pool.checkedout == 0
    connection = pool.connections[0]
    assert connection.closed and database._transaction_binding() is None
    assert ("rollback" in connection.events) is fail_second
    assert ("commit" in connection.events) is not fail_second
    assert case.admission.assert_ready.await_count == (1 if fail_second else 2)


def test_batch_cursor_uses_explicit_c_order_for_every_keyset_boundary():
    statement = repair._batch_sql("SELECT 1", '"fixture"."scratch"')
    assert '(source_id COLLATE "C",resource_id COLLATE "C")' in statement
    assert '(CAST(:after_source AS varchar) COLLATE "C",CAST(:after_resource AS varchar) COLLATE "C")' in statement
    assert 'ORDER BY source_id COLLATE "C",resource_id COLLATE "C" LIMIT :batch_size' in statement
    assert statement.count('ORDER BY source_id COLLATE "C" DESC,resource_id COLLATE "C" DESC LIMIT 1') == 2
