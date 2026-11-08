# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Application custody/order tests; native SQL and role semantics are separate."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.sql.elements import quoted_name

from process import cms_doctors_identity_candidate as identities
from process.provider_directory_entity_identity import bind_cms_doctors_group_batch


class Connection:
    def __init__(self, events, keys):
        self.events = events
        self.keys = keys
        self.conflict = False
        self.changed = False
        self.bindings = []

    async def fetch(self, query, *arguments):
        self.events.append(("read", query))
        if "SELECT DISTINCT" in query:
            return [(key,) for key in self.keys if key > arguments[0]][:512]
        return [pair for pair in self.bindings if pair[0] in arguments[0]]

    async def fetchval(self, query, *arguments):
        self.events.append(("check", query))
        return self.conflict if "WITH needed" in query else self.changed

    async def execute(self, query):
        self.events.append(("statement", query))

    @asynccontextmanager
    async def transaction(self, *args, **kwargs):
        self.events.append(("transaction", kwargs.get("isolation", "read committed")))
        try:
            yield self.session
        except BaseException:
            self.events.append(("rollback", None))
            raise
        else:
            self.events.append(("commit", None))

    @asynccontextmanager
    async def owner(self, *args):
        try:
            self.events.append(("savepoint", None))
            yield self.publisher
        except BaseException:
            self.events.append(("rollback", None))
            raise
        else:
            self.events.append(("commit", None))


@pytest.fixture
def runtime(monkeypatch):
    events = []
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    policy = SimpleNamespace(
        schema="mrf",
        owner_role="snapshot_owner",
        publisher_role="snapshot_publisher",
        runtime_roles=("snapshot_reader",),
        control_runtime_roles=(),
    )
    connection = Connection(events, ["001", "002"])
    publisher = SimpleNamespace(connection=connection, policy=policy)
    connection.session, connection.publisher = session, publisher
    context_by_field = {
        "import_date": "identitytest",
        "context": {"group_site_stage_owned": True, "artifact": {"content_sha256": "a" * 64}},
    }
    receipt_by_field = {"source_rows": 2, "generation_id": "b" * 64}
    stage = SimpleNamespace(schema="mrf", name="cms_doctor_group_site_identitytest")

    async def capture(driver, actual_policy, names, **kwargs):
        events.append(("capture", names))
        return {name: {"name": name, "oid": 10, "owner": 20, "layout_sha256": "c" * 64} for name in names}

    async def complete(*args):
        events.append(("complete", None))
        return len(connection.keys)

    async def copy(*args, **kwargs):
        events.append(("copy", (args, kwargs)))
        return 100

    monkeypatch.setattr(identities, "make_class", lambda *args: SimpleNamespace(__table__=stage))
    monkeypatch.setattr(identities, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(identities, "source_copy_session", lambda: session)
    monkeypatch.setattr(identities, "source_transaction", connection.transaction)
    monkeypatch.setattr(identities, "source_owner_transaction", connection.owner)
    monkeypatch.setattr(identities, "_source_identity", AsyncMock(return_value=99))
    monkeypatch.setattr(identities, "_complete", complete)
    monkeypatch.setattr(identities.storage, "_close_grants", AsyncMock())
    monkeypatch.setattr(identities.storage, "_capture", capture)
    monkeypatch.setattr(identities, "native_copy_projection", copy)
    return SimpleNamespace(
        events=events,
        session=session,
        database=database,
        connection=connection,
        publisher=publisher,
        context=context_by_field,
        receipt=receipt_by_field,
        stage=stage,
        owner=connection.owner,
    )


async def prepare(runtime, kind):
    return await identities.prepare_doctors_identity_kind(
        runtime.database,
        runtime.context,
        "identitytest",
        "mrf",
        runtime.receipt,
        kind=kind,
        cancel_check=AsyncMock(),
    )


@pytest.mark.parametrize("code", ["40001", "23505"])
def test_wrapped_control_cas_conflict_requires_whole_candidate_reprepare(code):
    error = RuntimeError("native control CAS")
    error.orig = SimpleNamespace(sqlstate=code)
    with pytest.raises(identities.CMSIdentityReprepareRequired, match="whole_candidate_reprepare") as refused:
        identities.reprepare_publication_conflict(error)
    assert refused.value.__cause__ is error


def test_unrelated_publication_failure_keeps_its_original_error():
    error = RuntimeError("unrelated")
    error.orig = SimpleNamespace(sqlstate="42P01")
    assert identities.reprepare_publication_conflict(error) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("count,copies,reads", [(1, 1, 2), (125, 1, 2), (513, 2, 3)])
async def test_private_preparation_uses_bounded_copy_then_indexes_then_complete_check(runtime, count, copies, reads):
    runtime.connection.keys = [f"{i:06}" for i in range(count)]
    assert await prepare(runtime, "group") == count
    events = runtime.events
    assert sum(event[0] == "copy" for event in events) == copies
    assert sum(event[0] == "read" for event in events) == reads
    copy_indexes = [i for i, event in enumerate(events) if event[0] == "copy"]
    index_indexes = [i for i, event in enumerate(events) if event[0] == "statement" and "ADD " in event[1]]
    complete_index = next(i for i, event in enumerate(events) if event[0] == "complete")
    assert max(copy_indexes) < min(index_indexes) < complete_index
    for event, payload in events:
        if event == "copy":
            args, kwargs = payload
            assert args[0] is runtime.session
            assert kwargs["max_bytes"] == 16 * 1024 * 1024 and kwargs["timeout"] == 300
            assert "coalesce(incumbent." in args[1] and "gen_random_uuid()" in args[1]
        if event == "statement":
            assert "INSERT INTO" not in payload and "pg_advisory" not in payload
    receipt = runtime.context["context"][identities._CONTEXT_KEY]["group"]
    assert receipt.source_oid == 99 and receipt.count == count


@pytest.mark.asyncio
async def test_invalid_artifact_or_unowned_source_refuses_before_transaction(runtime):
    runtime.context["context"]["artifact"] = {}
    with pytest.raises(RuntimeError, match="artifact_required"):
        await prepare(runtime, "site")
    assert runtime.events == []
    runtime.context["context"]["artifact"] = {"content_sha256": "a" * 64}
    runtime.context["context"]["group_site_stage_owned"] = False
    with pytest.raises(RuntimeError, match="source_not_owned"):
        await prepare(runtime, "group")
    assert runtime.events == []


@pytest.mark.asyncio
async def test_oversized_source_key_denies_before_header_or_copy_transport(runtime):
    runtime.connection.changed = True
    with pytest.raises(RuntimeError, match="source_key_too_large"):
        await prepare(runtime, "site")
    assert not any(event in {"read", "copy"} for event, _ in runtime.events)
    assert runtime.events[-1][0] == "rollback"


@pytest.mark.asyncio
async def test_selected_model_quoted_names_cross_strict_identifier_boundary(runtime):
    runtime.stage.schema = quoted_name("mrf", None)
    runtime.stage.name = quoted_name(runtime.stage.name, None)
    assert await prepare(runtime, "group") == 2
    candidate = runtime.context["context"][identities._CONTEXT_KEY]["group"]
    assert type(candidate.source_name) is str
    copy_options = next(payload[1] for event, payload in runtime.events if event == "copy")
    assert type(copy_options["schema_name"]) is str and copy_options["schema_name"] == "mrf"


@pytest.mark.asyncio
async def test_startup_requires_complete_managed_global_roots(runtime, monkeypatch):
    capture = AsyncMock()
    monkeypatch.setattr(identities, "capture_generic_source_global_roots", capture)
    await identities.require_doctors_identity_roots(runtime.database)
    capture.assert_awaited_once_with(runtime.publisher, tuple(spec[0] for spec in identities._SPECS.values()))


@pytest.mark.asyncio
async def test_copy_cancellation_rolls_back_candidate_before_any_global_write(runtime, monkeypatch):
    monkeypatch.setattr(identities, "native_copy_projection", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await prepare(runtime, "group")
    assert runtime.events[-1][0] == "rollback"
    assert identities._CONTEXT_KEY not in runtime.context["context"]
    assert not any("INSERT INTO" in payload for event, payload in runtime.events if event == "statement")


@pytest.mark.asyncio
async def test_native_canonical_winner_refuses_without_remapping_or_binding_insert(runtime, monkeypatch):
    await prepare(runtime, "group")
    await prepare(runtime, "site")
    monkeypatch.setattr(identities, "_verify_candidate", AsyncMock())
    runtime.events.clear()
    runtime.connection.conflict = True
    with pytest.raises(identities.CMSIdentityReprepareRequired, match="whole_candidate_reprepare"):
        await identities.publish_doctors_identities(runtime.database, runtime.context)
    assert runtime.events[-1][0] == "rollback"
    assert sum("WITH needed" in payload for event, payload in runtime.events if event == "check") == 1
    assert not any(payload.startswith("INSERT INTO") for event, payload in runtime.events if event == "statement")


@pytest.mark.asyncio
async def test_successful_identity_cut_is_four_grouped_writes_in_one_savepoint(runtime, monkeypatch):
    await prepare(runtime, "group")
    await prepare(runtime, "site")
    verify = AsyncMock()
    monkeypatch.setattr(identities, "_verify_candidate", verify)
    runtime.events.clear()
    await identities.publish_doctors_identities(runtime.database, runtime.context)
    assert verify.await_count == 2
    assert sum(event == "savepoint" for event, _ in runtime.events) == 1
    assert runtime.events[-1][0] == "commit"
    writes = [
        payload for event, payload in runtime.events if event in ("statement", "check") and "INSERT INTO" in payload
    ]
    assert len(writes) == 4


@pytest.mark.asyncio
async def test_changed_native_policy_or_layout_refuses_before_global_dml(runtime):
    await prepare(runtime, "group")
    entry = runtime.context["context"][identities._CONTEXT_KEY]["group"]
    changed = replace(entry, policy_identity=("wrong",))
    with pytest.raises(RuntimeError, match="policy_changed"):
        await identities._verify_candidate(runtime.database, changed, runtime.context)
    changed = replace(entry, table={**entry.table, "oid": 11})
    with pytest.raises(RuntimeError, match="storage_changed"):
        await identities._verify_candidate(runtime.database, changed, runtime.context)


@pytest.mark.asyncio
async def test_scalar_compatibility_only_reads_prepared_map_and_preserves_order(runtime, monkeypatch):
    await prepare(runtime, "group")
    await prepare(runtime, "site")
    monkeypatch.setattr(identities, "_verify_candidate", AsyncMock())
    first, second = uuid4(), uuid4()
    runtime.connection.bindings = [("001", first), ("002", second)]
    runtime.events.clear()
    with pytest.raises(RuntimeError, match="prepared_read_scope_required"):
        await bind_cms_doctors_group_batch(runtime.session, org_pac_ids=["001"])
    async with identities.doctors_identity_read_scope(runtime.database, runtime.context):
        assert await bind_cms_doctors_group_batch(runtime.session, org_pac_ids=["002", "001", "002"]) == [
            second,
            first,
            second,
        ]
        with pytest.raises(RuntimeError, match="prepared_read_scope_required"):
            await asyncio.create_task(identities.prepared_doctors_ids(runtime.session, "group", ["001"]))
    assert sum(event == "read" for event, _ in runtime.events) == 1
    assert not any(event == "statement" for event, _ in runtime.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("late family failure"), asyncio.CancelledError()])
async def test_family_failure_rolls_back_identity_cut_after_catch(runtime, monkeypatch, failure):
    doctors = importlib.import_module("process.cms_doctors")

    monkeypatch.setattr(doctors, "db", runtime.database)
    monkeypatch.setattr(doctors, "source_owner_transaction", runtime.owner)
    publish = AsyncMock(side_effect=lambda *args: runtime.events.append(("published identities", None)))
    swap = AsyncMock(side_effect=failure)
    monkeypatch.setattr(doctors, "publish_doctors_identities", publish)
    monkeypatch.setattr(doctors, "_swap_cms_doctors_tables", swap)
    incumbent = object()
    monkeypatch.setattr(doctors, "capture_doctors_predecessor", AsyncMock(return_value=incumbent))
    monkeypatch.setattr(doctors, "seal_doctors_stages", AsyncMock())
    with pytest.raises(type(failure)):
        await doctors._apply_locked_cms_doctors_stage(object(), "mrf", "identitytest", identity_ctx=runtime.context)
    publish.assert_awaited_once_with(runtime.database, runtime.context)
    assert runtime.events == [("savepoint", None), ("published identities", None), ("rollback", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize("failures", [1, 4])
async def test_global_winner_reprepares_both_maps_with_fixed_retry_bound(runtime, monkeypatch, failures):
    doctors = importlib.import_module("process.cms_doctors")

    monkeypatch.setattr(doctors, "db", runtime.database)
    prepare_sources = AsyncMock(return_value={"sites": 2})
    outcomes = [identities.CMSIdentityReprepareRequired("winner") for _ in range(failures)]
    if failures == 1:
        outcomes.append(None)
    publish = AsyncMock(side_effect=outcomes)
    discard = AsyncMock()
    monkeypatch.setattr(doctors, "_prepare_cms_doctors_sources", prepare_sources)
    monkeypatch.setattr(doctors, "_publish_cms_doctors_stage", publish)
    monkeypatch.setattr(doctors, "discard_doctors_identities", discard)
    monkeypatch.setattr(doctors, "raise_if_cancelled", AsyncMock())
    if failures == 4:
        with pytest.raises(identities.CMSIdentityReprepareRequired):
            await doctors._prepare_and_publish_doctors(runtime.context, object(), "mrf", 2)
        assert prepare_sources.await_count == publish.await_count == discard.await_count == 4
    else:
        assert await doctors._prepare_and_publish_doctors(runtime.context, object(), "mrf", 2) == {"sites": 2}
        assert prepare_sources.await_count == publish.await_count == 2
        discard.assert_awaited_once_with(runtime.database, runtime.context)
    assert all(call.kwargs["identity_ctx"] is runtime.context for call in publish.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", [False, True])
async def test_cleanup_verifies_exact_private_oid_before_drop(runtime, monkeypatch, replacement):
    await prepare(runtime, "group")
    candidate = runtime.context["context"][identities._CONTEXT_KEY]["group"]
    runtime.database._transaction_binding = lambda: None
    runtime.connection.fetchval = AsyncMock(return_value=11 if replacement else 10)
    if replacement:
        monkeypatch.setattr(
            identities.storage,
            "_capture",
            AsyncMock(return_value={candidate.table["name"]: {**candidate.table, "oid": 11}}),
        )
    runtime.events.clear()
    if replacement:
        with pytest.raises(RuntimeError, match="cleanup_storage_changed"):
            await identities.discard_doctors_identities(runtime.database, runtime.context)
        assert identities._CONTEXT_KEY in runtime.context["context"]
    else:
        await identities.discard_doctors_identities(runtime.database, runtime.context)
        assert identities._CONTEXT_KEY not in runtime.context["context"]
    drops = [sql for event, sql in runtime.events if event == "statement" and sql.startswith("DROP TABLE")]
    assert len(drops) == (not replacement)
    if drops:
        assert candidate.table["name"] in drops[0]
        lock_index = next(
            i for i, (event, sql) in enumerate(runtime.events) if event == "statement" and sql.startswith("LOCK TABLE")
        )
        capture_index = next(i for i, (event, _) in enumerate(runtime.events) if event == "capture")
        drop_index = next(
            i for i, (event, sql) in enumerate(runtime.events) if event == "statement" and sql.startswith("DROP TABLE")
        )
        assert lock_index < capture_index < drop_index


@pytest.mark.asyncio
async def test_cleanup_finishes_private_drop_before_repeated_cancellation(runtime, monkeypatch):
    await prepare(runtime, "group")
    runtime.database._transaction_binding = lambda: None
    entered, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def _drop(*args):
        entered.set()
        await release.wait()
        finished.set()

    monkeypatch.setattr(identities, "_discard_identities", _drop)
    task = asyncio.create_task(identities.discard_doctors_identities(runtime.database, runtime.context))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not finished.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_retained_late_failure_rolls_back_cut_without_receipt(runtime, monkeypatch, cancelled):
    from process import cms_doctors_preparation as preparation

    prepared = SimpleNamespace(
        committed=False,
        schema="mrf",
        import_date="identitytest",
        stage_oids=(("target", "stage", 41),),
        incumbent=object(),
        incumbent_authority=object(),
        context=runtime.context["context"],
        native_receipt=None,
    )

    async def _apply(*args, **kwargs):
        runtime.events.append(("published identities and family", None))
        if cancelled:
            raise asyncio.CancelledError
        return SimpleNamespace(relation_oids=(99,))

    native = SimpleNamespace(
        db=runtime.database,
        make_class=lambda *args: object(),
        DoctorClinicianAddress=object(),
        _lock_cms_doctors_publication=AsyncMock(),
        _apply_locked_cms_doctors_stage=_apply,
    )
    monkeypatch.setattr(preparation, "_native", lambda: native)
    monkeypatch.setattr(preparation, "source_owner_transaction", runtime.owner)
    monkeypatch.setattr(preparation, "_models", lambda: (object(),))
    for name in ("_assert_stage", "_assert_stage_indexes", "assert_prepared_cms_doctors_seal"):
        monkeypatch.setattr(preparation, name, AsyncMock())
    monkeypatch.setattr(preparation.archive, "_verify_incumbent", AsyncMock())
    monkeypatch.setattr(
        preparation.generation,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=prepared.incumbent_authority),
    )
    with pytest.raises(asyncio.CancelledError if cancelled else RuntimeError):
        await preparation.apply_prepared_cms_doctors_generation(prepared)
    assert prepared.native_receipt is None
    assert runtime.events == [("savepoint", None), ("published identities and family", None), ("rollback", None)]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [asyncio.CancelledError(), RuntimeError("source cleanup refused")])
async def test_retained_source_cleanup_failure_still_cleans_private_maps(runtime, monkeypatch, failure):
    from process import cms_doctors_preparation as preparation

    runtime.database._transaction_binding = lambda: None
    monkeypatch.setattr(preparation, "_native", lambda: SimpleNamespace(db=runtime.database))
    monkeypatch.setattr(preparation, "_shielded_cleanup", AsyncMock(side_effect=failure))
    discard = AsyncMock()
    monkeypatch.setattr(preparation, "discard_doctors_identities", discard)
    prepared = SimpleNamespace(committed=False, schema="mrf", stage_oids=(), import_date="identitytest", context={})
    with pytest.raises(type(failure)):
        await preparation.cleanup_prepared_cms_doctors(prepared)
    discard.assert_awaited_once_with(
        runtime.database, {"import_date": prepared.import_date, "context": prepared.context}
    )
