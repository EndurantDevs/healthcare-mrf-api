# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary scratch ownership survives failures without granting broader cleanup."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_ordinary_preparation as ordinary


@pytest.fixture
def ordinary_case(monkeypatch):
    @asynccontextmanager
    async def transaction():
        yield

    database = SimpleNamespace(
        transaction=Mock(side_effect=transaction),
        _transaction_binding=Mock(return_value=None),
        scalar=AsyncMock(),
        status=AsyncMock(),
    )
    actor_by_field = dict(session=11, current="writer", owner="owner", database_oid=42)
    session = object()
    monkeypatch.setattr(ordinary, "_native", lambda: SimpleNamespace(db=database))
    monkeypatch.setattr(ordinary.admitted, "_native", lambda: SimpleNamespace(db=database))
    monkeypatch.setattr(ordinary.receipts, "ordinary", AsyncMock(return_value=(session, object(), actor_by_field)))
    drain = AsyncMock()
    monkeypatch.setattr(candidate, "_drain_owned_stages", drain)
    active = ordinary._ACTIVE.set(None)
    admission = ordinary.admitted._ADMISSION.set(None)
    factory = ordinary.source._FACTORY.set(None)
    try:
        yield SimpleNamespace(database=database, session=session, drain=drain)
    finally:
        ordinary.source._FACTORY.reset(factory)
        ordinary.admitted._ADMISSION.reset(admission)
        ordinary._ACTIVE.reset(active)


@pytest.mark.asyncio
async def test_scope_cancels_and_settles_inherited_children_before_cleanup(ordinary_case):
    started, stopped = asyncio.Event(), asyncio.Event()

    async def child():
        assert ordinary.current() is owned
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async with ordinary.scope("sample", {"stage"}) as owned:
        task = asyncio.create_task(child())
        await started.wait()
        assert task in owned.children

        async def require_stopped(_schema, _identities):
            assert stopped.is_set() and task.done()

        ordinary_case.drain.side_effect = require_stopped
    assert task.cancelled() and stopped.is_set()
    assert not owned.active and ordinary.current() is None
    ordinary_case.drain.assert_awaited_once_with("sample", ())


@pytest.mark.asyncio
async def test_recreated_name_retains_the_original_oid_until_cleanup(ordinary_case):
    async with ordinary.scope("sample", {"stage"}) as owned:
        owned.owned_oids["stage"] = 101
        ordinary_case.database.scalar.side_effect = [None, 202]
        create = AsyncMock()
        assert await ordinary.create("sample", "stage", create) is owned
        create.assert_awaited_once_with(ordinary_case.session)
        assert owned.retired_oids == [("stage", "stage", 101)]
        assert owned.owned_oids == {"stage": 202}
        owned.keep((("stage", "stage", 202),))
    ordinary_case.drain.assert_awaited_once_with("sample", (("stage", "stage", 101),))


@pytest.mark.asyncio
async def test_missing_stage_is_not_locked_or_dropped_and_custody_is_retained(ordinary_case):
    async with ordinary.scope("sample", {"stage"}) as owned:
        owned.owned_oids["stage"] = 101
        ordinary_case.database.scalar.return_value = None
        assert await ordinary.drop("sample", "stage") is owned
        statements = [call.args[0] for call in ordinary_case.database.status.await_args_list]
        assert not any("LOCK TABLE" in sql or "DROP TABLE" in sql for sql in statements)
        assert owned.owned_oids == {"stage": 101} and not owned.pending_names
    ordinary_case.drain.assert_awaited_once_with("sample", (("stage", "stage", 101),))


@pytest.mark.asyncio
async def test_create_failure_clears_pending_names_and_preserves_the_error(ordinary_case):
    failure = RuntimeError("creation interrupted")
    with pytest.raises(RuntimeError) as raised:
        async with ordinary.scope("sample", {"stage"}) as owned:
            ordinary_case.database.scalar.return_value = None
            await ordinary.create("sample", "stage", AsyncMock(side_effect=failure))
    assert raised.value is failure and not owned.pending_names
    assert not owned.owned_oids and not owned.active and ordinary.current() is None
    ordinary_case.drain.assert_awaited_once_with("sample", ())


@pytest.mark.asyncio
async def test_operation_and_cleanup_failures_remain_visible_and_close_the_scope(ordinary_case):
    operation, cleanup = RuntimeError("operation failed"), RuntimeError("cleanup refused")
    ordinary_case.drain.side_effect = cleanup
    with pytest.raises(BaseExceptionGroup) as raised:
        async with ordinary.scope("sample", {"stage"}) as owned:
            raise operation
    assert raised.value.exceptions == (operation, cleanup)
    assert not owned.active and ordinary.current() is None


@pytest.mark.asyncio
async def test_conflicting_authority_is_refused_before_opening_a_transaction(ordinary_case):
    token = ordinary.source._FACTORY.set(object())
    try:
        with pytest.raises(RuntimeError, match="ordinary_scope_required"):
            async with ordinary.scope("sample", {"stage"}):
                pytest.fail("conflicting authority entered the scope")
    finally:
        ordinary.source._FACTORY.reset(token)
    ordinary_case.database.transaction.assert_not_called()
    ordinary_case.drain.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity,owned_oid", [((), 101), ((("stage", "stage", 999),), 101), ((("stage", "stage", True),), 1)]
)
async def test_keep_cannot_transfer_missing_foreign_or_boolean_oids(ordinary_case, identity, owned_oid):
    async with ordinary.scope("sample", {"stage"}) as owned:
        owned.owned_oids["stage"] = owned_oid
        with pytest.raises(RuntimeError, match="finalized_"):
            owned.keep(identity)
        assert not owned.kept_oids and owned.owned_oids == {"stage": owned_oid}
    ordinary_case.drain.assert_awaited_once_with("sample", (("stage", "stage", owned_oid),))


@pytest.mark.asyncio
@pytest.mark.parametrize("unregistered,borrowed", [(True, False), (False, True)])
async def test_scratch_entry_refuses_empty_or_borrowed_custody_before_opening_a_transaction(
    ordinary_case, monkeypatch, unregistered, borrowed
):
    admitted = ordinary.admitted
    capability = object()
    monkeypatch.setattr(
        admitted, "_row_scratch_state", lambda _: SimpleNamespace(owned_oids={} if unregistered else {"stage": 101})
    )
    ordinary_case.database._transaction_binding.return_value = object() if borrowed else None
    token = admitted._ROW_SCRATCH.set(None)
    try:
        with pytest.raises(RuntimeError, match="unregistered|replace transaction locks"):
            async with admitted.native_transaction(row_scratch=capability):
                pytest.fail("invalid scratch entry reached payload work")
    finally:
        admitted._ROW_SCRATCH.reset(token)
    ordinary_case.database.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("current_limit", [8192, 16384])
async def test_borrowed_transaction_must_already_carry_the_exact_signed_temp_limit(
    ordinary_case, monkeypatch, current_limit
):
    admitted = ordinary.admitted
    scope = SimpleNamespace(admission=SimpleNamespace(plan=SimpleNamespace(temp_file_limit_bytes_per_backend=16384)))
    apply = AsyncMock()
    monkeypatch.setattr(admitted, "apply_temp_file_limit", apply)
    ordinary_case.database.scalar.return_value = current_limit
    token = admitted._ADMISSION.set(scope)
    try:
        if current_limit == 16384:
            await admitted._apply_owned_temp_entry(ordinary_case.database, False)
        else:
            with pytest.raises(RuntimeError, match="bounded_owner_required"):
                await admitted._apply_owned_temp_entry(ordinary_case.database, False)
    finally:
        admitted._ADMISSION.reset(token)
    apply.assert_not_awaited()
    ordinary_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_status_cannot_replace_a_signed_temp_limit_with_a_smaller_value(ordinary_case, monkeypatch):
    admitted = ordinary.admitted
    scope = SimpleNamespace(admission=SimpleNamespace(plan=SimpleNamespace(temp_file_limit_bytes_per_backend=16384)))
    apply = AsyncMock()
    monkeypatch.setattr(admitted, "apply_temp_file_limit", apply)
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(ValueError, match="temp_limit_invalid"):
            await admitted._apply_status_setting(ordinary_case.database, object(), "temp_file_limit", "8kB")
    finally:
        admitted._ADMISSION.reset(token)
    apply.assert_not_awaited()
    ordinary_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("schema,name", [("other", "stage"), ("sample", "foreign")])
async def test_logging_cannot_borrow_another_schema_or_unowned_stage(ordinary_case, schema, name):
    admitted = ordinary.admitted
    callback = AsyncMock()
    scope = SimpleNamespace(
        db_schema="sample", stage_oids=(("live", "stage", 101),), admission=SimpleNamespace(before_logging=callback)
    )
    token = admitted._ADMISSION.set(scope)
    try:
        with pytest.raises(RuntimeError, match="not admitted"):
            await admitted.before_stage_logging(schema, name)
    finally:
        admitted._ADMISSION.reset(token)
    callback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("owned_name", ["stage", "compact"])
async def test_runner_scratch_cannot_be_renamed_by_the_shared_stage_swap(ordinary_case, owned_name):
    admitted = ordinary.admitted
    token = admitted._ADMISSION.set(SimpleNamespace(row_scratch_owners={owned_name: (101, object())}))
    try:
        with pytest.raises(RuntimeError, match="cannot be renamed"):
            await admitted._replace_owned_relation("sample", "stage", "compact")
    finally:
        admitted._ADMISSION.reset(token)
    ordinary_case.database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_preparation_cannot_enter_an_existing_cutover_transaction(ordinary_case, monkeypatch):
    pin = candidate.ProviderDirectoryAddressDatasetPin("source", "endpoint", "dataset", "a" * 64, "root")
    inputs = candidate.ProviderDirectoryAddressPreparationInput((pin,), "overlay_next", 101, 0)
    monkeypatch.setattr(candidate, "_native", lambda: SimpleNamespace(db=ordinary_case.database))
    ordinary_case.database._transaction_binding.return_value = object()
    with pytest.raises(RuntimeError, match="must precede the cutover transaction"):
        await candidate.prepare_provider_directory_entity_address({}, {}, preparation_input=inputs)
    ordinary_case.database.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("deferred", [False, True])
async def test_finalization_requires_present_and_completed_internal_preparation(ordinary_case, monkeypatch, deferred):
    monkeypatch.setattr(candidate, "current", lambda: object() if deferred else None)
    check = AsyncMock()
    monkeypatch.setattr(candidate, "assert_dataset_pins", check)
    with pytest.raises(RuntimeError, match="preparation is incomplete"):
        await candidate.prepare_finalized_generation(
            "sample", object(), {}, context={"publish_validation_deferred": deferred}
        )
    check.assert_not_awaited()
