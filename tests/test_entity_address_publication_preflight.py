# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication prerequisites fail before staging and remain checked at cutover."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_native_publication, tiger_held_inputs
from tests.test_entity_address_unified_lifecycle_contracts import (
    _configure_materialization_harness,
    entity_address,
)


@pytest.mark.asyncio
async def test_missing_inherited_custody_fails_before_stage_mutation(monkeypatch):
    sql_events, *_ = _configure_materialization_harness(monkeypatch)
    monkeypatch.setattr(entity_address, "_assert_cutover_has_no_dependent_views", AsyncMock())
    monkeypatch.setattr(tiger_held_inputs, "selected_captured_inventories", AsyncMock(return_value={}))
    monkeypatch.setattr(tiger_held_inputs, "selected_tiger_inventory", AsyncMock(return_value=None))
    session = SimpleNamespace(scalar=AsyncMock(return_value=True))

    @asynccontextmanager
    async def transaction():
        yield session

    session.begin = transaction
    monkeypatch.setattr(entity_address.db, "session_factory", transaction)
    import_context_by_key = {"import_date": "20260724", "context": {}}

    with pytest.raises(RuntimeError, match="requires protected captured inputs.*capture and select"):
        await entity_address.process_data(import_context_by_key, {"publish": True})

    assert not sql_events
    assert not import_context_by_key["context"].get("stage_prepared")
    assert not import_context_by_key["context"].get("cutover_dependency_preflight")
    assert "pg_inherits" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
async def test_publication_rechecks_custody_after_preflight_locks_are_released(monkeypatch):
    sql_events, *_ = _configure_materialization_harness(monkeypatch)
    monkeypatch.setattr(entity_address, "_assert_cutover_has_no_dependent_views", AsyncMock())
    selections = []

    @asynccontextmanager
    async def selected(_database, _schema, **_options):
        selections.append("selected")
        if len(selections) > 1:
            raise RuntimeError("synthetic custody changed after preflight")
        sql_events.append("CUSTODY_LOCKED")
        yield None
        sql_events.append("CUSTODY_RELEASED")

    monkeypatch.setattr(entity_address, "selected_publication_dependencies", selected)
    import_context_by_key = {"import_date": "20260724", "context": {}}
    await entity_address.process_data(import_context_by_key, {"publish": True})

    assert selections == ["selected"]
    assert sql_events.index("CUSTODY_RELEASED") < sql_events.index("CREATE_STAGE")
    assert import_context_by_key["context"]["cutover_dependency_preflight"] is True
    with pytest.raises(RuntimeError, match="custody changed after preflight"):
        await entity_address.publish_entity_address_unified_generation(import_context_by_key)
    assert selections == ["selected", "selected"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "publish,prepared,protected,test_mode",
    (
        (False, False, False, False),
        (True, True, False, False),
        (True, False, False, False),
        (True, False, True, True),
        (True, False, True, False),
    ),
)
async def test_preflight_preserves_prepared_inputs_and_controlled_selection_scope(
    monkeypatch, publish, prepared, protected, test_mode
):
    _configure_materialization_harness(monkeypatch)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROTECTED_PUBLICATION", str(protected).lower())
    monkeypatch.setattr(
        entity_address_native_publication, "bind_controlled_address_attempt", lambda _import_context_by_key: None
    )
    monkeypatch.setattr(entity_address, "_assert_cutover_has_no_dependent_views", AsyncMock())
    monkeypatch.setattr(
        entity_address.candidate_preparation,
        "geo_dependency_options",
        lambda: {"dependency_bindings": {}} if prepared else {},
    )
    monkeypatch.setattr(
        entity_address, "_ensure_schema_exists", AsyncMock(side_effect=RuntimeError("synthetic stage boundary"))
    )
    observed_options = []

    @asynccontextmanager
    async def selected(_database, _schema, **options):
        observed_options.append(options)
        yield None

    monkeypatch.setattr(entity_address, "selected_publication_dependencies", selected)
    import_context_by_key = {
        "import_date": "20260724",
        "control_run_id": "synthetic-run",
        "context": {"test_mode": test_mode},
    }
    with pytest.raises(RuntimeError, match="synthetic stage boundary"):
        await entity_address.process_data(import_context_by_key, {"publish": publish})

    expected = {"control_context": import_context_by_key} if protected and not test_mode else {}
    assert observed_options == ([expected] if publish and not prepared else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("handoff", (False, True))
async def test_lost_dependency_owner_blocks_native_publication_entry(monkeypatch, handoff):
    """Both publication paths check the original owner before writing serving authority."""
    from process import entity_address_snapshot_adoption as adoption

    fence = AsyncMock(side_effect=RuntimeError("synthetic dependency owner lost"))
    swap, capture, write = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(entity_address, "lock_selected_publication_dependencies", fence)
    monkeypatch.setattr(adoption, "_publish_result_swaps", swap)
    monkeypatch.setattr(entity_address_native_publication, "_capture_handoff", capture)
    monkeypatch.setattr(entity_address_native_publication, "_write_handoff", write)
    monkeypatch.setattr(entity_address_native_publication, "_complete_indexes", AsyncMock())

    @asynccontextmanager
    async def transaction(*_args):
        yield SimpleNamespace()

    monkeypatch.setattr(entity_address.serving_recovery, "address_transaction", transaction)
    monkeypatch.setattr(entity_address.db, "transaction", transaction)
    with pytest.raises(RuntimeError, match="synthetic dependency owner lost"):
        if handoff:
            await entity_address_native_publication.handoff_entity_address_generation(
                {
                    "control_run_id": "run_test",
                    "context": {
                        "_control_attempt_id": "run_test:" + "a" * 32,
                        "_control_attempt_started_at": "2026-01-01T00:00:00.000000+00:00",
                    },
                },
                schema_name="mrf",
                import_date="20260101",
                dependency_bindings={},
                row_count=1,
            )
        else:
            await entity_address._run_entity_address_cutover("mrf", [], [], [], [], {})
    fence.assert_awaited_once_with(entity_address.db, "mrf")
    swap.assert_not_awaited()
    capture.assert_not_awaited()
    write.assert_not_awaited()
