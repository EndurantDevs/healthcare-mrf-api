# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

adoption = importlib.import_module("process.entity_address_snapshot_adoption")
cutover_contract = importlib.import_module("process.entity_address_cutover_contract")
native = importlib.import_module("process.entity_address_unified")


@pytest.mark.asyncio
async def test_prepare_uses_exact_main_and_support_stage_set_without_worker_shutdown(
    monkeypatch,
):
    ensured_tables: list[str] = []
    stage_cls = SimpleNamespace(__tablename__="entity_address_unified_20260913")
    support_stage_class_map = {
        model: SimpleNamespace(__tablename__=f"{model.__tablename__}_20260913") for model in native.SUPPORT_TABLE_MODELS
    }
    swaps = [
        SimpleNamespace(stage_cls=stage_cls),
        *(SimpleNamespace(stage_cls=support_stage_cls) for support_stage_cls in support_stage_class_map.values()),
    ]
    plan = (swaps, [], ["relations"], ["required"])
    monkeypatch.setattr(
        adoption,
        "_prepared_full_result_stage",
        lambda **_kwargs: (
            stage_cls,
            support_stage_class_map,
            *plan,
        ),
    )

    async def ensure_logged(_schema, table_name):
        ensured_tables.append(table_name)

    validate = AsyncMock(return_value={"bridge_orphans": {}})
    run_phase = AsyncMock()
    monkeypatch.setattr(native, "_ensure_promoted_stage_logged", ensure_logged)
    monkeypatch.setattr(native, "_address_alias_generation", AsyncMock(return_value=9))
    monkeypatch.setattr(native, "_run_sql_phase", run_phase)
    monkeypatch.setattr(native, "_validate_publish_integrity", validate)

    prepared = await adoption.prepare_completed_entity_address_snapshot_adoption(
        db_schema=" mrf ",
        import_date=" 20260913 ",
    )

    assert len(native.SUPPORT_TABLE_MODELS) == 6
    assert ensured_tables == [swap.stage_cls.__tablename__ for swap in swaps]
    assert prepared.db_schema == "mrf"
    assert prepared.context == {"address_alias_generation": 9, "stage_persistence": "p"}
    assert prepared.publish_validation == {"bridge_orphans": {}}
    run_phase.assert_awaited_once_with(
        "ANALYZE mrf.entity_address_unified_20260913;",
        context=prepared.context,
        phase="entity-address snapshot analyzing restored main table",
    )
    validate.assert_awaited_once_with(
        "mrf",
        "entity_address_unified_20260913",
        support_stage_class_map,
        test_mode=False,
    )


@pytest.mark.asyncio
async def test_bound_sql_phase_preserves_settings_without_opening_an_engine_connection(monkeypatch):
    events: list[str] = []

    @asynccontextmanager
    async def tuned_transaction(database, settings, _quote_literal, _logger):
        assert database is bound_database
        assert settings == native._entity_address_sql_settings()
        events.append("scope entered")
        await apply_settings()
        yield
        events.append("scope exited")

    async def apply_settings(*_args):
        events.append("settings applied")

    async def status(_statement):
        events.append("statement run")
        return 4

    bound_database = SimpleNamespace(
        _transaction_binding=Mock(return_value=object()),
        acquire=Mock(),
        status=AsyncMock(side_effect=status),
    )
    monkeypatch.setattr(native, "db", bound_database)
    monkeypatch.setattr(native, "entity_address_tuned_transaction", tuned_transaction)

    rowcount = await native._status_with_entity_address_tuning("ANALYZE synthetic.stage")

    assert rowcount == 4
    assert events == [
        "scope entered",
        "settings applied",
        "statement run",
        "scope exited",
    ]
    bound_database.status.assert_awaited_once_with("ANALYZE synthetic.stage")
    bound_database.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_restore_setting_failure_propagates_and_rolls_back_nested_operation():
    events: list[str] = []

    @asynccontextmanager
    async def transaction():
        events.append("transaction entered")
        try:
            yield
        except RuntimeError:
            events.append("transaction rolled back")
            raise

    async def fail_restore(_statement):
        raise RuntimeError("restore rejected")

    database = SimpleNamespace(
        scalar=AsyncMock(return_value="7s"),
        status=AsyncMock(side_effect=fail_restore),
        transaction=transaction,
    )

    with pytest.raises(RuntimeError, match="restore rejected"):
        async with cutover_contract.preserve_transaction_sql_settings(
            database,
            ["statement_timeout"],
            native._sql_literal,
        ):
            events.append("operation ran")

    assert events == [
        "transaction entered",
        "operation ran",
        "transaction rolled back",
    ]
    database.status.assert_awaited_once_with("SET LOCAL statement_timeout = '7s';")


@pytest.mark.asyncio
async def test_unbound_publish_validation_keeps_parallel_operations(monkeypatch):
    operation_counter_map = {"active": 0, "peak": 0}

    async def operation(value):
        operation_counter_map["active"] += 1
        operation_counter_map["peak"] = max(operation_counter_map["peak"], operation_counter_map["active"])
        await asyncio.sleep(0)
        operation_counter_map["active"] -= 1
        return value

    monkeypatch.setattr(native, "db", SimpleNamespace())

    values = await native.run_publish_validation_operations(
        native.db,
        lambda: operation("first"),
        lambda: operation("second"),
    )

    assert values == ["first", "second"]
    assert operation_counter_map["peak"] == 2


@pytest.mark.parametrize(
    ("db_schema", "import_date"),
    [
        ("mrf; DROP SCHEMA mrf", "20260913"),
        ("mrf", "run-20260913"),
        ("mrf", "x" * 64),
        ("x" * 64, "20260913"),
        ("mrf", None),
    ],
)
def test_prepared_result_stage_rejects_unsafe_destination_identifiers_before_db_work(
    monkeypatch,
    db_schema,
    import_date,
):
    make_class = Mock()
    cutover_plan = Mock()
    database = Mock()
    monkeypatch.setattr(native, "make_class", make_class)
    monkeypatch.setattr(native, "_entity_address_cutover_plan", cutover_plan)
    monkeypatch.setattr(native, "db", database)

    with pytest.raises(ValueError):
        adoption._prepared_full_result_stage(
            db_schema=db_schema,
            import_date=import_date,
        )

    make_class.assert_not_called()
    cutover_plan.assert_not_called()
    assert database.mock_calls == []


def test_prepared_result_has_exact_safe_stage_tables(monkeypatch):
    stage_name_by_model: dict[type, str] = {}
    plan_call_map: dict[str, object] = {}

    def make_stage(model, suffix):
        table_name = f"{model.__tablename__}_{suffix}"
        stage_name_by_model[model] = table_name
        return SimpleNamespace(__tablename__=table_name)

    def cutover_plan(schema, stage_cls, support_stage_class_map, **_kwargs):
        plan_call_map.update(
            schema=schema,
            stage_cls=stage_cls,
            support_stage_class_map=support_stage_class_map,
        )
        return [], [], [], []

    monkeypatch.setattr(native, "make_class", make_stage)
    monkeypatch.setattr(native, "_entity_address_cutover_plan", cutover_plan)

    stage_cls, support_stage_class_map, *_ = adoption._prepared_full_result_stage(
        db_schema=" mrf ",
        import_date=" 20260913 ",
    )

    expected_names = {
        f"{model.__tablename__}_20260913" for model in (native.EntityAddressUnified, *native.SUPPORT_TABLE_MODELS)
    }
    actual_names = {stage_cls.__tablename__, *(stage.__tablename__ for stage in support_stage_class_map.values())}
    assert len(native.SUPPORT_TABLE_MODELS) == 6
    assert actual_names == expected_names
    assert len(actual_names) == 7
    assert set(stage_name_by_model.values()) == expected_names
    assert plan_call_map["schema"] == "mrf"


@pytest.mark.asyncio
async def test_adopt_delegates_only_named_local_fence_and_receipt_callbacks(monkeypatch):
    cutover_events: list[str] = []
    callback_events: list[str] = []

    async def verify_local_state():
        callback_events.append("verified")

    async def record_adoption():
        callback_events.append("receipted")

    async def run_cutover(*args, **kwargs):
        cutover_events.append("cutover")
        assert args[:6] == ("mrf", [], [], [], [], {"address_alias_generation": 4})
        assert kwargs["require_caller_owned_transaction"] is True
        callbacks = kwargs["callbacks"]
        await callbacks.before_cutover()
        await callbacks.after_publish()

    monkeypatch.setattr(native, "_run_entity_address_cutover", run_cutover)
    prepared = adoption.PreparedEntityAddressSnapshotAdoption(
        db_schema="mrf",
        stage_cls=object,
        support_stage_class_map={},
        swaps=[],
        patch_statements=[],
        relation_names=[],
        required_names=[],
        context={"address_alias_generation": 4},
        publish_validation={"address_alias_generation": 4},
    )

    publish_validation = await adoption.adopt_prepared_entity_address_snapshot(
        prepared,
        callbacks=adoption.EntityAddressSnapshotAdoptionCallbacks(
            verify_local_state=verify_local_state,
            record_adoption=record_adoption,
        ),
    )

    assert cutover_events == ["cutover"]
    assert callback_events == ["verified", "receipted"]
    assert publish_validation == {"address_alias_generation": 4}


@pytest.mark.asyncio
async def test_native_cutover_rejects_adoption_without_a_bound_caller_transaction(
    monkeypatch,
):
    monkeypatch.setattr(native, "db", SimpleNamespace())

    with pytest.raises(RuntimeError, match="caller-owned database transaction"):
        await native._run_entity_address_cutover(
            "mrf",
            [],
            [],
            [],
            [],
            {},
            require_caller_owned_transaction=True,
        )
