from unittest.mock import AsyncMock

import pytest

from db.models import ImportLog, PTGFile
from process.ptg_parts import table_setup


@pytest.mark.asyncio
async def test_strict_ptg_control_tables_are_shared_and_never_dropped(monkeypatch):
    status = AsyncMock()
    create_table = AsyncMock()
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(table_setup.db, "status", status)
    monkeypatch.setattr(table_setup.db, "create_table", create_table)
    monkeypatch.setattr(table_setup, "_ensure_indexes", ensure_indexes)

    classes = await table_setup._prepare_ptg_tables(
        "run_specific_suffix",
        False,
        initial_table_class_names=set(table_setup.PTG_CONTROL_TABLE_CLASS_NAMES),
    )

    assert classes["PTGFile"] is PTGFile
    assert classes["ImportLog"] is ImportLog
    assert all("DROP TABLE" not in str(call.args[0]) for call in status.await_args_list)
    assert {call.args[0].name for call in create_table.await_args_list} == {
        PTGFile.__tablename__,
        ImportLog.__tablename__,
    }


@pytest.mark.asyncio
async def test_retired_dense_model_would_remain_run_scoped(monkeypatch):
    monkeypatch.setattr(table_setup.db, "status", AsyncMock())
    monkeypatch.setattr(table_setup.db, "create_table", AsyncMock())
    monkeypatch.setattr(table_setup, "_ensure_indexes", AsyncMock())

    classes = await table_setup._prepare_ptg_tables(
        "synthetic_run",
        False,
        initial_table_class_names=set(),
    )

    assert classes["PTGFile"] is PTGFile
    assert classes["ImportLog"] is ImportLog
    assert classes["PTGProviderGroup"].__tablename__.endswith("_synthetic_run")
