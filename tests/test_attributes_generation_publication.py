# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import attributes


class _TrackedTransaction:
    def __init__(self, state):
        self.state = state

    async def __aenter__(self):
        self.state["active"] = True

    async def __aexit__(self, *_args):
        self.state["active"] = False


@pytest.mark.asyncio
async def test_idle_attribute_shutdown_does_not_touch_database(monkeypatch):
    ensure = AsyncMock()
    monkeypatch.setattr(attributes, "ensure_database", ensure)

    await attributes.finalize_attribute_tables({"context": {"tables_prepared": False}})

    ensure.assert_not_awaited()


@pytest.mark.asyncio
async def test_partial_attribute_family_fails_before_cutover_or_generation(monkeypatch):
    monkeypatch.setattr(attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        attributes,
        "make_class",
        lambda model, *_args, **_kwargs: SimpleNamespace(__tablename__=model.__tablename__ + "_stage"),
    )
    monkeypatch.setattr(
        attributes,
        "_is_table_available",
        AsyncMock(side_effect=[True, False, True, True]),
    )
    writer = AsyncMock()
    monkeypatch.setattr(attributes, "publish_local_reference_family_generation", writer)

    with pytest.raises(RuntimeError, match="stages are missing"):
        await attributes.finalize_attribute_tables(
            {
                "import_date": "20260914",
                "context": {"tables_prepared": True, "test_mode": True},
            }
        )

    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_attribute_family_writes_generation_inside_cutover(monkeypatch):
    state_by_field = {"active": False}
    monkeypatch.setattr(attributes, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        attributes,
        "make_class",
        lambda model, *_args, **_kwargs: SimpleNamespace(__tablename__=model.__tablename__ + "_stage"),
    )
    monkeypatch.setattr(attributes, "_is_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(attributes, "_finalize_attribute_stage", AsyncMock())

    async def swap(*_args):
        assert state_by_field["active"] is True

    async def write(*_args, **_kwargs):
        assert state_by_field["active"] is True

    monkeypatch.setattr(attributes, "_swap_attribute_stage", swap)
    monkeypatch.setattr(attributes, "publish_local_reference_family_generation", write)
    monkeypatch.setattr(attributes.db, "transaction", lambda: _TrackedTransaction(state_by_field))
    monkeypatch.setattr(attributes, "print_time_info", lambda _start: None)

    await attributes.finalize_attribute_tables(
        {
            "import_date": "20260914",
            "context": {"tables_prepared": True, "test_mode": True, "start": None},
        }
    )

    assert state_by_field["active"] is False
