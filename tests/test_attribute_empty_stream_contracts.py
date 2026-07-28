# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed contracts for empty federal and state attribute source streams."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


attributes = importlib.import_module("process.attributes")


class _AsyncFile:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _EmptyRows:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


@pytest.mark.asyncio
async def test_empty_attribute_streams_skip_staging(monkeypatch):
    """Empty sources are accepted without publishing synthetic staged rows."""

    staged_rows = AsyncMock()
    monkeypatch.setattr(attributes, "_prepare_attribute_tables", AsyncMock())
    monkeypatch.setattr(attributes, "process_rating_areas", AsyncMock())
    monkeypatch.setattr(attributes, "get_import_schema", lambda *_args: "mrf")
    monkeypatch.setattr(attributes, "make_class", lambda *_args, **_kwargs: "stage")
    monkeypatch.setattr(attributes, "download_it_and_save", AsyncMock())
    monkeypatch.setattr(attributes, "_safe_unzip", AsyncMock())
    monkeypatch.setattr(attributes.glob, "glob", lambda _pattern: ["/virtual/source.csv"])
    monkeypatch.setattr(attributes, "async_open", lambda *_args, **_kwargs: _AsyncFile())
    monkeypatch.setattr(attributes, "AsyncDictReader", lambda *_args, **_kwargs: _EmptyRows())
    monkeypatch.setattr(attributes, "push_objects", staged_rows)

    worker_context_by_key = {"redis": SimpleNamespace(), "import_date": "run", "context": {}}
    source_task_by_key = {"url": "https://example.test/source.zip", "year": "2026"}
    await attributes.process_attributes(worker_context_by_key, source_task_by_key)
    await attributes.process_benefits(worker_context_by_key, source_task_by_key)
    await attributes.process_prices(worker_context_by_key, source_task_by_key)
    await attributes.process_state_attributes(worker_context_by_key, source_task_by_key)

    staged_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_attribute_batch_omits_optional_type_and_prepared_state_is_idempotent(monkeypatch):
    """Generic batches do not claim a typed payload and prepared tables are not recreated."""

    received_payload_list = []

    async def enqueue(_name, payload, **_kwargs):
        received_payload_list.append(dict(payload))

    record_list = [{"attr_name": "name"}]
    await attributes._enqueue_attribute_batch(
        SimpleNamespace(enqueue_job=enqueue),
        record_list,
        test_mode=True,
    )
    assert received_payload_list == [
        {"attr_obj_list": record_list, "context": {"test_mode": True}}
    ]
    assert record_list == []

    original_prepared = attributes._TABLE_STATE_BY_KEY["is_prepared"]
    try:
        attributes._TABLE_STATE_BY_KEY["is_prepared"] = True
        ensure_database = AsyncMock()
        monkeypatch.setattr(attributes, "ensure_database", ensure_database)
        await attributes._prepare_attribute_tables({"import_date": "run"})
        ensure_database.assert_not_awaited()
    finally:
        attributes._TABLE_STATE_BY_KEY["is_prepared"] = original_prepared
