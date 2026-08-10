# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import importlib
import os

from asyncpg.exceptions import DeadlockDetectedError
import pytest
from sqlalchemy import BigInteger

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

process_pkg = importlib.import_module("process")
process_initial = importlib.import_module("process.initial")
process_npi = importlib.import_module("process.npi")
utils_module = importlib.import_module("process.ext.utils")

@pytest.mark.asyncio
async def test_push_objects_rewrite_respects_parameter_limit(monkeypatch):
    status_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            return self

        async def status(self):
            status_calls.append(len(self.chunk))

    def _fake_insert(_table):
        return _FakeStmt()

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="fake_upsert_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    import_rows = []
    for idx in range(8):
        import_row_by_field = {"id": idx, "value": f"v{idx}"}
        for extra in range(38):
            import_row_by_field[f"c{extra}"] = extra
        import_rows.append(import_row_by_field)

    monkeypatch.setattr(utils_module.db, "insert", _fake_insert)
    monkeypatch.setenv("HLTHPRT_MAX_INSERT_PARAMETERS", "120")
    monkeypatch.setenv("HLTHPRT_DRIVER_PARAM_LIMIT", "32767")

    await utils_module.push_objects(import_rows, fake_cls, rewrite=True, use_copy=False)

    assert status_calls == [3, 3, 2]


@pytest.mark.asyncio
async def test_push_objects_rewrite_prefers_copy_first(monkeypatch):
    copy_calls = []

    class _FakeDriver:
        async def copy_records_to_table(self, table_name, schema_name=None, columns=None, records=None):
            source_rows = []
            async for row in records:
                source_rows.append(row)
            copy_calls.append(
                {
                    "table_name": table_name,
                    "schema_name": schema_name,
                    "columns": list(columns or []),
                    "row_count": len(source_rows),
                }
            )

    class _FakeRaw:
        def __init__(self):
            self.driver_connection = _FakeDriver()

    class _FakeConn:
        def __init__(self):
            self.raw_connection = _FakeRaw()

    class _AcquireCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def _fail_insert(*_args, **_kwargs):
        raise AssertionError("insert fallback should not run when COPY succeeds")

    fake_table = SimpleNamespace(schema="mrf", c=[])
    fake_cls = SimpleNamespace(
        __tablename__="copy_first_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )
    source_rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]

    monkeypatch.setattr(utils_module.db, "acquire", lambda: _AcquireCtx())
    monkeypatch.setattr(utils_module.db, "insert", _fail_insert)

    await utils_module.push_objects(source_rows, fake_cls, rewrite=True)

    assert len(copy_calls) == 1
    assert copy_calls[0]["table_name"] == "copy_first_table"
    assert copy_calls[0]["schema_name"] == "mrf"
    assert copy_calls[0]["columns"] == ["id", "value"]
    assert copy_calls[0]["row_count"] == 2


@pytest.mark.asyncio
async def test_push_objects_falls_back_when_copy_rejects_json_payload(monkeypatch):
    status_calls = []

    class _FakeDriver:
        async def copy_records_to_table(self, *_args, **_kwargs):
            raise TypeError("descriptor 'encode' for 'str' objects doesn't apply to a 'dict' object")

    class _FakeRaw:
        def __init__(self):
            self.driver_connection = _FakeDriver()

    class _FakeConn:
        def __init__(self):
            self.raw_connection = _FakeRaw()

    class _AcquireCtx:
        async def __aenter__(self):
            return _FakeConn()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeStmt:
        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_nothing(self, index_elements=None):
            self.index_elements = index_elements
            return self

        async def status(self):
            status_calls.append(self.chunk)

    fake_table = SimpleNamespace(schema="mrf", c=[])
    fake_cls = SimpleNamespace(
        __tablename__="json_payload_table",
        __table__=fake_table,
        __my_index_elements__=["id"],
    )
    source_rows = [
        {"id": 1, "payload": {"telemedicine": True}},
        {"id": 2, "payload": {"telemedicine": False}},
    ]

    monkeypatch.setattr(utils_module.db, "acquire", lambda: _AcquireCtx())
    monkeypatch.setattr(utils_module.db, "insert", lambda _table: _FakeStmt())

    await utils_module.push_objects(source_rows, fake_cls)

    assert status_calls == [source_rows]


@pytest.mark.asyncio
async def test_push_objects_retries_deadlock_during_fallback_insert(monkeypatch):
    status_attempts = []
    sleep_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            self.index_elements = index_elements
            self.set_dict = set_
            return self

        async def status(self):
            status_attempts.append(len(self.chunk))
            if len(status_attempts) == 1:
                raise DeadlockDetectedError("deadlock detected")

    async def _sleep(delay):
        sleep_calls.append(delay)

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(schema="mrf", c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="deadlock_retry_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    monkeypatch.setattr(utils_module.db, "insert", lambda _table: _FakeStmt())
    monkeypatch.setattr(utils_module.asyncio, "sleep", _sleep)
    monkeypatch.setenv("HLTHPRT_DB_DEADLOCK_RETRIES", "2")

    await utils_module.push_objects([{"id": 1, "value": "a"}], fake_cls, rewrite=True, use_copy=False)

    assert status_attempts == [1, 1]
    assert sleep_calls == [0.5]


@pytest.mark.asyncio
async def test_push_objects_retries_transient_closed_connection(monkeypatch):
    status_attempts = []
    sleep_calls = []

    class _FakeStmt:
        def __init__(self):
            self.chunk = None
            self.excluded = SimpleNamespace(value="excluded")

        def values(self, chunk):
            self.chunk = chunk
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            self.index_elements = index_elements
            self.set_dict = set_
            return self

        async def status(self):
            status_attempts.append(len(self.chunk))
            if len(status_attempts) == 1:
                raise utils_module.SQLAlchemyError(
                    "ConnectionDoesNotExistError: connection was closed in the middle of operation"
                )

    async def _sleep(delay):
        sleep_calls.append(delay)

    fake_columns = [
        SimpleNamespace(name="id", primary_key=True),
        SimpleNamespace(name="value", primary_key=False),
    ]
    fake_table = SimpleNamespace(schema="mrf", c=fake_columns)
    fake_cls = SimpleNamespace(
        __tablename__="transient_retry_table",
        __table__=fake_table,
        __my_initial_indexes__=[{"index_elements": ["id"]}],
    )

    monkeypatch.setattr(utils_module.db, "insert", lambda _table: _FakeStmt())
    monkeypatch.setattr(utils_module.asyncio, "sleep", _sleep)
    monkeypatch.setenv("HLTHPRT_DB_DEADLOCK_RETRIES", "2")

    await utils_module.push_objects(
        [{"id": 1, "value": "a"}], fake_cls, rewrite=True, use_copy=False
    )

    assert status_attempts == [1, 1]
    assert sleep_calls == [0.5]
