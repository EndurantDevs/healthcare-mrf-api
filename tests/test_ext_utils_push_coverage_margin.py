# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from process.ext import utils
from tests.ext_utils_import_coverage_support import (
    _Acquire,
    _CopyDriver,
    _InsertStatement,
    _fake_table,
)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "copy_outcome",
    [
        ValueError("invalid copy payload"),
        SQLAlchemyError("driver unavailable"),
        TypeError("copy not supported"),
    ],
)
async def test_rewrite_copy_failures_use_bounded_insert_fallback(
    copy_outcome,
    monkeypatch,
):
    inserted_statements = []

    async def status(statement):
        inserted_statements.append(statement)

    driver = _CopyDriver(copy_outcome)
    table = _fake_table(include_value=False)
    fake_cls = SimpleNamespace(
        __tablename__="rewrite_fallback",
        __table__=table,
        __table_args__=({"schema": "tuple_schema"},),
        __my_index_elements__=["id"],
    )

    monkeypatch.setattr(utils.db, "acquire", lambda: _Acquire(driver))
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )
    monkeypatch.setenv("HLTHPRT_MAX_INSERT_PARAMETERS", "invalid")
    monkeypatch.setenv("HLTHPRT_DRIVER_PARAM_LIMIT", "invalid")

    await utils.push_objects(
        [{"id": 2}, {"id": 1}],
        fake_cls,
        rewrite=True,
    )

    assert len(inserted_statements) == 1
    assert inserted_statements[0].payload == [{"id": 1}, {"id": 2}]
    assert inserted_statements[0].conflict[0] == "nothing"


@pytest.mark.asyncio
async def test_rewrite_without_copy_method_updates_non_key_columns(monkeypatch):
    inserted_statements = []

    async def status(statement):
        inserted_statements.append(statement)

    fake_cls = SimpleNamespace(
        __tablename__="rewrite_update",
        __table__=_fake_table(),
        __table_args__={"schema": "dict_schema"},
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(
        utils.db,
        "acquire",
        lambda: _Acquire(SimpleNamespace()),
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )

    await utils.push_objects(
        [{"id": 1, "value": "new"}],
        fake_cls,
        rewrite=True,
    )

    assert inserted_statements[0].conflict == (
        "update",
        {
            "index_elements": ["id"],
            "set_": {"value": "excluded-value"},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("create_error", "attempt", "expected_error"),
    [
        (None, 0, None),
        (SQLAlchemyError("already exists"), 0, None),
        (SQLAlchemyError("permission denied"), 0, "permission denied"),
        (None, 5, "relation rewrite_retry does not exist"),
    ],
)
async def test_missing_table_retry_is_bounded_and_race_safe(
    create_error,
    attempt,
    expected_error,
    monkeypatch,
):
    original_push_objects = utils.push_objects
    recursive_call = AsyncMock(return_value="retried")
    create_table = AsyncMock(side_effect=create_error)
    monkeypatch.setattr(utils, "push_objects", recursive_call)
    monkeypatch.setattr(utils.db, "create_table", create_table)
    monkeypatch.setattr(utils.asyncio, "sleep", AsyncMock())

    driver = _CopyDriver(
        SQLAlchemyError("relation rewrite_retry does not exist")
    )
    fake_cls = SimpleNamespace(
        __tablename__="rewrite_retry",
        __table__=_fake_table(),
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(utils.db, "acquire", lambda: _Acquire(driver))

    invocation = original_push_objects(
        [{"id": 1, "value": "value"}],
        fake_cls,
        rewrite=True,
        _missing_table_attempt=attempt,
    )
    if expected_error:
        with pytest.raises(Exception, match=expected_error):
            await invocation
        recursive_call.assert_not_awaited()
    else:
        assert await invocation == "retried"
        recursive_call.assert_awaited_once()


@pytest.mark.asyncio
async def test_plain_copy_fallback_retries_rows_individually(monkeypatch):
    calls = []

    async def status(statement):
        calls.append(statement.payload)
        if isinstance(statement.payload, list):
            raise SQLAlchemyError("batch constraint failure")
        if statement.payload["id"] == 2:
            raise SQLAlchemyError("row constraint failure")

    driver = _CopyDriver(TypeError("x" * 300))
    fake_cls = SimpleNamespace(
        __tablename__="plain_fallback",
        __table__=_fake_table(schema="mrf"),
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(utils.db, "acquire", lambda: _Acquire(driver))
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )

    await utils.push_objects(
        [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        fake_cls,
    )

    assert calls == [
        [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
        {"id": 1, "value": "one"},
        {"id": 2, "value": "two"},
    ]


@pytest.mark.asyncio
async def test_retry_classifier_handles_nested_driver_errors(monkeypatch):
    class ConnectionDoesNotExistError(SQLAlchemyError):
        pass

    attempts = []

    async def status(statement):
        attempts.append(statement.payload)
        if len(attempts) == 1:
            outer = SQLAlchemyError("wrapper")
            outer.__cause__ = ConnectionDoesNotExistError("driver closed")
            raise outer

    fake_cls = SimpleNamespace(
        __tablename__="nested_retry",
        __table__=_fake_table(),
        __my_index_elements__=["id"],
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )
    monkeypatch.setattr(utils.asyncio, "sleep", AsyncMock())
    monkeypatch.setenv("HLTHPRT_DB_DEADLOCK_RETRIES", "invalid")

    await utils.push_objects(
        [{"id": 1, "value": "value"}],
        fake_cls,
        rewrite=True,
        use_copy=False,
    )

    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_rewrite_without_conflict_target_propagates_status_error(monkeypatch):
    successful_statements = []

    async def success(statement):
        successful_statements.append(statement)

    fake_cls = SimpleNamespace(
        __tablename__="no_conflict_target",
        __table__=_fake_table(),
    )
    monkeypatch.setattr(
        utils,
        "inspect",
        lambda _table: SimpleNamespace(primary_key=[]),
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(success),
    )
    await utils.push_objects(
        [{"id": 1, "value": "value"}],
        fake_cls,
        rewrite=True,
        use_copy=False,
    )
    assert successful_statements[0].conflict is None

    async def fail(_statement):
        raise SQLAlchemyError("write refused")

    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(fail),
    )
    with pytest.raises(SQLAlchemyError, match="write refused"):
        await utils.push_objects(
            [{"id": 1, "value": "value"}],
            fake_cls,
            rewrite=True,
            use_copy=False,
        )


@pytest.mark.asyncio
async def test_plain_copy_missing_method_uses_insert(monkeypatch):
    inserted_statements = []

    async def status(statement):
        inserted_statements.append(statement)

    fake_cls = SimpleNamespace(
        __tablename__="missing_copy_method",
        __table__=_fake_table(),
    )
    monkeypatch.setattr(
        utils.db,
        "acquire",
        lambda: _Acquire(SimpleNamespace()),
    )
    monkeypatch.setattr(
        utils.db,
        "insert",
        lambda _table: _InsertStatement(status),
    )

    await utils.push_objects(
        [{"id": 1}, {"id": 2}],
        fake_cls,
    )
    assert len(inserted_statements) == 1


@pytest.mark.asyncio
async def test_slow_insert_empty_and_unindexed_rows(monkeypatch):
    assert await utils.push_objects_slow([], object()) is None

    attempts = []

    class Statement:
        def values(self, payload):
            self.payload = payload
            return self

        async def status(self):
            attempts.append(self.payload)
            if isinstance(self.payload, list):
                raise SQLAlchemyError("batch refused")

    fake_cls = SimpleNamespace(__table__=object())
    monkeypatch.setattr(utils.db, "insert", lambda _target: Statement())
    await utils.push_objects_slow([{"id": 1}], fake_cls)
    assert attempts == [[{"id": 1}], {"id": 1}]
