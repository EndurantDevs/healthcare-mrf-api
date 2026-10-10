# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep control updates on the exact database owned by the admission fixture."""

import asyncio
import importlib
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

support = importlib.import_module("tests.cms_npd_admission_postgres_support")
control = importlib.import_module("process.control_lifecycle")


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_kind", ["normal", "body_error", "cancellation", "disconnect_failure"])
async def test_admission_fixture_binds_control_database_and_restores_alias(monkeypatch, exit_kind):
    previous_control_database = control.db
    failure = asyncio.CancelledError() if exit_kind == "cancellation" else RuntimeError("synthetic failure")
    statement = object()
    update_result = SimpleNamespace(all=Mock(return_value=[("synthetic-run",)]))
    database = SimpleNamespace(
        _database_override="synthetic_fixture",
        _reader_binding=Mock(return_value=None),
        _transaction_binding=Mock(return_value=None),
        connect=AsyncMock(),
        disconnect=AsyncMock(side_effect=failure if exit_kind == "disconnect_failure" else None),
        execute=AsyncMock(return_value=update_result),
    )
    factory = Mock(return_value=database)

    @asynccontextmanager
    async def migrated_database_url(**kwargs):
        assert kwargs.keys() == {"migration_prefixes"}
        yield (
            SimpleNamespace(
                host="synthetic", port=1, username="synthetic", password=None, database="synthetic_fixture"
            ),
            True,
        )

    monkeypatch.setattr(support, "Database", factory)
    monkeypatch.setattr(support, "_admission_database_url", migrated_database_url)
    expected = nullcontext() if exit_kind == "normal" else pytest.raises(type(failure))
    with expected as caught:
        async with support.admission_database(monkeypatch) as yielded:
            assert control.db is yielded is database, "control alias must be the exact fixture database"
            assert support.fhir.db is database
            assert await control._execute_control_run_update(statement) == 1
            if exit_kind in {"body_error", "cancellation"}:
                raise failure
    if exit_kind != "normal":
        assert caught.value is failure
    assert control.db is previous_control_database
    factory.assert_called_once_with()
    database.execute.assert_awaited_once_with(statement)
    update_result.all.assert_called_once_with()
    database.disconnect.assert_awaited_once_with()
    assert database.connect.await_count == 2
