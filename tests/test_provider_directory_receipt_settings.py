# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Receipt normalization preserves its original transaction and presentation settings."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_snapshot_destination as destination
from process import entity_address_snapshot_receipt as snapshot
from process import provider_directory_serving_recovery_receipts as receipts


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError])
async def test_receipt_scope_restores_the_original_boundary(monkeypatch, failure):
    session, events = object(), []
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))

    @asynccontextmanager
    async def preserve(*, database):
        assert database._transaction_binding().session is session
        events.append("preserve")
        try:
            yield
        finally:
            events.append("restore")

    async def normalize(actual, schema):
        assert actual is session and schema == "unit"
        events.append("normalize")

    async def read():
        async with receipts.normalized_receipt_scope(database, session, "unit"):
            events.append("receipt")
            if failure:
                raise failure("interrupted receipt")

    monkeypatch.setattr(destination, "_preserve_receipt_settings", preserve)
    monkeypatch.setattr(snapshot, "_normalize_receipt_session", normalize)
    if failure:
        with pytest.raises(failure):
            await read()
    else:
        await read()
    assert events == ["preserve", "normalize", "receipt", "restore"]


@pytest.mark.asyncio
async def test_receipt_scope_refuses_a_different_session(monkeypatch):
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    normalize = AsyncMock()
    monkeypatch.setattr(snapshot, "_normalize_receipt_session", normalize)
    with pytest.raises(ValueError, match="receipt_transaction_changed"):
        async with receipts.normalized_receipt_scope(database, object(), "unit"):
            pytest.fail("a detached session entered receipt normalization")
    normalize.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_existing_receipt_preserver_keeps_exact_list_settings(monkeypatch, explicit):
    previous_settings_by_name = {name: "original " + name for name in destination._RECEIPT_SETTING_NAMES}
    previous_settings_by_name["search_path"] = '"one, schema", pg_catalog, pg_temp'
    restored, events = {}, []

    async def scalar(statement, **parameters):
        name = parameters["setting_name"]
        if "current_setting" in statement:
            return previous_settings_by_name[name]
        assert statement == "SELECT pg_catalog.set_config(:setting_name, :setting_value, true)"
        restored[name] = parameters["setting_value"]

    @asynccontextmanager
    async def transaction():
        events.append("savepoint")
        yield
        events.append("release")

    database = SimpleNamespace(scalar=scalar, transaction=transaction)
    monkeypatch.setattr(destination, "db", object() if explicit else database)
    async with destination._preserve_receipt_settings(**({"database": database} if explicit else {})):
        events.append("read")
    assert events == ["savepoint", "read", "release"]
    assert restored == previous_settings_by_name
