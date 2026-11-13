# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader ownership and recovery around the API catalog read snapshot."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import network_serving_api_read as pins
from db import connection as database
from process.network_compact_layout import CompactAddressRelation
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest
from process.registry_network_catalog_read import _PINNED_READ
from tests.test_network_serving_read_context import _bound_reader, _Session

_SCHEMA = "registry_control"


class _PinSession(_Session):
    def __init__(self):
        super().__init__(active=True, modes=("repeatable read", "on"))
        self.events = []
        self.driver.fetchrow = AsyncMock()

    @asynccontextmanager
    async def begin_nested(self):
        """Model savepoint cleanup separately from the caller-owned outer transaction."""
        assert self.nested is None
        self.nested = object()
        self.events.append("savepoint")
        try:
            yield self.nested
        except BaseException:
            self.events.append("rollback-savepoint")
            raise
        else:
            self.events.append("release-savepoint")
        finally:
            self.nested = None


@pytest.fixture
def pin_reader(monkeypatch):
    session = _PinSession()
    manifest = PinnedNetworkServingManifest(
        3, "10000000-0000-0000-0000-000000000001", "candidate", 1, {}, 1, "a" * 64, 80
    )
    approved = object()
    relation = CompactAddressRelation(_SCHEMA, "registry_approved_record", 82)
    result = manifest, approved, relation
    monkeypatch.setattr(pins, "read_published_api_pin", AsyncMock(return_value=result))
    return SimpleNamespace(session=session, result=result)


@pytest.mark.parametrize("damage", ["unbound", "unverified", "foreign", "child_task", "inactive", "nested"])
async def test_unowned_reader_refuses_before_sql(pin_reader, damage):
    session = pin_reader.session

    async def enter(target):
        async with pins.api_catalog_read_snapshot(target, 3, _SCHEMA):
            raise AssertionError("an unowned reader entered the catalog snapshot")

    with _bound_reader(session):
        match damage:
            case "unbound":
                database._READER.set(None)
            case "unverified":
                session.info["api_reader_verified"] = False
            case "inactive":
                session.transaction = None
            case "nested":
                session.nested = object()
        with pytest.raises((NetworkServingReadUnavailable, ServiceUnavailable)):
            if damage == "child_task":
                await asyncio.create_task(enter(session))
            else:
                await enter(_PinSession() if damage == "foreign" else session)
    session.driver.fetchrow.assert_not_awaited()
    pins.read_published_api_pin.assert_not_awaited()


def _missing_table(schema=None, table=None, message=None):
    error = asyncpg.UndefinedTableError(message or "Synthetic missing relation")
    error.schema_name, error.table_name, error.message = schema, table, message
    return error


async def _read_catalog_body(pin_reader, outcome, failure):
    session = pin_reader.session
    inventories = execution._NAME_INVENTORIES.get()
    async with pins.api_catalog_read_snapshot(session, 3, _SCHEMA) as (manifest, relation):
        assert (manifest, relation) == (pin_reader.result[0], pin_reader.result[2])
        assert _PINNED_READ.get() == (session.driver, manifest, _SCHEMA)
        assert manifest._api_approved_source is pin_reader.result[1]
        assert len(execution._NAME_INVENTORIES.get()) == len(inventories) + 1
        if outcome in {"body_error", "cancel"}:
            raise failure
        if outcome == "reader_drift":
            session.info.update(api_reader_identity=dict(session.info["api_reader_identity"]))


@pytest.mark.parametrize("outcome", ["success", "body_error", "cancel", "reader_drift"])
async def test_catalog_snapshot_restores_nested_contexts(pin_reader, outcome):
    session = pin_reader.session
    failure = asyncio.CancelledError() if outcome == "cancel" else RuntimeError("Synthetic body failure")
    expected = {
        "success": nullcontext(),
        "body_error": pytest.raises(RuntimeError),
        "cancel": pytest.raises(asyncio.CancelledError),
        "reader_drift": pytest.raises(NetworkServingReadUnavailable),
    }[outcome]
    previous = (object(), object(), "outer_control")
    token = _PINNED_READ.set(previous)
    original = session.transaction
    try:
        with (
            _bound_reader(session),
            execution.network_read_name_inventory(
                session.driver, (CompactAddressRelation("outer_control", "outer_table", 90),)
            ),
        ):
            inventories = execution._NAME_INVENTORIES.get()
            with expected as caught:
                await _read_catalog_body(pin_reader, outcome, failure)
            if outcome in {"body_error", "cancel"}:
                assert caught.value is failure
            assert _PINNED_READ.get() is previous
            assert execution._NAME_INVENTORIES.get() is inventories
    finally:
        _PINNED_READ.reset(token)
    assert session.transaction is original and session.nested is None
    pins.read_published_api_pin.assert_awaited_once_with(session.driver, 3, _SCHEMA)
    session.driver.fetchrow.assert_not_awaited()


@pytest.mark.parametrize(
    "failure",
    [
        _missing_table(_SCHEMA, "network_serving_manifest"),
        asyncpg.InsufficientPrivilegeError("Synthetic denied relation"),
        asyncio.CancelledError(),
    ],
)
async def test_published_query_failure_never_changes_context(pin_reader, failure):
    """Unrelated query failures leave the caller's Reader and outer scopes intact."""
    session = pin_reader.session
    original = session.transaction
    pins.read_published_api_pin.side_effect = failure
    previous = _PINNED_READ.get()
    inventories = execution._NAME_INVENTORIES.get()
    with _bound_reader(session) as binding:
        with pytest.raises(type(failure)) as caught:
            await _read_catalog_body(pin_reader, "success", None)
        assert caught.value is failure
        assert _PINNED_READ.get() is previous and execution._NAME_INVENTORIES.get() is inventories
        assert database._READER.get() is binding and database.current_session() is session
    pins.read_published_api_pin.assert_awaited_once_with(session.driver, 3, _SCHEMA)
    session.driver.fetchrow.assert_not_awaited()
    assert session.events == ["savepoint", "rollback-savepoint"]
    assert session.transaction is original and session.nested is None and session.driver.active
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)
