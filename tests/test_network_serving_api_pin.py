# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader ownership and recovery around the API catalog read snapshot."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from api import network_read_execution as execution
from api import network_serving_api_read as pins
from db import connection as database
from process import network_approved_membership_source as approved_sources
from process import registry_network_catalog_read as catalogs
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import _BOUND_NAMES_SQL, NetworkReadIdentityError
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest
from process.registry_network_catalog_read import _PINNED_READ
from tests.test_network_serving_read_context import _bound_reader, _Session

_SCHEMA = "registry_control"


class _PinSession(_Session):
    def __init__(self):
        super().__init__(active=True, modes=("repeatable read", "on"))
        self.after_guard = None
        self.events = []
        self.guard_valid = True
        self.driver.fetchrow = AsyncMock(side_effect=self._fetchrow)

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

    async def _fetchrow(self, sql, *arguments):
        assert sql == _BOUND_NAMES_SQL
        names, oids, pid = arguments
        assert names == [f'"{_SCHEMA}"."registry_approved_record"']
        assert oids == [82] and pid == 42
        self.events.append("guard")
        if self.after_guard is not None:
            self.after_guard()
        return {"backend_matches": True, "relation_count": 1, "valid": self.guard_valid}


@pytest.fixture
def pin_reader(monkeypatch):
    session = _PinSession()
    monkeypatch.setattr(pins, "read_published_api_pin", AsyncMock(return_value=None))
    manifest = PinnedNetworkServingManifest(
        3, "10000000-0000-0000-0000-000000000001", "candidate", 1, {}, 1, "a" * 64, 80
    )
    approved = object()
    relation = CompactAddressRelation(_SCHEMA, "registry_approved_record", 82)
    result = manifest, approved, relation
    legacy = AsyncMock(return_value=manifest)
    approved_pin = AsyncMock(return_value=(approved, relation))
    semantic_reads = Mock()
    semantic_reads.attach_mock(legacy, "catalog")
    semantic_reads.attach_mock(approved_pin, "approved")
    monkeypatch.setattr(catalogs, "catalog_pin", legacy)
    monkeypatch.setattr(approved_sources, "_pin_verified_api_approved_source", approved_pin)
    return SimpleNamespace(
        session=session,
        result=result,
        legacy=legacy,
        approved_pin=approved_pin,
        semantic_reads=semantic_reads,
    )


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
        if outcome == "invalid_names":
            session.guard_valid = False
        if outcome == "reader_drift":
            session.after_guard = lambda: session.info.update(
                api_reader_identity=dict(session.info["api_reader_identity"])
            )


@pytest.mark.parametrize("outcome", ["success", "body_error", "cancel", "invalid_names", "reader_drift"])
async def test_catalog_snapshot_restores_nested_contexts(pin_reader, outcome):
    session = pin_reader.session
    failure = asyncio.CancelledError() if outcome == "cancel" else RuntimeError("Synthetic body failure")
    expected = {
        "success": nullcontext(),
        "body_error": pytest.raises(RuntimeError),
        "cancel": pytest.raises(asyncio.CancelledError),
        "invalid_names": pytest.raises(NetworkServingReadUnavailable),
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
            if outcome == "invalid_names":
                assert isinstance(caught.value.__cause__, NetworkReadIdentityError)
            assert _PINNED_READ.get() is previous
            assert execution._NAME_INVENTORIES.get() is inventories
    finally:
        _PINNED_READ.reset(token)
    assert session.events.count("guard") == int(outcome not in {"body_error", "cancel"})
    assert session.transaction is original and session.nested is None
    pin_reader.legacy.assert_awaited_once_with(session.driver, 3, _SCHEMA, api_read=True)
    pin_reader.approved_pin.assert_awaited_once_with(session.driver, pin_reader.result[0], control_schema=_SCHEMA)


@pytest.mark.parametrize(
    "failure",
    [
        _missing_table(_SCHEMA, "network_serving_manifest"),
        _missing_table(_SCHEMA, "network_candidate_custody"),
        _missing_table("another_control", "network_candidate_custody"),
        asyncpg.InsufficientPrivilegeError("Synthetic denied relation"),
        asyncio.CancelledError(),
    ],
)
async def test_published_query_failure_never_uses_legacy_or_changes_context(pin_reader, failure):
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
    pin_reader.legacy.assert_not_awaited()
    pin_reader.approved_pin.assert_not_awaited()
    session.driver.fetchrow.assert_not_awaited()
    assert session.events == ["savepoint", "rollback-savepoint"]
    assert session.transaction is original and session.nested is None and session.driver.active
    assert (session.begins, session.commits, session.rollbacks) == (0, 0, 0)
