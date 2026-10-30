# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native receipt protocol for semantic unit doubles; real identity uses PostgreSQL tests."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import _BOUND_NAMES_SQL, _CONSUMED_SQL, _RELATION_LOCK_MODES


class _ReceiptDriver:
    def __init__(self, session, delegate):
        self.session = session
        self.delegate = delegate

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def get_server_pid(self):
        return 42

    def is_in_transaction(self):
        return self.session.in_transaction()

    async def fetchrow(self, sql, *parameters):
        if sql not in (_CONSUMED_SQL, _BOUND_NAMES_SQL):
            return await self.delegate.fetchrow(sql, *parameters)
        names, oids, pid, *modes = parameters
        assert names and len(names) == len(oids) and pid == 42
        assert len(set(names)) == len(names) and len(set(oids)) == len(oids)
        assert all(oid is None or type(oid) is int and oid > 0 for oid in oids)
        if sql == _CONSUMED_SQL:
            assert modes == [list(_RELATION_LOCK_MODES)] and None not in oids
        else:
            assert modes == []
        return {"backend_matches": True, "relation_count": len(names), "valid": True}


def with_read_identity_receipts(session, *, driver=None):
    """Keep queued semantic SQL results separate from the native guard's catalog receipt."""
    if not hasattr(session, "in_transaction"):
        session.in_transaction = lambda: True
    if not hasattr(session, "get_transaction"):
        transaction = object()
        session.get_transaction = lambda: transaction
    native = _ReceiptDriver(session, driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=native)))
    session.connection = AsyncMock(return_value=connection)
    return session


def mock_approved_control_metadata(monkeypatch, *, events=None):
    """Mock only the native metadata seam in semantic tests, leaving result guards active."""

    async def pin(_connection, _manifest, **_options):
        if events is not None:
            events.append("approved-pin")
        return None, CompactAddressRelation("registry_control", "registry_approved_record", 99)

    monkeypatch.setattr("process.network_approved_membership_source.pin_api_retained_approved_membership_source", pin)
