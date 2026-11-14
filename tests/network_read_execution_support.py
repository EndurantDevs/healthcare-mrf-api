# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured transaction and native query interfaces for semantic unit doubles."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from process.network_compact_layout import CompactAddressRelation


class _QueryDriver:
    def __init__(self, session, delegate):
        self.session = session
        self.delegate = delegate

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def is_in_transaction(self):
        return self.session.in_transaction()

    async def fetchrow(self, sql, *parameters):
        return await self.delegate.fetchrow(sql, *parameters)


def with_read_session(session, *, driver=None):
    """Expose a configured transaction and optional native query delegate."""
    if not hasattr(session, "in_transaction"):
        session.in_transaction = lambda: True
    if not hasattr(session, "get_transaction"):
        transaction = object()
        session.get_transaction = lambda: transaction
    native = _QueryDriver(session, driver)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=native)))
    session.connection = AsyncMock(return_value=connection)
    return session


def mock_approved_control_metadata(monkeypatch, *, events=None):
    """Mock only the native metadata seam in semantic tests, leaving result guards active."""

    async def pin(_connection, _manifest, **_options):
        if events is not None:
            events.append("approved-pin")
        return None, CompactAddressRelation("registry_control", "registry_approved_record")

    monkeypatch.setattr("process.network_approved_membership_source.pin_api_retained_approved_membership_source", pin)
    monkeypatch.setattr("process.network_approved_membership_source._pin_verified_api_approved_source", pin)
