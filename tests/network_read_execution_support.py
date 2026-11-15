# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured transaction and native query interfaces for semantic unit doubles."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from process.network_compact_layout import CompactAddressRelation


def provider_snapshot_setup_result(statement, parameters, available_relations):
    """Model finite named-lock/catalog setup while leaving application queries to their own doubles."""
    import re

    import asyncpg
    from sqlalchemy.exc import DBAPIError

    from api.provider_profile_snapshot import _SNAPSHOT_RELATIONS_SQL

    sql = str(statement)
    if sql in {"SET LOCAL lock_timeout = '250ms'", "SET LOCAL statement_timeout = '5s'"}:
        return SimpleNamespace(all=lambda: [])
    if sql.startswith("LOCK TABLE ") and sql.endswith(" IN ACCESS SHARE MODE"):
        names = re.findall(r'"([^\"]+)"\."([^\"]+)"', sql)
        assert names
        if any(f"{schema}.{name}" not in available_relations for schema, name in names):
            raise DBAPIError(sql, None, asyncpg.UndefinedTableError("synthetic optional relation is absent"))
        return SimpleNamespace(all=lambda: [])
    if sql == _SNAPSHOT_RELATIONS_SQL:
        rows = [
            (name, f"{parameters['schema']}.{name}" in available_relations, "r", True, False)
            for name in parameters["names"]
        ]
        return SimpleNamespace(all=lambda: rows)
    return NotImplemented


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
