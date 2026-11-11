# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reader protocol doubles for orchestration, without native custody claims."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from sanic import Request
from sanic.compat import Header

from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import _CONSUMED_SQL


def provider_reader_double(legacy_documents):
    """Keep session/backend identity and bounded marker replies stable in route tests."""
    driver = SimpleNamespace(
        fetchval=AsyncMock(return_value=1), get_server_pid=lambda: 42, is_in_transaction=lambda: True
    )
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    transaction = object()
    session = SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        in_transaction=lambda: True,
        get_transaction=lambda: transaction,
        get_nested_transaction=lambda: None,
    )

    async def guarded_read(sql, *parameters):
        if sql == _CONSUMED_SQL:
            return {"backend_matches": True, "relation_count": len(parameters[0]), "valid": True}
        return {"complete": True, "network_ids": sorted(document["network_id"] for document in legacy_documents())}

    driver.fetchrow = AsyncMock(side_effect=guarded_read)
    return driver, session


def approved_record_relation(schema):
    """Return the fixed synthetic approved relation shared by route fixtures."""
    return CompactAddressRelation(schema, "registry_approved_record", 321)


def provider_test_request(path):
    """Build the same minimal request for managed collection and detail tests."""
    return Request(path, Header({}), "1.1", "POST", None, SimpleNamespace(config={}))
