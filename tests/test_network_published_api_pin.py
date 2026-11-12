# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Published-request reuse stays local to its original connection and scope."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_published_api_pin as published
from process.registry_network_catalog_read import _PINNED_READ


def test_supplied_manifest_flag_cannot_skip_request_audits():
    connection = SimpleNamespace(is_in_transaction=lambda: True)
    manifest = SimpleNamespace(_published_api_request=True)
    assert published.active_published_api_pin(connection, manifest) is None


@pytest.mark.parametrize("damage", [None, "connection", "manifest", "schema", "transaction", "replaced", "unpublished"])
def test_reuse_requires_exact_connection_manifest_and_active_scope(damage):
    connection = SimpleNamespace(is_in_transaction=lambda: damage != "transaction")
    transaction = object()
    session = SimpleNamespace(get_transaction=lambda: object() if damage == "replaced" else transaction)
    manifest = SimpleNamespace(
        _published_api_request=damage != "unpublished",
        _api_request_session=session,
        _api_request_transaction=transaction,
    )
    token = _PINNED_READ.set((connection, manifest, "example_control"))
    try:
        actual = object() if damage == "connection" else connection
        requested = object() if damage == "manifest" else manifest
        schema = "other_control" if damage == "schema" else "example_control"
        if damage in {"manifest", "schema", "transaction", "replaced"}:
            with pytest.raises(published.NetworkServingReadUnavailable):
                published.active_published_api_pin(actual, requested, control_schema=schema)
        else:
            resolved = published.active_published_api_pin(actual, requested, control_schema=schema)
            assert resolved is (manifest if damage is None else None)
    finally:
        _PINNED_READ.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("generation", [True, False, 0, -1, 1.0, "1", 2**63])
async def test_invalid_generation_never_runs_metadata_sql(generation):
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(published.NetworkServingReadUnavailable):
        await published.read_published_api_pin(connection, generation, "example_control")
    connection.fetchrow.assert_not_awaited()
