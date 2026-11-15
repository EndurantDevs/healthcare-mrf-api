# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin a published network generation in the configured API read transaction."""

from contextlib import asynccontextmanager

from api.network_read_execution import _read_driver
from api.network_serving_read_context import _entry_snapshot
from db.registry_schema import registry_schema
from process.network_published_api_pin import read_published_api_pin
from process.network_serving_read import NetworkServingReadUnavailable


async def resolve_api_network_pin(session, *, generation_id=None, control_schema=None):
    """Resolve current or retained publication metadata once for this request."""
    active, _transaction, _reader = _entry_snapshot(session)
    if not active:
        raise NetworkServingReadUnavailable("Network serving reads require an active transaction")
    schema = registry_schema() if control_schema is None else control_schema
    driver = await _read_driver(session)
    return await read_published_api_pin(driver, generation_id, schema)


@asynccontextmanager
async def api_catalog_read_snapshot(session, generation_id=None, control_schema=None):
    """Keep one logical generation for all catalog and membership queries in a request."""
    from process.registry_network_catalog_read import _PINNED_READ

    schema = registry_schema() if control_schema is None else control_schema
    manifest, approved, relation = await resolve_api_network_pin(
        session, generation_id=generation_id, control_schema=schema
    )
    object.__setattr__(manifest, "_api_approved_source", approved)
    token = _PINNED_READ.set((manifest, schema))
    try:
        yield manifest, relation
    finally:
        _PINNED_READ.reset(token)
