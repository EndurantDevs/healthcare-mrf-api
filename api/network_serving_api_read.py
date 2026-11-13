# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind protected API metadata to the original verified Reader transaction."""

from contextlib import asynccontextmanager

from api.network_read_execution import _original_read_driver, network_read_name_inventory
from api.network_serving_read_context import _entry_snapshot, _require_reader_owner
from api.provider_profile_snapshot import provider_read_savepoint
from db.registry_schema import registry_schema
from process.network_published_api_pin import read_published_api_pin
from process.network_serving_read import NetworkServingReadUnavailable


def _unavailable():
    return NetworkServingReadUnavailable("Protected API serving metadata is unavailable")


def _reader_entry(session):
    """Require the configured genuine Reader instead of accepting supplied identity JSON."""
    active, transaction, reader = _entry_snapshot(session)
    if active is not True or reader is None or session.info.get("api_reader_verified") is not True:
        raise _unavailable()
    return transaction, reader


async def _same_reader(session, transaction, reader, driver, backend_pid):
    """Keep metadata and buffered results on their original outer transaction and actor."""
    _require_reader_owner(session, reader)
    actual_driver, actual_pid = await _original_read_driver(session)
    if (
        actual_driver is not driver
        or actual_pid != backend_pid
        or session.get_transaction() is not transaction
        or session.get_nested_transaction() is not None
    ):
        raise _unavailable()


@asynccontextmanager
async def api_catalog_read_snapshot(session, generation_id=None, control_schema=None):
    """Guard control reads; address consumers retain their separate bound data scope."""
    from process.registry_network_catalog_read import _PINNED_READ

    schema = registry_schema() if control_schema is None else control_schema
    transaction, reader = _reader_entry(session)
    driver, backend_pid = await _original_read_driver(session)
    async with provider_read_savepoint(session):
        manifest, approved, relation = await read_published_api_pin(driver, generation_id, schema)
    await _same_reader(session, transaction, reader, driver, backend_pid)
    object.__setattr__(manifest, "_api_request_session", session)
    object.__setattr__(manifest, "_api_request_transaction", transaction)
    controls = (relation,)
    object.__setattr__(manifest, "_api_approved_source", approved)
    token = _PINNED_READ.set((driver, manifest, schema))
    try:
        with network_read_name_inventory(driver, controls):
            yield manifest, relation
            await _same_reader(session, transaction, reader, driver, backend_pid)
    finally:
        _PINNED_READ.reset(token)
