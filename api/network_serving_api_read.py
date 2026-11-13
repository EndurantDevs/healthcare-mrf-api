# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind protected API metadata to the original verified Reader transaction."""

from contextlib import asynccontextmanager

import asyncpg

from api.network_read_execution import _original_read_driver, network_read_name_inventory
from api.network_serving_read_context import _entry_snapshot, _require_reader_owner
from api.provider_profile_snapshot import provider_read_savepoint
from db.registry_schema import registry_schema
from process.network_published_api_pin import read_published_api_pin
from process.network_read_identity import NetworkReadIdentityError, verify_bound_network_names
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


def _is_missing_custody(error, schema):
    """Recognize only the optional historical ledger, never another SQL failure."""
    if not isinstance(error, asyncpg.UndefinedTableError) or error.sqlstate != "42P01":
        return False
    if error.schema_name is not None or error.table_name is not None:
        return error.schema_name == schema and error.table_name == "network_candidate_custody"
    return error.message == f'relation "{schema}.network_candidate_custody" does not exist'


async def _legacy_pin(driver, generation_id, schema):
    """Retain full historical verification; an indexed missing seal never becomes legacy."""
    from process.network_approved_membership_source import _pin_verified_api_approved_source
    from process.registry_network_catalog_read import catalog_pin

    manifest = await catalog_pin(driver, generation_id, schema, api_read=True)
    if manifest.supports_location_lookup:
        raise _unavailable()
    approved, relation = await _pin_verified_api_approved_source(driver, manifest, control_schema=schema)
    return manifest, approved, relation


@asynccontextmanager
async def api_catalog_read_snapshot(session, generation_id=None, control_schema=None):
    """Guard control reads; address consumers retain their separate bound data scope."""
    from process.registry_network_catalog_read import _PINNED_READ

    schema = registry_schema() if control_schema is None else control_schema
    transaction, reader = _reader_entry(session)
    driver, backend_pid = await _original_read_driver(session)
    try:
        async with provider_read_savepoint(session):
            resolved = await read_published_api_pin(driver, generation_id, schema)
    except asyncpg.UndefinedTableError as error:
        if not _is_missing_custody(error, schema):
            raise
        resolved = None
    if resolved is None:
        resolved = await _legacy_pin(driver, generation_id, schema)
    await _same_reader(session, transaction, reader, driver, backend_pid)
    manifest, approved, relation = resolved
    object.__setattr__(manifest, "_api_request_session", session)
    object.__setattr__(manifest, "_api_request_transaction", transaction)
    controls = (relation,)
    object.__setattr__(manifest, "_api_approved_source", approved)
    token = _PINNED_READ.set((driver, manifest, schema))
    try:
        with network_read_name_inventory(driver, controls):
            yield manifest, relation
            if getattr(manifest, "_published_api_request", False) is not True:
                try:
                    await verify_bound_network_names(driver, controls, expected_backend_pid=backend_pid)
                except NetworkReadIdentityError as error:
                    raise _unavailable() from error
            await _same_reader(session, transaction, reader, driver, backend_pid)
    finally:
        _PINNED_READ.reset(token)
