# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared native endpoint session locks before a fresh metadata publication snapshot."""

import asyncio
from contextlib import asynccontextmanager

from process.registry_fhir_catalog_sources import MAX_PAGE, validate_fhir_catalog_source_descriptor
from process.uhc_flex_practitioner_async_safety import drain_operation

_TRY_SQL = """WITH keys AS MATERIALIZED (SELECT key FROM unnest($1::bigint[]) keys(key) ORDER BY key)
SELECT key,pg_catalog.pg_try_advisory_lock_shared(key) AS acquired FROM keys ORDER BY key"""


class FHIRCatalogSourceFenceError(ValueError):
    """Refuse a busy logical endpoint lock or failed acquisition cleanup."""


def _fail(reason):
    raise FHIRCatalogSourceFenceError("registry_fhir_catalog_source_fence_" + reason)


def _endpoints(descriptors):
    if type(descriptors) is not tuple or not 1 <= len(descriptors) <= MAX_PAGE:
        _fail("invalid")
    return tuple(
        sorted({validate_fhir_catalog_source_descriptor(descriptor)["endpoint_id"] for descriptor in descriptors})
    )


async def _acquire(connection, keys, timeout_seconds, acquired_keys):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    task, cancellations = asyncio.current_task(), asyncio.current_task().cancelling()
    while len(acquired_keys) < len(keys):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            _fail("busy")
        pending = sorted(set(keys) - acquired_keys)
        records = await drain_operation(
            connection.fetch(_TRY_SQL, pending, timeout=remaining), preserve_cancellation=False
        )
        acquired_keys.update(record["key"] for record in records if record["acquired"] is True)
        if task.cancelling() > cancellations:
            raise asyncio.CancelledError
        if len(acquired_keys) < len(keys):
            await asyncio.sleep(min(0.05, max(0, deadline - asyncio.get_running_loop().time())))


async def _release(connection, keys):
    if connection.is_in_transaction():
        _fail("transaction_open")
    if keys:
        records = await connection.fetch(
            "SELECT key,pg_catalog.pg_advisory_unlock_shared(key) AS released "
            "FROM unnest($1::bigint[]) keys(key) ORDER BY key",
            sorted(keys),
            timeout=5,
        )
        if len(records) != len(keys) or any(record["released"] is not True for record in records):
            _fail("release_failed")


@asynccontextmanager
async def fhir_catalog_source_publication_fence(connection, descriptors, *, timeout_seconds=5):
    """Fence native Source publishers before RR begins; keep this connection through commit."""
    if connection.is_in_transaction():
        _fail("pre_snapshot_required")
    if type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 60:
        _fail("invalid")
    endpoints = _endpoints(descriptors)
    lock_records = await connection.fetch(
        "SELECT DISTINCT pg_catalog.hashtextextended(endpoint,0) AS key "
        "FROM unnest($1::text[]) endpoints(endpoint) ORDER BY key",
        endpoints,
        timeout=5,
    )
    keys = tuple(lock_record["key"] for lock_record in lock_records)
    if not keys:
        _fail("invalid")
    acquired_keys, original_error = set(), None
    try:
        await _acquire(connection, keys, timeout_seconds, acquired_keys)
        yield
    except BaseException as error:
        original_error = error
        raise
    finally:
        try:
            await drain_operation(_release(connection, acquired_keys), preserve_cancellation=False)
        except BaseException:
            connection.terminate()
            if original_error is None:
                raise
