# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared native endpoint session locks before a fresh metadata publication snapshot."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from process.registry_fhir_catalog_sources import MAX_PAGE, validate_fhir_catalog_source_descriptor
from process.uhc_flex_practitioner_async_safety import drain_operation

_ISSUED = ContextVar("registry_fhir_catalog_source_fence", default=None)
_HELD_SQL = """SELECT DISTINCT keys.lock_key FROM unnest($1::bigint[]) keys(lock_key)
JOIN pg_catalog.pg_locks held
 ON held.classid=((keys.lock_key>>32)&4294967295)::oid AND held.objid=(keys.lock_key&4294967295)::oid
WHERE held.locktype='advisory' AND held.pid=pg_catalog.pg_backend_pid() AND held.granted
 AND held.objsubid=1 AND {mode}
 AND held.database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())
ORDER BY keys.lock_key"""
_TRY_SQL = """WITH keys AS MATERIALIZED (SELECT key FROM unnest($1::bigint[]) keys(key) ORDER BY key)
SELECT key,pg_catalog.pg_try_advisory_lock_shared(key) AS acquired FROM keys ORDER BY key"""


class FHIRCatalogSourceFenceError(ValueError):
    """Refuse a busy, pre-existing, lost or incorrectly owned publication fence."""


@dataclass(frozen=True)
class _Fence:
    connection: object
    task: object
    endpoints: tuple[str, ...]
    keys: tuple[int, ...]


def _fail(reason):
    raise FHIRCatalogSourceFenceError("registry_fhir_catalog_source_fence_" + reason)


def _endpoints(descriptors):
    if type(descriptors) is not tuple or not 1 <= len(descriptors) <= MAX_PAGE:
        _fail("invalid")
    return tuple(
        sorted({validate_fhir_catalog_source_descriptor(descriptor)["endpoint_id"] for descriptor in descriptors})
    )


async def _held_keys(connection, keys, *, shared_only):
    if not keys:
        return set()
    mode = "held.mode='ShareLock'" if shared_only else "held.mode IN ('ShareLock','ExclusiveLock')"
    records = await connection.fetch(_HELD_SQL.format(mode=mode), keys, timeout=5)
    return {record["lock_key"] for record in records}


async def _acquire(connection, keys, timeout_seconds):
    deadline, acquired_keys = asyncio.get_running_loop().time() + timeout_seconds, set()
    while len(acquired_keys) < len(keys):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            _fail("busy")
        pending = sorted(set(keys) - acquired_keys)
        records = await drain_operation(
            connection.fetch(_TRY_SQL, pending, timeout=remaining), preserve_cancellation=True
        )
        acquired_keys.update(record["key"] for record in records if record["acquired"] is True)
        if len(acquired_keys) < len(keys):
            await asyncio.sleep(min(0.05, max(0, deadline - asyncio.get_running_loop().time())))


async def _release(connection, keys):
    if connection.is_in_transaction():
        _fail("transaction_open")
    held = sorted(await _held_keys(connection, keys, shared_only=True))
    if held:
        records = await connection.fetch(
            "SELECT key,pg_catalog.pg_advisory_unlock_shared(key) AS released "
            "FROM unnest($1::bigint[]) keys(key) ORDER BY key",
            held,
            timeout=5,
        )
        if len(records) != len(held) or any(record["released"] is not True for record in records):
            _fail("release_failed")
    if await _held_keys(connection, keys, shared_only=False):
        _fail("release_failed")


@asynccontextmanager
async def fhir_catalog_source_publication_fence(connection, descriptors, *, timeout_seconds=5):
    """Fence native Source publishers before RR begins; keep this connection through commit."""
    if connection.is_in_transaction() or _ISSUED.get() is not None:
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
    if not keys or await _held_keys(connection, keys, shared_only=False):
        _fail("preexisting_lock")
    token, original_error = None, None
    try:
        await _acquire(connection, keys, timeout_seconds)
        token = _ISSUED.set(_Fence(connection, asyncio.current_task(), endpoints, keys))
        yield
    except BaseException as error:
        original_error = error
        raise
    finally:
        if token is not None:
            _ISSUED.reset(token)
        try:
            await drain_operation(_release(connection, keys), preserve_cancellation=False)
        except BaseException:
            connection.terminate()
            if original_error is None:
                raise


async def require_fhir_catalog_source_publication_fence(connection, descriptors, *, required=True):
    """Verify the issued same-task context and its actual native shared session locks."""
    fence = _ISSUED.get()
    if fence is None and not required:
        return
    if type(fence) is not _Fence or fence.connection is not connection or fence.task is not asyncio.current_task():
        _fail("unavailable")
    if not set(_endpoints(descriptors)) <= set(fence.endpoints):
        _fail("scope_changed")
    if await _held_keys(connection, fence.keys, shared_only=True) != set(fence.keys):
        _fail("lost")
