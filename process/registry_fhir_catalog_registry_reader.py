# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep catalog directory custody on the configured original Reader through writer commit."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text

from db.connection import db
from process.network_serving_read import resolve_network_serving_manifest
from process.uhc_flex_practitioner_async_safety import drain_operation

_READS = ContextVar("fhir_catalog_registry_reads", default=None)
_IDENTITY_SQL = """SELECT session_user::text AS login,current_user::text AS actor,
 pg_backend_pid() AS backend,(SELECT oid FROM pg_database WHERE datname=current_database()) AS database_oid,
 current_setting('transaction_read_only') AS readonly,current_setting('transaction_isolation') AS isolation,
 (SELECT virtualxid FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid' AND granted) AS transaction"""


def _require(condition):
    if not condition:
        raise ValueError("registry_fhir_catalog_registry_reader_changed")


@dataclass
class _RegistryReads:
    writers: tuple
    task: object
    session: object = None
    reader: object = None
    transaction: object = None
    identity: object = None


async def _open_reader(binding):
    database = await db._connect_reader()
    session = binding.session = database.session_factory()
    identity_by_field = dict((await session.execute(text(_IDENTITY_SQL))).mappings().one())
    reader = (await (await session.connection()).get_raw_connection()).driver_connection
    _require(
        identity_by_field["actor"] == identity_by_field["login"] == database._reader_login[0]
        and (identity_by_field["isolation"], identity_by_field["readonly"]) == ("repeatable read", "on")
        and identity_by_field["transaction"] is not None
        and reader.is_in_transaction()
    )
    for writer in binding.writers:
        _require(writer is not reader and writer.is_in_transaction())
        actor = await writer.fetchrow(_IDENTITY_SQL)
        _require(
            actor["actor"] == actor["login"]
            and actor["actor"] != identity_by_field["actor"]
            and actor["backend"] != identity_by_field["backend"]
            and actor["database_oid"] == identity_by_field["database_oid"]
        )
        # A native lock collision proves the same database, including across identically named servers.
        key = int.from_bytes(uuid4().bytes[:8], "big", signed=True)
        _require(await writer.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", key) is True)
        _require(await reader.fetchval("SELECT pg_try_advisory_xact_lock($1::bigint)", key) is False)
    binding.reader, binding.transaction, binding.identity = reader, session.get_transaction(), identity_by_field


async def _verify_reader(binding):
    _require(
        binding.task is asyncio.current_task()
        and binding.session.get_transaction() is binding.transaction
        and binding.reader.is_in_transaction()
    )
    _require(dict(await binding.reader.fetchrow(_IDENTITY_SQL)) == binding.identity)


@asynccontextmanager
async def fhir_catalog_registry_reads(*writers):
    """Lazily own one native Reader without binding an API or Source transaction context.

    Enter this scope before opening writer transactions. Call
    ``verify_fhir_catalog_registry_reads`` immediately before each writer commit,
    then leave this scope after those commits; a helper-local scope is too short.
    """
    _require(bool(writers) and _READS.get() is None)
    binding = _RegistryReads(writers, asyncio.current_task())
    token = _READS.set(binding)
    failure = None
    try:
        yield
        if binding.reader is not None:
            await _verify_reader(binding)
    except BaseException as error:
        failure = error
        raise
    finally:
        _READS.reset(token)
        if binding.session is not None:
            try:
                try:
                    await drain_operation(binding.session.rollback(), preserve_cancellation=True)
                finally:
                    await drain_operation(binding.session.close(), preserve_cancellation=False)
            except BaseException:
                if failure is None:
                    raise
                failure.add_note("Catalog registry Reader cleanup failed")


async def fhir_catalog_registry_connection(connection):
    """Refuse unowned, cross-task or reopened snapshots before directory custody is read."""
    binding = _READS.get()
    _require(
        type(binding) is _RegistryReads
        and binding.task is asyncio.current_task()
        and (any(connection is writer for writer in binding.writers) or connection is binding.reader)
        and connection.is_in_transaction()
    )
    if binding.reader is None:
        await _open_reader(binding)
    await _verify_reader(binding)
    return binding.reader


async def verify_fhir_catalog_registry_reads():
    """Refuse lost custody before the enclosing writer transaction commits."""
    binding = _READS.get()
    _require(type(binding) is _RegistryReads and binding.task is asyncio.current_task())
    if binding.reader is not None:
        await _verify_reader(binding)


async def resolve_fhir_catalog_network_manifest(connection, *, generation_id=None, control_schema=None):
    """Retain the producer's locking verifier on the original configured Reader."""
    return await resolve_network_serving_manifest(
        await fhir_catalog_registry_connection(connection), generation_id=generation_id, control_schema=control_schema
    )


async def require_retained_fhir_catalog_sources(connection, descriptors, *, control_schema):
    """Recheck retained custody inside the catalog phase's still-open Reader."""
    from process.registry_fhir_catalog_retained import require_retained_fhir_catalog_sources as require

    return await require(await fhir_catalog_registry_connection(connection), descriptors, control_schema=control_schema)


async def catalog_projection_generations(connection, snapshot, *, grant, descriptors):
    """Resolve directory inputs on the owned Reader while retaining Sealer replay authority."""
    from process.registry_fhir_catalog_sync import resolve_fhir_catalog_projection_generations

    if snapshot["network_generation"] is not None or grant.retained_sources or grant.retained_source_recipes:
        connection = await fhir_catalog_registry_connection(connection)
    return await resolve_fhir_catalog_projection_generations(connection, snapshot, grant=grant, descriptors=descriptors)
