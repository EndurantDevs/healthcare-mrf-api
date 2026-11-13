# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Own a configured catalog Reader snapshot for the enclosing operation."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from sqlalchemy import text

from db.connection import db
from process.network_serving_read import resolve_network_serving_manifest
from process.uhc_flex_practitioner_async_safety import drain_operation

_READS = ContextVar("fhir_catalog_registry_reads", default=None)


def _require(condition):
    if not condition:
        raise ValueError("registry_fhir_catalog_registry_reader_changed")


@dataclass
class _RegistryReads:
    session: object = None
    reader: object = None
    transaction: object = None


async def _open_reader(binding):
    database = await db._connect_reader()
    session = binding.session = database.session_factory()
    # A raw driver read does not start SQLAlchemy's lazy transaction.
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    reader = (await (await session.connection()).get_raw_connection()).driver_connection
    binding.reader, binding.transaction = reader, session.get_transaction()


async def _verify_reader(binding):
    _require(
        binding.transaction is not None
        and binding.session.get_transaction() is binding.transaction
        and binding.reader.is_in_transaction()
    )


@asynccontextmanager
async def fhir_catalog_registry_reads(*writers):
    """Lazily own one configured SQLAlchemy Reader and settle its transaction on exit."""
    _require(bool(writers) and _READS.get() is None)
    binding = _RegistryReads()
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
                failure.add_note("Catalog Reader cleanup failed")


async def fhir_catalog_registry_connection(connection):
    """Return the active configured Reader for the enclosing catalog operation."""
    binding = _READS.get()
    _require(
        type(binding) is _RegistryReads
        and callable(getattr(connection, "is_in_transaction", None))
        and connection.is_in_transaction()
    )
    if binding.reader is None:
        await _open_reader(binding)
    await _verify_reader(binding)
    return binding.reader


async def resolve_fhir_catalog_network_manifest(connection, *, generation_id=None, control_schema=None):
    """Resolve the logical serving generation on the configured Reader."""
    return await resolve_network_serving_manifest(
        await fhir_catalog_registry_connection(connection), generation_id=generation_id, control_schema=control_schema
    )


async def require_retained_fhir_catalog_sources(connection, descriptors, *, control_schema):
    """Validate retained source data inside the catalog operation’s pinned Reader."""
    from process.registry_fhir_catalog_retained import require_retained_fhir_catalog_sources as require

    return await require(await fhir_catalog_registry_connection(connection), descriptors, control_schema=control_schema)


async def catalog_projection_generations(connection, snapshot, *, grant, descriptors):
    """Resolve directory inputs on the owned Reader while retaining Sealer replay authority."""
    from process.registry_fhir_catalog_sync import resolve_fhir_catalog_projection_generations

    if snapshot["network_generation"] is not None or grant.retained_sources or grant.retained_source_recipes:
        connection = await fhir_catalog_registry_connection(connection)
    return await resolve_fhir_catalog_projection_generations(connection, snapshot, grant=grant, descriptors=descriptors)
