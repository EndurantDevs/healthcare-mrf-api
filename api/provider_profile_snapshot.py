# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep one provider response on stable serving relations and one read snapshot."""

import asyncio
import re
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar
from functools import wraps

import asyncpg
from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, InterfaceError

from process import entity_address_result_generation as address_generation
from process import provider_directory_cms_serving_receipt as cms_serving_receipt
from process import reference_family_result_generation as reference_generation
from process.entity_address_cutover_contract import postgres_sqlstate
from process.npi_canonical_publication import NPI_CANONICAL_TABLES

_SNAPSHOT = ContextVar("provider_profile_read_snapshot", default=None)
_CMS_SERVING = ContextVar("provider_profile_cms_serving_generation", default=None)
_CMS_RECEIPT_TABLE = "provider_directory_cms_serving_receipt"
_PROFILE_GENERATION_TABLE = "provider_directory_profile_serving_generation"
_DOCTORS_TABLES = reference_generation.RELATION_NAMES_BY_IMPORTER["cms-doctors"]
_PROFILE_TABLES = (
    "provider_directory_profile",
    "provider_directory_profile_evidence",
    "provider_profile_projection",
    "provider_profile_source_publication",
    "provider_profile_import_run",
    "provider_profile_fact",
)
_DIRECTORY_TABLES = (
    "provider_directory_address_overlay",
    "provider_directory_source",
    "provider_directory_endpoint_dataset",
    "provider_directory_endpoint",
    "provider_directory_dataset_resource",
    "provider_directory_practitioner",
    "provider_directory_practitioner_role",
    "provider_directory_location",
    "provider_directory_organization",
    "provider_directory_organization_affiliation",
    "provider_directory_healthcare_service",
    "provider_directory_insurance_plan",
    "provider_directory_network_catalog",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_affiliation_organization",
)
_DETAIL_TABLES = (
    *NPI_CANONICAL_TABLES,
    *address_generation.RELATION_NAMES,
    *_DIRECTORY_TABLES,
    "npi_canonical_publication_receipt",
    "npi_canonical_publication_receipt_seal",
    "npi_result_generation",
    "mrf_address_evidence",
    "provider_enrichment_summary",
    "provider_enrollment_ffs",
    "provider_enrollment_ffs_additional_npi",
    "provider_enrollment_ffs_address",
    "provider_enrollment_ffs_secondary_specialty",
    "provider_enrollment_ffs_reassignment",
    "provider_enrollment_hospital",
    "provider_enrollment_hha",
    "provider_enrollment_hospice",
    "provider_enrollment_fqhc",
    "provider_enrollment_rhc",
    "provider_enrollment_snf",
)
_COMMON_TABLES = (
    *_DOCTORS_TABLES,
    *_PROFILE_TABLES,
    "provider_directory_address_overlay",
    *address_generation.RELATION_NAMES,
    _PROFILE_GENERATION_TABLE,
    reference_generation.TABLE_NAME,
    address_generation.TABLE_NAME,
    _CMS_RECEIPT_TABLE,
    "address_alias_state_v1",
)
_GEO_TABLES = tuple(
    sorted(
        set(
            (
                *_COMMON_TABLES,
                *NPI_CANONICAL_TABLES,
                "npi_canonical_publication_receipt",
                "npi_canonical_publication_receipt_seal",
                "npi_result_generation",
                "nucc_taxonomy",
            )
        )
    )
)


def snapshot_relation_available(table_ref):
    """Use the pinned optional-table inventory instead of a process-wide cache."""
    snapshot = _SNAPSHOT.get()
    if snapshot is None or table_ref not in snapshot:
        return None
    return snapshot[table_ref]


def snapshot_cms_serving_generation(schema):
    """Expose only the Doctors authority validated against this read's pinned relations."""
    serving = _CMS_SERVING.get()
    if serving is None:
        return None
    snapshot_schema, authority = serving
    if schema != snapshot_schema:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return authority.serving_generation.as_dict() if authority and authority.serving_generation else None


def _recoverable_statement_error(error):
    """A rolled-back statement can recover while its database connection remains usable."""
    original = getattr(error, "orig", None)
    if any(
        isinstance(value, (InterfaceError, asyncpg.InterfaceError, asyncpg.PostgresConnectionError, ConnectionError))
        for value in (error, original, error.__cause__, getattr(original, "__cause__", None))
    ):
        return False
    if isinstance(error, DBAPIError) and error.connection_invalidated:
        return False
    state = postgres_sqlstate(error)
    return bool(
        state
        and re.fullmatch(r"[0-9A-Z]{5}", state)
        and not state.startswith("08")
        and state not in {"57P01", "57P02", "57P03"}
    )


@asynccontextmanager
async def provider_directory_read_snapshot(database, session=None, *, reuse=False):
    """Own an ordinary read snapshot without changing the caller's Writer session."""
    from db.connection import has_reader_session

    if reuse:
        yield session
        return
    async with database.session() as read_session:
        has_reader = has_reader_session(database)
        async with nullcontext() if has_reader else read_session.begin():
            if not has_reader:
                await read_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            yield read_session


def provider_directory_snapshot_read(handler):
    """Keep ordinary provider HTTP loaders on one explicit read session."""

    @wraps(handler)
    async def snapshot_read(request, *args, session=None, **kwargs):
        """Borrow existing scopes or close one ordinary snapshot before returning."""
        if any(
            kwargs.get(name) is not None
            for name in ("native_args", "import_context", "native_npis", "native_scope_preparer")
        ):
            return await handler(request, *args, session=session, **kwargs)
        from api.endpoint import npi as provider
        from db.connection import has_reader_session

        request_session = session if session is not None else provider._request_session(request)
        should_reuse = (
            session is not None
            or request_session is None
            or has_reader_session(provider.db)
            or provider.current_network_address_scope() is not None
        )
        async with provider_directory_read_snapshot(provider.db, request_session, reuse=should_reuse) as read_session:
            return await handler(request, *args, session=read_session, **kwargs)

    return snapshot_read


@asynccontextmanager
async def provider_read_savepoint(session):
    """Keep optional read failures from aborting the caller's remaining reads."""
    body_error = None
    try:
        async with session.begin_nested() if session is not None else nullcontext():
            try:
                yield
            except BaseException as error:
                body_error = error
                raise
    except BaseException as error:
        if isinstance(body_error, asyncio.CancelledError):
            if error is not body_error:
                body_error.add_note("Optional savepoint cleanup failed: " + type(error).__name__)
            raise body_error
        if isinstance(error, asyncio.CancelledError):
            raise
        if body_error is not None and error is not body_error:
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error
        if isinstance(error, (DBAPIError, asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError)) and not (
            _recoverable_statement_error(error)
        ):
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error
        raise
    if body_error is not None:
        raise body_error


@asynccontextmanager
async def provider_optional_read(session, logger):
    """Keep optional provider reads recoverable within the caller's snapshot."""
    try:
        async with provider_read_savepoint(session):
            yield
    except ServiceUnavailable:
        raise
    except Exception as exc:
        logger.debug("Optional provider read failed: %s", exc)


async def _inventory_serving_relations(session, schema, *, include_detail):
    table_names = tuple(sorted(set((*_COMMON_TABLES, *(_DETAIL_TABLES if include_detail else ())))))
    return await _inventory_relation_names(session, schema, table_names)


async def _inventory_relation_names(session, schema, table_names):
    """Read optional relation availability in the request's repeatable snapshot."""
    result = await session.execute(
        text(
            "SELECT name,to_regclass(format('%I.%I',CAST(:schema AS text),name)) IS NOT NULL "
            "FROM unnest(CAST(:names AS text[])) AS names(name)"
        ),
        {"schema": schema, "names": list(table_names)},
    )
    return {f"{schema}.{name}": available for name, available in result.all()}


async def _read_native_authorities(session, schema, relation_availability, *, include_address):
    """Check installed native authorities before publishing their pinned metadata."""
    authorities_by_family = {}
    if relation_availability[f"{schema}.{reference_generation.TABLE_NAME}"]:
        has_doctors = (
            await session.execute(
                text(
                    f'SELECT EXISTS (SELECT 1 FROM "{schema}".{reference_generation.TABLE_NAME} '
                    "WHERE importer_id='cms-doctors')"
                ),
            )
        ).scalar()
        if has_doctors:
            authority = await reference_generation.read_reference_family_result_generation_authority(
                session,
                importer_id="cms-doctors",
                schema_name=schema,
            )
            authorities_by_family["cms-doctors"] = authority
    if include_address and relation_availability[f"{schema}.{address_generation.TABLE_NAME}"]:
        has_address_authority = (
            await session.execute(text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{address_generation.TABLE_NAME})'))
        ).scalar()
        if has_address_authority:
            authority = await address_generation.read_entity_address_result_generation_authority(
                session,
                schema_name=schema,
            )
            authorities_by_family["entity-address"] = authority
    return authorities_by_family


async def _requires_cms_receipt(session, schema, relation_availability):
    """Keep withdrawals guarded by receipt history without treating legacy Doctors as CMS NPD."""
    if relation_availability[f"{schema}.{_CMS_RECEIPT_TABLE}"]:
        result = await session.execute(text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_CMS_RECEIPT_TABLE})'))
        if result.scalar():
            return True
    if relation_availability[f"{schema}.{_PROFILE_GENERATION_TABLE}"]:
        result = await session.execute(
            text(
                f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_PROFILE_GENERATION_TABLE} '
                "WHERE singleton_key='global' AND source_vector_json @> '[{\"source_id\":\"cms-npd\"}]'::jsonb)"
            ),
        )
        return bool(result.scalar())
    return False


async def _read_cms_receipt(session, schema, relation_availability):
    """Accept only the immutable proof of the pinned native serving relations."""
    receipt = None
    if relation_availability[f"{schema}.{_CMS_RECEIPT_TABLE}"]:
        receipt = await cms_serving_receipt.read_serving_receipt(session, schema)
    if receipt is None:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return receipt


@asynccontextmanager
async def provider_profile_read_snapshot(database, schema, *, include_detail=False):
    """Bind sequential loader calls to one bounded, read-only serving snapshot."""
    async with _provider_read_snapshot(database, schema, include_detail=include_detail) as session:
        yield session


@asynccontextmanager
async def provider_geo_read_snapshot(database, schema):
    """Pin geo projection inputs while retaining native address and CMS withdrawal checks."""
    async with _provider_read_snapshot(database, schema, include_detail=True, table_names=_GEO_TABLES) as session:
        yield session


@asynccontextmanager
async def _provider_read_snapshot(database, schema, *, include_detail, table_names=None):
    """Share setup-only retry and exact Reader ownership across fixed projections."""
    if not isinstance(schema, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise ValueError("provider_profile_schema_invalid")
    max_attempts = 1 if database.has_reader_session() or database._transaction_binding() is not None else 3
    scope_options_by_name = {"include_detail": include_detail}
    if table_names is not None:
        scope_options_by_name["table_names"] = table_names
    for attempt in range(1, max_attempts + 1):
        has_yielded = False
        session = None
        try:
            async with database.reader_session() as session:
                async with _read_snapshot_scope(session, schema, **scope_options_by_name):
                    has_yielded = True
                    yield session
            return
        except DBAPIError as error:
            if not has_yielded and attempt < max_attempts and postgres_sqlstate(error) == "55P03":
                continue
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error


@asynccontextmanager
async def _read_snapshot_scope(session, schema, *, include_detail, table_names=None):
    """Finish setup before yielding; loader failures never replay a response."""
    if not session.info.get("api_reader_verified"):
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
    await session.execute(text("SET LOCAL statement_timeout = '5s'"))
    relation_availability = (
        await _inventory_serving_relations(session, schema, include_detail=include_detail)
        if table_names is None
        else await _inventory_relation_names(session, schema, table_names)
    )
    requires_cms_receipt = await _requires_cms_receipt(session, schema, relation_availability)
    authorities_by_family = await _read_native_authorities(
        session,
        schema,
        relation_availability,
        include_address=include_detail or requires_cms_receipt,
    )
    receipt = await _read_cms_receipt(session, schema, relation_availability) if requires_cms_receipt else None
    session.info["provider_profile_native_authorities"] = authorities_by_family
    session.info["provider_profile_cms_serving_receipt"] = receipt
    token = _SNAPSHOT.set(relation_availability)
    cms_token = _CMS_SERVING.set((schema, authorities_by_family.get("cms-doctors")))
    try:
        yield session
    finally:
        _CMS_SERVING.reset(cms_token)
        _SNAPSHOT.reset(token)
        session.info.pop("provider_profile_native_authorities", None)
        session.info.pop("provider_profile_cms_serving_receipt", None)
