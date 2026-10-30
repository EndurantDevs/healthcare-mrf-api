# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep one provider response on stable serving relations and one read snapshot."""

import asyncio
import re
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from api.network_read_execution import (
    _original_read_driver,
    _read_sql_failure_cause,
    _ReadSQLFailure,
    _recover_optional_read,
    _rollback_optional_read,
    _unavailable,
    execute_network_read,
    has_network_read_inventory,
    network_read_name_inventory,
)
from process import entity_address_result_generation as address_generation
from process import provider_directory_cms_serving_receipt as cms_serving_receipt
from process import reference_family_result_generation as reference_generation
from process.entity_address_cutover_contract import postgres_sqlstate
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError, verify_bound_network_names
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
_DIRECTORY_PARTITION_ROOTS = frozenset(
    (
        "provider_directory_dataset_resource",
        "provider_directory_dataset_network_plan",
        "provider_directory_dataset_insurance_plan",
        "provider_directory_dataset_affiliation_organization",
    )
)
_SNAPSHOT_RELATIONS_SQL = """SELECT name, c.oid::bigint, c.relkind::text AS relkind,
    NOT c.relispartition AND NOT EXISTS(
      SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS direct_heap,
    c.relkind='p' AND c.relpersistence='p' AND NOT c.relispartition
      AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid)
      AND pg_catalog.pg_get_partkeydef(c.oid)='LIST (dataset_id)'
      AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class leaf ON leaf.oid=i.inhrelid
        WHERE i.inhparent=c.oid AND (leaf.relkind<>'r' OR NOT leaf.relispartition OR leaf.relpersistence<>'p'
          OR leaf.relnamespace<>c.relnamespace
          OR EXISTS(SELECT FROM pg_catalog.pg_inherits other WHERE other.inhparent=leaf.oid
            OR (other.inhrelid=leaf.oid AND other.inhparent<>c.oid)))) AS directory_partition_root
    FROM unnest(CAST(:names AS text[])) AS names(name)
    LEFT JOIN pg_catalog.pg_namespace n ON n.nspname=:schema
    LEFT JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid AND c.relname=name"""
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
    return snapshot[table_ref] is not None


def snapshot_cms_serving_generation(schema):
    """Expose only the Doctors authority validated against this read's pinned relations."""
    serving = _CMS_SERVING.get()
    if serving is None:
        return None
    snapshot_schema, authority = serving
    if schema != snapshot_schema:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return authority.serving_generation.as_dict() if authority and authority.serving_generation else None


@asynccontextmanager
async def provider_read_savepoint(session):
    """Keep optional read failures from aborting a caller's remaining reads."""
    transaction = session.get_transaction() if hasattr(session, "get_transaction") else None
    parent = session.get_nested_transaction() if hasattr(session, "get_nested_transaction") else None
    body_error = rollback = None
    try:
        async with session.begin_nested() if session is not None else nullcontext() as savepoint:
            try:
                yield
            except BaseException as error:
                body_error = error
                if type(error) is _ReadSQLFailure:
                    rollback = await _rollback_optional_read(session, error, savepoint, transaction, parent)
                raise
    except BaseException as error:
        if isinstance(body_error, asyncio.CancelledError):
            if error is not body_error:
                body_error.add_note("Optional savepoint cleanup failed: " + type(error).__name__)
            raise body_error
        if isinstance(error, asyncio.CancelledError):
            raise
        if error is not body_error and (
            isinstance(body_error, ServiceUnavailable) or has_network_read_inventory(session)
        ):
            raise _unavailable() from error
        if rollback is not None and error is body_error:
            original = await _recover_optional_read(session, rollback)
            raise original
        raise
    if body_error is not None:
        # A suppressing/invalid context cannot authorize an optional guarded fallback.
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


async def _relation_oids(session, schema, table_names):
    result = await execute_network_read(
        session,
        text(
            "SELECT name, to_regclass(format('%I.%I', CAST(:schema AS text), name))::oid::bigint "
            "FROM unnest(CAST(:names AS text[])) AS names(name)"
        ),
        {"schema": schema, "names": list(table_names)},
    )
    return {f"{schema}.{name}": oid for name, oid in result.all()}


class _SnapshotSetupChanged(ServiceUnavailable):
    """A catalog rename raced snapshot setup before any loader could run."""


async def _snapshot_relation_oids(session, schema, table_names):
    """Pin declared legacy partition roots while keeping direct heaps lock-free."""
    result = await session.execute(
        text(_SNAPSHOT_RELATIONS_SQL),
        {"schema": schema, "names": list(table_names)},
    )
    rows = result.all()
    partition_roots = []
    for name, oid, kind, direct, partition_root in rows:
        if oid is None or kind == "r" and direct is True:
            continue
        if name not in _DIRECTORY_PARTITION_ROOTS or kind != "p" or partition_root is not True:
            raise ServiceUnavailable("Provider data is temporarily unavailable.")
        partition_roots.append(name)
    if partition_roots:
        qualified_names = ", ".join(f'"{schema}"."{name}"' for name in sorted(partition_roots))
        # Retain legacy recursive locks; immutable dataset publication still supplies custody.
        await session.execute(text(f"LOCK TABLE {qualified_names} IN ACCESS SHARE MODE"))
    return {f"{schema}.{name}": oid for name, oid, _kind, _direct, _partition_root in rows}


async def _inventory_serving_relations(session, schema, *, include_detail):
    """Inventory every optional name and reject a stale repeatable-read catalog."""
    table_names = tuple(sorted(set((*_COMMON_TABLES, *(_DETAIL_TABLES if include_detail else ())))))
    return await _inventory_relation_names(session, schema, table_names)


async def _inventory_relation_names(session, schema, table_names):
    """Reject unsupported topology and require an exact fresh name/OID recheck."""
    relation_oids = await _snapshot_relation_oids(session, schema, table_names)
    if await _relation_oids(session, schema, table_names) != relation_oids:
        raise _SnapshotSetupChanged("Provider data changed during the read; retry the request.")
    return relation_oids


async def _read_native_authorities(session, schema, relation_oids, *, include_address, verify_names):
    """Check installed native authorities before publishing their pinned metadata."""
    authorities_by_family = {}
    if relation_oids[f"{schema}.{reference_generation.TABLE_NAME}"] is not None:
        has_doctors = (
            await execute_network_read(
                session,
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
            await verify_names()
            authorities_by_family["cms-doctors"] = authority
            _require_matching_relations(authority, schema, _DOCTORS_TABLES, relation_oids)
    if include_address and relation_oids[f"{schema}.{address_generation.TABLE_NAME}"] is not None:
        has_address_authority = (
            await execute_network_read(
                session, text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{address_generation.TABLE_NAME})')
            )
        ).scalar()
        if has_address_authority:
            authority = await address_generation.read_entity_address_result_generation_authority(
                session,
                schema_name=schema,
            )
            await verify_names()
            authorities_by_family["entity-address"] = authority
            _require_matching_relations(authority, schema, address_generation.RELATION_NAMES, relation_oids)
    return authorities_by_family


async def _requires_cms_receipt(session, schema, relation_oids):
    """Keep withdrawals guarded by receipt history without treating legacy Doctors as CMS NPD."""
    if relation_oids[f"{schema}.{_CMS_RECEIPT_TABLE}"] is not None:
        result = await execute_network_read(
            session, text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_CMS_RECEIPT_TABLE})')
        )
        if result.scalar():
            return True
    if relation_oids[f"{schema}.{_PROFILE_GENERATION_TABLE}"] is not None:
        result = await execute_network_read(
            session,
            text(
                f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_PROFILE_GENERATION_TABLE} '
                "WHERE singleton_key='global' AND source_vector_json @> '[{\"source_id\":\"cms-npd\"}]'::jsonb)"
            ),
        )
        return bool(result.scalar())
    return False


async def _read_cms_receipt(session, schema, relation_oids, *, verify_names):
    """Accept only the immutable proof of the pinned native serving relations."""
    receipt = None
    if relation_oids[f"{schema}.{_CMS_RECEIPT_TABLE}"] is not None:
        receipt = await cms_serving_receipt.read_serving_receipt(session, schema)
        await verify_names()
    if receipt is None:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return receipt


def _require_matching_relations(authority, schema, table_names, relation_oids):
    """Preserve generation-less legacy reads while rejecting a drifted native receipt."""
    if authority.relation_oids is not None and authority.relation_oids != tuple(
        relation_oids[f"{schema}.{name}"] for name in table_names
    ):
        raise ServiceUnavailable("Provider data is temporarily unavailable.")


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
        except _SnapshotSetupChanged:
            if has_yielded or attempt == max_attempts:
                raise
        except (DBAPIError, _ReadSQLFailure) as error:
            original = error if isinstance(error, DBAPIError) else _read_sql_failure_cause(error, session)
            if (
                not has_yielded
                and attempt < max_attempts
                and original is not None
                and postgres_sqlstate(original) == "55P03"
            ):
                continue
            if isinstance(error, _ReadSQLFailure):
                raise
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error


@asynccontextmanager
async def _read_snapshot_scope(session, schema, *, include_detail, table_names=None):
    """Finish setup before yielding; loader failures never replay a response."""
    if not session.info.get("api_reader_verified"):
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
    await session.execute(text("SET LOCAL statement_timeout = '5s'"))
    driver, backend_pid = await _original_read_driver(session)
    relation_oids = (
        await _inventory_serving_relations(session, schema, include_detail=include_detail)
        if table_names is None
        else await _inventory_relation_names(session, schema, table_names)
    )
    relations = tuple(
        CompactAddressRelation(schema, name.split(".", 1)[1], oid)
        for name, oid in relation_oids.items()
        if oid is not None
    )
    absent_names = tuple((schema, name.split(".", 1)[1]) for name, oid in relation_oids.items() if oid is None)

    async def verify_names():
        """Check setup results on their original backend before publishing metadata."""
        current_driver, current_pid = await _original_read_driver(session)
        if current_driver is not driver or current_pid != backend_pid:
            raise ServiceUnavailable("Provider data is temporarily unavailable.")
        try:
            await verify_bound_network_names(
                driver, relations, expected_backend_pid=backend_pid, absent_names=absent_names
            )
        except NetworkReadIdentityError as error:
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error

    with network_read_name_inventory(driver, relations, absent_names=absent_names):
        requires_cms_receipt = await _requires_cms_receipt(session, schema, relation_oids)
        authorities_by_family = await _read_native_authorities(
            session,
            schema,
            relation_oids,
            include_address=include_detail or requires_cms_receipt,
            verify_names=verify_names,
        )
        receipt = (
            await _read_cms_receipt(session, schema, relation_oids, verify_names=verify_names)
            if requires_cms_receipt
            else None
        )
        await verify_names()
        session.info["provider_profile_native_authorities"] = authorities_by_family
        session.info["provider_profile_cms_serving_receipt"] = receipt
        token = _SNAPSHOT.set(relation_oids)
        cms_token = _CMS_SERVING.set((schema, authorities_by_family.get("cms-doctors")))
        try:
            yield session
        finally:
            _CMS_SERVING.reset(cms_token)
            _SNAPSHOT.reset(token)
            session.info.pop("provider_profile_native_authorities", None)
            session.info.pop("provider_profile_cms_serving_receipt", None)
