# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep one provider response on stable serving relations and one read snapshot."""

import re
from contextlib import asynccontextmanager, nullcontext
from contextvars import ContextVar

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import entity_address_result_generation as address_generation
from process import provider_directory_cms_serving_receipt as cms_serving_receipt
from process import reference_family_result_generation as reference_generation
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


def snapshot_relation_available(table_ref):
    """Use the pinned optional-table inventory instead of a process-wide cache."""
    snapshot = _SNAPSHOT.get()
    if snapshot is None or table_ref not in snapshot:
        return None
    return snapshot[table_ref] is not None


def snapshot_cms_serving_generation(schema):
    """Expose only the Doctors authority validated against this read's locked relations."""
    serving = _CMS_SERVING.get()
    if serving is None:
        return None
    snapshot_schema, authority = serving
    if schema != snapshot_schema:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return authority.serving_generation.as_dict() if authority and authority.serving_generation else None


def provider_read_savepoint(session):
    """Keep optional read failures from aborting a caller's remaining reads."""
    return session.begin_nested() if session is not None else nullcontext()


async def _relation_oids(session, schema, table_names):
    result = await session.execute(
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
    result = await session.execute(
        text(
            "SELECT name, c.oid::bigint FROM unnest(CAST(:names AS text[])) AS names(name) "
            "LEFT JOIN pg_namespace n ON n.nspname=:schema "
            "LEFT JOIN pg_class c ON c.relnamespace=n.oid AND c.relname=name"
        ),
        {"schema": schema, "names": list(table_names)},
    )
    return {f"{schema}.{name}": oid for name, oid in result.all()}


async def _lock_serving_relations(session, schema, *, include_detail):
    """Use the shared lexical lock order and reject a stale repeatable-read catalog."""
    table_names = tuple(
        sorted(
            set(
                (
                    *_DOCTORS_TABLES,
                    *_PROFILE_TABLES,
                    "provider_directory_address_overlay",
                    *address_generation.RELATION_NAMES,
                    *(_DETAIL_TABLES if include_detail else ()),
                    _PROFILE_GENERATION_TABLE,
                    reference_generation.TABLE_NAME,
                    address_generation.TABLE_NAME,
                    _CMS_RECEIPT_TABLE,
                )
            )
        )
    )
    relation_oids = await _snapshot_relation_oids(session, schema, table_names)
    installed_names = [name for name in table_names if relation_oids[f"{schema}.{name}"] is not None]
    if installed_names:
        qualified_names = ", ".join(f'"{schema}"."{name}"' for name in installed_names)
        await session.execute(text(f"LOCK TABLE {qualified_names} IN ACCESS SHARE MODE"))
    if await _relation_oids(session, schema, table_names) != relation_oids:
        raise _SnapshotSetupChanged("Provider data changed during the read; retry the request.")
    return relation_oids


async def _read_native_authorities(session, schema, relation_oids, *, include_address):
    """Check installed native authorities after locking their canonical relations."""
    authorities_by_family = {}
    if relation_oids[f"{schema}.{reference_generation.TABLE_NAME}"] is not None:
        has_doctors = await session.scalar(
            text(
                f'SELECT EXISTS (SELECT 1 FROM "{schema}".{reference_generation.TABLE_NAME} '
                "WHERE importer_id='cms-doctors')"
            )
        )
        if has_doctors:
            authority = await reference_generation.read_reference_family_result_generation_authority(
                session,
                importer_id="cms-doctors",
                schema_name=schema,
            )
            authorities_by_family["cms-doctors"] = authority
            _require_matching_relations(authority, schema, _DOCTORS_TABLES, relation_oids)
    if include_address and relation_oids[f"{schema}.{address_generation.TABLE_NAME}"] is not None:
        has_address_authority = await session.scalar(
            text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{address_generation.TABLE_NAME})')
        )
        if has_address_authority:
            authority = await address_generation.read_entity_address_result_generation_authority(
                session,
                schema_name=schema,
            )
            authorities_by_family["entity-address"] = authority
            _require_matching_relations(authority, schema, address_generation.RELATION_NAMES, relation_oids)
    return authorities_by_family


async def _requires_cms_receipt(session, schema, relation_oids):
    """Keep withdrawals guarded by receipt history without treating legacy Doctors as CMS NPD."""
    if relation_oids[f"{schema}.{_CMS_RECEIPT_TABLE}"] is not None and await session.scalar(
        text(f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_CMS_RECEIPT_TABLE})')
    ):
        return True
    if relation_oids[f"{schema}.{_PROFILE_GENERATION_TABLE}"] is not None:
        return bool(
            await session.scalar(
                text(
                    f'SELECT EXISTS (SELECT 1 FROM "{schema}".{_PROFILE_GENERATION_TABLE} '
                    "WHERE singleton_key='global' AND source_vector_json @> '[{\"source_id\":\"cms-npd\"}]'::jsonb)"
                )
            )
        )
    return False


async def _read_cms_receipt(session, schema, relation_oids):
    """Accept only the immutable proof of the locked native serving relations."""
    receipt = None
    if relation_oids[f"{schema}.{_CMS_RECEIPT_TABLE}"] is not None:
        receipt = await cms_serving_receipt.read_serving_receipt(session, schema)
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
    if not isinstance(schema, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise ValueError("provider_profile_schema_invalid")
    max_attempts = 1 if database._transaction_binding() is not None else 3
    for attempt in range(1, max_attempts + 1):
        has_yielded = False
        try:
            async with database.transaction() as session:
                async with _read_snapshot_scope(session, schema, include_detail=include_detail):
                    has_yielded = True
                    yield session
            return
        except _SnapshotSetupChanged:
            if has_yielded or attempt == max_attempts:
                raise
        except DBAPIError as error:
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error


@asynccontextmanager
async def _read_snapshot_scope(session, schema, *, include_detail):
    """Finish setup before yielding; loader failures never replay a response."""
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
    await session.execute(text("SET LOCAL statement_timeout = '5s'"))
    relation_oids = await _lock_serving_relations(session, schema, include_detail=include_detail)
    requires_cms_receipt = await _requires_cms_receipt(session, schema, relation_oids)
    authorities_by_family = await _read_native_authorities(
        session,
        schema,
        relation_oids,
        include_address=include_detail or requires_cms_receipt,
    )
    receipt = await _read_cms_receipt(session, schema, relation_oids) if requires_cms_receipt else None
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
