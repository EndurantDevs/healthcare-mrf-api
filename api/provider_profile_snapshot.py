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

PROFILE_CANONICAL_TARGETS_SQL = """jsonb_build_object(
        'profile', jsonb_build_object(
            'schema', (parse_ident(:profile_table_ref))[1],
            'name', (parse_ident(:profile_table_ref))[2]),
        'evidence', jsonb_build_object(
            'schema', (parse_ident(:evidence_table_ref))[1],
            'name', (parse_ident(:evidence_table_ref))[2]))"""


async def profile_serving_targets_sql(execute, table_ref, *, session):
    """Read both metadata editions while a granted common-column read pins the table."""
    await execute(text(f"SELECT generation_id FROM {table_ref} WHERE FALSE"), session=session)
    result = await execute(
        text(
            "SELECT attname FROM pg_catalog.pg_attribute WHERE attrelid=to_regclass(:table_ref) "
            "AND attnum>0 AND NOT attisdropped "
            "AND attname IN ('targets_json','profile_target_oid','evidence_target_oid')"
        ),
        session=session,
        params={"table_ref": table_ref},
    )
    fields = {row[0] for row in result.all()}
    if fields == {"targets_json"}:
        return "targets_json"
    if fields != {"profile_target_oid", "evidence_target_oid"}:
        raise RuntimeError("provider_directory_profile_serving_metadata_invalid")
    # Earlier publishers use these fixed canonical names; their opaque tokens are not read.
    return PROFILE_CANONICAL_TARGETS_SQL + " AS targets_json"


_SNAPSHOT = ContextVar("provider_profile_read_snapshot", default=None)
_GEO_BINDINGS = ContextVar("provider_geo_dependency_bindings", default=None)
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
    "entity_address_geo_assurance_state",
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


async def _inventory_relation_names(session, schema, table_names, *, locked_names=None):
    """Inspect logical shape only after named locks protect all present tables."""
    if locked_names is None:
        locked_names = await lock_reader_relations(session, ((schema, name) for name in table_names))
    result = await session.execute(text(_SNAPSHOT_RELATIONS_SQL), {"schema": schema, "names": list(table_names)})
    relations_by_name = {}
    for name, present, kind, direct, partition_root in result.all():
        qualified = f"{schema}.{name}"
        if bool(present) != (locked_names.get(qualified) is not None):
            raise _SnapshotSetupChanged("Provider data changed during the read; retry the request.")
        if present and not (kind == "r" and direct is True):
            if name not in _DIRECTORY_PARTITION_ROOTS or kind != "p" or partition_root is not True:
                raise ServiceUnavailable("Provider data is temporarily unavailable.")
        relations_by_name[qualified] = bool(present and locked_names.get(qualified) is not False)
    if set(relations_by_name) != {f"{schema}.{name}" for name in table_names}:
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
    return relations_by_name


_SNAPSHOT_RELATIONS_SQL = """SELECT name, c.relname IS NOT NULL AS present, c.relkind::text AS relkind,
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


_DIRECTORY_PARTITION_ROOTS = (
    "provider_directory_dataset_affiliation_organization",
    "provider_directory_dataset_insurance_plan",
    "provider_directory_dataset_network_plan",
    "provider_directory_dataset_resource",
)


class _SnapshotSetupChanged(ServiceUnavailable):
    """A cutover renamed serving tables after this snapshot began; setup retries with a fresh snapshot."""


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
            if authority.serving_generation is not None:
                _require_named_relations(schema, _DOCTORS_TABLES, relation_availability)
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
            if authority.serving_generation is not None:
                _require_named_relations(schema, address_generation.RELATION_NAMES, relation_availability)
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
    from api.ptg2_geo_policy import provider_address_geo_references

    async with (
        _provider_read_snapshot(database, schema, include_detail=True, table_names=_GEO_TABLES) as session,
        provider_address_geo_references(session, schema_name=schema),
    ):
        yield session


@asynccontextmanager
async def _provider_read_snapshot(database, schema, *, include_detail, table_names=None):
    """Share setup-only retry and exact Reader ownership across fixed projections."""
    if not isinstance(schema, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
        raise ValueError("provider_profile_schema_invalid")
    is_new_reader = not database.has_reader_session()
    max_attempts = 1 if not is_new_reader or database._transaction_binding() is not None else 3
    selected_names = table_names or tuple(sorted(set((*_COMMON_TABLES, *(_DETAIL_TABLES if include_detail else ())))))
    scope_options_by_name = {"include_detail": include_detail, "table_names": selected_names}
    for attempt in range(1, max_attempts + 1):
        has_yielded = False
        session = None
        try:
            async with database.reader_session() as session:
                await _prepare_provider_reader(session, schema, fresh=is_new_reader, table_names=selected_names)
                async with _read_snapshot_scope(session, schema, **scope_options_by_name):
                    has_yielded = True
                    yield session
            return
        except _SnapshotSetupChanged:
            if has_yielded or attempt == max_attempts:
                raise
        except DBAPIError as error:
            if not has_yielded and attempt < max_attempts and postgres_sqlstate(error) == "55P03":
                continue
            raise ServiceUnavailable("Provider data is temporarily unavailable.") from error


@asynccontextmanager
async def _read_snapshot_scope(session, schema, *, include_detail, table_names=None):
    """Finish logical setup before yielding; loader failures never replay a response."""
    if not session.info.get("api_reader_verified"):
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    selected_names = table_names or tuple(sorted(set((*_COMMON_TABLES, *(_DETAIL_TABLES if include_detail else ())))))
    pinned = session.info.get("provider_profile_relations")
    if pinned is None:
        relation_names = await _inventory_relation_names(session, schema, selected_names)
    else:
        if any(f"{schema}.{name}" not in pinned for name in selected_names):
            raise ServiceUnavailable("Provider data is temporarily unavailable.")
        relation_names = await _inventory_relation_names(session, schema, selected_names, locked_names=pinned)
    requires_cms_receipt = await _requires_cms_receipt(session, schema, relation_names)
    authorities_by_family = await _read_native_authorities(
        session, schema, relation_names, include_address=include_detail or requires_cms_receipt
    )
    receipt = await _read_cms_receipt(session, schema, relation_names) if requires_cms_receipt else None
    geo_bindings = None
    if relation_names.get(f"{schema}.entity_address_geo_assurance_state"):
        from process.entity_address_dependency_bindings import read_active_bindings

        geo_bindings = await read_active_bindings(session, schema)
    previous_authorities = session.info.get("provider_profile_native_authorities")
    previous_receipt = session.info.get("provider_profile_cms_serving_receipt")
    session.info["provider_profile_native_authorities"] = authorities_by_family
    session.info["provider_profile_cms_serving_receipt"] = receipt
    token = _SNAPSHOT.set(relation_names)
    cms_token = _CMS_SERVING.set((schema, authorities_by_family.get("cms-doctors")))
    geo_token = _GEO_BINDINGS.set((schema, geo_bindings))
    try:
        yield session
    finally:
        _GEO_BINDINGS.reset(geo_token)
        _CMS_SERVING.reset(cms_token)
        _SNAPSHOT.reset(token)
        for key, previous in (
            ("provider_profile_native_authorities", previous_authorities),
            ("provider_profile_cms_serving_receipt", previous_receipt),
        ):
            if previous is None:
                session.info.pop(key, None)
            else:
                session.info[key] = previous


def snapshot_geo_dependency_bindings(schema):
    """Reuse the active named input map read in this response's snapshot."""
    selected = _GEO_BINDINGS.get()
    return selected[1] if selected is not None and selected[0] == schema else None


def provider_snapshot_names(schema):
    """Return the fixed ordinary provider family before its first data snapshot."""
    return tuple((schema, name) for name in sorted(set((*_COMMON_TABLES, *_DETAIL_TABLES, *_GEO_TABLES))))


async def lock_reader_relations(session, relation_names):
    """Lock optional names before SELECT can freeze a repeatable-read snapshot."""
    relations_by_name = {}
    await session.execute(text("SET LOCAL lock_timeout = '250ms'"))
    await session.execute(text("SET LOCAL statement_timeout = '5s'"))
    relation_names = tuple(sorted(set(relation_names)))
    for schema, name in relation_names:
        if not all(
            isinstance(identifier, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", identifier)
            for identifier in (schema, name)
        ):
            raise ValueError("provider_profile_relation_invalid")
    pending = [relation_names] if relation_names else []
    while pending:
        batch = pending.pop()
        qualified_names = ", ".join(f'"{schema}"."{name}"' for schema, name in batch)
        try:
            async with session.begin_nested():
                await session.execute(text(f"LOCK TABLE {qualified_names} IN ACCESS SHARE MODE"))
        except DBAPIError as error:
            state = postgres_sqlstate(error)
            is_missing = state in {"42P01", "3F000"}
            if not is_missing and not (state == "42501" and any(name == "npi_result_generation" for _, name in batch)):
                raise
            if len(batch) == 1:
                schema, name = batch[0]
                relations_by_name[f"{schema}.{name}"] = None if is_missing else False
            else:
                # Isolate the failed names without a catalog read or changing lock order.
                middle = len(batch) // 2
                pending.extend((batch[middle:], batch[:middle]))
        else:
            relations_by_name.update((f"{schema}.{name}", f"{schema}.{name}") for schema, name in batch)
    return relations_by_name


async def _prepare_provider_reader(session, schema, *, fresh=False, table_names=None):
    """Pin a fresh Reader or reuse names locked by request middleware."""
    if "provider_profile_relations" in session.info:
        return
    if not fresh and session.in_transaction():
        raise ServiceUnavailable("Provider reads require a fresh or already pinned snapshot.")
    relation_names = (
        provider_snapshot_names(schema) if table_names is None else ((schema, name) for name in table_names)
    )
    session.info["provider_profile_relations"] = await lock_reader_relations(session, relation_names)


def _require_named_relations(schema, table_names, relation_availability):
    """Require the complete published family by its locked qualified names."""
    if any(not relation_availability.get(f"{schema}.{name}") for name in table_names):
        raise ServiceUnavailable("Provider data is temporarily unavailable.")
