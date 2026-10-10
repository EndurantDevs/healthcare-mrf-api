# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One request's exact canonical network and immutable address relation."""

import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from urllib.parse import parse_qs
from uuid import UUID

import asyncpg
from sanic.exceptions import InvalidUsage, SanicException, ServiceUnavailable
from sqlalchemy import MetaData, bindparam, func, or_, select, text
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import TextClause

from db.models import EntityAddressUnified

_SCOPE = ContextVar("canonical_network_address_scope", default=None)


@dataclass(frozen=True)
class NetworkAddressReadScope:
    manifest: object
    network_ids: tuple[int, ...]
    address_relation: object
    access_scope_sha256: str | None = None


def current_network_address_scope():
    """Return only the context bound by the trusted request integration."""
    return _SCOPE.get()


@contextmanager
def network_address_read_scope(manifest, network_ids, *, access_scope_sha256=None):
    """Keep SQL and hydration on the same filtered physical address heap."""
    from process.network_serving_read import PinnedNetworkServingManifest

    if type(manifest) is not PinnedNetworkServingManifest or type(network_ids) is not tuple or not network_ids:
        raise ValueError("network_address_scope_invalid")
    if len(network_ids) > 100 or any(type(value) is not int or not 1 <= value <= 2147483647 for value in network_ids):
        raise ValueError("network_address_scope_invalid")
    if network_ids != tuple(sorted(set(network_ids))):
        raise ValueError("network_address_scope_invalid")
    if access_scope_sha256 is not None and (
        type(access_scope_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None
    ):
        raise ValueError("network_access_scope_invalid")
    if manifest.schema_name != "network_candidate_" + UUID(manifest.candidate_id).hex:
        raise ValueError("network_address_scope_invalid")
    physical = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=manifest.schema_name)
    relation = (
        select(physical)
        .where(physical.c.canonical_network_ids.op("&&")(bindparam("_canonical_network_ids", type_=ARRAY(INTEGER))))
        .subquery("entity_address_unified")
    )
    token = _SCOPE.set(NetworkAddressReadScope(manifest, network_ids, relation, access_scope_sha256))
    try:
        yield _SCOPE.get()
    finally:
        _SCOPE.reset(token)


def scoped_address_relation_sql(table_ref):
    """Return a parameterized filtered relation only for the unified table."""
    scope = _SCOPE.get()
    if scope is None or not table_ref.endswith(".entity_address_unified"):
        return table_ref
    return (
        f'(SELECT * FROM "{scope.manifest.schema_name}".entity_address_unified '
        "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[]))"
    )


def is_scoped_address_relation(table_ref):
    """Recognize exactly the filtered relation bound in this request."""
    scope = _SCOPE.get()
    return scope is not None and table_ref == scoped_address_relation_sql("mrf.entity_address_unified")


def scoped_address_parameters(parameters_by_name):
    """Supply the same typed selector to every count, page and raw hydration."""
    scope = _SCOPE.get()
    if scope is None:
        return dict(parameters_by_name)
    if "_canonical_network_ids" in parameters_by_name and parameters_by_name["_canonical_network_ids"] != list(
        scope.network_ids
    ):
        raise ValueError("network_address_reserved_parameter")
    return {**parameters_by_name, "_canonical_network_ids": list(scope.network_ids)}


def scoped_address_statement(statement):
    """Adapt unified-table SQL expressions while retaining mapped-model identity."""
    scope = _SCOPE.get()
    if scope is None or isinstance(statement, TextClause):
        return statement
    if not getattr(statement, "is_select", False):
        raise ValueError("network_address_scope_read_only")
    original = EntityAddressUnified.__table__

    def replace_address(element):
        """Replace only unified-address objects, leaving provider joins intact."""
        if element._deannotate() is original:
            return scope.address_relation
        if getattr(element, "table", None) is original:
            return scope.address_relation.c[element.key]
        return None

    return visitors.replacement_traverse(statement, {}, replace_address)


def scoped_address_cache_key(cache_key):
    """Separate canonical selectors and each retained serving manifest."""
    scope = _SCOPE.get()
    if scope is None:
        return cache_key
    return (
        f"{cache_key}|network:{scope.manifest.manifest_sha256}:{','.join(map(str, scope.network_ids))}"
        f":{scope.access_scope_sha256 or ''}"
    )


def _model_table_columns(model):
    table = getattr(model, "__table__", None)
    if table is None:
        return set()
    return {str(column.key) for column in table.columns if getattr(column, "key", None)}


def _provider_detail_address_type_clause(address_model, table):
    if address_model is EntityAddressUnified:
        return table.c.type.in_(("primary", "secondary", "practice", "site"))
    return or_(table.c.type == "primary", table.c.type == "secondary")


def _npi_batch_address_filters(address_model, address_table, npis):
    if address_model is EntityAddressUnified:
        return [func.coalesce(address_table.c.npi, address_table.c.inferred_npi).in_(npis)]
    return [address_table.c.npi.in_(npis)]


def _request_selector(request, native_args):
    query_string = getattr(request, "query_string", None)
    query_by_name = parse_qs(query_string or "", keep_blank_values=True)
    for field in ("network_ids", "network_generation"):
        if field in query_by_name and len(query_by_name[field]) != 1:
            raise InvalidUsage("Canonical network parameters must occur once.")
    args = (
        native_args
        if native_args is not None
        else (
            {key: query_values[0] for key, query_values in query_by_name.items()}
            if query_string is not None
            else request.args
        )
    )
    absent_selector = object()
    if args.get("network_ids", absent_selector) is absent_selector:
        if args.get("network_generation", absent_selector) is not absent_selector:
            raise InvalidUsage("network_generation requires network_ids.")
        return None
    if any(
        args.get(field, absent_selector) is not absent_selector
        for field in (
            "plan_network",
            "plan_network_checksum",
            "checksum",
            "network_checksum",
            "checksum_network",
            "custom_import",
            "attachment_id",
            "import_name",
            "healthporta_plan_id",
            "plan_id",
            "plan_release_id",
            "plan_version_id",
        )
    ):
        raise InvalidUsage("Choose one explicit network selector namespace.")
    return args


async def _pin_network_scope(request, args):
    from api.control_auth import require_control_auth
    from process.network_serving_read import parse_canonical_network_ids, resolve_network_serving_manifest

    require_control_auth(request)
    try:
        access_scope_sha256 = request.headers.get("X-Network-Access-Scope")
        if access_scope_sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None:
            raise ValueError("network_access_scope_invalid")
        network_ids = parse_canonical_network_ids(args["network_ids"])
        generation = args.get("network_generation")
        if generation is not None:
            if (
                type(generation) is not str
                or not generation.isascii()
                or not generation.isdecimal()
                or str(int(generation)) != generation
            ):
                raise ValueError("network_generation_invalid")
            generation = int(generation)
            if not 1 <= generation <= 9223372036854775807:
                raise ValueError("network_generation_invalid")
    except ValueError as error:
        raise InvalidUsage("Canonical network selector is invalid.") from error
    session = request.ctx.sa_session
    await session.execute(text("SET LOCAL lock_timeout='1s'"))
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    manifest = await resolve_network_serving_manifest(driver, generation_id=generation)
    await session.execute(text(f'LOCK TABLE "{manifest.schema_name}".entity_address_unified IN ACCESS SHARE MODE'))
    return manifest, network_ids, access_scope_sha256


def canonical_network_read(handler):
    """Require trusted selectors and bind one retained heap for the whole handler."""

    @wraps(handler)
    async def wrapped(request, *args, **kwargs):
        """Hold the resolved address heap for this trusted handler invocation."""
        try:
            selector = _request_selector(request, kwargs.get("native_args"))
        except SanicException as refusal:
            refusal.headers = {**(refusal.headers or {}), "Cache-Control": "private, no-store"}
            raise
        if selector is None:
            return await handler(request, *args, **kwargs)
        from process.network_serving_read import NetworkServingReadUnavailable

        try:
            manifest, network_ids, access_scope_sha256 = await _pin_network_scope(request, selector)
        except SanicException as refusal:
            refusal.headers = {**(refusal.headers or {}), "Cache-Control": "private, no-store"}
            raise
        except (NetworkServingReadUnavailable, asyncpg.PostgresError, DBAPIError) as error:
            raise ServiceUnavailable(
                "Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"}
            ) from error
        with network_address_read_scope(manifest, network_ids, access_scope_sha256=access_scope_sha256):
            pinned_headers_by_name = {
                "X-Network-Generation": str(manifest.generation_id),
                "Cache-Control": "private, no-store",
            }
            try:
                http_response = await handler(request, *args, **kwargs)
            except SanicException as error:
                error.headers = {**(error.headers or {}), **pinned_headers_by_name}
                raise
            except (asyncpg.PostgresError, DBAPIError) as error:
                raise ServiceUnavailable(
                    "Canonical network data is unavailable.", headers=pinned_headers_by_name
                ) from error
            http_response.headers.update(pinned_headers_by_name)
            return http_response

    return wrapped
