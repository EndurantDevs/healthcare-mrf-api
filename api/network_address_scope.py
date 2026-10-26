# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""One request's exact canonical network and immutable address relation."""

import hashlib
import json
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from urllib.parse import parse_qs
from uuid import UUID

import asyncpg
from sanic.exceptions import Forbidden, InvalidUsage, SanicException, ServiceUnavailable
from sqlalchemy import MetaData, String, all_, bindparam, cast, func, or_, select, text
from sqlalchemy.dialects.postgresql import ARRAY, INTEGER
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import TextClause

from db.models import EntityAddressUnified
from process.registry_catalog_policy import RegistryOrganizationVisibility

_SCOPE = ContextVar("canonical_network_address_scope", default=None)


@dataclass(frozen=True)
class NetworkAddressReadScope:
    manifest: object
    network_ids: tuple[int, ...] | None
    address_relation: object
    access_scope_sha256: str | None = None
    excluded_network_ids: tuple[int, ...] | None = None
    address_predicate_sql: str | None = None
    organization_policy: RegistryOrganizationVisibility | None = None
    legacy_checksums: tuple[int, ...] | None = None


def current_network_address_scope():
    """Return only the context bound by the trusted request integration."""
    return _SCOPE.get()


def _approved_network_clause(network_id, scope):
    """Use active approved identity at the exact pinned revision, without listing IDs."""
    from db.models.registry_approval import RegistryApprovedRecord
    from db.registry_schema import registry_schema

    network = RegistryApprovedRecord.__table__.to_metadata(MetaData(), schema=registry_schema()).alias("policy_network")
    identity = cast(network_id, String)
    return (
        select(1)
        .select_from(network)
        .where(
            network.c.approved_revision == scope.manifest.approved_custom_revision,
            network.c.record_kind == "network",
            network.c.record_key == identity,
            network.c.record_json["archived"].as_boolean().is_(False),
            func.jsonb_typeof(network.c.record_json["network_id"]) == "number",
            network.c.record_json["network_id"].astext == identity,
            network_id
            != all_(bindparam("_network_excluded_ids", list(scope.excluded_network_ids), type_=ARRAY(INTEGER))),
        )
        .exists()
    )


def _visible_membership_predicate():
    """Require an active approved canonical membership permitted by this policy."""
    from db.registry_schema import registry_schema

    return f"""EXISTS (
      SELECT 1 FROM unnest(canonical_network_ids) AS policy_ids(network_id)
      JOIN "{registry_schema()}".registry_approved_record policy_network
        ON policy_network.record_key=policy_ids.network_id::text
      WHERE policy_network.approved_revision=:_network_approved_revision
        AND policy_network.record_kind='network'
        AND policy_network.record_json->'archived'='false'::jsonb
        AND jsonb_typeof(policy_network.record_json->'network_id')='number'
        AND policy_network.record_json->>'network_id'=policy_ids.network_id::text
        AND NOT policy_ids.network_id=ANY(CAST(:_network_excluded_ids AS integer[])))"""


def _default_address_predicate():
    """Keep public imported offices; custom offices need permitted approved membership."""
    # Publisher masks do not restrict ordinary public contact facts.
    return "row_origin IN ('base','ptg_overlay') OR " + _visible_membership_predicate()


def scoped_address_insurance_predicate():
    """Use visible canonical membership for managed reads and legacy semantics otherwise."""
    scope = current_network_address_scope()
    if scope is None or scope.legacy_checksums is not None:
        return "NOT (plans_network_array @@ '0'::query_int)"
    if scope.network_ids is not None:
        # The trusted scope already authorizes this exact approved selection.
        return "canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])"
    return _visible_membership_predicate()


def scoped_address_aca_insurance_predicate():
    """Require the same visible office membership and an active approved ACA binding."""
    scope = current_network_address_scope()
    if scope is None or scope.legacy_checksums is not None:
        return "NOT (plans_network_array @@ '0'::query_int)"
    from db.registry_schema import registry_schema

    permitted = (
        "policy_ids.network_id=ANY(CAST(:_canonical_network_ids AS INTEGER[]))"
        if scope.network_ids is not None
        else "NOT policy_ids.network_id=ANY(CAST(:_network_excluded_ids AS INTEGER[]))"
    )
    revision = scope.manifest.approved_custom_revision
    return f"""EXISTS (
      SELECT 1 FROM unnest(canonical_network_ids) AS policy_ids(network_id)
      JOIN "{registry_schema()}".registry_approved_record policy_network
        ON policy_network.record_key=policy_ids.network_id::text
      JOIN "{registry_schema()}".registry_approved_record aca_binding
        ON aca_binding.record_json->>'network_id'=policy_ids.network_id::text
      WHERE policy_network.approved_revision={revision}
        AND policy_network.record_kind='network'
        AND policy_network.record_json->'archived'='false'::jsonb
        AND jsonb_typeof(policy_network.record_json->'network_id')='number'
        AND policy_network.record_json->>'network_id'=policy_ids.network_id::text
        AND aca_binding.approved_revision={revision} AND aca_binding.record_kind='network_binding'
        AND aca_binding.record_json->'archived'='false'::jsonb
        AND aca_binding.record_json->>'source_system'='aca'
        AND jsonb_typeof(aca_binding.record_json->'network_id')='number'
        AND jsonb_typeof(aca_binding.record_json->'source_scope_json'->'plan_id')='string'
        AND aca_binding.record_json->'source_scope_json'->>'plan_id'<>''
        AND jsonb_typeof(aca_binding.record_json->'source_scope_json'->'plan_year')='number'
        AND jsonb_typeof(aca_binding.record_json->'source_scope_json'->'issuer_id')='string'
        AND jsonb_typeof(aca_binding.record_json->'source_scope_json'->'state')='string'
        AND aca_binding.record_json->'source_scope_json'->>'state'<>''
        AND jsonb_typeof(aca_binding.record_json->'source_scope_json'->'checksum_network')='number'
        AND {permitted})"""


def scoped_aca_plan_clause(*, plan_id, year, issuer_id, state, checksum_network):
    """Match complete approved ACA plan coordinates in the pinned revision."""
    from db.models.registry_approval import RegistryApprovedRecord
    from db.registry_schema import registry_schema

    scope = current_network_address_scope()
    if scope is None:
        raise ValueError("network_address_scope_required")
    binding = RegistryApprovedRecord.__table__.to_metadata(MetaData(), schema=registry_schema()).alias(
        "approved_aca_binding"
    )
    binding_record_json = binding.c.record_json
    source_scope = binding_record_json["source_scope_json"]
    network_id = binding_record_json["network_id"].astext.cast(INTEGER)
    network_clause = (
        network_id.in_(scope.network_ids)
        if scope.network_ids is not None
        else _approved_network_clause(network_id, scope)
    )
    return (
        select(1)
        .select_from(binding)
        .where(
            binding.c.approved_revision == scope.manifest.approved_custom_revision,
            binding.c.record_kind == "network_binding",
            binding_record_json["archived"].as_boolean().is_(False),
            binding_record_json["source_system"].astext == "aca",
            network_clause,
            source_scope["plan_id"].astext == plan_id,
            source_scope["plan_year"].astext.cast(INTEGER) == year,
            source_scope["issuer_id"].astext.cast(INTEGER) == issuer_id,
            source_scope["state"].astext == state,
            source_scope["checksum_network"].astext.cast(INTEGER) == checksum_network,
        )
        .exists()
    )


def _validate_network_scope_inputs(
    manifest, network_ids, access_scope_sha256, excluded_network_ids, organization_policy
):
    """Validate selectors and independent visibility before installing address scope."""
    from process.network_serving_read import PinnedNetworkServingManifest

    if type(manifest) is not PinnedNetworkServingManifest:
        raise ValueError("network_address_scope_invalid")
    if organization_policy is not None and type(organization_policy) is not RegistryOrganizationVisibility:
        raise ValueError("network_address_scope_invalid")
    if network_ids is None:
        from process.registry_network_catalog_read import _exclusions

        try:
            _exclusions(excluded_network_ids)
        except ValueError as error:
            raise ValueError("network_address_scope_invalid") from error
        if (
            type(manifest.approved_custom_revision) is not int
            or not 0 <= manifest.approved_custom_revision <= 9223372036854775807
        ):
            raise ValueError("network_address_scope_invalid")
        if access_scope_sha256 is None:
            raise ValueError("network_access_scope_invalid")
    elif (
        type(network_ids) is not tuple
        or not network_ids
        or len(network_ids) > 100
        or any(type(network_id) is not int or not 1 <= network_id <= 2147483647 for network_id in network_ids)
        or network_ids != tuple(sorted(set(network_ids)))
        or excluded_network_ids is not None
    ):
        raise ValueError("network_address_scope_invalid")
    if access_scope_sha256 is not None and (
        type(access_scope_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None
    ):
        raise ValueError("network_access_scope_invalid")
    if manifest.schema_name != "network_candidate_" + UUID(manifest.candidate_id).hex:
        raise ValueError("network_address_scope_invalid")


@contextmanager
def network_address_read_scope(
    manifest,
    network_ids,
    *,
    access_scope_sha256=None,
    excluded_network_ids=None,
    organization_policy=None,
    legacy_checksums=None,
):
    """Keep SQL and hydration on the same filtered physical address heap."""
    _validate_network_scope_inputs(
        manifest, network_ids, access_scope_sha256, excluded_network_ids, organization_policy
    )
    if legacy_checksums is not None and (
        network_ids is None
        or type(legacy_checksums) is not tuple
        or not 1 <= len(legacy_checksums) <= 50
        or any(type(checksum) is not int or not -2147483648 <= checksum <= 2147483647 for checksum in legacy_checksums)
        or legacy_checksums != tuple(sorted(set(legacy_checksums)))
    ):
        raise ValueError("network_legacy_scope_invalid")
    physical = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=manifest.schema_name)
    predicate_sql = _default_address_predicate() if network_ids is None else None
    predicate = (
        text(predicate_sql)
        if predicate_sql is not None
        else physical.c.canonical_network_ids.op("&&")(bindparam("_canonical_network_ids", type_=ARRAY(INTEGER)))
    )
    if legacy_checksums is not None:
        predicate = physical.c.plans_network_array.op("&&")(
            bindparam("_legacy_network_checksums", type_=ARRAY(INTEGER))
        )
    relation = select(physical).where(predicate).subquery("entity_address_unified")
    token = _SCOPE.set(
        NetworkAddressReadScope(
            manifest,
            network_ids,
            relation,
            access_scope_sha256,
            excluded_network_ids,
            predicate_sql,
            organization_policy,
            legacy_checksums,
        )
    )
    try:
        yield _SCOPE.get()
    finally:
        _SCOPE.reset(token)


def scoped_address_relation_sql(table_ref):
    """Return a parameterized filtered relation only for the unified table."""
    scope = _SCOPE.get()
    if scope is None or not table_ref.endswith(".entity_address_unified"):
        return table_ref
    if scope.network_ids is None:
        return (
            f'(SELECT * FROM "{scope.manifest.schema_name}".entity_address_unified WHERE {scope.address_predicate_sql})'
        )
    predicate = (
        "plans_network_array && CAST(:_legacy_network_checksums AS INTEGER[])"
        if scope.legacy_checksums is not None
        else "canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])"
    )
    return f'(SELECT * FROM "{scope.manifest.schema_name}".entity_address_unified WHERE {predicate})'


def is_scoped_address_relation(table_ref):
    """Recognize exactly the filtered relation bound in this request."""
    scope = _SCOPE.get()
    return scope is not None and table_ref == scoped_address_relation_sql("mrf.entity_address_unified")


def _is_reserved_parameter_equal(provided, expected):
    return (
        type(provided) is type(expected)
        and provided == expected
        and (type(expected) is not list or all(type(network_id) is int for network_id in provided))
    )


def scoped_address_parameters(parameters_by_name):
    """Supply the same typed selector to every count, page and raw hydration."""
    scope = _SCOPE.get()
    if scope is None:
        return dict(parameters_by_name)
    legacy_parameters = (
        {"_legacy_network_checksums": list(scope.legacy_checksums)} if scope.legacy_checksums is not None else {}
    )
    if "_legacy_network_checksums" in parameters_by_name and (
        "_legacy_network_checksums" not in legacy_parameters
        or not _is_reserved_parameter_equal(
            parameters_by_name["_legacy_network_checksums"], legacy_parameters["_legacy_network_checksums"]
        )
    ):
        raise ValueError("network_address_reserved_parameter")
    if scope.network_ids is None:
        reserved_by_name = {
            "_network_excluded_ids": list(scope.excluded_network_ids),
            "_network_approved_revision": scope.manifest.approved_custom_revision,
        }
        if "_canonical_network_ids" in parameters_by_name or any(
            key in parameters_by_name and not _is_reserved_parameter_equal(parameters_by_name[key], expected)
            for key, expected in reserved_by_name.items()
        ):
            raise ValueError("network_address_reserved_parameter")
        return {**parameters_by_name, **reserved_by_name}
    if any(key in parameters_by_name for key in ("_network_excluded_ids", "_network_approved_revision")):
        raise ValueError("network_address_reserved_parameter")
    if "_canonical_network_ids" in parameters_by_name and parameters_by_name["_canonical_network_ids"] != list(
        scope.network_ids
    ):
        raise ValueError("network_address_reserved_parameter")
    return {**parameters_by_name, "_canonical_network_ids": list(scope.network_ids), **legacy_parameters}


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
    if scope.network_ids is None:
        excluded_digest = hashlib.sha256(
            json.dumps(scope.excluded_network_ids, separators=(",", ":")).encode()
        ).hexdigest()
        return (
            f"{cache_key}|network:{scope.manifest.manifest_sha256}:default:"
            f"{scope.manifest.approved_custom_revision}:{excluded_digest}:{scope.access_scope_sha256}"
        )
    return (
        f"{cache_key}|network:{scope.manifest.manifest_sha256}:{','.join(map(str, scope.network_ids))}"
        f":{scope.access_scope_sha256 or ''}"
        + (f":checksums:{','.join(map(str, scope.legacy_checksums))}" if scope.legacy_checksums is not None else "")
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
        if args.get("network_generation", absent_selector) is not absent_selector or (
            native_args is None and getattr(request, "headers", {}).get("X-Network-Legacy-Checksums") is not None
        ):
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


def _request_legacy_checksums(request, args):
    """Accept a bounded exact checksum scope only beside trusted pinned authorization."""
    from process.registry_network_catalog_read import RegistryNetworkCatalogLegacyBatchSelector

    raw = request.headers.get("X-Network-Legacy-Checksums")
    if raw is None:
        return None
    if (
        len(request.headers.getall("X-Network-Legacy-Checksums")) != 1
        or len(raw) > 600
        or "network_generation" not in args
        or request.headers.get("X-Network-Access-Scope") is None
    ):
        raise ValueError("network_legacy_scope_invalid")
    selector = RegistryNetworkCatalogLegacyBatchSelector("checksum_network", tuple(raw.split(",")))
    checksums = tuple(int(value) for value in selector.values)
    if checksums != tuple(sorted(checksums)):
        raise ValueError("network_legacy_scope_invalid")
    return checksums


async def _pin_network_scope(request, args):
    from api.control_auth import require_control_auth
    from process.network_serving_read import parse_canonical_network_ids, resolve_network_serving_manifest

    require_control_auth(request)
    try:
        access_scope_sha256 = request.headers.get("X-Network-Access-Scope")
        if access_scope_sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", access_scope_sha256) is None:
            raise ValueError("network_access_scope_invalid")
        network_ids = parse_canonical_network_ids(args["network_ids"])
        legacy_checksums = _request_legacy_checksums(request, args)
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
    if legacy_checksums is not None:
        from db.registry_schema import registry_schema
        from process.network_address_projection import _identifier
        from process.registry_network_catalog_read import _LEGACY_BATCH_SQL

        mapping = await driver.fetchrow(
            _LEGACY_BATCH_SQL.format(control=_identifier(registry_schema())),
            manifest.approved_custom_revision,
            tuple(map(str, legacy_checksums)),
            "checksum_network",
            (),
        )
        if mapping is None or mapping["complete"] is not True or mapping["network_ids"] != list(network_ids):
            raise Forbidden("Legacy network scope does not match its authorized canonical identities.")
    return manifest, network_ids, access_scope_sha256, legacy_checksums


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
            manifest, network_ids, access_scope_sha256, legacy_checksums = await _pin_network_scope(request, selector)
        except SanicException as refusal:
            refusal.headers = {**(refusal.headers or {}), "Cache-Control": "private, no-store"}
            raise
        except (NetworkServingReadUnavailable, asyncpg.PostgresError, DBAPIError) as error:
            raise ServiceUnavailable(
                "Canonical network data is unavailable.", headers={"Cache-Control": "private, no-store"}
            ) from error
        with network_address_read_scope(
            manifest, network_ids, access_scope_sha256=access_scope_sha256, legacy_checksums=legacy_checksums
        ):
            pinned_headers_by_name = {
                "X-Network-Generation": str(manifest.generation_id),
                "Cache-Control": "private, no-store",
            }
            if legacy_checksums is not None:
                pinned_headers_by_name["X-Network-Legacy-Checksums"] = ",".join(map(str, legacy_checksums))
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
