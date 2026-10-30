# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fence selected generic publication heads using an existing native metadata actor."""

from contextlib import asynccontextmanager

from sqlalchemy import text

from db.migration_adoption import _normalized_expression
from db.migration_index_adoption import _shape_from_catalog
from db.migration_index_catalog import INDEX_CATALOG_QUERY
from process.network_address_projection import _identifier
from process.network_membership_writer_closure import _protected_owner
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from process.registry_refresh_anchor_service import _native_context, _read_engine

_TABLES = {
    "provider_directory_api_endpoint": ("endpoint_id",),
    "provider_directory_source": ("source_id", "endpoint_id"),
    "provider_directory_endpoint_dataset": ("dataset_id", "endpoint_id", "status", "is_current"),
}
_CURRENT_INDEX = "provider_directory_endpoint_dataset_current_idx"


def _require(condition):
    if not condition:
        raise RegistryRefreshAnchorUnavailable("registry_refresh_generic_head_fence_unavailable")


def _valid_index(catalog, key, predicate):
    if catalog is None or any(catalog[name] is not True for name in ("indisvalid", "indisready", "indislive")):
        return False
    shape = _shape_from_catalog(dict(catalog))
    return (
        shape.key_columns == (key,)
        and shape.predicate in {_normalized_expression(predicate), _normalized_expression("(" + predicate + ")")}
        and shape.unique
        and shape.immediate
        and shape.access_method == "btree"
        and not (shape.include_columns or shape.expression_keys or shape.exclusion or shape.nulls_not_distinct)
        and shape.operator_classes == ("pg_catalog.text_ops",)
        and shape.sort_options == (0,)
    )


async def _index(session, schema, table, name, key, predicate, owner_oid):
    catalog = (
        (await session.execute(INDEX_CATALOG_QUERY, dict(schema=schema, table_name=table, index_name=name)))
        .mappings()
        .one_or_none()
    )
    _require(_valid_index(catalog, key, predicate))
    owner_matches = await session.scalar(
        text("""SELECT index_relation.relowner=:owner AND index_relation.relkind='i'
        AND ARRAY[CASE WHEN attribute.attcollation=0 THEN ''
          ELSE format('%I.%I',collation_namespace.nspname,native_collation.collname) END]=CAST(:collations AS text[])
        FROM pg_class index_relation JOIN pg_namespace namespace ON namespace.oid=index_relation.relnamespace
        JOIN pg_index descriptor ON descriptor.indexrelid=index_relation.oid
        JOIN pg_class relation ON relation.oid=descriptor.indrelid
        JOIN pg_attribute attribute ON attribute.attrelid=relation.oid AND attribute.attname=:key
        LEFT JOIN pg_collation native_collation ON native_collation.oid=attribute.attcollation
        LEFT JOIN pg_namespace collation_namespace ON collation_namespace.oid=native_collation.collnamespace
        WHERE namespace.nspname=:schema AND index_relation.relname=:name AND relation.relname=:table"""),
        dict(owner=owner_oid, schema=schema, name=name, table=table, key=key, collations=catalog["collations"]),
    )
    _require(owner_matches is True)


async def _relation(session, driver, schema, table, columns):
    native = await driver.fetchrow(
        """SELECT owner.*,relation.oid AS table_oid,relation.relowner AS owner_oid,
        relation.relkind='r' AND relation.relpersistence='p'
          AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity
          AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=relation.oid OR inhparent=relation.oid) AS ordinary,
        has_any_column_privilege(current_user,relation.oid,'UPDATE') AS can_lock,
        (SELECT count(*)=$3::integer AND bool_and(has_column_privilege(current_user,relation.oid,attribute.attname,'SELECT'))
          FROM pg_attribute attribute WHERE attribute.attrelid=relation.oid AND attribute.attnum>0
          AND NOT attribute.attisdropped AND attribute.attname=ANY($4::text[])) AS can_read,
        ARRAY(SELECT index_relation.relname::text FROM pg_index native_index
          JOIN pg_class index_relation ON index_relation.oid=native_index.indexrelid
          WHERE native_index.indrelid=relation.oid AND native_index.indisprimary) AS primary_names
        FROM pg_namespace namespace JOIN pg_class relation ON relation.relnamespace=namespace.oid
        JOIN pg_roles owner ON owner.oid=relation.relowner
        WHERE namespace.nspname=$1 AND relation.relname=$2""",
        schema,
        table,
        len(columns),
        list(columns),
    )
    _require(
        _protected_owner(native)
        and native["ordinary"] is True
        and native["can_lock"] is True
        and native["can_read"] is True
    )
    _require(len(native["primary_names"]) == 1)
    await _index(session, schema, table, native["primary_names"][0], columns[0], "", native["owner_oid"])
    if table == "provider_directory_endpoint_dataset":
        await _index(session, schema, table, _CURRENT_INDEX, "endpoint_id", "is_current = true", native["owner_oid"])


async def _qualify(session, identity, sessions, endpoints):
    from process.registry_membership_refresh_runtime import _qualify_native_actor

    driver, actual = await _native_context(session)
    _require(actual == identity and await driver.fetchval("SHOW transaction_read_only") == "off")
    await _qualify_native_actor(driver, _read_engine(sessions).url.username)
    for schema in sorted({endpoint[0] for endpoint in endpoints}):
        for table, columns in _TABLES.items():
            await _relation(session, driver, schema, table, columns)
    return driver


async def _lock_rows(driver, schema, endpoints):
    namespace = _identifier(schema)
    endpoint_ids = sorted({endpoint[2] for endpoint in endpoints})
    source_by_id = {endpoint[1]: endpoint[2] for endpoint in endpoints}
    _require(len(source_by_id) == len(endpoints))
    actual_endpoints = await driver.fetch(
        f"SELECT endpoint_id FROM {namespace}.provider_directory_api_endpoint WHERE endpoint_id=ANY($1::text[]) "
        'ORDER BY endpoint_id COLLATE "C" FOR SHARE NOWAIT',
        endpoint_ids,
    )
    _require([record["endpoint_id"] for record in actual_endpoints] == endpoint_ids)
    actual_sources = await driver.fetch(
        f"SELECT source_id,endpoint_id FROM {namespace}.provider_directory_source WHERE source_id=ANY($1::text[]) "
        'ORDER BY source_id COLLATE "C" FOR SHARE NOWAIT',
        sorted(source_by_id),
    )
    _require({record["source_id"]: record["endpoint_id"] for record in actual_sources} == source_by_id)
    actual_heads = await driver.fetch(
        f"SELECT endpoint_id,dataset_id FROM {namespace}.provider_directory_endpoint_dataset "
        "WHERE endpoint_id=ANY($1::text[]) AND is_current IS TRUE AND status='published' "
        'ORDER BY endpoint_id COLLATE "C" FOR SHARE NOWAIT',
        endpoint_ids,
    )
    _require([record["endpoint_id"] for record in actual_heads] == endpoint_ids)


@asynccontextmanager
async def generic_refresh_head_fence(readers, endpoints, identity):
    """Hold advisory and exact row locks; protected-owner DDL follows the existing native boundary."""
    if not endpoints:
        yield
        return
    from process.registry_company_approval_fence import _company_approval_connection

    sessions = readers.head_fences.generic_sessions
    _require(sessions is not None and type(endpoints) is tuple and endpoints == tuple(sorted(set(endpoints))))
    async with _company_approval_connection(sessions) as connection:
        async with sessions(bind=connection) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            driver = await _qualify(session, identity, sessions, endpoints)
            for endpoint in sorted({selected[2] for selected in endpoints}):
                acquired = await driver.fetchval(
                    "SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))", endpoint
                )
                _require(acquired is True)
            for schema in sorted({selected[0] for selected in endpoints}):
                await _lock_rows(driver, schema, tuple(selected for selected in endpoints if selected[0] == schema))
            yield
