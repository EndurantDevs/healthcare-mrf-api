# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Choose the published all-address geo search projection for one nearby read."""

from __future__ import annotations

import logging
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_geo_search import (
    BUILD_LOCK_ID,
    ENTITY_TYPE_COLUMN,
    LAYOUT,
    LEGACY_LAYOUT,
    LEGACY_SEARCH_COLUMNS,
    RETIRE_LOCK_ID,
    SEARCH_COLUMNS,
    STATE_TABLE,
    TABLE_NAME_PATTERN,
    geo_entity_type_column_available,
    input_keys_sql,
)

logger = logging.getLogger(__name__)
_COLUMN_REFERENCE = re.compile(r'(?<!\w)"?a"?\s*\.\s*(?:"([^"]+)"|([a-z_][a-z0-9_]*))', re.IGNORECASE)
_stale_tables_logged: set[str] = set()
# The builder holds its session lock for a whole rebuild; pg_locks shows the 64-bit key as two halves.
_REBUILDING = (
    "EXISTS (SELECT 1 FROM pg_catalog.pg_locks AS held "
    "WHERE held.locktype = 'advisory' AND held.granted AND held.objsubid = 1 "
    f"AND held.classid = CAST('{BUILD_LOCK_ID >> 32}' AS oid) "
    f"AND held.objid = CAST('{BUILD_LOCK_ID & 0xFFFFFFFF}' AS oid) "
    "AND held.database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()))"
)


@dataclass(frozen=True, slots=True)
class ProviderGeoSearch:
    """The published projection generation a read uses: built from the published inputs, or still serving
    while its replacement is being built."""

    table_sql: str
    layout: int = LAYOUT
    has_entity_type_code: bool = False


async def current_provider_geo_search(connection, schema: str, *, include_service_locations: bool):
    """Return the current generation while it matches the published inputs or its replacement is being built.

    During a rebuild the previous generation lags the newly published addresses or providers by that one
    publication. Without a running rebuild a generation that no longer matches is not used (address table).
    """
    state = storage_relation(schema, STATE_TABLE)
    # The pin comes first and is held for the read's transaction, so no table checked below can be retired before
    # the read uses it. A missing table or grant must fall back here: an error would abort the transaction.
    readable_rows = await connection.all(
        text(
            "SELECT pg_try_advisory_xact_lock_shared(:retire_lock) "
            "AND COALESCE(has_table_privilege(to_regclass(:state), 'SELECT') "
            "AND has_table_privilege(to_regclass(:address_generation), 'SELECT') "
            "AND has_table_privilege(to_regclass(:npi_generation), 'SELECT'), false)"
        ),
        retire_lock=RETIRE_LOCK_ID,
        state=state,
        address_generation=storage_relation(schema, "entity_address_result_generation"),
        npi_generation=storage_relation(schema, "npi_result_generation"),
    )
    if not readable_rows or readable_rows[0][0] is not True:
        return None
    address_sql, npi_sql = input_keys_sql(schema)
    relation = "to_regclass(format('%I.%I', CAST(:schema AS text), s.table_name))"
    current_rows = await connection.all(
        text(
            f"""SELECT s.table_name, s.layout,
                       s.layout IN (:layout, :legacy_layout)
                       AND s.include_service_locations = :include_service_locations
                       AND COALESCE(has_table_privilege({relation}, 'SELECT'), false)
                       AND ((s.address_input = ({address_sql}) AND s.npi_input = ({npi_sql}))
                            OR (({address_sql}) IS NOT NULL AND ({npi_sql}) IS NOT NULL AND {_REBUILDING}))
                       AS is_servable,
                       {geo_entity_type_column_available(relation)} AS has_entity_type_code
                  FROM {state} AS s
                 WHERE s.is_current"""
        ),
        include_service_locations=include_service_locations,
        schema=schema,
        layout=LAYOUT,
        legacy_layout=LEGACY_LAYOUT,
    )
    if not current_rows:
        return None
    table_name, layout, is_servable, has_entity_type_code = current_rows[0]
    if not is_servable:
        if table_name not in _stale_tables_logged:
            _stale_tables_logged.add(table_name)
            logger.warning(
                "Provider geo search %s is not readable for the published inputs; reading addresses", table_name
            )
        return None
    if not isinstance(table_name, str) or not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError("provider_geo_search_table_invalid")
    return ProviderGeoSearch(
        f"{storage_identifier(schema)}.{table_name}", layout, layout == LAYOUT and has_entity_type_code
    )


def provider_geo_search_sql(search: ProviderGeoSearch | None, *clauses: str) -> str | None:
    """Use the projection only when every address-column predicate is one of its columns."""
    if search is None:
        return None
    used_columns = {quoted or unquoted.lower() for quoted, unquoted in _COLUMN_REFERENCE.findall(" ".join(clauses))}
    available_columns = SEARCH_COLUMNS if search.layout == LAYOUT else LEGACY_SEARCH_COLUMNS
    if search.has_entity_type_code:
        available_columns |= {ENTITY_TYPE_COLUMN}
    return search.table_sql if used_columns <= available_columns else None


async def request_geo_search(connection_scope, address_table_sql: str, **lookup):
    """Look up the projection for a read without a network scope on the unified address table."""
    from api.network_address_scope import current_network_address_scope
    from api.provider_list_sql import _is_unified_address_table

    if current_network_address_scope() is not None or not _is_unified_address_table(address_table_sql):
        return None
    async with connection_scope as connection:
        return await current_provider_geo_search(connection, **lookup)


def nearby_search_fields(network_search_sql, search: ProviderGeoSearch | None, *clauses: str) -> dict[str, object]:
    """Prefer the selected-network projection, then the all-address projection, else the address table."""
    search_table_sql = None if network_search_sql else provider_geo_search_sql(search, *clauses)
    if search is None or search_table_sql is None:
        return {"search_table_sql": network_search_sql}
    return {
        "search_table_sql": search_table_sql,
        "search_provider_facts": True,
        "search_has_entity_type_code": search.has_entity_type_code,
    }


@asynccontextmanager
async def nearest_first_pages(connection, search: ProviderGeoSearch | bool | None):
    """Read pages nearest-first; underestimated radius rows make a bitmap scan plus sort look cheaper."""
    if not search:
        yield
        return
    previous = (await connection.all(text("SELECT current_setting('enable_bitmapscan')")))[0][0]
    await connection.all(text("SELECT set_config('enable_bitmapscan', 'off', true)"))
    try:
        yield
    finally:
        await connection.all(text("SELECT set_config('enable_bitmapscan', :previous, true)"), previous=previous)
