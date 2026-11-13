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
    LAYOUT,
    RETIRE_LOCK_ID,
    SEARCH_COLUMNS,
    STATE_TABLE,
    TABLE_NAME_PATTERN,
    input_keys_sql,
)

logger = logging.getLogger(__name__)
_COLUMN_REFERENCE = re.compile(r'(?<!\w)"?a"?\s*\.\s*(?:"([^"]+)"|([a-z_][a-z0-9_]*))', re.IGNORECASE)
_stale_tables_logged: set[str] = set()


@dataclass(frozen=True, slots=True)
class ProviderGeoSearch:
    """One published projection generation whose inputs match the read's address and NPI generations."""

    table_sql: str


async def current_provider_geo_search(connection, schema: str, *, include_service_locations: bool):
    """Return the current generation when it was built from the published inputs, else None (address table)."""
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
    current_rows = await connection.all(
        text(
            f"""SELECT s.table_name,
                       s.layout = :layout AND s.include_service_locations = :include_service_locations
                       AND s.address_input = ({address_sql}) AND s.npi_input = ({npi_sql})
                       AND COALESCE(has_table_privilege(
                           to_regclass(format('%I.%I', CAST(:schema AS text), s.table_name)), 'SELECT'), false)
                       AS is_fresh
                  FROM {state} AS s
                 WHERE s.is_current"""
        ),
        include_service_locations=include_service_locations,
        schema=schema,
        layout=LAYOUT,
    )
    if not current_rows:
        return None
    table_name, is_fresh = current_rows[0][0], current_rows[0][1]
    if not is_fresh:
        if table_name not in _stale_tables_logged:
            _stale_tables_logged.add(table_name)
            logger.warning(
                "Provider geo search %s is not readable for the published inputs; reading addresses", table_name
            )
        return None
    if not isinstance(table_name, str) or not TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError("provider_geo_search_table_invalid")
    return ProviderGeoSearch(f"{storage_identifier(schema)}.{table_name}")


def provider_geo_search_sql(search: ProviderGeoSearch | None, *clauses: str) -> str | None:
    """Use the projection only when every address-column predicate is one of its columns."""
    if search is None:
        return None
    used_columns = {quoted or unquoted.lower() for quoted, unquoted in _COLUMN_REFERENCE.findall(" ".join(clauses))}
    return search.table_sql if used_columns <= SEARCH_COLUMNS else None


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
    if search_table_sql is None:
        return {"search_table_sql": network_search_sql}
    return {"search_table_sql": search_table_sql, "search_provider_facts": True}


@asynccontextmanager
async def nearest_first_pages(connection, search: ProviderGeoSearch | None):
    """Read projection pages nearest-first; underestimated radius rows make a bitmap scan plus sort look cheaper."""
    if search is None:
        yield
        return
    previous = (await connection.all(text("SELECT current_setting('enable_bitmapscan')")))[0][0]
    await connection.all(text("SELECT set_config('enable_bitmapscan', 'off', true)"))
    try:
        yield
    finally:
        await connection.all(text("SELECT set_config('enable_bitmapscan', :previous, true)"), previous=previous)
