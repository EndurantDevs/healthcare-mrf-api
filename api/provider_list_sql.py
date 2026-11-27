# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-list SQL helpers isolated from transport and compiler modules."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from functools import partial
from typing import Any

from sqlalchemy import BigInteger, bindparam, column, func, literal_column, select, table, text
from sqlalchemy.dialects.postgresql import ARRAY

from api.address_source_filter import address_source_filter_sql
from api.custom_import_provider_sql import ProviderImportQuery, merge_native_params
from api.network_address_scope import (
    current_network_address_scope,
    is_scoped_address_relation,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.provider_directory_dataset_sql import named_source_data_sql
from api.provider_profile_snapshot import provider_read_savepoint
from api.provider_search_sql import ProviderPageSource
from db.connection import ConnectionProxy
from db.models import EntityAddressUnified, NPIAddress
from process.custom_import.read_core import _local_statement_timeout

_LOGGER = logging.getLogger(__name__)
_NPI_COUNT_READ_SCOPE: ContextVar[tuple[Any, float] | None] = ContextVar("npi_count_read_scope", default=None)

_CUSTOM_IMPORT_PROVIDER_RELATION = "custom_import_provider_relation"
_NATIVE_PROVIDER_RELATION = "native_eligible_npis"
GEO_SERVICE_LOCATION_TYPES = ("primary", "secondary", "practice", "site")
MIN_PROVIDER_LIST_PHONE_CANDIDATES = 100
MAX_PROVIDER_LIST_PHONE_CANDIDATES = 500

_CURRENT_PROVIDER_DIRECTORY_PHONE_CTES = """
current_provider_directory_runs AS MATERIALIZED (
    SELECT source.source_id, dataset.dataset_id,
           COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar
               AS run_id
      FROM mrf.provider_directory_source AS source
      JOIN mrf.provider_directory_endpoint_dataset AS dataset
        ON dataset.endpoint_id = source.endpoint_id
     WHERE dataset.is_current IS TRUE
       AND dataset.status = 'published'
       AND dataset.published_at IS NOT NULL
       AND dataset.superseded_at IS NULL
), matching_provider_directory_phone_rows AS MATERIALIZED (
    SELECT overlay.npi, overlay.address_key, overlay.source_id,
           overlay.last_seen_run_id, overlay.source_record_id,
           overlay.resource_type, overlay.resource_id
      FROM mrf.provider_directory_address_overlay AS overlay
     WHERE overlay.phone_number = :phone_digits
       AND overlay.npi IS NOT NULL
       AND overlay.address_key IS NOT NULL
)
"""

_PHONE_CANDIDATE_ROWS_CTE = """
phone_candidate_rows AS MATERIALIZED (
    SELECT DISTINCT
           COALESCE(phone_address.npi, phone_address.inferred_npi)::bigint AS provider_npi,
           phone_address.address_key,
           false AS provider_directory_matched,
           NULL::varchar AS source_id,
           NULL::varchar AS source_record_id,
           phone_address.source_count::integer AS source_count
      FROM {address_table_sql} AS phone_address
     WHERE phone_address.type IN ({service_types})
       AND phone_address.address_key IS NOT NULL
       AND COALESCE(phone_address.npi, phone_address.inferred_npi) IS NOT NULL
       AND {direct_phone}
    UNION ALL
    SELECT DISTINCT
           overlay.npi::bigint AS provider_npi,
           overlay.address_key,
           true AS provider_directory_matched,
           overlay.source_id::varchar,
           overlay.source_record_id::varchar,
           NULL::integer AS source_count
      FROM matching_provider_directory_phone_rows AS overlay
      JOIN current_provider_directory_runs AS current_run
        ON current_run.source_id = overlay.source_id
       AND current_run.run_id = overlay.last_seen_run_id
      JOIN mrf.provider_directory_dataset_resource AS dataset_resource
        ON dataset_resource.dataset_id = current_run.dataset_id
       AND dataset_resource.resource_type = overlay.resource_type
       AND dataset_resource.resource_id = overlay.resource_id
)
"""

_RANKED_PHONE_CANDIDATE_CTES = """
phone_candidates_unranked AS MATERIALIZED (
    SELECT candidate.provider_npi, candidate.address_key,
           BOOL_OR(candidate.provider_directory_matched) AS provider_directory_matched,
           MAX(candidate.source_count) AS source_count
      FROM phone_candidate_rows AS candidate
  GROUP BY candidate.provider_npi, candidate.address_key
), phone_candidate_best_addresses AS MATERIALIZED (
    SELECT DISTINCT ON (candidate.provider_npi)
           candidate.provider_npi, candidate.address_key,
           candidate.provider_directory_matched, candidate.source_count
      FROM phone_candidates_unranked AS candidate
  ORDER BY candidate.provider_npi,
           candidate.provider_directory_matched DESC,
           candidate.source_count DESC NULLS LAST,
           candidate.address_key
), phone_candidates AS MATERIALIZED (
    SELECT candidate.provider_npi, candidate.address_key,
           candidate.provider_directory_matched
      FROM phone_candidate_best_addresses AS candidate
  ORDER BY candidate.provider_directory_matched DESC,
           candidate.source_count DESC NULLS LAST,
           candidate.provider_npi,
           candidate.address_key
     {candidate_limit_clause}
), phone_provider_directory_evidence AS MATERIALIZED (
    SELECT evidence.provider_npi,
           ARRAY_AGG(evidence.source_record_id ORDER BY evidence.source_id)
               AS source_record_ids
      FROM (
            SELECT candidate.provider_npi, candidate.source_id,
                   MIN(candidate.source_record_id) AS source_record_id
              FROM phone_candidate_rows AS candidate
              JOIN phone_candidates AS selected_candidate
                ON selected_candidate.provider_npi = candidate.provider_npi
             WHERE candidate.provider_directory_matched
               AND candidate.source_id IS NOT NULL
               AND candidate.source_record_id IS NOT NULL
          GROUP BY candidate.provider_npi, candidate.source_id
      ) AS evidence
  GROUP BY evidence.provider_npi
)
"""


def _is_unified_address_table(address_table_sql: str) -> bool:
    return address_table_sql.endswith(f".{EntityAddressUnified.__tablename__}") or is_scoped_address_relation(
        address_table_sql
    )


def _address_zip5_filter(
    alias: str,
    address_table_sql: str,
    *,
    any_array: bool = False,
) -> str:
    # Match the serving ZIP indexes, including rows without a populated zip5.
    column = (
        f"COALESCE({alias}.zip5, LEFT(COALESCE({alias}.postal_code, ''), 5))"
        if _is_unified_address_table(address_table_sql)
        else f"LEFT({alias}.postal_code, 5)"
    )
    operator = "ANY (:zip_codes)" if any_array else ":zip_code"
    return f"{column} = {operator}"


def _address_phone_digits_filter(alias: str, address_table_sql: str) -> str:
    raw_digits = f"regexp_replace(COALESCE({alias}.telephone_number, ''), '[^0-9]', '', 'g')"
    if _is_unified_address_table(address_table_sql):
        return f"COALESCE(NULLIF({alias}.phone_number, ''), {raw_digits}) = :phone_digits"
    return f"{raw_digits} = :phone_digits"


def _provider_list_phone_candidate_limit(
    page_limit: int,
    page_offset: int = 0,
    *,
    count_query: bool = False,
) -> int:
    """Bound phone candidates while retaining enough rows for paging/filtering."""

    if count_query:
        return MAX_PROVIDER_LIST_PHONE_CANDIDATES
    requested_window = max(int(page_offset), 0) + max(int(page_limit), 1)
    return min(
        max(requested_window * 8, MIN_PROVIDER_LIST_PHONE_CANDIDATES),
        MAX_PROVIDER_LIST_PHONE_CANDIDATES,
    )


def _address_phone_candidates_cte(
    address_table_sql: str,
    *,
    is_bounded: bool = True,
    editions=(),
    min_address_sources: int = 0,
) -> str | None:
    """Return indexed phone candidates, including current directory evidence."""

    if not _is_unified_address_table(address_table_sql) or is_scoped_address_relation(address_table_sql):
        return None
    direct_phone = _address_phone_digits_filter("phone_address", address_table_sql)
    if min_address_sources:
        direct_phone += " AND " + address_source_filter_sql("phone_address", min_address_sources)
    service_types = ", ".join(f"'{location_type}'" for location_type in GEO_SERVICE_LOCATION_TYPES)
    phone_candidate_rows_cte = _PHONE_CANDIDATE_ROWS_CTE.format(
        address_table_sql=address_table_sql,
        service_types=service_types,
        direct_phone=direct_phone,
    )
    if min_address_sources:
        phone_candidate_rows_cte = phone_candidate_rows_cte.replace(
            "AND dataset_resource.resource_id = overlay.resource_id",
            "AND dataset_resource.resource_id = overlay.resource_id "
            f"AND EXISTS (SELECT 1 FROM {address_table_sql} qualifying_address "
            f"WHERE qualifying_address.type IN ({service_types}) "
            "AND COALESCE(qualifying_address.npi, qualifying_address.inferred_npi) = overlay.npi "
            "AND qualifying_address.address_key = overlay.address_key "
            f"AND {address_source_filter_sql('qualifying_address', min_address_sources)})",
        )
    candidate_limit_clause = "LIMIT :candidate_limit" if is_bounded else ""
    return named_source_data_sql(
        ",\n".join(
            (
                _CURRENT_PROVIDER_DIRECTORY_PHONE_CTES.strip(),
                phone_candidate_rows_cte.strip(),
                _RANKED_PHONE_CANDIDATE_CTES.format(candidate_limit_clause=candidate_limit_clause).strip(),
            )
        ),
        "mrf",
        editions,
    )


def _address_phone_candidates_lateral_from(address_table_sql: str, alias: str, min_address_sources: int = 0) -> str:
    exact_phone = _address_phone_digits_filter("candidate_address", address_table_sql)
    service_location = _provider_list_address_type_clause(
        "candidate_address",
        address_table_sql,
        include_service_locations=True,
    )
    source_filter = (
        f" AND {address_source_filter_sql('candidate_address', min_address_sources)}" if min_address_sources else ""
    )
    return f"""
          FROM phone_candidates AS phone_match
     LEFT JOIN phone_provider_directory_evidence AS phone_evidence
            ON phone_evidence.provider_npi = phone_match.provider_npi
         CROSS JOIN LATERAL (
               SELECT candidate_address.*
                 FROM {address_table_sql} AS candidate_address
                WHERE candidate_address.address_key = phone_match.address_key
                  AND COALESCE(candidate_address.npi, candidate_address.inferred_npi) = phone_match.provider_npi
                  AND {service_location}{source_filter}
             ORDER BY ({exact_phone}) DESC,
                      candidate_address.source_count DESC NULLS LAST,
                      candidate_address.location_key
                LIMIT 1 OFFSET 0
         ) AS {alias}
    """


def _address_phone_candidates_join(
    alias: str,
    provider_npi_sql: str | None = None,
) -> str:
    provider_npi = provider_npi_sql or f"COALESCE({alias}.npi, {alias}.inferred_npi)"
    return f"""
          JOIN phone_candidates AS phone_match
            ON phone_match.provider_npi = {provider_npi}
           AND phone_match.address_key = {alias}.address_key
    """


def _provider_list_address_type_clause(
    alias: str,
    address_table_sql: str,
    *,
    include_service_locations: bool,
) -> str:
    if include_service_locations and _is_unified_address_table(address_table_sql):
        type_list = ", ".join(f"'{value}'" for value in GEO_SERVICE_LOCATION_TYPES)
        return f"{alias}.type IN ({type_list})"
    return f"{alias}.type = 'primary'"


def _append_available_filter(
    address_clauses: list[str],
    array_clause: str,
    exists_clause: str,
    *,
    is_array_available: bool,
    is_table_available: bool,
) -> None:
    if is_array_available and is_table_available:
        address_clauses.append(f"({array_clause} OR {exists_clause})")
    elif is_array_available:
        address_clauses.append(array_clause)
    elif is_table_available:
        address_clauses.append(exists_clause)
    else:
        address_clauses.append("1=0")


def _append_array_filters(
    address_clauses: list[str],
    filters_by_name: dict[str, Any],
    provider_npi_sql: str,
) -> dict[str, int]:
    """Append supported procedure and medication predicates and parameters."""
    parameters_by_name: dict[str, int] = {}
    filter_year = filters_by_name.get("filter_year")
    procedure_internal_codes = filters_by_name.get("procedure_internal_codes") or []
    medication_internal_codes = filters_by_name.get("medication_internal_codes") or []
    procedures_array_available = bool(filters_by_name.get("npi_procedures_array_available", True))
    medications_array_available = bool(filters_by_name.get("npi_medications_array_available", True))
    procedure_table_available = bool(filters_by_name.get("pricing_provider_procedure_available", False))
    medication_table_available = bool(filters_by_name.get("pricing_provider_prescription_available", False))
    if filter_year is not None and (procedure_internal_codes or medication_internal_codes):
        parameters_by_name["filter_year"] = int(filter_year)
    for idx, code in enumerate(procedure_internal_codes):
        parameter_name = f"procedure_code_{idx}"
        parameters_by_name[parameter_name] = int(code)
        array_clause = f"c.procedures_array @> ARRAY[:{parameter_name}]::INTEGER[]"
        claims_pp_relation = "mrf.pricing_provider_procedure"
        exists_clause = (
            "EXISTS ("
            f"SELECT 1 FROM {claims_pp_relation} AS pp "
            f"WHERE pp.npi = {provider_npi_sql} AND pp.procedure_code = :{parameter_name}"
            + (" AND pp.year = :filter_year" if filter_year is not None else "")
            + ")"
        )
        _append_available_filter(
            address_clauses,
            array_clause,
            exists_clause,
            is_array_available=procedures_array_available,
            is_table_available=procedure_table_available,
        )
    for idx, code in enumerate(medication_internal_codes):
        parameter_name = f"medication_code_{idx}"
        parameters_by_name[parameter_name] = int(code)
        array_clause = f"c.medications_array @> ARRAY[:{parameter_name}]::INTEGER[]"
        claims_pr_relation = "mrf.pricing_provider_prescription"
        exists_clause = (
            "EXISTS ("
            f"SELECT 1 FROM {claims_pr_relation} AS pr "
            f"WHERE pr.npi = {provider_npi_sql} "
            "AND pr.rx_code_system = 'HP_RX_CODE' "
            + ("AND pr.year = :filter_year " if filter_year is not None else "")
            + f"AND CASE WHEN pr.rx_code ~ '^-?[0-9]+$' THEN pr.rx_code::INTEGER END = :{parameter_name} "
            ")"
        )
        _append_available_filter(
            address_clauses,
            array_clause,
            exists_clause,
            is_array_available=medications_array_available,
            is_table_available=medication_table_available,
        )
    return parameters_by_name


def _sql_with_prefix_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes},\n" if available_ctes else "WITH "


def _sql_with_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes}\n" if available_ctes else ""


_OTHER_ZIP_PAGE_FILTERS = (
    "classification",
    "specialization",
    "section",
    "display_name",
    "codes",
    "min_address_sources",
    "plan_network",
    "has_insurance",
    "phone_digits",
    "address_key",
    "address_site_key",
    "npi",
)


def _provider_indexed_location_address(scope, address_sql, schema, zip_code, city_state):
    """Select indexed membership only on a qualified compact location search."""
    if (
        scope is not None
        and scope.is_compact_network_selection
        and scope.manifest.supports_location_lookup
        and (zip_code or city_state)
    ):
        lookup_by_name = {"zip_lookup": True} if zip_code else {"location_lookup": True}
        return True, scoped_address_relation_sql(f"{schema}.{EntityAddressUnified.__tablename__}", **lookup_by_name)
    return False, address_sql


def _provider_city_count_address(scope, address_sql, *, city, state, address_clauses):
    """Reuse the city page projection for the same complete native count scope."""
    if not city or not state or len(address_clauses) != 3:
        return address_sql
    from api.network_compact_address import compact_city_search_relation

    return compact_city_search_relation(scope.manifest.compact_layout, scope.network_ids) or address_sql


def _provider_plain_network_page(scope, filters, clauses, schema, page_end, *, is_plain, taxonomy_join=""):
    """Plan a compact city page or a bounded NPI page for native provider reads."""
    if not (
        is_plain
        and scope is not None
        and scope.is_compact_network_selection
        and scope.manifest.supports_location_lookup
    ):
        return None
    from api.network_compact_address import compact_city_page, compact_npi_page

    layout = scope.manifest.compact_layout
    is_city_page = len(clauses) == 3 and filters.get("city") and filters.get("state") and not filters.get("zip_code")
    if not is_city_page and (len(clauses) != 1 or page_end > 64):
        return None
    # Only the indexed city projection can keep a per-provider taxonomy probe.
    if taxonomy_join and not (is_city_page and layout.city_search is not None and "CROSS JOIN" not in taxonomy_join):
        return None
    indexed = scoped_address_relation_sql(f"{schema}.{EntityAddressUnified.__tablename__}", location_lookup=True)
    if is_city_page:
        keys, city_page, fallback = compact_city_page(layout, scope.network_ids, clauses[0], taxonomy_join)
        return keys, fallback, lambda page_sql: f"({city_page}) UNION ALL ({page_sql})", f"{indexed} as c"
    return None, None, partial(compact_npi_page, layout, scope.network_ids, clauses[0]), f"{indexed} as c"


def _provider_page_access(
    scope, filters, address_source, taxonomy_source, *, npi_sort, ordinary, native_only, bare_location
):
    """Choose NPI ordering and the office source for page discovery."""
    compact = scope is not None and scope.is_compact_network_selection
    network_count = len(scope.network_ids) if compact else 0
    location_lookup = compact and scope.manifest.supports_location_lookup
    is_nationwide_page = network_count in (4, 5)
    zip_code = filters.get("zip_code")
    location = zip_code or filters.get("city") or filters.get("state")
    plain_sort = npi_sort and bool(taxonomy_source)
    # Sort ZIP matches after location lookup instead of scanning the nationwide NPI index.
    indexed_zip_order = npi_sort and native_only and bare_location and bool(zip_code) and location_lookup
    computed = (
        indexed_zip_order
        or bool(filters.get("plan_network"))
        or (plain_sort and ((ordinary and zip_code) or (compact and (location or is_nationwide_page))))
    )
    is_bounded_page = network_count == 3 and location_lookup
    if not ((is_nationwide_page or is_bounded_page) and plain_sort and native_only and not location):
        return computed, None, ProviderPageSource(address_source)
    if not filters["primary_only"] or len(filters["codes"] or ()) != 1:
        return computed, None, ProviderPageSource(address_source)
    other_filters = (
        "classification",
        "section",
        "display_name",
        "specialization",
        "plan_network",
        "plan_release_id",
        "provider_sex_code",
        "has_insurance",
        "phone_digits",
        "address_key",
        "address_site_key",
        "npi",
        "min_address_sources",
        "procedure_codes_input",
        "medication_codes_input",
    )
    if any(filters.get(key) for key in other_filters):
        return computed, None, ProviderPageSource(address_source)
    from api.network_compact_address import compact_selected_offices

    keys, offices = compact_selected_offices(scope.manifest.compact_layout, scope.network_ids)
    bounded_source = (
        scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True) if is_bounded_page else None
    )
    return computed, keys, ProviderPageSource(f"{offices} as c {taxonomy_source}", bounded_source)


def _provider_registry_page_cte(
    projection: str,
    where: str,
    parameters: Mapping[str, object],
    filters: Mapping[str, object],
    address_clauses: Sequence[str],
    *,
    zip_table: str | None,
    has_additional_constraints: bool,
) -> str:
    """Limit ordinary ZIP organization registry lookups to eligible offices."""
    narrow_zip_search = (
        zip_table is not None
        and bool(filters.get("zip_code"))
        and parameters.get("entity_type_code") == 2
        and not has_additional_constraints
        and not any(filters.get(key) for key in _OTHER_ZIP_PAGE_FILTERS)
    )
    if (
        narrow_zip_search
        and parameters.keys() == {"entity_type_code", "name_like_0"}
        and projection == "b.npi"
        and filters.get("city")
        and filters.get("state")
    ):
        # A correlated lookup keeps broad names from driving a nationwide office search.
        return f"""
        zip_npis AS MATERIALIZED (
            SELECT DISTINCT COALESCE(c.npi, c.inferred_npi) AS npi
              FROM {zip_table} AS c
             WHERE {" and ".join(address_clauses)}
        ), filtered_npi AS MATERIALIZED (
            SELECT admitted.npi
              FROM zip_npis z CROSS JOIN LATERAL (
                  SELECT b.npi FROM mrf.npi AS b
                   WHERE {where} AND b.npi = z.npi OFFSET 0
              ) admitted
        )
        """
    materialization = "NOT MATERIALIZED" if narrow_zip_search and len(parameters) == 1 else "MATERIALIZED"
    return f"""
        filtered_npi AS {materialization} (
            SELECT {projection}
              FROM mrf.npi AS b
             WHERE {where}
        )
"""


def _address_npi_filter(alias: str, address_table_sql: str) -> str:
    if _is_unified_address_table(address_table_sql):
        return f"COALESCE({alias}.npi, {alias}.inferred_npi) = :npi_filter"
    return f"{alias}.npi = :npi_filter"


def _address_site_key_filter(alias: str, address_table_sql: str) -> str:
    if _is_unified_address_table(address_table_sql):
        return f"{alias}.premise_key = CAST(:address_site_key AS uuid)"
    return "1=0"


def _primary_address_order_clause(alias: str, address_table_sql: str) -> str:
    common = (
        f"{alias}.npi, "
        f"({alias}.lat IS NULL OR {alias}.long IS NULL), "
        f"(NULLIF(TRIM(COALESCE({alias}.first_line, '')), '') IS NULL), "
    )
    if _is_unified_address_table(address_table_sql):
        return (
            common
            + f"(COALESCE({alias}.address_precision, '') = 'city_zip'), "
            + f"{alias}.source_count DESC NULLS LAST, "
            + f"{alias}.updated_at DESC NULLS LAST, "
            + f"{alias}.location_key"
        )
    return common + f"{alias}.date_added DESC NULLS LAST, {alias}.checksum"


@contextlib.asynccontextmanager
async def _provider_list_connection(
    database: Any,
    import_context: ProviderImportQuery | None,
    request_session: Any,
    *,
    native_npis=None,
):
    """Reuse an explicit request snapshot, keeping unscoped compatibility reads independent."""

    if (
        request_session is None
        and import_context is None
        and native_npis is None
        and current_network_address_scope() is None
    ):
        async with database.acquire() as connection:
            yield connection
        return
    if request_session is None:
        if import_context is not None:
            raise RuntimeError("custom-import provider query requires a request session")
        raise RuntimeError("canonical provider query requires a request session")
    yield ConnectionProxy(database, request_session, None)


async def _npi_count_read(read: Callable[[], Awaitable[Any]]):
    """Share the count deadline across metadata and aggregate reads."""
    scope = _NPI_COUNT_READ_SCOPE.get()
    if scope is None:
        return await read()
    session, deadline = scope
    remaining_ms = math.ceil((deadline - time.monotonic()) * 1000)
    if remaining_ms <= 0:
        raise TimeoutError("provider count deadline expired")
    async with _local_statement_timeout(session, timeout_ms=remaining_ms):
        result = await read()
    if time.monotonic() >= deadline:
        raise TimeoutError("provider count deadline expired")
    return result


async def _bounded_provider_list_count(count, *, session, has_reader: bool, timeout: float):
    """Bound one ordinary count phase and recover its pinned Reader on timeout."""
    timeout = max(0.1, timeout)
    if not has_reader:
        async with asyncio.timeout(timeout):
            return await count()
    deadline = time.monotonic() + timeout
    token = _NPI_COUNT_READ_SCOPE.set((session, deadline))
    try:
        async with provider_read_savepoint(session):
            count_rows = await count(deadline=deadline)
            if time.monotonic() >= deadline:
                raise TimeoutError("provider count deadline expired")
            return count_rows
    finally:
        _NPI_COUNT_READ_SCOPE.reset(token)


def _provider_list_total_statement(address_model, *, has_insurance=False, city=None, state=None):
    """Count distinct unified providers or the legacy one-row-per-NPI primary heap."""
    address_table = address_model.__table__
    provider_npi = address_table.c.npi
    if address_model is EntityAddressUnified:
        provider_npi = func.coalesce(address_table.c.npi, address_table.c.inferred_npi)
        is_serving_type = address_table.c.type.in_(GEO_SERVICE_LOCATION_TYPES)
    else:
        is_serving_type = address_table.c.type == "primary"
    if not has_insurance:
        if address_model is EntityAddressUnified:
            return select(func.count(func.distinct(provider_npi))).where(is_serving_type, provider_npi.is_not(None))
        return select(func.count()).select_from(address_table).where(is_serving_type)
    conditions = [
        is_serving_type,
        provider_npi.is_not(None),
        literal_column("NOT (plans_network_array @@ '0'::query_int)"),
    ]
    if city:
        conditions.append(address_table.c.city_name == city)
    if state:
        conditions.append(address_table.c.state_name == state)
    if city is None and state is None and address_model is NPIAddress:
        return select(func.count()).select_from(address_table).where(*conditions)
    return select(func.count(func.distinct(provider_npi))).where(*conditions)


async def _provider_list_count(connection, query, parameters, session, *, deadline=None):
    """Bound optional Reader aggregates without cancelling their pinned connection."""
    if deadline is None:
        rows = await connection.all(query, **parameters)
    else:
        remaining_ms = math.ceil((deadline - time.monotonic()) * 1000)
        if remaining_ms <= 0:
            raise TimeoutError("optional count deadline expired")
        async with _local_statement_timeout(session, timeout_ms=remaining_ms):
            rows = await connection.all(query, **parameters)
        if time.monotonic() >= deadline:
            raise TimeoutError("optional count deadline expired")
    return rows[0][0] if rows else 0


def _provider_import_relation_cte(
    import_context: ProviderImportQuery | None,
) -> str | None:
    if import_context is None:
        return None
    return f"custom_import_provider_relation AS ({import_context.compiled.sql})"


def _provider_native_entity_scope():
    """Refer to the complete native provider CTE, never a candidate page."""

    native_providers = table(_NATIVE_PROVIDER_RELATION, column("npi", BigInteger))
    return select(native_providers.c.npi)


def _provider_import_membership_clause(
    import_context: ProviderImportQuery | None,
    provider_npi_sql: str,
    *,
    native_npis=None,
) -> str | None:
    """Return the imported-membership predicate for exact matching counts."""

    prefilter = _provider_import_match_clause(import_context, provider_npi_sql, native_npis=native_npis)
    predicates = [prefilter] if prefilter is not None else []
    if import_context is not None and import_context.require_match:
        predicates.append(
            "EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported "
            f"WHERE imported.entity_value = ({provider_npi_sql})::text)"
        )
    return " AND ".join(predicates) or None


def _provider_import_match_clause(
    import_context: ProviderImportQuery | None,
    provider_npi_sql: str,
    *,
    native_npis=None,
) -> str | None:
    """Prefilter page eligibility; the final text join enforces membership."""

    if native_npis is None and import_context is not None:
        native_npis = import_context.native_npis
    predicates = []
    if native_npis is not None:
        predicates.append(f"({provider_npi_sql}) = ANY(:__native_batch_npis)")
    if import_context is not None and import_context.require_match:
        if native_npis is None:
            # Keep native NPI indexes usable without changing canonical text membership.
            predicates.append(
                f"({provider_npi_sql})::bigint IN ("
                "SELECT CASE WHEN imported.entity_value ~ '^[0-9]{10}$' "
                "THEN imported.entity_value::bigint END "
                "FROM custom_import_provider_relation AS imported)"
            )
    return " AND ".join(predicates) or None


def _provider_import_order_clause(
    import_context: ProviderImportQuery,
    npi_sql: str,
) -> str:
    order_terms = ["(imported.entity_value IS NULL) ASC"]
    order_terms.extend(
        f"imported.sort_{ordinal} {term.direction.upper()} NULLS LAST"
        for ordinal, term in enumerate(import_context.prepared.normalized_order_terms)
    )
    order_terms.append(f"{npi_sql} ASC")
    return ", ".join(order_terms)


def _provider_import_count_page(eligible_sql, import_context, *, native_order="ORDER BY npi"):
    """Evaluate the exact eligible provider relation once, including empty pages."""

    order = (
        _provider_import_order_clause(import_context, "imported.npi")
        if import_context.prepared.normalized_order_terms
        else native_order.removeprefix("ORDER BY ")
    )
    sorts = "".join(
        f", imported.sort_{ordinal}" for ordinal, _term in enumerate(import_context.prepared.normalized_order_terms)
    )
    membership = "WHERE imported.entity_value IS NOT NULL" if import_context.require_match else ""
    matching = f"""
        SELECT eligible_npi.*, imported.entity_value{sorts}
          FROM ({eligible_sql}) AS eligible_npi
     LEFT JOIN custom_import_provider_relation AS imported
            ON imported.entity_value = eligible_npi.npi::text
        {membership}
    """
    if import_context.require_match and not sorts and native_order == "ORDER BY npi":
        # Exact text intersection avoids rescanning an underestimated imported set per native NPI.
        matching = f"""
            SELECT membership.entity_value::bigint AS npi FROM (
                SELECT eligible_npi.npi::text AS entity_value FROM ({eligible_sql}) AS eligible_npi
                INTERSECT SELECT imported.entity_value FROM custom_import_provider_relation AS imported
            ) AS membership
        """
    ctes = f"""matching_npis AS MATERIALIZED ({matching}),
        provider_totals AS (SELECT COUNT(*) AS _provider_total FROM matching_npis)"""
    position = f", ROW_NUMBER() OVER (ORDER BY {order}) AS _provider_page_position" if sorts else ""
    page = f"SELECT imported.*{position} FROM matching_npis AS imported ORDER BY {order} LIMIT :limit OFFSET :start"
    return ctes, page


def _provider_list_statement(sql: str, import_context: ProviderImportQuery | None, *, native_npis=None):
    statement = text(sql)
    if import_context is not None:
        statement = statement.bindparams(*import_context.compiled.typed_binds)
        if native_npis is None:
            native_npis = import_context.native_npis
    if native_npis is not None and ":__native_batch_npis" in sql:
        statement = statement.bindparams(bindparam("__native_batch_npis", type_=ARRAY(BigInteger)))
    return statement


def _provider_list_parameters(
    parameters_by_name: Mapping[str, object],
    import_context: ProviderImportQuery | None,
    *,
    native_npis=None,
) -> dict[str, object]:
    merged = (
        dict(parameters_by_name)
        if import_context is None
        else merge_native_params(parameters_by_name, import_context.compiled)
    )
    if native_npis is None and import_context is not None:
        native_npis = import_context.native_npis
    if native_npis is not None:
        if "__native_batch_npis" in merged:
            raise ValueError("native batch SQL parameters collide")
        merged["__native_batch_npis"] = list(native_npis)
    return scoped_address_parameters(merged)


def _extract_name_filters(request, *, args=None) -> list[str]:
    args = (getattr(request, "args", {}) or {}) if args is None else args
    names: list[str] = []
    if hasattr(args, "getlist"):
        names.extend(args.getlist("name_like"))
    elif hasattr(args, "getall"):
        try:
            names.extend(args.getall("name_like"))
        except Exception as exc:
            _LOGGER.debug("failed to read name_like filters with getall: %s", exc)
    else:
        maybe_name = args.get("name_like")
        if maybe_name:
            names.append(maybe_name)
    single_name = args.get("name_like")
    if single_name:
        names.append(single_name)
    normalized_names: list[str] = []
    seen_names = set()
    for name in names:
        if not name:
            continue
        normalized_name = str(name).lower()
        if normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        normalized_names.append(normalized_name)
    return normalized_names
