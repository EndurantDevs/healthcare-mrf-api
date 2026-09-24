# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-list SQL helpers isolated from transport and compiler modules."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from api.custom_import_provider_sql import ProviderImportQuery, merge_native_params
from db.connection import ConnectionProxy
from db.models import EntityAddressUnified

_LOGGER = logging.getLogger(__name__)

_CUSTOM_IMPORT_PROVIDER_RELATION = "custom_import_provider_relation"
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
    return address_table_sql.endswith(f".{EntityAddressUnified.__tablename__}")


def _address_zip5_filter(
    alias: str,
    address_table_sql: str,
    *,
    any_array: bool = False,
) -> str:
    column = f"{alias}.zip5" if _is_unified_address_table(address_table_sql) else f"LEFT({alias}.postal_code, 5)"
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
) -> str | None:
    """Return indexed phone candidates, including current directory evidence."""

    if not _is_unified_address_table(address_table_sql):
        return None
    direct_phone = _address_phone_digits_filter("phone_address", address_table_sql)
    service_types = ", ".join(f"'{location_type}'" for location_type in GEO_SERVICE_LOCATION_TYPES)
    phone_candidate_rows_cte = _PHONE_CANDIDATE_ROWS_CTE.format(
        address_table_sql=address_table_sql,
        service_types=service_types,
        direct_phone=direct_phone,
    )
    candidate_limit_clause = "LIMIT :candidate_limit" if is_bounded else ""
    return ",\n".join(
        (
            _CURRENT_PROVIDER_DIRECTORY_PHONE_CTES.strip(),
            phone_candidate_rows_cte.strip(),
            _RANKED_PHONE_CANDIDATE_CTES.format(candidate_limit_clause=candidate_limit_clause).strip(),
        )
    )


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


def _sql_with_prefix_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes},\n" if available_ctes else "WITH "


def _sql_with_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes}\n" if available_ctes else ""


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
):
    """Reuse the signed request transaction only for imported provider reads."""

    if import_context is None:
        async with database.acquire() as connection:
            yield connection
        return
    if request_session is None:
        raise RuntimeError("custom-import provider query requires a request session")
    yield ConnectionProxy(database, request_session, None)


def _provider_import_relation_cte(
    import_context: ProviderImportQuery | None,
) -> str | None:
    if import_context is None:
        return None
    return f"custom_import_provider_relation AS ({import_context.compiled.sql})"


def _provider_import_membership_clause(
    import_context: ProviderImportQuery | None,
    provider_npi_sql: str,
) -> str | None:
    """Return the imported-membership predicate for exact matching counts."""

    if import_context is None or not import_context.require_match:
        return None
    return (
        "EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported "
        f"WHERE imported.entity_value = ({provider_npi_sql})::text)"
    )


def _provider_import_match_clause(
    import_context: ProviderImportQuery | None,
    provider_npi_sql: str,
) -> str | None:
    """Return the one correlated predicate used by filter-only requests."""

    if import_context is None or import_context.prepared.normalized_order_terms:
        return None
    return _provider_import_membership_clause(import_context, provider_npi_sql)


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


def _provider_list_statement(sql: str, import_context: ProviderImportQuery | None):
    statement = text(sql)
    if import_context is not None:
        statement = statement.bindparams(*import_context.compiled.typed_binds)
    return statement


def _provider_list_parameters(
    parameters_by_name: Mapping[str, object],
    import_context: ProviderImportQuery | None,
) -> dict[str, object]:
    if import_context is None:
        return dict(parameters_by_name)
    return merge_native_params(parameters_by_name, import_context.compiled)


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
