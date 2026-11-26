# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose imported-field ordering into exact nearby-provider SQL."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any
from uuid import UUID

from sanic.exceptions import InvalidUsage
from sqlalchemy import BigInteger, bindparam, column, select, table, text

from api.custom_import_provider_sql import ProviderImportQuery
from api.provider_geo_search import nearest_first_pages
from api.provider_list_sql import _address_zip5_filter, _provider_list_connection, _provider_list_parameters
from db.tiger_models import Zip_zcta5

_IMPORT_RELATION = "custom_import_provider_relation"
_NATIVE_GEO_RELATION = "native_eligible_geo"
_PAGE_LIMIT_PARAMETER = "__custom_import_geo_page_limit"
_CURSOR_NPI_PARAMETER = "__custom_import_geo_cursor_npi"
_CURSOR_ADDRESS_KEY_PARAMETER = "__custom_import_geo_cursor_address_key"


def nearby_extra_clauses(
    filters: Sequence[str], plan_member_sql: str | None, plan_filter_position: int
) -> tuple[str, tuple[str, str] | None]:
    """Compose common predicates and the legacy page and cursor plan filters."""

    separator = "\n          AND "
    if plan_member_sql is None:
        return (separator + separator.join(filters) if filters else ""), None

    before, after = filters[:plan_filter_position], filters[plan_filter_position:]
    membership = f"a.npi IN ({plan_member_sql})"
    # OFFSET prevents flattening the probe into a join before distance ordering.
    legacy_membership = (
        f"EXISTS (SELECT 1 FROM ({plan_member_sql}) AS plan_members WHERE plan_members.npi = a.npi OFFSET 0)"
    )
    return (
        separator + separator.join((*before, membership, *after)),
        (
            separator + separator.join((*before, legacy_membership, *after)),
            separator + separator.join((*before, f"({membership}) IS TRUE", *after)),
        ),
    )


def normalize_state_filter(raw_value: Any) -> str | None:
    """Normalize the optional state constraint shared by provider location queries."""
    if raw_value in (None, "", "null"):
        return None
    state_value = str(raw_value).strip()
    if not state_value:
        return None
    if len(state_value) > 2:
        raise InvalidUsage("Parameter 'state' is too long (max 2 chars)")
    normalized = state_value.upper()
    if not re.fullmatch(r"[A-Z]{2}", normalized):
        raise InvalidUsage("Parameter 'state' must be a 2-letter code")
    return normalized


def canonical_zip_anchor_statement():
    """Resolve the first ZIP against the canonical relation, independent of search_path."""
    zip_table = Zip_zcta5.__table__
    return (
        select(zip_table.c.intptlat, zip_table.c.intptlon).where(zip_table.c.zcta5ce == bindparam("zip_code")).limit(1)
    )


@dataclass(frozen=True, slots=True)
class ImportedGeoStatements:
    """Exact count, optional anchor check, and page statements for one query."""

    count: Any
    page: Any
    anchor: Any | None
    parameters: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class NearbySqlQuery:
    """Trusted nearby-search fragments after request normalization."""

    taxonomy_conditions: str
    extra_clause: str
    ilike_clause: str
    use_taxonomy_filter: bool
    primary_only: bool
    address_table_sql: str
    geo_precision_clause: str
    geo_type_clause: str
    search_table_sql: str | None = None
    # The all-address projection bakes NPI existence, primary taxonomy and shared identities into each row.
    search_provider_facts: bool = False
    search_has_entity_type_code: bool = False

    @property
    def search_extra_clause(self) -> str:
        """Use the NPI fact on a projection that carries it without changing the cursor filter."""
        if self.search_has_entity_type_code:
            return self.extra_clause.replace(GEO_ENTITY_TYPE_PREDICATE, "a.entity_type_code = :entity_type_code")
        return self.extra_clause

    def with_legacy_plan_membership(self, page_clauses: tuple[str, str] | None) -> NearbySqlQuery:
        """Keep plan membership as a boolean filter during native address deduplication."""
        if self.search_table_sql or page_clauses is None:
            return self
        _, membership_filter = page_clauses
        return replace(self, extra_clause=membership_filter)


@dataclass(frozen=True, slots=True)
class ImportedGeoQuery:
    """The normalized native query and page state for an imported geo read."""

    nearby: NearbySqlQuery
    native_parameters: Mapping[str, object]
    limit: int
    cursor_anchor: tuple[str, str] | None


async def _fetch_imported_geo_page(database, import_context, request_session, imported_statements):
    """Check a signed cursor anchor and fetch one exact imported geo page."""

    async with _provider_list_connection(database, import_context, request_session) as connection:
        rows = await connection.all(imported_statements.page, **imported_statements.parameters)
        if imported_statements.anchor is not None:
            anchor_count_mapping = getattr(rows[0], "_mapping", {}) if rows else {}
            anchor_count = anchor_count_mapping.get("_geo_anchor_count")
            if int(anchor_count or 0) != 1:
                raise InvalidUsage("custom-import geo cursor anchor is unavailable")
        return rows


def build_imported_geo_statements(
    import_context: ProviderImportQuery,
    imported_query: ImportedGeoQuery,
    *,
    plan_member_sql: str | None = None,
    extra_filters: Sequence[str] = (),
    legacy_page_clauses: tuple[str, str] | None = None,
) -> ImportedGeoStatements:
    """Build one unbounded eligible relation before imported sorting and paging."""

    if type(import_context) is not ProviderImportQuery:
        raise ValueError("custom-import geo context is invalid")
    if type(imported_query) is not ImportedGeoQuery:
        raise ValueError("custom-import geo query is invalid")
    if type(imported_query.limit) is not int or not 1 <= imported_query.limit <= 50:
        raise ValueError("custom-import geo page limit is invalid")
    if not import_context.require_match and not import_context.prepared.normalized_order_terms:
        raise ValueError("custom-import geo ordering is required")

    parameters_by_name, normalized_anchor = _imported_geo_page_parameters(import_context, imported_query)
    common_ctes = _common_ctes(
        import_context=import_context,
        nearby=imported_query.nearby,
        plan_member_sql=plan_member_sql,
        extra_filters=extra_filters,
        legacy_page_clauses=legacy_page_clauses,
    )
    count = _statement(
        f"WITH {common_ctes}\nSELECT COUNT(*) AS total_count\n  FROM selected_geo",
        import_context,
    )
    if normalized_anchor is None:
        return ImportedGeoStatements(
            count=count,
            page=_page_statement(common_ctes, import_context, imported_query.nearby, has_anchor=False),
            anchor=None,
            parameters=parameters_by_name,
        )

    anchor_ctes = _anchor_ctes()
    anchor = _statement(
        f"WITH {common_ctes},\n{anchor_ctes}\nSELECT COUNT(*) AS anchor_count\n  FROM cursor_anchor_rows",
        import_context,
    )
    return ImportedGeoStatements(
        count=count,
        page=_page_statement(
            f"{common_ctes},\n{anchor_ctes},\ncursor_anchor AS MATERIALIZED (\n    SELECT * FROM cursor_anchor_rows\n)",
            import_context,
            imported_query.nearby,
            has_anchor=True,
        ),
        anchor=anchor,
        parameters=parameters_by_name,
    )


def _imported_geo_page_parameters(
    import_context: ProviderImportQuery,
    imported_query: ImportedGeoQuery,
) -> tuple[Mapping[str, object], tuple[str, str] | None]:
    """Bind the lookahead and any cursor anchor without mutating request parameters."""
    parameters_by_name = dict(imported_query.native_parameters)
    _add_parameter(parameters_by_name, _PAGE_LIMIT_PARAMETER, imported_query.limit + 1)
    normalized_anchor = _normalized_cursor_anchor(imported_query.cursor_anchor)
    if normalized_anchor is not None:
        _add_parameter(parameters_by_name, _CURSOR_NPI_PARAMETER, normalized_anchor[0])
        _add_parameter(parameters_by_name, _CURSOR_ADDRESS_KEY_PARAMETER, normalized_anchor[1])
    parameters_by_name = _provider_list_parameters(parameters_by_name, import_context)
    return MappingProxyType(parameters_by_name), normalized_anchor


def _common_ctes(
    *,
    import_context: ProviderImportQuery,
    nearby: NearbySqlQuery,
    plan_member_sql: str | None,
    extra_filters: Sequence[str],
    legacy_page_clauses: tuple[str, str] | None,
) -> str:
    nearby = nearby.with_legacy_plan_membership(legacy_page_clauses)
    projected_plan_membership = None
    if nearby.search_provider_facts and plan_member_sql is not None and import_context.require_match:
        projected_plan_membership = plan_member_sql
        non_plan_clause, _ = nearby_extra_clauses(extra_filters, None, 0)
        nearby = replace(nearby, extra_clause=non_plan_clause)
    native_cte = _native_eligible_geo_cte(
        nearby,
        projected_plan_membership=projected_plan_membership,
        spatial_first=plan_member_sql is None
        and import_context.require_match
        and not import_context.prepared.normalized_order_terms,
    )
    materialize_relation = import_context.require_match or import_context.has_native_scope
    # Keep optional unscoped SQL inlineable, so the join can narrow the import.
    materialization = " MATERIALIZED" if materialize_relation else ""
    relation_cte = f"{_IMPORT_RELATION} AS{materialization} ({import_context.compiled.sql})"
    selected_cte = _selected_geo_cte(import_context, materialize_relation=materialize_relation)
    return f"{native_cte},\n{relation_cte},\n{selected_cte}"


def native_geo_entity_scope():
    """Use every native eligible address before imported filtering and paging."""

    native_geo = table(_NATIVE_GEO_RELATION, column("npi_code", BigInteger))
    return select(native_geo.c.npi_code).distinct()


BOUNDED_CONTINUATION_RADIUS_SHARE = 0.1
_BOUNDED_CONTINUATION_ATTEMPTS = 4
_PAST_EVERY_IDENTITY = MappingProxyType(
    {"cursor_npi": 9999999999, "cursor_address_key": "ffffffff-ffff-ffff-ffff-ffffffffffff"}
)
_GEO_CENTER = "Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision)))"
_GEO_RADIUS = ":radius * 1609.34"
_PAGE_EDGE = "CAST(:page_edge_meters AS double precision)"


def _native_geo_source(nearby: NearbySqlQuery, *, spatial_first: bool = False) -> tuple[str, str, str]:
    """Return the NPI column, row source and row filters shared by eligible-row reads."""
    on_search = bool(nearby.search_table_sql)
    taxonomy_where = _taxonomy_filter_where(nearby, on_search=on_search, spatial_first=spatial_first)
    npi_column, npi_join = _search_npi_source(nearby) if on_search else _LIVE_NPI_SOURCE
    geo_source = f"{nearby.search_table_sql or nearby.address_table_sql} AS a{npi_join}"
    extra_clause = nearby.search_extra_clause if on_search else nearby.extra_clause
    filters = f"""
       AND a.lat IS NOT NULL
       AND a.long IS NOT NULL
       AND a.address_key IS NOT NULL{taxonomy_where}
       {nearby.geo_precision_clause}
       {nearby.geo_type_clause}
       {extra_clause}{nearby.ilike_clause}"""
    return npi_column, geo_source, filters


def build_nearby_page_edge_sql(nearby: NearbySqlQuery, *, legacy_page_clauses: tuple[str, str] | None = None) -> str:
    """Find how far the next ``:limit`` raw rows after the cursor reach inside the central share of the radius.

    Later identities cannot precede those rows, so a continuation only has to dedupe the disc that holds them.
    NULL means the rows run past ``:page_edge_share`` of the radius, where one read of the whole radius is cheaper.
    """
    nearby = nearby.with_legacy_plan_membership(legacy_page_clauses)
    npi_column, geo_source, filters = _native_geo_source(nearby)
    distance = f"{_nearby_geography(nearby)} <-> {_GEO_CENTER}"
    return f"""SELECT {distance} AS page_edge_meters
  FROM {geo_source}
 WHERE ST_DWithin({_nearby_geography(nearby)}, {_GEO_CENTER}, {_GEO_RADIUS} * :page_edge_share){filters}
   AND ({distance}, {npi_column}, a.address_key) > (
       CAST(:cursor_distance_meters AS double precision),
       CAST(:cursor_npi AS bigint),
       CAST(:cursor_address_key AS uuid))
 ORDER BY {distance}
OFFSET :limit - 1 LIMIT 1"""


def _native_eligible_geo_cte(
    nearby: NearbySqlQuery,
    *,
    projected_plan_membership: str | None = None,
    spatial_first: bool = False,
    cursor_bounded: bool = False,
) -> str:
    on_search = bool(nearby.search_table_sql)
    npi_column, geo_source, filters = _native_geo_source(nearby, spatial_first=spatial_first)
    row_keys = _address_row_key_columns(nearby.address_table_sql)
    address_key_columns = ",\n           ".join(f"a.{column} AS _geo_address_{column}" for column in row_keys)
    geography = _nearby_geography(nearby)
    center = _GEO_CENTER
    # <-> is spherical while ST_DWithin is spheroidal: widen a bounded disc by more than their largest difference.
    radius = f"LEAST({_GEO_RADIUS}, {_PAGE_EDGE} * 1.01)" if cursor_bounded else _GEO_RADIUS
    preceding_ctes = ""
    if projected_plan_membership is not None:
        if not on_search or not nearby.search_provider_facts:
            raise ValueError("plan membership prefilter requires a provider geo projection")
        columns = ", ".join(f"a.{column}" for column in dict.fromkeys(("npi", "address_key", "geo", "type", *row_keys)))
        # Use PostGIS's index bound, then check exact distance only for plan members.
        preceding_ctes = f"""plan_member_npis AS MATERIALIZED ({projected_plan_membership}),
bounding_members AS MATERIALIZED (
    SELECT {columns}
      FROM {geo_source}
     WHERE {geography} && _ST_Expand({center}, {radius}){filters}
       AND a.npi IN (SELECT npi FROM plan_member_npis)
),
"""
        npi_column = "a.npi"
        geo_source = "bounding_members AS a"
        filters = ""
    return f"""{preceding_ctes}{_NATIVE_GEO_RELATION} AS MATERIALIZED (
    SELECT DISTINCT ON ({npi_column}, a.address_key)
           {npi_column} AS npi_code,
           a.address_key,
           {geography} <-> {center} AS cursor_distance_meters,
           {address_key_columns}
      FROM {geo_source}
     WHERE ST_DWithin({geography}, {center}, {radius}){filters}
     ORDER BY {npi_column} ASC,
              a.address_key ASC,
              {geography} <-> {center} ASC,
              CASE a.type
                  WHEN 'primary' THEN 0
                  WHEN 'practice' THEN 1
                  WHEN 'site' THEN 2
                  WHEN 'secondary' THEN 3
                  ELSE 9
              END ASC,
              {nearby_row_tiebreaker(nearby.address_table_sql)}
)"""


def _nearby_geography(nearby: NearbySqlQuery) -> str:
    return (
        "a.geo"
        if nearby.search_table_sql
        else "Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision))"
    )


def _address_row_key_columns(address_table_sql: str) -> tuple[str, ...]:
    from api.provider_list_sql import _is_unified_address_table

    if _is_unified_address_table(address_table_sql):
        return ("location_key",)
    return ("checksum", "type")


def _selected_geo_cte(import_context: ProviderImportQuery, *, materialize_relation: bool | None = None) -> str:
    if materialize_relation is None:
        materialize_relation = import_context.require_match or import_context.has_native_scope
    if not import_context.prepared.normalized_order_terms:
        # Keep membership as a hashable subplan when spatial cardinality is underestimated.
        return (
            "selected_geo AS MATERIALIZED (\n"
            f"    SELECT * FROM {_NATIVE_GEO_RELATION}\n"
            f"     WHERE ({_NATIVE_GEO_RELATION}.npi_code::text IN ("
            f"SELECT entity_value FROM {_IMPORT_RELATION})) IS TRUE\n"
            ")"
        )

    import_columns = (
        "imported.entity_value AS _custom_import_entity_value",
        *(f"imported.sort_{ordinal}" for ordinal, _term in enumerate(import_context.prepared.normalized_order_terms)),
    )
    if materialize_relation:
        # Join the two materialized CTEs once via FULL JOIN + OFFSET 0 so PostgreSQL
        # cannot collapse it to an underestimated Nested Loop join.
        select_import_columns = ",\n                 ".join(import_columns)
        require_match_clause = (
            "\n       AND joined._custom_import_entity_value IS NOT NULL" if import_context.require_match else ""
        )
        return (
            "selected_geo AS MATERIALIZED (\n"
            "    SELECT joined.*\n"
            "      FROM (\n"
            f"          SELECT {_NATIVE_GEO_RELATION}.*,\n"
            f"                 {select_import_columns}\n"
            f"            FROM {_NATIVE_GEO_RELATION}\n"
            f"       FULL JOIN {_IMPORT_RELATION} AS imported\n"
            f"              ON imported.entity_value = {_NATIVE_GEO_RELATION}.npi_code::text\n"
            "          OFFSET 0\n"
            "      ) AS joined\n"
            f"     WHERE joined.npi_code IS NOT NULL{require_match_clause}\n"
            ")"
        )
    select_import_columns = ",\n           ".join(import_columns)
    return (
        "selected_geo AS MATERIALIZED (\n"
        f"    SELECT {_NATIVE_GEO_RELATION}.*,\n"
        f"           {select_import_columns}\n"
        f"      FROM {_NATIVE_GEO_RELATION}\n"
        f" LEFT JOIN {_IMPORT_RELATION} AS imported\n"
        f"        ON imported.entity_value = {_NATIVE_GEO_RELATION}.npi_code::text\n"
        ")"
    )


def _taxonomy_filter_where(nearby: NearbySqlQuery, *, on_search: bool = False, spatial_first: bool = False) -> str:
    if not nearby.use_taxonomy_filter:
        return ""
    # The uncorrelated taxonomy lookup runs once as an init plan.
    taxonomy_int_codes = f"(SELECT ARRAY_AGG(int_code) FROM mrf.nucc_taxonomy WHERE {nearby.taxonomy_conditions})"
    taxonomy_match = f"a.taxonomy_array && {taxonomy_int_codes}"
    typed_primary = nearby.primary_only and on_search and nearby.search_provider_facts
    if spatial_first and typed_primary:
        # Exact totals visit the whole radius. Keep overlap as a filter so the smaller spatial index can be used.
        taxonomy_match = f"COALESCE({taxonomy_match}, FALSE)"
    taxonomy_where = f"\n       AND {taxonomy_match}"
    if typed_primary:
        taxonomy_where += (
            "\n       AND a.primary_taxonomy_codes && "
            f"ARRAY(SELECT CAST(code AS text) FROM mrf.nucc_taxonomy WHERE {nearby.taxonomy_conditions})"
        )
    elif nearby.primary_only:
        taxonomy_where += (
            "\n       AND EXISTS ("
            "SELECT 1 FROM mrf.npi_taxonomy AS provider_taxonomy "
            "WHERE provider_taxonomy.npi = a.npi "
            "AND provider_taxonomy.healthcare_provider_taxonomy_code "
            f"= ANY (ARRAY(SELECT code FROM mrf.nucc_taxonomy WHERE {nearby.taxonomy_conditions})) "
            "AND UPPER(COALESCE("
            "provider_taxonomy.healthcare_provider_primary_taxonomy_switch, '')) = 'Y')"
        )
    return taxonomy_where


def nearby_row_tiebreaker(address_table_sql: str, *, alias: str = "a") -> str:
    """Return the deterministic address-column tie breaker for nearby SQL."""

    from api.provider_list_sql import _is_unified_address_table

    column_name = "location_key" if _is_unified_address_table(address_table_sql) else "type"
    return f"{alias}.{column_name} ASC"


def nearby_batch_identities(
    batch_rows: Sequence[Any],
) -> tuple[set[tuple[int, str]], tuple[float, int, str] | None]:
    """Return page identities and the final native KNN cursor from one batch."""

    batch_identities: set[tuple[int, str]] = set()
    last_cursor = None
    for batch_row in batch_rows:
        row_mapping = getattr(batch_row, "_mapping", None)
        if row_mapping is None:
            continue
        npi_value = row_mapping.get("npi_code") or row_mapping.get("npi")
        address_key_value = row_mapping.get("address_key")
        distance_value = row_mapping.get("cursor_distance_meters")
        if npi_value is None or address_key_value is None:
            continue
        batch_identities.add((int(npi_value), str(address_key_value).lower()))
        if distance_value is not None:
            last_cursor = (
                float(distance_value),
                int(npi_value),
                str(address_key_value),
            )
    return batch_identities, last_cursor


_NATIVE_NEARBY_SQL_TEMPLATE = """
WITH sub_s AS (
    SELECT d.npi AS npi_code,
           ROUND(
               CAST(
                   ST_Distance(
                       Geography(
                           ST_MakePoint(
                               (a.long)::double precision,
                               (a.lat)::double precision
                           )
                       ),
                       Geography(
                           ST_MakePoint(
                               CAST(:in_long AS double precision),
                               CAST(:in_lat AS double precision)
                           )
                       )
                   ) / 1609.34 AS NUMERIC
               ),
               2
           ) AS distance,
           Geography(
               ST_MakePoint(
                   (a.long)::double precision,
                   (a.lat)::double precision
               )
           ) <-> Geography(
               ST_MakePoint(
                   CAST(:in_long AS double precision),
                   CAST(:in_lat AS double precision)
               )
           ) AS cursor_distance_meters,
           a.*,
           d.*
      FROM {address_table_sql} AS a
      JOIN mrf.npi AS d ON d.npi = a.npi
     WHERE ST_DWithin(
               Geography(
                   ST_MakePoint(
                       (a.long)::double precision,
                       (a.lat)::double precision
                   )
               ),
               Geography(
                   ST_MakePoint(
                       CAST(:in_long AS double precision),
                       CAST(:in_lat AS double precision)
                   )
               ),
               :radius * 1609.34
           )
       AND a.lat IS NOT NULL
       AND a.long IS NOT NULL
       AND a.address_key IS NOT NULL
       {taxonomy_where}
       {geo_precision_clause}
       {geo_type_clause}
       {extra_clause}{ilike_clause}{cursor_clause}
     ORDER BY Geography(
                  ST_MakePoint(
                      (a.long)::double precision,
                      (a.lat)::double precision
                  )
              ) <-> Geography(
                  ST_MakePoint(
                      CAST(:in_long AS double precision),
                      CAST(:in_lat AS double precision)
                  )
              ) ASC,
              a.npi ASC,
              a.address_key ASC,
              CASE a.type
                  WHEN 'primary' THEN 0
                  WHEN 'practice' THEN 1
                  WHEN 'site' THEN 2
                  WHEN 'secondary' THEN 3
                  ELSE 9
              END ASC,
              {row_tiebreaker}
        LIMIT :limit
)
SELECT sub_s.*, taxonomy.*, nucc.display_name AS taxonomy_display
  FROM sub_s
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON sub_s.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc
    ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY sub_s.cursor_distance_meters ASC,
          sub_s.npi_code ASC,
          sub_s.address_key ASC,
          CASE sub_s.type
              WHEN 'primary' THEN 0
              WHEN 'practice' THEN 1
              WHEN 'site' THEN 2
              WHEN 'secondary' THEN 3
              ELSE 9
          END ASC,
          {outer_row_tiebreaker};
"""


_NATIVE_NEARBY_COUNT_SOURCE_TEMPLATE = """
  FROM {address_table_sql} AS a
  JOIN mrf.npi AS d ON d.npi = a.npi
 WHERE ST_DWithin(
           Geography(
               ST_MakePoint(
                   (a.long)::double precision,
                   (a.lat)::double precision
               )
           ),
           Geography(
               ST_MakePoint(
                   CAST(:in_long AS double precision),
                   CAST(:in_lat AS double precision)
               )
           ),
           :radius * 1609.34
       )
   AND a.lat IS NOT NULL
   AND a.long IS NOT NULL
   AND a.address_key IS NOT NULL
   {taxonomy_where}
   {geo_precision_clause}
   {geo_type_clause}
   {bbox_clause}
   {extra_clause}{ilike_clause}"""


def _native_cursor_page(nearby: NearbySqlQuery, cursor_clause: str, cursor_bounded: bool) -> str:
    """Dedupe eligible aliases before the cursor; a bounded disc keeps that cheap near the centre only."""
    edge_clause = f"\n       AND a.cursor_distance_meters <= {_PAGE_EDGE}" if cursor_bounded else ""
    return f"""WITH {_native_eligible_geo_cte(nearby, cursor_bounded=cursor_bounded)},
page_geo AS MATERIALIZED (
    SELECT a.*,
           ROW_NUMBER() OVER (
               ORDER BY a.cursor_distance_meters ASC, a.npi_code ASC, a.address_key ASC
           ) AS _geo_page_position
      FROM {_NATIVE_GEO_RELATION} AS a
     WHERE TRUE{cursor_clause}{edge_clause}
     ORDER BY a.cursor_distance_meters ASC, a.npi_code ASC, a.address_key ASC
     LIMIT :limit
),
{_hydrated_geo_cte(None, nearby)}
SELECT hydrated_geo.*, taxonomy.*, nucc.display_name AS taxonomy_display
  FROM hydrated_geo
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON hydrated_geo.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc
    ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY hydrated_geo._geo_page_position ASC,
          hydrated_geo.npi_code ASC,
          hydrated_geo.address_key ASC"""


def build_native_nearby_sql(
    nearby: NearbySqlQuery,
    *,
    cursor_clause: str = "",
    use_search_page: bool = False,
    legacy_page_clauses: tuple[str, str] | None = None,
    cursor_bounded: bool = False,
) -> str:
    """Keep initial KNN reads bounded and paginate canonical address identities.

    ``cursor_bounded`` dedupes only the disc within ``:page_edge_meters`` and returns the identities inside it.
    """

    if nearby.search_table_sql and use_search_page and not cursor_clause:
        return _native_search_page(nearby)
    if cursor_clause:
        return _native_cursor_page(
            nearby.with_legacy_plan_membership(legacy_page_clauses), cursor_clause, cursor_bounded
        )

    legacy_initial_clause, _ = legacy_page_clauses or (None, None)
    extra_clause = nearby.extra_clause if legacy_initial_clause is None else legacy_initial_clause
    if re.fullmatch(r"[a-z_][a-z_0-9]*\.entity_address_unified", nearby.address_table_sql):
        zip_filter = _address_zip5_filter("a", nearby.address_table_sql, any_array=True)
        if f"AND {zip_filter}" in map(str.strip, extra_clause.splitlines()):
            # Let a custom single-ZIP plan intersect both indexes without steering generic plans.
            extra_clause += (
                "\n          AND (cardinality(:zip_codes) <> 1 OR a.zip5 = ANY (:zip_codes) OR a.zip5 IS NULL)"
            )
    taxonomy_where = _taxonomy_filter_where(nearby)
    return _NATIVE_NEARBY_SQL_TEMPLATE.format(
        taxonomy_where=taxonomy_where,
        geo_precision_clause=nearby.geo_precision_clause,
        geo_type_clause=nearby.geo_type_clause,
        extra_clause=extra_clause,
        ilike_clause=nearby.ilike_clause,
        cursor_clause=cursor_clause,
        row_tiebreaker=nearby_row_tiebreaker(nearby.address_table_sql),
        outer_row_tiebreaker=nearby_row_tiebreaker(
            nearby.address_table_sql,
            alias="sub_s",
        ),
        address_table_sql=nearby.address_table_sql,
    )


async def fetch_nearby_batch(
    connection,
    nearby: NearbySqlQuery,
    cursor_clause: str,
    parameters_by_name: Mapping[str, object],
    page_options_by_name: Mapping[str, Any],
    *,
    is_requested_continuation: bool,
) -> Sequence[Any]:
    """Read the native batch after the cursor, deduping only the disc that holds it when the cursor is central.

    A disc whose rows all repeat served identities is skipped: the read resumes just past its edge. Farther out,
    and for the first page, one read of the whole radius serves the batch.
    """
    batch_parameters_by_name = dict(parameters_by_name)
    edge_sql = build_nearby_page_edge_sql(nearby, legacy_page_clauses=page_options_by_name.get("legacy_page_clauses"))
    for _attempt in range(_BOUNDED_CONTINUATION_ATTEMPTS if is_requested_continuation else 0):
        central_meters = BOUNDED_CONTINUATION_RADIUS_SHARE * float(batch_parameters_by_name["radius"]) * 1609.34
        if batch_parameters_by_name["cursor_distance_meters"] > central_meters:
            break
        async with nearest_first_pages(connection, True):
            # Search past the central share so a batch that straddles it still finds its edge.
            edge_rows = await connection.all(
                text(edge_sql), page_edge_share=2 * BOUNDED_CONTINUATION_RADIUS_SHARE, **batch_parameters_by_name
            )
            if not edge_rows:
                break
            page_edge_meters = float(edge_rows[0][0])
            bounded_sql = build_native_nearby_sql(
                nearby, cursor_clause=cursor_clause, cursor_bounded=True, **page_options_by_name
            )
            batch_rows = await connection.all(
                text(bounded_sql), page_edge_meters=page_edge_meters, **batch_parameters_by_name
            )
        if batch_rows:
            return batch_rows
        batch_parameters_by_name.update(_PAST_EVERY_IDENTITY, cursor_distance_meters=page_edge_meters)
    radius_sql = build_native_nearby_sql(nearby, cursor_clause=cursor_clause, **page_options_by_name)
    return await connection.all(text(radius_sql), **batch_parameters_by_name)


def build_native_nearby_count_sql(
    nearby: NearbySqlQuery,
    *,
    bbox_clause: str = "",
    legacy_plan: tuple[str, Sequence[str]] | None = None,
) -> str:
    """Build the exact native nearby count query from normalized fragments."""

    if nearby.search_table_sql:
        return _search_nearby_count_sql(nearby, bbox_clause)
    plan_member_sql, filters = legacy_plan or (None, ())
    extra_clause = nearby.extra_clause if plan_member_sql is None else nearby_extra_clauses(filters, None, 0)[0]
    source_sql = _NATIVE_NEARBY_COUNT_SOURCE_TEMPLATE.format(
        taxonomy_where=_taxonomy_filter_where(nearby),
        geo_precision_clause=nearby.geo_precision_clause,
        geo_type_clause=nearby.geo_type_clause,
        bbox_clause=bbox_clause,
        extra_clause=extra_clause,
        ilike_clause=nearby.ilike_clause,
        address_table_sql=nearby.address_table_sql,
    )
    if plan_member_sql is None:
        return f"\nSELECT COUNT(DISTINCT (a.npi, a.address_key)) AS total_count{source_sql};\n"
    # Evaluate the spatial and office predicates before matching the plan's members.
    return f"""WITH local_rows AS MATERIALIZED (
    SELECT a.npi, a.address_key{source_sql}
)
SELECT COUNT(DISTINCT (npi, address_key)) AS total_count
  FROM local_rows
 WHERE npi IN ({plan_member_sql});"""


def _search_nearby_count_sql(nearby: NearbySqlQuery, bbox_clause: str) -> str:
    """Count exact projected office identities with at most one existence probe per NPI."""
    taxonomy_where = _taxonomy_filter_where(nearby, on_search=True)
    name_scope = name_filter = ""
    if nearby.search_provider_facts and nearby.ilike_clause and not _PROVIDER_COLUMN.search(nearby.search_extra_clause):
        # Let the trigram index select names once before matching projected offices.
        name_scope = f"""WITH matched_npis AS MATERIALIZED (
    SELECT d.npi FROM mrf.npi AS d WHERE TRUE{nearby.ilike_clause}
)
"""
        name_filter = "\n   AND a.npi IN (SELECT npi FROM matched_npis)"
        nearby = replace(nearby, ilike_clause="")
    predicates = _search_geo_predicates(nearby, taxonomy_where) + name_filter + "\n   " + bbox_clause
    if not nearby.search_provider_facts and not _PROVIDER_COLUMN.search(predicates):
        return f"""WITH nearby_npis AS MATERIALIZED (
    SELECT a.npi, count(DISTINCT a.address_key) AS office_count
      FROM {nearby.search_table_sql} AS a
     WHERE {predicates}
     GROUP BY a.npi
)
SELECT COALESCE(sum(nearby_npis.office_count), 0)::bigint AS total_count
  FROM nearby_npis
 WHERE EXISTS (SELECT 1 FROM mrf.npi AS d WHERE d.npi = nearby_npis.npi);"""
    total = (
        "COUNT(*) FILTER (WHERE NOT a.shares_identity) "
        "+ COUNT(DISTINCT (a.npi, a.address_key)) FILTER (WHERE a.shares_identity)"
        if nearby.search_provider_facts
        else "COUNT(DISTINCT (a.npi, a.address_key))"
    )
    return f"""{name_scope}SELECT {total} AS total_count
  FROM {nearby.search_table_sql} AS a{_search_npi_source(nearby)[1]}
 WHERE {predicates};"""


GEO_ENTITY_TYPE_PREDICATE = "d.entity_type_code = :entity_type_code"
_LIVE_NPI_SOURCE = ("d.npi", "\n      JOIN mrf.npi AS d ON d.npi = a.npi")
_PROVIDER_COLUMN = re.compile(r"(?<![\w.])d\s*\.")


def _search_npi_source(nearby: NearbySqlQuery) -> tuple[str, str]:
    """Skip the provider join when the search rows already prove NPI existence and no provider column is used."""
    if nearby.search_provider_facts and not _PROVIDER_COLUMN.search(nearby.search_extra_clause + nearby.ilike_clause):
        return "a.npi", ""
    return _LIVE_NPI_SOURCE


def _search_geo_predicates(nearby: NearbySqlQuery, taxonomy_where: str) -> str:
    return f"""ST_DWithin(
           a.geo,
           Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision))),
           :radius * 1609.34)
   AND a.lat IS NOT NULL
   AND a.long IS NOT NULL
   AND a.address_key IS NOT NULL{taxonomy_where}
   {nearby.geo_precision_clause}
   {nearby.geo_type_clause}
   {nearby.search_extra_clause}{nearby.ilike_clause}"""


def _native_search_page(nearby: NearbySqlQuery) -> str:
    """Limit physical search rows before hydrating wide provider/address records."""
    taxonomy_where = _taxonomy_filter_where(nearby, on_search=True)
    npi_column, npi_join = _search_npi_source(nearby)
    return f"""WITH page_geo AS MATERIALIZED (
    SELECT {npi_column} AS npi_code, a.address_key, a.location_key AS _geo_address_location_key,
           a.geo <-> Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision)))
               AS cursor_distance_meters
      FROM {nearby.search_table_sql} AS a{npi_join}
     WHERE {_search_geo_predicates(nearby, taxonomy_where)}
     ORDER BY a.geo <-> Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision))),
              a.npi, a.address_key,
              CASE a.type WHEN 'primary' THEN 0 WHEN 'practice' THEN 1 WHEN 'site' THEN 2 WHEN 'secondary' THEN 3 ELSE 9 END,
              a.location_key
     LIMIT :limit
),
{_hydrated_geo_cte(None, nearby, include_position=False)}
SELECT hydrated_geo.*, taxonomy.*, nucc.display_name AS taxonomy_display
  FROM hydrated_geo
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON hydrated_geo.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY hydrated_geo.cursor_distance_meters, hydrated_geo.npi_code, hydrated_geo.address_key,
          CASE hydrated_geo.type WHEN 'primary' THEN 0 WHEN 'practice' THEN 1 WHEN 'site' THEN 2 WHEN 'secondary' THEN 3 ELSE 9 END,
          hydrated_geo.location_key;"""


def _anchor_ctes() -> str:
    return (
        "cursor_anchor_rows AS MATERIALIZED (\n"
        "    SELECT *\n"
        "      FROM selected_geo\n"
        f"     WHERE npi_code::text = :{_CURSOR_NPI_PARAMETER}\n"
        f"       AND address_key = CAST(:{_CURSOR_ADDRESS_KEY_PARAMETER} AS uuid)\n"
        ")"
    )


def _page_statement(
    common_ctes: str,
    import_context: ProviderImportQuery,
    nearby: NearbySqlQuery,
    *,
    has_anchor: bool,
):
    order_clause = _order_clause(import_context, "selected_geo")
    page_order_clause = _order_clause(import_context, "page_rows")
    anchor_join = "\n      CROSS JOIN cursor_anchor" if has_anchor else ""
    keyset_clause = _keyset_clause(import_context, "selected_geo", "cursor_anchor") if has_anchor else ""
    anchor_count = ", (SELECT COUNT(*) FROM cursor_anchor_rows) AS _geo_anchor_count" if has_anchor else ""
    return _statement(
        f"""WITH {common_ctes},
geo_totals AS (SELECT COUNT(*) AS _geo_total{anchor_count} FROM selected_geo),
page_geo AS MATERIALIZED (
    SELECT page_rows.*,
           ROW_NUMBER() OVER (ORDER BY {page_order_clause}) AS _geo_page_position
      FROM (
          SELECT selected_geo.*
            FROM selected_geo{anchor_join}{keyset_clause}
           ORDER BY {order_clause}
           LIMIT :{_PAGE_LIMIT_PARAMETER}
      ) AS page_rows
),
{_hydrated_geo_cte(import_context, nearby)}
SELECT hydrated_geo.*, taxonomy.*, nucc.display_name AS taxonomy_display, geo_totals.*
  FROM geo_totals
  LEFT JOIN hydrated_geo ON TRUE
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON hydrated_geo.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc
    ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY hydrated_geo._geo_page_position ASC,
          hydrated_geo.npi_code ASC,
          hydrated_geo.address_key ASC""",
        import_context,
    )


def _hydrated_geo_cte(
    import_context: ProviderImportQuery | None, nearby: NearbySqlQuery, *, include_position: bool = True
) -> str:
    address_key_join = "".join(
        f"\n       AND a.{column} = page_geo._geo_address_{column}"
        for column in _address_row_key_columns(nearby.address_table_sql)
    )
    import_columns = ""
    if import_context is not None and import_context.prepared.normalized_order_terms:
        import_columns = ",\n           page_geo._custom_import_entity_value" + "".join(
            f",\n           page_geo.sort_{ordinal}"
            for ordinal, _term in enumerate(import_context.prepared.normalized_order_terms)
        )
    position_column = ",\n           page_geo._geo_page_position" if include_position else ""
    return f"""hydrated_geo AS MATERIALIZED (
    SELECT page_geo.npi_code,
           ROUND(
               CAST(
                   ST_Distance(
                       Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision)),
                       Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision)))
                   ) / 1609.34 AS NUMERIC
               ),
               2
           ) AS distance,
           page_geo.cursor_distance_meters,
           a.*, d.*{import_columns}{position_column}
      FROM page_geo
      JOIN {nearby.address_table_sql} AS a
        ON a.npi = page_geo.npi_code{address_key_join}
      JOIN mrf.npi AS d ON d.npi = page_geo.npi_code
)"""


def _order_clause(import_context: ProviderImportQuery, alias: str) -> str:
    order_terms = import_context.prepared.normalized_order_terms
    if not order_terms:
        return f"{alias}.cursor_distance_meters ASC, {alias}.npi_code ASC, {alias}.address_key ASC"
    terms = [f"({alias}._custom_import_entity_value IS NULL) ASC"]
    terms.extend(
        f"{alias}.sort_{ordinal} {term.direction.upper()} NULLS LAST" for ordinal, term in enumerate(order_terms)
    )
    terms.extend(
        (
            f"{alias}.cursor_distance_meters ASC",
            f"{alias}.npi_code ASC",
            f"{alias}.address_key ASC",
        )
    )
    return ", ".join(terms)


def _keyset_clause(
    import_context: ProviderImportQuery,
    candidate_alias: str,
    anchor_alias: str,
) -> str:
    order_terms = import_context.prepared.normalized_order_terms
    if not order_terms:
        order_components = [
            (f"{candidate_alias}.cursor_distance_meters", f"{anchor_alias}.cursor_distance_meters", "asc", False),
            (f"{candidate_alias}.npi_code", f"{anchor_alias}.npi_code", "asc", False),
            (f"{candidate_alias}.address_key", f"{anchor_alias}.address_key", "asc", False),
        ]
    else:
        order_components = [
            (
                f"({candidate_alias}._custom_import_entity_value IS NULL)",
                f"({anchor_alias}._custom_import_entity_value IS NULL)",
                "asc",
                False,
            ),
            *(
                (
                    f"{candidate_alias}.sort_{ordinal}",
                    f"{anchor_alias}.sort_{ordinal}",
                    term.direction,
                    True,
                )
                for ordinal, term in enumerate(order_terms)
            ),
            (f"{candidate_alias}.cursor_distance_meters", f"{anchor_alias}.cursor_distance_meters", "asc", False),
            (f"{candidate_alias}.npi_code", f"{anchor_alias}.npi_code", "asc", False),
            (f"{candidate_alias}.address_key", f"{anchor_alias}.address_key", "asc", False),
        ]
    prefixes: list[str] = []
    later_clauses: list[str] = []
    for candidate, anchor, direction, nullable in order_components:
        later_clauses.append(
            "(" + " AND ".join([*prefixes, _after_value(candidate, anchor, direction, nullable)]) + ")"
        )
        prefixes.append(f"{candidate} IS NOT DISTINCT FROM {anchor}")
    return "\n     WHERE (" + " OR ".join(later_clauses) + ")"


def _after_value(candidate: str, anchor: str, direction: str, nullable: bool) -> str:
    operator = ">" if direction == "asc" else "<"
    if not nullable:
        return f"{candidate} {operator} {anchor}"
    return f"({anchor} IS NOT NULL AND ({candidate} IS NULL OR {candidate} {operator} {anchor}))"


def _normalized_cursor_anchor(anchor: tuple[str, str] | None) -> tuple[str, str] | None:
    if anchor is None:
        return None
    if (
        type(anchor) is not tuple
        or len(anchor) != 2
        or type(anchor[0]) is not str
        or type(anchor[1]) is not str
        or len(anchor[0]) != 10
        or not anchor[0].isascii()
        or not anchor[0].isdigit()
    ):
        raise ValueError("custom-import geo cursor anchor is invalid")
    try:
        address_key = str(UUID(anchor[1]))
    except ValueError as exc:
        raise ValueError("custom-import geo cursor anchor is invalid") from exc
    return anchor[0], address_key


def _add_parameter(parameters_by_name: dict[str, object], name: str, value: object) -> None:
    if name in parameters_by_name:
        raise ValueError("native and custom-import geo SQL parameters collide")
    parameters_by_name[name] = value


def _statement(sql: str, import_context: ProviderImportQuery):
    return text(sql).bindparams(*import_context.compiled.typed_binds)


__all__ = (
    "ImportedGeoQuery",
    "ImportedGeoStatements",
    "NearbySqlQuery",
    "build_imported_geo_statements",
    "build_native_nearby_count_sql",
    "build_native_nearby_sql",
    "build_nearby_page_edge_sql",
    "fetch_nearby_batch",
    "nearby_batch_identities",
    "nearby_extra_clauses",
    "nearby_row_tiebreaker",
)
