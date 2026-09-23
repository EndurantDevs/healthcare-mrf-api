# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compose imported-field ordering into exact nearby-provider SQL."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any
from uuid import UUID

from sqlalchemy import text

from api.custom_import_provider_sql import ProviderImportQuery, merge_native_params

_IMPORT_RELATION = "custom_import_provider_relation"
_PAGE_LIMIT_PARAMETER = "__custom_import_geo_page_limit"
_CURSOR_NPI_PARAMETER = "__custom_import_geo_cursor_npi"
_CURSOR_ADDRESS_KEY_PARAMETER = "__custom_import_geo_cursor_address_key"


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


@dataclass(frozen=True, slots=True)
class ImportedGeoQuery:
    """The normalized native query and page state for an imported geo read."""

    nearby: NearbySqlQuery
    native_parameters: Mapping[str, object]
    limit: int
    cursor_anchor: tuple[str, str] | None


def build_imported_geo_statements(
    import_context: ProviderImportQuery,
    imported_query: ImportedGeoQuery,
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

    parameters_by_name = dict(imported_query.native_parameters)
    _add_parameter(parameters_by_name, _PAGE_LIMIT_PARAMETER, imported_query.limit + 1)
    normalized_anchor = _normalized_cursor_anchor(imported_query.cursor_anchor)
    if normalized_anchor is not None:
        _add_parameter(parameters_by_name, _CURSOR_NPI_PARAMETER, normalized_anchor[0])
        _add_parameter(
            parameters_by_name,
            _CURSOR_ADDRESS_KEY_PARAMETER,
            normalized_anchor[1],
        )
    parameters_by_name = merge_native_params(parameters_by_name, import_context.compiled)

    common_ctes = _common_ctes(
        import_context=import_context,
        nearby=imported_query.nearby,
    )
    count = _statement(
        f"WITH {common_ctes}\nSELECT COUNT(*) AS total_count\n  FROM selected_geo",
        import_context,
    )
    if normalized_anchor is None:
        return ImportedGeoStatements(
            count=count,
            page=_page_statement(common_ctes, import_context, has_anchor=False),
            anchor=None,
            parameters=MappingProxyType(parameters_by_name),
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
            has_anchor=True,
        ),
        anchor=anchor,
        parameters=MappingProxyType(parameters_by_name),
    )


def _common_ctes(
    *,
    import_context: ProviderImportQuery,
    nearby: NearbySqlQuery,
) -> str:
    relation_cte = f"{_IMPORT_RELATION} AS ({import_context.compiled.sql})"
    eligible_cte = _eligible_geo_cte(
        import_context,
        nearby,
    )
    selected_cte = _selected_geo_cte(import_context)
    return f"{relation_cte},\n{eligible_cte},\n{selected_cte}"


def _eligible_geo_cte(
    import_context: ProviderImportQuery,
    nearby: NearbySqlQuery,
) -> str:
    taxonomy_from, taxonomy_where = _taxonomy_filter_parts(nearby)
    membership_clause = _membership_clause(import_context)
    return f"""eligible_geo AS MATERIALIZED (
    SELECT DISTINCT ON (d.npi, a.address_key)
           d.npi AS npi_code,
           ROUND(
               CAST(
                   ST_Distance(
                       Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision)),
                       Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision)))
                   ) / 1609.34 AS NUMERIC
               ),
               2
           ) AS distance,
           Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision))
               <-> Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision)))
               AS cursor_distance_meters,
           a.*, d.*
      FROM {nearby.address_table_sql} AS a
      JOIN mrf.npi AS d ON d.npi = a.npi{taxonomy_from}
     WHERE ST_DWithin(
               Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision)),
               Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision))),
               :radius * 1609.34
           )
       AND a.lat IS NOT NULL
       AND a.long IS NOT NULL
       AND a.address_key IS NOT NULL{taxonomy_where}
       {nearby.geo_precision_clause}
       {nearby.geo_type_clause}
       {nearby.extra_clause}{nearby.ilike_clause}{membership_clause}
     ORDER BY d.npi ASC,
              a.address_key ASC,
              Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision))
                  <-> Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision))) ASC,
              CASE a.type
                  WHEN 'primary' THEN 0
                  WHEN 'practice' THEN 1
                  WHEN 'site' THEN 2
                  WHEN 'secondary' THEN 3
                  ELSE 9
              END ASC,
              {nearby_row_tiebreaker(nearby.address_table_sql)}
)"""


def _selected_geo_cte(import_context: ProviderImportQuery) -> str:
    if not import_context.prepared.normalized_order_terms:
        return "selected_geo AS MATERIALIZED (\n    SELECT * FROM eligible_geo\n)"

    select_import_columns = ",\n           ".join(
        [
            "imported.entity_value AS _custom_import_entity_value",
            *(
                f"imported.sort_{ordinal}"
                for ordinal, _term in enumerate(import_context.prepared.normalized_order_terms)
            ),
        ]
    )
    require_match_clause = "\n     WHERE imported.entity_value IS NOT NULL" if import_context.require_match else ""
    return (
        "selected_geo AS MATERIALIZED (\n"
        "    SELECT eligible_geo.*,\n"
        f"           {select_import_columns}\n"
        "      FROM eligible_geo\n"
        f" LEFT JOIN {_IMPORT_RELATION} AS imported\n"
        "        ON imported.entity_value = eligible_geo.npi_code::text"
        f"{require_match_clause}\n"
        ")"
    )


def _taxonomy_filter_parts(
    nearby: NearbySqlQuery,
    *,
    filter_alias: str = "taxonomy_filter",
) -> tuple[str, str]:
    if not nearby.use_taxonomy_filter:
        return "", ""
    taxonomy_from = (
        "\n      CROSS JOIN (\n"
        "            SELECT ARRAY_AGG(code) AS taxonomy_codes,\n"
        "                   ARRAY_AGG(int_code) AS codes\n"
        "              FROM mrf.nucc_taxonomy\n"
        f"             WHERE {nearby.taxonomy_conditions}\n"
        f"      ) AS {filter_alias}"
    )
    taxonomy_where = f"\n       AND a.taxonomy_array && {filter_alias}.codes"
    if nearby.primary_only:
        taxonomy_where += (
            "\n       AND EXISTS ("
            "SELECT 1 FROM mrf.npi_taxonomy AS provider_taxonomy "
            "WHERE provider_taxonomy.npi = a.npi "
            "AND provider_taxonomy.healthcare_provider_taxonomy_code "
            f"= ANY({filter_alias}.taxonomy_codes) "
            "AND UPPER(COALESCE("
            "provider_taxonomy.healthcare_provider_primary_taxonomy_switch, '')) = 'Y')"
        )
    return taxonomy_from, taxonomy_where


def _membership_clause(import_context: ProviderImportQuery) -> str:
    if import_context.prepared.normalized_order_terms or not import_context.require_match:
        return ""
    return (
        f"\n       AND EXISTS (SELECT 1 FROM {_IMPORT_RELATION} AS imported WHERE imported.entity_value = d.npi::text)"
    )


def nearby_row_tiebreaker(address_table_sql: str, *, alias: str = "a") -> str:
    """Return the deterministic address-column tie breaker for nearby SQL."""

    column_name = "location_key" if address_table_sql.endswith(".entity_address_unified") else "type"
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
      JOIN mrf.npi AS d ON d.npi = a.npi{taxonomy_from}
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


_NATIVE_NEARBY_COUNT_SQL_TEMPLATE = """
SELECT COUNT(DISTINCT (a.npi, a.address_key)) AS total_count
  FROM {address_table_sql} AS a
  JOIN mrf.npi AS d ON d.npi = a.npi{taxonomy_from}
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
   {extra_clause}{ilike_clause};
"""


def build_native_nearby_sql(
    nearby: NearbySqlQuery,
    *,
    cursor_clause: str = "",
) -> str:
    """Build the bounded native KNN query from normalized nearby fragments."""

    taxonomy_from, taxonomy_where = _taxonomy_filter_parts(
        nearby,
        filter_alias="g",
    )
    return _NATIVE_NEARBY_SQL_TEMPLATE.format(
        taxonomy_from=taxonomy_from,
        taxonomy_where=taxonomy_where,
        geo_precision_clause=nearby.geo_precision_clause,
        geo_type_clause=nearby.geo_type_clause,
        extra_clause=nearby.extra_clause,
        ilike_clause=nearby.ilike_clause,
        cursor_clause=cursor_clause,
        row_tiebreaker=nearby_row_tiebreaker(nearby.address_table_sql),
        outer_row_tiebreaker=nearby_row_tiebreaker(
            nearby.address_table_sql,
            alias="sub_s",
        ),
        address_table_sql=nearby.address_table_sql,
    )


def build_native_nearby_count_sql(
    nearby: NearbySqlQuery,
    *,
    bbox_clause: str = "",
) -> str:
    """Build the exact native nearby count query from normalized fragments."""

    taxonomy_from, taxonomy_where = _taxonomy_filter_parts(
        nearby,
        filter_alias="g",
    )
    return _NATIVE_NEARBY_COUNT_SQL_TEMPLATE.format(
        taxonomy_from=taxonomy_from,
        taxonomy_where=taxonomy_where,
        geo_precision_clause=nearby.geo_precision_clause,
        geo_type_clause=nearby.geo_type_clause,
        bbox_clause=bbox_clause,
        extra_clause=nearby.extra_clause,
        ilike_clause=nearby.ilike_clause,
        address_table_sql=nearby.address_table_sql,
    )


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
    *,
    has_anchor: bool,
):
    order_clause = _order_clause(import_context, "selected_geo")
    anchor_join = "\n      CROSS JOIN cursor_anchor" if has_anchor else ""
    keyset_clause = _keyset_clause(import_context, "selected_geo", "cursor_anchor") if has_anchor else ""
    return _statement(
        f"""WITH {common_ctes},
page_geo AS MATERIALIZED (
    SELECT selected_geo.*,
           ROW_NUMBER() OVER (ORDER BY {order_clause}) AS _geo_page_position
      FROM selected_geo{anchor_join}{keyset_clause}
     ORDER BY {order_clause}
     LIMIT :{_PAGE_LIMIT_PARAMETER}
)
SELECT page_geo.*, taxonomy.*, nucc.display_name AS taxonomy_display
  FROM page_geo
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON page_geo.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc
    ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY page_geo._geo_page_position ASC,
          page_geo.npi_code ASC,
          page_geo.address_key ASC""",
        import_context,
    )


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
    "nearby_batch_identities",
    "nearby_row_tiebreaker",
)
