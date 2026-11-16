# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded provider enrichment SQL and atomic office selection."""

from functools import partial

from api.ptg2_address_policy import address_display_rank_sql

_PROVIDER_ENRICHMENT_SQL = """
    WITH source_npis AS MATERIALIZED (
        SELECT UNNEST(CAST(:npis AS bigint[])) AS npi
    )
    {fallback_cte}
    SELECT
        source_npis.npi,
        selected_addr.location_hash,
        selected_addr.state_name AS state,
        selected_addr.city_name AS city,
        LEFT(COALESCE(selected_addr.postal_code, ''), 5) AS zip5,
        selected_addr.location_source,
        selected_addr.location_source AS location_confidence_code,
        jsonb_build_object(
            'npi', source_npis.npi, 'type', selected_addr.type,
            'checksum', selected_addr.checksum,
            'first_line', selected_addr.first_line,
            'second_line', selected_addr.second_line,
            'city_name', selected_addr.city_name,
            'state_name', selected_addr.state_name,
            'city', selected_addr.city_name,
            'state', selected_addr.state_name,
            'postal_code', selected_addr.postal_code,
            'country_code', selected_addr.country_code,
            {address_source_count_pair_sql}
            'telephone_number', selected_addr.telephone_number,
            'fax_number', selected_addr.fax_number,
            'phone_number', selected_addr.phone_number,
            'phone_extension', selected_addr.phone_extension,
            'fax_number_digits', selected_addr.fax_number_digits,
            'fax_extension', selected_addr.fax_extension,
            'address_key', selected_addr.address_key,
            'address_site_key', selected_addr.address_site_key,
            'lat', selected_addr.lat, 'long', selected_addr.long
        )::text AS address_payload,
        selected_addr.telephone_number,
        selected_addr.fax_number,
        selected_addr.phone_number,
        selected_addr.phone_extension,
        selected_addr.fax_number_digits,
        selected_addr.fax_extension,
        COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
        COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties,
        COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
        COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations,
        tax.primary_specialty, tax.primary_specialization,
        n.provider_sex_code, {provider_name_sql} AS provider_name
    FROM source_npis
    LEFT JOIN {npi_data_table} n ON n.npi = source_npis.npi
    LEFT JOIN LATERAL (
        SELECT addr.*, {postal_box_rank_sql} AS display_rank
          FROM {npi_address_table} addr
         WHERE addr.npi = source_npis.npi
         {address_source_count_sql}
         ORDER BY {postal_box_rank_sql},
                  (addr.type = 'primary') DESC, addr.type, addr.checksum
         LIMIT 1
    ) addr ON TRUE
    {fallback_join}
    {selected_address_join}
    {taxonomy_lateral_sql}
    ORDER BY provider_name, source_npis.npi
"""


_PROVIDER_ENRICHMENT_ADDRESS_COLUMNS = (
    "type",
    "checksum",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "phone_number",
    "phone_extension",
    "fax_number_digits",
    "fax_extension",
    "lat",
    "long",
)


_PROVIDER_ENRICHMENT_FALLBACK_JOIN_SQL = """
    LEFT JOIN fallback_addresses na
      ON na.npi = source_npis.npi
     AND (
         addr.npi IS NULL
         OR addr.display_rank = 2
         OR (addr.display_rank = 1 AND na.display_rank = 0)
     )"""


def _fallback_address_column_sql(
    column: str,
    fallback_columns: set[str],
    sql_type: str = "varchar",
) -> str:
    """Select an available legacy column or a typed null."""

    if column in fallback_columns:
        return f"na.{column}"
    return f"NULL::{sql_type} AS {column}"


def _provider_enrichment_fallback_cte_sql(
    fallback_columns: set[str],
) -> str:
    """Build the source-NPI-bounded best legacy address CTE."""

    from api import ptg2_serving as serving

    fallback_column_sql = partial(
        _fallback_address_column_sql,
        fallback_columns=fallback_columns,
    )
    return f"""
    , fallback_addresses AS MATERIALIZED (
        SELECT DISTINCT ON (na.npi)
               na.npi, na.type, na.checksum, na.address_key,
               na.first_line, na.second_line, na.city_name,
               na.state_name, na.postal_code, na.country_code,
               {fallback_column_sql("telephone_number")},
               {fallback_column_sql("fax_number")},
               {fallback_column_sql("phone_number")},
               {fallback_column_sql("phone_extension")},
               {fallback_column_sql("fax_number_digits")},
               {fallback_column_sql("fax_extension")},
               na.lat, na.long,
               {address_display_rank_sql("na")} AS display_rank
          FROM {serving.PTG2_SCHEMA}.npi_address na
          JOIN source_npis source_filter ON source_filter.npi = na.npi
         WHERE {address_display_rank_sql("na")} < 2
         ORDER BY na.npi,
                  {address_display_rank_sql("na")},
                  CASE na.type WHEN 'primary' THEN 0
                       WHEN 'practice' THEN 1
                       WHEN 'secondary' THEN 2 ELSE 3 END,
                  na.checksum
    )"""


def _unified_provider_selected_address_join_sql() -> str:
    """Switch all address-owned fields together using one fallback marker."""

    selected_column_sql = ",\n               ".join(
        f"CASE WHEN na.npi IS NOT NULL THEN na.{column} ELSE addr.{column} END AS {column}"
        for column in _PROVIDER_ENRICHMENT_ADDRESS_COLUMNS
    )
    return f"""
    LEFT JOIN LATERAL (
        SELECT {selected_column_sql},
               CASE WHEN na.npi IS NOT NULL
                    THEN na.address_key::text
                    ELSE addr.address_key::text END AS address_key,
               CASE WHEN na.npi IS NOT NULL
                    THEN NULL::text
                    ELSE addr.premise_key::text END AS address_site_key,
               CASE WHEN na.npi IS NOT NULL
                    THEN CONCAT(
                        'npi_address:', na.npi, ':',
                        na.type, ':', na.checksum
                    )
                    ELSE CONCAT(
                        'entity_address_unified:', addr.location_key
                    ) END AS location_hash,
               CASE WHEN na.npi IS NOT NULL
                    THEN 'npi_address'::varchar
                    ELSE 'entity_address_unified'::varchar
                END AS location_source
    ) selected_addr ON TRUE"""
