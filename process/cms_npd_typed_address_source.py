# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The CMS NPD provider address table as one NPI-keyed ``practice`` source of the unified address table.

Rows use the same ``npi`` entity identity as the other NPI sources, so an address that another source
already supplies for the NPI merges with it (same type) or corroborates it through source evidence.
"""

from process.cms_npd_typed_addresses import ADDRESS_TABLE

ADDRESS_SOURCE = "cms_npd"

_SELECT = """
            SELECT
                'npi'::varchar AS entity_type,
                c.npi::varchar AS entity_id,
                c.npi::bigint AS npi,
                NULL::bigint AS inferred_npi,
                NULL::float8 AS inference_confidence,
                NULL::varchar AS inference_method,
                {entity_name} AS entity_name,
                {entity_subtype} AS entity_subtype,
                'practice'::varchar AS type,
                {taxonomy_array} AS taxonomy_array,
                {plans_network_array} AS plans_network_array,
                {procedures_array} AS procedures_array,
                {medications_array} AS medications_array,
                ARRAY[]::varchar[] AS aca_plan_array,
                ARRAY[]::varchar[] AS aca_network_array,
                ARRAY[]::varchar[] AS ptg_plan_array,
                ARRAY[]::varchar[] AS ptg_source_array,
                ARRAY[]::varchar[] AS group_plan_array,
                '{base_address_version}'::varchar AS base_address_version,
                c.first_line::varchar AS first_line,
                c.second_line::varchar AS second_line,
                c.city_name::varchar AS city_name,
                c.state_name::varchar AS state_name,
                LEFT(c.postal_code, 5)::varchar AS postal_code,
                c.country_code::varchar AS country_code,
                c.telephone_number::varchar AS telephone_number,
                c.fax_number::varchar AS fax_number,
                NULL::varchar AS formatted_address,
                c.latitude::numeric AS lat,
                c.longitude::numeric AS long,
                NULL::date AS date_added,
                NULL::varchar AS place_id,
                c.address_key::uuid AS address_key,
                COALESCE(c.updated_at, {now})::timestamp AS updated_at,
                '{address_source}'::varchar AS address_source,
                c.source_record_id::varchar AS source_record_id
              FROM {schema}.{table} AS c
              {npi_join}
              {pa_from}
             WHERE c.npi IS NOT NULL
            """

_PRIMARY_ARRAYS = """LEFT JOIN LATERAL (SELECT pa.taxonomy_array, pa.plans_network_array, pa.procedures_array,
              pa.medications_array FROM {schema}.npi_address AS pa WHERE pa.npi = c.npi AND pa.type = 'primary'
              ORDER BY pa.checksum LIMIT 1) AS pa ON TRUE"""
_ARRAYS = ("taxonomy_array", "plans_network_array", "procedures_array", "medications_array")


def eau_source_selects(native, db_schema, available):
    """The source select, or none while the table does not exist yet."""
    if not available.get(ADDRESS_TABLE, False):
        return []
    has_npi, has_npi_address = available.get("npi", False), available.get("npi_address", False)
    array_by_name = {
        name: f"COALESCE(pa.{name}, ARRAY[0]::int[])::int[]" if has_npi_address else "ARRAY[0]::int[]"
        for name in _ARRAYS
    }
    return [
        _SELECT.format(
            schema=db_schema,
            table=ADDRESS_TABLE,
            entity_name=native._npi_entity_name_expr("n") if has_npi else "NULL::varchar",
            entity_subtype=native._npi_entity_subtype_expr("n") if has_npi else "NULL::varchar",
            base_address_version=native.BASE_ADDRESS_VERSION,
            now=native.candidate_preparation.semantic_now_sql(),
            address_source=ADDRESS_SOURCE,
            npi_join=f"LEFT JOIN {db_schema}.npi AS n ON n.npi = c.npi" if has_npi else "",
            pa_from=_PRIMARY_ARRAYS.format(schema=db_schema) if has_npi_address else "",
            **array_by_name,
        )
    ]
