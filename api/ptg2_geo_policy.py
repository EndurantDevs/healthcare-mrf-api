# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed spatial policy for provider addresses used in pricing."""

from __future__ import annotations

import re
from typing import Any, Mapping

from sqlalchemy import text


_PROVIDER_ADDRESS_GEO_CAPABILITY_SQL = """
    SELECT
        to_regclass(:geo_zip_table) IS NOT NULL AS has_geo_zip,
        (
            SELECT COUNT(DISTINCT column_name) = 3
              FROM information_schema.columns
             WHERE table_schema = :geo_schema
               AND table_name = 'geo_zip_lookup'
               AND column_name IN ('zip_code', 'state', 'state_name')
        ) AS has_geo_zip_columns,
        to_regclass(:reference_zip_state_table) IS NOT NULL AS has_zip_state,
        (
            SELECT COUNT(DISTINCT column_name) = 2
              FROM information_schema.columns
             WHERE table_schema = :reference_schema
               AND table_name = 'zip_state'
               AND column_name IN ('zip', 'stusps')
        ) AS has_zip_state_columns,
        to_regclass(:reference_zcta_table) IS NOT NULL AS has_zcta,
        (
            SELECT COUNT(DISTINCT column_name) = 2
              FROM information_schema.columns
             WHERE table_schema = :reference_schema
               AND table_name = 'zcta5'
               AND column_name IN ('zcta5ce', 'the_geom')
        ) AS has_zcta_columns,
        (
            to_regtype('geometry') IS NOT NULL
            AND to_regtype('geography') IS NOT NULL
            AND to_regprocedure('st_covers(geometry,geometry)') IS NOT NULL
            AND to_regprocedure('st_setsrid(geometry,integer)') IS NOT NULL
            AND to_regprocedure(
                'st_makepoint(double precision,double precision)'
            ) IS NOT NULL
            AND to_regprocedure(
                'st_dwithin(geography,geography,double precision,boolean)'
            ) IS NOT NULL
            AND to_regprocedure('postgis_typmod_srid(integer)') IS NOT NULL
            AND to_regprocedure('postgis_typmod_type(integer)') IS NOT NULL
        ) AS has_spatial_functions,
        COALESCE(
            (
                SELECT attribute.atttypid = to_regtype('geometry')
                       AND postgis_typmod_srid(attribute.atttypmod) = 4269
                       AND postgis_typmod_type(attribute.atttypmod) IN (
                           'Polygon', 'MultiPolygon'
                       )
                  FROM pg_attribute AS attribute
                 WHERE attribute.attrelid = to_regclass(:reference_zcta_table)
                   AND attribute.attname = 'the_geom'
                   AND attribute.attnum > 0
                   AND NOT attribute.attisdropped
            ),
            FALSE
        ) AS has_zcta_geometry_contract,
        EXISTS (
            SELECT 1
              FROM pg_index AS index_meta
              JOIN pg_class AS index_record
                ON index_record.oid = index_meta.indexrelid
              JOIN pg_am AS index_method
                ON index_method.oid = index_record.relam
              JOIN pg_attribute AS key_attribute
                ON key_attribute.attrelid = index_meta.indrelid
               AND key_attribute.attname = 'zcta5ce'
               AND key_attribute.attnum > 0
               AND NOT key_attribute.attisdropped
             WHERE index_meta.indrelid = to_regclass(:reference_zcta_table)
               AND index_record.relkind IN ('i', 'I')
               AND index_method.amname = 'btree'
               AND index_meta.indisvalid
               AND index_meta.indisready
               AND index_meta.indislive
               AND index_meta.indpred IS NULL
               AND index_meta.indnkeyatts >= 1
               AND index_meta.indkey[0] = key_attribute.attnum
        ) AS has_zcta_zip_index
"""

_PROVIDER_ADDRESS_GEO_CAPABILITY_FIELDS = (
    "has_geo_zip",
    "has_geo_zip_columns",
    "has_zip_state",
    "has_zip_state_columns",
    "has_zcta",
    "has_zcta_columns",
    "has_spatial_functions",
    "has_zcta_geometry_contract",
    "has_zcta_zip_index",
)

_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _validated_sql_identifier(value: str, *, field_name: str) -> str:
    """Return one safe unquoted PostgreSQL identifier."""

    identifier = str(value or "")
    if not _SQL_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"{field_name} must be a simple PostgreSQL identifier")
    return identifier


def _display_zip5_sql(alias: str) -> str:
    alias = _validated_sql_identifier(alias, field_name="address alias")
    return (
        "NULLIF(LEFT(REGEXP_REPLACE(COALESCE("
        f"{alias}.postal_code, ''), '[^0-9]', '', 'g'), 5), '')"
    )


def _canonical_zip_state_sql(alias: str, schema_name: str) -> str:
    alias = _validated_sql_identifier(alias, field_name="address alias")
    schema_name = _validated_sql_identifier(
        schema_name,
        field_name="address schema",
    )
    displayed_state = f"UPPER(BTRIM(COALESCE({alias}.state_name, '')))"
    canonical_state = f"UPPER(BTRIM(COALESCE({alias}.state_code, '')))"
    canonical_zip = f"BTRIM(COALESCE({alias}.zip5, ''))"
    return f"""(
        EXISTS (
            SELECT 1
              FROM {schema_name}.geo_zip_lookup AS address_zip
             WHERE address_zip.zip_code = {canonical_zip}
               AND UPPER(BTRIM(COALESCE(address_zip.state, ''))) = {canonical_state}
               AND {displayed_state} IN (
                    UPPER(BTRIM(COALESCE(address_zip.state, ''))),
                    UPPER(BTRIM(COALESCE(address_zip.state_name, '')))
               )
        )
        OR EXISTS (
            SELECT 1
              FROM tiger.zip_state AS address_zip_state
             WHERE address_zip_state.zip = {canonical_zip}
               AND UPPER(BTRIM(COALESCE(address_zip_state.stusps, ''))) = {canonical_state}
               AND {displayed_state} = UPPER(BTRIM(COALESCE(address_zip_state.stusps, '')))
        )
    )"""


def provider_address_identity_coherence_sql(
    alias: str,
    *,
    schema_name: str,
) -> str:
    """Require displayed and canonical US postal identity to agree."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    normalized_country = f"UPPER(BTRIM(COALESCE({alias}.country_code, '')))"
    return f"""(
        {alias}.address_key IS NOT NULL
        AND NULLIF(BTRIM(COALESCE({alias}.zip5, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE({alias}.state_code, '')), '') IS NOT NULL
        AND {_display_zip5_sql(alias)} = BTRIM({alias}.zip5)
        AND {normalized_country} IN (
            'US', 'USA', 'UNITED STATES', 'UNITED STATES OF AMERICA', '840'
        )
        AND {_canonical_zip_state_sql(alias, schema_name)}
    )"""


def provider_address_point_coherence_sql(alias: str) -> str:
    """Require one usable point to fall inside its address ZIP polygon."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    return f"""(
        {alias}.lat IS NOT NULL
        AND {alias}.long IS NOT NULL
        AND {alias}.lat::double precision BETWEEN -90.0 AND 90.0
        AND {alias}.long::double precision BETWEEN -180.0 AND 180.0
        AND EXISTS (
            SELECT 1
              FROM tiger.zcta5 AS address_zcta
             WHERE address_zcta.zcta5ce = BTRIM({alias}.zip5)
               AND ST_Covers(
                    address_zcta.the_geom,
                    ST_SetSRID(
                        ST_MakePoint(
                            {alias}.long::double precision,
                            {alias}.lat::double precision
                        ),
                        4269
                    )
               )
        )
    )"""


def provider_address_location_filter_sql(
    alias: str,
    *,
    schema_name: str,
    exact_zip_predicate: str | None,
    radius_predicates: list[str],
) -> str | None:
    """Compose exact-ZIP fallback and address-bound radius eligibility."""

    if not exact_zip_predicate and not radius_predicates:
        return None
    identity_sql = provider_address_identity_coherence_sql(
        alias,
        schema_name=schema_name,
    )
    point_sql = provider_address_point_coherence_sql(alias)
    missing_point_sql = f"({alias}.lat IS NULL AND {alias}.long IS NULL)"
    radius_sql = " AND ".join(radius_predicates)
    radius_branch = f"({point_sql} AND ({radius_sql}))" if radius_sql else None
    if exact_zip_predicate and radius_branch:
        exact_without_point = (
            f"({exact_zip_predicate} AND {missing_point_sql})"
        )
        return f"({identity_sql} AND ({exact_without_point} OR {radius_branch}))"
    if exact_zip_predicate:
        return (
            f"({exact_zip_predicate} AND {identity_sql} AND {missing_point_sql})"
        )
    return f"({identity_sql} AND {radius_branch})" if radius_branch else None


def _capability_fields(result_row: Any) -> Mapping[str, Any]:
    fields = getattr(result_row, "_mapping", None)
    if isinstance(fields, Mapping):
        return fields
    return {}


async def is_provider_address_geo_capability_available(
    session: Any,
    *,
    schema_name: str,
    reference_schema: str = "tiger",
) -> bool:
    """Check canonical ZIP and polygon dependencies for a real API session."""

    try:
        schema_name = _validated_sql_identifier(
            schema_name,
            field_name="address schema",
        )
        reference_schema = _validated_sql_identifier(
            reference_schema,
            field_name="spatial reference schema",
        )
    except ValueError:
        return False
    execute = getattr(session, "execute", None)
    begin_nested = getattr(session, "begin_nested", None)
    if not callable(execute) or not callable(begin_nested):
        return False
    try:
        async with begin_nested():
            capability_query_result = await execute(
                text(_PROVIDER_ADDRESS_GEO_CAPABILITY_SQL),
                {
                    "geo_schema": schema_name,
                    "geo_zip_table": f"{schema_name}.geo_zip_lookup",
                    "reference_schema": reference_schema,
                    "reference_zip_state_table": (
                        f"{reference_schema}.zip_state"
                    ),
                    "reference_zcta_table": f"{reference_schema}.zcta5",
                },
            )
        fields = _capability_fields(capability_query_result.first())
        return all(bool(fields.get(field)) for field in _PROVIDER_ADDRESS_GEO_CAPABILITY_FIELDS)
    except Exception:
        return False


__all__ = [
    "is_provider_address_geo_capability_available",
    "provider_address_identity_coherence_sql",
    "provider_address_location_filter_sql",
    "provider_address_point_coherence_sql",
]
