# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed spatial policy for provider addresses used in pricing."""

from __future__ import annotations

import re
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Mapping

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text

from api.ptg2_address_policy import postal_box_address_sql
from api.ptg2_geo_projection import (
    _PROJECTION_DEPENDENCIES,
    GEO_ASSURANCE_STATE_TABLE,
    projected_boolean_sql,
    projection_dependency_relation_sql,
    projection_named_dependency_lock_sql,
    retained_tiger_children_sql,
    validate_projection_dependency_qnames,
)

_REFERENCE_READ = ContextVar("provider_geo_reference_read", default=None)


async def _read_reference_bindings(session, schema_name):
    """Select local active TIGER names while keeping mutable evidence current."""
    state = f'"{schema_name}"."{GEO_ASSURANCE_STATE_TABLE}"'
    bindings = None
    if await session.scalar(text("SELECT to_regclass(:state) IS NOT NULL"), {"state": state}):
        await session.execute(text(f"LOCK TABLE {state} IN ACCESS SHARE MODE"))
        bindings = await session.scalar(text(f"SELECT active_dependency_bindings FROM {state} WHERE singleton IS TRUE"))
    if bindings is None:
        if await session.scalar(text("SELECT " + retained_tiger_children_sql())):
            raise ValueError("geo reference selection is unavailable")
        return None
    if not isinstance(bindings, Mapping) or any(
        not isinstance(binding_entry, Mapping)
        or set(binding_entry)
        not in ({"schema_name", "table_name"}, {"schema_name", "table_name", "relation_oid", "relfilenode"})
        for binding_entry in bindings.values()
    ):
        raise ValueError("geo reference selection is invalid")
    names = validate_projection_dependency_qnames(
        schema_name,
        {
            key: {field: binding_entry[field] for field in ("schema_name", "table_name")}
            for key, binding_entry in bindings.items()
        },
    )
    selected_by_name = {
        f"{schema or schema_name}.{name}": {"schema_name": schema or schema_name, "table_name": name}
        for schema, name in _PROJECTION_DEPENDENCIES
    }
    tiger_schema = names["tiger.zip_state"]["schema_name"]
    if not (tiger_schema == "tiger" or re.fullmatch(r"reference_family_archive_[0-9a-f]{32}", tiger_schema)):
        raise ValueError("geo reference selection is invalid")
    if tiger_schema == "tiger" and await session.scalar(text("SELECT " + retained_tiger_children_sql())):
        raise ValueError("geo reference selection is unavailable")
    for name in ("zip_state", "zcta5"):
        if names["tiger." + name] != {"schema_name": tiger_schema, "table_name": name}:
            raise ValueError("geo reference selection is invalid")
        selected_by_name["tiger." + name] = names["tiger." + name]
    await session.execute(text(projection_named_dependency_lock_sql(schema_name, selected_by_name)))
    return selected_by_name


@asynccontextmanager
async def provider_address_geo_references(session, *, schema_name, request_args=None):
    """Keep selected reference names inside the caller's existing read snapshot."""
    if request_args is not None and not any(
        request_args.get(name) not in (None, "", "null") for name in ("zip5", "zip", "lat", "long")
    ):
        yield None
        return
    schema_name = _validated_sql_identifier(schema_name, field_name="address schema")
    try:
        bindings = await _read_reference_bindings(session, schema_name)
    except ValueError as error:
        raise ServiceUnavailable("Provider data is temporarily unavailable.") from error
    token = _REFERENCE_READ.set((schema_name, bindings))
    try:
        yield bindings
    finally:
        _REFERENCE_READ.reset(token)


def _reference_bindings(schema_name):
    selected = _REFERENCE_READ.get()
    return selected[1] if selected is not None and selected[0] == schema_name else None


def _reference_relation(schema_name, name):
    bindings = _reference_bindings(schema_name)
    relation = projection_dependency_relation_sql(schema_name, "tiger." + name, dependency_bindings=bindings)
    return "ONLY " + relation if bindings is not None else relation


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
            to_regtype('public.geometry') IS NOT NULL
            AND to_regtype('public.geography') IS NOT NULL
            AND to_regprocedure('public.st_covers(public.geometry,public.geometry)') IS NOT NULL
            AND to_regprocedure('public.st_setsrid(public.geometry,integer)') IS NOT NULL
            AND to_regprocedure(
                'public.st_makepoint(double precision,double precision)'
            ) IS NOT NULL
            AND to_regprocedure(
                'public.st_dwithin(public.geography,public.geography,double precision,boolean)'
            ) IS NOT NULL
            AND to_regprocedure('public.postgis_typmod_srid(integer)') IS NOT NULL
            AND to_regprocedure('public.postgis_typmod_type(integer)') IS NOT NULL
        ) AS has_spatial_functions,
        COALESCE(
            (
                SELECT attribute.atttypid = to_regtype('public.geometry')
                       AND public.postgis_typmod_srid(attribute.atttypmod) = 4269
                       AND public.postgis_typmod_type(attribute.atttypmod) IN (
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
    return f"NULLIF(LEFT(REGEXP_REPLACE(COALESCE({alias}.postal_code, ''), '[^0-9]', '', 'g'), 5), '')"


def _canonical_zip_state_sql(
    alias: str,
    schema_name: str,
    *,
    reference_aliases: tuple[str, str] | None = None,
) -> str:
    alias = _validated_sql_identifier(alias, field_name="address alias")
    schema_name = _validated_sql_identifier(
        schema_name,
        field_name="address schema",
    )
    displayed_state = f"UPPER(BTRIM(COALESCE({alias}.state_name, '')))"
    canonical_state = f"UPPER(BTRIM(COALESCE({alias}.state_code, '')))"
    canonical_zip = f"BTRIM(COALESCE({alias}.zip5, ''))"
    if reference_aliases is not None:
        geo_zip_alias, zip_state_alias = (
            _validated_sql_identifier(reference_alias, field_name="reference alias")
            for reference_alias in reference_aliases
        )
        return f"({geo_zip_alias}.zip_code IS NOT NULL OR {zip_state_alias}.zip IS NOT NULL)"
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
              FROM {_reference_relation(schema_name, "zip_state")} AS address_zip_state
             WHERE address_zip_state.zip = {canonical_zip}
               AND UPPER(BTRIM(COALESCE(address_zip_state.stusps, ''))) = {canonical_state}
               AND {displayed_state} = UPPER(BTRIM(COALESCE(address_zip_state.stusps, '')))
        )
    )"""


def provider_address_identity_reference_joins_sql(
    alias: str,
    *,
    schema_name: str,
    geo_zip_alias: str,
    zip_state_alias: str,
    dependency_bindings=None,
) -> str:
    """Join the same canonical postal references used by legacy assurance."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    schema_name = _validated_sql_identifier(schema_name, field_name="address schema")
    geo_zip_alias = _validated_sql_identifier(geo_zip_alias, field_name="reference alias")
    zip_state_alias = _validated_sql_identifier(zip_state_alias, field_name="reference alias")
    displayed_state = f"UPPER(BTRIM(COALESCE({alias}.state_name, '')))"
    canonical_state = f"UPPER(BTRIM(COALESCE({alias}.state_code, '')))"
    canonical_zip = f"BTRIM(COALESCE({alias}.zip5, ''))"
    dependency_bindings = dependency_bindings if dependency_bindings is not None else _reference_bindings(schema_name)
    geo_relation = projection_dependency_relation_sql(
        schema_name, f"{schema_name}.geo_zip_lookup", dependency_bindings=dependency_bindings
    )
    zip_relation = projection_dependency_relation_sql(
        schema_name, "tiger.zip_state", dependency_bindings=dependency_bindings
    )
    return f"""LEFT JOIN {geo_relation} AS {geo_zip_alias}
      ON {geo_zip_alias}.zip_code = {canonical_zip}
     AND UPPER(BTRIM(COALESCE({geo_zip_alias}.state, ''))) = {canonical_state}
     AND {displayed_state} IN (
          UPPER(BTRIM(COALESCE({geo_zip_alias}.state, ''))),
          UPPER(BTRIM(COALESCE({geo_zip_alias}.state_name, '')))
     )
    LEFT JOIN {zip_relation} AS {zip_state_alias}
      ON {zip_state_alias}.zip = {canonical_zip}
     AND UPPER(BTRIM(COALESCE({zip_state_alias}.stusps, ''))) = {canonical_state}
     AND {displayed_state} = UPPER(BTRIM(COALESCE({zip_state_alias}.stusps, '')))"""


def provider_address_identity_coherence_sql(
    alias: str,
    *,
    schema_name: str,
    use_projection: bool = True,
    reference_aliases: tuple[str, str] | None = None,
) -> str:
    """Require displayed and canonical US postal identity to agree."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    normalized_country = f"UPPER(BTRIM(COALESCE({alias}.country_code, '')))"
    legacy_sql = f"""(
        {alias}.address_key IS NOT NULL
        AND NOT {postal_box_address_sql(alias)}
        AND NULLIF(BTRIM(COALESCE({alias}.zip5, '')), '') IS NOT NULL
        AND NULLIF(BTRIM(COALESCE({alias}.state_code, '')), '') IS NOT NULL
        AND {_display_zip5_sql(alias)} = BTRIM({alias}.zip5)
        AND {normalized_country} IN (
            'US', 'USA', 'UNITED STATES', 'UNITED STATES OF AMERICA', '840'
        )
        AND {
        _canonical_zip_state_sql(
            alias,
            schema_name,
            reference_aliases=reference_aliases,
        )
    }
    )"""
    if not use_projection:
        return legacy_sql
    return projected_boolean_sql(
        alias,
        "geo_identity_coherent",
        schema_name=schema_name,
        legacy_sql=legacy_sql,
    )


def provider_address_point_coherence_sql(
    alias: str,
    *,
    schema_name: str | None = None,
    use_projection: bool = True,
    zcta_alias: str | None = None,
) -> str:
    """Require one usable point to fall inside its address ZIP polygon."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    zcta_alias = _validated_sql_identifier(zcta_alias, field_name="reference alias") if zcta_alias else None
    point_covered_sql = (
        f"""public.ST_Covers(
                    {zcta_alias}.the_geom,
                    public.ST_SetSRID(
                        public.ST_MakePoint(
                            {alias}.long::double precision,
                            {alias}.lat::double precision
                        ),
                        4269
                    )
               )"""
        if zcta_alias
        else f"""EXISTS (
            SELECT 1
              FROM {_reference_relation(schema_name or "mrf", "zcta5")} AS address_zcta
             WHERE address_zcta.zcta5ce = BTRIM({alias}.zip5)
               AND public.ST_Covers(
                    address_zcta.the_geom,
                    public.ST_SetSRID(
                        public.ST_MakePoint(
                            {alias}.long::double precision,
                            {alias}.lat::double precision
                        ),
                        4269
                    )
               )
        )"""
    )
    legacy_sql = f"""(
        {alias}.lat IS NOT NULL
        AND {alias}.long IS NOT NULL
        AND {alias}.lat::double precision BETWEEN -90.0 AND 90.0
        AND {alias}.long::double precision BETWEEN -180.0 AND 180.0
        AND {point_covered_sql}
    )"""
    if not use_projection:
        return legacy_sql
    if schema_name is None:
        raise ValueError("schema_name is required when using geo projection")
    if _reference_bindings(schema_name) is not None:
        return legacy_sql
    return projected_boolean_sql(
        alias,
        "geo_point_coherent",
        schema_name=schema_name,
        legacy_sql=legacy_sql,
    )


def provider_address_point_reference_join_sql(
    alias: str,
    *,
    zcta_alias: str,
    schema_name: str = "mrf",
    dependency_bindings=None,
) -> str:
    """Join the ZIP polygon used by the legacy point-coherence predicate."""

    alias = _validated_sql_identifier(alias, field_name="address alias")
    zcta_alias = _validated_sql_identifier(zcta_alias, field_name="reference alias")
    dependency_bindings = dependency_bindings if dependency_bindings is not None else _reference_bindings(schema_name)
    relation = projection_dependency_relation_sql(schema_name, "tiger.zcta5", dependency_bindings=dependency_bindings)
    return f"JOIN {relation} AS {zcta_alias} ON {zcta_alias}.zcta5ce = BTRIM({alias}.zip5)"


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
    point_sql = provider_address_point_coherence_sql(
        alias,
        schema_name=schema_name,
    )
    missing_point_sql = f"({alias}.lat IS NULL AND {alias}.long IS NULL)"
    radius_sql = " AND ".join(radius_predicates)
    radius_branch = f"({point_sql} AND ({radius_sql}))" if radius_sql else None
    if exact_zip_predicate and radius_branch:
        exact_without_point = f"({exact_zip_predicate} AND {missing_point_sql})"
        return f"({identity_sql} AND ({exact_without_point} OR {radius_branch}))"
    if exact_zip_predicate:
        return f"({exact_zip_predicate} AND {identity_sql} AND ({missing_point_sql} OR {point_sql}))"
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
        bindings = _reference_bindings(schema_name)
        if bindings is not None:
            reference_schema = bindings["tiger.zip_state"]["schema_name"]
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
                    "reference_zip_state_table": (f"{reference_schema}.zip_state"),
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
    "provider_address_identity_reference_joins_sql",
    "provider_address_location_filter_sql",
    "provider_address_point_coherence_sql",
    "provider_address_point_reference_join_sql",
    "provider_address_geo_references",
]
