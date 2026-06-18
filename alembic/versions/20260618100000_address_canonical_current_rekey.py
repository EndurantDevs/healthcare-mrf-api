"""Rewrite canonical address keys to the current identity format."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op


revision = "20260618100000_address_canonical_current_rekey"
down_revision = "20260616123000_mrf_address_npi_bigint"
branch_labels = None
depends_on = None


def _foundation_module():
    path = Path(__file__).with_name("20260611100000_address_canonical_foundation.py")
    spec = importlib.util.spec_from_file_location("_address_canonical_foundation", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load address canonical foundation migration from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _q(schema: str, name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_exists(bind, schema: str, table: str) -> bool:
    return bool(
        bind.exec_driver_sql(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.tables
                 WHERE table_schema = %(schema)s
                   AND table_name = %(table)s
            );
            """,
            {"schema": schema, "table": table},
        ).scalar()
    )


def _column_exists(bind, schema: str, table: str, column: str) -> bool:
    return bool(
        bind.exec_driver_sql(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = %(schema)s
                   AND table_name = %(table)s
                   AND column_name = %(column)s
            );
            """,
            {"schema": schema, "table": table, "column": column},
        ).scalar()
    )


def _create_rekey_candidates_sql(schema: str) -> str:
    qschema = _quote_ident(schema)
    archive = _q(schema, "address_archive_v2")
    candidates = "pg_temp.address_archive_rekey_candidates"
    return f"""
CREATE TEMP TABLE address_archive_rekey_candidates ON COMMIT DROP AS
WITH source AS (
    SELECT
        a.*,
        {qschema}.addr_identity_key_v1(
            a.first_line,
            a.second_line,
            a.city_name,
            a.state_name,
            a.postal_code,
            a.country_code
        ) AS function_identity_key,
        {qschema}.addr_premise_identity_key_v1(
            a.first_line,
            a.second_line,
            a.city_name,
            a.state_name,
            a.postal_code,
            a.country_code
        ) AS function_premise_identity_key
    FROM {archive} AS a
),
identities AS (
    SELECT
        source.*,
        COALESCE(
            function_identity_key,
            CASE
                WHEN COALESCE(precision, 'street') = 'street'
                 AND line1_norm IS NOT NULL
                 AND state_code IS NOT NULL
                 AND zip5 IS NOT NULL
                 AND COALESCE(country_code, 'US') = 'US'
                THEN concat(
                    'v2|',
                    line1_norm,
                    '|',
                    COALESCE(unit_norm, ''),
                    '||',
                    state_code,
                    '|',
                    zip5,
                    '|',
                    COALESCE(country_code, 'US'),
                    '|street'
                )
                WHEN COALESCE(precision, 'street') = 'city_zip'
                 AND city_norm IS NOT NULL
                 AND state_code IS NOT NULL
                 AND zip5 IS NOT NULL
                 AND COALESCE(country_code, 'US') = 'US'
                THEN concat(
                    'v2||',
                    COALESCE(unit_norm, ''),
                    '|',
                    city_norm,
                    '|',
                    state_code,
                    '|',
                    zip5,
                    '|',
                    COALESCE(country_code, 'US'),
                    '|city_zip'
                )
                ELSE NULL
            END
        ) AS new_identity_key,
        COALESCE(
            function_premise_identity_key,
            CASE
                WHEN line1_norm IS NOT NULL
                 AND state_code IS NOT NULL
                 AND zip5 IS NOT NULL
                 AND COALESCE(country_code, 'US') = 'US'
                THEN concat(
                    'v2|',
                    line1_norm,
                    '|||',
                    state_code,
                    '|',
                    zip5,
                    '|',
                    COALESCE(country_code, 'US'),
                    '|street'
                )
                ELSE NULL
            END
        ) AS new_premise_identity_key
    FROM source
)
SELECT
    address_key AS old_address_key,
    merged_into AS old_merged_into,
    {qschema}.addr_key_from_identity_v1(new_identity_key) AS new_address_key,
    new_identity_key,
    {qschema}.addr_key_from_identity_v1(new_premise_identity_key) AS new_premise_key,
    split_part(new_identity_key, '|', 8) AS new_precision,
    line1_norm,
    COALESCE(unit_norm, '') AS unit_norm,
    city_norm,
    state_code,
    zip5,
    zip4,
    country_code,
    first_line,
    second_line,
    city_name,
    state_name,
    postal_code,
    telephone_number,
    fax_number,
    formatted_address,
    lat,
    long,
    place_id,
    geo_source,
    geocode_source,
    geocode_quality,
    postal_validation_status,
    geocoded_at,
    source_bits,
    first_seen_at,
    last_seen_at,
    date_added,
    display_priority
FROM identities;

CREATE INDEX ON {candidates} (old_address_key);
CREATE INDEX ON {candidates} (old_merged_into) WHERE old_merged_into IS NOT NULL;
CREATE INDEX ON {candidates} (new_address_key) WHERE new_address_key IS NOT NULL;
"""


def _create_rewrite_map_sql() -> str:
    return """
CREATE TEMP TABLE address_archive_rewrite_map ON COMMIT DROP AS
SELECT
    c.old_address_key,
    COALESCE(target.new_address_key, c.new_address_key) AS new_address_key,
    COALESCE(target.new_identity_key, c.new_identity_key) AS new_identity_key,
    COALESCE(target.new_premise_key, c.new_premise_key) AS new_premise_key,
    COALESCE(target.new_precision, c.new_precision) AS new_precision
FROM address_archive_rekey_candidates AS c
LEFT JOIN address_archive_rekey_candidates AS target
  ON target.old_address_key = c.old_merged_into
WHERE COALESCE(target.new_address_key, c.new_address_key) IS NOT NULL;

CREATE UNIQUE INDEX ON address_archive_rewrite_map (old_address_key);
CREATE INDEX ON address_archive_rewrite_map (new_address_key);
"""


def _create_rekeyed_archive_sql() -> str:
    return """
CREATE TEMP TABLE address_archive_v2_rekeyed ON COMMIT DROP AS
WITH mapped AS (
    SELECT
        c.*,
        m.new_address_key AS mapped_address_key,
        m.new_identity_key AS mapped_identity_key,
        m.new_premise_key AS mapped_premise_key,
        m.new_precision AS mapped_precision
    FROM address_archive_rekey_candidates AS c
    JOIN address_archive_rewrite_map AS m
      ON m.old_address_key = c.old_address_key
),
aggregated AS (
    SELECT
        mapped_address_key,
        bit_or(source_bits) AS source_bits,
        min(first_seen_at) AS first_seen_at,
        max(last_seen_at) AS last_seen_at,
        min(display_priority) AS display_priority,
        max(date_added) AS date_added
    FROM mapped
    GROUP BY mapped_address_key
),
ranked AS (
    SELECT
        mapped.*,
        row_number() OVER (
            PARTITION BY mapped_address_key
            ORDER BY
                (lat IS NOT NULL AND long IS NOT NULL) DESC,
                geocoded_at DESC NULLS LAST,
                date_added DESC NULLS LAST,
                display_priority ASC,
                last_seen_at DESC NULLS LAST,
                first_seen_at ASC NULLS LAST,
                old_address_key
        ) AS display_rank
    FROM mapped
)
SELECT
    r.mapped_address_key AS address_key,
    r.mapped_identity_key AS identity_key,
    2::smallint AS identity_version,
    COALESCE(NULLIF(r.mapped_precision, ''), 'street') AS precision,
    r.mapped_premise_key AS premise_key,
    r.line1_norm,
    COALESCE(r.unit_norm, '') AS unit_norm,
    r.city_norm,
    r.state_code,
    r.zip5,
    r.zip4,
    COALESCE(r.country_code, 'US') AS country_code,
    r.first_line,
    r.second_line,
    r.city_name,
    r.state_name,
    r.postal_code,
    r.telephone_number,
    r.fax_number,
    r.formatted_address,
    r.lat,
    r.long,
    r.place_id,
    r.geo_source,
    r.geocode_source,
    r.geocode_quality,
    r.postal_validation_status,
    r.geocoded_at,
    a.source_bits,
    a.first_seen_at,
    a.last_seen_at,
    a.date_added,
    a.display_priority,
    NULL::uuid AS merged_into
FROM ranked AS r
JOIN aggregated AS a
  ON a.mapped_address_key = r.mapped_address_key
WHERE r.display_rank = 1;

CREATE UNIQUE INDEX ON address_archive_v2_rekeyed (address_key);
"""


def _create_rekeyed_checksums_sql(schema: str) -> str:
    checksum_map = _q(schema, "address_checksum_map")
    checksum_collision = _q(schema, "address_checksum_collision")
    return f"""
CREATE TEMP TABLE address_checksum_map_rekeyed ON COMMIT DROP AS
SELECT
    cm.checksum,
    m.new_address_key AS address_key
FROM {checksum_map} AS cm
JOIN address_archive_rewrite_map AS m
  ON m.old_address_key = cm.address_key
GROUP BY cm.checksum, m.new_address_key;

CREATE UNIQUE INDEX ON address_checksum_map_rekeyed (checksum);

CREATE TEMP TABLE address_checksum_collision_rekeyed ON COMMIT DROP AS
SELECT
    cc.checksum,
    m.new_address_key AS address_key,
    min(cc.detected_at) AS detected_at
FROM {checksum_collision} AS cc
JOIN address_archive_rewrite_map AS m
  ON m.old_address_key = cc.address_key
GROUP BY cc.checksum, m.new_address_key;

CREATE UNIQUE INDEX ON address_checksum_collision_rekeyed (checksum, address_key);
"""


def _rekey_source_tables(bind, foundation, schema: str) -> None:
    for table in foundation.ADDRESS_KEY_TABLES:
        if not _table_exists(bind, schema, table) or not _column_exists(bind, schema, table, "address_key"):
            continue
        bind.exec_driver_sql(
            f"""
            UPDATE {_q(schema, table)} AS target
               SET address_key = m.new_address_key
              FROM address_archive_rewrite_map AS m
             WHERE target.address_key = m.old_address_key
               AND target.address_key IS DISTINCT FROM m.new_address_key;
            """
        )


def upgrade() -> None:
    foundation = _foundation_module()
    schema = foundation._schema()
    bind = op.get_bind()
    archive = _q(schema, "address_archive_v2")
    checksum_map = _q(schema, "address_checksum_map")
    checksum_collision = _q(schema, "address_checksum_collision")

    if not _table_exists(bind, schema, "address_archive_v2"):
        return

    foundation._exec_sql_batch(bind, foundation._create_functions_sql(schema))
    bind.exec_driver_sql(
        f"ALTER TABLE {archive} ALTER COLUMN identity_version SET DEFAULT 2;"
    )

    foundation._exec_sql_batch(bind, _create_rekey_candidates_sql(schema))
    unmapped_rows = int(
        bind.exec_driver_sql(
            """
            SELECT count(*)
              FROM address_archive_rekey_candidates
             WHERE new_address_key IS NULL
                OR new_identity_key IS NULL;
            """
        ).scalar()
        or 0
    )
    if unmapped_rows:
        raise RuntimeError(
            "Cannot rewrite address_archive_v2 to current keys; "
            f"{unmapped_rows} archive rows are not representable."
        )

    foundation._exec_sql_batch(bind, _create_rewrite_map_sql())
    duplicate_checksum_rows = int(
        bind.exec_driver_sql(
            f"""
            SELECT count(*)
              FROM (
                    SELECT cm.checksum
                      FROM {checksum_map} AS cm
                      JOIN address_archive_rewrite_map AS m
                        ON m.old_address_key = cm.address_key
                     GROUP BY cm.checksum
                    HAVING count(DISTINCT m.new_address_key) > 1
                   ) duplicated;
            """
        ).scalar()
        or 0
    )
    if duplicate_checksum_rows:
        raise RuntimeError(
            "Cannot rewrite address_checksum_map; "
            f"{duplicate_checksum_rows} checksums would map to multiple current keys."
        )

    foundation._exec_sql_batch(bind, _create_rekeyed_archive_sql())
    foundation._exec_sql_batch(bind, _create_rekeyed_checksums_sql(schema))
    _rekey_source_tables(bind, foundation, schema)

    bind.exec_driver_sql(f"TRUNCATE TABLE {checksum_map}, {checksum_collision}, {archive};")
    bind.exec_driver_sql(
        f"""
        INSERT INTO {archive} (
            address_key, identity_key, identity_version, precision, premise_key,
            line1_norm, unit_norm, city_norm, state_code, zip5, zip4, country_code,
            first_line, second_line, city_name, state_name, postal_code,
            telephone_number, fax_number, formatted_address, lat, long, place_id,
            geo_source, geocode_source, geocode_quality, postal_validation_status,
            geocoded_at, source_bits, first_seen_at, last_seen_at, date_added,
            display_priority, merged_into
        )
        SELECT
            address_key, identity_key, identity_version, precision, premise_key,
            line1_norm, unit_norm, city_norm, state_code, zip5, zip4, country_code,
            first_line, second_line, city_name, state_name, postal_code,
            telephone_number, fax_number, formatted_address, lat, long, place_id,
            geo_source, geocode_source, geocode_quality, postal_validation_status,
            geocoded_at, source_bits, first_seen_at, last_seen_at, date_added,
            display_priority, merged_into
        FROM address_archive_v2_rekeyed;
        """
    )
    bind.exec_driver_sql(
        f"""
        INSERT INTO {checksum_map} (checksum, address_key)
        SELECT checksum, address_key
          FROM address_checksum_map_rekeyed;
        """
    )
    bind.exec_driver_sql(
        f"""
        INSERT INTO {checksum_collision} (checksum, address_key, detected_at)
        SELECT checksum, address_key, detected_at
          FROM address_checksum_collision_rekeyed;
        """
    )


def downgrade() -> None:
    return None
