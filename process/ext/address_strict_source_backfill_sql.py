# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict, target-scoped source evidence for reviewed address aliases."""

from __future__ import annotations

import re
from dataclasses import dataclass

from process.ext import address_alias_sql


TARGET_TABLE = "address_strict_backfill_targets"
EVIDENCE_TABLE = "address_strict_backfill_evidence"
REVIEWED_CANDIDATE_TABLE = "address_strict_backfill_reviewed_candidates"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SourceProjection:
    """One bounded raw-source projection contributing one provenance bit.

    ``address_key`` is only the indexed prefilter.  The generated query always
    recomputes both the address key and identity from these component fields.
    """

    name: str
    table: str
    source_bit: int
    first_line: str
    second_line: str
    city: str
    state: str
    postal_code: str
    country: str
    stored_key_type: str = "uuid"


SOURCE_PROJECTIONS = (
    SourceProjection(
        "nppes", "npi_address", 1,
        "first_line", "second_line", "city_name", "state_name", "postal_code",
        "COALESCE(NULLIF(country_code, ''), 'US')",
    ),
    SourceProjection(
        "cms_doctors", "doctor_clinician_address", 2,
        "address_line1", "address_line2", "city", "state", "zip_code", "'US'",
    ),
    SourceProjection(
        "provider_enrollment", "provider_enrollment_ffs", 4,
        "address_line_1", "address_line_2", "city", "state", "zip_code", "'US'",
    ),
    SourceProjection(
        "marketplace_mrf", "mrf_address_evidence", 16,
        "first_line", "second_line", "city_name", "state_name", "postal_code",
        "COALESCE(NULLIF(country_code, ''), 'US')",
    ),
    SourceProjection(
        "pharmacy_license", "pharmacy_license_record_v1", 32,
        "address_line1", "address_line2", "city", "state", "zip_code", "'US'",
    ),
    SourceProjection(
        "provider_directory_overlay", "provider_directory_address_overlay", 128,
        "first_line", "second_line", "city_name",
        "COALESCE(NULLIF(state_name, ''), state_code)", "postal_code",
        "COALESCE(NULLIF(country_code, ''), 'US')",
    ),
    SourceProjection(
        "provider_directory_location", "provider_directory_location", 128,
        "first_line", "second_line", "city_name",
        "COALESCE(NULLIF(state_name, ''), state_code)", "postal_code",
        "COALESCE(NULLIF(country_code, ''), 'US')", "text",
    ),
)


def _quote_ident(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f'"{value}"'


def _relation(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def relation_exists_sql() -> str:
    return "SELECT to_regclass(:qualified_relation) IS NOT NULL;"


def address_key_index_exists_sql() -> str:
    """Return whether a relation has a valid, non-partial leading key index."""
    return """
        SELECT EXISTS (
            SELECT 1
            FROM pg_index AS index_meta
            JOIN pg_class AS source_relation
              ON source_relation.oid = index_meta.indrelid
            JOIN pg_namespace AS source_namespace
              ON source_namespace.oid = source_relation.relnamespace
            WHERE source_namespace.nspname = :schema_name
              AND source_relation.relname = :table_name
              AND index_meta.indisvalid
              AND index_meta.indisready
              AND index_meta.indpred IS NULL
              AND pg_get_indexdef(index_meta.indexrelid, 1, TRUE) = 'address_key'
        );
    """


def lock_candidates_sql(*, schema: str) -> str:
    candidates = _relation(schema, address_alias_sql.ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"LOCK TABLE {candidates} IN SHARE MODE;"


def create_reviewed_candidates_sql(*, schema: str) -> str:
    candidates = _relation(schema, address_alias_sql.ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        CREATE TEMP TABLE {REVIEWED_CANDIDATE_TABLE} ON COMMIT DROP AS
        SELECT
            source_address_key,
            source_identity_key,
            target_address_key,
            target_identity_key,
            candidate_count,
            target_strict_source_bits,
            target_strict_source_count,
            decision
        FROM {candidates}
        WHERE run_id = CAST(:shadow_run_id AS uuid);
    """


def reviewed_candidate_rows_sql() -> str:
    return f"""
        SELECT
            source_address_key::text AS source_address_key,
            source_identity_key,
            target_address_key::text AS target_address_key,
            target_identity_key,
            candidate_count,
            target_strict_source_bits,
            target_strict_source_count,
            decision
        FROM {REVIEWED_CANDIDATE_TABLE}
        ORDER BY source_address_key, target_address_key;
    """


def create_targets_sql(*, archive: str) -> str:
    return f"""
        CREATE TEMP TABLE {TARGET_TABLE} ON COMMIT DROP AS
        SELECT DISTINCT
            target.address_key,
            target.identity_key
        FROM {REVIEWED_CANDIDATE_TABLE} AS candidate
        JOIN {archive} AS target
          ON target.address_key = candidate.target_address_key
         AND target.identity_key = candidate.target_identity_key
         AND target.merged_into IS NULL;
    """


def drifted_target_count_sql(*, archive: str) -> str:
    return f"""
        SELECT count(*)::bigint
        FROM {REVIEWED_CANDIDATE_TABLE} AS candidate
        WHERE NOT EXISTS (
            SELECT 1
            FROM {archive} AS target
            WHERE target.address_key = candidate.target_address_key
              AND target.identity_key = candidate.target_identity_key
              AND target.merged_into IS NULL
        );
    """


def create_target_index_sql() -> str:
    return f"ALTER TABLE {TARGET_TABLE} ADD PRIMARY KEY (address_key);"


def analyze_targets_sql() -> str:
    return f"ANALYZE {TARGET_TABLE};"


def create_evidence_sql() -> str:
    return f"""
        CREATE TEMP TABLE {EVIDENCE_TABLE} (
            target_address_key uuid NOT NULL,
            source_bit integer NOT NULL,
            source_name text NOT NULL,
            PRIMARY KEY (target_address_key, source_bit, source_name)
        ) ON COMMIT DROP;
    """


def evidence_insert_sql(*, schema: str, projection: SourceProjection) -> str:
    source = _relation(schema, projection.table)
    key_expression = (
        "source.address_key = target.address_key::text"
        if projection.stored_key_type == "text"
        else "source.address_key = target.address_key"
    )

    def value(expression: str) -> str:
        if expression in {"NULL", "'US'"} or expression.startswith("COALESCE("):
            return expression.replace("country_code", "source.country_code").replace(
                "state_name", "source.state_name"
            ).replace("state_code", "source.state_code")
        return f"source.{_quote_ident(expression)}"

    components = ",\n                        ".join(
        value(expression)
        for expression in (
            projection.first_line,
            projection.second_line,
            projection.city,
            projection.state,
            projection.postal_code,
            projection.country,
        )
    )
    return f"""
        INSERT INTO {EVIDENCE_TABLE} (
            target_address_key,
            source_bit,
            source_name
        )
        SELECT
            target.address_key,
            {projection.source_bit},
            '{projection.name}'
        FROM {TARGET_TABLE} AS target
        WHERE EXISTS (
            SELECT 1
            FROM {source} AS source
            WHERE {key_expression}
              AND {_quote_ident(schema)}.addr_key_v1(
                        {components}
                  ) = target.address_key
              AND {_quote_ident(schema)}.addr_identity_key_v1(
                        {components}
                  ) = target.identity_key
        )
        ON CONFLICT DO NOTHING;
    """


def target_count_sql() -> str:
    return f"SELECT count(*)::bigint FROM {TARGET_TABLE};"


def evidence_rows_sql() -> str:
    return f"""
        SELECT
            target.address_key::text AS target_address_key,
            COALESCE(bit_or(evidence.source_bit), 0)::integer AS strict_source_bits,
            count(DISTINCT evidence.source_bit)::integer AS source_count,
            COALESCE(
                array_agg(DISTINCT evidence.source_name ORDER BY evidence.source_name)
                    FILTER (WHERE evidence.source_name IS NOT NULL),
                ARRAY[]::text[]
            ) AS source_names
        FROM {TARGET_TABLE} AS target
        LEFT JOIN {EVIDENCE_TABLE} AS evidence
          ON evidence.target_address_key = target.address_key
        GROUP BY target.address_key
        ORDER BY target.address_key;
    """


def evidence_target_count_sql() -> str:
    return f"SELECT count(DISTINCT target_address_key)::bigint FROM {EVIDENCE_TABLE};"


def evidence_metrics_sql() -> str:
    return f"""
        SELECT source_name, count(*)::bigint AS target_count
        FROM {EVIDENCE_TABLE}
        GROUP BY source_name
        ORDER BY source_name;
    """


def evidence_pair_count_sql() -> str:
    return f"SELECT count(*)::bigint FROM {EVIDENCE_TABLE};"


def apply_evidence_sql(*, archive: str) -> str:
    return f"""
        WITH aggregated AS (
            SELECT
                target_address_key,
                bit_or(source_bit)::integer AS value
            FROM {EVIDENCE_TABLE}
            GROUP BY target_address_key
        ), updated AS (
            UPDATE {archive} AS target
               SET source_bits = target.source_bits | aggregated.value,
                   strict_source_bits = target.strict_source_bits | aggregated.value,
                   last_seen_at = now()
              FROM aggregated
             WHERE target.address_key = aggregated.target_address_key
               AND (target.strict_source_bits & aggregated.value) <> aggregated.value
            RETURNING 1
        )
        SELECT count(*)::bigint FROM updated;
    """
