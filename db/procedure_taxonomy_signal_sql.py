# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SQL shared by taxonomy-signal migration and staged publication."""

from __future__ import annotations

import os

from db.plan_release_storage import storage_identifier

TAXONOMY_PUBLICATION_STATE = "procedure_taxonomy_publication"
TAXONOMY_SERVING_VIEW = "procedure_taxonomy_serving"


def procedure_taxonomy_publication_schema() -> str | None:
    """Return the configured schema for the named taxonomy publication."""

    schema = os.getenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", "").strip()
    if schema:
        procedure_taxonomy_control_schema(schema)
    return schema or None


def procedure_taxonomy_control_schema(publication_schema: str) -> str:
    """Derive the separate namespace for source revision and publication state."""
    control_schema = f"{publication_schema}_control"
    storage_identifier(control_schema)
    return control_schema


_QUALITY_BASE_SQL = """
        quality_base AS (
            SELECT
                pp.procedure_code,
                pp.year,
                pp.npi,
                pp.total_services,
                pp.total_beneficiaries,
                p.provider_type,
                UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_code, '')), '')) AS taxonomy_code,
                CASE
                    WHEN UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_classification, '')), ''))
                         IN ('UNKNOWN', 'NA', 'N/A')
                    THEN NULL
                    ELSE NULLIF(BTRIM(COALESCE(qf.taxonomy_classification, '')), '')
                END AS quality_classification,
                'quality_feature'::varchar AS evidence_source
            FROM {provider_procedure_relation} pp
            JOIN {provider_relation} p
              ON p.npi = pp.npi
             AND p.year = pp.year
            JOIN {quality_relation} qf
              ON qf.npi = pp.npi
             AND qf.year = pp.year
            WHERE NULLIF(BTRIM(COALESCE(qf.taxonomy_code, '')), '') IS NOT NULL
              AND UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_code, '')), ''))
                  NOT IN ('UNKNOWN', 'NA', 'N/A')
        ),
        """

_EMPTY_QUALITY_BASE_SQL = """
        quality_base AS (
            SELECT
                NULL::bigint AS procedure_code,
                NULL::int AS year,
                NULL::bigint AS npi,
                NULL::float8 AS total_services,
                NULL::float8 AS total_beneficiaries,
                NULL::varchar AS provider_type,
                NULL::varchar AS taxonomy_code,
                NULL::varchar AS quality_classification,
                'quality_feature'::varchar AS evidence_source
            WHERE FALSE
        ),
        """

_TAXONOMY_CHOICE_SQL = """
        taxonomy_choice AS (
            SELECT DISTINCT ON (t.npi)
                t.npi,
                UPPER(NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), ''))
                    AS taxonomy_code
            FROM {taxonomy_relation} t
            WHERE NULLIF(BTRIM(COALESCE(t.healthcare_provider_taxonomy_code, '')), '')
                  IS NOT NULL
            ORDER BY
                t.npi,
                CASE
                    WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'
                    THEN 0
                    ELSE 1
                END,
                t.checksum
        ),
        """

_EMPTY_TAXONOMY_CHOICE_SQL = """
        taxonomy_choice AS (
            SELECT NULL::bigint AS npi, NULL::varchar AS taxonomy_code
            WHERE FALSE
        ),
        """

_QUALITY_CODE_SQL = """
        CASE
            WHEN UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_code, '')), ''))
                 IN ('UNKNOWN', 'NA', 'N/A')
            THEN NULL
            ELSE UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_code, '')), ''))
        END
        """

_QUALITY_CLASSIFICATION_SQL = """
        CASE
            WHEN UPPER(NULLIF(BTRIM(COALESCE(qf.taxonomy_classification, '')), ''))
                 IN ('UNKNOWN', 'NA', 'N/A')
            THEN NULL
            ELSE NULLIF(BTRIM(COALESCE(qf.taxonomy_classification, '')), '')
        END
        """

_NUCC_JOIN_SQL = """
        LEFT JOIN (
            SELECT
                UPPER(BTRIM(COALESCE(code, ''))) AS taxonomy_code,
                MAX(classification) AS classification,
                MAX(specialization) AS specialization,
                MAX(display_name) AS display_name
            FROM {nucc_relation}
            WHERE NULLIF(BTRIM(COALESCE(code, '')), '') IS NOT NULL
              AND UPPER(BTRIM(COALESCE(code, ''))) NOT IN ('UNKNOWN', 'NA', 'N/A')
            GROUP BY UPPER(BTRIM(COALESCE(code, '')))
        ) nu ON nu.taxonomy_code = b.taxonomy_code
        """

_SIGNAL_ROWS_CTES = """
        WITH
        {quality_cte}
        {taxonomy_choice_cte}
        fallback_base AS (
            SELECT
                pp.procedure_code,
                pp.year,
                pp.npi,
                pp.total_services,
                pp.total_beneficiaries,
                p.provider_type,
                COALESCE({quality_code}, tc.taxonomy_code) AS taxonomy_code,
                {quality_classification} AS quality_classification,
                'quality_or_nppes'::varchar AS evidence_source
            FROM {provider_procedure_relation} pp
            JOIN {provider_relation} p
              ON p.npi = pp.npi
             AND p.year = pp.year
            {quality_join}
            LEFT JOIN taxonomy_choice tc ON tc.npi = pp.npi
        ),
        evidence_base AS (
            SELECT * FROM quality_base
            UNION ALL
            SELECT * FROM fallback_base WHERE taxonomy_code IS NOT NULL
        ),
        aggregated AS (
            SELECT
                b.procedure_code,
                b.year,
                'all'::varchar AS setting_key,
                b.evidence_source,
                b.taxonomy_code,
                MAX({classification})::varchar AS classification,
                MAX({specialization})::varchar AS specialization,
                MAX({display_name})::varchar AS display_name,
                COUNT(DISTINCT b.npi)::int AS distinct_npis,
                COALESCE(SUM(b.total_services), 0)::float8 AS total_services,
                COALESCE(SUM(b.total_beneficiaries), 0)::float8 AS total_beneficiaries,
                COALESCE(
                    ARRAY_REMOVE(
                        ARRAY_AGG(
                            DISTINCT NULLIF(BTRIM(COALESCE(b.provider_type, '')), '')
                        ),
                        NULL
                    ),
                    ARRAY[]::varchar[]
                )::varchar[] AS provider_types
            FROM evidence_base b
            {nucc_join}
            WHERE b.taxonomy_code IS NOT NULL
            GROUP BY
                b.procedure_code,
                b.year,
                b.evidence_source,
                b.taxonomy_code
        ),
        ranked AS (
            SELECT
                a.*,
                ROW_NUMBER() OVER (
                    PARTITION BY a.procedure_code, a.year, a.evidence_source
                    ORDER BY
                        a.distinct_npis DESC,
                        a.total_services DESC,
                        a.taxonomy_code ASC
                ) AS evidence_rank
            FROM aggregated a
        )
    """


_SIGNAL_INSERT_SQL = """
        INSERT INTO {signal_relation} (
            procedure_code,
            year,
            setting_key,
            evidence_source,
            taxonomy_code,
            classification,
            specialization,
            display_name,
            distinct_npis,
            total_services,
            total_beneficiaries,
            provider_types,
            source_relation_fingerprint,
            updated_at
        )
        SELECT
            procedure_code,
            year,
            setting_key,
            evidence_source,
            taxonomy_code,
            classification,
            specialization,
            display_name,
            distinct_npis,
            total_services,
            total_beneficiaries,
            provider_types,
            {source_relation_fingerprint},
            NOW()
        FROM ranked
        WHERE evidence_rank <= 50
        ON CONFLICT (
            procedure_code,
            year,
            setting_key,
            evidence_source,
            taxonomy_code
        ) DO UPDATE SET
            classification = excluded.classification,
            specialization = excluded.specialization,
            display_name = excluded.display_name,
            distinct_npis = excluded.distinct_npis,
            total_services = excluded.total_services,
            total_beneficiaries = excluded.total_beneficiaries,
            provider_types = excluded.provider_types,
            source_relation_fingerprint = excluded.source_relation_fingerprint,
            updated_at = excluded.updated_at
    """

_SIGNAL_SELECT_SQL = """
        SELECT procedure_code, year, setting_key, evidence_source, taxonomy_code,
               classification, specialization, display_name, distinct_npis,
               total_services, total_beneficiaries, provider_types, NOW() AS updated_at
        FROM ranked
        WHERE evidence_rank <= 50
    """


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table(schema: str, name: str) -> str:
    return f"{_quote(schema)}.{_quote(name)}"


def _relation_oid_sql(schema: str, table: str | None) -> str:
    if table is None:
        return "'0'"
    relation = _table(schema, table).replace("'", "''")
    return f"COALESCE(to_regclass('{relation}')::oid::text, '0')"


def procedure_taxonomy_signal_fingerprint_sql(
    *,
    schema: str,
    provider_table: str,
    provider_procedure_table: str,
    quality_feature_table: str | None,
    npi_taxonomy_table: str | None,
    nucc_taxonomy_table: str | None,
) -> str:
    """Identify the exact source relations used to build one signal generation."""

    source_tables = (
        provider_table,
        provider_procedure_table,
        quality_feature_table,
        npi_taxonomy_table,
        nucc_taxonomy_table,
    )
    relation_oids = ", ".join(_relation_oid_sql(schema, source_table) for source_table in source_tables)
    return f"concat_ws(':', {relation_oids})"


def _quality_source_sql(
    provider_relation: str,
    provider_procedure_relation: str,
    quality_relation: str | None,
) -> tuple[str, str]:
    if quality_relation is None:
        return _EMPTY_QUALITY_BASE_SQL, ""
    quality_cte = _QUALITY_BASE_SQL.format(
        provider_procedure_relation=provider_procedure_relation,
        provider_relation=provider_relation,
        quality_relation=quality_relation,
    )
    quality_join = f"LEFT JOIN {quality_relation} qf ON qf.npi = pp.npi AND qf.year = pp.year"
    return quality_cte, quality_join


def _signal_rows_ctes(
    *,
    provider_relation: str,
    provider_procedure_relation: str,
    quality_relation: str | None,
    taxonomy_relation: str | None,
    nucc_relation: str | None,
) -> str:
    quality_cte, quality_join = _quality_source_sql(
        provider_relation,
        provider_procedure_relation,
        quality_relation,
    )
    taxonomy_choice_cte = (
        _TAXONOMY_CHOICE_SQL.format(taxonomy_relation=taxonomy_relation)
        if taxonomy_relation
        else _EMPTY_TAXONOMY_CHOICE_SQL
    )
    nucc_join = _NUCC_JOIN_SQL.format(nucc_relation=nucc_relation) if nucc_relation else ""
    return _SIGNAL_ROWS_CTES.format(
        quality_cte=quality_cte,
        taxonomy_choice_cte=taxonomy_choice_cte,
        provider_procedure_relation=provider_procedure_relation,
        provider_relation=provider_relation,
        quality_join=quality_join,
        quality_code=_QUALITY_CODE_SQL if quality_relation else "NULL::varchar",
        quality_classification=(_QUALITY_CLASSIFICATION_SQL if quality_relation else "NULL::varchar"),
        nucc_join=nucc_join,
        classification=(
            "COALESCE(b.quality_classification, nu.classification)" if nucc_relation else "b.quality_classification"
        ),
        specialization="nu.specialization" if nucc_relation else "NULL::varchar",
        display_name="nu.display_name" if nucc_relation else "NULL::varchar",
    )


def procedure_taxonomy_signal_select_sql(
    *,
    provider: tuple[str, str],
    procedure: tuple[str, str],
    quality: tuple[str, str] | None,
    npi_taxonomy: tuple[str, str] | None,
    nucc_taxonomy: tuple[str, str] | None,
) -> str:
    """Select the ranked evidence rows from explicitly named input relations."""

    return (
        _signal_rows_ctes(
            provider_relation=_table(*provider),
            provider_procedure_relation=_table(*procedure),
            quality_relation=_table(*quality) if quality else None,
            taxonomy_relation=_table(*npi_taxonomy) if npi_taxonomy else None,
            nucc_relation=_table(*nucc_taxonomy) if nucc_taxonomy else None,
        )
        + _SIGNAL_SELECT_SQL
    )


def procedure_taxonomy_signal_insert_sql(
    *,
    schema: str,
    signal_table: str,
    provider_table: str,
    provider_procedure_table: str,
    quality_feature_table: str | None,
    npi_taxonomy_table: str | None,
    nucc_taxonomy_table: str | None,
) -> str:
    """Populate the legacy table during the historical fast-path migration."""

    ctes = _signal_rows_ctes(
        provider_relation=_table(schema, provider_table),
        provider_procedure_relation=_table(schema, provider_procedure_table),
        quality_relation=_table(schema, quality_feature_table) if quality_feature_table else None,
        taxonomy_relation=_table(schema, npi_taxonomy_table) if npi_taxonomy_table else None,
        nucc_relation=_table(schema, nucc_taxonomy_table) if nucc_taxonomy_table else None,
    )
    return ctes + _SIGNAL_INSERT_SQL.format(
        source_relation_fingerprint=procedure_taxonomy_signal_fingerprint_sql(
            schema=schema,
            provider_table=provider_table,
            provider_procedure_table=provider_procedure_table,
            quality_feature_table=quality_feature_table,
            npi_taxonomy_table=npi_taxonomy_table,
            nucc_taxonomy_table=nucc_taxonomy_table,
        ),
        signal_relation=_table(schema, signal_table),
    )
