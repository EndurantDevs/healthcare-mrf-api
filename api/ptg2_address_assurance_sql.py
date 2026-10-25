# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared complete-address assurance SQL before optional provider ranking."""

UNIFIED_ASSURED_LOCATION_CTES_SQL = """
WITH located AS MATERIALIZED (
    {located_scan_sql}
), nppes_requested AS MATERIALIZED (
    SELECT DISTINCT located.npi, located.address_key
    FROM located
    WHERE {legacy_geo_evidence_required_sql}
      AND (located.address_source_mask & 1) <> 0
      AND located.address_key IS NOT NULL
), nppes_assured AS MATERIALIZED (
    SELECT requested.npi, requested.address_key
    FROM nppes_requested requested
    CROSS JOIN LATERAL (
        SELECT 1
        FROM {ptg2_schema}.npi_address nppes
        WHERE nppes.npi = requested.npi
          AND nppes.address_key = requested.address_key
          AND nppes.date_added IS NOT NULL
        LIMIT 1
    ) complete_nppes
), mrf_requested AS MATERIALIZED (
    SELECT DISTINCT located.npi, located.address_key
    FROM located
    LEFT JOIN nppes_assured nppes
      ON nppes.npi = located.npi
     AND nppes.address_key = located.address_key
    WHERE {legacy_geo_evidence_required_sql}
      AND located.address_key IS NOT NULL
      AND (
          (located.address_source_mask & 1) = 0
          OR nppes.npi IS NULL
      )
), mrf_assured AS MATERIALIZED (
    SELECT requested.npi, requested.address_key
    FROM mrf_requested requested
    CROSS JOIN LATERAL (
        SELECT 1
        FROM {ptg2_schema}.mrf_address mrf
        WHERE mrf.npi = requested.npi
          AND mrf.address_key = requested.address_key
          AND {mrf_issuer_assurance_sql}
          AND {mrf_lineage_complete_sql}
        LIMIT 1
    ) assured
), cms_requested AS MATERIALIZED (
    SELECT DISTINCT located.npi, located.address_key, located.premise_key
    FROM located
    LEFT JOIN nppes_assured nppes
      ON nppes.npi = located.npi
     AND nppes.address_key = located.address_key
    LEFT JOIN mrf_assured mrf
      ON mrf.npi = located.npi
     AND mrf.address_key = located.address_key
    WHERE {legacy_geo_evidence_required_sql}
      AND (located.address_source_mask & 4) <> 0
      AND located.address_key IS NOT NULL
      AND located.premise_key IS NOT NULL
      AND (
          (located.address_source_mask & 1) = 0
          OR nppes.npi IS NULL
      )
      AND mrf.npi IS NULL
), cms_assured AS MATERIALIZED (
    SELECT requested.npi, requested.address_key, requested.premise_key
    FROM cms_requested requested
    CROSS JOIN LATERAL (
        SELECT 1
        FROM {ptg2_schema}.entity_address_unified candidate
        JOIN {ptg2_schema}.npi_address anchor_source
          ON anchor_source.npi = candidate.npi
         AND anchor_source.address_key = candidate.address_key
         AND anchor_source.date_added IS NOT NULL
        WHERE candidate.npi = requested.npi
          AND candidate.premise_key = requested.premise_key
          AND (candidate.address_source_mask & 1) <> 0
          AND candidate.type IN ('primary', 'secondary', 'practice', 'site')
          AND EXISTS (
              SELECT 1
              FROM {ptg2_schema}.doctor_clinician_address doctor
              WHERE doctor.npi = requested.npi
                AND doctor.address_key = requested.address_key
                AND doctor.updated_at IS NOT NULL
          )
        LIMIT 1
    ) nppes
), classified_locations AS MATERIALIZED (
    SELECT
        located.*,
        {assured_geo_evidence_level_sql} AS geo_evidence_level
    FROM located
    LEFT JOIN nppes_assured nppes
      ON nppes.npi = located.npi
     AND nppes.address_key = located.address_key
    LEFT JOIN mrf_assured mrf
      ON mrf.npi = located.npi
     AND mrf.address_key = located.address_key
    LEFT JOIN cms_assured cms
      ON cms.npi = located.npi
     AND cms.address_key = located.address_key
     AND cms.premise_key = located.premise_key
)"""
