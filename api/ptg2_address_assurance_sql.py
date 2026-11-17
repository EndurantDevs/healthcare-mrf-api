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


MEMBERSHIP_NPI_SQL = """
WITH matched AS MATERIALIZED (
    SELECT
        addr.npi,
        {distance_sql} AS distance_miles,
        ROW_NUMBER() OVER (
            PARTITION BY addr.npi
            ORDER BY {distance_sql} ASC NULLS LAST,
                     CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     addr.checksum,
                     addr.location_key
        ) AS address_rank
    FROM {address_table} addr
    WHERE {address_filter_sql}
      AND {address_assurance_sql}
      AND EXISTS (
          SELECT 1
          FROM {npi_scope_table} npi_scope
          WHERE npi_scope.snapshot_key = :shared_snapshot_key
            AND npi_scope.npi = addr.npi
          OFFSET 0
      )
)
SELECT npi, distance_miles, address_rank
FROM matched
WHERE address_rank = 1
ORDER BY distance_miles ASC NULLS LAST, npi
LIMIT :limit OFFSET :offset
"""


MEMBERSHIP_LOCATION_SQL = """
WITH matched AS MATERIALIZED (
    SELECT
        addr.npi,
        {location_hash_sql} AS location_hash,
        addr.state_name AS state,
        addr.city_name AS city,
        LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
        {distance_sql} AS distance_miles,
        addr.type,
        addr.checksum,
        {telephone_sql} AS telephone_number,
        {fax_sql} AS fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        jsonb_build_object(
            'first_line', addr.first_line,
            'second_line', addr.second_line,
            'city', addr.city_name,
            'state', addr.state_name,
            'postal_code', addr.postal_code,
            'country_code', addr.country_code,
            'telephone_number', {telephone_sql},
            'fax_number', {fax_sql},
            'phone_number', addr.phone_number,
            'phone_extension', addr.phone_extension,
            'fax_number_digits', addr.fax_number_digits,
            'fax_extension', addr.fax_extension,
            'address_key', addr.address_key::text,
            {address_site_key_json_pair_sql}
            'location_key', {location_key_sql},
            'address_sources', {address_sources_sql},
            'source_record_ids', {source_record_ids_sql},
            'source_count', {source_count_sql},
            'multi_source_confirmed', {multi_source_sql},
            'source_mask', {source_mask_sql},
            'address_source_mask', {address_source_mask_sql},
            'location_confidence_id', {location_confidence_sql},
            'lat', addr.lat,
            'long', addr.long
        )::text AS address_payload,
        ROW_NUMBER() OVER (
            PARTITION BY addr.npi
            ORDER BY {postal_box_rank_sql},
                     {distance_sql} ASC NULLS LAST,
                     CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     addr.checksum,
                     {location_tiebreak_sql}
        ) AS address_rank
    FROM {address_table} addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE {filter_sql}
      AND {address_assurance_sql}
)
SELECT *
FROM matched
WHERE address_rank = 1
ORDER BY distance_miles ASC NULLS LAST, npi
LIMIT :limit OFFSET :offset
"""


MEMBERSHIP_UNIFIED_ASSURED_LOCATION_SQL = (
    UNIFIED_ASSURED_LOCATION_CTES_SQL
    + """, ranked AS MATERIALIZED (
    SELECT
        classified.location_key,
        classified.npi,
        classified.distance_miles,
        classified.geo_evidence_level,
        ROW_NUMBER() OVER (
            PARTITION BY classified.npi
            ORDER BY classified.distance_miles ASC NULLS LAST,
                     CASE classified.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     classified.checksum,
                     classified.location_key
        ) AS address_rank
    FROM classified_locations AS classified
    WHERE classified.geo_evidence_level IS NOT NULL
), selected AS MATERIALIZED (
    SELECT *
    FROM ranked
    WHERE address_rank = 1
    ORDER BY distance_miles ASC NULLS LAST, npi
    LIMIT :limit OFFSET :offset
)
SELECT
    addr.npi,
    {location_hash_sql} AS location_hash,
    addr.state_name AS state,
    addr.city_name AS city,
    LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
    selected.distance_miles,
    addr.type,
    addr.checksum,
    {telephone_sql} AS telephone_number,
    {fax_sql} AS fax_number,
    addr.phone_number,
    addr.phone_extension,
    addr.fax_number_digits,
    addr.fax_extension,
    selected.geo_evidence_level AS _geo_evidence_level,
    {selected_geo_evidence_source_id_sql} AS _geo_evidence_source_id,
    jsonb_build_object(
        'first_line', addr.first_line,
        'second_line', addr.second_line,
        'city', addr.city_name,
        'state', addr.state_name,
        'postal_code', addr.postal_code,
        'country_code', addr.country_code,
        'telephone_number', {telephone_sql},
        'fax_number', {fax_sql},
        'phone_number', addr.phone_number,
        'phone_extension', addr.phone_extension,
        'fax_number_digits', addr.fax_number_digits,
        'fax_extension', addr.fax_extension,
        'address_key', addr.address_key::text,
        {address_site_key_json_pair_sql}
        'location_key', {location_key_sql},
        'address_sources', {address_sources_sql},
        'source_record_ids', {source_record_ids_sql},
        'source_count', {source_count_sql},
        'multi_source_confirmed', {multi_source_sql},
        'source_mask', {source_mask_sql},
        'address_source_mask', {address_source_mask_sql},
        'location_confidence_id', {location_confidence_sql},
        'lat', addr.lat,
        'long', addr.long
    )::text AS address_payload,
    selected.address_rank
FROM selected
JOIN {address_table} addr ON addr.location_key = selected.location_key
ORDER BY selected.distance_miles ASC NULLS LAST, addr.npi
"""
)


MEMBERSHIP_KNN_SPARSE_SCOPE_LIMIT = 4096
MEMBERSHIP_EXACT_NPI_SCOPE_LIMIT = 16384


MEMBERSHIP_LOCATION_KNN_SQL = """
WITH scope_probe AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS npi_count
    FROM (
        SELECT 1
        FROM {npi_scope_table}
        WHERE snapshot_key = :shared_snapshot_key
        LIMIT {scope_probe_limit}
    ) scoped_npis
), sparse_geocoded_candidates AS MATERIALIZED (
    SELECT
        addr.location_key,
        {distance_sql} AS candidate_distance_miles
    FROM {address_table} addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE (SELECT npi_count < {scope_probe_limit} FROM scope_probe)
      AND npi_scope.snapshot_key = :shared_snapshot_key
      AND addr.npi IS NOT NULL
      AND CASE
              WHEN (SELECT npi_count < {scope_probe_limit} FROM scope_probe)
              THEN ({knn_prefilter_sql})
              ELSE FALSE
          END
      AND addr.type IN ('primary', 'secondary', 'practice', 'site')
      AND COALESCE(addr.address_precision, '') <> 'city_zip'
      AND addr.lat IS NOT NULL
      AND addr.long IS NOT NULL
      -- A scalar fence keeps sparse scopes on their NPI-first plan.
      AND (SELECT {raw_geo_radius_sql} OFFSET 0)
    ORDER BY {knn_order_sql},
             addr.npi,
             CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
             addr.checksum,
             addr.location_key
    LIMIT :raw_probe_limit
), broad_geocoded_candidates AS MATERIALIZED (
    SELECT
        addr.location_key,
        {distance_sql} AS candidate_distance_miles
    FROM {address_table} addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE (SELECT npi_count >= {scope_probe_limit} FROM scope_probe)
      AND npi_scope.snapshot_key = :shared_snapshot_key
      AND addr.npi IS NOT NULL
      AND CASE
              WHEN (SELECT npi_count >= {scope_probe_limit} FROM scope_probe)
              THEN ({knn_prefilter_sql})
              ELSE FALSE
          END
      AND addr.type IN ('primary', 'secondary', 'practice', 'site')
      AND COALESCE(addr.address_precision, '') <> 'city_zip'
      AND addr.lat IS NOT NULL
      AND addr.long IS NOT NULL
      -- Keep the radius planner-visible so underfilled broad scans can stop.
      AND {raw_geo_radius_sql}
    ORDER BY {knn_order_sql},
             addr.npi,
             CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
             addr.checksum,
             addr.location_key
    LIMIT :raw_probe_limit
), geocoded_candidates AS MATERIALIZED (
    SELECT * FROM sparse_geocoded_candidates
    UNION ALL
    SELECT * FROM broad_geocoded_candidates
), geocoded_probe_stats AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS raw_probe_count
    FROM geocoded_candidates
), exact_zip_candidates AS MATERIALIZED (
    SELECT
        exact_zip.location_key,
        NULL::double precision AS candidate_distance_miles
    FROM geocoded_probe_stats
    CROSS JOIN LATERAL (
        SELECT addr.location_key
        FROM {address_table} addr
        JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
        WHERE geocoded_probe_stats.raw_probe_count < :raw_probe_limit
          AND npi_scope.snapshot_key = :shared_snapshot_key
          AND addr.npi IS NOT NULL
          AND CASE
                  WHEN geocoded_probe_stats.raw_probe_count < :raw_probe_limit
                  THEN ({knn_prefilter_sql})
                  ELSE FALSE
              END
          AND addr.type IN ('primary', 'secondary', 'practice', 'site')
          AND {exact_zip_candidate_filter_sql}
        ORDER BY addr.npi,
                 CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                 addr.checksum,
                 addr.location_key
        LIMIT GREATEST(
            :raw_probe_limit - geocoded_probe_stats.raw_probe_count,
            0
        )
    ) exact_zip
), exact_zip_probe_stats AS MATERIALIZED (
    SELECT COUNT(*)::bigint AS raw_probe_count
    FROM exact_zip_candidates
), candidate_keys AS MATERIALIZED (
    SELECT * FROM geocoded_candidates
    UNION ALL
    SELECT * FROM exact_zip_candidates
), nearest_addresses AS MATERIALIZED (
    SELECT
        addr.npi,
        addr.state_name,
        addr.city_name,
        addr.postal_code,
        addr.lat,
        addr.long,
        addr.type,
        addr.checksum,
        addr.telephone_number,
        addr.fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        addr.first_line,
        addr.second_line,
        addr.country_code,
        addr.address_key,
        addr.premise_key,
        {location_key_sql} AS location_key,
        {address_sources_sql} AS address_sources,
        {source_record_ids_sql} AS source_record_ids,
        {source_count_sql} AS source_count,
        {multi_source_sql} AS multi_source_confirmed,
        {source_mask_sql} AS source_mask,
        {address_source_mask_sql} AS address_source_mask,
        {location_confidence_sql} AS location_confidence_id,
        {geo_evidence_level_sql} AS geo_evidence_level,
        candidate.candidate_distance_miles
    FROM candidate_keys candidate
    CROSS JOIN LATERAL (
        SELECT candidate_address.*
        FROM {address_table} candidate_address
        WHERE candidate_address.location_key = candidate.location_key
        -- Fence the bounded candidates onto the location-key PK lookup.
        OFFSET 0
    ) addr
    JOIN {npi_scope_table} npi_scope ON npi_scope.npi = addr.npi
    WHERE {filter_sql}
), probe_stats AS MATERIALIZED (
    SELECT
        geocoded.raw_probe_count AS geocoded_probe_count,
        exact_zip.raw_probe_count AS exact_zip_probe_count
    FROM geocoded_probe_stats geocoded
    CROSS JOIN exact_zip_probe_stats exact_zip
), matched AS MATERIALIZED (
    SELECT
        addr.npi,
        {location_hash_sql} AS location_hash,
        addr.state_name AS state,
        addr.city_name AS city,
        LEFT(COALESCE(addr.postal_code, ''), 5) AS zip5,
        addr.candidate_distance_miles AS distance_miles,
        addr.type,
        addr.checksum,
        {telephone_sql} AS telephone_number,
        {fax_sql} AS fax_number,
        addr.phone_number,
        addr.phone_extension,
        addr.fax_number_digits,
        addr.fax_extension,
        addr.geo_evidence_level AS _geo_evidence_level,
        {knn_geo_evidence_source_id_sql} AS _geo_evidence_source_id,
        jsonb_build_object(
            'first_line', addr.first_line,
            'second_line', addr.second_line,
            'city', addr.city_name,
            'state', addr.state_name,
            'postal_code', addr.postal_code,
            'country_code', addr.country_code,
            'telephone_number', {telephone_sql},
            'fax_number', {fax_sql},
            'phone_number', addr.phone_number,
            'phone_extension', addr.phone_extension,
            'fax_number_digits', addr.fax_number_digits,
            'fax_extension', addr.fax_extension,
            'address_key', addr.address_key::text,
            {address_site_key_json_pair_sql}
            'location_key', addr.location_key,
            'address_sources', addr.address_sources,
            'source_record_ids', addr.source_record_ids,
            'source_count', addr.source_count,
            'multi_source_confirmed', addr.multi_source_confirmed,
            'source_mask', addr.source_mask,
            'address_source_mask', addr.address_source_mask,
            'location_confidence_id', addr.location_confidence_id,
            'lat', addr.lat,
            'long', addr.long
        )::text AS address_payload,
        ROW_NUMBER() OVER (
            PARTITION BY addr.npi
            ORDER BY addr.candidate_distance_miles ASC NULLS LAST,
                     CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                     addr.checksum,
                     addr.location_key
        ) AS address_rank
    FROM nearest_addresses addr
    WHERE addr.geo_evidence_level IS NOT NULL
)
SELECT selected.*,
       (
           probe_stats.geocoded_probe_count + probe_stats.exact_zip_probe_count < :raw_probe_limit
           AND NOT EXISTS (SELECT 1 FROM matched WHERE address_rank = 1 OFFSET :limit)
       ) AS _ptg_source_exhausted,
       (selected.npi IS NULL) AS _ptg_probe_empty
FROM probe_stats
LEFT JOIN LATERAL (
    SELECT matched.*
    FROM matched
    WHERE address_rank = 1
    ORDER BY distance_miles ASC NULLS LAST, npi
    LIMIT :limit
) selected ON TRUE
ORDER BY selected.distance_miles ASC NULLS LAST, selected.npi
"""
