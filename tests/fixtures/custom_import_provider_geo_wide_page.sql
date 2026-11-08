-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
-- Frozen pre-hydration query for synthetic payload parity.
WITH custom_import_provider_relation AS (SELECT :__custom_import_0 AS entity_value, :__custom_import_1 AS sort_0 UNION ALL SELECT :__custom_import_2 AS entity_value, :__custom_import_3 AS sort_0 UNION ALL SELECT :__custom_import_4 AS entity_value, :__custom_import_5 AS sort_0),
eligible_geo AS MATERIALIZED (
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
      FROM {address_table} AS a
      JOIN mrf.npi AS d ON d.npi = a.npi
     WHERE ST_DWithin(
               Geography(ST_MakePoint((a.long)::double precision, (a.lat)::double precision)),
               Geography(ST_MakePoint(CAST(:in_long AS double precision), CAST(:in_lat AS double precision))),
               :radius * 1609.34
           )
       AND a.lat IS NOT NULL
       AND a.long IS NOT NULL
       AND a.address_key IS NOT NULL

       AND (a.type = 'primary' OR a.type = 'secondary'){membership_clause}

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
              {address_tiebreaker}
),
selected_geo AS MATERIALIZED (
    SELECT eligible_geo.*,
           imported.entity_value AS _custom_import_entity_value,
           imported.sort_0
      FROM eligible_geo
 LEFT JOIN custom_import_provider_relation AS imported
        ON imported.entity_value = eligible_geo.npi_code::text{require_match_clause}
),
geo_totals AS (SELECT COUNT(*) AS _geo_total FROM selected_geo),
page_geo AS MATERIALIZED (
    SELECT selected_geo.*,
           ROW_NUMBER() OVER (ORDER BY (selected_geo._custom_import_entity_value IS NULL) ASC, selected_geo.sort_0 {direction} NULLS LAST, selected_geo.cursor_distance_meters ASC, selected_geo.npi_code ASC, selected_geo.address_key ASC) AS _geo_page_position
      FROM selected_geo
     ORDER BY (selected_geo._custom_import_entity_value IS NULL) ASC, selected_geo.sort_0 {direction} NULLS LAST, selected_geo.cursor_distance_meters ASC, selected_geo.npi_code ASC, selected_geo.address_key ASC
     LIMIT :__custom_import_geo_page_limit
)
SELECT page_geo.*, taxonomy.*, nucc.display_name AS taxonomy_display, geo_totals.*
  FROM geo_totals
  LEFT JOIN page_geo ON TRUE
  LEFT JOIN mrf.npi_taxonomy AS taxonomy ON page_geo.npi_code = taxonomy.npi
  LEFT JOIN mrf.nucc_taxonomy AS nucc
    ON nucc.code = taxonomy.healthcare_provider_taxonomy_code
 ORDER BY page_geo._geo_page_position ASC,
          page_geo.npi_code ASC,
          page_geo.address_key ASC
