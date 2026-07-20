# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Set-wise SQL for immutable Provider Directory source summaries."""

from __future__ import annotations


SOURCE_SUMMARY_METRICS_SQL_TEMPLATE = """
    WITH immutable_resources AS (
        SELECT resource_type,
               payload_json::jsonb AS payload
          FROM {resource_ref}
         WHERE dataset_id = :dataset_id
           AND resource_type = ANY(CAST(:summary_resource_types AS varchar[]))
    ), summarized AS (
        SELECT COUNT(DISTINCT NULLIF(BTRIM(payload ->> 'npi'), ''))
                   FILTER (
                       WHERE resource_type = ANY(
                           CAST(:npi_resource_types AS varchar[])
                       )
                   )::bigint AS distinct_npis,
               COALESCE(
                   SUM(
                       CASE
                           WHEN resource_type = 'Organization'
                            AND jsonb_typeof(payload -> 'address_json') = 'array'
                           THEN jsonb_array_length(payload -> 'address_json')
                           WHEN resource_type = ANY(
                                    CAST(:normalized_address_resource_types AS varchar[])
                                )
                            AND jsonb_typeof(payload -> 'addresses') = 'array'
                           THEN jsonb_array_length(payload -> 'addresses')
                           ELSE 0
                       END
                   ),
                   0
               )::bigint AS address_records,
               COUNT(*) FILTER (
                   WHERE resource_type = 'Location'
                     AND (
                         (
                             jsonb_typeof(payload -> 'addresses') = 'array'
                             AND jsonb_array_length(payload -> 'addresses') > 0
                         )
                         OR NULLIF(BTRIM(payload ->> 'first_line'), '')
                            IS NOT NULL
                         OR NULLIF(BTRIM(payload ->> 'city_name'), '')
                            IS NOT NULL
                         OR NULLIF(BTRIM(payload ->> 'state_code'), '')
                            IS NOT NULL
                         OR NULLIF(BTRIM(payload ->> 'postal_code'), '')
                            IS NOT NULL
                     )
               )::bigint AS addressed_locations,
               COUNT(*) FILTER (
                   WHERE resource_type = 'Location'
                     AND NULLIF(BTRIM(payload ->> 'latitude'), '') IS NOT NULL
                     AND NULLIF(BTRIM(payload ->> 'longitude'), '') IS NOT NULL
               )::bigint AS geocoded_locations
          FROM immutable_resources
    )
    SELECT distinct_npis,
           address_records,
           addressed_locations,
           geocoded_locations
      FROM summarized;
"""


def source_summary_metrics_sql(resource_ref: str) -> str:
    """Bind the schema-qualified immutable resource relation."""
    return SOURCE_SUMMARY_METRICS_SQL_TEMPLATE.format(
        resource_ref=resource_ref,
    )
