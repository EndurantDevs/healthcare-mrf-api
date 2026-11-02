# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded ordinal pages and compact indexed status for v2 witness mapping."""

WITNESS_PAGE_SQL = """ordinals AS MATERIALIZED (
 SELECT ordinal FROM {fact_relation}
 WHERE recipe_sha256={identity} AND ordinal>{cursor}
 AND ordinal<=LEAST({cursor},9223372036854774807::bigint)+1000 ORDER BY ordinal LIMIT 1000
), sized AS MATERIALIZED (
 SELECT ordinals.ordinal,selected.row_bytes FROM ordinals
 CROSS JOIN LATERAL (SELECT octet_length(fact_json::text) AS row_bytes FROM {fact_relation}
   WHERE recipe_sha256={identity} AND ordinal=ordinals.ordinal OFFSET 0) selected
), bounded AS MATERIALIZED (
 SELECT ordinal,row_bytes,sum(row_bytes) OVER(ORDER BY ordinal) AS page_bytes FROM sized
), facts AS MATERIALIZED (
 SELECT selected.ordinal,selected.fact_json,bounded.row_bytes FROM bounded
 CROSS JOIN LATERAL (SELECT ordinal,fact_json FROM {fact_relation}
   WHERE recipe_sha256={identity} AND ordinal=bounded.ordinal OFFSET 0) selected
 WHERE bounded.page_bytes<=8388608 OR bounded.ordinal=(SELECT min(ordinal) FROM sized)
)"""

WITNESS_HEADERS_SQL = """WITH {page}, invalid AS (
 SELECT 1 FROM facts WHERE fact_json->'fact_version' IS DISTINCT FROM '2'::jsonb
 OR fact_json->'source_binding' IS DISTINCT FROM $2::jsonb
 OR fact_json->>'kind' IS NULL OR fact_json->>'kind' NOT IN ('resource','edge')
), headers AS (
 SELECT fact_json->>'resource_type' AS resource_type,fact_json->>'resource_id' AS resource_id,
 fact_json->'payload_sha256' AS payload_sha256,(fact_json->>'malformed')::boolean AS malformed,
 fact_json->>'omission_reason' AS omission_reason FROM facts WHERE fact_json->>'kind'='resource'
), inserted AS (
 INSERT INTO {workspace}(recipe_sha256,resource_type,resource_id,payload_sha256,malformed,omission_reason)
 SELECT $1,resource_type,resource_id,payload_sha256,malformed,omission_reason FROM headers
 WHERE NOT EXISTS(SELECT 1 FROM invalid) ON CONFLICT DO NOTHING RETURNING 1
)
SELECT EXISTS(SELECT 1 FROM invalid) OR (SELECT count(*) FROM inserted)<>(SELECT count(*) FROM headers) AS invalid,
 (SELECT count(*) FROM facts) AS fact_rows,(SELECT max(ordinal) FROM facts) AS last_ordinal"""

WITNESS_MAP_SQL_V2 = """WITH {bindings},{page}, edges AS MATERIALIZED (
 SELECT facts.ordinal,facts.fact_json,header.header_tid,header.payload_sha256,
 header.malformed,header.omission_reason FROM facts
 LEFT JOIN LATERAL (SELECT ctid AS header_tid,payload_sha256,malformed,omission_reason FROM {workspace}
   WHERE recipe_sha256=$8 AND resource_type=facts.fact_json->>'resource_type' COLLATE "C"
   AND resource_id=facts.fact_json->>'resource_id' COLLATE "C" OFFSET 0) header ON true
 WHERE facts.fact_json->>'kind'='edge'
), invalid AS (
 SELECT 1 FROM facts WHERE fact_json->'fact_version' IS DISTINCT FROM '2'::jsonb
 OR fact_json->'source_binding' IS DISTINCT FROM $10::jsonb
 OR fact_json->>'kind' IS NULL OR fact_json->>'kind' NOT IN ('resource','edge')
 UNION ALL SELECT 1 FROM edges WHERE header_tid IS NULL
 OR payload_sha256 IS DISTINCT FROM fact_json->'payload_sha256'
), mapped AS MATERIALIZED (
 SELECT edges.ordinal,edges.header_tid,edges.omission_reason,approved.network_id,
 (edges.fact_json->>'provider_only')::boolean AS provider_only,
 edges.fact_json->>'location_id' IS NOT NULL AS has_location,
 edges.malformed OR (edges.fact_json->>'provider_unresolved')::boolean
 OR (edges.fact_json->>'location_unresolved')::boolean OR edges.fact_json->>'source_key' IS NULL
 OR (NOT (edges.fact_json->>'network_witness_complete')::boolean AND (NOT $9::boolean OR approved.network_id IS NOT NULL)) AS unresolved
 FROM edges LEFT JOIN approved_network_bindings approved
 ON approved.record_json->>'source_key'=edges.fact_json->>'source_key'
 AND approved.record_json->'source_scope_json'=edges.fact_json->'source_scope_json'
), edge_status AS MATERIALIZED (
 SELECT *,NOT unresolved AND omission_reason IS NULL AND network_id IS NULL AS omitted,
 NOT unresolved AND omission_reason IS NULL AND network_id IS NOT NULL AS accepted FROM mapped
), grouped AS (
 SELECT header_tid,count(*) AS edge_count,coalesce(bool_or(unresolved),false) AS has_unresolved,
 coalesce(bool_or(accepted),false) AS has_accepted,
 coalesce(bool_or(accepted AND provider_only),false) AS has_provider_only
 FROM edge_status WHERE header_tid IS NOT NULL GROUP BY header_tid
), updated AS (
 UPDATE {workspace} status SET edge_count=status.edge_count+grouped.edge_count,
 has_unresolved=status.has_unresolved OR grouped.has_unresolved,
 has_accepted=status.has_accepted OR grouped.has_accepted,
 has_provider_only=status.has_provider_only OR grouped.has_provider_only
 FROM grouped WHERE status.ctid=grouped.header_tid AND NOT EXISTS(SELECT 1 FROM invalid) RETURNING 1
), inserted AS (
 {insert}
)
SELECT EXISTS(SELECT 1 FROM invalid) AS invalid,(SELECT count(*) FROM facts) AS fact_rows,
 (SELECT max(ordinal) FROM facts) AS last_ordinal,
 (SELECT count(*) FROM edge_status WHERE accepted AND has_location) AS membership_rows,
 (SELECT count(*) FROM edge_status WHERE omitted) AS omitted_rows,
 (SELECT count(*) FROM edge_status WHERE unresolved) AS unresolved_rows,
 (SELECT count(*) FROM inserted) AS inserted_rows"""

WITNESS_TOTALS_SQL = """WITH resource_status AS (
 SELECT has_provider_only,CASE WHEN malformed OR has_unresolved OR (omission_reason IS NULL AND edge_count=0)
 THEN 'unresolved' WHEN omission_reason IS NOT NULL THEN 'omitted'
 WHEN has_accepted THEN 'accepted' ELSE 'omitted' END AS outcome
 FROM {workspace} WHERE recipe_sha256=$1
)
SELECT count(*) AS raw_source_rows,count(*) FILTER(WHERE outcome='accepted') AS accepted_raw_source_rows,
 count(*) FILTER(WHERE outcome='unresolved') AS unresolved_raw_source_rows,
 count(*) FILTER(WHERE outcome='omitted') AS omitted_raw_source_rows,
 count(*) FILTER(WHERE has_provider_only) AS provider_only_rows FROM resource_status"""
