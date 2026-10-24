-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT 'source_occurrence_position' AS failure_code,o.occurrence_id AS row_identity
FROM __CANDIDATE__.custom_import_build_occurrence o
WHERE o.origin='source' AND NOT EXISTS (
    SELECT 1 FROM __CONTROL__.custom_import_prepared_source_part part
    WHERE part.execution_id=:execution_id AND part.dataset_id=:dataset_id
      AND part.definition_revision_id=:definition_revision_id AND part.schema_revision_id=:schema_revision_id
      AND part.stream_slot=o.stream_slot AND part.part_ordinal=o.source_part_ordinal
      AND part.payload_contract='custom-import/prepared-source-rows/v1'
      AND o.part_row_ordinal>=0 AND o.part_row_ordinal<part.record_count
      AND o.source_ordinal=part.first_source_ordinal-1+o.part_row_ordinal
) LIMIT 1;
-- statement boundary --
WITH expected AS (
    SELECT * FROM jsonb_to_recordset(CAST(:streams AS jsonb)) AS stream(
        stream_slot smallint,part_count integer,record_count bigint,byte_count bigint,manifest_byte_count bigint)
), parts AS (
    SELECT p.dataset_id,p.definition_revision_id,p.schema_revision_id,p.stream_slot,p.part_ordinal,
        p.first_source_ordinal,p.record_count,p.byte_count,p.canonical_manifest,p.payload_contract,
        row_number() OVER ordered AS expected_part,
        coalesce(sum(record_count) OVER (ordered ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),0)+1 AS expected_first
    FROM __CONTROL__.custom_import_prepared_source_part p WHERE execution_id=:execution_id
    WINDOW ordered AS (PARTITION BY stream_slot ORDER BY part_ordinal)
), totals AS (
    SELECT stream_slot,count(*) AS part_count,sum(record_count) AS record_count,sum(byte_count) AS byte_count,
        sum(octet_length(canonical_manifest)) AS manifest_byte_count
    FROM parts GROUP BY stream_slot
)
SELECT 'capture_stream' AS failure_code,:build_id AS row_identity
WHERE EXISTS (
    SELECT 1 FROM __CONTROL__.custom_import_source_stream s
    LEFT JOIN __CONTROL__.custom_import_capture c ON c.capture_bundle_id=:capture_bundle_id AND c.stream_slot=s.stream_slot
    LEFT JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=:build_id AND bs.stream_slot=s.stream_slot
    LEFT JOIN expected ON expected.stream_slot=s.stream_slot
    LEFT JOIN totals ON totals.stream_slot=s.stream_slot
    WHERE s.definition_revision_id=:definition_revision_id AND (
        c.stream_slot IS NULL OR bs.build_id IS NULL OR c.capture_state IS DISTINCT FROM 'sealed'
        OR ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id)
            IS DISTINCT FROM ROW(:dataset_id,:definition_revision_id,:schema_revision_id)
        OR c.payload_contract IS NOT NULL OR bs.replay_verified_at IS NULL OR expected.stream_slot IS NULL
        OR expected.record_count IS DISTINCT FROM bs.next_source_ordinal
        OR bs.next_part_ordinal IS DISTINCT FROM expected.part_count+1 OR bs.next_part_row_ordinal<>0
        OR ROW(totals.part_count,totals.record_count,totals.byte_count,totals.manifest_byte_count)
            IS DISTINCT FROM ROW(expected.part_count,expected.record_count,expected.byte_count,expected.manifest_byte_count)))
    OR EXISTS (SELECT 1 FROM expected WHERE NOT EXISTS (
        SELECT 1 FROM __CONTROL__.custom_import_source_stream s
        WHERE s.definition_revision_id=:definition_revision_id AND s.stream_slot=expected.stream_slot))
    OR EXISTS (SELECT 1 FROM parts WHERE
        ROW(dataset_id,definition_revision_id,schema_revision_id)
            IS DISTINCT FROM ROW(:dataset_id,:definition_revision_id,:schema_revision_id)
        OR payload_contract<>'custom-import/prepared-source-rows/v1'
        OR part_ordinal<>expected_part OR first_source_ordinal<>expected_first
        OR NOT EXISTS (SELECT 1 FROM expected WHERE expected.stream_slot=parts.stream_slot))
    OR (SELECT count(*) FROM __CONTROL__.custom_import_source_stream WHERE definition_revision_id=:definition_revision_id)
        IS DISTINCT FROM (SELECT stream_count::bigint FROM __CONTROL__.custom_import_capture_bundle WHERE capture_bundle_id=:capture_bundle_id)
    OR (SELECT count(*) FROM __CONTROL__.custom_import_capture WHERE capture_bundle_id=:capture_bundle_id)
        IS DISTINCT FROM (SELECT stream_count::bigint FROM __CONTROL__.custom_import_capture_bundle WHERE capture_bundle_id=:capture_bundle_id)
    OR :source_occurrence_count IS DISTINCT FROM (SELECT count(*) FROM __CANDIDATE__.custom_import_build_occurrence WHERE origin='source')
    OR :next_rejection_ordinal IS DISTINCT FROM (SELECT count(*) FROM __CANDIDATE__.custom_import_rejection)
    OR :source_frozen_at IS NULL OR :graph_frozen_at IS NULL OR :output_frozen_at IS NULL;
