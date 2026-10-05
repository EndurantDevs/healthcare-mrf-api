-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT violation.* FROM (
        -- All source evidence is scanned, including rejected records and losing
        -- collapse-identical occurrences. Only graph attachment uses accepted choices.
        WITH expected_build AS (
            SELECT b.* FROM __CONTROL__.custom_import_build_attempt b
            WHERE b.execution_id=$5 AND b.producing_fence=$7
        ), streams AS (
            SELECT s.*,coalesce(d.canonical_definition::jsonb->'streams'->(s.stream_slot-1)->>'duplicate_policy','reject') AS duplicate_policy
            FROM __CONTROL__.custom_import_source_stream s
            JOIN __CONTROL__.custom_import_definition_revision d ON d.definition_revision_id=s.definition_revision_id
              AND d.dataset_id=s.dataset_id AND d.schema_revision_id=s.schema_revision_id
            WHERE s.dataset_id=$2 AND s.definition_revision_id=$3 AND s.schema_revision_id=$4
        ), accepted_children AS (
            SELECT o.* FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN streams s ON s.stream_slot=o.stream_slot AND s.record_kind='child' AND s.collection_slot=o.collection_slot
            WHERE o.origin='source' AND o.child_revision_id IS NOT NULL AND o.resolved_rejection_id IS NULL
              AND (s.duplicate_policy<>'collapse_identical' OR NOT EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence later
                WHERE later.build_id=o.build_id AND later.origin='source' AND later.stream_slot=o.stream_slot
                  AND later.root_record_id=o.root_record_id AND later.collection_slot=o.collection_slot
                  AND later.raw_parent_key_sha256=o.raw_parent_key_sha256 AND later.child_key_sha256=o.child_key_sha256
                  AND later.child_revision_id IS NOT NULL AND later.source_ordinal>o.source_ordinal
              ))
        ), plan_roots AS (
            SELECT o.root_record_id FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.origin='source' AND o.record_kind='root' AND o.root_record_id IS NOT NULL
            UNION
            SELECT m.root_record_id FROM __BASE__.custom_import_generation_family m
            WHERE m.generation_id=$9 AND m.dataset_id=$2
        ), ordered_source_roots AS (
            SELECT o.*,row_number() OVER (PARTITION BY o.build_id,o.root_record_id ORDER BY o.occurrence_id) AS position
            FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.origin='source' AND o.record_kind='root' AND o.root_record_id IS NOT NULL
        ), plan_inputs AS (
            SELECT b.build_id,root.root_record_id,o.occurrence_id,o.root_revision_id,o.resolved_rejection_id,
              m.family_revision_id AS base_family_revision_id,b.refresh_mode
            FROM expected_build b CROSS JOIN plan_roots root
            LEFT JOIN ordered_source_roots o ON o.build_id=b.build_id AND o.root_record_id=root.root_record_id AND o.position=1
            LEFT JOIN __BASE__.custom_import_generation_family m ON m.generation_id=$9
              AND m.dataset_id=$2 AND m.root_record_id=root.root_record_id
            WHERE b.plan_complete_at IS NOT NULL OR $1 IS NOT NULL
        ), expected_plans AS (
            SELECT input.*,
              CASE WHEN input.occurrence_id IS NOT NULL AND input.resolved_rejection_id IS NULL
                AND input.root_revision_id IS NOT NULL THEN 'source'
                WHEN input.base_family_revision_id IS NOT NULL
                  AND (input.refresh_mode='upsert' OR input.resolved_rejection_id IS NOT NULL) THEN 'retained' END AS selection_kind
            FROM plan_inputs input
        )
        SELECT 'occurrence_owner'::text AS failure_code,o.occurrence_id::bigint AS row_identity
        FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE NOT EXISTS (
            SELECT 1 FROM expected_build b
            JOIN __CONTROL__.custom_import_build_stream bs ON bs.build_id=b.build_id AND bs.stream_slot=o.stream_slot
            JOIN streams s ON s.stream_slot=bs.stream_slot
            JOIN __CANDIDATE__.custom_import_pack p ON p.pack_id=o.pack_id
            WHERE b.build_id=o.build_id AND ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256,p.stream_slot)
              IS NOT DISTINCT FROM ROW($5,$2,$3,$4,
                $6,$7,$8,o.stream_slot)
              AND ROW(s.record_kind,coalesce(s.collection_slot,0)) IS NOT DISTINCT FROM ROW(o.record_kind,o.collection_slot)
        )
          OR (o.root_record_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_record r
            WHERE r.root_record_id=o.root_record_id AND r.dataset_id=$2
          ))
        UNION ALL
        SELECT 'source_occurrence_position',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.origin='source' AND NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_capture_parquet_part part
            WHERE part.capture_bundle_id=$6 AND part.stream_slot=o.stream_slot
              AND part.part_ordinal=o.source_part_ordinal AND part.record_count IS NOT NULL
              AND o.part_row_ordinal>=0 AND o.part_row_ordinal<part.record_count
        )
        UNION ALL
        SELECT 'occurrence_root_revision',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.root_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_revision r WHERE r.root_revision_id=o.root_revision_id
              AND ROW(r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.root_record_id,r.pack_id)
                IS NOT DISTINCT FROM ROW($2,$3,$4,o.root_record_id,o.pack_id)
              AND (o.origin='retained' OR r.source_ordinal IS NOT DISTINCT FROM o.source_ordinal)
        )
        UNION ALL
        SELECT 'occurrence_child_revision',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.child_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c WHERE c.child_revision_id=o.child_revision_id
              AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot,c.pack_id,c.child_key_sha256)
                IS NOT DISTINCT FROM ROW($2,$3,$4,o.root_record_id,o.collection_slot,o.pack_id,o.child_key_sha256)
              AND (o.origin='retained' OR c.source_ordinal IS NOT DISTINCT FROM o.source_ordinal)
        )
        UNION ALL
        SELECT 'occurrence_initial_rejection',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.rejection_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_rejection r WHERE r.rejection_id=o.rejection_id
              AND ROW(r.execution_id,r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.producing_fence,r.producing_token_sha256,
                r.pack_id,r.source_ordinal,coalesce(r.collection_slot,0))
                IS NOT DISTINCT FROM ROW($5,$2,$3,$4,$7,$8,
                  o.pack_id,o.source_ordinal,o.collection_slot)
        )
        UNION ALL
        SELECT 'occurrence_resolved_rejection',o.occurrence_id FROM __CANDIDATE__.custom_import_build_occurrence o
        WHERE o.resolved_rejection_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_rejection r
            JOIN __CANDIDATE__.custom_import_build_occurrence event
              ON event.build_id=o.build_id AND event.origin='source'
              AND event.pack_id=r.pack_id AND event.source_ordinal=r.source_ordinal
              AND event.collection_slot=coalesce(r.collection_slot,0)
              AND (event.rejection_id=r.rejection_id OR event.resolved_rejection_id=r.rejection_id)
            WHERE r.rejection_id=o.resolved_rejection_id
              AND ROW(r.execution_id,r.dataset_id,r.definition_revision_id,r.schema_revision_id,r.producing_fence,r.producing_token_sha256)
                IS NOT DISTINCT FROM ROW($5,$2,$3,$4,$7,$8)
              AND (event.occurrence_id=o.occurrence_id OR (
                o.record_kind='root' AND event.record_kind='child' AND event.raw_parent_key_sha256=o.raw_parent_key_sha256
                AND event.raw_parent_key_canonical COLLATE "C"=o.raw_parent_key_canonical COLLATE "C"
              ))
        )
        UNION ALL
        SELECT 'root_revision_missing_occurrence',r.root_revision_id FROM __CANDIDATE__.custom_import_root_revision r
        WHERE EXISTS(SELECT 1 FROM expected_build) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN expected_build b ON b.build_id=o.build_id WHERE o.root_revision_id=r.root_revision_id
        )
        UNION ALL
        SELECT 'rejection_occurrence_identity',r.rejection_id FROM __CANDIDATE__.custom_import_rejection r
        JOIN __CANDIDATE__.custom_import_build_occurrence event ON event.origin='source'
          AND event.pack_id=r.pack_id AND event.source_ordinal=r.source_ordinal
          AND (event.rejection_id=r.rejection_id OR event.resolved_rejection_id=r.rejection_id)
        LEFT JOIN __CANDIDATE__.custom_import_root_record key ON key.root_record_id=event.root_record_id
        WHERE r.root_key_sha256 IS DISTINCT FROM key.logical_key_sha256
          OR r.canonical_root_key COLLATE "C" IS DISTINCT FROM key.canonical_logical_key COLLATE "C"
        UNION ALL
        SELECT 'child_revision_missing_occurrence',c.child_revision_id FROM __CANDIDATE__.custom_import_child_revision c
        WHERE EXISTS(SELECT 1 FROM expected_build) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN expected_build b ON b.build_id=o.build_id WHERE o.child_revision_id=c.child_revision_id
        )
        UNION ALL
        SELECT 'rejection_missing_occurrence',r.rejection_id FROM __CANDIDATE__.custom_import_rejection r
        WHERE EXISTS(SELECT 1 FROM expected_build) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            JOIN expected_build b ON b.build_id=o.build_id
            WHERE o.rejection_id=r.rejection_id OR o.resolved_rejection_id=r.rejection_id
        )
        UNION ALL
        SELECT 'family_plan_reference',plan.root_record_id FROM __CANDIDATE__.custom_import_build_family plan
        WHERE NOT EXISTS(SELECT 1 FROM expected_build b WHERE b.build_id=plan.build_id)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_record r WHERE r.root_record_id=plan.root_record_id
              AND r.dataset_id=$2 AND r.logical_key_sha256 IS NOT DISTINCT FROM plan.root_key_sha256
          )
          OR (plan.family_revision_id IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f WHERE f.family_revision_id=plan.family_revision_id
              AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id,f.producing_execution_id,f.producing_fence,f.producing_token_sha256)
                IS NOT DISTINCT FROM ROW($2,$4,plan.root_record_id,$5,$7,$8)
          ))
          OR ($1 IS NOT NULL AND (plan.family_revision_id IS NULL OR plan.complete_at IS NULL))
        UNION ALL
        SELECT 'source_family_plan',plan.root_record_id FROM __CANDIDATE__.custom_import_build_family plan
        WHERE plan.selection_kind='source' AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence o
            WHERE o.occurrence_id=plan.source_root_occurrence_id AND o.build_id=plan.build_id
              AND o.root_record_id=plan.root_record_id AND o.origin='source' AND o.record_kind='root'
              AND o.root_revision_id IS NOT NULL AND o.rejection_id IS NULL AND o.resolved_rejection_id IS NULL
              AND (plan.family_revision_id IS NULL OR EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f
                WHERE f.family_revision_id=plan.family_revision_id AND f.root_revision_id=o.root_revision_id
              ))
        )
        UNION ALL
        SELECT 'family_missing_plan',f.family_revision_id FROM __CANDIDATE__.custom_import_family_revision f
        WHERE EXISTS(SELECT 1 FROM expected_build) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_family plan JOIN expected_build b ON b.build_id=plan.build_id
            WHERE plan.family_revision_id=f.family_revision_id AND plan.root_record_id=f.root_record_id
        )
        UNION ALL
        SELECT 'expected_family_plan_missing',expected.root_record_id FROM expected_plans expected
        WHERE expected.selection_kind IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_build_family plan
            WHERE plan.build_id=expected.build_id AND plan.root_record_id=expected.root_record_id
              AND plan.selection_kind=expected.selection_kind
              AND plan.source_root_occurrence_id IS NOT DISTINCT FROM
                CASE WHEN expected.selection_kind='source' THEN expected.occurrence_id END
              AND plan.base_family_revision_id IS NOT DISTINCT FROM
                CASE WHEN expected.selection_kind='retained' THEN expected.base_family_revision_id END
        )
        UNION ALL
        SELECT 'family_plan_not_selected',plan.root_record_id FROM __CANDIDATE__.custom_import_build_family plan
        WHERE EXISTS(SELECT 1 FROM expected_build b WHERE b.build_id=plan.build_id
            AND (b.plan_complete_at IS NOT NULL OR $1 IS NOT NULL)) AND NOT EXISTS (
            SELECT 1 FROM expected_plans expected WHERE expected.build_id=plan.build_id
              AND expected.root_record_id=plan.root_record_id AND expected.selection_kind=plan.selection_kind
        )
        UNION ALL
        SELECT 'source_edge_not_accepted',edge.child_revision_id FROM __CANDIDATE__.custom_import_family_child edge
        JOIN __CANDIDATE__.custom_import_build_family plan ON plan.family_revision_id=edge.family_revision_id
        WHERE plan.selection_kind='source' AND NOT EXISTS (
            SELECT 1 FROM accepted_children o WHERE o.build_id=plan.build_id AND o.root_record_id=plan.root_record_id
              AND o.collection_slot=edge.collection_slot AND o.child_revision_id=edge.child_revision_id
        )
        UNION ALL
        SELECT 'accepted_child_missing_edge',o.child_revision_id FROM accepted_children o
        JOIN __CANDIDATE__.custom_import_build_family plan ON plan.build_id=o.build_id
          AND plan.root_record_id=o.root_record_id AND plan.selection_kind='source'
        WHERE (plan.complete_at IS NOT NULL OR $1 IS NOT NULL) AND NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_child edge WHERE edge.family_revision_id=plan.family_revision_id
              AND edge.collection_slot=o.collection_slot AND edge.child_revision_id=o.child_revision_id
        )
     ) violation LIMIT 1;
